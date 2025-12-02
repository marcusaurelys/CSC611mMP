from __future__ import annotations

import argparse
import queue
import threading
import time
import urllib.parse
from typing import Iterable

import Pyro5.api
from Pyro5 import errors as pyro_errors
import requests
from bs4 import BeautifulSoup

PDF_EXTENSIONS = (".pdf",)
REQUEST_HEADERS = {"User-Agent": "CSC611M-Distributed-Crawler (+student project)"}
HTML_MIME_PREFIXES = ("text/html", "application/xhtml+xml", "application/pdf")


def _is_pdf_url(url: str) -> bool:
    # quick URL-level filter (case-insensitive)
    return url.lower().endswith(PDF_EXTENSIONS)


def is_html_response(resp: requests.Response) -> bool:
    ctype = resp.headers.get("Content-Type", "").lower()
    return ctype.startswith("text/html")


def normalize_url(base: str, link: str) -> str | None:
    resolved = urllib.parse.urljoin(base, link.split("#", 1)[0])
    parsed = urllib.parse.urlparse(resolved)
    if parsed.scheme not in {"http", "https"}:
        return None
    return urllib.parse.urlunparse(parsed)


@Pyro5.api.expose
@Pyro5.api.behavior(instance_mode="single")
class Coordinator:
    def __init__(self, start_url: str, minutes: float):
        self.start_url = start_url.rstrip("/")
        self.start_netloc = urllib.parse.urlparse(self.start_url).netloc

        self.frontier: queue.Queue[str] = queue.Queue()
        self.visited: set[str] = set()
        self.discovered: set[str] = set()
        self.results: dict[str, str] = {}

        # track in-flight URLs and which worker they were assigned to
        self.in_flight: dict[str, float] = {}       # url -> timestamp_assigned
        self.worker_for_url: dict[str, str] = {}    # url -> worker_id

        self.stop_event = threading.Event()
        self.lock = threading.Lock()

        self.frontier.put(self.start_url)
        self.discovered.add(self.start_url)

        self.active_workers: set[str] = set()

        # Timer thread for global stop
        self._timer = threading.Thread(
            target=self._countdown, args=(minutes,), daemon=True
        )
        self._timer.start()

        # watchdog thread to requeue stuck URLs
        self._requeue_thread = threading.Thread(
            target=self._requeue_stuck, args=(10.0,), daemon=True  # timeout in seconds
        )
        self._requeue_thread.start()

    def register_worker(self, worker_id: str) -> None:
        with self.lock:
            self.active_workers.add(worker_id)
        print(f"[COORD] Worker registered: {worker_id}")

    def unregister_worker(self, worker_id: str) -> None:
        with self.lock:
            self.active_workers.discard(worker_id)
        print(f"[COORD] Worker unregistered: {worker_id}")

    def can_shutdown(self) -> bool:
        with self.lock:
            # If timer hasn't fired yet, don't stop.
            if not self.stop_event.is_set():
                return False

            # After the timer fires, workers will no longer request new URLs.
            # So anything left in `frontier` is "dead work" and should NOT
            # block shutdown. The only real pending work is URLs that are
            # currently in-flight (being processed by some worker).
            no_live_workers = not self.active_workers
            no_in_flight = not self.in_flight

            # Allow shutdown if:
            #   - timer expired AND no live workers, OR
            #   - timer expired AND nothing is in-flight (even if some worker
            #     died without unregistering or there are leftover URLs in frontier).
            return no_live_workers or no_in_flight



    def _countdown(self, minutes: float) -> None:
        time.sleep(minutes * 60)
        self.stop_event.set()
        print("[COORD] Timer expired; stopping crawl")

    def _requeue_stuck(self, timeout: float = 30.0) -> None:
        """Periodically requeue URLs that have been in-flight for too long."""
        while not self.stop_event.is_set():
            now = time.time()
            with self.lock:
                stuck = [url for url, ts in self.in_flight.items() if now - ts > timeout]
                for url in stuck:
                    worker_id = self.worker_for_url.get(url)
                    print(
                        f"[COORD] Requeuing stuck URL {url} (previously assigned to {worker_id})"
                    )
                    self.frontier.put(url)
                    self.in_flight.pop(url, None)
                    self.worker_for_url.pop(url, None)
            time.sleep(5.0)

    def request_url(self, worker_id: str) -> str | None:
        if self.stop_event.is_set():
            return None
        try:
            url = self.frontier.get_nowait()
            with self.lock:
                self.in_flight[url] = time.time()
                self.worker_for_url[url] = worker_id
            print(f"[COORD] Assigned {url} to {worker_id}")
            return url
        except queue.Empty:
            print(f"[COORD] Frontier drained; waiting for new URLs...")
            return "WAITING"

    def submit_page(
        self, worker_id: str, url: str, title: str, links: Iterable[str]
    ) -> None:
        link_list = list(links)
        with self.lock:
            # done with this URL → remove from in-flight tracking
            self.in_flight.pop(url, None)
            self.worker_for_url.pop(url, None)

            self.visited.add(url)
            self.results[url] = title

            # If we've already decided to stop, don't enqueue any new links
            if self.stop_event.is_set():
                print(f"[COORD] Stop flag set; not enqueuing new links from {url}")
                link_count = 0
            else:
                for link in link_list:
                    normalized = normalize_url(url, link)
                    if not normalized:
                        continue
                    parsed = urllib.parse.urlparse(normalized)
                    if parsed.netloc != self.start_netloc:
                        continue
                    if normalized.endswith("/"):
                        normalized = normalized[:-1]
                    if normalized in self.discovered:
                        continue
                    self.discovered.add(normalized)
                    self.frontier.put(normalized)
                link_count = len(link_list)
        print(f"[COORD] {worker_id} submitted {url} with {link_count} links")

    def report_failure(self, worker_id: str, url: str, reason: str) -> None:
        with self.lock:
            # failure also completes this in-flight URL
            self.in_flight.pop(url, None)
            self.worker_for_url.pop(url, None)

            self.visited.add(url)  # mark as visited even on failure
        print(f"[COORD] {worker_id} failed {url}: {reason}")

    def should_stop(self) -> bool:
        return self.stop_event.is_set()

    def shutdown(self) -> None:
        self.stop_event.set()

    def snapshot(self) -> dict[str, int]:
        with self.lock:
            return {
                "frontier_size": self.frontier.qsize(),
                "visited_count": len(self.visited),
                "discovered_count": len(self.discovered),
                "results": len(self.results),
                "in_flight_count": len(self.in_flight),
            }


class CrawlWorker:
    def __init__(
        self, coordinator_uri: str, start_url: str, worker_id: str, num_threads: int = 1
    ):
        # Store URI instead of a single shared proxy
        self.coordinator_uri = coordinator_uri
        self.start_url = start_url.rstrip("/")
        self.worker_id = worker_id
        self.num_threads = num_threads

        # One proxy just for register/unregister in main thread
        self._control_proxy = Pyro5.api.Proxy(coordinator_uri)
        self._control_proxy.register_worker(self.worker_id)

    def run(self) -> None:
        print(f"[WORKER {self.worker_id}] Starting with {self.num_threads} local threads")

        threads: list[threading.Thread] = []
        for idx in range(self.num_threads):
            t = threading.Thread(
                target=self._thread_main,
                args=(idx + 1,),
                daemon=True,
            )
            t.start()
            threads.append(t)

        try:
            # Wait for all worker threads to finish
            for t in threads:
                t.join()
        finally:
            # Unregister once per worker process
            try:
                self._control_proxy.unregister_worker(self.worker_id)
            except Exception:
                pass
            try:
                self._control_proxy._pyroRelease()
            except Exception:
                pass
            print(f"[WORKER {self.worker_id}] All local threads stopped, worker shutting down")

    def _thread_main(self, thread_index: int) -> None:
        """Main loop for each local thread on the worker machine."""
        worker_tag = f"{self.worker_id}-T{thread_index}"
        proxy = Pyro5.api.Proxy(self.coordinator_uri)
        session = requests.Session()
        session.headers.update(REQUEST_HEADERS)
        session.max_redirects = 10

        try:
            while True:
                # Global stop check first
                try:
                    if proxy.should_stop():
                        print(f"[WORKER {worker_tag}] Coordinator requested stop")
                        break
                except Pyro5.errors.CommunicationError:
                    print(f"[WORKER {worker_tag}] Lost connection while checking stop flag, exiting")
                    break

                # Ask coordinator for a URL
                try:
                    url = proxy.request_url(worker_tag)
                except Pyro5.errors.CommunicationError:
                    print(f"[WORKER {worker_tag}] Lost connection to coordinator, exiting")
                    break

                if url is None:
                    print(f"[WORKER {worker_tag}] No more work (None), exiting")
                    break
                elif url == "WAITING":
                    print(f"[WORKER {worker_tag}] No work yet, waiting...")
                    time.sleep(1)
                    continue

                print(f"[WORKER {worker_tag}] Crawling {url}")
                self._crawl(url, proxy, session, worker_tag)
                print(f"[WORKER {worker_tag}] Finished {url}")
        finally:
            try:
                proxy._pyroRelease()
            except Exception:
                pass

    def _crawl(
        self,
        url: str,
        proxy: Pyro5.api.Proxy,
        session: requests.Session,
        worker_tag: str,
    ) -> None:
        try:

            resp = session.get(url, timeout=5, allow_redirects=True)
            resp.raise_for_status()
        except requests.exceptions.TooManyRedirects as exc:
            msg = f"{type(exc).__name__}: {exc}"
            print(f"[WORKER {worker_tag}] Error fetching {url}: {msg}")
            try:
                proxy.report_failure(worker_tag, url, f"TooManyRedirects: {exc}")
            except pyro_errors.CommunicationError:
                pass
            return
        except Exception as exc:
            msg = f"{type(exc).__name__}: {exc}"
            print(f"[WORKER {worker_tag}] Error fetching {url}: {msg}")
            try:
                proxy.report_failure(worker_tag, url, msg)
            except pyro_errors.CommunicationError:
                pass
            return

        # If coordinator decided to stop while we were downloading, bail out now
        try:
            if proxy.should_stop():
                print(
                    f"[WORKER {worker_tag}] Stop requested after downloading {url}, "
                    "skipping parse/submit"
                )
                try:
                    proxy.report_failure(worker_tag, url, "Skipped due to shutdown")
                except pyro_errors.CommunicationError:
                    pass
                return
        except pyro_errors.CommunicationError:
            return

        if not is_html_response(resp):
            print(f"[WORKER {worker_tag}] Non-HTML / PDF content at {url}")
            try:
                proxy.report_failure(worker_tag, url, "Non-HTML content")
            except pyro_errors.CommunicationError:
                pass
            return

        try:
            soup = BeautifulSoup(resp.text, "html.parser")
            if _is_pdf_url(url):
                title = "PDF File"
            else:
                title = soup.title.string.strip() if soup.title else "No title"

            links = [a.get("href") for a in soup.find_all("a", href=True)]
        except Exception as exc:
            msg = f"{type(exc).__name__}: {exc}"
            print(f"[WORKER {worker_tag}] Error parsing {url}: {msg}")
            try:
                proxy.report_failure(worker_tag, url, msg)
            except pyro_errors.CommunicationError:
                pass
            return

        # another stop check here
        try:
            if proxy.should_stop():
                print(
                    f"[WORKER {worker_tag}] Stop requested while scraping {url}, "
                    "not submitting to coordinator"
                )
                try:
                    proxy.report_failure(worker_tag, url, "Skipped due to shutdown")
                except pyro_errors.CommunicationError:
                    pass
                return
        except pyro_errors.CommunicationError:
            return

        try:
            proxy.submit_page(worker_tag, url, title, links)
        except pyro_errors.CommunicationError:
            return

        time.sleep(0.5)  # polite crawling delay


def persist_results(coordinator: Coordinator) -> None:
    with coordinator.lock:
        with open("./outputs/distributed_results.txt", "w", encoding="utf-8") as f:
            f.write(
                f"number of pages scraped: {len(coordinator.results)}\n"
                f"number of urls discovered: {len(coordinator.discovered)}\n"
                f"number of unique urls accessed: {len(coordinator.visited)}\n"
            )

        with open("./outputs/distributed_sites.csv", "w", encoding="utf-8") as f:
            f.write("link,title\n")
            for link, title in coordinator.results.items():
                safe_title = (title or "").replace('"', '""')
                f.write(f'{link},"{safe_title}"\n')

    print("Saved distributed_results.txt and distributed_sites.csv")


def start_coordinator(
    start_url: str, minutes: float, host: str, ns_host: str, ns_port: int = 9090
) -> None:
    coordinator = Coordinator(start_url, minutes)

    with Pyro5.api.Daemon(host=host) as daemon:
        uri = daemon.register(coordinator, objectId="crawler.coordinator")
        print(f"[COORD] Coordinator daemon URI: {uri}")

        # Register with Name Server
        try:
            ns = Pyro5.api.locate_ns(host=ns_host, port=ns_port)
            try:
                # safe=True will raise if name already exists; catch and overwrite
                ns.register("crawler.coordinator", uri, safe=True)
            except Pyro5.errors.NamingError:
                ns.remove("crawler.coordinator")
                ns.register("crawler.coordinator", uri)
            print(
                f"[COORD] Registered with Name Server at {ns_host} as 'crawler.coordinator'"
            )
        except Exception as e:
            print(f"[COORD] WARNING: could not register with Name Server at {ns_host}: {e}")
            return

        print("[COORD] Waiting for workers to connect...")
        daemon.requestLoop(loopCondition=lambda: not coordinator.can_shutdown())

    persist_results(coordinator)


def start_worker(
    start_url: str, worker_id: str, threads: int, ns_host: str, ns_port: int = 9090
) -> None:
    # Look up coordinator via Name Server
    try:
        ns = Pyro5.api.locate_ns(host=ns_host, port=ns_port)
    except Exception as e:
        print(f"[WORKER {worker_id}] ERROR: cannot locate Name Server at {ns_host}: {e}")
        return

    try:
        uri = ns.lookup("crawler.coordinator")
    except Exception as e:
        print(
            f"[WORKER {worker_id}] ERROR: cannot find 'crawler.coordinator' in Name Server: {e}"
        )
        return

    print(f"[WORKER {worker_id}] Found coordinator at {uri}")

    worker = CrawlWorker(str(uri), start_url, worker_id, num_threads=threads)
    print(f"[WORKER {worker_id}] Starting crawl")
    worker.run()


def parse_args():
    parser = argparse.ArgumentParser(description="Distributed Web Crawler")
    subparsers = parser.add_subparsers(dest="role", required=True)

    coord = subparsers.add_parser("coordinator", help="Start the coordinator")
    coord.add_argument("start_url")
    coord.add_argument("minutes", type=float)
    coord.add_argument("--host", default="0.0.0.0")
    coord.add_argument(
        "--port",
        type=int,
        default=0,
        help="Local daemon port (0 = auto; avoid 9090 to not clash with Name Server)",
    )
    coord.add_argument(
        "--ns-host",
        default="localhost",
        help="Host where the Pyro5 Name Server is running",
    )
    coord.add_argument(
        "--ns-port",
        type=int,
        default=9090,
        help="Port where the Pyro5 Name Server is running",
    )

    worker = subparsers.add_parser("worker", help="Start a crawl worker")
    worker.add_argument("--start-url", required=True, dest="start_url")
    worker.add_argument("--worker-id", default="remote")
    worker.add_argument(
        "--threads", type=int, default=1, help="Local threads per worker machine"
    )
    worker.add_argument(
        "--ns-host",
        default="localhost",
        help="Host where the Pyro5 Name Server is running",
    )
    worker.add_argument(
        "--ns-port",
        type=int,
        default=9090,
        help="Port where the Pyro5 Name Server is running",
    )

    return parser.parse_args()
