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
REQUEST_HEADERS = { "User-Agent": "CSC611M-Distributed-Crawler (+student project)" }
HTML_MIME_PREFIXES = ("text/html", "application/xhtml+xml", "application/pdf")

def _is_pdf_url(url: str) -> bool:
    # quick URL-level filter (case-insensitive)
    return url.lower().endswith(PDF_EXTENSIONS)

def is_html_response(resp: requests.Response) -> bool:
    ctype = resp.headers.get("Content-Type", "").lower()
    return any(ctype.startswith(prefix) for prefix in HTML_MIME_PREFIXES)

def normalize_url(base: str, link: str) -> str:
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
        self.stop_event = threading.Event()
        self.lock = threading.Lock()
        
        self.frontier.put(self.start_url)
        self.discovered.add(self.start_url)
        
        self.active_workers: set[str] = set()
        
        self._timer = threading.Thread(target=self._countdown, args=(minutes,), daemon=True)
        self._timer.start()
    
    def register_worker(self, worker_id: str) -> None:
        with self.lock:
            self.active_workers.add(worker_id)
        print(f"[COORD] Worker registered: {worker_id}")

    def unregister_worker(self, worker_id: str) -> None:
        with self.lock:
            self.active_workers.discard(worker_id)
        print(f"[COORD] Worker unregistered: {worker_id}")

    def can_shutdown(self) -> bool:
        # Only allow daemon to stop once:
        #   1) timer fired (stop_event set), AND
        #   2) no more active workers
        with self.lock:
            return self.stop_event.is_set() and not self.active_workers

    
    def _countdown(self, minutes: float) -> None:
        time.sleep(minutes * 60)
        self.stop_event.set()
        print("[COORD] Timer expired; stopping crawl")
    
    def request_url(self, worker_id: str) -> str | None:
        if self.stop_event.is_set():
            return None
        try:
            url = self.frontier.get_nowait()
            print(f"[COORD] Assigned {url} to {worker_id}")
            return url
        except queue.Empty:
            print(f"[COORD] Frontier drained; waiting for new URLs...")
            return "WAITING"
        
        
    def submit_page(self, worker_id: str, url: str, title: str, links: Iterable[str]) -> None:
        link_list = list(links)
        with self.lock:
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
            self.visited.add(url)   # mark as visited even on failure
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
            }



class CrawlWorker:
    def __init__(self, coordinator_uri: str, start_url: str, worker_id: str):
        self.proxy = Pyro5.api.Proxy(coordinator_uri)
        self.start_url = start_url.rstrip("/")
        self.worker_id = worker_id
        self.session = requests.Session()
        self.session.headers.update(REQUEST_HEADERS)
        
        self.proxy.register_worker(self.worker_id)

    def run(self) -> None:
        try:
            while True:
                try:
                    url = self.proxy.request_url(self.worker_id)
                except Pyro5.errors.CommunicationError:
                    # Coordinator went away unexpectedly
                    print(f"[WORKER {self.worker_id}] Lost connection to coordinator, exiting")
                    break

                if url is None:
                    if self.proxy.should_stop():
                        print(f"[WORKER {self.worker_id}] Coordinator requested stop")
                        break
                    time.sleep(1)
                    continue
                elif url == "WAITING":
                    print(f"[WORKER {self.worker_id}] No work yet, waiting...")
                    time.sleep(1)
                    continue
                
                print(f"[WORKER {self.worker_id}] Crawling {url}")
                self._crawl(url)
                print(f"[WORKER {self.worker_id}] Finished {url}")
        finally:
            # Even if we got an exception, try to unregister once
            try:
                self.proxy.unregister_worker(self.worker_id)
            except Exception:
                pass
            print(f"[WORKER {self.worker_id}] Stopping crawl as instructed by coordinator")


    def _crawl(self, url: str) -> None:
        self.session.max_redirects = 10
        try:
            resp = self.session.get(url, timeout=10, allow_redirects=True)
            resp.raise_for_status()
        except requests.exceptions.TooManyRedirects as exc:
            # Avoid infinite redirects; just report and stop this URL
            msg = f"{type(exc).__name__}: {exc}"
            print(f"[WORKER {self.worker_id}] Error fetching {url}: {msg}")
            try:
                self.proxy.report_failure(self.worker_id, url, f"TooManyRedirects: {exc}")
            except pyro_errors.CommunicationError:
                # Coordinator already gone; just ignore
                pass
            return
        except Exception as exc:
            try:
                self.proxy.report_failure(self.worker_id, url, str(exc))
            except pyro_errors.CommunicationError:
                pass
            return
        
        # If coordinator decided to stop while we were downloading, bail out now
        try:
            if self.proxy.should_stop():
                print(f"[WORKER {self.worker_id}] Stop requested after downloading {url}, skipping parse/submit")
                # mark this as "accessed but not scraped"
                self.proxy.report_failure(self.worker_id, url, "Skipped due to shutdown")
                return
        except pyro_errors.CommunicationError:
            # Coordinator is gone; just stop quietly.
            return

        if not is_html_response(resp):
            print(f"[WORKER {self.worker_id}] Non-HTML / PDF content at {url}")
            try:
                self.proxy.report_failure(self.worker_id, url, "Non-HTML content")
            except pyro_errors.CommunicationError:
                pass
            return

        soup = BeautifulSoup(resp.text, "html.parser")
        if _is_pdf_url(url):
            title = "PDF File"
        else:
            title = soup.title.string.strip() if soup.title else "No title"
        links = [a.get("href") for a in soup.find_all("a", href=True)]
        
        # another stop check here
        try:
            if self.proxy.should_stop():
                print(f"[WORKER {self.worker_id}] Stop requested while scraping {url}, not submitting to coordinator")
                self.proxy.report_failure(self.worker_id, url, "Skipped due to shutdown")
                return
        except pyro_errors.CommunicationError:
            return

        try:
            self.proxy.submit_page(self.worker_id, url, title, links)
        except pyro_errors.CommunicationError:
            # Coordinator is gone; no point continuing
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


def start_coordinator(start_url: str, minutes: float, host: str, port: int) -> None:
    coordinator = Coordinator(start_url, minutes)
    with Pyro5.api.Daemon(host=host, port=port) as daemon:
        uri = daemon.register(coordinator, objectId="crawler.coordinator")
        print(f"[COORD] Coordinator is running at: {uri}")
        print("[COORD] Waiting for workers to connect...")
        daemon.requestLoop(loopCondition=lambda: not coordinator.can_shutdown())
    
    persist_results(coordinator)


def start_worker(uri: str, start_url: str, worker_id: str) -> None:
    worker = CrawlWorker(uri, start_url, worker_id)
    print(f"[WORKER {worker_id}] Starting crawl")
    worker.run()
    
def parse_args():
    parser = argparse.ArgumentParser(description="Distributed Web Crawler")
    subparsers = parser.add_subparsers(dest="role", required=True)
    
    coord = subparsers.add_parser("coordinator", help="Start the coordinator")
    coord.add_argument("start_url")
    coord.add_argument("minutes", type=float)
    coord.add_argument("--host", default="0.0.0.0")
    coord.add_argument("--port", type=int, default=9090)
    
    worker = subparsers.add_parser("worker", help="Start a crawl worker")
    worker.add_argument("--uri", required=True, help="URI of the coordinator")
    worker.add_argument("--start-url", required=True, dest="start_url")
    worker.add_argument("--worker-id", default="remote")
    
    return parser.parse_args()