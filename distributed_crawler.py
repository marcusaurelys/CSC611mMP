from __future__ import annotations

import argparse
import queue
import threading
import time
import urllib.parse
from typing import Iterable

import Pyro5.api
import requests
from bs4 import BeautifulSoup

REQUEST_HEADERS = { "User-Agent": "CSC611M-Distributed-Crawler (+student project)" }
HTML_MIME_PREFIXES = ("text/html", "application/xhtml+xml", "application/pdf")

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
        
        self._timer = threading.Thread(target=self._countdown, args=(minutes,), daemon=True)
        self._timer.start()
    
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
            print(f"[COORD] Frontier drained; stopping crawl")
            self.stop_event.set()
            return None
        
        
    def submit_page(self, worker_id: str, url: str, title: str, links: Iterable[str]) -> None:
        link_list = list(links)
        with self.lock:
            self.visited.add(url)
            self.results[url] = title
            
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
    def __init__(self, coordinator_uri: str, start_url: str, worker_id: str, hmac_secret: str | None):
        self.proxy = Pyro5.api.Proxy(coordinator_uri)
        self.start_url = start_url.rstrip("/")
        self.worker_id = worker_id
        self.session = requests.Session()
        self.session.headers.update(REQUEST_HEADERS)

    def run(self) -> None:
        while True:
            url = self.proxy.request_url(self.worker_id)
            if url is None:
                if self.proxy.should_stop():
                    break
                time.sleep(1)
                continue
            self._crawl(url)
        print(f"[WORKER {self.worker_id}] Stopping crawl as instructed by coordinator")

    def _crawl(self, url: str) -> None:
        try:
            resp = self.session.get(url, timeout=10)
        except Exception as exc:
            self.proxy.report_failure(self.worker_id, url, str(exc))
            return
        
        if resp.status_code != 200:
            self.proxy.report_failure(self.worker_id, url, f"HTTP {resp.status_code}")
            return
        
        if not is_html_response(resp):
            self.proxy.report_failure(self.worker_id, url, "Non-HTML content")
            return

        soup = BeautifulSoup(resp.text, "html.parser")
        title = soup.title.string.strip() if soup.title else "No title"
        links = [a.get("href") for a in soup.find_all("a", href=True)]
        self.proxy.submit_page(self.worker_id, url, title, links)
        time.sleep(0.5) # polite crawling delay


def persist_results(coordinator: Coordinator) -> None:
    with coordinator.lock:
        with open("distributed_results.txt", "w", encoding="utf-8") as f:
            f.write(
                f"number of pages scraped: {len(coordinator.results)}\n"
                f"number of urls discovered: {len(coordinator.discovered)}\n"
                f"number of unique urls accessed: {len(coordinator.visited)}\n"
            )
        
        with open("distributed_sites.csv", "w", encoding="utf-8") as f:
            f.write("link,title\n")
            for link, title in coordinator.results.items():
                safe_title = (title or "").replace('"', '""')
                f.write(f'{link},"{safe_title}"\n')
                
    print("Saved distributed_results.txt and distributed_sites.csv")


def start_coordinator(start_url: str, minutes: float, host: str, port: int, hmac_secret: str | None) -> None:
    coordinator = Coordinator(start_url, minutes)
    with Pyro5.api.Daemon(host=host, port=port) as daemon:
        uri = daemon.register(coordinator, objectId="crawler.coordinator")
        print(f"[COORD] Coordinator is running at: {uri}")
        print("[COORD] Waiting for workers to connect...")
        daemon.requestLoop(loopCondition=lambda: not coordinator.should_stop())
    
    persist_results(coordinator)


def start_worker(uri: str, start_url: str, worker_id: str, hmac_secret: str | None) -> None:
    worker = CrawlWorker(uri, start_url, worker_id, hmac_secret)
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
    coord.add_argument("--hmac-secret", default=None, dest="hmac_secret")
    
    worker = subparsers.add_parser("worker", help="Start a crawl worker")
    worker.add_argument("--uri", required=True, help="URI of the coordinator")
    worker.add_argument("--start-url", required=True, dest="start_url")
    worker.add_argument("--worker-id", default="remote")
    worker.add_argument("--hmac-secret", default=None, dest="hmac_secret")
    
    return parser.parse_args()


def main():
    args = parse_args()
    if args.role == "coordinator":
        start_coordinator(
            start_url=args.start_url,
            minutes=args.minutes,
            host=args.host,
            port=args.port,
            hmac_secret=args.hmac_secret,
        )
    elif args.role == "worker":
        start_worker(
            uri=args.uri,
            start_url=args.start_url,
            worker_id=args.worker_id,
            hmac_secret=args.hmac_secret,
        )

if __name__ == "__main__":
    main()