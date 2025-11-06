import threading
import time
import queue
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor

PDF_EXTENSIONS = (".pdf",)
HTML_MIME_PREFIXES = ("text/html", "application/xhtml+xml")
REQUEST_HEADERS = {"User-Agent": "CSC611M-Crawler/1.0 (+student project)"}

class Timer:
    def __init__(self, minutes):
        self.done = threading.Event()
        self.minutes = minutes
        threading.Thread(target=self._run, daemon=True).start()

    def _run(self):
        time.sleep(self.minutes * 60)
        self.done.set()

    def is_done(self):
        return self.done.is_set()
    

class Crawler:
    def __init__(self, url, nodes, minutes):
        self.start_url = url
        self.nodes = int(nodes)
        self.minutes = float(minutes)
        assert isinstance(self.start_url, str), "Mali url"
        assert isinstance(self.nodes, int), "Mali nodes"
        assert isinstance(self.minutes, float), "Mali minutes"
        
        self.frontier = queue.Queue()
        self.seen = set()
        self.seen_lock = threading.Lock()
        self.results = dict()
        self.res_lock = threading.Lock()
        self.total_urls_found = set()
        self.url_lock = threading.Lock()
        
        print(f"Initialized crawler on {self.start_url} with {self.nodes} nodes for {self.minutes} minutes...")
    
    def _is_pdf_url(self, url: str) -> bool:
        # quick URL-level filter (case-insensitive)
        return url.lower().endswith(PDF_EXTENSIONS)

    def _is_html_response(self, resp: requests.Response) -> bool:
        ctype = resp.headers.get("Content-Type", "").lower()
        return any(ctype.startswith(prefix) for prefix in HTML_MIME_PREFIXES)

    def worker(self, timer, i):
        session = requests.Session()
        session.headers.update(REQUEST_HEADERS)
        
        while not timer.is_done():
            try:
                url = self.frontier.get(timeout=1) #timeout so this doesn't block forever...
            except:
                print(f"[THREAD {i}]: waiting for input")
                continue
                
            # Skip URLs that look like PDFs outright
            if self._is_pdf_url(url):
                # print(f"[THREAD {i}]: Skipping PDF link {url}")
                continue

            with self.seen_lock: #if url is in seen stop here, otherwise add it to seen
                if url in self.seen:
                    continue
                self.seen.add(url)
            
            try:
                print(f"[THREAD {i}]: Scraping {url}")
                res = session.get(url, timeout=10)
                
                # Skip non-OK status codes
                if res.status_code != 200:
                    print(f"[THREAD {i}]: Skipping {url} (status {res.status_code})")
                    continue
                    
                # Skip non-HTML responses, including PDFs served without .pdf in the URL
                if not self._is_html_response(res):
                    # print(f"[THREAD {i}]: Skipping non-HTML content at {url} (Content-Type={res.headers.get('Content-Type')})")
                    continue
                
                soup = BeautifulSoup(res.text, "html.parser")
                title = soup.title.string if soup.title else "No title"

                with self.res_lock:
                    self.results[url] = title

                children = []
                for link in soup.find_all("a", href=True):
                    child = link["href"].strip()
                    
                   # basic normalization similar to your original approach
                    if child.startswith("/"):
                        child = self.start_url.rstrip("/") + child

                    # Drop fragments and mailto/tel/javascript
                    if child.startswith("#") or child.startswith("mailto:") or child.startswith("tel:") or child.startswith("javascript:"):
                        continue

                    # trim trailing slash for consistency
                    if child.endswith("/"):
                        child = child[:-1]

                    # Stay under the same site root you provided originally
                    if child.startswith(self.start_url):
                        # Do not enqueue PDF links
                        if self._is_pdf_url(child):
                            continue
                        children.append(child)
                
                for child in children:
                    with self.url_lock:
                        self.total_urls_found.add(child)
                    self.frontier.put(child)
                    
                print(f"[THREAD {i}]: Done scraping {url}, children added to frontier")
                #sleep so we dont die
                time.sleep(3)
            except:
                continue

    def crawl(self):
        self.frontier.put(self.start_url)
        with ThreadPoolExecutor(max_workers=self.nodes) as pool: 
            t = Timer(self.minutes)
            for i in range(self.nodes):
                pool.submit(self.worker, t, i+1)
            while not t.is_done():
                time.sleep(1)

        print("Done crawling!")

        with open("results.txt", "w", encoding="utf-8") as f:
            f.write(
                f"number of pages scraped: {len(self.seen)} \n"
                f"number of urls found: {len(self.total_urls_found)} \n"
                f"number of unique urls accessed: {len(self.results)}"
            )
        print("Saved text file...")

        with open("sites.csv", "w", encoding="utf-8") as f:
            f.write("link, title\n")
        with open("sites.csv", "a", encoding="utf-8") as f:
            for link, title in self.results.items():
                safe_title = (title or "").replace('"', '""')
                f.write(f'{link},"{safe_title}"\n')
        print("Saved csv file...")