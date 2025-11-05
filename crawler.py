import threading
import time
import queue
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor

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
        
        print(f"Initialized crawler on {self.start_url} with {self.nodes} nodes for {self.minutes} minutes...")

    def worker(self, timer, i):
        while not timer.is_done():
            try:
                url = self.frontier.get(timeout=1) #timeout so this doesn't block forever...
            except:
                print(f"[THREAD {i}]: waiting for input")
                continue

            with self.seen_lock: #if url is in seen stop here, otherwise add it to seen
                if url in self.seen:
                    continue
                self.seen.add(url)
            
            try:
                print(f"[THREAD {i}]: Scraping {url}")
                res = requests.get(url)
                soup = BeautifulSoup(res.text, "html.parser")
                title = soup.title.string if soup.title else "No title"

                with self.res_lock:
                    self.results[url] = title


                children = []
                for link in soup.find_all("a", href=True):
                    child = link["href"]
                    if child.startswith("/"):
                        child = self.start_url + child
                    if child.startswith(self.start_url):
                        children.append(child)
                
                for child in children:
                    self.frontier.put(child)
                print(f"[THREAD {i}]: Done scraping {url}, children added to frontier")
                #sleep so we dont die
                time.sleep(1.5)
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

        with open("results.txt", "w") as f:
            f.write(f"number of pages scraped: {len(self.seen)} \nnumber of urls found: NOT YET IMPLEMENTED \nnumber of unique urls accessed: {len(self.results)}")
        print("Saved text file...")
        with open("sites.csv", "w") as f:
            f.write("link, title\n")
        with open("sites.csv", "a") as f:
            for link, title in self.results.items():
                f.write(f"{link},\"{title}\"\n")
        print("Saved csv file...")