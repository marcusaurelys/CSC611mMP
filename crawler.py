import threading
import time

class Timer:
    def __init__(self, minutes):
        self.done = threading.Event()
        self.minutes = minutes
        threading.Thread(target=self._run, daemon=True).start()

    def _run(self):
        time.sleep(self.minutes * 60)
        self.done.set()

    def isDone(self):
        return self.done.is_set()

class Crawler:
    def __init__(self, url, nodes, minutes):
        self.url = url
        self.nodes = int(nodes)
        self.minutes = float(minutes)
        assert isinstance(self.url, str), "Mali url"
        assert isinstance(self.nodes, int), "Mali nodes"
        assert isinstance(self.minutes, float), "Mali minutes"
        print(f"Initialized crawler on {self.url} with {self.nodes} nodes for {self.minutes} minutes...")



    def crawl(self):
        t = Timer(self.minutes)

        while not t.isDone():
            print("Crawling")
            time.sleep(0.1)

        print("Done crawling!")
        print("Saved text file...")
        print("Saved csv file...")