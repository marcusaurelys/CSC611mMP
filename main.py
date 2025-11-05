import sys
from crawler import Crawler

def main():
    #try:
        url = sys.argv[1]
        nodes = sys.argv[2]
        minutes = sys.argv[3]
        crawler = Crawler(url, nodes, minutes)
        crawler.crawl()

    #except Exception:
    #    print("Invalid arguments or something...")
    #    print("Follow the format: python main.py <url> <number of nodes>")



if __name__ == "__main__":
    main()