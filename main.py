import sys
from crawler import Crawler

def main():
    # Expected formats:
    #   python main.py <url> <nodes> <minutes>
    #   python main.py <url> <minutes>     ← nodes omitted (single-threaded)
    try:
        if len(sys.argv) == 4:
            url = sys.argv[1]
            nodes = sys.argv[2]
            minutes = sys.argv[3]
        elif len(sys.argv) == 3:
            url = sys.argv[1]
            nodes = None          # no threads specified
            minutes = sys.argv[2]
        else:
            print("Usage:")
            print("  python main.py <url> <nodes> <minutes>")
            print("  or: python main.py <url> <minutes>  (runs single-threaded)")
            return

        crawler = Crawler(url, nodes, minutes)
        crawler.crawl()

    except Exception as e:
        print(f"Error: {e}")
        print("Follow the format:")
        print("  python main.py <url> <nodes> <minutes>")
        print("  or: python main.py <url> <minutes>  (nodes optional)")

if __name__ == "__main__":
    main()
