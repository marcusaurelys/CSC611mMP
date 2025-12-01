import sys
from crawler import Crawler

from distributed_crawler import CrawlWorker, Coordinator, start_coordinator, start_worker, parse_args

"""
Distributed Web Crawler

1) Start the coordinator:
    python main.py coordinator <URL> <MINUTES> --host <HOST> --port 9090

2) Start one or more workers:
    python main.py worker --uri PYRO:crawler.coordinator@<coord_host_ip>:9090 --start-url<URL> --worker-id <WORKER_ID>
"""

def main():
    args = parse_args()
    if args.role == "coordinator":
        start_coordinator(
            start_url=args.start_url,
            minutes=args.minutes,
            host=args.host,
            port=args.port
        )
    elif args.role == "worker":
        start_worker(
            uri=args.uri,
            start_url=args.start_url,
            worker_id=args.worker_id,
            threads=args.threads
        )

if __name__ == "__main__":
    main()
