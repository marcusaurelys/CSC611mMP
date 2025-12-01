import sys
from crawler import Crawler

from distributed_crawler import CrawlWorker, Coordinator, start_coordinator, start_worker, parse_args

"""
Distributed Web Crawler

1) Start the namer server in a different machine:
    python -m Pyro5.nameserver -n <ip-addr-of-the-machine-for-ns> -p <ns-port>

2) Start the coordinator:
    python main.py coordinator https://www.dlsu.edu.ph <minutes> --host <ip-addr-of-host> --ns-host <ip-addr-of-name-server> --ns-port <ns-port>

3) Start one or more workers:
    python main.py worker --start-url https://www.dlsu.edu.ph --worker-id W1 --threads 4 --ns-host <ip-addr-of-name-server> --ns-port <ns-port>
"""

def main():
    args = parse_args()
    if args.role == "coordinator":
        start_coordinator(
            start_url=args.start_url,
            minutes=args.minutes,
            host=args.host,
            ns_host=args.ns_host,
            ns_port=args.ns_port
        )
    elif args.role == "worker":
        start_worker(
            start_url=args.start_url,
            worker_id=args.worker_id,
            threads=args.threads,
            ns_host=args.ns_host,
            ns_port=args.ns_port
        )

if __name__ == "__main__":
    main()
