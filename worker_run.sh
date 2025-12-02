#!/bin/bash

START_URL=$1
MINUTES=$2
THREADS=$3
NS_IP=$4


echo "[SCRIPT] Starting Worker..."
python3 main.py worker \
    --start-url "$START_URL" \
    --worker-id WORKER1 \
    --threads "$THREADS" \
    --ns-host "$NS_IP" --ns-port 9090 &
WORKER_PID=$!



