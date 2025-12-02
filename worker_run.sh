#!/bin/bash

START_URL=$1
WORKER_ID=$2
MINUTES=$3
THREADS=$4
NS_IP=$5


echo "[SCRIPT] Starting Worker..."
python3 main.py worker \
    --start-url "$START_URL" \
    --worker-id "$WORKER_ID" \
    --threads "$THREADS" \
    --ns-host "$NS_IP" --ns-port 9090 &
WORKER_PID=$!



