#!/bin/bash

START_URL=$1
MINUTES=$2
THREADS=$3
HOST_IP=$(hostname -I | awk '{print $1}')

echo "[SCRIPT] Starting Pyro5 Name Server on $HOST_IP..."
python3 -m Pyro5.nameserver -n "$HOST_IP" --port 9090 &
NS_PID=$!

sleep 2

echo "[SCRIPT] Starting Coordinator..."
python3 main.py coordinator "$START_URL" "$MINUTES" \
    --host "$HOST_IP" --ns-host "$HOST_IP" --ns-port 9090 &
COORD_PID=$!

sleep 2

echo "[SCRIPT] Starting Worker..."
python3 main.py worker \
    --start-url "$START_URL" \
    --worker-id LOCAL \
    --threads "$THREADS" \
    --ns-host "$HOST_IP" --ns-port 9090 &
WORKER_PID=$!

# When script exits, kill remaining processes
trap "kill $NS_PID $WORKER_PID 2>/dev/null" EXIT

echo "[SCRIPT] Waiting for coordinator (PID=$COORD_PID) to exit..."
wait $COORD_PID   # <--- waits ONLY for the coordinator

echo "[SCRIPT] Coordinator ended. Killing worker + nameserver..."
kill $WORKER_PID 2>/dev/null
kill $NS_PID 2>/dev/null

echo "[SCRIPT] Shutdown complete."
