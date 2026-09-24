#!/usr/bin/env bash
# Stops all brokers started by start-cluster.sh.
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="$ROOT/.logs"

for PID_FILE in "$LOG_DIR"/broker-*.pid; do
  [[ -e "$PID_FILE" ]] || continue
  PID=$(cat "$PID_FILE")
  if kill -0 "$PID" 2>/dev/null; then
    kill "$PID" && echo "[hermes] stopped pid $PID ($(basename "$PID_FILE" .pid))"
  else
    echo "[hermes] $(basename "$PID_FILE" .pid) not running"
  fi
  rm -f "$PID_FILE"
done
