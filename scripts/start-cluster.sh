#!/usr/bin/env bash
# Starts a local 3-node Hermes cluster.
#   REST ports:   8081 8082 8083
#   Socket ports: 9091 9092 9093
# Logs land in .logs/, PIDs in .logs/*.pid. Stop with scripts/stop-cluster.sh.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
JAR="$ROOT/hermes-rest/target/hermes-rest-1.0.0.jar"
MEMBERS="1@localhost:9091,2@localhost:9092,3@localhost:9093"
LOG_DIR="$ROOT/.logs"

if [[ ! -f "$JAR" ]]; then
  echo "[hermes] building..."
  (cd "$ROOT" && mvn -q -DskipTests package)
fi

mkdir -p "$LOG_DIR"

for ID in 1 2 3; do
  REST_PORT=$((8080 + ID))
  PID_FILE="$LOG_DIR/broker-$ID.pid"
  if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
    echo "[hermes] broker $ID already running (pid $(cat "$PID_FILE"))"
    continue
  fi
  HERMES_BROKER_ID=$ID \
  HERMES_REST_PORT=$REST_PORT \
  HERMES_MEMBERS=$MEMBERS \
  HERMES_DATA_DIR="$ROOT/data/broker-$ID" \
    nohup java -jar "$JAR" > "$LOG_DIR/broker-$ID.log" 2>&1 &
  echo $! > "$PID_FILE"
  echo "[hermes] broker $ID starting: REST :$REST_PORT, socket :$((9090 + ID)) (pid $!)"
done

echo "[hermes] waiting for REST APIs..."
for ID in 1 2 3; do
  REST_PORT=$((8080 + ID))
  for _ in $(seq 1 60); do
    if curl -sf "http://localhost:$REST_PORT/api/metrics" > /dev/null 2>&1; then
      echo "[hermes] broker $ID ready on :$REST_PORT"
      break
    fi
    sleep 0.5
  done
done

echo
echo "  cluster up:  curl http://localhost:8081/api/cluster"
echo "  demo:        python3 clients/python/demo.py"
echo "  dashboard:   cd dashboard && npm install && npm run dev"
