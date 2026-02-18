#!/usr/bin/env bash
set -euo pipefail

INTERVAL="${1:-300}"
STATE_PATH="${2:-data/zinc22/purchasable_druglike.smi.state.json}"
LOG_PATH="${3:-logs/zinc22_status.log}"
PYTHON_BIN="${PYTHON_BIN:-python}"

if ! [[ "$INTERVAL" =~ ^[0-9]+$ ]]; then
  echo "Interval must be an integer (seconds). Got: $INTERVAL" >&2
  exit 1
fi

mkdir -p "$(dirname "$LOG_PATH")"
echo "Started zinc22 monitor at $(date +'%Y-%m-%d %H:%M:%S %z')" >> "$LOG_PATH"

while true; do
  ts="$(date +'%Y-%m-%d %H:%M:%S %z')"
  echo "----- ${ts} -----" >> "$LOG_PATH"
  if [ -f "$STATE_PATH" ]; then
    "$PYTHON_BIN" -u -m duetscreen status --zinc-state "$STATE_PATH" >> "$LOG_PATH" 2>&1
  else
    echo "state_missing=$STATE_PATH" >> "$LOG_PATH"
  fi
  sleep "$INTERVAL"
done
