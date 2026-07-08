#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"
log_dir="${RUSNEB_LOG_DIR:-run-logs}"
ssh_target="${RUSNEB_SSH:-ubuntu@151.145.94.114}"
mkdir -p "$log_dir"

parser=()
if [[ -x ./target-codex/release/rusneb-parser ]] &&
  ./target-codex/release/rusneb-parser crawl --help | grep -q -- "--ssh"; then
  parser=(./target-codex/release/rusneb-parser)
elif [[ -x ./target/release/rusneb-parser ]] &&
  ./target/release/rusneb-parser crawl --help | grep -q -- "--ssh"; then
  parser=(./target/release/rusneb-parser)
else
  parser=(cargo run --release --)
fi

echo "Resetting failed HTTP 403 rows to pending..."
"${parser[@]}" retry-failed --http-status 403 >> "$log_dir/rusneb-crawl.log" 2>&1

nohup "${parser[@]}" crawl \
  --catalog 25 --access open \
  --publishyear-prev 1 --publishyear-next 2026 --shard-years \
  --workers 8 \
  --max-consecutive-transport-errors 16 \
  --transient-error-pause-secs 120 \
  --ssh "$ssh_target" \
  >> "$log_dir/rusneb-crawl.log" 2>&1 &

echo "$!" > "$log_dir/crawl.pid"
echo "Started rusneb crawl as PID $(cat "$log_dir/crawl.pid")"
echo "Log: $log_dir/rusneb-crawl.log"
echo "SSH: $ssh_target"
