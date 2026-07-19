#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

log_dir="${RUSNEB_LOG_DIR:-run-logs}"
db_path="${RUSNEB_DB:-state/rusneb.sqlite}"
ssh_target="${RUSNEB_SSH-ubuntu@151.145.94.114}"
workers="${RUSNEB_WORKERS:-8}"
force="${RUSNEB_FORCE:-0}"
init_db="${RUSNEB_INIT_DB:-0}"
lock_dir=""
crawler_started=0

# Print script usage and supported environment variables.
usage() {
  cat <<'USAGE'
Usage: ./continue.sh [OPTIONS]

Options:
  --db PATH       SQLite state database path [default: state/rusneb.sqlite]
  --ssh TARGET    SSH SOCKS tunnel target [default: ubuntu@151.145.94.114]
  --no-ssh        Run without an SSH tunnel
  --workers N     Maximum item workers [default: 8]
  --log-dir PATH  Directory for per-run logs [default: run-logs]
  --init-db       Allow creating a missing SQLite database
  --force         Ignore an existing crawler lock for the same SQLite database
  -h, --help      Show this help

Environment overrides:
  RUSNEB_DB, RUSNEB_SSH, RUSNEB_WORKERS, RUSNEB_LOG_DIR,
  RUSNEB_INIT_DB=1, RUSNEB_FORCE=1
USAGE
}

# Print an error and exit.
die() {
  echo "error: $*" >&2
  exit 1
}

# Return true when a value is a positive integer.
is_positive_integer() {
  [[ "$1" =~ ^[1-9][0-9]*$ ]]
}

# Return true when a PID currently exists.
pid_is_running() {
  [[ "${1:-}" =~ ^[0-9]+$ ]] && kill -0 "$1" 2>/dev/null
}

# Remove a stale lock directory created by this script.
remove_lock_dir() {
  [[ -n "$lock_dir" && -d "$lock_dir" ]] || return 0
  rm -f "$lock_dir/pid" "$lock_dir/db" "$lock_dir/log" "$lock_dir/started_at"
  rmdir "$lock_dir"
}

# Remove the lock if the script fails before the crawler is started.
cleanup_failed_start() {
  local status=$?
  if [[ "$status" -ne 0 && "$crawler_started" -eq 0 ]]; then
    remove_lock_dir 2>/dev/null || true
  fi
  exit "$status"
}

trap cleanup_failed_start EXIT

while [[ $# -gt 0 ]]; do
  case "$1" in
    --db)
      [[ $# -ge 2 ]] || die "--db requires a path"
      db_path="$2"
      shift 2
      ;;
    --ssh)
      [[ $# -ge 2 ]] || die "--ssh requires a target"
      ssh_target="$2"
      shift 2
      ;;
    --no-ssh)
      ssh_target=""
      shift
      ;;
    --workers)
      [[ $# -ge 2 ]] || die "--workers requires a number"
      workers="$2"
      shift 2
      ;;
    --log-dir)
      [[ $# -ge 2 ]] || die "--log-dir requires a path"
      log_dir="$2"
      shift 2
      ;;
    --init-db)
      init_db=1
      shift
      ;;
    --force)
      force=1
      shift
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    *)
      die "unknown option: $1"
      ;;
  esac
done

is_positive_integer "$workers" || die "--workers must be a positive integer"
[[ "$force" == 0 || "$force" == 1 ]] || die "RUSNEB_FORCE must be 0 or 1"
[[ "$init_db" == 0 || "$init_db" == 1 ]] || die "RUSNEB_INIT_DB must be 0 or 1"
[[ -n "$db_path" ]] || die "--db must not be empty"
[[ -n "$log_dir" ]] || die "--log-dir must not be empty"

db_dir="$(dirname -- "$db_path")"
db_file="$(basename -- "$db_path")"
if [[ ! -d "$db_dir" ]]; then
  if [[ "$init_db" == 1 ]]; then
    mkdir -p "$db_dir"
  else
    die "database directory does not exist: $db_dir (use --init-db to create a new DB)"
  fi
fi
db_dir_abs="$(cd "$db_dir" && pwd -P)"
db_abs="$db_dir_abs/$db_file"

if [[ ! -e "$db_abs" && "$init_db" != 1 ]]; then
  die "SQLite database not found: $db_abs (use --init-db to create a new DB)"
fi
if [[ -e "$db_abs" ]]; then
  [[ -f "$db_abs" ]] || die "SQLite database path is not a regular file: $db_abs"
  [[ -r "$db_abs" ]] || die "SQLite database is not readable: $db_abs"
  [[ -w "$db_abs" ]] || die "SQLite database is not writable: $db_abs"
fi
[[ -w "$db_dir_abs" ]] || die "SQLite database directory is not writable: $db_dir_abs"
for sidecar in "$db_abs-wal" "$db_abs-shm"; do
  [[ ! -e "$sidecar" || -w "$sidecar" ]] || die "SQLite sidecar is not writable: $sidecar"
done

mkdir -p "$log_dir"
log_dir_abs="$(cd "$log_dir" && pwd -P)"
timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
log_file="$log_dir_abs/rusneb-crawl-$timestamp.log"
latest_log="$log_dir_abs/rusneb-crawl.latest.log"
pid_file="$log_dir_abs/crawl.pid"

parser=()

# Return true when a parser command supports the options this script needs.
parser_supports_required_options() {
  local help
  help="$("$@" crawl --help)"
  [[ "$help" == *"--ssh"* && "$help" == *"--skip-no-year-shard"* ]]
}

if [[ -x ./target-codex/release/rusneb-parser ]] &&
  parser_supports_required_options ./target-codex/release/rusneb-parser; then
  parser=(./target-codex/release/rusneb-parser)
elif [[ -x ./target/release/rusneb-parser ]] &&
  parser_supports_required_options ./target/release/rusneb-parser; then
  parser=(./target/release/rusneb-parser)
else
  command -v cargo >/dev/null || die "cargo is required when no compatible release binary exists"
  [[ -f Cargo.toml ]] || die "Cargo.toml not found; cannot run via cargo"
  parser=(cargo run --release --)
fi

if [[ -n "$ssh_target" ]]; then
  command -v ssh >/dev/null || die "ssh is required when --ssh is enabled"
fi

lock_dir="$db_abs.lock"
if [[ -d "$lock_dir" ]]; then
  existing_pid="$(cat "$lock_dir/pid" 2>/dev/null || true)"
  if pid_is_running "$existing_pid" && [[ "$force" != 1 ]]; then
    die "crawler already appears to be running for $db_abs as PID $existing_pid (use --force to override)"
  fi
  echo "Removing stale crawler lock: $lock_dir"
  remove_lock_dir || die "cannot remove stale lock: $lock_dir"
fi
mkdir "$lock_dir" || die "cannot create crawler lock: $lock_dir"
printf '%s\n' "$$" > "$lock_dir/pid"
printf '%s\n' "$db_abs" > "$lock_dir/db"
printf '%s\n' "$log_file" > "$lock_dir/log"
printf '%s\n' "$timestamp" > "$lock_dir/started_at"

: > "$log_file"
ln -sfn "$(basename -- "$log_file")" "$latest_log"

echo "Resetting failed HTTP 403 rows to pending..."
"${parser[@]}" retry-failed --db "$db_abs" --http-status 403 >> "$log_file" 2>&1

crawl_args=(
  crawl
  --db "$db_abs"
  --catalog 25 --access open
  --publishyear-prev 1 --publishyear-next 2026 --shard-years
  --workers "$workers"
  --max-consecutive-transport-errors 16
  --transient-error-pause-secs 120
)
if [[ -n "$ssh_target" ]]; then
  crawl_args+=(--ssh "$ssh_target")
fi

nohup "${parser[@]}" "${crawl_args[@]}" >> "$log_file" 2>&1 &
crawl_pid=$!
crawler_started=1
printf '%s\n' "$crawl_pid" > "$pid_file"
printf '%s\n' "$crawl_pid" > "$lock_dir/pid"

echo "Started rusneb crawl as PID $crawl_pid"
echo "Database: $db_abs"
echo "Lock: $lock_dir"
echo "Log: $log_file"
echo "Latest log symlink: $latest_log"
if [[ -n "$ssh_target" ]]; then
  echo "SSH: $ssh_target"
else
  echo "SSH: disabled"
fi
