#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

db_path="${RUSNEB_DB:-state/rusneb.sqlite}"
out_dir="${RUSNEB_OUT_DIR:-out}"
prefix="${RUSNEB_EXPORT_PREFIX:-rusneb}"
crawl_command="${RUSNEB_CRAWL_COMMAND:-./continue.sh}"
dataset_name="${RUSNEB_DATASET_NAME:-rusneb metadata}"
batch_size="${RUSNEB_PARQUET_BATCH_SIZE:-2048}"
max_attempts="${RUSNEB_MAX_ATTEMPTS:-5}"
export_jsonl="${RUSNEB_EXPORT_JSONL:-1}"
export_parquet="${RUSNEB_EXPORT_PARQUET:-1}"

# Print script usage and supported environment variables.
usage() {
  cat <<'USAGE'
Usage: ./export.sh [OPTIONS]

Options:
  --db PATH             SQLite state database path [default: state/rusneb.sqlite]
  --out-dir PATH        Output directory [default: out]
  --prefix NAME         Dataset file prefix [default: rusneb]
  --crawl-command TEXT  Crawl command recorded in the manifest [default: ./continue.sh]
  --dataset-name TEXT   Dataset name recorded in the manifest [default: rusneb metadata]
  --batch-size N        Parquet batch size [default: 2048]
  --max-attempts N      Max attempts used for manifest failure classification [default: 5]
  --no-jsonl            Do not export JSON Lines
  --no-parquet          Do not export Parquet
  -h, --help            Show this help

Environment overrides:
  RUSNEB_DB, RUSNEB_OUT_DIR, RUSNEB_EXPORT_PREFIX, RUSNEB_CRAWL_COMMAND,
  RUSNEB_DATASET_NAME, RUSNEB_PARQUET_BATCH_SIZE, RUSNEB_MAX_ATTEMPTS,
  RUSNEB_EXPORT_JSONL=0, RUSNEB_EXPORT_PARQUET=0
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

# Return true when a value is either 0 or 1.
is_binary_flag() {
  [[ "$1" == 0 || "$1" == 1 ]]
}

# Convert a path with an existing parent directory to an absolute path.
absolute_path_with_existing_parent() {
  local path=$1
  local dir
  local file
  dir="$(dirname -- "$path")"
  file="$(basename -- "$path")"
  [[ -d "$dir" ]] || die "directory does not exist: $dir"
  printf '%s/%s\n' "$(cd "$dir" && pwd -P)" "$file"
}

# Return true when a parser command supports every export command used here.
parser_supports_required_options() {
  "$@" export-jsonl --help >/dev/null &&
    "$@" export-parquet --help >/dev/null &&
    "$@" export-manifest --help >/dev/null
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --db)
      [[ $# -ge 2 ]] || die "--db requires a path"
      db_path="$2"
      shift 2
      ;;
    --out-dir)
      [[ $# -ge 2 ]] || die "--out-dir requires a path"
      out_dir="$2"
      shift 2
      ;;
    --prefix)
      [[ $# -ge 2 ]] || die "--prefix requires a name"
      prefix="$2"
      shift 2
      ;;
    --crawl-command)
      [[ $# -ge 2 ]] || die "--crawl-command requires text"
      crawl_command="$2"
      shift 2
      ;;
    --dataset-name)
      [[ $# -ge 2 ]] || die "--dataset-name requires text"
      dataset_name="$2"
      shift 2
      ;;
    --batch-size)
      [[ $# -ge 2 ]] || die "--batch-size requires a number"
      batch_size="$2"
      shift 2
      ;;
    --max-attempts)
      [[ $# -ge 2 ]] || die "--max-attempts requires a number"
      max_attempts="$2"
      shift 2
      ;;
    --no-jsonl)
      export_jsonl=0
      shift
      ;;
    --no-parquet)
      export_parquet=0
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

[[ -n "$db_path" ]] || die "--db must not be empty"
[[ -n "$out_dir" ]] || die "--out-dir must not be empty"
[[ -n "$prefix" ]] || die "--prefix must not be empty"
[[ -n "$crawl_command" ]] || die "--crawl-command must not be empty"
[[ -n "$dataset_name" ]] || die "--dataset-name must not be empty"
is_positive_integer "$batch_size" || die "--batch-size must be a positive integer"
is_positive_integer "$max_attempts" || die "--max-attempts must be a positive integer"
is_binary_flag "$export_jsonl" || die "RUSNEB_EXPORT_JSONL must be 0 or 1"
is_binary_flag "$export_parquet" || die "RUSNEB_EXPORT_PARQUET must be 0 or 1"
[[ "$export_jsonl" == 1 || "$export_parquet" == 1 ]] || die "at least one dataset format must be enabled"

db_abs="$(absolute_path_with_existing_parent "$db_path")"
[[ -f "$db_abs" ]] || die "SQLite database not found: $db_abs"
[[ -r "$db_abs" ]] || die "SQLite database is not readable: $db_abs"
[[ -w "$db_abs" ]] || die "SQLite database is not writable: $db_abs"

mkdir -p "$out_dir"
out_dir_abs="$(cd "$out_dir" && pwd -P)"
[[ -w "$out_dir_abs" ]] || die "output directory is not writable: $out_dir_abs"

command -v sha256sum >/dev/null || die "sha256sum is required"

parser=()
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

jsonl_output="$out_dir_abs/$prefix.jsonl.xz"
parquet_output="$out_dir_abs/$prefix.parquet"
manifest_output="$out_dir_abs/manifest.json"
sha256_output="$out_dir_abs/SHA256SUMS"
manifest_files=()
hash_files=()

echo "Database: $db_abs"
echo "Output directory: $out_dir_abs"

echo
echo "SQLite state:"
"${parser[@]}" stats --db "$db_abs"

if [[ "$export_jsonl" == 1 ]]; then
  echo
  echo "Exporting JSON Lines: $jsonl_output"
  "${parser[@]}" export-jsonl --db "$db_abs" --output "$jsonl_output"
  manifest_files+=("$jsonl_output")
  hash_files+=("$jsonl_output")
fi

if [[ "$export_parquet" == 1 ]]; then
  echo
  echo "Exporting Parquet: $parquet_output"
  "${parser[@]}" export-parquet --db "$db_abs" --output "$parquet_output" --batch-size "$batch_size"
  manifest_files+=("$parquet_output")
  hash_files+=("$parquet_output")
fi

echo
echo "Exporting manifest: $manifest_output"
manifest_args=(
  export-manifest
  --db "$db_abs"
  --output "$manifest_output"
  --dataset-name "$dataset_name"
  --crawl-command "$crawl_command"
  --max-attempts "$max_attempts"
)
for file in "${manifest_files[@]}"; do
  manifest_args+=(--file "$file")
done
"${parser[@]}" "${manifest_args[@]}"
hash_files+=("$manifest_output")

echo
echo "Writing hashes: $sha256_output"
sha256_tmp="$out_dir_abs/.SHA256SUMS.tmp"
(
  cd "$out_dir_abs"
  sha256sum "${hash_files[@]##*/}" > "$sha256_tmp"
)
mv "$sha256_tmp" "$sha256_output"

echo
echo "Export complete:"
for file in "${hash_files[@]}" "$sha256_output"; do
  printf '  %s\n' "$file"
done
