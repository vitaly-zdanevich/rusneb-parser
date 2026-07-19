# rusneb-parser

[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=vitaly-zdanevich_rusneb-parser&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=vitaly-zdanevich_rusneb-parser)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=vitaly-zdanevich_rusneb-parser&metric=coverage)](https://sonarcloud.io/summary/new_code?id=vitaly-zdanevich_rusneb-parser)
[![Bugs](https://sonarcloud.io/api/project_badges/measure?project=vitaly-zdanevich_rusneb-parser&metric=bugs)](https://sonarcloud.io/summary/new_code?id=vitaly-zdanevich_rusneb-parser)
[![Vulnerabilities](https://sonarcloud.io/api/project_badges/measure?project=vitaly-zdanevich_rusneb-parser&metric=vulnerabilities)](https://sonarcloud.io/summary/new_code?id=vitaly-zdanevich_rusneb-parser)
[![Code Smells](https://sonarcloud.io/api/project_badges/measure?project=vitaly-zdanevich_rusneb-parser&metric=code_smells)](https://sonarcloud.io/summary/new_code?id=vitaly-zdanevich_rusneb-parser)
[![Duplicated Lines](https://sonarcloud.io/api/project_badges/measure?project=vitaly-zdanevich_rusneb-parser&metric=duplicated_lines_density)](https://sonarcloud.io/summary/new_code?id=vitaly-zdanevich_rusneb-parser)
[![Maintainability](https://sonarcloud.io/api/project_badges/measure?project=vitaly-zdanevich_rusneb-parser&metric=sqale_rating)](https://sonarcloud.io/summary/new_code?id=vitaly-zdanevich_rusneb-parser)
[![Reliability](https://sonarcloud.io/api/project_badges/measure?project=vitaly-zdanevich_rusneb-parser&metric=reliability_rating)](https://sonarcloud.io/summary/new_code?id=vitaly-zdanevich_rusneb-parser)
[![Security](https://sonarcloud.io/api/project_badges/measure?project=vitaly-zdanevich_rusneb-parser&metric=security_rating)](https://sonarcloud.io/summary/new_code?id=vitaly-zdanevich_rusneb-parser)
[![Lines of Code](https://sonarcloud.io/api/project_badges/measure?project=vitaly-zdanevich_rusneb-parser&metric=ncloc)](https://sonarcloud.io/summary/new_code?id=vitaly-zdanevich_rusneb-parser)
[![Technical Debt](https://sonarcloud.io/api/project_badges/measure?project=vitaly-zdanevich_rusneb-parser&metric=sqale_index)](https://sonarcloud.io/summary/new_code?id=vitaly-zdanevich_rusneb-parser)

Metadata crawler for [rusneb.ru](https://rusneb.ru/).

The crawler is deliberately conservative by default: it uses one item fetch worker, a configurable delay between requests, and a SQLite state file for crash-safe resume. The item worker maximum can be increased with `--workers`; search-page discovery stays single-threaded and keeps only a small item backlog ahead of the workers. Adaptive worker limiting is enabled by default for multi-worker crawls: transient server errors and timeout bursts lower the active worker count, and sustained successful fetches raise it again. If card/search transient errors happen repeatedly, affected rows are put back to `pending` without spending retry attempts, the crawler pauses, and then retries. Final data can be exported as JSON Lines (`.jsonl`, `.jsonl.gz`, `.jsonl.xz`) and, with the default feature set, Parquet.

## What It Collects

For each catalog ID it stores:

- metadata parsed from the public card page, including title, authors, description, bibliographic description, and every field in `Детальная информация`;
- MARC21 XML from `local/components/exalead/search.page.detail/ajax/marcExport.php?book_id=...`, parsed into fields while also preserving raw XML;
- PDF links from card buttons and MARC `856$u`;
- selected viewer/access API metadata from `/rest_api/viewer/access/`, with ephemeral access tokens removed.

It does not download PDF files.

## Examples

Fetch one known record:

```sh
cargo run -- crawl --no-discover --id 000199_000009_015267348 --max-items 1
```

Discover one search-result page for books, without fetching items:

```sh
cargo run -- crawl --catalog 25 --max-pages 1 --max-items 0
```

Continue from the same checkpoint and fetch queued items:

```sh
cargo run -- crawl --no-discover --max-items 100 --delay-ms 1500
```

Run with up to ten parallel item workers:

```sh
cargo run -- crawl --workers 10
```

Route all rusneb HTTP requests through an SSH dynamic SOCKS tunnel:

```sh
cargo run -- crawl --ssh ubuntu@151.145.94.114 --workers 8
```

This starts `ssh -N -D 127.0.0.1:<local-port> ...` and configures the HTTP client with a `socks5h://` proxy, so rusneb connections and DNS resolution go through the SSH host. If the SSH tunnel cannot start, the crawl exits before making rusneb requests instead of falling back to the local IP.

Split a broad search into one resumable search stream per publication year:

```sh
cargo run -- crawl --catalog 25 --access open \
  --publishyear-prev 1800 --publishyear-next 2026 --shard-years \
  --workers 8 --max-consecutive-transport-errors 16 --transient-error-pause-secs 120
```

Equivalent rusneb.ru search filter:
<https://rusneb.ru/search/?q=&c[]=25&access[]=open&publishyear_prev=1800&publishyear_next=2026>

Each year gets a separate SQLite search checkpoint. Records already saved from earlier broad crawls are skipped by ID. Year-sharded crawls also seed a date-ascending no-year prefix shard by default, because rusneb.ru's date sort shows records without a publication year before dated records. The no-year prefix runs after dated years and stops after five consecutive non-empty pages add no new IDs, which keeps it from repeatedly walking known dated records. Use `--skip-no-year-shard` to disable it, `--no-year-max-pages` to change its page cap, or `--no-year-stop-after-known-pages 0` to disable the duplicate-page stop.

If a year shard ends with rusneb.ru still reporting more rows than the crawler discovered, the crawler automatically seeds sorted overflow shards and keeps going.

rusneb.ru can report more than 9,990 results for one query while returning zero records after page 666; it can also report small count gaps after a normal-looking result stream. Automatic overflow shards first discover the same year through different orderings, which exposes records hidden behind those gaps. By default the crawler tries title, author, and publication-date sorts in both supported directions. If sorted shards still leave a gap, the crawler loads the first-party advanced-search facet values from [rusneb.ru/search/extended/](https://rusneb.ru/search/extended/) and seeds facet shards for `lang` and `idlibrary` by default. SQLite still de-duplicates item IDs, so already saved records are not fetched again. Add `--overflow-sort field:asc|desc` to replace the default sort list, `--overflow-facet field` to replace the default facet list, or `--no-auto-overflow` to disable automatic overflow behavior:

```sh
cargo run -- crawl --catalog 25 --access open \
  --publishyear-prev 1 --publishyear-next 2026 --shard-years \
  --overflow-sort document_titlesort:desc --overflow-sort document_authorsort:asc \
  --overflow-facet lang --overflow-facet idlibrary \
  --workers 8
```

Known years can still be forced manually with `--overflow-year 1911 --overflow-year 1912`.

Equivalent rusneb.ru overflow filter for 1911:
<https://rusneb.ru/search/?by=document_titlesort&order=desc&q=&c[]=25&access[]=open&publishyear_prev=1911&publishyear_next=1911>

Start at five workers, allow adaptive limiting to drop as low as three, then recover after stable successful fetches:

```sh
cargo run -- crawl --workers 5 --min-workers 3
```

Resume the long books crawl with the checked-in operational wrapper:

```sh
./continue.sh
./continue.sh --workers 8 --ssh ubuntu@151.145.94.114
```

`continue.sh` uses strict Bash mode, refuses to create a missing SQLite database unless `--init-db` is passed, writes a timestamped log under `run-logs/`, and creates a database-specific lock directory next to the SQLite file. It starts a background runner, records the runner PID in `run-logs/crawl.pid`, and records the actual crawler child PID inside the lock directory. After the crawler exits, the runner appends final `stats` and `validate-coverage --catalog 25 --access open --require-year` output to the same log. Use `--no-validate` to disable that final check, or `--validate-top` to control how many suspicious coverage rows are printed. If a stale lock remains after a crash, the next run removes it; use `--force` only when you have checked that no crawler is still using the same database.

For unreliable network/server periods, tune the automatic transient-error pause threshold:

```sh
cargo run -- crawl --workers 10 --max-consecutive-transport-errors 30 --transient-error-pause-secs 120
```

Repeated `HTTP 403` responses are treated separately from server `5xx` errors. After several consecutive card/search `403` responses, the crawler assumes rusneb.ru may be temporarily blocking the client, puts the triggering row back to `pending` without spending an attempt, and pauses all workers:

```sh
cargo run -- crawl --max-consecutive-403-errors 8 --http-403-pause-secs 600
```

Use `--max-consecutive-403-errors 0` only when you want every `403` to count as a normal failed row.

Use a fixed worker count without adaptive limiting:

```sh
cargo run -- crawl --workers 3 --fixed-workers
```

Export all completed records:

```sh
./export.sh
cargo run -- export-jsonl --output out/rusneb.jsonl.gz
cargo run -- export-jsonl --output out/rusneb.jsonl.xz
cargo run -- export-parquet --output out/rusneb.parquet
```

`export.sh` writes `out/rusneb.jsonl.xz`, `out/rusneb.parquet`, `out/manifest.json`, and `out/SHA256SUMS` by default. Use `--no-parquet`, `--no-jsonl`, `--prefix`, `--out-dir`, or `--crawl-command` to adjust the export.

Export a companion manifest for sharing dataset archives:

```sh
cargo run -- export-manifest \
  --output out/manifest.json \
  --crawl-command './continue.sh' \
  --file out/rusneb.jsonl.xz \
  --file out/rusneb.parquet
```

The manifest records the parser version, git revision when available, SQLite crawl counts, failed item diagnostics, inferred state start/finish timestamps, and SHA-256 hashes for every `--file`.

Print checkpoint state:

```sh
cargo run -- stats
```

Validate that completed search pages cover the result totals reported by rusneb.ru:

```sh
cargo run -- validate-coverage
cargo run -- validate-coverage --catalog 25 --access open --require-year
```

This is an offline SQLite check. It reports individual shard gaps and also groups shards by the same base search with sort/order removed. The grouped check uses the durable `search_items` membership table to count the union of IDs discovered by overlapping overflow shards, so a year can validate successfully even when one sorted shard is individually window-limited. For databases created before `search_items`, validation falls back to the largest completed shard count when exact page membership is unavailable.

Automatic facet overflow shards are grouped with their base query by removing `lang` and `idlibrary` from the validation grouping key. If you used custom `--overflow-facet` fields for the crawl, pass the same fields to `validate-coverage`.

If a temporary rusneb.ru block leaves failed `HTTP 403` rows, wait until the site is reachable again, then reset only those rows to `pending` and rerun the same crawl command:

```sh
cargo run -- retry-failed --http-status 403
```

Browse saved records in SQLite:

```sh
sqlite3 -header -column state/rusneb.sqlite \
  "SELECT id, title, year, catalog, pdf_count FROM records_flat LIMIT 20;"
```

## GitHub Releases

The GitHub Actions workflow runs tests on pushes and pull requests. The Sonar workflow generates `coverage/lcov.info`, uploads it as a GitHub Actions artifact, and passes it to SonarCloud. Pushing any git tag builds release archives for Linux, Windows, macOS, and Android, then publishes them to GitHub Releases. The Android artifact is a raw `aarch64-linux-android` command-line binary, not an APK.

```sh
git tag v0.4.0
git push origin v0.4.0
```

## Resume Model

The default state file is `state/rusneb.sqlite`. On crawl startup, any `in_progress` search page or item is reset to `pending`, so Ctrl-C or a power loss resumes from the last committed checkpoint instead of starting over.

Each saved record and its item status are committed in one transaction. Search pages and item IDs are de-duplicated by primary keys.

If an item card returns `HTTP 404`, the item is stored with terminal status `missing`. Missing items stay visible in `stats`, are not retried on resume, and do not make an otherwise complete crawl fail.

## Why SQLite Instead of Plain Text

SQLite is used for crawler state, not as the final data format. The final dataset can still be exported as JSON Lines or Parquet.

A plain text file works well for append-only output, but it is a poor fit for a durable crawl queue:

- crash recovery needs to know which search pages and item cards are `pending`, `in_progress`, `done`, `missing`, or `failed`;
- item IDs are discovered repeatedly across search pages, so the crawler needs cheap de-duplication;
- a saved record and its queue status must be committed together, otherwise a power loss can create mismatches;
- retry counters, error messages, timestamps, and search-page checkpoints need to be updated in place;
- after restart, stale `in_progress` rows can be safely moved back to `pending`.

SQLite gives these properties with one local file and no server process. Plain text is still used at the export step, where append-only records are the right shape.
