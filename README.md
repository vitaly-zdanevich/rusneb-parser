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

Each year gets a separate SQLite search checkpoint. Records already saved from earlier broad crawls are skipped by ID.

rusneb.ru can report more than 9,990 results for one query while returning zero records after page 666. For such years, add sorted overflow shards. They discover the same year through a different ordering, which exposes records hidden behind that search window. SQLite still de-duplicates item IDs, so already saved records are not fetched again:

```sh
cargo run -- crawl --catalog 25 --access open \
  --publishyear-prev 1 --publishyear-next 2026 --shard-years \
  --overflow-year 1911 --overflow-year 1912 \
  --overflow-sort document_titlesort:desc \
  --workers 8
```

Equivalent rusneb.ru overflow filter for 1911:
<https://rusneb.ru/search/?by=document_titlesort&order=desc&q=&c[]=25&access[]=open&publishyear_prev=1911&publishyear_next=1911>

Start at five workers, allow adaptive limiting to drop as low as three, then recover after stable successful fetches:

```sh
cargo run -- crawl --workers 5 --min-workers 3
```

For unreliable network/server periods, tune the automatic transient-error pause threshold:

```sh
cargo run -- crawl --workers 10 --max-consecutive-transport-errors 30 --transient-error-pause-secs 120
```

Use a fixed worker count without adaptive limiting:

```sh
cargo run -- crawl --workers 3 --fixed-workers
```

Export all completed records:

```sh
cargo run -- export-jsonl --output out/rusneb.jsonl.gz
cargo run -- export-jsonl --output out/rusneb.jsonl.xz
cargo run -- export-parquet --output out/rusneb.parquet
```

Print checkpoint state:

```sh
cargo run -- stats
```

Validate that completed search pages cover the result totals reported by rusneb.ru:

```sh
cargo run -- validate-coverage
cargo run -- validate-coverage --catalog 25 --access open --require-year
```

This is an offline SQLite check. It flags unfinished search shards and shards where rusneb reported more rows than the crawler discovered, including likely pagination-window cases around 9,990 results.

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

The GitHub Actions workflow runs tests on pushes and pull requests. Pushing any git tag builds release archives for Linux, Windows, macOS, and Android, then publishes them to GitHub Releases. The Android artifact is a raw `aarch64-linux-android` command-line binary, not an APK.

```sh
git tag v0.4.0
git push origin v0.4.0
```

## Resume Model

The default state file is `state/rusneb.sqlite`. On crawl startup, any `in_progress` search page or item is reset to `pending`, so Ctrl-C or a power loss resumes from the last committed checkpoint instead of starting over.

Each saved record and its item status are committed in one transaction. Search pages and item IDs are de-duplicated by primary keys.

## Why SQLite Instead of Plain Text

SQLite is used for crawler state, not as the final data format. The final dataset can still be exported as JSON Lines or Parquet.

A plain text file works well for append-only output, but it is a poor fit for a durable crawl queue:

- crash recovery needs to know which search pages and item cards are `pending`, `in_progress`, `done`, or `failed`;
- item IDs are discovered repeatedly across search pages, so the crawler needs cheap de-duplication;
- a saved record and its queue status must be committed together, otherwise a power loss can create mismatches;
- retry counters, error messages, timestamps, and search-page checkpoints need to be updated in place;
- after restart, stale `in_progress` rows can be safely moved back to `pending`.

SQLite gives these properties with one local file and no server process. Plain text is still used at the export step, where append-only records are the right shape.
