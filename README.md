# rusneb-parser

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

Browse saved records in SQLite:

```sh
sqlite3 -header -column state/rusneb.sqlite \
  "SELECT id, title, year, catalog, pdf_count FROM records_flat LIMIT 20;"
```

## GitHub Releases

The GitHub Actions workflow runs tests on pushes and pull requests. Pushing any git tag builds release archives for Linux, Windows, macOS, and Android, then publishes them to GitHub Releases. The Android artifact is a raw `aarch64-linux-android` command-line binary, not an APK.

```sh
git tag v0.1.8
git push origin v0.1.8
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
