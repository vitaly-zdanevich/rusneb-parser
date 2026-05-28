mod db;
mod export;
mod model;
mod rusneb;

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use db::Db;
use rusneb::{RusnebClient, SearchParams};
use std::io::BufRead;
use std::path::PathBuf;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

#[derive(Debug, Parser)]
#[command(version, about = "Single-threaded NЭБ/rusneb.ru metadata crawler")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Discover catalog IDs from rusneb.ru search pages and fetch metadata.
    Crawl(CrawlArgs),
    /// Add explicit catalog IDs to the durable crawl queue.
    EnqueueIds(EnqueueIdsArgs),
    /// Export records stored in SQLite to JSON Lines.
    ExportJsonl(ExportJsonlArgs),
    /// Export a flattened Parquet file plus the full JSON record column.
    ExportParquet(ExportParquetArgs),
    /// Print durable crawl state counts.
    Stats(StatsArgs),
}

#[derive(Debug, Args)]
struct CommonArgs {
    /// SQLite state DB. This is the durable resume/checkpoint file.
    #[arg(long, default_value = "state/rusneb.sqlite")]
    db: PathBuf,
}

#[derive(Debug, Args)]
struct CrawlArgs {
    #[command(flatten)]
    common: CommonArgs,

    /// rusneb.ru base URL.
    #[arg(long, default_value = "https://rusneb.ru")]
    base_url: String,

    /// Search query. Empty query crawls the selected catalog/filter.
    #[arg(long, default_value = "")]
    query: String,

    /// Catalog filter value, repeatable. Example: --catalog 25 for "Книги".
    #[arg(long = "catalog")]
    catalogs: Vec<String>,

    /// Access filter, repeatable. Example: --access open.
    #[arg(long = "access")]
    access: Vec<String>,

    /// Year lower bound, maps to NЭБ's publishyear_prev parameter.
    #[arg(long)]
    publishyear_prev: Option<String>,

    /// Year upper bound, maps to NЭБ's publishyear_next parameter.
    #[arg(long)]
    publishyear_next: Option<String>,

    /// Search sort field. Example: document_publishyearsort.
    #[arg(long)]
    sort_by: Option<String>,

    /// Search sort order: asc or desc.
    #[arg(long)]
    order: Option<String>,

    /// Extra search parameter as key=value. Repeatable.
    #[arg(long = "extra-param")]
    extra_params: Vec<String>,

    /// First search result page to seed.
    #[arg(long, default_value_t = 1)]
    start_page: u64,

    /// Maximum number of search pages to discover in this run.
    #[arg(long)]
    max_pages: Option<u64>,

    /// Maximum number of item records to fetch in this run.
    #[arg(long)]
    max_items: Option<u64>,

    /// Do not discover search pages; only process queued or explicit IDs.
    #[arg(long)]
    no_discover: bool,

    /// Explicit catalog ID to queue before crawling. Repeatable.
    #[arg(long = "id")]
    ids: Vec<String>,

    /// File with one catalog ID per line to queue before crawling.
    #[arg(long)]
    ids_file: Option<PathBuf>,

    /// Delay between HTTP requests. Applies to every request, including MARC and viewer API calls.
    #[arg(long, default_value_t = 0)]
    delay_ms: u64,

    /// Per-request timeout.
    #[arg(long, default_value_t = 45)]
    timeout_secs: u64,

    /// Maximum attempts per search page or item before it is left failed.
    #[arg(long, default_value_t = 5)]
    max_attempts: u32,

    /// User-Agent sent to rusneb.ru.
    #[arg(
        long,
        default_value = "rusneb-parser/0.1 single-threaded metadata crawler"
    )]
    user_agent: String,

    /// Export gzip JSONL from the SQLite records table before exit.
    #[arg(long)]
    export_jsonl: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct EnqueueIdsArgs {
    #[command(flatten)]
    common: CommonArgs,

    /// Explicit catalog ID. Repeatable.
    #[arg(long = "id")]
    ids: Vec<String>,

    /// File with one catalog ID per line.
    #[arg(long)]
    ids_file: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct ExportJsonlArgs {
    #[command(flatten)]
    common: CommonArgs,

    /// Output path. Use .jsonl.gz, .jsonl.xz, or plain .jsonl.
    #[arg(long)]
    output: PathBuf,
}

#[derive(Debug, Args)]
struct ExportParquetArgs {
    #[command(flatten)]
    common: CommonArgs,

    /// Output .parquet path.
    #[arg(long)]
    output: PathBuf,

    /// Rows per Arrow/Parquet batch.
    #[arg(long, default_value_t = 2048)]
    batch_size: usize,
}

#[derive(Debug, Args)]
struct StatsArgs {
    #[command(flatten)]
    common: CommonArgs,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Crawl(args) => crawl(args),
        Command::EnqueueIds(args) => enqueue_ids(args),
        Command::ExportJsonl(args) => {
            let db = Db::open(&args.common.db)?;
            let count = export::export_jsonl(&db, &args.output)?;
            eprintln!("exported {count} records to {}", args.output.display());
            Ok(())
        }
        Command::ExportParquet(args) => {
            let db = Db::open(&args.common.db)?;
            let count = export::export_parquet(&db, &args.output, args.batch_size)?;
            eprintln!("exported {count} records to {}", args.output.display());
            Ok(())
        }
        Command::Stats(args) => print_stats(args),
    }
}

fn crawl(args: CrawlArgs) -> Result<()> {
    let shutdown = install_shutdown_handler()?;
    let mut db = Db::open(&args.common.db)?;
    let mut client = RusnebClient::new(
        &args.base_url,
        &args.user_agent,
        Duration::from_millis(args.delay_ms),
        Duration::from_secs(args.timeout_secs),
    )?;

    let explicit_ids = load_ids(&args.ids, args.ids_file.as_ref())?;
    if !explicit_ids.is_empty() {
        let inserted = db.enqueue_items(None, None, &explicit_ids)?;
        eprintln!(
            "queued {} explicit IDs ({} new)",
            explicit_ids.len(),
            inserted
        );
    }

    let search_params = SearchParams {
        base_url: args.base_url.clone(),
        query: args.query.clone(),
        catalogs: args.catalogs.clone(),
        access: args.access.clone(),
        publishyear_prev: args.publishyear_prev.clone(),
        publishyear_next: args.publishyear_next.clone(),
        sort_by: args.sort_by.clone(),
        order: args.order.clone(),
        extra: parse_extra_params(&args.extra_params)?,
    };
    let search_key = search_params.key_json()?;
    db.set_meta("search_params", &search_key)?;
    let search_key = stable_search_key(&search_key);
    let params_json = search_params.key_json()?;

    let last_search_page = args
        .max_pages
        .map(|n| args.start_page.saturating_add(n).saturating_sub(1));
    if !args.no_discover {
        db.seed_search_page(&search_key, &params_json, args.start_page)?;
    }

    let mut fetched_items = 0u64;
    loop {
        if shutdown.load(Ordering::SeqCst) {
            eprintln!("shutdown requested; stopping after current checkpoint");
            break;
        }

        let item_limit_reached = args.max_items.is_some_and(|max| fetched_items >= max);
        if !item_limit_reached {
            if let Some(item) = db.next_item(args.max_attempts)? {
                db.mark_item_started(&item.id)?;
                match client.fetch_record(&item.id) {
                    Ok(record) => {
                        let fetched_at = record.fetched_at_unix;
                        let json = serde_json::to_string(&record)?;
                        db.save_record(&item.id, &json, fetched_at)?;
                        fetched_items += 1;
                        eprintln!("saved {}", item.id);
                    }
                    Err(error) => {
                        db.fail_item(&item.id, &error.message, error.status)?;
                        eprintln!("failed {}: {}", item.id, error.message);
                    }
                }
                continue;
            }
        } else if args.no_discover {
            eprintln!("max item limit reached");
            break;
        }

        if args.no_discover {
            break;
        }

        let Some(page) = db.next_search_page(&search_key, args.max_attempts)? else {
            break;
        };
        if last_search_page.is_some_and(|last| page.page > last) {
            break;
        }

        db.mark_search_page_started(&page.search_key, page.page)?;
        match client.fetch_search_page(&search_params, page.page) {
            Ok(result) => {
                let inserted =
                    db.enqueue_items(Some(&page.search_key), Some(page.page), &result.ids)?;
                let next_page = if result.ids.is_empty() {
                    None
                } else {
                    let candidate = page.page + 1;
                    if last_search_page.is_some_and(|last| candidate > last) {
                        None
                    } else {
                        Some(candidate)
                    }
                };
                db.complete_search_page(
                    &page.search_key,
                    page.page,
                    result.ids.len(),
                    result.total_results,
                    &params_json,
                    next_page,
                )?;
                eprintln!(
                    "search page {}: {} IDs ({} new), total={:?}",
                    page.page,
                    result.ids.len(),
                    inserted,
                    result.total_results
                );
            }
            Err(error) => {
                db.fail_search_page(&page.search_key, page.page, &format!("{error:#}"))?;
                eprintln!("failed search page {}: {error:#}", page.page);
            }
        }
    }

    if let Some(output) = args.export_jsonl {
        let count = export::export_jsonl(&db, &output)?;
        eprintln!("exported {count} records to {}", output.display());
    }

    Ok(())
}

fn enqueue_ids(args: EnqueueIdsArgs) -> Result<()> {
    let mut db = Db::open(&args.common.db)?;
    let ids = load_ids(&args.ids, args.ids_file.as_ref())?;
    let inserted = db.enqueue_items(None, None, &ids)?;
    eprintln!("queued {} IDs ({} new)", ids.len(), inserted);
    Ok(())
}

fn print_stats(args: StatsArgs) -> Result<()> {
    let db = Db::open(&args.common.db)?;
    println!("records: {}", db.count_records()?);
    println!("items:");
    for (status, count) in db.status_counts("items")? {
        println!("  {status}: {count}");
    }
    println!("search_pages:");
    for (status, count) in db.status_counts("search_pages")? {
        println!("  {status}: {count}");
    }
    Ok(())
}

fn install_shutdown_handler() -> Result<Arc<AtomicBool>> {
    let shutdown = Arc::new(AtomicBool::new(false));
    let handler_flag = Arc::clone(&shutdown);
    ctrlc::set_handler(move || {
        handler_flag.store(true, Ordering::SeqCst);
    })
    .context("installing Ctrl-C handler")?;
    Ok(shutdown)
}

fn load_ids(ids: &[String], ids_file: Option<&PathBuf>) -> Result<Vec<String>> {
    let mut out = Vec::new();
    out.extend(
        ids.iter()
            .map(|id| id.trim().to_string())
            .filter(|id| !id.is_empty()),
    );

    if let Some(path) = ids_file {
        let file =
            std::fs::File::open(path).with_context(|| format!("opening {}", path.display()))?;
        let reader = std::io::BufReader::new(file);
        for line in reader.lines() {
            let line = line?;
            let id = line.trim();
            if !id.is_empty() && !id.starts_with('#') {
                out.push(id.to_string());
            }
        }
    }

    out.sort();
    out.dedup();
    Ok(out)
}

fn parse_extra_params(values: &[String]) -> Result<Vec<(String, String)>> {
    values
        .iter()
        .map(|value| {
            let Some((key, value)) = value.split_once('=') else {
                anyhow::bail!("extra parameter must be key=value: {value}");
            };
            Ok((key.to_string(), value.to_string()))
        })
        .collect()
}

fn stable_search_key(params_json: &str) -> String {
    let digest = fnv1a64(params_json.as_bytes());
    format!("{digest:016x}")
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}
