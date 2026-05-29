mod db;
mod export;
mod model;
mod rusneb;

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use db::Db;
use rusneb::{RusnebClient, SearchParams};
use std::io::BufRead;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::thread;
use std::time::Duration;

#[derive(Debug, Parser)]
#[command(version, about = "NЭБ/rusneb.ru metadata crawler")]
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

    /// Parallel item fetch workers. Search-page discovery remains single-threaded.
    #[arg(long, default_value = "1")]
    workers: NonZeroUsize,

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

    /// Stop after this many consecutive card-page transport errors. Default is max(10, workers * 2). Use 0 to disable.
    #[arg(long)]
    max_consecutive_transport_errors: Option<u64>,

    /// User-Agent sent to rusneb.ru.
    #[arg(long, default_value = "rusneb-parser/0.1 metadata crawler")]
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

#[derive(Debug, Default)]
struct WorkerStats {
    saved: u64,
    failed: u64,
    deferred: u64,
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
    db.reset_interrupted_work()?;

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
    if !args.no_discover {
        db.seed_search_page(&search_key, &params_json, args.start_page)?;
    }

    eprintln!(
        "starting {} item worker{}",
        args.workers.get(),
        if args.workers.get() == 1 { "" } else { "s" }
    );

    let discovery_done = Arc::new(AtomicBool::new(args.no_discover));
    let started_items = Arc::new(AtomicU64::new(0));
    let consecutive_transport_errors = Arc::new(AtomicU64::new(0));
    let workers = args.workers.get();
    let transport_error_limit = args
        .max_consecutive_transport_errors
        .or_else(|| Some((workers as u64).saturating_mul(2).max(10)))
        .filter(|limit| *limit > 0);
    let mut worker_handles = Vec::with_capacity(workers);

    for worker_index in 0..workers {
        let db_path = args.common.db.clone();
        let base_url = args.base_url.clone();
        let user_agent = args.user_agent.clone();
        let shutdown = Arc::clone(&shutdown);
        let discovery_done = Arc::clone(&discovery_done);
        let started_items = Arc::clone(&started_items);
        let consecutive_transport_errors = Arc::clone(&consecutive_transport_errors);
        let delay = Duration::from_millis(args.delay_ms);
        let timeout = Duration::from_secs(args.timeout_secs);
        let max_attempts = args.max_attempts;
        let max_items = args.max_items;

        worker_handles.push(thread::spawn(move || {
            let shutdown_on_error = Arc::clone(&shutdown);
            let result = item_worker(
                worker_index + 1,
                db_path,
                base_url,
                user_agent,
                delay,
                timeout,
                max_attempts,
                max_items,
                transport_error_limit,
                shutdown,
                discovery_done,
                started_items,
                consecutive_transport_errors,
            );
            if result.is_err() {
                shutdown_on_error.store(true, Ordering::SeqCst);
            }
            result
        }));
    }

    let discovery_result = if args.no_discover {
        Ok(())
    } else {
        match RusnebClient::new(
            &args.base_url,
            &args.user_agent,
            Duration::from_millis(args.delay_ms),
            Duration::from_secs(args.timeout_secs),
        ) {
            Ok(mut client) => run_search_discovery(
                &args,
                &mut db,
                &mut client,
                &search_params,
                &search_key,
                &params_json,
                &shutdown,
            ),
            Err(error) => Err(error),
        }
    };

    if discovery_result.is_err() {
        shutdown.store(true, Ordering::SeqCst);
    }
    discovery_done.store(true, Ordering::SeqCst);

    let mut worker_stats = WorkerStats::default();
    let mut worker_errors = Vec::new();
    for handle in worker_handles {
        match handle.join() {
            Ok(Ok(stats)) => {
                worker_stats.saved += stats.saved;
                worker_stats.failed += stats.failed;
                worker_stats.deferred += stats.deferred;
            }
            Ok(Err(error)) => worker_errors.push(error),
            Err(error) => worker_errors.push(anyhow::anyhow!("worker thread panicked: {error:?}")),
        }
    }

    discovery_result?;
    if !worker_errors.is_empty() {
        let mut message = format!("{} item worker(s) failed", worker_errors.len());
        for error in worker_errors {
            message.push_str(&format!("\n- {error:#}"));
        }
        anyhow::bail!("{message}");
    }

    eprintln!(
        "item workers stopped: saved={}, failed={}, deferred={}",
        worker_stats.saved, worker_stats.failed, worker_stats.deferred
    );

    if let Some(output) = args.export_jsonl {
        let count = export::export_jsonl(&db, &output)?;
        eprintln!("exported {count} records to {}", output.display());
    }

    Ok(())
}

fn run_search_discovery(
    args: &CrawlArgs,
    db: &mut Db,
    client: &mut RusnebClient,
    search_params: &SearchParams,
    search_key: &str,
    params_json: &str,
    shutdown: &AtomicBool,
) -> Result<()> {
    let last_search_page = args
        .max_pages
        .map(|n| args.start_page.saturating_add(n).saturating_sub(1));
    let backlog_target = (args.workers.get() as u64).saturating_mul(3).max(15);

    loop {
        if shutdown.load(Ordering::SeqCst) {
            eprintln!("shutdown requested; stopping discovery after current checkpoint");
            break;
        }

        if args.max_items.is_none() && db.count_item_backlog(args.max_attempts)? >= backlog_target {
            thread::sleep(Duration::from_millis(250));
            continue;
        }

        let Some(page) = db.next_search_page(search_key, args.max_attempts)? else {
            break;
        };
        if last_search_page.is_some_and(|last| page.page > last) {
            break;
        }

        db.mark_search_page_started(&page.search_key, page.page)?;
        match client.fetch_search_page(search_params, page.page) {
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
                    params_json,
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

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn item_worker(
    worker_id: usize,
    db_path: PathBuf,
    base_url: String,
    user_agent: String,
    delay: Duration,
    timeout: Duration,
    max_attempts: u32,
    max_items: Option<u64>,
    transport_error_limit: Option<u64>,
    shutdown: Arc<AtomicBool>,
    discovery_done: Arc<AtomicBool>,
    started_items: Arc<AtomicU64>,
    consecutive_transport_errors: Arc<AtomicU64>,
) -> Result<WorkerStats> {
    let mut db = Db::open(&db_path)?;
    let mut client = RusnebClient::new(&base_url, &user_agent, delay, timeout)?;
    let mut stats = WorkerStats::default();

    loop {
        if shutdown.load(Ordering::SeqCst) {
            break;
        }

        let reserved_slot = max_items.is_some();
        if !reserve_item_slot(max_items, &started_items) {
            break;
        }

        let Some(item) = db.claim_next_item(max_attempts).with_context(|| {
            format!(
                "worker {worker_id} claiming next item from {}",
                db_path.display()
            )
        })?
        else {
            if reserved_slot {
                started_items.fetch_sub(1, Ordering::SeqCst);
            }
            if discovery_done.load(Ordering::SeqCst) || shutdown.load(Ordering::SeqCst) {
                break;
            }
            thread::sleep(Duration::from_millis(250));
            continue;
        };

        match client.fetch_record(&item.id) {
            Ok(record) => {
                consecutive_transport_errors.store(0, Ordering::SeqCst);
                let fetched_at = record.fetched_at_unix;
                let json = serde_json::to_string(&record)?;
                db.save_record(&item.id, &json, fetched_at)?;
                stats.saved += 1;
                eprintln!("worker {worker_id}: saved {}", item.id);
            }
            Err(error) => {
                if error.status.is_none() {
                    db.defer_item_after_transport_error(&item.id, &error.message)?;
                    stats.deferred += 1;
                    let consecutive =
                        consecutive_transport_errors.fetch_add(1, Ordering::SeqCst) + 1;
                    eprintln!(
                        "worker {worker_id}: deferred {} after transport error: {}",
                        item.id, error.message
                    );
                    if transport_error_limit.is_some_and(|limit| consecutive >= limit) {
                        eprintln!(
                            "worker {worker_id}: stopping after {consecutive} consecutive transport errors"
                        );
                        shutdown.store(true, Ordering::SeqCst);
                    }
                } else {
                    db.fail_item(&item.id, &error.message, error.status)?;
                    stats.failed += 1;
                    eprintln!("worker {worker_id}: failed {}: {}", item.id, error.message);
                }
            }
        }
    }

    Ok(stats)
}

fn reserve_item_slot(max_items: Option<u64>, started_items: &AtomicU64) -> bool {
    let Some(max_items) = max_items else {
        return true;
    };

    let mut current = started_items.load(Ordering::SeqCst);
    loop {
        if current >= max_items {
            return false;
        }

        match started_items.compare_exchange(
            current,
            current + 1,
            Ordering::SeqCst,
            Ordering::SeqCst,
        ) {
            Ok(_) => return true,
            Err(actual) => current = actual,
        }
    }
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
