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
    atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering},
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

    /// Minimum active workers when adaptive worker limiting slows down after transient errors.
    #[arg(long, default_value = "1")]
    min_workers: NonZeroUsize,

    /// Disable adaptive worker limiting; all --workers stay active until shutdown.
    #[arg(long)]
    fixed_workers: bool,

    /// Consecutive transient errors before adaptive limiting lowers active workers by one.
    #[arg(long, default_value_t = 3)]
    adaptive_decrease_after: u64,

    /// Successful item fetches before adaptive limiting raises active workers by one.
    #[arg(long, default_value_t = 50)]
    adaptive_increase_after: u64,

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

    /// Pause after this many consecutive transient card/search errors. Default is max(20, workers * 4). Use 0 to disable.
    #[arg(long)]
    max_consecutive_transport_errors: Option<u64>,

    /// Pause duration after too many consecutive transient errors.
    #[arg(long, default_value_t = 60)]
    transient_error_pause_secs: u64,

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

#[derive(Debug)]
struct WorkerControl {
    enabled: bool,
    min_workers: usize,
    max_workers: usize,
    decrease_after: u64,
    increase_after: u64,
    active_workers: AtomicUsize,
    stable_successes: AtomicU64,
}

impl WorkerControl {
    fn new(
        enabled: bool,
        min_workers: usize,
        max_workers: usize,
        decrease_after: u64,
        increase_after: u64,
    ) -> Self {
        Self {
            enabled,
            min_workers,
            max_workers,
            decrease_after,
            increase_after,
            active_workers: AtomicUsize::new(max_workers),
            stable_successes: AtomicU64::new(0),
        }
    }

    fn should_worker_run(&self, worker_id: usize) -> bool {
        !self.enabled || worker_id <= self.active_workers.load(Ordering::SeqCst)
    }

    fn on_success(&self, worker_id: usize) {
        if !self.enabled {
            return;
        }

        let successes = self.stable_successes.fetch_add(1, Ordering::SeqCst) + 1;
        if successes < self.increase_after {
            return;
        }
        if self
            .stable_successes
            .compare_exchange(successes, 0, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }

        let mut current = self.active_workers.load(Ordering::SeqCst);
        loop {
            if current >= self.max_workers {
                return;
            }
            let next = current + 1;
            match self.active_workers.compare_exchange(
                current,
                next,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    eprintln!(
                        "worker {worker_id}: adaptive workers increased to {next}/{} after {successes} successful item fetches",
                        self.max_workers
                    );
                    return;
                }
                Err(actual) => current = actual,
            }
        }
    }

    fn on_transient_error(&self, source: &str, consecutive_errors: u64) {
        if !self.enabled {
            return;
        }

        self.on_item_failure();
        if consecutive_errors == 0 || consecutive_errors % self.decrease_after != 0 {
            return;
        }

        let mut current = self.active_workers.load(Ordering::SeqCst);
        loop {
            if current <= self.min_workers {
                return;
            }
            let next = current - 1;
            match self.active_workers.compare_exchange(
                current,
                next,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    eprintln!(
                        "{source}: adaptive workers decreased to {next}/{} after {consecutive_errors} consecutive transient errors",
                        self.max_workers
                    );
                    return;
                }
                Err(actual) => current = actual,
            }
        }
    }

    fn on_item_failure(&self) {
        if self.enabled {
            self.stable_successes.store(0, Ordering::SeqCst);
        }
    }
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

    let discovery_done = Arc::new(AtomicBool::new(args.no_discover));
    let started_items = Arc::new(AtomicU64::new(0));
    let consecutive_transient_errors = Arc::new(AtomicU64::new(0));
    let transient_pause_until = Arc::new(AtomicI64::new(0));
    let workers = args.workers.get();
    let min_workers = args.min_workers.get().min(workers);
    let adaptive_enabled = !args.fixed_workers && min_workers < workers;
    let worker_control = Arc::new(WorkerControl::new(
        adaptive_enabled,
        min_workers,
        workers,
        args.adaptive_decrease_after.max(1),
        args.adaptive_increase_after.max(1),
    ));
    let transient_error_pause_threshold = args
        .max_consecutive_transport_errors
        .or_else(|| Some((workers as u64).saturating_mul(4).max(20)))
        .filter(|limit| *limit > 0);
    let transient_error_pause = Duration::from_secs(args.transient_error_pause_secs);

    eprintln!(
        "starting {} item worker{}{}",
        workers,
        if workers == 1 { "" } else { "s" },
        if adaptive_enabled {
            format!(" (adaptive active range {min_workers}..={workers})")
        } else {
            " (fixed active count)".to_string()
        }
    );

    let mut worker_handles = Vec::with_capacity(workers);

    for worker_index in 0..workers {
        let db_path = args.common.db.clone();
        let base_url = args.base_url.clone();
        let user_agent = args.user_agent.clone();
        let shutdown = Arc::clone(&shutdown);
        let discovery_done = Arc::clone(&discovery_done);
        let started_items = Arc::clone(&started_items);
        let consecutive_transient_errors = Arc::clone(&consecutive_transient_errors);
        let transient_pause_until = Arc::clone(&transient_pause_until);
        let worker_control = Arc::clone(&worker_control);
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
                transient_error_pause_threshold,
                transient_error_pause,
                shutdown,
                discovery_done,
                started_items,
                consecutive_transient_errors,
                transient_pause_until,
                worker_control,
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
                &consecutive_transient_errors,
                &transient_pause_until,
                &worker_control,
                transient_error_pause_threshold,
                transient_error_pause,
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
    consecutive_transient_errors: &AtomicU64,
    transient_pause_until: &AtomicI64,
    worker_control: &WorkerControl,
    transient_error_pause_threshold: Option<u64>,
    transient_error_pause: Duration,
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
        if wait_for_transient_pause(shutdown, transient_pause_until) {
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
                consecutive_transient_errors.store(0, Ordering::SeqCst);
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
                if is_transient_failure(error.status) {
                    db.defer_search_page_after_transient_error(
                        &page.search_key,
                        page.page,
                        &error.message,
                    )?;
                    let consecutive =
                        consecutive_transient_errors.fetch_add(1, Ordering::SeqCst) + 1;
                    eprintln!(
                        "deferred search page {} after transient error: {}",
                        page.page, error.message
                    );
                    worker_control.on_transient_error("search discovery", consecutive);
                    maybe_pause_after_transient_errors(
                        "search discovery",
                        consecutive,
                        transient_error_pause_threshold,
                        transient_error_pause,
                        consecutive_transient_errors,
                        transient_pause_until,
                    );
                } else {
                    db.fail_search_page(&page.search_key, page.page, &error.message)?;
                    eprintln!("failed search page {}: {}", page.page, error.message);
                }
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
    transient_error_pause_threshold: Option<u64>,
    transient_error_pause: Duration,
    shutdown: Arc<AtomicBool>,
    discovery_done: Arc<AtomicBool>,
    started_items: Arc<AtomicU64>,
    consecutive_transient_errors: Arc<AtomicU64>,
    transient_pause_until: Arc<AtomicI64>,
    worker_control: Arc<WorkerControl>,
) -> Result<WorkerStats> {
    let mut db = Db::open(&db_path)?;
    let mut client = RusnebClient::new(&base_url, &user_agent, delay, timeout)?;
    let mut stats = WorkerStats::default();

    loop {
        if shutdown.load(Ordering::SeqCst) {
            break;
        }
        if wait_for_transient_pause(&shutdown, &transient_pause_until) {
            break;
        }

        if !worker_control.should_worker_run(worker_id) {
            if discovery_done.load(Ordering::SeqCst) && db.count_item_backlog(max_attempts)? == 0 {
                break;
            }
            thread::sleep(Duration::from_millis(500));
            continue;
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
                consecutive_transient_errors.store(0, Ordering::SeqCst);
                let title = log_record_title(&record);
                let fetched_at = record.fetched_at_unix;
                let json = serde_json::to_string(&record)?;
                db.save_record(&item.id, &json, fetched_at)?;
                stats.saved += 1;
                eprintln!("worker {worker_id}: saved {} | {title}", item.id);
                worker_control.on_success(worker_id);
            }
            Err(error) => {
                if is_transient_failure(error.status) {
                    db.defer_item_after_transient_error(&item.id, &error.message, error.status)?;
                    stats.deferred += 1;
                    let consecutive =
                        consecutive_transient_errors.fetch_add(1, Ordering::SeqCst) + 1;
                    eprintln!(
                        "worker {worker_id}: deferred {} after transient error: {}",
                        item.id, error.message
                    );
                    let source = format!("worker {worker_id}");
                    worker_control.on_transient_error(&source, consecutive);
                    maybe_pause_after_transient_errors(
                        &source,
                        consecutive,
                        transient_error_pause_threshold,
                        transient_error_pause,
                        &consecutive_transient_errors,
                        &transient_pause_until,
                    );
                } else {
                    db.fail_item(&item.id, &error.message, error.status)?;
                    stats.failed += 1;
                    worker_control.on_item_failure();
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

fn log_record_title(record: &model::RusnebRecord) -> String {
    record
        .metadata
        .title
        .as_deref()
        .map(|title| title.split_whitespace().collect::<Vec<_>>().join(" "))
        .filter(|title| !title.is_empty())
        .unwrap_or_else(|| "<no title>".to_string())
}

fn is_transient_failure(status: Option<u16>) -> bool {
    match status {
        None => true,
        Some(status) => (500..600).contains(&status),
    }
}

fn wait_for_transient_pause(shutdown: &AtomicBool, pause_until: &AtomicI64) -> bool {
    loop {
        if shutdown.load(Ordering::SeqCst) {
            return true;
        }

        let remaining = pause_until.load(Ordering::SeqCst) - db::now_unix();
        if remaining <= 0 {
            return false;
        }

        thread::sleep(Duration::from_secs(remaining.min(5) as u64));
    }
}

fn maybe_pause_after_transient_errors(
    source: &str,
    consecutive_errors: u64,
    threshold: Option<u64>,
    pause: Duration,
    consecutive_transient_errors: &AtomicU64,
    pause_until: &AtomicI64,
) {
    let Some(threshold) = threshold else {
        return;
    };
    if consecutive_errors < threshold {
        return;
    }

    let pause_secs = pause.as_secs();
    if pause_secs == 0 {
        consecutive_transient_errors.store(0, Ordering::SeqCst);
        return;
    }

    let until = db::now_unix().saturating_add(pause_secs as i64);
    let mut current = pause_until.load(Ordering::SeqCst);
    while current < until {
        match pause_until.compare_exchange(current, until, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => {
                consecutive_transient_errors.store(0, Ordering::SeqCst);
                eprintln!(
                    "{source}: pausing for {pause_secs}s after {consecutive_errors} consecutive transient errors"
                );
                return;
            }
            Err(actual) => current = actual,
        }
    }

    consecutive_transient_errors.store(0, Ordering::SeqCst);
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
