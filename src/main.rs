mod db;
mod export;
mod manifest;
mod model;
mod rusneb;

use anyhow::{Context, Result};
use clap::{ArgAction, Args, Parser, Subcommand};
use db::{CompletedSearchPage, Db};
use rusneb::{RusnebClient, SearchParams};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::io::BufRead;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::process::{Child, Command as ProcessCommand, Stdio};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering},
};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Debug, Parser)]
#[command(version, about = "NЭБ/rusneb.ru metadata crawler")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
#[allow(clippy::large_enum_variant)]
enum Command {
    /// Discover catalog IDs from rusneb.ru search pages and fetch metadata.
    Crawl(CrawlArgs),
    /// Add explicit catalog IDs to the durable crawl queue.
    EnqueueIds(EnqueueIdsArgs),
    /// Reset failed rows to pending so the next crawl retries them.
    RetryFailed(RetryFailedArgs),
    /// Export records stored in SQLite to JSON Lines.
    ExportJsonl(ExportJsonlArgs),
    /// Export a flattened Parquet file plus the full JSON record column.
    ExportParquet(ExportParquetArgs),
    /// Export a JSON manifest describing the crawl state and output files.
    ExportManifest(ExportManifestArgs),
    /// Print durable crawl state counts.
    Stats(StatsArgs),
    /// Validate whether completed search pages cover rusneb-reported result totals.
    ValidateCoverage(ValidateCoverageArgs),
    /// Print one completion report with crawl state, coverage, and retry hints.
    Report(ReportArgs),
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

    /// Split discovery into one search stream per year in the inclusive publishyear range.
    #[arg(long)]
    shard_years: bool,

    /// Do not add the date-ascending shard that captures records without publication year.
    #[arg(long = "skip-no-year-shard", action = ArgAction::SetFalse, default_value_t = true)]
    no_year_shard: bool,

    /// Maximum search pages to discover for the no-year prefix shard.
    #[arg(long, default_value_t = 666)]
    no_year_max_pages: u64,

    /// Stop the no-year prefix shard after this many consecutive non-empty pages add no new IDs.
    #[arg(long, default_value_t = 5)]
    no_year_stop_after_known_pages: u64,

    /// Add extra sorted search streams for this year. Repeatable; use when one year exceeds rusneb's search window.
    #[arg(long = "overflow-year")]
    overflow_years: Vec<u32>,

    /// Sort shard for each --overflow-year, formatted as field:asc or field:desc. Repeatable.
    /// Without --overflow-year, the same sort list is used for automatic overflow shards.
    #[arg(long = "overflow-sort")]
    overflow_sorts: Vec<String>,

    /// Disable automatic sorted shards for year shards that hit rusneb's search result window.
    #[arg(long = "no-auto-overflow", action = ArgAction::SetFalse, default_value_t = true)]
    auto_overflow: bool,

    /// Disable automatic advanced-search facet shards after sorted overflow still leaves a gap.
    #[arg(long = "no-auto-overflow-facets", action = ArgAction::SetFalse, default_value_t = true)]
    auto_overflow_facets: bool,

    /// Advanced-search facet field for automatic overflow sharding. Repeatable.
    ///
    /// Without explicit values, the crawler uses lang and idlibrary.
    #[arg(long = "overflow-facet")]
    overflow_facets: Vec<String>,

    /// Maximum number of automatic facet filters combined in one overflow shard.
    #[arg(long, default_value_t = 2)]
    max_overflow_facet_depth: usize,

    /// Rows that indicate rusneb's search result window limit in diagnostics.
    #[arg(long, default_value_t = 9990)]
    search_window_limit_results: u64,

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

    /// Consecutive HTTP 403 card/search errors before treating rusneb as temporarily blocking this client. Default is max(4, workers). Use 0 to disable.
    #[arg(long)]
    max_consecutive_403_errors: Option<u64>,

    /// Pause duration after too many consecutive HTTP 403 errors.
    #[arg(long, default_value_t = 600)]
    http_403_pause_secs: u64,

    /// User-Agent sent to rusneb.ru.
    #[arg(long, default_value = "rusneb-parser/0.1 metadata crawler")]
    user_agent: String,

    /// Route all rusneb HTTP requests through this SSH dynamic SOCKS tunnel target.
    #[arg(long)]
    ssh: Option<String>,

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
struct RetryFailedArgs {
    #[command(flatten)]
    common: CommonArgs,

    /// Only retry failures with this HTTP status. Example: --http-status 403.
    #[arg(long)]
    http_status: Option<u16>,
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
struct ExportManifestArgs {
    #[command(flatten)]
    common: CommonArgs,

    /// Output manifest JSON path.
    #[arg(long, default_value = "out/manifest.json")]
    output: PathBuf,

    /// Human-readable dataset name stored in the manifest.
    #[arg(long, default_value = "rusneb metadata")]
    dataset_name: String,

    /// Crawl command to record in the manifest. Pass the real command or script used for the dataset.
    #[arg(long)]
    crawl_command: Option<String>,

    /// Output file to include with size and SHA-256 hash. Repeatable.
    #[arg(long = "file")]
    files: Vec<PathBuf>,

    /// Maximum attempts used to classify retryable and exhausted failures in the summary.
    #[arg(long, default_value_t = 5)]
    max_attempts: u32,

    /// Maximum failed item IDs to include as a diagnostic sample.
    #[arg(long, default_value_t = 50)]
    failed_item_sample: u64,
}

#[derive(Debug, Args)]
struct StatsArgs {
    #[command(flatten)]
    common: CommonArgs,
}

#[derive(Debug, Args)]
struct ValidateCoverageArgs {
    #[command(flatten)]
    common: CommonArgs,

    /// Only validate shards whose catalog filter contains this value. Repeatable.
    #[arg(long = "catalog")]
    catalogs: Vec<String>,

    /// Only validate shards whose access filter contains this value. Repeatable.
    #[arg(long = "access")]
    access: Vec<String>,

    /// Only validate shards with a publication-year filter.
    #[arg(long)]
    require_year: bool,

    /// Rows that indicate rusneb's search result window limit when a shard still has a gap.
    #[arg(long, default_value_t = 9990)]
    window_limit_results: u64,

    /// Maximum suspicious shards to print.
    #[arg(long, default_value_t = 50)]
    top: usize,

    /// Print every shard, including shards without coverage gaps.
    #[arg(long)]
    show_all: bool,

    /// Advanced-search facet fields generated by automatic overflow sharding.
    ///
    /// These fields are ignored when grouping overlapping shards for full-coverage validation.
    #[arg(long = "overflow-facet")]
    overflow_facets: Vec<String>,
}

#[derive(Debug, Args)]
struct ReportArgs {
    #[command(flatten)]
    coverage: ValidateCoverageArgs,

    /// Maximum attempts used to classify retryable and exhausted failures.
    #[arg(long, default_value_t = 5)]
    max_attempts: u32,

    /// Maximum failed item IDs to print as diagnostics.
    #[arg(long, default_value_t = 20)]
    failed_item_sample: u64,
}

#[derive(Debug, Clone)]
struct SearchJob {
    label: String,
    params: SearchParams,
    search_key: String,
    params_json: String,
    max_pages: Option<u64>,
    stop_after_known_pages: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SearchSort {
    field: String,
    order: String,
}

#[derive(Debug)]
struct CoverageGroup {
    key: String,
    label: String,
    search_keys: Vec<String>,
    shard_count: usize,
    unfinished_shards: usize,
    unique_item_ids: u64,
    reported_total_results: Option<u64>,
}

#[derive(Debug)]
struct CoverageValidationSummary {
    shards: usize,
    groups: usize,
    unfinished_shards: usize,
    gap_shards: usize,
    window_limited_shards: usize,
    per_query_missing_results: u64,
    unfinished_groups: usize,
    gap_groups: usize,
    grouped_missing_results: u64,
    display_shards_total: usize,
    display_groups_total: usize,
    display_shard_lines: Vec<String>,
    display_group_lines: Vec<String>,
}

impl CoverageValidationSummary {
    fn is_ok(&self) -> bool {
        self.unfinished_groups == 0 && self.gap_groups == 0
    }
}

impl CoverageGroup {
    fn missing_results(&self) -> Option<u64> {
        self.reported_total_results
            .map(|total| total.saturating_sub(self.unique_item_ids))
    }

    fn has_coverage_gap(&self) -> bool {
        self.missing_results().is_some_and(|missing| missing > 0)
    }
}

struct AutoOverflowContext<'a> {
    args: &'a CrawlArgs,
    db: &'a Db,
    client: &'a mut RusnebClient,
    overflow_sorts: &'a [SearchSort],
    overflow_facets: &'a [String],
    overflow_facet_values: &'a mut Option<BTreeMap<String, Vec<String>>>,
    queued_search_keys: &'a mut BTreeSet<String>,
}

#[derive(Debug)]
struct SshTunnel {
    child: Child,
    proxy_url: String,
    target: String,
}

impl SshTunnel {
    fn start(target: &str) -> Result<Self> {
        let port = reserve_local_port()?;
        let bind = format!("127.0.0.1:{port}");
        let mut child = ProcessCommand::new("ssh")
            .args([
                "-N",
                "-D",
                &bind,
                "-o",
                "ExitOnForwardFailure=yes",
                "-o",
                "BatchMode=yes",
                "-o",
                "ServerAliveInterval=30",
                "-o",
                "ServerAliveCountMax=3",
                target,
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .with_context(|| format!("starting SSH SOCKS tunnel via {target}"))?;

        let addr: SocketAddr = bind.parse().expect("valid local SSH bind address");
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if TcpStream::connect_timeout(&addr, Duration::from_millis(200)).is_ok() {
                if let Some(status) = child
                    .try_wait()
                    .with_context(|| format!("checking SSH tunnel process for {target}"))?
                {
                    anyhow::bail!("SSH tunnel via {target} exited before it was ready: {status}");
                }
                break;
            }
            if let Some(status) = child
                .try_wait()
                .with_context(|| format!("checking SSH tunnel process for {target}"))?
            {
                anyhow::bail!("SSH tunnel via {target} exited before it was ready: {status}");
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                anyhow::bail!("timed out waiting for SSH tunnel via {target} on {bind}");
            }
            thread::sleep(Duration::from_millis(100));
        }

        let proxy_url = format!("socks5h://{bind}");
        eprintln!("SSH tunnel ready: {target} -> {proxy_url}");
        Ok(Self {
            child,
            proxy_url,
            target: target.to_string(),
        })
    }
}

impl Drop for SshTunnel {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
        eprintln!("SSH tunnel closed: {}", self.target);
    }
}

fn reserve_local_port() -> Result<u16> {
    let listener = TcpListener::bind(("127.0.0.1", 0)).context("reserving local SSH SOCKS port")?;
    Ok(listener
        .local_addr()
        .context("reading local SSH SOCKS port")?
        .port())
}

#[derive(Debug, Default)]
struct WorkerStats {
    saved: u64,
    missing: u64,
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
        if consecutive_errors == 0
            || self.decrease_after == 0
            || !consecutive_errors.is_multiple_of(self.decrease_after)
        {
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
        Command::RetryFailed(args) => retry_failed(args),
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
        Command::ExportManifest(args) => {
            let db = Db::open(&args.common.db)?;
            manifest::export_manifest(&db, &args)?;
            eprintln!("exported manifest to {}", args.output.display());
            Ok(())
        }
        Command::Stats(args) => print_stats(args),
        Command::ValidateCoverage(args) => validate_coverage(args),
        Command::Report(args) => report(args),
    }
}

fn build_search_jobs(args: &CrawlArgs) -> Result<Vec<SearchJob>> {
    let extra = parse_extra_params(&args.extra_params)?;
    let overflow_sorts = effective_overflow_sorts(args)?;
    let base_params = SearchParams {
        base_url: args.base_url.clone(),
        query: args.query.clone(),
        catalogs: args.catalogs.clone(),
        access: args.access.clone(),
        publishyear_prev: args.publishyear_prev.clone(),
        publishyear_next: args.publishyear_next.clone(),
        sort_by: args.sort_by.clone(),
        order: args.order.clone(),
        extra,
    };

    if !args.shard_years {
        if !args.overflow_years.is_empty() || !args.overflow_sorts.is_empty() {
            anyhow::bail!("--overflow-year and --overflow-sort require --shard-years");
        }
        return Ok(vec![make_search_job("default".to_string(), base_params)?]);
    }
    if args.overflow_years.is_empty() && !args.auto_overflow && !args.overflow_sorts.is_empty() {
        anyhow::bail!("--overflow-sort without --overflow-year requires automatic overflow");
    }
    if args.no_year_shard && args.no_year_max_pages == 0 {
        anyhow::bail!("--no-year-max-pages must be greater than zero");
    }

    let from_year = parse_year_bound("--publishyear-prev", args.publishyear_prev.as_deref())?;
    let to_year = parse_year_bound("--publishyear-next", args.publishyear_next.as_deref())?;
    if from_year > to_year {
        anyhow::bail!(
            "--publishyear-prev must be less than or equal to --publishyear-next when --shard-years is used"
        );
    }

    let overflow_years = args.overflow_years.iter().copied().collect::<BTreeSet<_>>();
    for year in &overflow_years {
        if *year < from_year || *year > to_year {
            anyhow::bail!(
                "--overflow-year {year} is outside --publishyear-prev..--publishyear-next"
            );
        }
    }

    let mut jobs = Vec::with_capacity(
        (to_year - from_year + 1) as usize
            + overflow_years.len().saturating_mul(overflow_sorts.len()),
    );
    for year in from_year..=to_year {
        let mut params = base_params.clone();
        let year_string = year.to_string();
        params.publishyear_prev = Some(year_string.clone());
        params.publishyear_next = Some(year_string.clone());
        jobs.push(make_search_job(
            format!("year {year_string}"),
            params.clone(),
        )?);

        if overflow_years.contains(&year) {
            for sort in &overflow_sorts {
                let mut sorted_params = params.clone();
                sorted_params.sort_by = Some(sort.field.clone());
                sorted_params.order = Some(sort.order.clone());
                jobs.push(make_search_job(
                    format!("year {year_string} sort {}:{}", sort.field, sort.order),
                    sorted_params,
                )?);
            }
        }
    }
    if args.no_year_shard {
        let mut params = base_params.clone();
        params.publishyear_prev = None;
        params.publishyear_next = None;
        params.sort_by = Some("document_publishyearsort".to_string());
        params.order = Some("asc".to_string());
        jobs.push(make_limited_search_job(
            "no-year prefix sort document_publishyearsort:asc".to_string(),
            params,
            Some(args.no_year_max_pages),
            (args.no_year_stop_after_known_pages > 0)
                .then_some(args.no_year_stop_after_known_pages),
        )?);
    }
    Ok(jobs)
}

fn effective_overflow_sorts(args: &CrawlArgs) -> Result<Vec<SearchSort>> {
    let sorts = parse_overflow_sorts(&args.overflow_sorts)?;
    if !sorts.is_empty()
        || !args.shard_years
        || (!args.auto_overflow && args.overflow_years.is_empty())
    {
        return Ok(sorts);
    }
    Ok(default_overflow_sorts())
}

fn default_overflow_sorts() -> Vec<SearchSort> {
    vec![
        SearchSort {
            field: "document_titlesort".to_string(),
            order: "asc".to_string(),
        },
        SearchSort {
            field: "document_titlesort".to_string(),
            order: "desc".to_string(),
        },
        SearchSort {
            field: "document_authorsort".to_string(),
            order: "asc".to_string(),
        },
        SearchSort {
            field: "document_authorsort".to_string(),
            order: "desc".to_string(),
        },
        SearchSort {
            field: "document_publishyearsort".to_string(),
            order: "asc".to_string(),
        },
        SearchSort {
            field: "document_publishyearsort".to_string(),
            order: "desc".to_string(),
        },
    ]
}

fn effective_overflow_facets(args: &CrawlArgs) -> Vec<String> {
    if !args.auto_overflow
        || !args.auto_overflow_facets
        || !args.shard_years
        || args.max_overflow_facet_depth == 0
    {
        return Vec::new();
    }
    let fields = if args.overflow_facets.is_empty() {
        default_overflow_facets()
    } else {
        args.overflow_facets.clone()
    };
    dedup_nonempty_strings(fields)
}

fn default_overflow_facets() -> Vec<String> {
    vec!["lang".to_string(), "idlibrary".to_string()]
}

fn dedup_nonempty_strings(values: Vec<String>) -> Vec<String> {
    let mut out = Vec::new();
    let mut seen = BTreeSet::new();
    for value in values {
        let value = value.trim().to_string();
        if !value.is_empty() && seen.insert(value.clone()) {
            out.push(value);
        }
    }
    out
}

fn make_search_job(label: String, params: SearchParams) -> Result<SearchJob> {
    make_limited_search_job(label, params, None, None)
}

fn make_limited_search_job(
    label: String,
    params: SearchParams,
    max_pages: Option<u64>,
    stop_after_known_pages: Option<u64>,
) -> Result<SearchJob> {
    let params_json = params.key_json()?;
    let search_key = stable_search_key(&params_json);
    Ok(SearchJob {
        label,
        params,
        search_key,
        params_json,
        max_pages,
        stop_after_known_pages,
    })
}

fn write_search_jobs_meta(db: &Db, search_jobs: &[SearchJob]) -> Result<()> {
    let search_jobs_json = if search_jobs.len() == 1 {
        search_jobs[0].params_json.clone()
    } else {
        serde_json::to_string(
            &search_jobs
                .iter()
                .map(|job| &job.params_json)
                .collect::<Vec<_>>(),
        )?
    };
    db.set_meta("search_params", &search_jobs_json)
}

/// Add newly discovered search shards to both the in-memory queue and durable metadata.
fn queue_search_jobs(
    db: &Db,
    search_jobs: &mut Vec<SearchJob>,
    pending_jobs: &mut VecDeque<SearchJob>,
    new_jobs: Vec<SearchJob>,
) -> Result<()> {
    if new_jobs.is_empty() {
        return Ok(());
    }

    for new_job in &new_jobs {
        pending_jobs.push_back(new_job.clone());
    }
    search_jobs.extend(new_jobs);
    write_search_jobs_meta(db, search_jobs)
}

/// Scan all known shards after the discovery queue drains and seed any new overflow work.
fn seed_auto_overflow_jobs_for_known_gaps(
    ctx: &mut AutoOverflowContext<'_>,
    search_jobs: &[SearchJob],
) -> Result<Vec<SearchJob>> {
    let mut new_jobs = Vec::new();
    for job in search_jobs {
        new_jobs.extend(seed_auto_overflow_jobs(ctx, job)?);
    }
    Ok(new_jobs)
}

fn seed_auto_overflow_jobs(
    ctx: &mut AutoOverflowContext<'_>,
    source_job: &SearchJob,
) -> Result<Vec<SearchJob>> {
    let args = ctx.args;
    if !args.auto_overflow || !args.shard_years {
        return Ok(Vec::new());
    }
    let Some(year) = exact_year(&source_job.params).map(str::to_string) else {
        return Ok(Vec::new());
    };

    let Some(shard) = ctx.db.coverage_shard(&source_job.search_key)? else {
        return Ok(Vec::new());
    };
    if shard.has_unfinished_pages()
        || !shard.ended_on_empty_done_page()
        || !shard.has_coverage_gap()
    {
        return Ok(Vec::new());
    }
    let group = coverage_group_for_params(ctx.db, &source_job.params_json, ctx.overflow_facets)?;
    if group
        .as_ref()
        .is_some_and(|group| group.unfinished_shards > 0 || !group.has_coverage_gap())
    {
        return Ok(Vec::new());
    }

    let mut new_jobs = Vec::new();
    let gap_kind = if shard.looks_window_limited(args.search_window_limit_results) {
        "window-limited"
    } else {
        "coverage-gap"
    };
    for sort in ctx.overflow_sorts {
        if source_job.params.sort_by.as_deref() == Some(sort.field.as_str())
            && source_job.params.order.as_deref() == Some(sort.order.as_str())
        {
            continue;
        }

        let mut params = source_job.params.clone();
        params.sort_by = Some(sort.field.clone());
        params.order = Some(sort.order.clone());
        let job = make_search_job(
            format!(
                "year {year} auto overflow sort {}:{}",
                sort.field, sort.order
            ),
            params,
        )?;

        if ctx.queued_search_keys.insert(job.search_key.clone()) {
            let inserted =
                ctx.db
                    .seed_search_page(&job.search_key, &job.params_json, args.start_page)?;
            eprintln!(
                "auto overflow shard {} ({}) {} after {gap_kind} shard {} discovered {} of {:?} reported results",
                job.label,
                job.search_key,
                if inserted { "seeded" } else { "queued" },
                source_job.search_key,
                shard.discovered_results,
                shard.reported_total_results
            );
            new_jobs.push(job);
        }
    }
    if !new_jobs.is_empty() {
        return Ok(new_jobs);
    }

    if ctx.overflow_facets.is_empty() {
        return Ok(new_jobs);
    }
    if overflow_facet_depth(&source_job.params, ctx.overflow_facets)
        >= args.max_overflow_facet_depth
    {
        return Ok(new_jobs);
    }

    if ctx.overflow_facet_values.is_none() {
        let values = ctx
            .client
            .fetch_advanced_filter_values(ctx.overflow_facets)
            .map_err(|error| {
                anyhow::anyhow!(
                    "fetching advanced-search facet values for overflow sharding failed: {}",
                    error.message
                )
            })?;
        let value_count = values.values().map(Vec::len).sum::<usize>();
        eprintln!(
            "loaded {value_count} advanced-search overflow facet value(s) for {}",
            ctx.overflow_facets.join(",")
        );
        *ctx.overflow_facet_values = Some(values);
    }

    let facet_values = ctx
        .overflow_facet_values
        .as_ref()
        .expect("overflow facet values were just loaded");
    let mut base_params = source_job.params.clone();
    base_params.sort_by = None;
    base_params.order = None;

    for field in ctx.overflow_facets {
        if extra_param_value(&base_params, field).is_some() {
            continue;
        }
        let Some(values) = facet_values.get(field) else {
            continue;
        };
        for value in values {
            let mut params = base_params.clone();
            set_extra_param(&mut params, field, value);
            let job = make_search_job(facet_overflow_label(&year, &params, field, value), params)?;

            if ctx.queued_search_keys.insert(job.search_key.clone()) {
                let inserted =
                    ctx.db
                        .seed_search_page(&job.search_key, &job.params_json, args.start_page)?;
                eprintln!(
                    "auto overflow shard {} ({}) {} after {gap_kind} shard {} discovered {} of {:?} reported results",
                    job.label,
                    job.search_key,
                    if inserted { "seeded" } else { "queued" },
                    source_job.search_key,
                    shard.discovered_results,
                    shard.reported_total_results
                );
                new_jobs.push(job);
            }
        }
    }

    Ok(new_jobs)
}

fn overflow_facet_depth(params: &SearchParams, overflow_facets: &[String]) -> usize {
    params
        .extra
        .iter()
        .filter(|(key, _)| overflow_facets.iter().any(|field| field == key))
        .count()
}

fn extra_param_value<'a>(params: &'a SearchParams, key: &str) -> Option<&'a str> {
    params
        .extra
        .iter()
        .find_map(|(extra_key, value)| (extra_key == key).then_some(value.as_str()))
}

fn set_extra_param(params: &mut SearchParams, key: &str, value: &str) {
    if let Some((_, existing)) = params
        .extra
        .iter_mut()
        .find(|(extra_key, _)| extra_key == key)
    {
        *existing = value.to_string();
        return;
    }
    params.extra.push((key.to_string(), value.to_string()));
}

fn facet_overflow_label(year: &str, params: &SearchParams, field: &str, value: &str) -> String {
    let existing = params
        .extra
        .iter()
        .filter(|(key, _)| key != field)
        .map(|(key, value)| format!("{key}={}", abbreviate_label_value(value)))
        .collect::<Vec<_>>();
    if existing.is_empty() {
        format!(
            "year {year} auto overflow facet {field}={}",
            abbreviate_label_value(value)
        )
    } else {
        format!(
            "year {year} auto overflow facet {} {field}={}",
            existing.join(" "),
            abbreviate_label_value(value)
        )
    }
}

fn abbreviate_label_value(value: &str) -> String {
    const MAX_CHARS: usize = 80;
    let mut chars = value.chars();
    let truncated = chars.by_ref().take(MAX_CHARS).collect::<String>();
    if chars.next().is_some() {
        format!("{truncated}...")
    } else {
        truncated
    }
}

fn exact_year(params: &SearchParams) -> Option<&str> {
    match (
        params.publishyear_prev.as_deref(),
        params.publishyear_next.as_deref(),
    ) {
        (Some(from), Some(to)) if from == to => Some(from),
        _ => None,
    }
}

fn parse_year_bound(name: &str, value: Option<&str>) -> Result<u32> {
    let Some(value) = value else {
        anyhow::bail!("{name} is required when --shard-years is used");
    };
    value
        .parse::<u32>()
        .with_context(|| format!("{name} must be an unsigned year: {value}"))
}

fn crawl(args: CrawlArgs) -> Result<()> {
    let shutdown = install_shutdown_handler()?;
    let mut db = Db::open(&args.common.db)?;
    db.reset_interrupted_work()?;
    let overflow_facets = effective_overflow_facets(&args);

    let explicit_ids = load_ids(&args.ids, args.ids_file.as_ref())?;
    if !explicit_ids.is_empty() {
        let inserted = db.enqueue_items(None, None, &explicit_ids)?;
        eprintln!(
            "queued {} explicit IDs ({} new)",
            explicit_ids.len(),
            inserted
        );
    }

    let mut search_jobs = build_search_jobs(&args)?;
    let overflow_sorts = effective_overflow_sorts(&args)?;
    write_search_jobs_meta(&db, &search_jobs)?;
    if !args.no_discover {
        for job in &search_jobs {
            db.seed_search_page(&job.search_key, &job.params_json, args.start_page)?;
        }
        eprintln!(
            "seeded {} search shard{}",
            search_jobs.len(),
            if search_jobs.len() == 1 { "" } else { "s" }
        );
    }

    let discovery_done = Arc::new(AtomicBool::new(args.no_discover));
    let started_items = Arc::new(AtomicU64::new(0));
    let consecutive_transient_errors = Arc::new(AtomicU64::new(0));
    let consecutive_403_errors = Arc::new(AtomicU64::new(0));
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
    let http_403_pause_threshold = args
        .max_consecutive_403_errors
        .or_else(|| Some((workers as u64).max(4)))
        .filter(|limit| *limit > 0);
    let http_403_pause = Duration::from_secs(args.http_403_pause_secs);
    let ssh_tunnel = args.ssh.as_deref().map(SshTunnel::start).transpose()?;
    let proxy_url = ssh_tunnel.as_ref().map(|tunnel| tunnel.proxy_url.clone());

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
        let consecutive_403_errors = Arc::clone(&consecutive_403_errors);
        let transient_pause_until = Arc::clone(&transient_pause_until);
        let worker_control = Arc::clone(&worker_control);
        let delay = Duration::from_millis(args.delay_ms);
        let timeout = Duration::from_secs(args.timeout_secs);
        let max_attempts = args.max_attempts;
        let max_items = args.max_items;
        let proxy_url = proxy_url.clone();

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
                proxy_url,
                transient_error_pause_threshold,
                transient_error_pause,
                http_403_pause_threshold,
                http_403_pause,
                shutdown,
                discovery_done,
                started_items,
                consecutive_transient_errors,
                consecutive_403_errors,
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
            proxy_url.as_deref(),
        ) {
            Ok(mut client) => {
                let mut queued_search_keys = search_jobs
                    .iter()
                    .map(|job| job.search_key.clone())
                    .collect::<BTreeSet<_>>();
                let mut pending_jobs = VecDeque::from(search_jobs.clone());
                let mut overflow_facet_values = None;
                let mut result = Ok(());
                loop {
                    while let Some(job) = pending_jobs.pop_front() {
                        if shutdown.load(Ordering::SeqCst) {
                            break;
                        }
                        eprintln!(
                            "discovering search shard {} ({})",
                            job.label, job.search_key
                        );
                        result = run_search_discovery(
                            &args,
                            &mut db,
                            &mut client,
                            &job.params,
                            &job.search_key,
                            &job.params_json,
                            job.max_pages,
                            job.stop_after_known_pages,
                            &shutdown,
                            &consecutive_transient_errors,
                            &consecutive_403_errors,
                            &transient_pause_until,
                            &worker_control,
                            transient_error_pause_threshold,
                            transient_error_pause,
                            http_403_pause_threshold,
                            http_403_pause,
                        );
                        if result.is_err() {
                            break;
                        }
                        let mut auto_overflow_ctx = AutoOverflowContext {
                            args: &args,
                            db: &db,
                            client: &mut client,
                            overflow_sorts: &overflow_sorts,
                            overflow_facets: &overflow_facets,
                            overflow_facet_values: &mut overflow_facet_values,
                            queued_search_keys: &mut queued_search_keys,
                        };
                        match seed_auto_overflow_jobs(&mut auto_overflow_ctx, &job) {
                            Ok(new_jobs) => {
                                if let Err(error) = queue_search_jobs(
                                    &db,
                                    &mut search_jobs,
                                    &mut pending_jobs,
                                    new_jobs,
                                ) {
                                    result = Err(error);
                                    break;
                                }
                            }
                            Err(error) => {
                                result = Err(error);
                                break;
                            }
                        }
                    }

                    if result.is_err() || shutdown.load(Ordering::SeqCst) {
                        break;
                    }

                    let mut auto_overflow_ctx = AutoOverflowContext {
                        args: &args,
                        db: &db,
                        client: &mut client,
                        overflow_sorts: &overflow_sorts,
                        overflow_facets: &overflow_facets,
                        overflow_facet_values: &mut overflow_facet_values,
                        queued_search_keys: &mut queued_search_keys,
                    };
                    match seed_auto_overflow_jobs_for_known_gaps(
                        &mut auto_overflow_ctx,
                        &search_jobs,
                    ) {
                        Ok(new_jobs) => {
                            if new_jobs.is_empty() {
                                break;
                            }
                            eprintln!(
                                "self-healing coverage scan queued {} automatic overflow shard{}",
                                new_jobs.len(),
                                if new_jobs.len() == 1 { "" } else { "s" }
                            );
                            if let Err(error) = queue_search_jobs(
                                &db,
                                &mut search_jobs,
                                &mut pending_jobs,
                                new_jobs,
                            ) {
                                result = Err(error);
                                break;
                            }
                        }
                        Err(error) => {
                            result = Err(error);
                            break;
                        }
                    }
                }
                result
            }
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
                worker_stats.missing += stats.missing;
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
        "item workers stopped: saved={}, missing={}, failed={}, deferred={}",
        worker_stats.saved, worker_stats.missing, worker_stats.failed, worker_stats.deferred
    );

    let completion_error = report_crawl_completion(
        &db.crawl_completion_summary(args.max_attempts)?,
        args.max_attempts,
        args.max_pages.is_some() || args.max_items.is_some(),
        shutdown.load(Ordering::SeqCst),
    );

    if let Some(output) = args.export_jsonl {
        let count = export::export_jsonl(&db, &output)?;
        eprintln!("exported {count} records to {}", output.display());
    }

    if let Some(message) = completion_error {
        anyhow::bail!(message);
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn run_search_discovery(
    args: &CrawlArgs,
    db: &mut Db,
    client: &mut RusnebClient,
    search_params: &SearchParams,
    search_key: &str,
    params_json: &str,
    job_max_pages: Option<u64>,
    stop_after_known_pages: Option<u64>,
    shutdown: &AtomicBool,
    consecutive_transient_errors: &AtomicU64,
    consecutive_403_errors: &AtomicU64,
    transient_pause_until: &AtomicI64,
    worker_control: &WorkerControl,
    transient_error_pause_threshold: Option<u64>,
    transient_error_pause: Duration,
    http_403_pause_threshold: Option<u64>,
    http_403_pause: Duration,
) -> Result<()> {
    let max_pages = min_optional_u64(args.max_pages, job_max_pages);
    let last_search_page = max_pages.map(|n| args.start_page.saturating_add(n).saturating_sub(1));
    let backlog_target = (args.workers.get() as u64).saturating_mul(3).max(15);
    let mut known_page_streak = 0;

    loop {
        if shutdown.load(Ordering::SeqCst) {
            eprintln!("shutdown requested; stopping discovery after current checkpoint");
            break;
        }
        if wait_for_transient_pause(shutdown, transient_pause_until) {
            break;
        }

        if args.max_items.is_none_or(|max_items| max_items > 0)
            && db.count_item_backlog(args.max_attempts)? >= backlog_target
        {
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
                consecutive_403_errors.store(0, Ordering::SeqCst);
                let inserted =
                    db.enqueue_items(Some(&page.search_key), Some(page.page), &result.ids)?;
                let reached_known_page_stop = if result.ids.is_empty() {
                    false
                } else if inserted == 0 {
                    known_page_streak += 1;
                    stop_after_known_pages.is_some_and(|threshold| known_page_streak >= threshold)
                } else {
                    known_page_streak = 0;
                    false
                };
                let next_page = if result.ids.is_empty() || reached_known_page_stop {
                    None
                } else {
                    let candidate = page.page + 1;
                    if last_search_page.is_some_and(|last| candidate > last) {
                        None
                    } else {
                        Some(candidate)
                    }
                };
                db.complete_search_page(CompletedSearchPage {
                    search_key: &page.search_key,
                    page: page.page,
                    ids: &result.ids,
                    total_results: result.total_results,
                    params_json,
                    next_page,
                })?;
                eprintln!(
                    "search page {}: {} IDs ({} new), total={:?}",
                    page.page,
                    result.ids.len(),
                    inserted,
                    result.total_results
                );
                if reached_known_page_stop {
                    eprintln!(
                        "search page {}: stopping shard after {} consecutive non-empty page(s) with no new IDs",
                        page.page, known_page_streak
                    );
                }
            }
            Err(error) => {
                if is_transient_failure(error.status) {
                    consecutive_403_errors.store(0, Ordering::SeqCst);
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
                } else if is_http_403_failure(error.status)
                    && should_defer_after_http_403_burst(
                        "search discovery",
                        consecutive_403_errors,
                        http_403_pause_threshold,
                        http_403_pause,
                        transient_pause_until,
                    )
                {
                    db.defer_search_page_after_transient_error(
                        &page.search_key,
                        page.page,
                        &error.message,
                    )?;
                    eprintln!(
                        "deferred search page {} after likely HTTP 403 block: {}",
                        page.page, error.message
                    );
                } else {
                    if !is_http_403_failure(error.status) {
                        consecutive_403_errors.store(0, Ordering::SeqCst);
                    }
                    db.fail_search_page(&page.search_key, page.page, &error.message)?;
                    eprintln!("failed search page {}: {}", page.page, error.message);
                }
            }
        }
    }

    Ok(())
}

fn min_optional_u64(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
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
    proxy_url: Option<String>,
    transient_error_pause_threshold: Option<u64>,
    transient_error_pause: Duration,
    http_403_pause_threshold: Option<u64>,
    http_403_pause: Duration,
    shutdown: Arc<AtomicBool>,
    discovery_done: Arc<AtomicBool>,
    started_items: Arc<AtomicU64>,
    consecutive_transient_errors: Arc<AtomicU64>,
    consecutive_403_errors: Arc<AtomicU64>,
    transient_pause_until: Arc<AtomicI64>,
    worker_control: Arc<WorkerControl>,
) -> Result<WorkerStats> {
    let mut db = Db::open(&db_path)?;
    let mut client =
        RusnebClient::new(&base_url, &user_agent, delay, timeout, proxy_url.as_deref())?;
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
                consecutive_403_errors.store(0, Ordering::SeqCst);
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
                    consecutive_403_errors.store(0, Ordering::SeqCst);
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
                } else if is_http_403_failure(error.status)
                    && should_defer_after_http_403_burst(
                        &format!("worker {worker_id}"),
                        &consecutive_403_errors,
                        http_403_pause_threshold,
                        http_403_pause,
                        &transient_pause_until,
                    )
                {
                    db.defer_item_after_transient_error(&item.id, &error.message, error.status)?;
                    stats.deferred += 1;
                    eprintln!(
                        "worker {worker_id}: deferred {} after likely HTTP 403 block: {}",
                        item.id, error.message
                    );
                } else if is_missing_item_failure(error.status) {
                    consecutive_403_errors.store(0, Ordering::SeqCst);
                    db.mark_item_missing(&item.id, &error.message, error.status)?;
                    stats.missing += 1;
                    eprintln!("worker {worker_id}: missing {}: {}", item.id, error.message);
                } else {
                    if !is_http_403_failure(error.status) {
                        consecutive_403_errors.store(0, Ordering::SeqCst);
                    }
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

fn is_missing_item_failure(status: Option<u16>) -> bool {
    status == Some(404)
}

fn is_http_403_failure(status: Option<u16>) -> bool {
    status == Some(403)
}

fn is_transient_failure(status: Option<u16>) -> bool {
    match status {
        None => true,
        Some(status) => (500..600).contains(&status),
    }
}

fn should_defer_after_http_403_burst(
    source: &str,
    consecutive_403_errors: &AtomicU64,
    threshold: Option<u64>,
    pause: Duration,
    pause_until: &AtomicI64,
) -> bool {
    let Some(threshold) = threshold else {
        return false;
    };
    let consecutive = consecutive_403_errors.fetch_add(1, Ordering::SeqCst) + 1;
    if consecutive < threshold {
        return false;
    }

    let pause_secs = pause.as_secs();
    if pause_secs == 0 {
        consecutive_403_errors.store(0, Ordering::SeqCst);
        eprintln!(
            "{source}: HTTP 403 block threshold reached after {consecutive} consecutive HTTP 403 errors; retrying affected rows without spending attempts"
        );
        return true;
    }

    let until = db::now_unix().saturating_add(pause_secs as i64);
    let mut current = pause_until.load(Ordering::SeqCst);
    while current < until {
        match pause_until.compare_exchange(current, until, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => {
                consecutive_403_errors.store(0, Ordering::SeqCst);
                eprintln!(
                    "{source}: pausing for {pause_secs}s after {consecutive} consecutive HTTP 403 errors; retrying affected rows without spending attempts"
                );
                return true;
            }
            Err(actual) => current = actual,
        }
    }

    consecutive_403_errors.store(0, Ordering::SeqCst);
    true
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

fn report_crawl_completion(
    summary: &db::CrawlCompletionSummary,
    max_attempts: u32,
    stopped_by_limit: bool,
    shutdown_requested: bool,
) -> Option<String> {
    eprintln!(
        "crawl state: records={}, items(done={}, missing={}, pending={}, in_progress={}, failed={}, retryable_failed={}, exhausted_failed={}), search_pages(done={}, pending={}, in_progress={}, failed={}, retryable_failed={}, exhausted_failed={})",
        summary.records,
        summary.items.done,
        summary.items.missing,
        summary.items.pending,
        summary.items.in_progress,
        summary.items.failed,
        summary.retryable_failed_items,
        summary.exhausted_failed_items,
        summary.search_pages.done,
        summary.search_pages.pending,
        summary.search_pages.in_progress,
        summary.search_pages.failed,
        summary.retryable_failed_search_pages,
        summary.exhausted_failed_search_pages
    );

    if summary.failed_403_items > 0 || summary.failed_403_search_pages > 0 {
        eprintln!(
            "failed HTTP 403 rows: items={}, search_pages={}",
            summary.failed_403_items, summary.failed_403_search_pages
        );
    }

    let exhausted_failures = summary.exhausted_failed_items + summary.exhausted_failed_search_pages;
    if exhausted_failures > 0 {
        let message = format!(
            "crawl incomplete: {} failed item(s) and {} failed search page(s) reached --max-attempts={max_attempts}",
            summary.exhausted_failed_items, summary.exhausted_failed_search_pages
        );
        eprintln!("{message}");
        eprintln!(
            "retry exhausted failures with `rusneb-parser retry-failed --http-status 403` for HTTP 403 rows, or rerun crawl later with a higher --max-attempts"
        );
        return Some(message);
    }

    let unfinished = summary.items.pending
        + summary.items.in_progress
        + summary.retryable_failed_items
        + summary.search_pages.pending
        + summary.search_pages.in_progress
        + summary.retryable_failed_search_pages;
    if unfinished == 0 {
        eprintln!("crawl complete: no pending or failed work remains");
        return None;
    }

    if shutdown_requested {
        eprintln!("crawl interrupted: rerun the same crawl command to resume pending work");
        return None;
    }

    if stopped_by_limit {
        eprintln!("crawl stopped by configured --max-pages or --max-items limit");
        return None;
    }

    let message = format!("crawl incomplete: {unfinished} pending or retryable row(s) remain");
    eprintln!("{message}");
    Some(message)
}

fn enqueue_ids(args: EnqueueIdsArgs) -> Result<()> {
    let mut db = Db::open(&args.common.db)?;
    let ids = load_ids(&args.ids, args.ids_file.as_ref())?;
    let inserted = db.enqueue_items(None, None, &ids)?;
    eprintln!("queued {} IDs ({} new)", ids.len(), inserted);
    Ok(())
}

fn retry_failed(args: RetryFailedArgs) -> Result<()> {
    let db = Db::open(&args.common.db)?;
    let counts = db.retry_failed(args.http_status)?;
    match args.http_status {
        Some(status) => eprintln!(
            "reset failed HTTP {status} rows to pending: {} item(s), {} search page(s)",
            counts.items, counts.search_pages
        ),
        None => eprintln!(
            "reset all failed rows to pending: {} item(s), {} search page(s)",
            counts.items, counts.search_pages
        ),
    }
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

fn validate_coverage(args: ValidateCoverageArgs) -> Result<()> {
    let db = Db::open(&args.common.db)?;
    let summary = coverage_validation_summary(&db, &args)?;
    print_coverage_validation_summary(&summary, args.top);

    if summary.is_ok() {
        println!("coverage ok: completed search groups cover all reported totals");
        return Ok(());
    }

    anyhow::bail!(
        "coverage validation failed: {} unfinished group(s), {} group(s) with coverage gaps",
        summary.unfinished_groups,
        summary.gap_groups
    )
}

fn report(args: ReportArgs) -> Result<()> {
    let db = Db::open(&args.coverage.common.db)?;
    let state = db.crawl_completion_summary(args.max_attempts)?;
    let coverage = coverage_validation_summary(&db, &args.coverage)?;
    let failed_statuses = db.failed_item_http_status_counts()?;
    let failed_sample = db.failed_item_sample(args.failed_item_sample)?;

    println!("completion report:");
    println!("  db: {}", args.coverage.common.db.display());
    println!("  records: {}", state.records);
    print_work_status_summary("items", &state.items);
    println!("    retryable_failed: {}", state.retryable_failed_items);
    println!("    exhausted_failed: {}", state.exhausted_failed_items);
    print_work_status_summary("search_pages", &state.search_pages);
    println!(
        "    retryable_failed: {}",
        state.retryable_failed_search_pages
    );
    println!(
        "    exhausted_failed: {}",
        state.exhausted_failed_search_pages
    );

    println!("coverage:");
    print_coverage_validation_summary(&coverage, args.coverage.top);

    if !failed_statuses.is_empty() {
        println!("failed item HTTP statuses:");
        for count in failed_statuses {
            let status = count
                .http_status
                .map(|status| status.to_string())
                .unwrap_or_else(|| "<none>".to_string());
            println!("  HTTP {status}: {}", count.count);
        }
    }

    if !failed_sample.is_empty() {
        println!("failed item sample:");
        for item in failed_sample {
            let status = item
                .last_http_status
                .map(|status| status.to_string())
                .unwrap_or_else(|| "<none>".to_string());
            let error = item.last_error.unwrap_or_else(|| "<no error>".to_string());
            println!(
                "  {} | attempts={} HTTP={} updated_at={} | {}",
                item.id, item.attempts, status, item.updated_at_unix, error
            );
        }
    }

    let unfinished = unfinished_work(&state);
    let exhausted = state.exhausted_failed_items + state.exhausted_failed_search_pages;
    let complete = unfinished == 0 && exhausted == 0 && coverage.is_ok();
    if complete {
        println!("completion ok: no pending, retryable, exhausted, or coverage-gap work found");
        return Ok(());
    }

    println!("next steps:");
    if state.failed_403_items > 0 || state.failed_403_search_pages > 0 {
        println!(
            "  retry HTTP 403 rows: rusneb-parser retry-failed --db {} --http-status 403",
            args.coverage.common.db.display()
        );
    }
    if exhausted > 0 {
        println!(
            "  retry exhausted failures: rusneb-parser retry-failed --db {}",
            args.coverage.common.db.display()
        );
    }
    if unfinished > 0 {
        println!("  rerun the crawl command to drain pending or retryable work");
    }
    if !coverage.is_ok() {
        println!(
            "  rerun the crawl command so automatic overflow sharding can close coverage gaps"
        );
    }

    anyhow::bail!("completion report found incomplete crawl state")
}

fn print_work_status_summary(label: &str, summary: &db::WorkStatusSummary) {
    println!("{label}:");
    println!("    done: {}", summary.done);
    println!("    missing: {}", summary.missing);
    println!("    pending: {}", summary.pending);
    println!("    in_progress: {}", summary.in_progress);
    println!("    failed: {}", summary.failed);
    if summary.other > 0 {
        println!("    other: {}", summary.other);
    }
}

fn unfinished_work(summary: &db::CrawlCompletionSummary) -> u64 {
    summary.items.pending
        + summary.items.in_progress
        + summary.retryable_failed_items
        + summary.search_pages.pending
        + summary.search_pages.in_progress
        + summary.retryable_failed_search_pages
}

fn coverage_validation_summary(
    db: &Db,
    args: &ValidateCoverageArgs,
) -> Result<CoverageValidationSummary> {
    let overflow_facets = effective_coverage_overflow_facets(args);
    let report = db.coverage_report()?;
    let shards = report
        .shards
        .iter()
        .filter(|shard| coverage_shard_matches_filters(shard, args))
        .collect::<Vec<_>>();
    if shards.is_empty() {
        anyhow::bail!("coverage validation failed: no search pages found");
    }

    let groups = build_coverage_groups(db, &shards, &overflow_facets)?;
    let unfinished_shards = shards
        .iter()
        .filter(|shard| shard.has_unfinished_pages())
        .count();
    let gap_shards = shards
        .iter()
        .filter(|shard| shard.has_coverage_gap())
        .count();
    let window_limited_shards = shards
        .iter()
        .filter(|shard| shard.looks_window_limited(args.window_limit_results))
        .count();
    let per_query_missing_results = shards
        .iter()
        .filter_map(|shard| shard.missing_results())
        .sum::<u64>();
    let unfinished_groups = groups
        .iter()
        .filter(|group| group.unfinished_shards > 0)
        .count();
    let gap_groups = groups
        .iter()
        .filter(|group| group.has_coverage_gap())
        .count();
    let grouped_missing_results = groups
        .iter()
        .filter_map(CoverageGroup::missing_results)
        .sum::<u64>();

    let shard_count = shards.len();
    let group_count = groups.len();

    let mut display_shards = shards
        .into_iter()
        .filter(|shard| args.show_all || shard.has_unfinished_pages() || shard.has_coverage_gap())
        .collect::<Vec<_>>();
    display_shards.sort_by_key(|shard| {
        std::cmp::Reverse((
            shard.missing_results().unwrap_or(0),
            shard.discovered_results,
        ))
    });
    let display_shards_total = display_shards.len();
    let display_shard_lines = display_shards
        .iter()
        .take(args.top)
        .map(|shard| coverage_shard_line(shard, args.window_limit_results))
        .collect::<Vec<_>>();

    let mut display_groups = groups
        .iter()
        .filter(|group| args.show_all || group.unfinished_shards > 0 || group.has_coverage_gap())
        .collect::<Vec<_>>();
    display_groups.sort_by_key(|group| {
        std::cmp::Reverse((group.missing_results().unwrap_or(0), group.unique_item_ids))
    });
    let display_groups_total = display_groups.len();
    let display_group_lines = display_groups
        .iter()
        .take(args.top)
        .map(|group| coverage_group_line(group))
        .collect::<Vec<_>>();

    Ok(CoverageValidationSummary {
        shards: shard_count,
        groups: group_count,
        unfinished_shards,
        gap_shards,
        window_limited_shards,
        per_query_missing_results,
        unfinished_groups,
        gap_groups,
        grouped_missing_results,
        display_shards_total,
        display_groups_total,
        display_shard_lines,
        display_group_lines,
    })
}

fn print_coverage_validation_summary(summary: &CoverageValidationSummary, top: usize) {
    println!("search coverage:");
    println!("  shards: {}", summary.shards);
    println!("  groups: {}", summary.groups);
    println!("  unfinished_shards: {}", summary.unfinished_shards);
    println!("  gap_shards: {}", summary.gap_shards);
    println!("  window_limited_shards: {}", summary.window_limited_shards);
    println!(
        "  per_query_missing_results: {}",
        summary.per_query_missing_results
    );
    println!("  unfinished_groups: {}", summary.unfinished_groups);
    println!("  gap_groups: {}", summary.gap_groups);
    println!(
        "  grouped_missing_results: {}",
        summary.grouped_missing_results
    );

    if !summary.display_shard_lines.is_empty() {
        println!("shards:");
        for line in &summary.display_shard_lines {
            println!("{line}");
        }
        if summary.display_shards_total > top {
            println!(
                "  ... {} more shard(s), raise --top to print them",
                summary.display_shards_total - top
            );
        }
    }

    if !summary.display_group_lines.is_empty() {
        println!("groups:");
        for line in &summary.display_group_lines {
            println!("{line}");
        }
        if summary.display_groups_total > top {
            println!(
                "  ... {} more group(s), raise --top to print them",
                summary.display_groups_total - top
            );
        }
    }
}

fn build_coverage_groups(
    db: &Db,
    shards: &[&db::CoverageShard],
    overflow_facets: &[String],
) -> Result<Vec<CoverageGroup>> {
    let mut grouped = BTreeMap::<String, Vec<&db::CoverageShard>>::new();
    for shard in shards {
        grouped
            .entry(coverage_group_key(&shard.params_json, overflow_facets)?)
            .or_default()
            .push(*shard);
    }

    let mut out = Vec::with_capacity(grouped.len());
    for (key, group_shards) in grouped {
        let search_keys = group_shards
            .iter()
            .map(|shard| shard.search_key.clone())
            .collect::<Vec<_>>();
        let stored_unique_item_ids = db.count_unique_search_items_for_keys(&search_keys)?;
        let max_discovered_results = group_shards
            .iter()
            .map(|shard| shard.discovered_results)
            .max()
            .unwrap_or(0);
        let unique_item_ids = stored_unique_item_ids.max(max_discovered_results);
        let label = coverage_group_label(&group_shards[0].params_json, overflow_facets)?;
        let reported_total_results = group_shards
            .iter()
            .filter_map(|shard| shard.reported_total_results)
            .max();
        let unfinished_shards = group_shards
            .iter()
            .filter(|shard| shard.has_unfinished_pages())
            .count();
        out.push(CoverageGroup {
            key,
            label,
            search_keys,
            shard_count: group_shards.len(),
            unfinished_shards,
            unique_item_ids,
            reported_total_results,
        });
    }
    Ok(out)
}

fn coverage_group_for_params(
    db: &Db,
    params_json: &str,
    overflow_facets: &[String],
) -> Result<Option<CoverageGroup>> {
    let target_key = coverage_group_key(params_json, overflow_facets)?;
    let report = db.coverage_report()?;
    let mut group_shards = Vec::new();
    for shard in &report.shards {
        if coverage_group_key(&shard.params_json, overflow_facets)? == target_key {
            group_shards.push(shard);
        }
    }
    let mut groups = build_coverage_groups(db, &group_shards, overflow_facets)?;
    Ok(groups.pop())
}

fn coverage_shard_matches_filters(shard: &db::CoverageShard, args: &ValidateCoverageArgs) -> bool {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(&shard.params_json) else {
        return false;
    };
    if !args.catalogs.is_empty() && !json_array_contains_all(&value, "catalogs", &args.catalogs) {
        return false;
    }
    if !args.access.is_empty() && !json_array_contains_all(&value, "access", &args.access) {
        return false;
    }
    if args.require_year
        && json_string(&value, "publishyear_prev").is_none()
        && json_string(&value, "publishyear_next").is_none()
    {
        return false;
    }
    true
}

fn coverage_shard_line(shard: &db::CoverageShard, window_limit_results: u64) -> String {
    let missing = shard.missing_results().unwrap_or(0);
    let flags = coverage_flags(shard, window_limit_results);
    format!(
        "  {} | {} | pages={} done={} pending={} in_progress={} failed={} discovered={} unique={} total={:?} missing={} max_done_page={:?}{}",
        shard.search_key,
        coverage_shard_label(&shard.params_json),
        shard.pages,
        shard.done_pages,
        shard.pending_pages,
        shard.in_progress_pages,
        shard.failed_pages,
        shard.discovered_results,
        shard.unique_item_ids,
        shard.reported_total_results,
        missing,
        shard.max_done_page,
        flags
    )
}

fn coverage_group_line(group: &CoverageGroup) -> String {
    let missing = group.missing_results().unwrap_or(0);
    format!(
        "  {} | {} | shards={} unfinished={} unique={} total={:?} missing={} keys={}",
        group.key,
        group.label,
        group.shard_count,
        group.unfinished_shards,
        group.unique_item_ids,
        group.reported_total_results,
        missing,
        group.search_keys.join(",")
    )
}

fn coverage_flags(shard: &db::CoverageShard, window_limit_results: u64) -> String {
    let mut flags = Vec::new();
    if shard.has_unfinished_pages() {
        flags.push("unfinished");
    }
    if shard.has_coverage_gap() {
        flags.push("gap");
    }
    if shard.looks_window_limited(window_limit_results) {
        flags.push("window-limit");
    }
    if flags.is_empty() {
        String::new()
    } else {
        format!(" flags={}", flags.join(","))
    }
}

fn coverage_shard_label(params_json: &str) -> String {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(params_json) else {
        return "<invalid params>".to_string();
    };

    let mut parts = Vec::new();
    if let Some(query) = json_string(&value, "query").filter(|query| !query.is_empty()) {
        parts.push(format!("q={query:?}"));
    }
    if let Some(catalogs) =
        json_string_array(&value, "catalogs").filter(|values| !values.is_empty())
    {
        parts.push(format!("catalog={}", catalogs.join(",")));
    }
    if let Some(access) = json_string_array(&value, "access").filter(|values| !values.is_empty()) {
        parts.push(format!("access={}", access.join(",")));
    }
    match (
        json_string(&value, "publishyear_prev"),
        json_string(&value, "publishyear_next"),
    ) {
        (Some(from), Some(to)) if from == to => parts.push(format!("year={from}")),
        (Some(from), Some(to)) => parts.push(format!("year={from}..{to}")),
        (Some(from), None) => parts.push(format!("year>={from}")),
        (None, Some(to)) => parts.push(format!("year<={to}")),
        (None, None) => {}
    }
    if let Some(sort_by) = json_string(&value, "sort_by") {
        let order = json_string(&value, "order").unwrap_or_else(|| "?".to_string());
        parts.push(format!("sort={sort_by}:{order}"));
    }
    if let Some(extra) = json_extra_params(&value).filter(|values| !values.is_empty()) {
        parts.push(format!("extra={}", extra.join(",")));
    }

    if parts.is_empty() {
        "default".to_string()
    } else {
        parts.join(" ")
    }
}

fn effective_coverage_overflow_facets(args: &ValidateCoverageArgs) -> Vec<String> {
    if args.overflow_facets.is_empty() {
        default_overflow_facets()
    } else {
        dedup_nonempty_strings(args.overflow_facets.clone())
    }
}

fn coverage_group_key(params_json: &str, overflow_facets: &[String]) -> Result<String> {
    let mut value = serde_json::from_str::<serde_json::Value>(params_json)
        .context("parsing search params for coverage group")?;
    normalize_coverage_group_params(&mut value, overflow_facets);
    serde_json::to_string(&value).context("serializing normalized coverage group key")
}

fn coverage_group_label(params_json: &str, overflow_facets: &[String]) -> Result<String> {
    let mut value = serde_json::from_str::<serde_json::Value>(params_json)
        .context("parsing search params for coverage group label")?;
    normalize_coverage_group_params(&mut value, overflow_facets);
    Ok(coverage_shard_label(&serde_json::to_string(&value)?))
}

fn normalize_coverage_group_params(value: &mut serde_json::Value, overflow_facets: &[String]) {
    if let Some(object) = value.as_object_mut() {
        object.remove("sort_by");
        object.remove("order");
        if let Some(extra) = object
            .get_mut("extra")
            .and_then(serde_json::Value::as_array_mut)
        {
            extra.retain(|item| {
                let Some(pair) = item.as_array() else {
                    return true;
                };
                let Some(key) = pair.first().and_then(serde_json::Value::as_str) else {
                    return true;
                };
                !overflow_facets.iter().any(|field| field == key)
            });
        }
    }
}

fn json_string(value: &serde_json::Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
}

fn json_string_array(value: &serde_json::Value, key: &str) -> Option<Vec<String>> {
    value
        .get(key)
        .and_then(serde_json::Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(serde_json::Value::as_str)
                .map(str::to_string)
                .collect()
        })
}

fn json_array_contains_all(value: &serde_json::Value, key: &str, expected: &[String]) -> bool {
    let Some(actual) = json_string_array(value, key) else {
        return false;
    };
    expected.iter().all(|value| actual.contains(value))
}

fn json_extra_params(value: &serde_json::Value) -> Option<Vec<String>> {
    value
        .get("extra")
        .and_then(serde_json::Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| {
                    let pair = item.as_array()?;
                    let key = pair.first()?.as_str()?;
                    let value = pair.get(1)?.as_str()?;
                    Some(format!("{key}={value}"))
                })
                .collect()
        })
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

fn parse_overflow_sorts(values: &[String]) -> Result<Vec<SearchSort>> {
    let mut sorts = Vec::new();
    let mut seen = BTreeSet::new();
    for value in values {
        let Some((field, order)) = value.split_once(':') else {
            anyhow::bail!("overflow sort must be field:asc or field:desc: {value}");
        };
        let field = field.trim();
        let order = order.trim().to_ascii_lowercase();
        if field.is_empty() {
            anyhow::bail!("overflow sort field must not be empty: {value}");
        }
        if order != "asc" && order != "desc" {
            anyhow::bail!("overflow sort order must be asc or desc: {value}");
        }
        let key = (field.to_string(), order.clone());
        if seen.insert(key.clone()) {
            sorts.push(SearchSort {
                field: key.0,
                order: key.1,
            });
        }
    }
    Ok(sorts)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_overflow_sort_jobs_only_for_selected_years() {
        let cli = Cli::parse_from([
            "rusneb-parser",
            "crawl",
            "--publishyear-prev",
            "1911",
            "--publishyear-next",
            "1912",
            "--shard-years",
            "--skip-no-year-shard",
            "--overflow-year",
            "1911",
            "--overflow-sort",
            "document_titlesort:desc",
        ]);
        let Command::Crawl(args) = cli.command else {
            panic!("expected crawl command");
        };

        let jobs = build_search_jobs(&args).unwrap();

        assert_eq!(
            jobs.iter()
                .map(|job| job.label.as_str())
                .collect::<Vec<_>>(),
            vec![
                "year 1911",
                "year 1911 sort document_titlesort:desc",
                "year 1912"
            ]
        );
        assert_eq!(jobs[0].params.sort_by, None);
        assert_eq!(
            jobs[1].params.sort_by.as_deref(),
            Some("document_titlesort")
        );
        assert_eq!(jobs[1].params.order.as_deref(), Some("desc"));
        assert_eq!(jobs[2].params.sort_by, None);
    }

    #[test]
    fn builds_limited_no_year_prefix_job_after_year_jobs() {
        let cli = Cli::parse_from([
            "rusneb-parser",
            "crawl",
            "--publishyear-prev",
            "1911",
            "--publishyear-next",
            "1911",
            "--shard-years",
            "--no-year-max-pages",
            "12",
        ]);
        let Command::Crawl(args) = cli.command else {
            panic!("expected crawl command");
        };

        let jobs = build_search_jobs(&args).unwrap();

        assert_eq!(jobs.len(), 2);
        assert_eq!(jobs[0].label, "year 1911");
        assert_eq!(
            jobs[1].label,
            "no-year prefix sort document_publishyearsort:asc"
        );
        assert_eq!(jobs[1].params.publishyear_prev, None);
        assert_eq!(jobs[1].params.publishyear_next, None);
        assert_eq!(
            jobs[1].params.sort_by.as_deref(),
            Some("document_publishyearsort")
        );
        assert_eq!(jobs[1].params.order.as_deref(), Some("asc"));
        assert_eq!(jobs[1].max_pages, Some(12));
        assert_eq!(jobs[1].stop_after_known_pages, Some(5));
    }

    #[test]
    fn parses_and_deduplicates_overflow_sorts() {
        let sorts = parse_overflow_sorts(&[
            "document_titlesort:DESC".to_string(),
            "document_titlesort:desc".to_string(),
            "document_authorsort:asc".to_string(),
        ])
        .unwrap();

        assert_eq!(
            sorts,
            vec![
                SearchSort {
                    field: "document_titlesort".to_string(),
                    order: "desc".to_string()
                },
                SearchSort {
                    field: "document_authorsort".to_string(),
                    order: "asc".to_string()
                }
            ]
        );
    }
}
