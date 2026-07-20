use rusqlite::{Connection, params};
use serde_json::Value;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const MOCK_ID: &str = "mock-record-1";
const MISSING_ID: &str = "missing-record";
const TRANSIENT_ID: &str = "transient-record";
const FORBIDDEN_ID: &str = "forbidden-record";
const NO_YEAR_ID: &str = "no-year-record";
const OVERFLOW_NORMAL_ID: &str = "overflow-normal";
const OVERFLOW_SORTED_ID: &str = "overflow-sorted";
const OVERFLOW_LANG_A_ID: &str = "overflow-lang-a";
const OVERFLOW_LANG_B_ID: &str = "overflow-lang-b";
const OVERFLOW_LANG_C_ID: &str = "overflow-lang-c";
const RESUME_ID_A: &str = "resume-record-a";
const RESUME_ID_B: &str = "resume-record-b";
const RESUME_ID_C: &str = "resume-record-c";
const TOTAL_DRIFT_ID_A: &str = "total-drift-a";
const TOTAL_DRIFT_ID_B: &str = "total-drift-b";
const FINAL_SCAN_ID_A: &str = "final-scan-a";
const FINAL_SCAN_ID_B: &str = "final-scan-b";
const FINAL_SCAN_ID_C: &str = "final-scan-c";
const FINAL_SCAN_ID_D: &str = "final-scan-d";

/// End-to-end mock behavior used by the local rusneb HTTP server.
#[derive(Clone, Copy)]
enum MockMode {
    /// Serve one complete record with search, card, MARC XML, and viewer JSON endpoints.
    CompleteRecord,
    /// Serve one search result whose card page returns HTTP 404.
    MissingRecord,
    /// Serve one search result whose card page always returns HTTP 500.
    TransientCard500,
    /// Serve one search result whose card page always returns HTTP 403.
    ForbiddenCard403,
    /// Serve HTTP 403 for the first search request, then a normal search page.
    Search403ThenOk,
    /// Serve two search result shards: the default year shard and one sorted overflow shard.
    OverflowSearch,
    /// Serve overlapping overflow shards whose combined unique IDs cover the reported total.
    OverflowSearchCompleteUnion,
    /// Serve sorted shards that stay incomplete until advanced-search facet shards are added.
    OverflowFacetSearch,
    /// Serve a no-publication-year prefix shard plus an empty year shard.
    NoYearSearch,
    /// Serve a no-year prefix shard that only repeats IDs already discovered by year shards.
    NoYearKnownStop,
    /// Serve overlapping pages used to verify resume after interrupted discovery and item fetch.
    ResumePowerLoss,
    /// Serve search pages whose reported total decreases before the terminal empty page.
    SearchTotalDrift,
    /// Serve a group gap that can only be healed after the known discovery queue drains.
    FinalScanOverflow,
}

/// Temporary directory removed automatically when the test exits.
struct TempWorkspace {
    path: PathBuf,
}

impl TempWorkspace {
    /// Create a unique directory under the system temporary directory.
    fn new(name: &str) -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let suffix = COUNTER.fetch_add(1, Ordering::SeqCst);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is before UNIX_EPOCH")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "rusneb-parser-{name}-{}-{nanos}-{suffix}",
            std::process::id()
        ));
        fs::create_dir_all(&path).expect("create temp workspace");
        Self { path }
    }

    /// Return a path inside the temporary directory.
    fn join(&self, path: &str) -> PathBuf {
        self.path.join(path)
    }
}

impl Drop for TempWorkspace {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

/// Small blocking HTTP server for deterministic CLI E2E tests.
struct MockRusnebServer {
    base_url: String,
    requests: Arc<Mutex<Vec<String>>>,
    shutdown: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl MockRusnebServer {
    /// Start the mock server on a local random port.
    fn start(mode: MockMode) -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind mock server");
        listener
            .set_nonblocking(true)
            .expect("set mock server nonblocking");
        let addr = listener.local_addr().expect("read mock server address");
        let requests = Arc::new(Mutex::new(Vec::new()));
        let shutdown = Arc::new(AtomicBool::new(false));

        let thread_requests = Arc::clone(&requests);
        let thread_shutdown = Arc::clone(&shutdown);
        let handle = thread::spawn(move || {
            while !thread_shutdown.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        handle_connection(stream, mode, Arc::clone(&thread_requests))
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(_) => break,
                }
            }
        });

        Self {
            base_url: format!("http://{addr}"),
            requests,
            shutdown,
            handle: Some(handle),
        }
    }

    /// Return all request targets received by the server.
    fn requests(&self) -> Vec<String> {
        self.requests.lock().expect("lock request log").clone()
    }
}

impl Drop for MockRusnebServer {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(self.base_url.trim_start_matches("http://"));
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

#[test]
fn crawl_persists_record_resumes_and_exports_jsonl() {
    let workspace = TempWorkspace::new("record");
    let server = MockRusnebServer::start(MockMode::CompleteRecord);
    let db = workspace.join("state.sqlite");
    let output = workspace.join("out.jsonl");
    let manifest = workspace.join("manifest.json");

    run_ok(
        Command::new(parser_bin())
            .arg("crawl")
            .arg("--db")
            .arg(&db)
            .arg("--base-url")
            .arg(&server.base_url)
            .arg("--catalog")
            .arg("25")
            .arg("--access")
            .arg("open")
            .arg("--max-pages")
            .arg("1")
            .arg("--workers")
            .arg("1")
            .arg("--timeout-secs")
            .arg("5"),
    );
    assert_eq!(server.requests().len(), 4);

    run_ok(
        Command::new(parser_bin())
            .arg("crawl")
            .arg("--db")
            .arg(&db)
            .arg("--base-url")
            .arg(&server.base_url)
            .arg("--catalog")
            .arg("25")
            .arg("--access")
            .arg("open")
            .arg("--max-pages")
            .arg("1")
            .arg("--workers")
            .arg("1")
            .arg("--timeout-secs")
            .arg("5"),
    );
    assert_eq!(server.requests().len(), 4);

    run_ok(
        Command::new(parser_bin())
            .arg("export-jsonl")
            .arg("--db")
            .arg(&db)
            .arg("--output")
            .arg(&output),
    );
    let validation = run_ok(
        Command::new(parser_bin())
            .arg("validate-coverage")
            .arg("--db")
            .arg(&db)
            .arg("--catalog")
            .arg("25")
            .arg("--access")
            .arg("open"),
    );
    let validation_stdout =
        String::from_utf8(validation.stdout).expect("validation stdout is UTF-8");
    assert!(validation_stdout.contains("coverage ok"));
    assert_eq!(search_item_count(&db), 1);

    let report = run_ok(
        Command::new(parser_bin())
            .arg("report")
            .arg("--db")
            .arg(&db)
            .arg("--catalog")
            .arg("25")
            .arg("--access")
            .arg("open"),
    );
    let report_stdout = String::from_utf8(report.stdout).expect("report stdout is UTF-8");
    assert!(report_stdout.contains("completion ok"));
    assert!(report_stdout.contains("records: 1"));

    run_ok(
        Command::new(parser_bin())
            .arg("export-manifest")
            .arg("--db")
            .arg(&db)
            .arg("--output")
            .arg(&manifest)
            .arg("--crawl-command")
            .arg("rusneb-parser crawl --catalog 25 --access open")
            .arg("--file")
            .arg(&output),
    );

    let jsonl = fs::read_to_string(&output).expect("read exported JSONL");
    let lines = jsonl.lines().collect::<Vec<_>>();
    assert_eq!(lines.len(), 1);

    let record: Value = serde_json::from_str(lines[0]).expect("parse exported record JSON");
    assert_eq!(record["id"], MOCK_ID);
    assert_eq!(record["metadata"]["title"], "Mock Book & Test");
    assert_eq!(record["metadata"]["authors"][0], "Mock Author");
    assert_eq!(record["metadata"]["year"], "1911");
    assert_eq!(record["metadata"]["detail_map"]["Каталог"][0], "Книги");
    assert_eq!(
        record["metadata"]["detail_map"]["Место издания"][0],
        "Тестоград"
    );
    assert_json_array_contains(&record["metadata"]["topics"], "Тестовая тематика");
    assert_json_array_contains(&record["metadata"]["topics"], "Mock subject");
    assert_json_array_contains(
        &record["metadata"]["pdf_links"],
        &format!("{}/files/card.pdf", server.base_url),
    );
    assert_json_array_contains(
        &record["metadata"]["pdf_links"],
        "http://example.test/marc.pdf",
    );
    assert_eq!(record["marc21"]["leader"], "01234nam a2200000 i 4500");
    assert_eq!(record["viewer_access"]["access"], true);
    assert!(record["viewer_access"].get("token").is_none());
    assert!(record["viewer_access"].get("viewer").is_none());

    let manifest_json: Value =
        serde_json::from_str(&fs::read_to_string(&manifest).expect("read manifest"))
            .expect("parse manifest JSON");
    assert_eq!(manifest_json["schema_version"], 1);
    assert_eq!(manifest_json["tool"]["version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(
        manifest_json["commands"]["crawl"],
        "rusneb-parser crawl --catalog 25 --access open"
    );
    assert_eq!(manifest_json["sqlite"]["records"], 1);
    assert_eq!(manifest_json["sqlite"]["items"]["done"], 1);
    assert_eq!(manifest_json["failed_items"]["total"], 0);
    assert_eq!(
        manifest_json["outputs"][0]["bytes"],
        fs::metadata(&output).expect("read output metadata").len()
    );
    assert_hex_sha256(&manifest_json["outputs"][0]["sha256"]);
}

#[test]
fn crawl_marks_card_404_as_terminal_missing() {
    let workspace = TempWorkspace::new("missing");
    let server = MockRusnebServer::start(MockMode::MissingRecord);
    let db = workspace.join("state.sqlite");

    run_ok(
        Command::new(parser_bin())
            .arg("crawl")
            .arg("--db")
            .arg(&db)
            .arg("--base-url")
            .arg(&server.base_url)
            .arg("--catalog")
            .arg("25")
            .arg("--access")
            .arg("open")
            .arg("--max-pages")
            .arg("1")
            .arg("--workers")
            .arg("1")
            .arg("--timeout-secs")
            .arg("5"),
    );
    assert_eq!(server.requests().len(), 2);

    let stats = run_ok(Command::new(parser_bin()).arg("stats").arg("--db").arg(&db));
    let stdout = String::from_utf8(stats.stdout).expect("stats stdout is UTF-8");
    assert!(stdout.contains("records: 0"));
    assert!(stdout.contains("  missing: 1"));
    assert!(!stdout.contains("  failed: 1"));

    run_ok(
        Command::new(parser_bin())
            .arg("crawl")
            .arg("--db")
            .arg(&db)
            .arg("--base-url")
            .arg(&server.base_url)
            .arg("--catalog")
            .arg("25")
            .arg("--access")
            .arg("open")
            .arg("--max-pages")
            .arg("1")
            .arg("--workers")
            .arg("1")
            .arg("--timeout-secs")
            .arg("5"),
    );
    assert_eq!(server.requests().len(), 2);
}

#[test]
fn crawl_defers_repeated_card_500_without_spending_attempts() {
    let workspace = TempWorkspace::new("transient");
    let server = MockRusnebServer::start(MockMode::TransientCard500);
    let db = workspace.join("state.sqlite");

    run_ok(
        Command::new(parser_bin())
            .arg("crawl")
            .arg("--db")
            .arg(&db)
            .arg("--base-url")
            .arg(&server.base_url)
            .arg("--catalog")
            .arg("25")
            .arg("--access")
            .arg("open")
            .arg("--max-pages")
            .arg("1")
            .arg("--max-items")
            .arg("2")
            .arg("--workers")
            .arg("1")
            .arg("--max-attempts")
            .arg("1")
            .arg("--timeout-secs")
            .arg("5"),
    );

    let requests = server.requests();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request == &&format!("/catalog/{TRANSIENT_ID}/"))
            .count(),
        2
    );
    assert_eq!(item_status(&db, TRANSIENT_ID), "pending");
    assert_eq!(item_attempts(&db, TRANSIENT_ID), 0);
    assert_eq!(item_last_http_status(&db, TRANSIENT_ID), Some(500));

    let stats = run_ok(Command::new(parser_bin()).arg("stats").arg("--db").arg(&db));
    let stdout = String::from_utf8(stats.stdout).expect("stats stdout is UTF-8");
    assert!(stdout.contains("records: 0"));
    assert!(stdout.contains("  pending: 1"));
    assert!(!stdout.contains("  failed: 1"));
}

#[test]
fn crawl_exhausts_card_403_and_retry_failed_resets_it() {
    let workspace = TempWorkspace::new("forbidden");
    let server = MockRusnebServer::start(MockMode::ForbiddenCard403);
    let db = workspace.join("state.sqlite");

    let crawl = run_command(
        Command::new(parser_bin())
            .arg("crawl")
            .arg("--db")
            .arg(&db)
            .arg("--base-url")
            .arg(&server.base_url)
            .arg("--catalog")
            .arg("25")
            .arg("--access")
            .arg("open")
            .arg("--max-pages")
            .arg("1")
            .arg("--workers")
            .arg("1")
            .arg("--max-attempts")
            .arg("2")
            .arg("--max-consecutive-403-errors")
            .arg("0")
            .arg("--timeout-secs")
            .arg("5"),
    );
    assert!(!crawl.status.success());
    let stderr = String::from_utf8(crawl.stderr).expect("crawl stderr is UTF-8");
    assert!(stderr.contains("failed HTTP 403 rows: items=1"));
    assert!(stderr.contains("crawl incomplete: 1 failed item(s)"));

    let requests = server.requests();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request == &&format!("/catalog/{FORBIDDEN_ID}/"))
            .count(),
        2
    );
    assert_eq!(item_status(&db, FORBIDDEN_ID), "failed");
    assert_eq!(item_attempts(&db, FORBIDDEN_ID), 2);
    assert_eq!(item_last_http_status(&db, FORBIDDEN_ID), Some(403));

    let report = run_command(
        Command::new(parser_bin())
            .arg("report")
            .arg("--db")
            .arg(&db)
            .arg("--catalog")
            .arg("25")
            .arg("--access")
            .arg("open")
            .arg("--max-attempts")
            .arg("2")
            .arg("--failed-item-sample")
            .arg("1"),
    );
    assert!(!report.status.success());
    let report_stdout = String::from_utf8(report.stdout).expect("report stdout is UTF-8");
    assert!(report_stdout.contains("failed item HTTP statuses"));
    assert!(report_stdout.contains("HTTP 403: 1"));
    assert!(report_stdout.contains(FORBIDDEN_ID));
    assert!(report_stdout.contains("retry-failed"));

    run_ok(
        Command::new(parser_bin())
            .arg("retry-failed")
            .arg("--db")
            .arg(&db)
            .arg("--http-status")
            .arg("403"),
    );
    assert_eq!(item_status(&db, FORBIDDEN_ID), "pending");
    assert_eq!(item_attempts(&db, FORBIDDEN_ID), 0);
    assert_eq!(item_last_http_status(&db, FORBIDDEN_ID), None);
}

#[test]
fn crawl_defers_card_403_after_global_block_threshold() {
    let workspace = TempWorkspace::new("forbidden-block");
    let server = MockRusnebServer::start(MockMode::ForbiddenCard403);
    let db = workspace.join("state.sqlite");

    let crawl = run_ok(
        Command::new(parser_bin())
            .arg("crawl")
            .arg("--db")
            .arg(&db)
            .arg("--base-url")
            .arg(&server.base_url)
            .arg("--catalog")
            .arg("25")
            .arg("--access")
            .arg("open")
            .arg("--max-pages")
            .arg("1")
            .arg("--max-items")
            .arg("2")
            .arg("--workers")
            .arg("1")
            .arg("--max-attempts")
            .arg("5")
            .arg("--max-consecutive-403-errors")
            .arg("2")
            .arg("--http-403-pause-secs")
            .arg("0")
            .arg("--timeout-secs")
            .arg("5"),
    );
    let stderr = String::from_utf8(crawl.stderr).expect("crawl stderr is UTF-8");
    assert!(stderr.contains("HTTP 403 block threshold reached"));

    let requests = server.requests();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request == &&format!("/catalog/{FORBIDDEN_ID}/"))
            .count(),
        2
    );
    assert_eq!(item_status(&db, FORBIDDEN_ID), "pending");
    assert_eq!(item_attempts(&db, FORBIDDEN_ID), 1);
    assert_eq!(item_last_http_status(&db, FORBIDDEN_ID), Some(403));
}

#[test]
fn crawl_defers_search_403_after_global_block_threshold() {
    let workspace = TempWorkspace::new("search-403-block");
    let server = MockRusnebServer::start(MockMode::Search403ThenOk);
    let db = workspace.join("state.sqlite");

    let crawl = run_ok(
        Command::new(parser_bin())
            .arg("crawl")
            .arg("--db")
            .arg(&db)
            .arg("--base-url")
            .arg(&server.base_url)
            .arg("--catalog")
            .arg("25")
            .arg("--access")
            .arg("open")
            .arg("--max-pages")
            .arg("1")
            .arg("--max-items")
            .arg("0")
            .arg("--workers")
            .arg("1")
            .arg("--max-attempts")
            .arg("5")
            .arg("--max-consecutive-403-errors")
            .arg("1")
            .arg("--http-403-pause-secs")
            .arg("0")
            .arg("--timeout-secs")
            .arg("5"),
    );
    let stderr = String::from_utf8(crawl.stderr).expect("crawl stderr is UTF-8");
    assert!(stderr.contains("HTTP 403 block threshold reached"));

    assert_eq!(search_page_status(&db, 1), "done");
    assert_eq!(search_page_status_count(&db, "failed"), 0);
    assert_eq!(
        server
            .requests()
            .iter()
            .filter(|request| request.starts_with("/search/"))
            .count(),
        2
    );
}

#[test]
fn crawl_resumes_interrupted_search_page() {
    let workspace = TempWorkspace::new("interrupted-search");
    let server = MockRusnebServer::start(MockMode::CompleteRecord);
    let db = workspace.join("state.sqlite");

    run_ok(
        Command::new(parser_bin())
            .arg("crawl")
            .arg("--db")
            .arg(&db)
            .arg("--base-url")
            .arg(&server.base_url)
            .arg("--catalog")
            .arg("25")
            .arg("--access")
            .arg("open")
            .arg("--max-pages")
            .arg("1")
            .arg("--max-items")
            .arg("0")
            .arg("--workers")
            .arg("1")
            .arg("--timeout-secs")
            .arg("5"),
    );
    assert_eq!(search_page_status(&db, 1), "done");

    clone_search_page_from_existing_shard(&db, 1, 2, "in_progress");
    assert_eq!(search_page_status(&db, 2), "in_progress");

    run_ok(
        Command::new(parser_bin())
            .arg("crawl")
            .arg("--db")
            .arg(&db)
            .arg("--base-url")
            .arg(&server.base_url)
            .arg("--catalog")
            .arg("25")
            .arg("--access")
            .arg("open")
            .arg("--max-pages")
            .arg("2")
            .arg("--max-items")
            .arg("0")
            .arg("--workers")
            .arg("1")
            .arg("--timeout-secs")
            .arg("5"),
    );

    assert_eq!(search_page_status(&db, 2), "done");
    assert_eq!(search_page_status_count(&db, "in_progress"), 0);
    assert_eq!(
        server
            .requests()
            .iter()
            .filter(|request| request.starts_with("/search/"))
            .count(),
        2
    );
}

#[test]
fn crawl_resumes_power_loss_mid_discovery_and_item_fetch() {
    let workspace = TempWorkspace::new("resume-power-loss");
    let server = MockRusnebServer::start(MockMode::ResumePowerLoss);
    let db = workspace.join("state.sqlite");

    run_ok(
        Command::new(parser_bin())
            .arg("crawl")
            .arg("--db")
            .arg(&db)
            .arg("--base-url")
            .arg(&server.base_url)
            .arg("--catalog")
            .arg("25")
            .arg("--access")
            .arg("open")
            .arg("--max-pages")
            .arg("1")
            .arg("--max-items")
            .arg("0")
            .arg("--workers")
            .arg("1")
            .arg("--timeout-secs")
            .arg("5"),
    );

    assert_eq!(search_page_status(&db, 1), "done");
    assert_eq!(
        item_ids(&db),
        vec![RESUME_ID_A.to_string(), RESUME_ID_B.to_string()]
    );
    assert_eq!(search_item_count(&db), 2);

    clone_search_page_from_existing_shard(&db, 1, 2, "in_progress");
    mark_item_in_progress(&db, RESUME_ID_A);
    assert_eq!(search_page_status(&db, 2), "in_progress");
    assert_eq!(item_status(&db, RESUME_ID_A), "in_progress");

    run_ok(
        Command::new(parser_bin())
            .arg("crawl")
            .arg("--db")
            .arg(&db)
            .arg("--base-url")
            .arg(&server.base_url)
            .arg("--catalog")
            .arg("25")
            .arg("--access")
            .arg("open")
            .arg("--max-pages")
            .arg("3")
            .arg("--workers")
            .arg("1")
            .arg("--timeout-secs")
            .arg("5"),
    );

    assert_eq!(
        item_ids(&db),
        vec![
            RESUME_ID_A.to_string(),
            RESUME_ID_B.to_string(),
            RESUME_ID_C.to_string()
        ]
    );
    assert_eq!(
        record_ids(&db),
        vec![
            RESUME_ID_A.to_string(),
            RESUME_ID_B.to_string(),
            RESUME_ID_C.to_string()
        ]
    );
    assert_eq!(item_status_count(&db, "done"), 3);
    assert_eq!(item_status_count(&db, "pending"), 0);
    assert_eq!(item_status_count(&db, "in_progress"), 0);
    assert_eq!(search_page_status_count(&db, "done"), 3);
    assert_eq!(search_page_status_count(&db, "in_progress"), 0);
    assert_eq!(search_item_count(&db), 4);
    assert_eq!(item_attempts(&db, RESUME_ID_A), 2);
    assert_eq!(item_attempts(&db, RESUME_ID_B), 1);
    assert_eq!(item_attempts(&db, RESUME_ID_C), 1);

    let requests = server.requests();
    assert_eq!(catalog_request_count(&requests, RESUME_ID_A), 1);
    assert_eq!(catalog_request_count(&requests, RESUME_ID_B), 1);
    assert_eq!(catalog_request_count(&requests, RESUME_ID_C), 1);
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.starts_with("/search/"))
            .count(),
        3
    );

    let report = run_ok(
        Command::new(parser_bin())
            .arg("report")
            .arg("--db")
            .arg(&db)
            .arg("--catalog")
            .arg("25")
            .arg("--access")
            .arg("open"),
    );
    let report_stdout = String::from_utf8(report.stdout).expect("report stdout is UTF-8");
    assert!(report_stdout.contains("completion ok"));
    assert!(report_stdout.contains("records: 3"));
}

#[test]
fn crawl_discovers_sorted_overflow_search_shards() {
    let workspace = TempWorkspace::new("overflow");
    let server = MockRusnebServer::start(MockMode::OverflowSearch);
    let db = workspace.join("state.sqlite");

    run_ok(
        Command::new(parser_bin())
            .arg("crawl")
            .arg("--db")
            .arg(&db)
            .arg("--base-url")
            .arg(&server.base_url)
            .arg("--catalog")
            .arg("25")
            .arg("--access")
            .arg("open")
            .arg("--publishyear-prev")
            .arg("1911")
            .arg("--publishyear-next")
            .arg("1911")
            .arg("--shard-years")
            .arg("--skip-no-year-shard")
            .arg("--overflow-year")
            .arg("1911")
            .arg("--overflow-sort")
            .arg("document_titlesort:desc")
            .arg("--max-pages")
            .arg("1")
            .arg("--max-items")
            .arg("0")
            .arg("--workers")
            .arg("1")
            .arg("--timeout-secs")
            .arg("5"),
    );

    let requests = server.requests();
    let search_requests = requests
        .iter()
        .filter(|request| request.starts_with("/search/"))
        .collect::<Vec<_>>();
    assert_eq!(search_requests.len(), 2);
    assert!(search_requests.iter().any(|request| {
        request.contains("by=document_titlesort") && request.contains("order=desc")
    }));

    let stats = run_ok(Command::new(parser_bin()).arg("stats").arg("--db").arg(&db));
    let stdout = String::from_utf8(stats.stdout).expect("stats stdout is UTF-8");
    assert!(stdout.contains("records: 0"));
    assert!(stdout.contains("  pending: 2"));

    let validation = run_command(
        Command::new(parser_bin())
            .arg("validate-coverage")
            .arg("--db")
            .arg(&db)
            .arg("--catalog")
            .arg("25")
            .arg("--access")
            .arg("open")
            .arg("--require-year"),
    );
    assert!(!validation.status.success());
    let validation_stdout =
        String::from_utf8(validation.stdout).expect("validation stdout is UTF-8");
    assert!(validation_stdout.contains("gap_shards: 2"));
    assert!(validation_stdout.contains("document_titlesort:desc"));
}

#[test]
fn crawl_auto_detects_sorted_overflow_search_shards() {
    let workspace = TempWorkspace::new("auto-overflow");
    let server = MockRusnebServer::start(MockMode::OverflowSearch);
    let db = workspace.join("state.sqlite");

    run_ok(
        Command::new(parser_bin())
            .arg("crawl")
            .arg("--db")
            .arg(&db)
            .arg("--base-url")
            .arg(&server.base_url)
            .arg("--catalog")
            .arg("25")
            .arg("--access")
            .arg("open")
            .arg("--publishyear-prev")
            .arg("1911")
            .arg("--publishyear-next")
            .arg("1911")
            .arg("--shard-years")
            .arg("--skip-no-year-shard")
            .arg("--search-window-limit-results")
            .arg("1")
            .arg("--no-auto-overflow-facets")
            .arg("--max-items")
            .arg("0")
            .arg("--workers")
            .arg("1")
            .arg("--timeout-secs")
            .arg("5"),
    );

    let requests = server.requests();
    let search_requests = requests
        .iter()
        .filter(|request| request.starts_with("/search/"))
        .collect::<Vec<_>>();
    assert_eq!(search_requests.len(), 14);
    assert!(search_requests.iter().any(|request| {
        request.contains("by=document_titlesort") && request.contains("order=desc")
    }));
    assert!(search_requests.iter().any(|request| {
        request.contains("by=document_authorsort") && request.contains("order=asc")
    }));
    assert!(search_requests.iter().any(|request| {
        request.contains("by=document_authorsort") && request.contains("order=desc")
    }));

    let stats = run_ok(Command::new(parser_bin()).arg("stats").arg("--db").arg(&db));
    let stdout = String::from_utf8(stats.stdout).expect("stats stdout is UTF-8");
    assert!(stdout.contains("records: 0"));
    assert!(stdout.contains("  pending: 2"));

    let validation = run_command(
        Command::new(parser_bin())
            .arg("validate-coverage")
            .arg("--db")
            .arg(&db)
            .arg("--catalog")
            .arg("25")
            .arg("--access")
            .arg("open")
            .arg("--require-year")
            .arg("--window-limit-results")
            .arg("1"),
    );
    assert!(!validation.status.success());
    let validation_stdout =
        String::from_utf8(validation.stdout).expect("validation stdout is UTF-8");
    assert!(validation_stdout.contains("gap_shards: 7"));
    assert!(validation_stdout.contains("window_limited_shards: 7"));
    assert!(validation_stdout.contains("gap_groups: 1"));
    assert!(validation_stdout.contains("document_titlesort:desc"));
}

#[test]
fn crawl_auto_detects_small_coverage_gap_overflow_search_shards() {
    let workspace = TempWorkspace::new("small-gap-overflow");
    let server = MockRusnebServer::start(MockMode::OverflowSearchCompleteUnion);
    let db = workspace.join("state.sqlite");

    run_ok(
        Command::new(parser_bin())
            .arg("crawl")
            .arg("--db")
            .arg(&db)
            .arg("--base-url")
            .arg(&server.base_url)
            .arg("--catalog")
            .arg("25")
            .arg("--access")
            .arg("open")
            .arg("--publishyear-prev")
            .arg("1911")
            .arg("--publishyear-next")
            .arg("1911")
            .arg("--shard-years")
            .arg("--skip-no-year-shard")
            .arg("--max-items")
            .arg("0")
            .arg("--workers")
            .arg("1")
            .arg("--timeout-secs")
            .arg("5"),
    );

    let requests = server.requests();
    let search_requests = requests
        .iter()
        .filter(|request| request.starts_with("/search/"))
        .collect::<Vec<_>>();
    assert_eq!(search_requests.len(), 14);
    assert!(search_requests.iter().any(|request| {
        request.contains("by=document_titlesort") && request.contains("order=desc")
    }));
    assert!(
        requests
            .iter()
            .all(|request| !request.starts_with("/search/extended/"))
    );

    let validation = run_ok(
        Command::new(parser_bin())
            .arg("validate-coverage")
            .arg("--db")
            .arg(&db)
            .arg("--catalog")
            .arg("25")
            .arg("--access")
            .arg("open")
            .arg("--require-year"),
    );
    let stdout = String::from_utf8(validation.stdout).expect("validation stdout is UTF-8");
    assert!(stdout.contains("gap_groups: 0"));
    assert!(stdout.contains("coverage ok"));
}

#[test]
fn crawl_auto_detects_facet_overflow_after_sorted_gap() {
    let workspace = TempWorkspace::new("facet-overflow");
    let server = MockRusnebServer::start(MockMode::OverflowFacetSearch);
    let db = workspace.join("state.sqlite");

    run_ok(
        Command::new(parser_bin())
            .arg("crawl")
            .arg("--db")
            .arg(&db)
            .arg("--base-url")
            .arg(&server.base_url)
            .arg("--catalog")
            .arg("25")
            .arg("--access")
            .arg("open")
            .arg("--publishyear-prev")
            .arg("1911")
            .arg("--publishyear-next")
            .arg("1911")
            .arg("--shard-years")
            .arg("--skip-no-year-shard")
            .arg("--overflow-facet")
            .arg("lang")
            .arg("--max-items")
            .arg("0")
            .arg("--workers")
            .arg("1")
            .arg("--timeout-secs")
            .arg("5"),
    );

    let requests = server.requests();
    assert!(
        requests
            .iter()
            .any(|request| request.starts_with("/search/extended/"))
    );
    assert!(
        requests
            .iter()
            .any(|request| request.contains("lang=facet-a"))
    );
    assert!(
        requests
            .iter()
            .any(|request| request.contains("lang=facet-b"))
    );
    assert!(
        requests
            .iter()
            .any(|request| request.contains("lang=facet-c"))
    );

    let validation = run_ok(
        Command::new(parser_bin())
            .arg("validate-coverage")
            .arg("--db")
            .arg(&db)
            .arg("--catalog")
            .arg("25")
            .arg("--access")
            .arg("open")
            .arg("--require-year")
            .arg("--overflow-facet")
            .arg("lang"),
    );
    let stdout = String::from_utf8(validation.stdout).expect("validation stdout is UTF-8");
    assert!(stdout.contains("gap_groups: 0"));
    assert!(stdout.contains("coverage ok"));
    assert_eq!(search_item_count(&db), 10);
}

#[test]
fn crawl_self_heals_coverage_gap_after_known_queue_drains() {
    let workspace = TempWorkspace::new("final-scan-overflow");
    let server = MockRusnebServer::start(MockMode::FinalScanOverflow);
    let db = workspace.join("state.sqlite");

    run_ok(
        Command::new(parser_bin())
            .arg("crawl")
            .arg("--db")
            .arg(&db)
            .arg("--base-url")
            .arg(&server.base_url)
            .arg("--catalog")
            .arg("25")
            .arg("--access")
            .arg("open")
            .arg("--publishyear-prev")
            .arg("1911")
            .arg("--publishyear-next")
            .arg("1911")
            .arg("--shard-years")
            .arg("--skip-no-year-shard")
            .arg("--overflow-year")
            .arg("1911")
            .arg("--overflow-sort")
            .arg("document_titlesort:desc")
            .arg("--max-items")
            .arg("0")
            .arg("--workers")
            .arg("1")
            .arg("--timeout-secs")
            .arg("5"),
    );

    let requests = server.requests();
    assert!(
        requests
            .iter()
            .any(|request| request.starts_with("/search/extended/")),
        "self-healing scan did not load advanced-search facets: {requests:?}"
    );
    assert!(
        requests
            .iter()
            .any(|request| request.contains("lang=facet-final")),
        "self-healing scan did not seed facet shards after the manual shard drained: {requests:?}"
    );
    assert_eq!(search_item_count(&db), 4);

    let validation = run_ok(
        Command::new(parser_bin())
            .arg("validate-coverage")
            .arg("--db")
            .arg(&db)
            .arg("--catalog")
            .arg("25")
            .arg("--access")
            .arg("open")
            .arg("--require-year"),
    );
    let stdout = String::from_utf8(validation.stdout).expect("validation stdout is UTF-8");
    assert!(stdout.contains("gap_groups: 0"));
    assert!(stdout.contains("coverage ok"));
}

#[test]
fn validate_coverage_accepts_latest_non_empty_total_after_search_total_drift() {
    let workspace = TempWorkspace::new("total-drift");
    let server = MockRusnebServer::start(MockMode::SearchTotalDrift);
    let db = workspace.join("state.sqlite");

    run_ok(
        Command::new(parser_bin())
            .arg("crawl")
            .arg("--db")
            .arg(&db)
            .arg("--base-url")
            .arg(&server.base_url)
            .arg("--catalog")
            .arg("25")
            .arg("--access")
            .arg("open")
            .arg("--publishyear-prev")
            .arg("1907")
            .arg("--publishyear-next")
            .arg("1907")
            .arg("--shard-years")
            .arg("--skip-no-year-shard")
            .arg("--max-items")
            .arg("0")
            .arg("--workers")
            .arg("1")
            .arg("--timeout-secs")
            .arg("5"),
    );

    assert_eq!(search_item_count(&db), 2);
    let validation = run_ok(
        Command::new(parser_bin())
            .arg("validate-coverage")
            .arg("--db")
            .arg(&db)
            .arg("--catalog")
            .arg("25")
            .arg("--access")
            .arg("open")
            .arg("--require-year"),
    );
    let stdout = String::from_utf8(validation.stdout).expect("validation stdout is UTF-8");
    assert!(stdout.contains("gap_groups: 0"));
    assert!(stdout.contains("coverage ok"));
}

#[test]
fn validate_coverage_accepts_complete_overflow_group_union() {
    let workspace = TempWorkspace::new("overflow-union");
    let server = MockRusnebServer::start(MockMode::OverflowSearchCompleteUnion);
    let db = workspace.join("state.sqlite");

    run_ok(
        Command::new(parser_bin())
            .arg("crawl")
            .arg("--db")
            .arg(&db)
            .arg("--base-url")
            .arg(&server.base_url)
            .arg("--catalog")
            .arg("25")
            .arg("--access")
            .arg("open")
            .arg("--publishyear-prev")
            .arg("1911")
            .arg("--publishyear-next")
            .arg("1911")
            .arg("--shard-years")
            .arg("--skip-no-year-shard")
            .arg("--overflow-year")
            .arg("1911")
            .arg("--overflow-sort")
            .arg("document_titlesort:desc")
            .arg("--max-pages")
            .arg("1")
            .arg("--max-items")
            .arg("0")
            .arg("--workers")
            .arg("1")
            .arg("--timeout-secs")
            .arg("5"),
    );

    let validation = run_ok(
        Command::new(parser_bin())
            .arg("validate-coverage")
            .arg("--db")
            .arg(&db)
            .arg("--catalog")
            .arg("25")
            .arg("--access")
            .arg("open")
            .arg("--require-year"),
    );
    let stdout = String::from_utf8(validation.stdout).expect("validation stdout is UTF-8");
    assert!(stdout.contains("gap_shards: 2"));
    assert!(stdout.contains("gap_groups: 0"));
    assert!(stdout.contains("coverage ok"));
    assert_eq!(search_item_count(&db), 2);
}

#[test]
fn validate_coverage_falls_back_to_page_counts_without_memberships() {
    let workspace = TempWorkspace::new("legacy-coverage");
    let server = MockRusnebServer::start(MockMode::CompleteRecord);
    let db = workspace.join("state.sqlite");

    run_ok(
        Command::new(parser_bin())
            .arg("crawl")
            .arg("--db")
            .arg(&db)
            .arg("--base-url")
            .arg(&server.base_url)
            .arg("--catalog")
            .arg("25")
            .arg("--access")
            .arg("open")
            .arg("--max-pages")
            .arg("1")
            .arg("--max-items")
            .arg("0")
            .arg("--workers")
            .arg("1")
            .arg("--timeout-secs")
            .arg("5"),
    );
    clear_search_items(&db);

    let validation = run_ok(
        Command::new(parser_bin())
            .arg("validate-coverage")
            .arg("--db")
            .arg(&db)
            .arg("--catalog")
            .arg("25")
            .arg("--access")
            .arg("open"),
    );
    let stdout = String::from_utf8(validation.stdout).expect("validation stdout is UTF-8");
    assert!(stdout.contains("gap_groups: 0"));
    assert!(stdout.contains("coverage ok"));
}

#[test]
fn crawl_discovers_no_year_prefix_shard() {
    let workspace = TempWorkspace::new("no-year");
    let server = MockRusnebServer::start(MockMode::NoYearSearch);
    let db = workspace.join("state.sqlite");

    run_ok(
        Command::new(parser_bin())
            .arg("crawl")
            .arg("--db")
            .arg(&db)
            .arg("--base-url")
            .arg(&server.base_url)
            .arg("--catalog")
            .arg("25")
            .arg("--access")
            .arg("open")
            .arg("--publishyear-prev")
            .arg("1911")
            .arg("--publishyear-next")
            .arg("1911")
            .arg("--shard-years")
            .arg("--no-year-max-pages")
            .arg("1")
            .arg("--no-auto-overflow-facets")
            .arg("--max-items")
            .arg("0")
            .arg("--workers")
            .arg("1")
            .arg("--timeout-secs")
            .arg("5"),
    );

    let requests = server.requests();
    let search_requests = requests
        .iter()
        .filter(|request| request.starts_with("/search/"))
        .collect::<Vec<_>>();
    assert_eq!(search_requests.len(), 8);
    assert!(search_requests.iter().any(|request| {
        request.contains("by=document_publishyearsort")
            && request.contains("order=asc")
            && !request.contains("publishyear_prev")
            && !request.contains("publishyear_next")
    }));
    assert!(search_requests.iter().all(|request| {
        !(request.contains("by=document_publishyearsort")
            && request.contains("order=asc")
            && request.contains("PAGEN_1=2"))
    }));

    let stats = run_ok(Command::new(parser_bin()).arg("stats").arg("--db").arg(&db));
    let stdout = String::from_utf8(stats.stdout).expect("stats stdout is UTF-8");
    assert!(stdout.contains("records: 0"));
    assert!(stdout.contains("  pending: 1"));
}

#[test]
fn crawl_stops_no_year_prefix_after_known_pages() {
    let workspace = TempWorkspace::new("no-year-known-stop");
    let server = MockRusnebServer::start(MockMode::NoYearKnownStop);
    let db = workspace.join("state.sqlite");

    run_ok(
        Command::new(parser_bin())
            .arg("crawl")
            .arg("--db")
            .arg(&db)
            .arg("--base-url")
            .arg(&server.base_url)
            .arg("--catalog")
            .arg("25")
            .arg("--access")
            .arg("open")
            .arg("--publishyear-prev")
            .arg("1911")
            .arg("--publishyear-next")
            .arg("1911")
            .arg("--shard-years")
            .arg("--no-year-max-pages")
            .arg("5")
            .arg("--no-year-stop-after-known-pages")
            .arg("1")
            .arg("--no-auto-overflow-facets")
            .arg("--max-items")
            .arg("0")
            .arg("--workers")
            .arg("1")
            .arg("--timeout-secs")
            .arg("5"),
    );

    let requests = server.requests();
    let search_requests = requests
        .iter()
        .filter(|request| request.starts_with("/search/"))
        .collect::<Vec<_>>();
    assert_eq!(search_requests.len(), 3);
    assert!(search_requests.iter().any(|request| {
        request.contains("by=document_publishyearsort")
            && request.contains("order=asc")
            && !request.contains("publishyear_prev")
            && !request.contains("publishyear_next")
    }));
    assert!(search_requests.iter().all(|request| {
        !(request.contains("by=document_publishyearsort")
            && request.contains("order=asc")
            && request.contains("PAGEN_1=2"))
    }));
    assert_eq!(search_item_count(&db), 2);
}

/// Return the test-built rusneb-parser binary path.
fn parser_bin() -> &'static Path {
    Path::new(env!("CARGO_BIN_EXE_rusneb-parser"))
}

/// Run a command and fail with stdout/stderr context on non-zero exit.
fn run_ok(command: &mut Command) -> Output {
    let output = run_command(command);
    if !output.status.success() {
        panic!(
            "command failed: {command:?}\nstatus: {}\nstdout:\n{}\nstderr:\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    output
}

/// Run a command and return its output.
fn run_command(command: &mut Command) -> Output {
    command.output().expect("run rusneb-parser command")
}

/// Handle one HTTP connection from the mock rusneb client.
fn handle_connection(mut stream: TcpStream, mode: MockMode, requests: Arc<Mutex<Vec<String>>>) {
    let mut first_line = String::new();
    {
        let mut reader = BufReader::new(&mut stream);
        if reader.read_line(&mut first_line).is_err() {
            return;
        }
    }

    let target = first_line
        .split_whitespace()
        .nth(1)
        .unwrap_or("/")
        .to_string();
    let request_number = {
        let mut requests = requests.lock().expect("lock request log");
        requests.push(target.clone());
        requests.len()
    };

    let (status, content_type, body) = route_response(mode, &target, request_number);
    write_response(&mut stream, status, content_type, &body);
}

/// Return the mock response for a request target.
fn route_response(
    mode: MockMode,
    target: &str,
    request_number: usize,
) -> (u16, &'static str, String) {
    match mode {
        MockMode::CompleteRecord => route_complete_record(target),
        MockMode::MissingRecord => route_missing_record(target),
        MockMode::TransientCard500 => route_transient_card_500(target),
        MockMode::ForbiddenCard403 => route_forbidden_card_403(target),
        MockMode::Search403ThenOk => route_search_403_then_ok(target, request_number),
        MockMode::OverflowSearch => route_overflow_search(target),
        MockMode::OverflowSearchCompleteUnion => route_overflow_complete_union_search(target),
        MockMode::OverflowFacetSearch => route_overflow_facet_search(target),
        MockMode::NoYearSearch => route_no_year_search(target),
        MockMode::NoYearKnownStop => route_no_year_known_stop_search(target),
        MockMode::ResumePowerLoss => route_resume_power_loss(target),
        MockMode::SearchTotalDrift => route_search_total_drift(target),
        MockMode::FinalScanOverflow => route_final_scan_overflow(target),
    }
}

/// Return the full-record mock response for a request target.
fn route_complete_record(target: &str) -> (u16, &'static str, String) {
    if target.starts_with("/search/") {
        return (200, "text/html; charset=utf-8", search_html(&[MOCK_ID], 1));
    }
    if target == format!("/catalog/{MOCK_ID}/") {
        return (200, "text/html; charset=utf-8", card_html());
    }
    if target.starts_with("/local/components/exalead/search.page.detail/ajax/marcExport.php") {
        return (200, "application/xml; charset=utf-8", marc_xml());
    }
    if target.starts_with("/rest_api/viewer/access/") {
        return (
            200,
            "application/json; charset=utf-8",
            r#"{"access":true,"token":"secret","viewer":{"token":"secret"}}"#.to_string(),
        );
    }
    (404, "text/plain; charset=utf-8", "not found".to_string())
}

/// Return the missing-record mock response for a request target.
fn route_missing_record(target: &str) -> (u16, &'static str, String) {
    if target.starts_with("/search/") {
        return (
            200,
            "text/html; charset=utf-8",
            search_html(&[MISSING_ID], 1),
        );
    }
    if target == format!("/catalog/{MISSING_ID}/") {
        return (404, "text/plain; charset=utf-8", "missing".to_string());
    }
    (404, "text/plain; charset=utf-8", "not found".to_string())
}

/// Return the transient-card mock response for a request target.
fn route_transient_card_500(target: &str) -> (u16, &'static str, String) {
    if target.starts_with("/search/") {
        return (
            200,
            "text/html; charset=utf-8",
            search_html(&[TRANSIENT_ID], 1),
        );
    }
    if target == format!("/catalog/{TRANSIENT_ID}/") {
        return (500, "text/plain; charset=utf-8", "server error".to_string());
    }
    (404, "text/plain; charset=utf-8", "not found".to_string())
}

/// Return the forbidden-card mock response for a request target.
fn route_forbidden_card_403(target: &str) -> (u16, &'static str, String) {
    if target.starts_with("/search/") {
        return (
            200,
            "text/html; charset=utf-8",
            search_html(&[FORBIDDEN_ID], 1),
        );
    }
    if target == format!("/catalog/{FORBIDDEN_ID}/") {
        return (403, "text/plain; charset=utf-8", "forbidden".to_string());
    }
    (404, "text/plain; charset=utf-8", "not found".to_string())
}

/// Return one search HTTP 403 response, then a normal search response.
fn route_search_403_then_ok(target: &str, request_number: usize) -> (u16, &'static str, String) {
    if target.starts_with("/search/") && request_number == 1 {
        return (403, "text/plain; charset=utf-8", "forbidden".to_string());
    }
    if target.starts_with("/search/") {
        return (200, "text/html; charset=utf-8", search_html(&[MOCK_ID], 1));
    }
    (404, "text/plain; charset=utf-8", "not found".to_string())
}

/// Return the overflow-shard mock response for a request target.
fn route_overflow_search(target: &str) -> (u16, &'static str, String) {
    if !target.starts_with("/search/") {
        return (404, "text/plain; charset=utf-8", "not found".to_string());
    }
    if target.contains("PAGEN_1=2") {
        return (200, "text/html; charset=utf-8", search_html(&[], 12_853));
    }

    let id = if target.contains("by=document_titlesort") && target.contains("order=desc") {
        OVERFLOW_SORTED_ID
    } else {
        OVERFLOW_NORMAL_ID
    };
    (200, "text/html; charset=utf-8", search_html(&[id], 12_853))
}

/// Return overflow shard pages that are incomplete individually but complete as one group.
fn route_overflow_complete_union_search(target: &str) -> (u16, &'static str, String) {
    if !target.starts_with("/search/") {
        return (404, "text/plain; charset=utf-8", "not found".to_string());
    }
    if target.contains("PAGEN_1=2") {
        return (200, "text/html; charset=utf-8", search_html(&[], 2));
    }

    let id = if target.contains("by=document_titlesort") && target.contains("order=desc") {
        OVERFLOW_SORTED_ID
    } else {
        OVERFLOW_NORMAL_ID
    };
    (200, "text/html; charset=utf-8", search_html(&[id], 2))
}

/// Return search pages where generated facet shards are needed to close the coverage gap.
fn route_overflow_facet_search(target: &str) -> (u16, &'static str, String) {
    if target.starts_with("/search/extended/") {
        return (
            200,
            "text/html; charset=utf-8",
            advanced_filter_html(&[
                ("lang", "facet-a"),
                ("lang", "facet-b"),
                ("lang", "facet-c"),
            ]),
        );
    }
    if !target.starts_with("/search/") {
        return (404, "text/plain; charset=utf-8", "not found".to_string());
    }

    if target.contains("lang=facet-a") {
        return facet_search_response(target, OVERFLOW_LANG_A_ID);
    }
    if target.contains("lang=facet-b") {
        return facet_search_response(target, OVERFLOW_LANG_B_ID);
    }
    if target.contains("lang=facet-c") {
        return facet_search_response(target, OVERFLOW_LANG_C_ID);
    }
    if target.contains("PAGEN_1=2") {
        return (200, "text/html; charset=utf-8", search_html(&[], 4));
    }
    (
        200,
        "text/html; charset=utf-8",
        search_html(&[OVERFLOW_NORMAL_ID], 4),
    )
}

/// Return one complete facet shard page pair.
fn facet_search_response(target: &str, id: &str) -> (u16, &'static str, String) {
    if target.contains("PAGEN_1=2") {
        return (200, "text/html; charset=utf-8", search_html(&[], 1));
    }
    (200, "text/html; charset=utf-8", search_html(&[id], 1))
}

/// Return the no-year-prefix mock response for a request target.
fn route_no_year_search(target: &str) -> (u16, &'static str, String) {
    if !target.starts_with("/search/") {
        return (404, "text/plain; charset=utf-8", "not found".to_string());
    }
    if target.contains("by=document_publishyearsort")
        && target.contains("order=asc")
        && !target.contains("publishyear_prev")
        && !target.contains("publishyear_next")
    {
        return (
            200,
            "text/html; charset=utf-8",
            search_html(&[NO_YEAR_ID], 42),
        );
    }
    (200, "text/html; charset=utf-8", search_html(&[], 1))
}

/// Return a year shard followed by a no-year shard that repeats known IDs.
fn route_no_year_known_stop_search(target: &str) -> (u16, &'static str, String) {
    if !target.starts_with("/search/") {
        return (404, "text/plain; charset=utf-8", "not found".to_string());
    }
    if target.contains("by=document_publishyearsort")
        && target.contains("order=asc")
        && !target.contains("publishyear_prev")
        && !target.contains("publishyear_next")
    {
        if target.contains("PAGEN_1=2") {
            return (
                200,
                "text/html; charset=utf-8",
                search_html(&[NO_YEAR_ID], 42),
            );
        }
        return (200, "text/html; charset=utf-8", search_html(&[MOCK_ID], 42));
    }
    if target.contains("publishyear_prev=1911") && target.contains("publishyear_next=1911") {
        if target.contains("PAGEN_1=2") {
            return (200, "text/html; charset=utf-8", search_html(&[], 1));
        }
        return (200, "text/html; charset=utf-8", search_html(&[MOCK_ID], 1));
    }
    (200, "text/html; charset=utf-8", search_html(&[], 1))
}

/// Return overlapping pages and complete records for resume-after-power-loss assertions.
fn route_resume_power_loss(target: &str) -> (u16, &'static str, String) {
    if target.starts_with("/search/") {
        if target.contains("PAGEN_1=2") {
            return (
                200,
                "text/html; charset=utf-8",
                search_html(&[RESUME_ID_B, RESUME_ID_C], 3),
            );
        }
        if target.contains("PAGEN_1=3") {
            return (200, "text/html; charset=utf-8", search_html(&[], 3));
        }
        return (
            200,
            "text/html; charset=utf-8",
            search_html(&[RESUME_ID_A, RESUME_ID_B], 3),
        );
    }

    if [RESUME_ID_A, RESUME_ID_B, RESUME_ID_C]
        .iter()
        .any(|id| target == format!("/catalog/{id}/"))
    {
        return (200, "text/html; charset=utf-8", card_html());
    }
    if target.starts_with("/local/components/exalead/search.page.detail/ajax/marcExport.php") {
        return (200, "application/xml; charset=utf-8", marc_xml());
    }
    if target.starts_with("/rest_api/viewer/access/") {
        return (
            200,
            "application/json; charset=utf-8",
            r#"{"access":true}"#.to_string(),
        );
    }
    (404, "text/plain; charset=utf-8", "not found".to_string())
}

/// Return a search whose total count drops to match the latest non-empty page.
fn route_search_total_drift(target: &str) -> (u16, &'static str, String) {
    if target.starts_with("/search/") {
        if target.contains("PAGEN_1=2") {
            return (
                200,
                "text/html; charset=utf-8",
                search_html(&[TOTAL_DRIFT_ID_B], 2),
            );
        }
        if target.contains("PAGEN_1=3") {
            return (200, "text/html; charset=utf-8", search_html(&[], 2));
        }
        return (
            200,
            "text/html; charset=utf-8",
            search_html(&[TOTAL_DRIFT_ID_A], 3),
        );
    }
    (404, "text/plain; charset=utf-8", "not found".to_string())
}

/// Return a group gap that needs the final self-healing scan to seed more sorted shards.
fn route_final_scan_overflow(target: &str) -> (u16, &'static str, String) {
    if target.starts_with("/search/extended/") {
        return (
            200,
            "text/html; charset=utf-8",
            advanced_filter_html(&[("lang", "facet-final")]),
        );
    }
    if !target.starts_with("/search/") {
        return (404, "text/plain; charset=utf-8", "not found".to_string());
    }
    if target.contains("PAGEN_1=2") {
        return (200, "text/html; charset=utf-8", search_html(&[], 4));
    }
    if target.contains("lang=facet-final") {
        return (
            200,
            "text/html; charset=utf-8",
            search_html(&[FINAL_SCAN_ID_D], 1),
        );
    }
    if target.contains("by=document_titlesort") && target.contains("order=desc") {
        return (
            200,
            "text/html; charset=utf-8",
            search_html(&[FINAL_SCAN_ID_B, FINAL_SCAN_ID_C], 2),
        );
    }
    (
        200,
        "text/html; charset=utf-8",
        search_html(&[FINAL_SCAN_ID_A], 4),
    )
}

/// Write one HTTP/1.1 response.
fn write_response(stream: &mut TcpStream, status: u16, content_type: &str, body: &str) {
    let status_text = match status {
        200 => "OK",
        403 => "Forbidden",
        404 => "Not Found",
        500 => "Internal Server Error",
        _ => "OK",
    };
    write!(
        stream,
        "HTTP/1.1 {status} {status_text}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
    .expect("write mock response");
}

/// Build a rusneb-like search page with result links.
fn search_html(ids: &[&str], total: u64) -> String {
    let links = ids
        .iter()
        .map(|id| {
            format!(
                r#"<a class="search-list__item_link max_height_unset" href="/catalog/{id}/">Mock {id}</a>
                   <a class="search-result__content-main-read-button" href="/catalog/{id}/">Подробнее</a>"#
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    format!("<html><body>Найдено {total} результатов {links}</body></html>")
}

/// Build a minimal advanced-search page containing first-party facet values.
fn advanced_filter_html(values: &[(&str, &str)]) -> String {
    let values = values
        .iter()
        .map(|(field, value)| format!(r#"<div data-id="{field}" data-value="{value}"></div>"#))
        .collect::<Vec<_>>()
        .join("\n");
    format!("<html><body>{values}</body></html>")
}

/// Build a rusneb-like catalog card.
fn card_html() -> String {
    r#"
    <html>
      <head>
        <meta property="og:title" content="Mock Book &amp; Test">
        <meta name="description" content="Mock description">
        <meta property="book:release_date" content="1911">
      </head>
      <body>
        <h1>Mock Book fallback</h1>
        <span itemprop="author">Mock Author</span>
        <div id="toClipBoard">Mock bibliographic description</div>
        <a href="/files/card.pdf">PDF</a>
        <div class="cards-section">
          <h2>Детальная информация</h2>
          <div class="cards-table">
            <div class="cards-table__row">
              <div class="cards-table__left">Каталог</div>
              <div class="cards-table__right"><a href="/search/?c[]=25">Книги</a></div>
            </div>
            <div class="cards-table__row">
              <div class="cards-table__left">Место издания</div>
              <div class="cards-table__right">Тестоград</div>
            </div>
            <div class="cards-table__row">
              <div class="cards-table__left">Издательство</div>
              <div class="cards-table__right">Mock Publisher</div>
            </div>
            <div class="cards-table__row">
              <div class="cards-table__left">Тематика</div>
              <div class="cards-table__right">Тестовая тематика</div>
            </div>
          </div>
        </div>
      </body>
    </html>
    "#
    .to_string()
}

/// Build a small MARC21 XML record with one PDF link and one subject.
fn marc_xml() -> String {
    r#"<?xml version="1.0" encoding="UTF-8"?>
    <marc:collection xmlns:marc="http://www.loc.gov/MARC21/slim">
      <marc:record>
        <marc:leader>01234nam a2200000 i 4500</marc:leader>
        <marc:controlfield tag="001">mock-control</marc:controlfield>
        <marc:datafield tag="650" ind1=" " ind2="0">
          <marc:subfield code="a">Mock subject</marc:subfield>
        </marc:datafield>
        <marc:datafield tag="856" ind1="4" ind2="0">
          <marc:subfield code="u">http://example.test/marc.pdf</marc:subfield>
        </marc:datafield>
      </marc:record>
    </marc:collection>
    "#
    .to_string()
}

/// Assert that a JSON array contains a string.
fn assert_json_array_contains(value: &Value, expected: &str) {
    let values = value.as_array().expect("expected JSON array");
    assert!(
        values.iter().any(|value| value.as_str() == Some(expected)),
        "expected {expected:?} in {values:?}"
    );
}

/// Assert that a JSON value contains a lowercase SHA-256 hex digest.
fn assert_hex_sha256(value: &Value) {
    let digest = value.as_str().expect("expected SHA-256 string");
    assert_eq!(digest.len(), 64);
    assert!(
        digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "invalid SHA-256 hex digest: {digest}"
    );
}

/// Return the durable item status for a catalog ID.
fn item_status(db: &Path, id: &str) -> String {
    let conn = Connection::open(db).expect("open SQLite DB");
    conn.query_row(
        "SELECT status FROM items WHERE id = ?1",
        params![id],
        |row| row.get(0),
    )
    .expect("read item status")
}

/// Return the durable item attempt counter for a catalog ID.
fn item_attempts(db: &Path, id: &str) -> i64 {
    let conn = Connection::open(db).expect("open SQLite DB");
    conn.query_row(
        "SELECT attempts FROM items WHERE id = ?1",
        params![id],
        |row| row.get(0),
    )
    .expect("read item attempts")
}

/// Return the durable item HTTP status for a catalog ID.
fn item_last_http_status(db: &Path, id: &str) -> Option<i64> {
    let conn = Connection::open(db).expect("open SQLite DB");
    conn.query_row(
        "SELECT last_http_status FROM items WHERE id = ?1",
        params![id],
        |row| row.get(0),
    )
    .expect("read item HTTP status")
}

/// Return all durable item IDs in stable order.
fn item_ids(db: &Path) -> Vec<String> {
    query_string_column(db, "SELECT id FROM items ORDER BY id")
}

/// Return all persisted record IDs in stable order.
fn record_ids(db: &Path) -> Vec<String> {
    query_string_column(db, "SELECT id FROM records ORDER BY id")
}

/// Return the status of one search page, independent of its generated search key.
fn search_page_status(db: &Path, page: i64) -> String {
    let conn = Connection::open(db).expect("open SQLite DB");
    conn.query_row(
        "SELECT status FROM search_pages WHERE page = ?1",
        params![page],
        |row| row.get(0),
    )
    .expect("read search page status")
}

/// Clone an existing shard row to simulate an interrupted durable search page.
fn clone_search_page_from_existing_shard(db: &Path, source_page: i64, new_page: i64, status: &str) {
    let conn = Connection::open(db).expect("open SQLite DB");
    let updated = conn
        .execute(
            "INSERT INTO search_pages(search_key, page, params_json, status, updated_at)
             SELECT search_key, ?2, params_json, ?3, updated_at
             FROM search_pages
             WHERE page = ?1",
            params![source_page, new_page, status],
        )
        .expect("clone search page");
    assert_eq!(updated, 1);
}

/// Mark an item as interrupted after it was claimed but before a record was saved.
fn mark_item_in_progress(db: &Path, id: &str) {
    let conn = Connection::open(db).expect("open SQLite DB");
    let updated = conn
        .execute(
            "UPDATE items
             SET status = 'in_progress',
                 attempts = attempts + 1,
                 last_error = NULL,
                 last_http_status = NULL
             WHERE id = ?1",
            params![id],
        )
        .expect("mark item in progress");
    assert_eq!(updated, 1);
}

/// Count items with one durable status.
fn item_status_count(db: &Path, status: &str) -> i64 {
    let conn = Connection::open(db).expect("open SQLite DB");
    conn.query_row(
        "SELECT COUNT(*) FROM items WHERE status = ?1",
        params![status],
        |row| row.get(0),
    )
    .expect("count item status")
}

/// Count search pages with one durable status.
fn search_page_status_count(db: &Path, status: &str) -> i64 {
    let conn = Connection::open(db).expect("open SQLite DB");
    conn.query_row(
        "SELECT COUNT(*) FROM search_pages WHERE status = ?1",
        params![status],
        |row| row.get(0),
    )
    .expect("count search page status")
}

/// Count persisted search result memberships.
fn search_item_count(db: &Path) -> i64 {
    let conn = Connection::open(db).expect("open SQLite DB");
    conn.query_row("SELECT COUNT(*) FROM search_items", [], |row| row.get(0))
        .expect("count search items")
}

/// Remove search memberships to simulate databases created before this table existed.
fn clear_search_items(db: &Path) {
    let conn = Connection::open(db).expect("open SQLite DB");
    conn.execute("DELETE FROM search_items", [])
        .expect("clear search items");
}

/// Return one string column from SQLite in row order.
fn query_string_column(db: &Path, sql: &str) -> Vec<String> {
    let conn = Connection::open(db).expect("open SQLite DB");
    let mut stmt = conn.prepare(sql).expect("prepare string column query");
    let rows = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .expect("query string column");
    rows.map(|row| row.expect("read string row")).collect()
}

/// Count how many catalog card fetches were made for one ID.
fn catalog_request_count(requests: &[String], id: &str) -> usize {
    let expected = format!("/catalog/{id}/");
    requests
        .iter()
        .filter(|request| request.as_str() == expected)
        .count()
}
