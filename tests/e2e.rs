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
const OVERFLOW_NORMAL_ID: &str = "overflow-normal";
const OVERFLOW_SORTED_ID: &str = "overflow-sorted";

/// End-to-end mock behavior used by the local rusneb HTTP server.
#[derive(Clone, Copy)]
enum MockMode {
    /// Serve one complete record with search, card, MARC XML, and viewer JSON endpoints.
    CompleteRecord,
    /// Serve two search result shards: the default year shard and one sorted overflow shard.
    OverflowSearch,
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
    requests
        .lock()
        .expect("lock request log")
        .push(target.clone());

    let (status, content_type, body) = route_response(mode, &target);
    write_response(&mut stream, status, content_type, &body);
}

/// Return the mock response for a request target.
fn route_response(mode: MockMode, target: &str) -> (u16, &'static str, String) {
    match mode {
        MockMode::CompleteRecord => route_complete_record(target),
        MockMode::OverflowSearch => route_overflow_search(target),
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

/// Return the overflow-shard mock response for a request target.
fn route_overflow_search(target: &str) -> (u16, &'static str, String) {
    if !target.starts_with("/search/") {
        return (404, "text/plain; charset=utf-8", "not found".to_string());
    }

    let id = if target.contains("by=document_titlesort") && target.contains("order=desc") {
        OVERFLOW_SORTED_ID
    } else {
        OVERFLOW_NORMAL_ID
    };
    (200, "text/html; charset=utf-8", search_html(&[id], 12_853))
}

/// Write one HTTP/1.1 response.
fn write_response(stream: &mut TcpStream, status: u16, content_type: &str, body: &str) {
    let status_text = match status {
        200 => "OK",
        404 => "Not Found",
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
