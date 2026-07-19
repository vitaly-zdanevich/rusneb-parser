use crate::ExportManifestArgs;
use crate::db::{self, Db};
use anyhow::{Context, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::process::Command as ProcessCommand;

/// Export a JSON manifest that describes the SQLite crawl state and dataset files.
pub fn export_manifest(db: &Db, args: &ExportManifestArgs) -> Result<()> {
    let manifest_started_at = now_rfc3339();
    let summary = db.crawl_completion_summary(args.max_attempts)?;
    let state_time_bounds = db.state_time_bounds()?;
    let output_files = args
        .files
        .iter()
        .map(|path| output_file_manifest(path))
        .collect::<Result<Vec<_>>>()?;
    let failed_items = FailedItemsManifest {
        total: summary.items.failed,
        by_http_status: db
            .failed_item_http_status_counts()?
            .into_iter()
            .map(FailedHttpStatusCountManifest::from)
            .collect(),
        sample: db
            .failed_item_sample(args.failed_item_sample)?
            .into_iter()
            .map(FailedItemSampleManifest::from)
            .collect(),
    };
    let manifest_finished_at = now_rfc3339();

    let manifest = DatasetManifest {
        schema_version: 1,
        dataset_name: args.dataset_name.clone(),
        manifest_started_at,
        manifest_finished_at,
        tool: ToolManifest {
            name: env!("CARGO_PKG_NAME").to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            git_commit: git_output(&["rev-parse", "HEAD"]),
            git_describe: git_output(&["describe", "--tags", "--dirty", "--always"]),
            git_dirty: git_dirty(),
        },
        commands: CommandsManifest {
            export_manifest: std::env::args_os()
                .map(|arg| arg.to_string_lossy().into_owned())
                .collect(),
            crawl: args.crawl_command.clone(),
        },
        sqlite: SqliteManifest {
            path: path_string(&args.common.db),
            records: summary.records,
            items: WorkStatusManifest::from(&summary.items),
            search_pages: WorkStatusManifest::from(&summary.search_pages),
            retryable_failed_items: summary.retryable_failed_items,
            exhausted_failed_items: summary.exhausted_failed_items,
            retryable_failed_search_pages: summary.retryable_failed_search_pages,
            exhausted_failed_search_pages: summary.exhausted_failed_search_pages,
            failed_403_items: summary.failed_403_items,
            failed_403_search_pages: summary.failed_403_search_pages,
            time_range: StateTimeRangeManifest::from(state_time_bounds),
        },
        failed_items,
        outputs: output_files,
    };

    write_manifest_atomic(&args.output, &manifest)
}

#[derive(Debug, Serialize)]
struct DatasetManifest {
    schema_version: u32,
    dataset_name: String,
    manifest_started_at: String,
    manifest_finished_at: String,
    tool: ToolManifest,
    commands: CommandsManifest,
    sqlite: SqliteManifest,
    failed_items: FailedItemsManifest,
    outputs: Vec<OutputFileManifest>,
}

#[derive(Debug, Serialize)]
struct ToolManifest {
    name: String,
    version: String,
    git_commit: Option<String>,
    git_describe: Option<String>,
    git_dirty: Option<bool>,
}

#[derive(Debug, Serialize)]
struct CommandsManifest {
    export_manifest: Vec<String>,
    crawl: Option<String>,
}

#[derive(Debug, Serialize)]
struct SqliteManifest {
    path: String,
    records: u64,
    items: WorkStatusManifest,
    search_pages: WorkStatusManifest,
    retryable_failed_items: u64,
    exhausted_failed_items: u64,
    retryable_failed_search_pages: u64,
    exhausted_failed_search_pages: u64,
    failed_403_items: u64,
    failed_403_search_pages: u64,
    time_range: StateTimeRangeManifest,
}

#[derive(Debug, Serialize)]
struct WorkStatusManifest {
    pending: u64,
    in_progress: u64,
    done: u64,
    missing: u64,
    failed: u64,
    other: u64,
}

impl From<&db::WorkStatusSummary> for WorkStatusManifest {
    fn from(summary: &db::WorkStatusSummary) -> Self {
        Self {
            pending: summary.pending,
            in_progress: summary.in_progress,
            done: summary.done,
            missing: summary.missing,
            failed: summary.failed,
            other: summary.other,
        }
    }
}

#[derive(Debug, Serialize)]
struct StateTimeRangeManifest {
    started_at_unix: Option<i64>,
    started_at: Option<String>,
    finished_at_unix: Option<i64>,
    finished_at: Option<String>,
}

impl From<db::StateTimeBounds> for StateTimeRangeManifest {
    fn from(bounds: db::StateTimeBounds) -> Self {
        Self {
            started_at_unix: bounds.started_at_unix,
            started_at: bounds.started_at_unix.and_then(unix_to_rfc3339),
            finished_at_unix: bounds.finished_at_unix,
            finished_at: bounds.finished_at_unix.and_then(unix_to_rfc3339),
        }
    }
}

#[derive(Debug, Serialize)]
struct FailedItemsManifest {
    total: u64,
    by_http_status: Vec<FailedHttpStatusCountManifest>,
    sample: Vec<FailedItemSampleManifest>,
}

#[derive(Debug, Serialize)]
struct FailedHttpStatusCountManifest {
    http_status: Option<u16>,
    count: u64,
}

impl From<db::FailedHttpStatusCount> for FailedHttpStatusCountManifest {
    fn from(count: db::FailedHttpStatusCount) -> Self {
        Self {
            http_status: count.http_status,
            count: count.count,
        }
    }
}

#[derive(Debug, Serialize)]
struct FailedItemSampleManifest {
    id: String,
    attempts: u32,
    last_http_status: Option<u16>,
    last_error: Option<String>,
    updated_at_unix: i64,
    updated_at: Option<String>,
}

impl From<db::FailedItemSample> for FailedItemSampleManifest {
    fn from(item: db::FailedItemSample) -> Self {
        Self {
            id: item.id,
            attempts: item.attempts,
            last_http_status: item.last_http_status,
            last_error: item.last_error,
            updated_at_unix: item.updated_at_unix,
            updated_at: unix_to_rfc3339(item.updated_at_unix),
        }
    }
}

#[derive(Debug, Serialize)]
struct OutputFileManifest {
    path: String,
    bytes: u64,
    sha256: String,
}

fn output_file_manifest(path: &Path) -> Result<OutputFileManifest> {
    let metadata =
        std::fs::metadata(path).with_context(|| format!("reading {}", path.display()))?;
    if !metadata.is_file() {
        anyhow::bail!(
            "manifest output file is not a regular file: {}",
            path.display()
        );
    }

    Ok(OutputFileManifest {
        path: path_string(path),
        bytes: metadata.len(),
        sha256: sha256_file(path)?,
    })
}

fn sha256_file(path: &Path) -> Result<String> {
    let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 1024 * 1024];

    loop {
        let read = reader
            .read(&mut buffer)
            .with_context(|| format!("reading {}", path.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }

    Ok(hex_lower(&hasher.finalize()))
}

fn write_manifest_atomic(output: &Path, manifest: &DatasetManifest) -> Result<()> {
    create_parent_dir(output)?;
    let tmp = tmp_path(output);
    let file = File::create(&tmp).with_context(|| format!("creating {}", tmp.display()))?;
    let mut writer = BufWriter::new(file);
    serde_json::to_writer_pretty(&mut writer, manifest)?;
    writer.write_all(b"\n")?;
    let file = writer.into_inner()?;
    file.sync_all()?;
    std::fs::rename(&tmp, output)
        .with_context(|| format!("renaming {} to {}", tmp.display(), output.display()))?;
    Ok(())
}

fn create_parent_dir(output: &Path) -> Result<()> {
    if let Some(parent) = output.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    Ok(())
}

fn tmp_path(output: &Path) -> PathBuf {
    let file_name = output
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("manifest");
    output.with_file_name(format!(".{file_name}.tmp"))
}

fn now_rfc3339() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn unix_to_rfc3339(timestamp: i64) -> Option<String> {
    DateTime::<Utc>::from_timestamp(timestamp, 0)
        .map(|time| time.to_rfc3339_opts(SecondsFormat::Secs, true))
}

fn git_output(args: &[&str]) -> Option<String> {
    ProcessCommand::new("git")
        .args(args)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string())
        .filter(|value| !value.is_empty())
}

fn git_exit_code(args: &[&str]) -> Option<i32> {
    ProcessCommand::new("git")
        .args(args)
        .status()
        .ok()
        .and_then(|status| status.code())
}

fn git_dirty() -> Option<bool> {
    let worktree = git_exit_code(&["diff", "--quiet", "--ignore-submodules", "HEAD", "--"])?;
    let index = git_exit_code(&["diff", "--cached", "--quiet", "--ignore-submodules"])?;
    match (worktree, index) {
        (0 | 1, 0 | 1) => Some(worktree != 0 || index != 0),
        _ => None,
    }
}

fn path_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}
