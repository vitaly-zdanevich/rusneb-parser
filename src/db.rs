use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use std::path::Path;

#[derive(Debug)]
pub struct SearchPage {
    pub search_key: String,
    pub page: u64,
}

#[derive(Debug)]
pub struct CrawlItem {
    pub id: String,
}

#[derive(Debug)]
pub struct Db {
    conn: Connection,
}

#[derive(Debug, Default)]
pub struct WorkStatusSummary {
    pub pending: u64,
    pub in_progress: u64,
    pub done: u64,
    pub failed: u64,
    pub other: u64,
}

#[derive(Debug, Default)]
pub struct CrawlCompletionSummary {
    pub records: u64,
    pub items: WorkStatusSummary,
    pub search_pages: WorkStatusSummary,
    pub retryable_failed_items: u64,
    pub exhausted_failed_items: u64,
    pub retryable_failed_search_pages: u64,
    pub exhausted_failed_search_pages: u64,
    pub failed_403_items: u64,
    pub failed_403_search_pages: u64,
}

#[derive(Debug, Default)]
pub struct RetryFailedCounts {
    pub items: u64,
    pub search_pages: u64,
}

/// Aggregated search coverage information for all discovered search shards.
#[derive(Debug, Default)]
pub struct CoverageReport {
    pub shards: Vec<CoverageShard>,
}

/// Aggregated coverage counters for one unique search parameter set.
#[derive(Debug)]
pub struct CoverageShard {
    pub search_key: String,
    pub params_json: String,
    pub pages: u64,
    pub done_pages: u64,
    pub pending_pages: u64,
    pub in_progress_pages: u64,
    pub failed_pages: u64,
    pub discovered_results: u64,
    pub reported_total_results: Option<u64>,
    pub max_done_page: Option<u64>,
}

impl CoverageShard {
    /// Return the reported rows that were not reached by the completed pages.
    pub fn missing_results(&self) -> Option<u64> {
        self.reported_total_results
            .map(|total| total.saturating_sub(self.discovered_results))
    }

    /// Return true when this shard still has search-page work not marked done.
    pub fn has_unfinished_pages(&self) -> bool {
        self.pending_pages > 0 || self.in_progress_pages > 0 || self.failed_pages > 0
    }

    /// Return true when completed pages do not cover the reported total.
    pub fn has_coverage_gap(&self) -> bool {
        self.missing_results().is_some_and(|missing| missing > 0)
    }

    /// Return true when the gap looks like rusneb's search result window limit.
    pub fn looks_window_limited(&self, window_limit_results: u64) -> bool {
        self.has_coverage_gap() && self.discovered_results >= window_limit_results
    }
}

impl Db {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating {}", parent.display()))?;
        }

        let conn = Connection::open(path).with_context(|| format!("opening {}", path.display()))?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.busy_timeout(std::time::Duration::from_secs(30))?;

        let db = Self { conn };
        db.migrate()?;
        Ok(db)
    }

    fn migrate(&self) -> Result<()> {
        self.conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS search_pages (
                search_key TEXT NOT NULL,
                page INTEGER NOT NULL,
                params_json TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                attempts INTEGER NOT NULL DEFAULT 0,
                result_count INTEGER,
                total_results INTEGER,
                last_error TEXT,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY (search_key, page)
            );

            CREATE INDEX IF NOT EXISTS idx_search_pages_next
                ON search_pages(search_key, status, page);

            CREATE TABLE IF NOT EXISTS items (
                id TEXT PRIMARY KEY,
                first_seen_search_key TEXT,
                first_seen_page INTEGER,
                status TEXT NOT NULL DEFAULT 'pending',
                attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                last_http_status INTEGER,
                updated_at INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_items_next
                ON items(status, updated_at);

            CREATE TABLE IF NOT EXISTS records (
                id TEXT PRIMARY KEY,
                json TEXT NOT NULL,
                fetched_at INTEGER NOT NULL
            );

            CREATE VIEW IF NOT EXISTS records_flat AS
            SELECT
                id,
                json_extract(json, '$.metadata.title') AS title,
                json_extract(json, '$.metadata.year') AS year,
                json_extract(json, '$.metadata.authors') AS authors_json,
                json_extract(json, '$.metadata.detail_map."Каталог"[0]') AS catalog,
                json_extract(json, '$.metadata.detail_map."Библиотека"[0]') AS library,
                json_extract(json, '$.metadata.detail_map."Язык"[0]') AS language,
                json_extract(json, '$.metadata.bibliographic_description') AS bibliographic_description,
                json_extract(json, '$.metadata.description') AS description,
                json_extract(json, '$.metadata.topics') AS topics_json,
                json_extract(json, '$.metadata.pdf_links') AS pdf_links_json,
                json_array_length(json_extract(json, '$.metadata.pdf_links')) AS pdf_count,
                json_extract(json, '$.url') AS url,
                fetched_at
            FROM records;
            "#,
        )?;
        Ok(())
    }

    pub fn reset_interrupted_work(&self) -> Result<()> {
        let now = now_unix();
        self.conn.execute(
            "UPDATE search_pages SET status = 'pending', updated_at = ?1 WHERE status = 'in_progress'",
            params![now],
        )?;
        self.conn.execute(
            "UPDATE items SET status = 'pending', updated_at = ?1 WHERE status = 'in_progress'",
            params![now],
        )?;
        Ok(())
    }

    pub fn set_meta(&self, key: &str, value: &str) -> Result<()> {
        self.conn.execute(
            "INSERT INTO meta(key, value) VALUES (?1, ?2)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![key, value],
        )?;
        Ok(())
    }

    pub fn seed_search_page(&self, search_key: &str, params_json: &str, page: u64) -> Result<()> {
        self.conn.execute(
            "INSERT OR IGNORE INTO search_pages(search_key, page, params_json, updated_at)
             VALUES (?1, ?2, ?3, ?4)",
            params![search_key, page as i64, params_json, now_unix()],
        )?;
        Ok(())
    }

    pub fn next_search_page(
        &self,
        search_key: &str,
        max_attempts: u32,
    ) -> Result<Option<SearchPage>> {
        let mut stmt = self.conn.prepare(
            "SELECT search_key, page
             FROM search_pages
             WHERE search_key = ?1
               AND status IN ('pending', 'failed')
               AND attempts < ?2
             ORDER BY page
             LIMIT 1",
        )?;

        let page = stmt
            .query_row(params![search_key, max_attempts as i64], |row| {
                Ok(SearchPage {
                    search_key: row.get(0)?,
                    page: row.get::<_, i64>(1)? as u64,
                })
            })
            .optional()?;

        Ok(page)
    }

    pub fn mark_search_page_started(&self, search_key: &str, page: u64) -> Result<()> {
        self.conn.execute(
            "UPDATE search_pages
             SET status = 'in_progress', attempts = attempts + 1, last_error = NULL, updated_at = ?3
             WHERE search_key = ?1 AND page = ?2",
            params![search_key, page as i64, now_unix()],
        )?;
        Ok(())
    }

    pub fn complete_search_page(
        &mut self,
        search_key: &str,
        page: u64,
        result_count: usize,
        total_results: Option<u64>,
        params_json: &str,
        next_page: Option<u64>,
    ) -> Result<()> {
        let tx = self.conn.transaction()?;
        tx.execute(
            "UPDATE search_pages
             SET status = 'done',
                 result_count = ?3,
                 total_results = ?4,
                 last_error = NULL,
                 updated_at = ?5
             WHERE search_key = ?1 AND page = ?2",
            params![
                search_key,
                page as i64,
                result_count as i64,
                total_results.map(|n| n as i64),
                now_unix()
            ],
        )?;
        if let Some(next_page) = next_page {
            tx.execute(
                "INSERT OR IGNORE INTO search_pages(search_key, page, params_json, updated_at)
                 VALUES (?1, ?2, ?3, ?4)",
                params![search_key, next_page as i64, params_json, now_unix()],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn fail_search_page(&self, search_key: &str, page: u64, error: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE search_pages
             SET status = 'failed', last_error = ?3, updated_at = ?4
             WHERE search_key = ?1 AND page = ?2",
            params![search_key, page as i64, error, now_unix()],
        )?;
        Ok(())
    }

    pub fn defer_search_page_after_transient_error(
        &self,
        search_key: &str,
        page: u64,
        error: &str,
    ) -> Result<()> {
        self.conn.execute(
            "UPDATE search_pages
             SET status = 'pending',
                 attempts = CASE WHEN attempts > 0 THEN attempts - 1 ELSE 0 END,
                 last_error = ?3,
                 updated_at = ?4
             WHERE search_key = ?1 AND page = ?2",
            params![search_key, page as i64, error, now_unix()],
        )?;
        Ok(())
    }

    pub fn enqueue_items(
        &mut self,
        search_key: Option<&str>,
        page: Option<u64>,
        ids: &[String],
    ) -> Result<usize> {
        let tx = self.conn.transaction()?;
        let mut inserted = 0usize;
        for id in ids {
            inserted += tx.execute(
                "INSERT OR IGNORE INTO items(
                    id, first_seen_search_key, first_seen_page, updated_at
                 )
                 VALUES (?1, ?2, ?3, ?4)",
                params![id, search_key, page.map(|n| n as i64), now_unix()],
            )?;
        }
        tx.commit()?;
        Ok(inserted)
    }

    pub fn claim_next_item(&mut self, max_attempts: u32) -> Result<Option<CrawlItem>> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let id = tx
            .query_row(
                "SELECT id
                 FROM items
                 WHERE status IN ('pending', 'failed')
                   AND attempts < ?1
                 ORDER BY updated_at, id
                 LIMIT 1",
                params![max_attempts as i64],
                |row| row.get::<_, String>(0),
            )
            .optional()?;

        let Some(id) = id else {
            tx.commit()?;
            return Ok(None);
        };

        tx.execute(
            "UPDATE items
             SET status = 'in_progress',
                 attempts = attempts + 1,
                 last_error = NULL,
                 last_http_status = NULL,
                 updated_at = ?3
             WHERE id = ?1
               AND status IN ('pending', 'failed')
               AND attempts < ?2",
            params![id, max_attempts as i64, now_unix()],
        )?;
        tx.commit()?;

        Ok(Some(CrawlItem { id }))
    }

    pub fn save_record(&mut self, id: &str, record_json: &str, fetched_at: i64) -> Result<()> {
        let tx = self.conn.transaction()?;
        tx.execute(
            "INSERT INTO records(id, json, fetched_at)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(id) DO UPDATE
             SET json = excluded.json, fetched_at = excluded.fetched_at",
            params![id, record_json, fetched_at],
        )?;
        tx.execute(
            "UPDATE items
             SET status = 'done',
                 last_error = NULL,
                 last_http_status = NULL,
                 updated_at = ?2
             WHERE id = ?1",
            params![id, now_unix()],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn fail_item(&self, id: &str, error: &str, http_status: Option<u16>) -> Result<()> {
        self.conn.execute(
            "UPDATE items
             SET status = 'failed',
                 last_error = ?2,
                 last_http_status = ?3,
                 updated_at = ?4
             WHERE id = ?1",
            params![
                id,
                error,
                http_status.map(|status| status as i64),
                now_unix()
            ],
        )?;
        Ok(())
    }

    pub fn defer_item_after_transient_error(
        &self,
        id: &str,
        error: &str,
        http_status: Option<u16>,
    ) -> Result<()> {
        self.conn.execute(
            "UPDATE items
             SET status = 'pending',
                 attempts = CASE WHEN attempts > 0 THEN attempts - 1 ELSE 0 END,
                 last_error = ?2,
                 last_http_status = ?3,
                 updated_at = ?4
             WHERE id = ?1",
            params![
                id,
                error,
                http_status.map(|status| status as i64),
                now_unix()
            ],
        )?;
        Ok(())
    }

    pub fn count_records(&self) -> Result<u64> {
        Ok(self
            .conn
            .query_row("SELECT COUNT(*) FROM records", [], |row| {
                row.get::<_, i64>(0)
            })? as u64)
    }

    pub fn count_item_backlog(&self, max_attempts: u32) -> Result<u64> {
        Ok(self.conn.query_row(
            "SELECT COUNT(*)
             FROM items
             WHERE status IN ('pending', 'in_progress')
                OR (status = 'failed' AND attempts < ?1)",
            params![max_attempts as i64],
            |row| row.get::<_, i64>(0),
        )? as u64)
    }

    pub fn status_counts(&self, table: &str) -> Result<Vec<(String, u64)>> {
        if table != "items" && table != "search_pages" {
            anyhow::bail!("unsupported table for status counts: {table}");
        }
        let mut stmt = self.conn.prepare(&format!(
            "SELECT status, COUNT(*) FROM {table} GROUP BY status"
        ))?;
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as u64))
        })?;

        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    pub fn crawl_completion_summary(&self, max_attempts: u32) -> Result<CrawlCompletionSummary> {
        Ok(CrawlCompletionSummary {
            records: self.count_records()?,
            items: self.work_status_summary("items")?,
            search_pages: self.work_status_summary("search_pages")?,
            retryable_failed_items: self.count_failed_attempts("items", max_attempts, true)?,
            exhausted_failed_items: self.count_failed_attempts("items", max_attempts, false)?,
            retryable_failed_search_pages: self.count_failed_attempts(
                "search_pages",
                max_attempts,
                true,
            )?,
            exhausted_failed_search_pages: self.count_failed_attempts(
                "search_pages",
                max_attempts,
                false,
            )?,
            failed_403_items: self.conn.query_row(
                "SELECT COUNT(*)
                 FROM items
                 WHERE status = 'failed'
                   AND (last_http_status = 403 OR last_error LIKE '%HTTP 403%')",
                [],
                |row| row.get::<_, i64>(0),
            )? as u64,
            failed_403_search_pages: self.conn.query_row(
                "SELECT COUNT(*)
                 FROM search_pages
                 WHERE status = 'failed'
                   AND last_error LIKE '%HTTP 403%'",
                [],
                |row| row.get::<_, i64>(0),
            )? as u64,
        })
    }

    pub fn retry_failed(&self, http_status: Option<u16>) -> Result<RetryFailedCounts> {
        let now = now_unix();
        let items = match http_status {
            Some(status) => {
                let pattern = format!("%HTTP {status}%");
                self.conn.execute(
                    "UPDATE items
                     SET status = 'pending',
                         attempts = 0,
                         last_error = NULL,
                         last_http_status = NULL,
                         updated_at = ?3
                     WHERE status = 'failed'
                       AND (last_http_status = ?1 OR last_error LIKE ?2)",
                    params![status as i64, pattern, now],
                )?
            }
            None => self.conn.execute(
                "UPDATE items
                 SET status = 'pending',
                     attempts = 0,
                     last_error = NULL,
                     last_http_status = NULL,
                     updated_at = ?1
                 WHERE status = 'failed'",
                params![now],
            )?,
        };
        let search_pages = match http_status {
            Some(status) => {
                let pattern = format!("%HTTP {status}%");
                self.conn.execute(
                    "UPDATE search_pages
                     SET status = 'pending',
                         attempts = 0,
                         last_error = NULL,
                         updated_at = ?2
                     WHERE status = 'failed'
                       AND last_error LIKE ?1",
                    params![pattern, now],
                )?
            }
            None => self.conn.execute(
                "UPDATE search_pages
                 SET status = 'pending',
                     attempts = 0,
                     last_error = NULL,
                     updated_at = ?1
                 WHERE status = 'failed'",
                params![now],
            )?,
        };

        Ok(RetryFailedCounts {
            items: items as u64,
            search_pages: search_pages as u64,
        })
    }

    /// Aggregate completed search pages and reported totals by search shard.
    pub fn coverage_report(&self) -> Result<CoverageReport> {
        let mut stmt = self.conn.prepare(
            "SELECT
                 search_key,
                 MIN(params_json),
                 COUNT(*),
                 SUM(CASE WHEN status = 'done' THEN 1 ELSE 0 END),
                 SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END),
                 SUM(CASE WHEN status = 'in_progress' THEN 1 ELSE 0 END),
                 SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END),
                 SUM(CASE WHEN status = 'done' THEN COALESCE(result_count, 0) ELSE 0 END),
                 MAX(total_results),
                 MAX(CASE WHEN status = 'done' THEN page ELSE NULL END)
             FROM search_pages
             GROUP BY search_key
             ORDER BY search_key",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(CoverageShard {
                search_key: row.get(0)?,
                params_json: row.get(1)?,
                pages: row.get::<_, i64>(2)? as u64,
                done_pages: row.get::<_, i64>(3)? as u64,
                pending_pages: row.get::<_, i64>(4)? as u64,
                in_progress_pages: row.get::<_, i64>(5)? as u64,
                failed_pages: row.get::<_, i64>(6)? as u64,
                discovered_results: row.get::<_, i64>(7)? as u64,
                reported_total_results: row.get::<_, Option<i64>>(8)?.map(|value| value as u64),
                max_done_page: row.get::<_, Option<i64>>(9)?.map(|value| value as u64),
            })
        })?;

        let mut shards = Vec::new();
        for row in rows {
            shards.push(row?);
        }
        Ok(CoverageReport { shards })
    }

    fn work_status_summary(&self, table: &str) -> Result<WorkStatusSummary> {
        let mut summary = WorkStatusSummary::default();
        for (status, count) in self.status_counts(table)? {
            match status.as_str() {
                "pending" => summary.pending = count,
                "in_progress" => summary.in_progress = count,
                "done" => summary.done = count,
                "failed" => summary.failed = count,
                _ => summary.other += count,
            }
        }
        Ok(summary)
    }

    fn count_failed_attempts(
        &self,
        table: &str,
        max_attempts: u32,
        retryable: bool,
    ) -> Result<u64> {
        if table != "items" && table != "search_pages" {
            anyhow::bail!("unsupported table for failed attempt count: {table}");
        }
        let op = if retryable { "<" } else { ">=" };
        Ok(self.conn.query_row(
            &format!(
                "SELECT COUNT(*)
                 FROM {table}
                 WHERE status = 'failed'
                   AND attempts {op} ?1"
            ),
            params![max_attempts as i64],
            |row| row.get::<_, i64>(0),
        )? as u64)
    }

    pub fn for_each_record<F>(&self, mut f: F) -> Result<usize>
    where
        F: FnMut(&str, &str) -> Result<()>,
    {
        let mut stmt = self
            .conn
            .prepare("SELECT id, json FROM records ORDER BY id")?;
        let mut rows = stmt.query([])?;
        let mut count = 0usize;
        while let Some(row) = rows.next()? {
            let id: String = row.get(0)?;
            let json: String = row.get(1)?;
            f(&id, &json)?;
            count += 1;
        }
        Ok(count)
    }
}

pub fn now_unix() -> i64 {
    chrono::Utc::now().timestamp()
}
