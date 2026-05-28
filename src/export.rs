use crate::db::Db;
use anyhow::{Context, Result};
use flate2::Compression;
use flate2::write::GzEncoder;
use serde_json::Value;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use xz2::write::XzEncoder;

pub fn export_jsonl(db: &Db, output: &Path) -> Result<usize> {
    match jsonl_compression(output) {
        JsonlCompression::Gzip => export_jsonl_gz(db, output),
        JsonlCompression::Xz => export_jsonl_xz(db, output),
        JsonlCompression::Plain => export_jsonl_plain(db, output),
    }
}

fn export_jsonl_gz(db: &Db, output: &Path) -> Result<usize> {
    create_parent_dir(output)?;
    let tmp = tmp_path(output);
    let file = File::create(&tmp).with_context(|| format!("creating {}", tmp.display()))?;
    let encoder = GzEncoder::new(file, Compression::default());
    let mut writer = BufWriter::new(encoder);

    let count = write_jsonl_records(db, &mut writer)?;
    let encoder = writer.into_inner()?;
    let file = encoder.finish()?;
    file.sync_all()?;
    std::fs::rename(&tmp, output)
        .with_context(|| format!("renaming {} to {}", tmp.display(), output.display()))?;
    Ok(count)
}

fn export_jsonl_xz(db: &Db, output: &Path) -> Result<usize> {
    create_parent_dir(output)?;
    let tmp = tmp_path(output);
    let file = File::create(&tmp).with_context(|| format!("creating {}", tmp.display()))?;
    let encoder = XzEncoder::new(file, 6);
    let mut writer = BufWriter::new(encoder);

    let count = write_jsonl_records(db, &mut writer)?;
    let encoder = writer
        .into_inner()
        .map_err(|error| anyhow::anyhow!("flushing xz writer: {}", error.error()))?;
    let file = encoder.finish()?;
    file.sync_all()?;
    std::fs::rename(&tmp, output)
        .with_context(|| format!("renaming {} to {}", tmp.display(), output.display()))?;
    Ok(count)
}

fn export_jsonl_plain(db: &Db, output: &Path) -> Result<usize> {
    create_parent_dir(output)?;
    let tmp = tmp_path(output);
    let file = File::create(&tmp).with_context(|| format!("creating {}", tmp.display()))?;
    let mut writer = BufWriter::new(file);

    let count = write_jsonl_records(db, &mut writer)?;
    let file = writer.into_inner()?;
    file.sync_all()?;
    std::fs::rename(&tmp, output)
        .with_context(|| format!("renaming {} to {}", tmp.display(), output.display()))?;
    Ok(count)
}

fn write_jsonl_records<W: Write>(db: &Db, writer: &mut W) -> Result<usize> {
    db.for_each_record(|_, json| {
        writer.write_all(json.as_bytes())?;
        writer.write_all(b"\n")?;
        Ok(())
    })
}

enum JsonlCompression {
    Gzip,
    Xz,
    Plain,
}

fn jsonl_compression(output: &Path) -> JsonlCompression {
    match output.extension().and_then(|ext| ext.to_str()) {
        Some("gz") => JsonlCompression::Gzip,
        Some("xz") => JsonlCompression::Xz,
        _ => JsonlCompression::Plain,
    }
}

#[cfg(feature = "parquet-export")]
pub fn export_parquet(db: &Db, output: &Path, batch_size: usize) -> Result<usize> {
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use std::sync::Arc;

    create_parent_dir(output)?;
    let tmp = tmp_path(output);
    let file = File::create(&tmp).with_context(|| format!("creating {}", tmp.display()))?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("title", DataType::Utf8, true),
        Field::new("authors_json", DataType::Utf8, true),
        Field::new("year", DataType::Utf8, true),
        Field::new("catalog", DataType::Utf8, true),
        Field::new("language", DataType::Utf8, true),
        Field::new("publication_place", DataType::Utf8, true),
        Field::new("publisher", DataType::Utf8, true),
        Field::new("pdf_links_json", DataType::Utf8, true),
        Field::new("detail_json", DataType::Utf8, true),
        Field::new("marc_xml", DataType::Utf8, true),
        Field::new("record_json", DataType::Utf8, false),
    ]));
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;

    let mut rows = FlatRows::default();
    let mut count = 0usize;
    db.for_each_record(|id, json| {
        rows.push(id, json)?;
        count += 1;
        if rows.len() >= batch_size.max(1) {
            write_batch(&mut writer, schema.clone(), &mut rows)?;
        }
        Ok(())
    })?;
    if rows.len() > 0 {
        write_batch(&mut writer, schema, &mut rows)?;
    }

    writer.close()?;
    std::fs::rename(&tmp, output)
        .with_context(|| format!("renaming {} to {}", tmp.display(), output.display()))?;
    Ok(count)
}

#[cfg(not(feature = "parquet-export"))]
pub fn export_parquet(_db: &Db, _output: &Path, _batch_size: usize) -> Result<usize> {
    anyhow::bail!("this binary was built without the parquet-export feature");
}

#[cfg(feature = "parquet-export")]
#[derive(Default)]
struct FlatRows {
    id: Vec<Option<String>>,
    title: Vec<Option<String>>,
    authors_json: Vec<Option<String>>,
    year: Vec<Option<String>>,
    catalog: Vec<Option<String>>,
    language: Vec<Option<String>>,
    publication_place: Vec<Option<String>>,
    publisher: Vec<Option<String>>,
    pdf_links_json: Vec<Option<String>>,
    detail_json: Vec<Option<String>>,
    marc_xml: Vec<Option<String>>,
    record_json: Vec<Option<String>>,
}

#[cfg(feature = "parquet-export")]
impl FlatRows {
    fn len(&self) -> usize {
        self.id.len()
    }

    fn push(&mut self, id: &str, json: &str) -> Result<()> {
        let value: Value = serde_json::from_str(json)?;
        self.id.push(Some(id.to_string()));
        self.title
            .push(json_pointer_string(&value, "/metadata/title"));
        self.authors_json
            .push(json_pointer_compact(&value, "/metadata/authors"));
        self.year
            .push(json_pointer_string(&value, "/metadata/year"));
        self.catalog.push(first_detail(&value, "Каталог"));
        self.language.push(first_detail(&value, "Язык"));
        self.publication_place.push(
            first_detail(&value, "Место издания")
                .or_else(|| first_detail(&value, "Место публикации")),
        );
        self.publisher.push(first_detail(&value, "Издательство"));
        self.pdf_links_json
            .push(json_pointer_compact(&value, "/metadata/pdf_links"));
        self.detail_json
            .push(json_pointer_compact(&value, "/metadata/detail_map"));
        self.marc_xml
            .push(json_pointer_string(&value, "/marc21/raw_xml"));
        self.record_json.push(Some(json.to_string()));
        Ok(())
    }

    fn clear(&mut self) {
        *self = Self::default();
    }
}

#[cfg(feature = "parquet-export")]
fn write_batch(
    writer: &mut parquet::arrow::ArrowWriter<File>,
    schema: std::sync::Arc<arrow_schema::Schema>,
    rows: &mut FlatRows,
) -> Result<()> {
    use arrow_array::{ArrayRef, RecordBatch, StringArray};
    use std::sync::Arc;

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(std::mem::take(&mut rows.id))),
        Arc::new(StringArray::from(std::mem::take(&mut rows.title))),
        Arc::new(StringArray::from(std::mem::take(&mut rows.authors_json))),
        Arc::new(StringArray::from(std::mem::take(&mut rows.year))),
        Arc::new(StringArray::from(std::mem::take(&mut rows.catalog))),
        Arc::new(StringArray::from(std::mem::take(&mut rows.language))),
        Arc::new(StringArray::from(std::mem::take(
            &mut rows.publication_place,
        ))),
        Arc::new(StringArray::from(std::mem::take(&mut rows.publisher))),
        Arc::new(StringArray::from(std::mem::take(&mut rows.pdf_links_json))),
        Arc::new(StringArray::from(std::mem::take(&mut rows.detail_json))),
        Arc::new(StringArray::from(std::mem::take(&mut rows.marc_xml))),
        Arc::new(StringArray::from(std::mem::take(&mut rows.record_json))),
    ];
    let batch = RecordBatch::try_new(schema, arrays)?;
    writer.write(&batch)?;
    rows.clear();
    Ok(())
}

#[cfg(feature = "parquet-export")]
fn json_pointer_string(value: &Value, pointer: &str) -> Option<String> {
    value
        .pointer(pointer)
        .and_then(Value::as_str)
        .map(str::to_string)
        .filter(|value| !value.is_empty())
}

#[cfg(feature = "parquet-export")]
fn json_pointer_compact(value: &Value, pointer: &str) -> Option<String> {
    value.pointer(pointer).map(|value| value.to_string())
}

#[cfg(feature = "parquet-export")]
fn first_detail(value: &Value, key: &str) -> Option<String> {
    value
        .pointer("/metadata/detail_map")
        .and_then(|map| map.get(key))
        .and_then(Value::as_array)
        .and_then(|values| values.first())
        .and_then(Value::as_str)
        .map(str::to_string)
        .filter(|value| !value.is_empty())
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
        .unwrap_or("export");
    output.with_file_name(format!(".{file_name}.tmp"))
}
