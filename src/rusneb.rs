use crate::db::now_unix;
use crate::model::{
    CardMetadata, DetailField, Link, MarcControlField, MarcDataField, MarcSubfield, MarcXmlRecord,
    RusnebRecord, SourceUrls,
};
use anyhow::{Context, Result};
use html_escape::decode_html_entities;
use regex::Regex;
use reqwest::{Proxy, blocking::Client};
use scraper::{ElementRef, Html, Selector};
use serde::Serialize;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::thread;
use std::time::{Duration, Instant};
use url::Url;

#[derive(Debug, Clone, Serialize)]
pub struct SearchParams {
    pub base_url: String,
    pub query: String,
    pub catalogs: Vec<String>,
    pub access: Vec<String>,
    pub publishyear_prev: Option<String>,
    pub publishyear_next: Option<String>,
    pub sort_by: Option<String>,
    pub order: Option<String>,
    pub extra: Vec<(String, String)>,
}

impl SearchParams {
    pub fn key_json(&self) -> Result<String> {
        Ok(serde_json::to_string(self)?)
    }
}

#[derive(Debug)]
pub struct SearchPageResult {
    pub ids: Vec<String>,
    pub total_results: Option<u64>,
}

#[derive(Debug)]
pub struct RusnebClient {
    client: Client,
    base_url: Url,
    delay: Duration,
    last_request: Option<Instant>,
}

#[derive(Debug)]
struct TextResponse {
    status: u16,
    body: String,
}

#[derive(Debug)]
pub struct FetchFailure {
    pub status: Option<u16>,
    pub message: String,
}

impl std::fmt::Display for FetchFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for FetchFailure {}

impl RusnebClient {
    pub fn new(
        base_url: &str,
        user_agent: &str,
        delay: Duration,
        timeout: Duration,
        proxy_url: Option<&str>,
    ) -> Result<Self> {
        let mut builder = Client::builder()
            .user_agent(user_agent)
            .cookie_store(true)
            .timeout(timeout);
        if let Some(proxy_url) = proxy_url {
            builder = builder.proxy(
                Proxy::all(proxy_url)
                    .with_context(|| format!("configuring HTTP proxy {proxy_url}"))?,
            );
        }
        let client = builder.build()?;

        Ok(Self {
            client,
            base_url: Url::parse(base_url).context("invalid base URL")?,
            delay,
            last_request: None,
        })
    }

    pub fn search_url(&self, params: &SearchParams, page: u64) -> Result<Url> {
        let mut url = self.base_url.join("/search/")?;
        {
            let mut pairs = url.query_pairs_mut();
            pairs.append_pair("q", &params.query);
            for catalog in &params.catalogs {
                pairs.append_pair("c[]", catalog);
            }
            for access in &params.access {
                pairs.append_pair("access[]", access);
            }
            if let Some(value) = &params.publishyear_prev {
                pairs.append_pair("publishyear_prev", value);
            }
            if let Some(value) = &params.publishyear_next {
                pairs.append_pair("publishyear_next", value);
            }
            if let Some(value) = &params.sort_by {
                pairs.append_pair("by", value);
            }
            if let Some(value) = &params.order {
                pairs.append_pair("order", value);
            }
            for (key, value) in &params.extra {
                pairs.append_pair(key, value);
            }
            if page > 1 {
                pairs.append_pair("PAGEN_1", &page.to_string());
            }
        }
        Ok(url)
    }

    pub fn fetch_search_page(
        &mut self,
        params: &SearchParams,
        page: u64,
    ) -> std::result::Result<SearchPageResult, FetchFailure> {
        let url = self.search_url(params, page).map_err(to_fetch_failure)?;
        let response = self.get_text(url.as_str()).map_err(to_fetch_failure)?;
        if !(200..300).contains(&response.status) {
            return Err(FetchFailure {
                status: Some(response.status),
                message: format!("search page HTTP {}", response.status),
            });
        }

        Ok(SearchPageResult {
            ids: extract_catalog_ids(&response.body),
            total_results: extract_total_results(&response.body),
        })
    }

    /// Fetch advanced-search option values for exact-field overflow sharding.
    ///
    /// rusneb exposes some filters, such as language and source library, as free text fields in
    /// the advanced-search form. The values embedded in that form are safer shard candidates than
    /// guessed query prefixes because they are first-party filter values accepted by `/search/`.
    pub fn fetch_advanced_filter_values(
        &mut self,
        fields: &[String],
    ) -> std::result::Result<BTreeMap<String, Vec<String>>, FetchFailure> {
        let url = self
            .base_url
            .join("/search/extended/")
            .map_err(to_fetch_failure)?;
        let response = self.get_text(url.as_str()).map_err(to_fetch_failure)?;
        if !(200..300).contains(&response.status) {
            return Err(FetchFailure {
                status: Some(response.status),
                message: format!("advanced search HTTP {}", response.status),
            });
        }

        Ok(extract_advanced_filter_values(&response.body, fields))
    }

    pub fn fetch_record(&mut self, id: &str) -> std::result::Result<RusnebRecord, FetchFailure> {
        let fetched_at_unix = now_unix();
        let fetched_at = chrono::Utc::now().to_rfc3339();
        let card_url = self
            .base_url
            .join(&format!("/catalog/{id}/"))
            .map_err(to_fetch_failure)?;
        let marc_url = self
            .base_url
            .join(&format!(
                "/local/components/exalead/search.page.detail/ajax/marcExport.php?book_id={id}"
            ))
            .map_err(to_fetch_failure)?;
        let viewer_url = self
            .base_url
            .join(&format!("/rest_api/viewer/access/?book_id={id}&viewer=Y"))
            .map_err(to_fetch_failure)?;

        let card = self.get_text(card_url.as_str()).map_err(to_fetch_failure)?;
        if !(200..300).contains(&card.status) {
            return Err(FetchFailure {
                status: Some(card.status),
                message: format!("card HTTP {}", card.status),
            });
        }

        let mut fetch_errors = Vec::new();

        let marc_xml = match self.get_text(marc_url.as_str()) {
            Ok(response) if (200..300).contains(&response.status) => {
                if response.body.trim_start().starts_with("<?xml")
                    || response.body.contains("<marc:record")
                    || response.body.contains("<record")
                {
                    Some(response.body)
                } else {
                    fetch_errors.push(format!(
                        "MARC response was not XML, HTTP {}",
                        response.status
                    ));
                    None
                }
            }
            Ok(response) => {
                fetch_errors.push(format!("MARC HTTP {}", response.status));
                None
            }
            Err(error) => {
                fetch_errors.push(format!("MARC request failed: {error:#}"));
                None
            }
        };

        let viewer_access = match self.get_text(viewer_url.as_str()) {
            Ok(response) if (200..300).contains(&response.status) => {
                match serde_json::from_str::<Value>(&response.body) {
                    Ok(value) => Some(sanitize_viewer_access(value)),
                    Err(error) => {
                        fetch_errors.push(format!("viewer JSON parse failed: {error}"));
                        None
                    }
                }
            }
            Ok(response) => {
                fetch_errors.push(format!("viewer access HTTP {}", response.status));
                None
            }
            Err(error) => {
                fetch_errors.push(format!("viewer access request failed: {error:#}"));
                None
            }
        };

        let marc21 = match marc_xml {
            Some(xml) => match parse_marc_xml(&xml) {
                Ok(record) => Some(record),
                Err(error) => {
                    fetch_errors.push(format!("MARC XML parse failed: {error:#}"));
                    Some(MarcXmlRecord {
                        raw_xml: xml,
                        leader: None,
                        control_fields: Vec::new(),
                        data_fields: Vec::new(),
                        pdf_links: Vec::new(),
                    })
                }
            },
            None => None,
        };

        let mut metadata = parse_card_metadata(&card.body, &self.base_url);
        if let Some(marc) = &marc21 {
            merge_unique(&mut metadata.pdf_links, marc.pdf_links.clone());
            merge_unique(&mut metadata.topics, marc_topics(marc));
        }

        Ok(RusnebRecord {
            id: id.to_string(),
            url: card_url.to_string(),
            fetched_at,
            fetched_at_unix,
            source: SourceUrls {
                card_html: card_url.to_string(),
                marc21_xml: marc_url.to_string(),
                viewer_access_json: viewer_url.to_string(),
            },
            metadata,
            marc21,
            viewer_access,
            fetch_errors,
        })
    }

    fn get_text(&mut self, url: &str) -> Result<TextResponse> {
        self.throttle();
        let response = self.client.get(url).send()?;
        let status = response.status().as_u16();
        let body = response.text()?;
        Ok(TextResponse { status, body })
    }

    fn throttle(&mut self) {
        if let Some(last_request) = self.last_request {
            let elapsed = last_request.elapsed();
            if elapsed < self.delay {
                thread::sleep(self.delay - elapsed);
            }
        }
        self.last_request = Some(Instant::now());
    }
}

fn to_fetch_failure(error: impl Into<anyhow::Error>) -> FetchFailure {
    let error = error.into();
    FetchFailure {
        status: None,
        message: format!("{error:#}"),
    }
}

fn extract_catalog_ids(html: &str) -> Vec<String> {
    let document = Html::parse_document(html);
    let result_link_selector = Selector::parse(
        r#".search-list__item_link[href], .search-result__content-main-read-button[href]"#,
    )
    .expect("valid result link selector");
    let mut seen = HashSet::new();
    let mut out = Vec::new();

    for node in document.select(&result_link_selector) {
        let Some(href) = node.value().attr("href") else {
            continue;
        };
        let Some(id) = catalog_id_from_href(href) else {
            continue;
        };
        if seen.insert(id.clone()) {
            out.push(id);
        }
    }
    if !out.is_empty() {
        return out;
    }

    let re = Regex::new(r#"/catalog/([^/"?#]+)/?"#).expect("valid catalog regex");
    for capture in re.captures_iter(html) {
        let Some(id) = capture.get(1).map(|m| m.as_str()) else {
            continue;
        };
        let id = decode_html_entities(id).to_string();
        if id.is_empty() || id == "undefined" {
            continue;
        }
        if seen.insert(id.clone()) {
            out.push(id);
        }
    }

    out
}

fn catalog_id_from_href(href: &str) -> Option<String> {
    let re = Regex::new(r#"^/catalog/([^/"?#]+)/?"#).expect("valid catalog href regex");
    re.captures(href)
        .and_then(|capture| capture.get(1))
        .map(|id| decode_html_entities(id.as_str()).to_string())
        .filter(|id| !id.is_empty() && id != "undefined")
}

fn extract_total_results(html: &str) -> Option<u64> {
    let re = Regex::new(r"Найдено\s+([0-9\s]+)\s+результ").ok()?;
    let digits = re
        .captures(html)?
        .get(1)?
        .as_str()
        .chars()
        .filter(|ch| ch.is_ascii_digit())
        .collect::<String>();
    digits.parse().ok()
}

fn extract_advanced_filter_values(html: &str, fields: &[String]) -> BTreeMap<String, Vec<String>> {
    let requested = fields.iter().map(String::as_str).collect::<BTreeSet<_>>();
    let document = Html::parse_document(html);
    let selector = Selector::parse("[data-id][data-value]").expect("valid data attribute selector");
    let mut seen = BTreeSet::<(String, String)>::new();
    let mut out = BTreeMap::<String, Vec<String>>::new();

    for node in document.select(&selector) {
        let Some(field) = node.value().attr("data-id") else {
            continue;
        };
        if !requested.contains(field) {
            continue;
        }

        let Some(value) = node.value().attr("data-value") else {
            continue;
        };
        let value = decode_html_entities(value).trim().to_string();
        if value.is_empty() {
            continue;
        }

        let key = (field.to_string(), value.clone());
        if seen.insert(key) {
            out.entry(field.to_string()).or_default().push(value);
        }
    }

    out
}

fn parse_card_metadata(html: &str, base_url: &Url) -> CardMetadata {
    let document = Html::parse_document(html);
    let mut metadata = CardMetadata::default();

    metadata.og = extract_og(&document);
    metadata.title = metadata
        .og
        .get("title")
        .cloned()
        .or_else(|| meta_content(&document, r#"meta[name="title"]"#))
        .or_else(|| first_text(&document, "h1"))
        .or_else(|| first_text(&document, "title"));

    metadata.description = meta_content(&document, r#"meta[name="description"]"#)
        .or_else(|| metadata.og.get("description").cloned())
        .or_else(|| first_text(&document, ".cards-content .color_gray"));

    metadata.authors = select_texts(&document, r#"[itemprop="author"]"#);
    metadata.bibliographic_description = first_text(&document, "#toClipBoard");
    metadata.detail = extract_detail_fields(&document, base_url);
    metadata.detail_map = detail_map(&metadata.detail);
    metadata.year = metadata.og.get("release_date").cloned().or_else(|| {
        first_detail_value(
            &metadata.detail_map,
            &["Год издания", "Год издания/создания"],
        )
    });

    metadata.topics = detail_topics(&metadata.detail);
    metadata.pdf_links = extract_pdf_links(&document, base_url);

    metadata
}

fn extract_og(document: &Html) -> BTreeMap<String, String> {
    let selector = Selector::parse("meta[property], meta[name]").expect("valid selector");
    let mut out = BTreeMap::new();
    for node in document.select(&selector) {
        let value = node.value();
        let Some(content) = value.attr("content").map(normalize_ws) else {
            continue;
        };
        let key = value
            .attr("property")
            .or_else(|| value.attr("name"))
            .unwrap_or_default();
        if let Some(stripped) = key.strip_prefix("og:") {
            out.insert(stripped.to_string(), content);
        } else if let Some(stripped) = key.strip_prefix("book:") {
            out.insert(stripped.to_string(), content);
        }
    }
    out
}

fn meta_content(document: &Html, selector: &str) -> Option<String> {
    let selector = Selector::parse(selector).ok()?;
    document
        .select(&selector)
        .find_map(|node| node.value().attr("content"))
        .map(normalize_ws)
        .filter(|value| !value.is_empty())
}

fn first_text(document: &Html, selector: &str) -> Option<String> {
    let selector = Selector::parse(selector).ok()?;
    document
        .select(&selector)
        .map(|node| element_text(&node))
        .find(|value| !value.is_empty())
}

fn select_texts(document: &Html, selector: &str) -> Vec<String> {
    let Ok(selector) = Selector::parse(selector) else {
        return Vec::new();
    };
    let mut seen = BTreeSet::new();
    for node in document.select(&selector) {
        let text = element_text(&node);
        if !text.is_empty() {
            seen.insert(text);
        }
    }
    seen.into_iter().collect()
}

fn extract_detail_fields(document: &Html, base_url: &Url) -> Vec<DetailField> {
    let Ok(section_selector) = Selector::parse(".cards-section") else {
        return Vec::new();
    };
    let Ok(heading_selector) = Selector::parse("h2") else {
        return Vec::new();
    };
    let Ok(row_selector) = Selector::parse(".cards-table__row") else {
        return Vec::new();
    };
    let Ok(left_selector) = Selector::parse(".cards-table__left") else {
        return Vec::new();
    };
    let Ok(right_selector) = Selector::parse(".cards-table__right") else {
        return Vec::new();
    };

    let mut fields = Vec::new();
    for section in document.select(&section_selector) {
        let heading = section
            .select(&heading_selector)
            .next()
            .map(|node| element_text(&node))
            .unwrap_or_default();
        if !heading.contains("Детальная информация") {
            continue;
        }

        for row in section.select(&row_selector) {
            let label = row
                .select(&left_selector)
                .next()
                .map(|node| element_text(&node))
                .unwrap_or_default();
            let value_node = row.select(&right_selector).next();
            let value = value_node
                .as_ref()
                .map(detail_value_text)
                .unwrap_or_default();
            if label.is_empty() && value.is_empty() {
                continue;
            }
            let links = value_node
                .as_ref()
                .map(|node| extract_links(node, base_url))
                .unwrap_or_default();
            fields.push(DetailField {
                label,
                value,
                links,
            });
        }
    }

    fields
}

fn extract_links(node: &ElementRef<'_>, base_url: &Url) -> Vec<Link> {
    let selector = Selector::parse("a[href]").expect("valid selector");
    let mut links = Vec::new();
    for link in node.select(&selector) {
        let text = element_text(&link);
        let Some(href) = link.value().attr("href") else {
            continue;
        };
        let href = absolutize(base_url, href);
        links.push(Link { text, href });
    }
    links
}

fn detail_value_text(node: &ElementRef<'_>) -> String {
    let box_selector = Selector::parse(".cards-table__right_box").expect("valid selector");
    let box_values = node
        .select(&box_selector)
        .map(|node| element_text(&node))
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    if !box_values.is_empty() {
        return normalize_ws(&box_values.join(" "));
    }

    let link_selector = Selector::parse(".cards-table__right_link").expect("valid selector");
    let mut text = element_text(node);
    for link in node.select(&link_selector) {
        let link_text = element_text(&link);
        if !link_text.is_empty() {
            text = text.replace(&link_text, "");
        }
    }
    normalize_ws(&text)
}

fn sanitize_viewer_access(mut value: Value) -> Value {
    if let Some(object) = value.as_object_mut() {
        object.remove("token");
        object.remove("viewer");
    }
    value
}

fn extract_pdf_links(document: &Html, base_url: &Url) -> Vec<String> {
    let selector = Selector::parse("a[href]").expect("valid selector");
    let mut links = Vec::new();
    for node in document.select(&selector) {
        let href = node.value().attr("href").unwrap_or_default();
        let text = element_text(&node).to_lowercase();
        let href_lower = href.to_lowercase();
        if href_lower.contains("doc_type=pdf")
            || href_lower.ends_with(".pdf")
            || (text.contains("pdf") && href_lower.contains("getfiles.php"))
        {
            links.push(absolutize(base_url, href));
        }
    }
    dedup(links)
}

fn detail_map(fields: &[DetailField]) -> BTreeMap<String, Vec<String>> {
    let mut map: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for field in fields {
        if field.label.is_empty() || field.value.is_empty() {
            continue;
        }
        map.entry(field.label.clone())
            .or_default()
            .push(field.value.clone());
    }
    map
}

fn first_detail_value(map: &BTreeMap<String, Vec<String>>, labels: &[&str]) -> Option<String> {
    labels.iter().find_map(|label| {
        map.get(*label)
            .and_then(|values| values.first())
            .cloned()
            .filter(|value| !value.is_empty())
    })
}

fn detail_topics(fields: &[DetailField]) -> Vec<String> {
    let topic_label_parts = ["Тем", "Предмет", "ББК", "УДК", "Ключ"];
    let mut topics = Vec::new();
    for field in fields {
        if topic_label_parts
            .iter()
            .any(|part| field.label.contains(part))
        {
            topics.push(field.value.clone());
        }
    }
    dedup(topics)
}

fn parse_marc_xml(xml: &str) -> Result<MarcXmlRecord> {
    let doc = roxmltree::Document::parse(xml)?;
    let leader = doc
        .descendants()
        .find(|node| node.is_element() && node.tag_name().name() == "leader")
        .and_then(|node| node.text())
        .map(normalize_ws);

    let mut control_fields = Vec::new();
    let mut data_fields = Vec::new();
    let mut pdf_links = Vec::new();

    for node in doc.descendants().filter(|node| node.is_element()) {
        match node.tag_name().name() {
            "controlfield" => {
                if let Some(tag) = node.attribute("tag") {
                    control_fields.push(MarcControlField {
                        tag: tag.to_string(),
                        value: normalize_ws(node.text().unwrap_or_default()),
                    });
                }
            }
            "datafield" => {
                let Some(tag) = node.attribute("tag") else {
                    continue;
                };
                let mut subfields = Vec::new();
                for child in node.children().filter(|child| child.is_element()) {
                    if child.tag_name().name() != "subfield" {
                        continue;
                    }
                    let code = child.attribute("code").unwrap_or_default().to_string();
                    let value = normalize_ws(child.text().unwrap_or_default());
                    if tag == "856" && code == "u" && value.to_lowercase().contains(".pdf") {
                        pdf_links.push(value.clone());
                    }
                    subfields.push(MarcSubfield { code, value });
                }
                data_fields.push(MarcDataField {
                    tag: tag.to_string(),
                    ind1: node.attribute("ind1").map(str::to_string),
                    ind2: node.attribute("ind2").map(str::to_string),
                    subfields,
                });
            }
            _ => {}
        }
    }

    Ok(MarcXmlRecord {
        raw_xml: xml.to_string(),
        leader,
        control_fields,
        data_fields,
        pdf_links: dedup(pdf_links),
    })
}

fn marc_topics(marc: &MarcXmlRecord) -> Vec<String> {
    let mut topics = Vec::new();
    for field in &marc.data_fields {
        if matches!(
            field.tag.as_str(),
            "600" | "610" | "611" | "630" | "650" | "651" | "653" | "655" | "979"
        ) {
            for subfield in &field.subfields {
                if matches!(subfield.code.as_str(), "a" | "b" | "x" | "y" | "z") {
                    topics.push(subfield.value.clone());
                }
            }
        }
    }
    dedup(topics)
}

fn element_text(node: &ElementRef<'_>) -> String {
    normalize_ws(&node.text().collect::<Vec<_>>().join(" "))
}

fn normalize_ws(input: &str) -> String {
    decode_html_entities(input)
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn absolutize(base_url: &Url, href: &str) -> String {
    base_url
        .join(href)
        .map(|url| url.to_string())
        .unwrap_or_else(|_| href.to_string())
}

fn merge_unique(target: &mut Vec<String>, values: Vec<String>) {
    let mut seen = target.iter().cloned().collect::<HashSet<_>>();
    for value in values {
        if !value.is_empty() && seen.insert(value.clone()) {
            target.push(value);
        }
    }
}

fn dedup(values: Vec<String>) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for value in values {
        if !value.is_empty() && seen.insert(value.clone()) {
            out.push(value);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_catalog_ids_once() {
        let html = r#"
          <a class="search-list__item_link" href="/catalog/000199_000009_015267348/">x</a>
          <a class="search-result__content-main-read-button" href="/catalog/000199_000009_015267348/">dup</a>
          <a class="search-list__item_link" href="/catalog/000200_000018_v19_rc_1637278">y</a>
          <a href="/catalog/000199_000009_parent">parent relation</a>
        "#;
        assert_eq!(
            extract_catalog_ids(html),
            vec![
                "000199_000009_015267348".to_string(),
                "000200_000018_v19_rc_1637278".to_string()
            ]
        );
    }

    #[test]
    fn extracts_total_results() {
        let html = "Найдено 22 514 496   результатов";
        assert_eq!(extract_total_results(html), Some(22_514_496));
    }

    #[test]
    fn extracts_advanced_filter_values_for_requested_fields() {
        let html = r#"
          <div data-id="lang" data-value="Русский"></div>
          <div data-id="lang" data-value="Русский"></div>
          <div data-id="lang" data-value="Английский"></div>
          <div data-id="idlibrary" data-value="Российская государственная библиотека (РГБ)"></div>
          <div data-id="publisher" data-value="Ignored"></div>
          <div data-id="lang" data-value=""></div>
        "#;
        let fields = vec!["lang".to_string(), "idlibrary".to_string()];

        let values = extract_advanced_filter_values(html, &fields);

        assert_eq!(
            values.get("lang"),
            Some(&vec!["Русский".to_string(), "Английский".to_string()])
        );
        assert_eq!(
            values.get("idlibrary"),
            Some(&vec![
                "Российская государственная библиотека (РГБ)".to_string()
            ])
        );
        assert!(!values.contains_key("publisher"));
    }

    #[test]
    fn parses_detail_fields() {
        let html = r#"
        <div class="cards-section">
          <h2> Детальная информация </h2>
          <div class="cards-table">
            <div class="cards-table__row">
              <div class="cards-table__left">Год издания</div>
              <div class="cards-table__right">2005</div>
            </div>
            <div class="cards-table__row">
              <div class="cards-table__left">Каталог</div>
              <div class="cards-table__right"><a href="/search/?c[]=25">Книги</a></div>
            </div>
          </div>
        </div>
        "#;
        let base = Url::parse("https://rusneb.ru/").unwrap();
        let metadata = parse_card_metadata(html, &base);
        assert_eq!(metadata.year.as_deref(), Some("2005"));
        assert_eq!(metadata.detail.len(), 2);
        assert_eq!(
            metadata.detail[1].links[0].href,
            "https://rusneb.ru/search/?c[]=25"
        );
    }

    #[test]
    fn parses_marc_xml() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
        <marc:collection xmlns:marc="http://www.loc.gov/MARC21/slim">
          <marc:record>
            <marc:leader>01234nam a2200000 i 4500</marc:leader>
            <marc:controlfield tag="001">015267348</marc:controlfield>
            <marc:datafield tag="856" ind1="1" ind2="1">
              <subfield code="q">application/pdf</subfield>
              <subfield code="u">http://example.test/file.pdf</subfield>
            </marc:datafield>
          </marc:record>
        </marc:collection>"#;
        let marc = parse_marc_xml(xml).unwrap();
        assert_eq!(marc.leader.as_deref(), Some("01234nam a2200000 i 4500"));
        assert_eq!(marc.control_fields[0].tag, "001");
        assert_eq!(marc.pdf_links, vec!["http://example.test/file.pdf"]);
    }
}
