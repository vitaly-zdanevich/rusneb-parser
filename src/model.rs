use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Debug, Serialize, Deserialize)]
pub struct RusnebRecord {
    pub id: String,
    pub url: String,
    pub fetched_at: String,
    pub fetched_at_unix: i64,
    pub source: SourceUrls,
    pub metadata: CardMetadata,
    pub marc21: Option<MarcXmlRecord>,
    pub viewer_access: Option<Value>,
    pub fetch_errors: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SourceUrls {
    pub card_html: String,
    pub marc21_xml: String,
    pub viewer_access_json: String,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct CardMetadata {
    pub title: Option<String>,
    pub authors: Vec<String>,
    pub year: Option<String>,
    pub bibliographic_description: Option<String>,
    pub description: Option<String>,
    pub detail: Vec<DetailField>,
    pub detail_map: BTreeMap<String, Vec<String>>,
    pub topics: Vec<String>,
    pub pdf_links: Vec<String>,
    pub og: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DetailField {
    pub label: String,
    pub value: String,
    pub links: Vec<Link>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Link {
    pub text: String,
    pub href: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MarcXmlRecord {
    pub raw_xml: String,
    pub leader: Option<String>,
    pub control_fields: Vec<MarcControlField>,
    pub data_fields: Vec<MarcDataField>,
    pub pdf_links: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MarcControlField {
    pub tag: String,
    pub value: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MarcDataField {
    pub tag: String,
    pub ind1: Option<String>,
    pub ind2: Option<String>,
    pub subfields: Vec<MarcSubfield>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MarcSubfield {
    pub code: String,
    pub value: String,
}
