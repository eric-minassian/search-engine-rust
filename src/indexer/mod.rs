use std::collections::HashMap;

#[derive(Debug, PartialEq)]
pub struct IndexData {
    pub doc_id: u64,
    pub tf_idf: f64,
}

pub type InvertedIndex = HashMap<String, Vec<IndexData>>;
