use crate::{
    error::{Error, Result},
    inverted_index::disk_inverted_index::DiskInvertedIndex,
    tokenizer::Tokenizer,
};
use std::collections::HashMap;

use super::search_result::SearchResult;

pub struct SearchEngine {
    inverted_index_db: DiskInvertedIndex,
    tokenizer: Tokenizer,
}

impl SearchEngine {
    pub fn new(inverted_index_db: DiskInvertedIndex) -> Result<Self> {
        Ok(Self {
            inverted_index_db,
            tokenizer: Tokenizer::new()?,
        })
    }

    pub fn search(&mut self, query: &str) -> Result<Vec<SearchResult>> {
        let mut document_ids: HashMap<u64, f64> = HashMap::new();

        let stemmed_tokens = self.tokenizer.tokenize(query);

        for token in stemmed_tokens {
            if let Some(document_indexes) = self.inverted_index_db.get(&token).map_err(|e| {
                Error::Generic(format!("Failed to get document indexes for token: {e}"))
            })? {
                for document_index in document_indexes {
                    *document_ids.entry(document_index.doc_id).or_insert(0.0) +=
                        document_index.tf_idf;
                }
            }
        }

        let mut document_ids: Vec<_> = document_ids.into_iter().collect();
        document_ids.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Greater));

        document_ids
            .into_iter()
            .map(|(doc_id, score)| {
                self.inverted_index_db
                    .get_doc(doc_id)
                    .map_err(|e| Error::Generic(format!("Failed to get document: {e}")))
                    .and_then(|doc_opt| {
                        doc_opt.ok_or_else(|| Error::Generic("Document not found".to_string()))
                    })
                    .map(|doc| SearchResult::new(doc.url, score))
            })
            .collect()
    }
}
