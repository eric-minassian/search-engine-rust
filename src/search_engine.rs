use rust_stemmers::{Algorithm, Stemmer};
use std::collections::HashMap;
use tokenizers::Tokenizer;

use crate::{
    error::{Error, Result},
    inverted_index::{disk_inverted_index::DiskInvertedIndex, doc_map::Doc},
};

pub struct SearchEngine {
    inverted_index_db: DiskInvertedIndex,
    stemmer: Stemmer,
    tokenizer: Tokenizer,
}

impl SearchEngine {
    pub fn new(inverted_index_db: DiskInvertedIndex) -> Result<Self> {
        let stemmer = Stemmer::create(Algorithm::English);
        let tokenizer = Tokenizer::from_pretrained("bert-base-cased", None)
            .map_err(|e| Error::Generic(format!("Failed to load tokenizer: {e}")))?;

        Ok(Self {
            inverted_index_db,
            stemmer,
            tokenizer,
        })
    }

    pub fn search(&mut self, query: &str) -> Result<Vec<Doc>> {
        let mut document_ids: HashMap<u64, f64> = HashMap::new();

        let binding = self
            .tokenizer
            .encode(query, false)
            .map_err(|e| Error::Generic(format!("Failed to tokenize query: {e}")))?;

        let stemmed_tokens = binding
            .get_tokens()
            .iter()
            .map(|token| self.stemmer.stem(token))
            .collect::<Vec<_>>();

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
        document_ids.sort_by(|a, b| {
            a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Greater) // Treat None (i.e., when comparing with NaN) as if 'a' is greater, to sort NaN to the end
        });

        document_ids
            .iter()
            .map(|(doc_id, _)| {
                self.inverted_index_db
                    .get_doc(*doc_id)
                    .map_err(|e| Error::Generic(format!("Failed to get document: {e}")))
                    .and_then(|doc_opt| {
                        doc_opt.ok_or_else(|| Error::Generic("Document not found".to_string()))
                    })
                    .map(|doc| Doc::new(doc.url))
            })
            .collect()
    }
}
