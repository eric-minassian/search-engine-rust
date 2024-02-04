use rust_stemmers::{Algorithm, Stemmer};
use std::collections::HashMap;
use tokenizers::Tokenizer;

use crate::inverted_index::disk_inverted_index::DiskInvertedIndex;

pub struct SearchResult {
    pub url: String,
}

pub struct SearchEngine {
    inverted_index_db: DiskInvertedIndex,
    stemmer: Stemmer,
    tokenizer: Tokenizer,
}

impl SearchEngine {
    pub fn new(inverted_index_db: DiskInvertedIndex) -> Self {
        let stemmer = Stemmer::create(Algorithm::English);
        let tokenizer = Tokenizer::from_pretrained("bert-base-cased", None).unwrap();

        SearchEngine {
            inverted_index_db,
            stemmer,
            tokenizer,
        }
    }

    pub fn search(&mut self, query: &str) -> Vec<SearchResult> {
        let mut document_ids: HashMap<u64, f64> = HashMap::new();

        let binding = self.tokenizer.encode(query, false).unwrap();
        let stemmed_tokens = binding
            .get_tokens()
            .iter()
            .map(|token| self.stemmer.stem(token))
            .collect::<Vec<_>>();

        for token in stemmed_tokens {
            if let Ok(document_indexes) = self.inverted_index_db.get(&token) {
                document_indexes.unwrap().iter().for_each(|document_index| {
                    *document_ids.entry(document_index.doc_id).or_insert(0.0) +=
                        document_index.tf_idf;
                });
            }
        }

        let mut document_ids: Vec<_> = document_ids.into_iter().collect();
        document_ids.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

        document_ids
            .iter()
            .map(|(doc_id, _)| SearchResult {
                url: self
                    .inverted_index_db
                    .get_doc(*doc_id)
                    .unwrap()
                    .unwrap()
                    .url
                    .clone(),
            })
            .collect()
    }
}
