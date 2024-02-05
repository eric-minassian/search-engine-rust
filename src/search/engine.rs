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
            if let Some(document_indexes) = self.inverted_index_db.get(&token)? {
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
                    .and_then(|doc_opt| {
                        doc_opt.ok_or_else(|| Error::Generic("Document not found".to_string()))
                    })
                    .map(|doc| SearchResult::new(doc.url, score))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_search() {
        let mut search_engine = SearchEngine::new(
            DiskInvertedIndex::from(
                "tests/test-data/search_test_db.test".into(),
                "tests/test-data/search_test_seek.test".into(),
                "tests/test-data/search_test_url_map.test".into(),
                "tests/test-data/search_test_url_map_seek.test".into(),
            )
            .expect("Failed to create search engine"),
        )
        .expect("Failed to create search engine");

        let results = search_engine.search("eric").unwrap();
        assert_eq!(results.len(), 3);

        assert_eq!(results[0].url, "https://www.ericminassian.com/");
        assert_eq!(results[0].score, 9.1);
        assert_eq!(
            results[1].url,
            "https://www.linkedin.com/in/minassian-eric/"
        );
        assert_eq!(results[1].score, 2.4);
        assert_eq!(results[2].url, "https://www.github.com/eric-minassian");
        assert_eq!(results[2].score, 1.2);
    }

    #[test]
    fn test_search_no_results() {
        let mut search_engine = SearchEngine::new(
            DiskInvertedIndex::from(
                "tests/test-data/search_test_db.test".into(),
                "tests/test-data/search_test_seek.test".into(),
                "tests/test-data/search_test_url_map.test".into(),
                "tests/test-data/search_test_url_map_seek.test".into(),
            )
            .expect("Failed to create search engine"),
        )
        .expect("Failed to create search engine");

        let results = search_engine.search("not_in_index").unwrap();
        assert_eq!(results.len(), 0);
    }
}
