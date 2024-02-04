use crate::database::{constants::TEMP_FILE_SUFFIX, disk_hash_map::DiskHashMap};

use super::{
    constants::{BOLD_WEIGHT, HEADER_WEIGHT, TITLE_WEIGHT},
    doc_map::{Doc, DocMap},
};
use rust_stemmers::{Algorithm, Stemmer};
use scraper::{Html, Selector};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs::{rename, File},
    io::{self, BufReader},
    path::PathBuf,
};
use tokenizers::Tokenizer;
use walkdir::WalkDir;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct CrawlFile {
    url: String,
    content: String,
    encoding: String,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct TermIndex {
    pub doc_id: u64,
    pub tf_idf: f64,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct TempTermIndex {
    pub doc_id: u64,
    pub tf: u64,
}

pub type InvertedIndex = HashMap<String, Vec<TempTermIndex>>;

pub struct DiskInvertedIndex {
    pub db: DiskHashMap<String, Vec<TermIndex>>,
    pub url_map: DiskHashMap<u64, Doc>,
}

impl DiskInvertedIndex {
    pub fn new(
        db_path: PathBuf,
        url_map_path: PathBuf,
        crawled_data_path: PathBuf,
    ) -> io::Result<Self> {
        create_index(db_path.clone(), url_map_path.clone(), crawled_data_path)?;

        let db = DiskHashMap::from_path(db_path)?;
        let url_map = DiskHashMap::from_path(url_map_path)?;

        Ok(Self { db, url_map })
    }

    pub fn from_path(db_path: PathBuf, url_map_path: PathBuf) -> io::Result<Self> {
        let db = DiskHashMap::from_path(db_path)?;
        let url_map = DiskHashMap::from_path(url_map_path)?;

        Ok(Self { db, url_map })
    }

    pub fn get(&mut self, key: &str) -> io::Result<Option<Vec<TermIndex>>> {
        self.db.get(&key.to_string())
    }

    pub fn get_doc(&mut self, key: u64) -> io::Result<Option<Doc>> {
        self.url_map.get(&key)
    }
}

fn create_index(db_path: PathBuf, url_map_path: PathBuf, data_path: PathBuf) -> io::Result<()> {
    let stemmer = Stemmer::create(Algorithm::English);
    let tokenizer = Tokenizer::from_pretrained("bert-base-cased", None).unwrap();

    let mut db = DiskHashMap::new(db_path.clone())?;
    let mut url_map = DiskHashMap::new(url_map_path)?;

    let mut inverted_index = InvertedIndex::new();
    let mut doc_map = DocMap::new();

    let mut num_docs = 0;

    for (doc_id, entry) in WalkDir::new(&data_path)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .enumerate()
    {
        let data: CrawlFile =
            serde_json::from_reader(BufReader::new(File::open(entry.path()).unwrap())).unwrap();

        let document = Html::parse_document(&data.content);

        // Extract and filter all text
        let all_text = document.root_element().text().collect::<Vec<_>>().join(" ");

        let mut word_count: HashMap<String, u32> = HashMap::new();

        tokenizer
            .encode(all_text, false)
            .unwrap()
            .get_tokens()
            .iter()
            .for_each(|token| {
                let count = word_count.entry(token.clone()).or_insert(0);
                *count += 1;
            });

        let bolded_words = document
            .select(&Selector::parse("b, strong").unwrap())
            .map(|element| element.text().collect::<Vec<_>>().join(" "))
            .collect::<Vec<_>>()
            .join(" ");

        let title_words = document
            .select(&Selector::parse("title").unwrap())
            .map(|element| element.text().collect::<Vec<_>>().join(" "))
            .collect::<Vec<_>>()
            .join(" ");

        let header_words = document
            .select(&Selector::parse("h1, h2, h3, h4, h5").unwrap())
            .map(|element| element.text().collect::<Vec<_>>().join(" "))
            .collect::<Vec<_>>()
            .join(" ");

        let bolded_words = bolded_words
            .split_whitespace()
            .map(str::trim)
            .map(|token| stemmer.stem(token))
            .map(String::from)
            .collect::<Vec<_>>();

        let title_words = title_words
            .split_whitespace()
            .map(str::trim)
            .map(|token| stemmer.stem(token))
            .map(String::from)
            .collect::<Vec<_>>();

        let header_words = header_words
            .split_whitespace()
            .map(str::trim)
            .map(|token| stemmer.stem(token))
            .map(String::from)
            .collect::<Vec<_>>();

        for word in bolded_words {
            let count = word_count.entry(word).or_insert(0);
            *count += (BOLD_WEIGHT - 1.0) as u32;
        }

        for word in title_words {
            let count = word_count.entry(word).or_insert(0);
            *count += (TITLE_WEIGHT - 1.0) as u32;
        }

        for word in header_words {
            let count = word_count.entry(word).or_insert(0);
            *count += (HEADER_WEIGHT - 1.0) as u32;
        }

        for (word, count) in word_count {
            let index_data = TempTermIndex {
                doc_id: doc_id as u64,
                tf: count as u64,
            };

            inverted_index
                .entry(word)
                .or_insert_with(Vec::new)
                .push(index_data);
        }

        doc_map.insert(doc_id as u64, Doc::new(data.url));

        if doc_id % 1_000 == 0 {
            println!("Processed {} documents", doc_id);
        }

        if doc_id % 10_000 == 0 {
            db.extend(inverted_index)?;
            url_map.insert(doc_map)?;

            inverted_index = InvertedIndex::new();
            doc_map = DocMap::new();
        }

        num_docs += 1;
    }

    db.extend(inverted_index)?;
    url_map.insert(doc_map)?;

    calculate_scores(db, db_path, num_docs)?;

    Ok(())
}

pub fn calculate_scores(
    db: DiskHashMap<String, Vec<TempTermIndex>>,
    db_path: PathBuf,
    num_docs: u64,
) -> io::Result<()> {
    let temp_db_path = format!("{}{}", db_path.display(), TEMP_FILE_SUFFIX);

    let mut temp_db = DiskHashMap::new(temp_db_path.clone().into())?;

    let mut final_map: HashMap<String, Vec<TermIndex>> = HashMap::new();

    for (i, (key, value)) in db.enumerate() {
        let data_len = value.len();

        let new_data = value
            .iter()
            .map(|index_data| {
                let tf_idf =
                    calculate_tf_idf(index_data.tf as f64, data_len as f64, num_docs as f64);

                TermIndex {
                    doc_id: index_data.doc_id,
                    tf_idf,
                }
            })
            .collect();

        final_map.insert(key, new_data);

        if i % 1_000 == 0 {
            println!("Translate {} documents", i);
        }

        if i % 10_000 == 0 {
            temp_db.extend(final_map)?;
            final_map = HashMap::new();
        }
    }

    temp_db.extend(final_map)?;

    std::fs::rename(temp_db_path, db_path)?;

    Ok(())
}

fn calculate_tf_idf(tf: f64, df: f64, n: f64) -> f64 {
    (1.0 + tf.log10()) * (n / df).log10()
}
