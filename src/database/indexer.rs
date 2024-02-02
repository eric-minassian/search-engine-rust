use rust_stemmers::{Algorithm, Stemmer};
use scraper::{Html, Selector};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs::{remove_file, rename, File},
    io::{self, BufRead, BufReader, BufWriter, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    str::FromStr,
};
use tokenizers::tokenizer::Tokenizer;
use walkdir::WalkDir;

use crate::database::DATA_DELIMITER;

use super::{
    doc_map::Doc, InvertedIndexDatabase, DOCUMENT_DELIMITER, KEY_DELIMITER, TEMP_FILE_SUFFIX,
};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct TempTermIndex {
    pub doc_id: u64,
    pub tf: u64,
}

impl FromStr for TempTermIndex {
    type Err = std::io::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.split(DATA_DELIMITER);

        let doc_id = parts
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Couldn't parse doc_id"))?
            .parse()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Couldn't parse doc_id"))?;
        let tf = parts
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Couldn't parse tf"))?
            .parse()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Couldn't parse tf"))?;

        Ok(Self { doc_id, tf })
    }
}

pub type InvertedIndex = HashMap<String, Vec<TempTermIndex>>;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct CrawlFile {
    url: String,
    content: String,
    encoding: String,
}

const BOLD_WEIGHT: f64 = 3.0;
const HEADER_WEIGHT: f64 = 5.0;
const TITLE_WEIGHT: f64 = 10.0;

impl InvertedIndexDatabase {
    fn get_internal(&mut self, key: &str) -> io::Result<Vec<TempTermIndex>> {
        let seek_pos = self.seek_pos_map.get(key).copied().ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, "Key not found in document index")
        })?;

        self.database.seek(SeekFrom::Start(seek_pos))?;

        let mut data = String::new();
        self.database.read_line(&mut data)?;

        let values = match data.split_once(KEY_DELIMITER) {
            Some((_, value)) => &value[key.len()..],
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "KEY_DELIMITER not found",
                ))
            }
        }
        .trim();

        values
            .split(DOCUMENT_DELIMITER)
            .map(|data| data.parse())
            .collect()
    }

    pub fn create_index(&mut self, data_path: PathBuf) {
        let stemmer = Stemmer::create(Algorithm::English);
        let tokenizer = Tokenizer::from_pretrained("bert-base-cased", None).unwrap();

        let mut inverted_index = InvertedIndex::new();

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

            // all_text
            //     .split_whitespace()
            //     .map(str::trim)
            //     .map(|token| stemmer.stem(token))
            //     .map(|stem| stem.to_lowercase())
            //     // .map(String::from)
            //     .for_each(|token| {
            //         let count = word_count.entry(token).or_insert(0);
            //         *count += 1;
            //     });

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

            self.doc_map.insert(doc_id as u64, Doc::new(data.url));
        }

        self.set(&inverted_index).unwrap();

        self.calculate_scores().unwrap();
    }

    pub fn calculate_scores(&mut self) -> io::Result<()> {
        let temp_db_path = format!("{}{}", self.db_path.display(), TEMP_FILE_SUFFIX);
        let temp_db = File::create(Path::new(&temp_db_path))?;
        let mut writer = BufWriter::new(temp_db);

        for (key, _) in self.seek_pos_map.clone() {
            let data = self.get_internal(&key)?;
            let data_len = data.len();

            let new_data = data
                .iter()
                .map(|index_data| {
                    let tf_idf = self.calculate_tf_idf(
                        index_data.tf as f64,
                        data_len as f64,
                        self.doc_map.len() as f64,
                    );

                    format!("{}{}{}", index_data.doc_id, DATA_DELIMITER, tf_idf)
                })
                .collect::<Vec<_>>()
                .join(DOCUMENT_DELIMITER);

            writeln!(writer, "{}{}{}{}", key.len(), KEY_DELIMITER, key, new_data)?;
        }

        writer.flush()?;

        remove_file(&self.db_path)?;
        rename(temp_db_path, &self.db_path)?;

        self.database = BufReader::new(File::open(&self.db_path)?);
        self.refresh_index()?;

        self.close()?;

        Ok(())
    }

    fn calculate_tf_idf(&self, tf: f64, df: f64, n: f64) -> f64 {
        (1.0 + tf.log10()) * (n / df).log10()
    }
}