use super::{
    constants::{BOLD_WEIGHT, HEADER_WEIGHT, MAX_ITERATIONS, TEMP_FILE_SUFFIX, TITLE_WEIGHT},
    doc_map::{Doc, DocID, DocMap, TF, TFIDF},
};
use crate::{
    error::{Error, Result},
    kv_database::database::KVDatabase,
    tokenizer::Tokenizer,
};
use scraper::{Html, Selector};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs::{rename, File},
    io::BufReader,
    path::PathBuf,
};
use walkdir::WalkDir;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct CrawlFile {
    url: String,
    content: String,
    encoding: String,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct TermIndex {
    pub doc_id: DocID,
    pub tf_idf: TFIDF,
}

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct TempTermIndex {
    pub doc_id: DocID,
    pub tf: TF,
}

pub type TempInvertedIndex = HashMap<String, Vec<TempTermIndex>>;
pub type InvertedIndex = HashMap<String, Vec<TermIndex>>;

pub struct DiskInvertedIndex {
    pub db: KVDatabase<String, Vec<TermIndex>>,
    pub url_map: KVDatabase<DocID, Doc>,
}

impl DiskInvertedIndex {
    pub fn new(
        db_path: PathBuf,
        seek_path: PathBuf,
        url_map_path: PathBuf,
        url_map_seek_path: PathBuf,
        crawled_data_path: PathBuf,
    ) -> Result<Self> {
        create_index(
            db_path.clone(),
            seek_path.clone(),
            url_map_path.clone(),
            url_map_seek_path.clone(),
            crawled_data_path,
        )?;

        let db = KVDatabase::from(db_path, seek_path)?;
        let url_map = KVDatabase::from(url_map_path, url_map_seek_path)?;

        Ok(Self { db, url_map })
    }

    pub fn from(
        db_path: PathBuf,
        seek_path: PathBuf,
        url_map_path: PathBuf,
        url_map_seek_path: PathBuf,
    ) -> Result<Self> {
        let db = KVDatabase::from(db_path, seek_path)?;
        let url_map = KVDatabase::from(url_map_path, url_map_seek_path)?;

        Ok(Self { db, url_map })
    }

    pub fn get(&mut self, key: &str) -> Result<Option<Vec<TermIndex>>> {
        self.db.get(&key.to_string())
    }

    pub fn get_doc(&mut self, doc_id: DocID) -> Result<Option<Doc>> {
        self.url_map.get(&doc_id)
    }
}

#[allow(clippy::too_many_lines)]
fn create_index(
    db_path: PathBuf,
    seek_path: PathBuf,
    url_map_path: PathBuf,
    url_map_seek_path: PathBuf,
    data_path: PathBuf,
) -> Result<()> {
    let tokenizer = Tokenizer::new()?;

    let mut db = KVDatabase::new(db_path.clone(), seek_path.clone())?;
    let mut url_map = KVDatabase::new(url_map_path, url_map_seek_path)?;

    let mut inverted_index = TempInvertedIndex::new();
    let mut doc_map = DocMap::new();

    let mut num_docs = 0;

    for (doc_id, entry) in WalkDir::new(data_path)
        .into_iter()
        .filter_map(std::result::Result::ok)
        .filter(|e| e.file_type().is_file())
        .enumerate()
    {
        let data: CrawlFile = serde_json::from_reader(BufReader::new(File::open(entry.path())?))?;

        let document = Html::parse_document(&data.content);
        let doc_id = doc_id as DocID;

        // Extract and filter all text
        let mut word_count: HashMap<String, u32> = HashMap::new();

        let all_text = document.root_element().text().collect::<Vec<_>>();
        let bolded_words = select_text(&document, "b, strong").unwrap_or_default();
        let title_words = select_text(&document, "title").unwrap_or_default();
        let header_words = select_text(&document, "h1, h2, h3, h4, h5").unwrap_or_default();

        update_word_count(all_text, &tokenizer, &mut word_count, 1);
        update_word_count(
            title_words,
            &tokenizer,
            &mut word_count,
            TITLE_WEIGHT as u32,
        );
        update_word_count(
            bolded_words,
            &tokenizer,
            &mut word_count,
            BOLD_WEIGHT as u32,
        );
        update_word_count(
            header_words,
            &tokenizer,
            &mut word_count,
            HEADER_WEIGHT as u32,
        );

        for (word, count) in word_count {
            let index_data = TempTermIndex { doc_id, tf: count };

            inverted_index.entry(word).or_default().push(index_data);
        }

        doc_map.insert(doc_id, Doc::new(data.url));

        if doc_id % MAX_ITERATIONS == 0 {
            db.extend(inverted_index)?;
            url_map.insert(doc_map)?;

            inverted_index = TempInvertedIndex::new();
            doc_map = DocMap::new();

            println!("Processed {doc_id} documents");
        }

        num_docs += 1;
    }

    db.extend(inverted_index)?;
    url_map.insert(doc_map)?;

    calculate_scores(db, db_path, seek_path, num_docs)?;

    Ok(())
}

fn select_text<'a>(document: &'a Html, selector: &str) -> Result<Vec<&'a str>> {
    Ok(document
        .select(
            &Selector::parse(selector)
                .map_err(|e| Error::Generic(format!("Failed to parse selector: {e}")))?,
        )
        .map(|element| element.text().collect::<Vec<_>>())
        .collect::<Vec<_>>()
        .concat())
}

fn update_word_count(
    title_words: Vec<&str>,
    tokenizer: &Tokenizer,
    word_count: &mut HashMap<String, u32>,
    weight: u32,
) {
    title_words
        .iter()
        .map(|text| tokenizer.tokenize(text))
        .for_each(|tokens| {
            tokens.into_iter().for_each(|token| {
                let count = word_count.entry(token).or_insert(0);
                *count += weight;
            });
        });
}

pub fn calculate_scores(
    mut db: KVDatabase<String, Vec<TempTermIndex>>,
    db_path: PathBuf,
    seek_path: PathBuf,
    num_docs: u64,
) -> Result<()> {
    let temp_db_path = format!("{}{}", db_path.display(), TEMP_FILE_SUFFIX);
    let temp_seek_path = format!("{}{}", seek_path.display(), TEMP_FILE_SUFFIX);

    let mut temp_db = KVDatabase::new(temp_db_path.clone().into(), temp_seek_path.clone().into())?;

    let mut final_map: HashMap<String, Vec<TermIndex>> = HashMap::new();

    for (i, data) in db.into_iter().enumerate() {
        let (key, value) = data?;

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

        if i % MAX_ITERATIONS as usize == 0 {
            temp_db.extend(final_map)?;
            final_map = HashMap::new();
            println!("Translate {i} words to tf-idf scores");
        }
    }

    temp_db.extend(final_map)?;

    rename(temp_db_path, db_path)?;
    rename(temp_seek_path, seek_path)?;

    Ok(())
}

fn calculate_tf_idf(tf: f64, df: f64, n: f64) -> f64 {
    (1.0 + tf.log10()) * (n / df).log10()
}
