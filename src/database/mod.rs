mod doc_map;
mod indexer;

use std::{
    collections::HashMap,
    fs::{remove_file, rename, File},
    io::{self, BufRead, BufReader, BufWriter, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    str::FromStr,
};

use self::{
    doc_map::{Doc, DocMap},
    indexer::InvertedIndex,
};

#[derive(Debug, PartialEq)]
pub struct TermIndex {
    pub doc_id: u64,
    pub tf_idf: f64,
}

impl FromStr for TermIndex {
    type Err = std::io::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.split(DATA_DELIMITER);
        let doc_id = parts
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Couldn't parse doc_id"))?
            .parse()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Couldn't parse doc_id"))?;
        let tf_idf = parts
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Couldn't parse tf_idf"))?
            .parse()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Couldn't parse tf_idf"))?;

        Ok(Self { doc_id, tf_idf })
    }
}

const KEY_DELIMITER: &str = ":";
const DOCUMENT_DELIMITER: &str = "|";
const DATA_DELIMITER: &str = ",";
const TEMP_FILE_SUFFIX: &str = ".tmp";

type SeekPosMap = HashMap<String, u64>;

pub struct InvertedIndexDatabase {
    db_path: PathBuf,
    seek_pos_map_path: PathBuf,
    doc_map_path: PathBuf,
    database: BufReader<File>,
    seek_pos_map: SeekPosMap,
    doc_map: DocMap,
}

impl InvertedIndexDatabase {
    pub fn from_cache(
        db_path: PathBuf,
        doc_index_path: PathBuf,
        url_map_path: PathBuf,
    ) -> io::Result<Self> {
        let doc_index = serde_json::from_reader(BufReader::new(File::open(&doc_index_path)?))?;
        let url_map = serde_json::from_reader(BufReader::new(File::open(&url_map_path)?))?;

        Ok(Self {
            database: BufReader::new(File::open(&db_path)?),
            db_path,
            seek_pos_map_path: doc_index_path,
            doc_map_path: url_map_path,
            seek_pos_map: doc_index,
            doc_map: url_map,
        })
    }

    pub fn from_crawled_data(
        crawled_data_path: PathBuf,
        db_path: PathBuf,
        doc_index_path: PathBuf,
        url_map_path: PathBuf,
    ) -> io::Result<Self> {
        let mut db = Self {
            database: BufReader::new(File::create(&db_path)?),
            db_path,
            seek_pos_map_path: doc_index_path,
            doc_map_path: url_map_path,
            seek_pos_map: HashMap::new(),
            doc_map: DocMap::new(),
        };

        db.create_index(crawled_data_path);

        Ok(db)
    }

    fn close(&self) -> io::Result<()> {
        let file = File::create(&self.seek_pos_map_path)?;
        let mut writer = BufWriter::new(file);
        serde_json::to_writer(&mut writer, &self.seek_pos_map)?;
        writer.flush()?;

        let file = File::create(&self.doc_map_path)?;
        let mut writer = BufWriter::new(file);
        serde_json::to_writer(&mut writer, &self.doc_map)?;
        writer.flush()?;

        Ok(())
    }

    fn refresh_index(&mut self) -> io::Result<()> {
        self.seek_pos_map.clear();
        self.database.seek(SeekFrom::Start(0))?;

        let mut seek_pos;
        let mut line = String::new();

        loop {
            seek_pos = self.database.stream_position()?;
            self.database.read_line(&mut line)?;

            if line.is_empty() {
                break;
            };

            let split_data = line.split_once(KEY_DELIMITER).unwrap();
            let key_len = split_data.0.parse().unwrap();
            let line_without = split_data.1;

            self.seek_pos_map
                .insert(line_without[..key_len].to_string(), seek_pos);

            line.clear();
        }

        Ok(())
    }

    fn set(&mut self, inverted_index: &InvertedIndex) -> io::Result<()> {
        if inverted_index.is_empty() {
            return Ok(());
        }

        let temp_db_path = format!("{}{}", self.db_path.display(), TEMP_FILE_SUFFIX);
        let temp_db = File::create(Path::new(&temp_db_path))?;
        let mut writer = BufWriter::new(temp_db);

        for (key, value) in inverted_index.iter() {
            let value_str = value
                .iter()
                .map(|x| format!("{}{}{}", x.doc_id, DATA_DELIMITER, x.tf))
                .collect::<Vec<String>>()
                .join(DOCUMENT_DELIMITER);

            let new_value = if let Some(&offset) = self.seek_pos_map.get(key) {
                self.database.seek(SeekFrom::Start(offset))?;
                let mut old_value = String::new();
                self.database.read_line(&mut old_value)?;
                let old_value = old_value.split(KEY_DELIMITER).nth(1).unwrap()[key.len()..].trim();
                format!("{}{}{}", old_value, DOCUMENT_DELIMITER, value_str)
            } else {
                value_str
            };

            writeln!(writer, "{}{}{}{}", key.len(), KEY_DELIMITER, key, new_value)?;
        }

        for (key, &offset) in &self.seek_pos_map {
            if !inverted_index.contains_key(key) {
                self.database.seek(SeekFrom::Start(offset))?;
                let mut line = String::new();
                self.database.read_line(&mut line)?;
                write!(writer, "{}", line)?;
            }
        }

        writer.flush()?;

        remove_file(&self.db_path)?;
        rename(temp_db_path, &self.db_path)?;

        self.database = BufReader::new(File::open(&self.db_path)?);
        self.refresh_index()?;

        Ok(())
    }

    pub fn get(&mut self, key: &str) -> io::Result<Vec<TermIndex>> {
        let seek_pos = match self.seek_pos_map.get(key) {
            Some(pos) => pos,
            None => return Ok(Vec::new()),
        };

        self.database.seek(SeekFrom::Start(*seek_pos))?;

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

    pub fn get_doc(&self, doc_id: u64) -> Option<&Doc> {
        self.doc_map.get(&doc_id)
    }
}

#[cfg(test)]
mod tests {
    use tests::{doc_map::Doc, indexer::TempTermIndex};

    use super::*;

    #[test]
    fn general_test() {
        let db_path = PathBuf::from("tests/test_db.db");
        let doc_index_path = PathBuf::from("tests/test_doc_index.json");
        let url_map_path = PathBuf::from("tests/test_url_map.json");

        let mut db = InvertedIndexDatabase {
            database: BufReader::new(File::create(&db_path).unwrap()),
            db_path: db_path.clone(),
            seek_pos_map_path: doc_index_path.clone(),
            doc_map_path: url_map_path.clone(),
            seek_pos_map: HashMap::new(),
            doc_map: HashMap::new(),
        };

        assert_eq!(db.get("hello").unwrap(), Vec::new());

        for i in 1..9 {
            db.doc_map.insert(i, Doc::new(format!("https://{}", i)));
        }

        let mut inverted_index = HashMap::new();

        inverted_index.insert(
            "hello".to_string(),
            vec![
                TempTermIndex { doc_id: 1, tf: 3 },
                TempTermIndex { doc_id: 2, tf: 5 },
                TempTermIndex { doc_id: 3, tf: 4 },
            ],
        );

        inverted_index.insert(
            "jeff".to_string(),
            vec![
                TempTermIndex { doc_id: 4, tf: 2 },
                TempTermIndex { doc_id: 7, tf: 4 },
            ],
        );

        db.set(&inverted_index).unwrap();

        let mut inverted_index = HashMap::new();

        inverted_index.insert(
            "hello".to_string(),
            vec![
                TempTermIndex { doc_id: 5, tf: 34 },
                TempTermIndex { doc_id: 7, tf: 13 },
                TempTermIndex {
                    doc_id: 8,
                    tf: 22_342,
                },
            ],
        );

        db.set(&inverted_index).unwrap();
        db.calculate_scores().unwrap();

        let expected_output = vec![
            TermIndex {
                doc_id: 1,
                tf_idf: 0.1845496633819414,
            },
            TermIndex {
                doc_id: 2,
                tf_idf: 0.21226716587714,
            },
            TermIndex {
                doc_id: 3,
                tf_idf: 0.20015935128721957,
            },
            TermIndex {
                doc_id: 5,
                tf_idf: 0.31627977764580667,
            },
            TermIndex {
                doc_id: 7,
                tf_idf: 0.2641134116987305,
            },
            TermIndex {
                doc_id: 8,
                tf_idf: 0.668312550575298,
            },
        ];

        assert_eq!(db.get("hello").unwrap(), expected_output);

        db.close().unwrap();
        drop(db);

        let mut db =
            InvertedIndexDatabase::from_cache(db_path, doc_index_path, url_map_path).unwrap();

        assert_eq!(db.get("hello").unwrap(), expected_output);
    }
}
