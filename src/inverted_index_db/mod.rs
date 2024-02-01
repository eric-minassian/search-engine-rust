mod indexer;

use std::{
    collections::HashMap,
    fs::{remove_file, rename, File},
    io::{self, BufRead, BufReader, BufWriter, Seek, SeekFrom, Write},
    path::Path,
};

use self::indexer::{DocumentIndex, IndexData, InvertedIndex};

const KEY_DELIMITER: &str = ":";
const DOCUMENT_DELIMITER: &str = "|";
const DATA_DELIMITER: &str = ",";
const TEMP_DB_SUFFIX: &str = ".tmp";

#[derive(Debug)]
pub struct InvertedIndexDatabase {
    db_path: String,
    doc_index_path: String,
    url_map_path: String,
    database: BufReader<File>,
    doc_index: HashMap<String, u64>,
    url_map: HashMap<u64, String>,
}

impl InvertedIndexDatabase {
    pub fn new(
        db_path: String,
        doc_index_path: String,
        url_map_path: String,
        restart: bool,
    ) -> io::Result<Self> {
        let doc_index;
        let url_map;

        if restart {
            if Path::new(&db_path).exists() {
                std::fs::remove_file(&db_path)?;
            }
            if Path::new(&doc_index_path).exists() {
                std::fs::remove_file(&doc_index_path)?;
            }
            File::create(&db_path)?;

            doc_index = HashMap::new();
            url_map = HashMap::new();
        } else {
            doc_index = serde_json::from_reader(BufReader::new(File::open(&doc_index_path)?))?;
            url_map = serde_json::from_reader(BufReader::new(File::open(&url_map_path)?))?;
        };

        Ok(InvertedIndexDatabase {
            database: BufReader::new(File::open(&db_path)?),
            db_path,
            doc_index,
            url_map_path,
            doc_index_path,
            url_map,
        })
    }

    pub fn initialize(&mut self) {
        self.create_index("data".to_string());
    }

    pub fn close(&self) -> io::Result<()> {
        let file = File::create(&self.doc_index_path)?;
        let mut writer = BufWriter::new(file);
        serde_json::to_writer(&mut writer, &self.doc_index)?;
        writer.flush()?;

        let file = File::create(&self.url_map_path)?;
        let mut writer = BufWriter::new(file);
        serde_json::to_writer(&mut writer, &self.url_map)?;
        writer.flush()?;

        Ok(())
    }

    fn refresh_index(&mut self) -> io::Result<()> {
        self.doc_index.clear();
        self.database.seek(SeekFrom::Start(0))?;

        let mut seek_pos;
        let mut line = String::new();

        loop {
            seek_pos = self.database.stream_position()?;
            self.database.read_line(&mut line)?;

            if line.is_empty() {
                break;
            };

            // let key_len = line.split(KEY_DELIMITER).nth(0).unwrap().parse().unwrap();
            // let line_without = line.split(KEY_DELIMITER).nth(1).unwrap();
            let split_data = line.split_once(KEY_DELIMITER).unwrap();
            let key_len = split_data.0.parse().unwrap();
            let line_without = split_data.1;

            self.doc_index
                .insert(line_without[..key_len].to_string(), seek_pos);

            line.clear();
        }

        Ok(())
    }

    fn set(&mut self, inverted_index: &InvertedIndex) -> io::Result<()> {
        if inverted_index.is_empty() {
            return Ok(());
        }

        let temp_db_path = format!("{}{}", self.db_path, TEMP_DB_SUFFIX);
        let temp_db = File::create(Path::new(&temp_db_path))?;
        let mut writer = BufWriter::new(temp_db);

        for (key, value) in inverted_index.iter() {
            let value_str = value
                .iter()
                .map(|x| format!("{}{}{}", x.doc_id, DATA_DELIMITER, x.tf))
                .collect::<Vec<String>>()
                .join(DOCUMENT_DELIMITER);

            let new_value = if let Some(&offset) = self.doc_index.get(key) {
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

        for (key, &offset) in &self.doc_index {
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

    fn get_internal(&mut self, key: &str) -> io::Result<Vec<IndexData>> {
        let seek_pos = match self.doc_index.get(key) {
            Some(pos) => pos,
            None => return Ok(Vec::new()),
        };

        self.database.seek(SeekFrom::Start(*seek_pos))?;

        let mut data = String::new();
        self.database.read_line(&mut data)?;
        let values = &data.split_once(KEY_DELIMITER).unwrap().1[key.len()..];

        values
            .split(DOCUMENT_DELIMITER)
            .map(|data| {
                let mut parts = data.split(DATA_DELIMITER).map(str::trim);
                let doc_id = parts
                    .next()
                    .ok_or_else(|| io::Error::other("Missing doc_id"))?
                    .parse()
                    .map_err(|_| io::Error::other("Couldn't parse doc_id"))?;

                let tf_idf = parts
                    .next()
                    .ok_or_else(|| io::Error::other("Missing tf_idf"))?
                    .parse()
                    .map_err(|_| io::Error::other("Couldn't parse tf_idf"))?;

                Ok(IndexData { doc_id, tf: tf_idf })
            })
            .collect()
    }

    pub fn get(&mut self, key: &str) -> io::Result<Vec<DocumentIndex>> {
        let seek_pos = match self.doc_index.get(key) {
            Some(pos) => pos,
            None => return Ok(Vec::new()),
        };

        self.database.seek(SeekFrom::Start(*seek_pos))?;

        let mut data = String::new();
        self.database.read_line(&mut data)?;
        let values = &data
            .split(KEY_DELIMITER)
            .nth(1)
            .ok_or_else(|| io::Error::other("Invalid data format"))?[key.len()..];

        values
            .split(DOCUMENT_DELIMITER)
            .map(|data| {
                let mut parts = data.split(DATA_DELIMITER).map(str::trim);
                let doc_id = parts
                    .next()
                    .ok_or_else(|| io::Error::other("Missing doc_id"))?
                    .parse()
                    .map_err(|_| io::Error::other("Couldn't parse doc_id"))?;

                let tf_idf = parts
                    .next()
                    .ok_or_else(|| io::Error::other("Missing tf_idf"))?
                    .parse()
                    .map_err(|_| io::Error::other("Couldn't parse tf_idf"))?;

                Ok(DocumentIndex { doc_id, tf_idf })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn general_test() {
        let mut db = InvertedIndexDatabase::new(
            "database.db".to_string(),
            "doc_index.json".to_string(),
            "url_map.json".to_string(),
            true,
        )
        .unwrap();

        assert_eq!(db.get("hello").unwrap(), Vec::new());

        for i in 1..9 {
            db.url_map.insert(i, format!("https://{}", i));
        }

        let mut inverted_index = HashMap::new();

        inverted_index.insert(
            "hello".to_string(),
            vec![
                IndexData { doc_id: 1, tf: 3 },
                IndexData { doc_id: 2, tf: 5 },
                IndexData { doc_id: 3, tf: 4 },
            ],
        );

        inverted_index.insert(
            "jeff".to_string(),
            vec![
                IndexData { doc_id: 4, tf: 2 },
                IndexData { doc_id: 7, tf: 4 },
            ],
        );

        db.set(&inverted_index).unwrap();

        let mut inverted_index = HashMap::new();

        inverted_index.insert(
            "hello".to_string(),
            vec![
                IndexData { doc_id: 5, tf: 34 },
                IndexData { doc_id: 7, tf: 13 },
                IndexData {
                    doc_id: 8,
                    tf: 22_342,
                },
            ],
        );

        db.set(&inverted_index).unwrap();
        db.calculate_scores().unwrap();

        let expected_output = vec![
            DocumentIndex {
                doc_id: 1,
                tf_idf: 0.1845496633819414,
            },
            DocumentIndex {
                doc_id: 2,
                tf_idf: 0.21226716587714,
            },
            DocumentIndex {
                doc_id: 3,
                tf_idf: 0.20015935128721957,
            },
            DocumentIndex {
                doc_id: 5,
                tf_idf: 0.31627977764580667,
            },
            DocumentIndex {
                doc_id: 7,
                tf_idf: 0.2641134116987305,
            },
            DocumentIndex {
                doc_id: 8,
                tf_idf: 0.668312550575298,
            },
        ];

        assert_eq!(db.get("hello").unwrap(), expected_output);

        db.close().unwrap();
        drop(db);

        let mut db = InvertedIndexDatabase::new(
            "database.db".to_string(),
            "doc_index.json".to_string(),
            "url_map.json".to_string(),
            false,
        )
        .unwrap();

        assert_eq!(db.get("hello").unwrap(), expected_output);
    }
}
