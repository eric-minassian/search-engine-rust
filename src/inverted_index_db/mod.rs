use std::{
    collections::HashMap,
    fs::{self, remove_file, rename, File},
    io::{self, BufRead, BufReader, BufWriter, Seek, SeekFrom, Write},
    path::Path,
};

use crate::indexer::{IndexData, InvertedIndex};

const KEY_DELIMITER: &str = ":";
const DOCUMENT_DELIMITER: &str = "|";
const DATA_DELIMITER: &str = ",";
const TEMP_DB_SUFFIX: &str = ".tmp";

#[derive(Debug)]
pub struct InvertedIndexDatabase {
    db_path: String,
    doc_index_path: String,
    doc_index: HashMap<String, u64>,
    database: BufReader<File>,
}

impl InvertedIndexDatabase {
    pub fn new(db_path: String, doc_index_path: String, restart: bool) -> io::Result<Self> {
        let index = if restart {
            if Path::new(&db_path).exists() {
                std::fs::remove_file(&db_path)?;
            }
            if Path::new(&doc_index_path).exists() {
                std::fs::remove_file(&doc_index_path)?;
            }
            File::create(&db_path)?;

            HashMap::new()
        } else {
            serde_json::from_str(&fs::read_to_string(&doc_index_path)?)?
        };

        Ok(InvertedIndexDatabase {
            database: BufReader::new(File::open(&db_path)?),
            db_path,
            doc_index_path,
            doc_index: index,
        })
    }

    pub fn close(&self) -> io::Result<()> {
        let file = File::create(&self.doc_index_path)?;
        let mut writer = BufWriter::new(file);
        serde_json::to_writer(&mut writer, &self.doc_index)?;
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

            let key_len = line.split(KEY_DELIMITER).nth(0).unwrap().parse().unwrap();
            let line_without = line.split(KEY_DELIMITER).nth(1).unwrap();

            self.doc_index
                .insert(line_without[..key_len].to_string(), seek_pos);

            line.clear();
        }

        Ok(())
    }

    pub fn set(&mut self, inverted_index: &InvertedIndex) -> io::Result<()> {
        if inverted_index.is_empty() {
            return Ok(());
        }

        let temp_db_path = format!("{}{}", self.db_path, TEMP_DB_SUFFIX);
        let temp_db = File::create(Path::new(&temp_db_path))?;
        let mut writer = BufWriter::new(temp_db);

        for (key, value) in inverted_index.iter() {
            let value_str = value
                .iter()
                .map(|x| format!("{}{}{}", x.doc_id, DATA_DELIMITER, x.tf_idf))
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

    pub fn get(&mut self, key: &str) -> io::Result<Vec<IndexData>> {
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

                Ok(IndexData { doc_id, tf_idf })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn general_test() {
        let mut db =
            InvertedIndexDatabase::new("temp.txt".to_string(), "temp.file".to_string(), true)
                .unwrap();

        assert_eq!(db.get("hello").unwrap(), Vec::new());

        let mut inverted_index = HashMap::new();

        inverted_index.insert(
            "hello".to_string(),
            vec![
                IndexData {
                    doc_id: 1,
                    tf_idf: 0.4,
                },
                IndexData {
                    doc_id: 2,
                    tf_idf: 0.3,
                },
                IndexData {
                    doc_id: 4,
                    tf_idf: 0.23,
                },
            ],
        );

        inverted_index.insert(
            "jeff".to_string(),
            vec![
                IndexData {
                    doc_id: 4,
                    tf_idf: 0.43,
                },
                IndexData {
                    doc_id: 7,
                    tf_idf: 0.2,
                },
            ],
        );

        db.set(&inverted_index).unwrap();

        assert_eq!(
            db.get("hello").unwrap(),
            vec![
                IndexData {
                    doc_id: 1,
                    tf_idf: 0.4,
                },
                IndexData {
                    doc_id: 2,
                    tf_idf: 0.3,
                },
                IndexData {
                    doc_id: 4,
                    tf_idf: 0.23,
                },
            ]
        );

        assert_eq!(
            db.get("jeff").unwrap(),
            vec![
                IndexData {
                    doc_id: 4,
                    tf_idf: 0.43,
                },
                IndexData {
                    doc_id: 7,
                    tf_idf: 0.2,
                },
            ],
        );

        let mut inverted_index = HashMap::new();

        inverted_index.insert(
            "hello".to_string(),
            vec![
                IndexData {
                    doc_id: 8,
                    tf_idf: 3.4,
                },
                IndexData {
                    doc_id: 9,
                    tf_idf: 1.3,
                },
                IndexData {
                    doc_id: 42,
                    tf_idf: 0.22342,
                },
            ],
        );

        db.set(&inverted_index).unwrap();

        assert_eq!(
            db.get("hello").unwrap(),
            vec![
                IndexData {
                    doc_id: 1,
                    tf_idf: 0.4,
                },
                IndexData {
                    doc_id: 2,
                    tf_idf: 0.3,
                },
                IndexData {
                    doc_id: 4,
                    tf_idf: 0.23,
                },
                IndexData {
                    doc_id: 8,
                    tf_idf: 3.4,
                },
                IndexData {
                    doc_id: 9,
                    tf_idf: 1.3,
                },
                IndexData {
                    doc_id: 42,
                    tf_idf: 0.22342,
                },
            ]
        );

        db.close().unwrap();
        drop(db);

        let mut db =
            InvertedIndexDatabase::new("temp.txt".to_string(), "temp.file".to_string(), false)
                .unwrap();

        assert_eq!(
            db.get("hello").unwrap(),
            vec![
                IndexData {
                    doc_id: 1,
                    tf_idf: 0.4,
                },
                IndexData {
                    doc_id: 2,
                    tf_idf: 0.3,
                },
                IndexData {
                    doc_id: 4,
                    tf_idf: 0.23,
                },
                IndexData {
                    doc_id: 8,
                    tf_idf: 3.4,
                },
                IndexData {
                    doc_id: 9,
                    tf_idf: 1.3,
                },
                IndexData {
                    doc_id: 42,
                    tf_idf: 0.22342,
                },
            ]
        );
    }
}
