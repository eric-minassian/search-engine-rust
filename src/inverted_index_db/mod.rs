use std::{
    collections::HashMap,
    fs::{self, remove_file, rename, File},
    io::{self, BufRead, BufReader, BufWriter, Seek, SeekFrom, Write},
    path::Path,
};

use crate::indexer::{IndexData, InvertedIndex};

#[derive(Debug)]
pub struct InvertedIndexDatabase {
    db_path: String,
    doc_index_path: String,
    doc_index: HashMap<String, u64>,
    database: Option<BufReader<File>>,
}

impl InvertedIndexDatabase {
    pub fn new(db_path: String, doc_index_path: String, restart: bool) -> io::Result<Self> {
        let index;

        if restart {
            if Path::new(&db_path).exists() {
                std::fs::remove_file(&db_path)?;
            }
            if Path::new(&doc_index_path).exists() {
                std::fs::remove_file(&doc_index_path)?;
            }
            File::create(&db_path)?;
            index = HashMap::new();
        } else {
            index = serde_json::from_str(&fs::read_to_string(&doc_index_path)?)?
        }

        Ok(InvertedIndexDatabase {
            database: Some(BufReader::new(File::open(&db_path)?)),
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

    pub fn refresh_index(&mut self) -> io::Result<()> {
        match &mut self.database {
            None => return Err(io::Error::other("Database not open")),
            Some(database) => {
                self.doc_index.clear();

                let mut seek_pos;
                let mut line = String::new();

                loop {
                    seek_pos = database.stream_position()?;
                    database.read_line(&mut line)?;

                    if line.is_empty() {
                        break;
                    };

                    self.doc_index.insert(
                        line.split("<>").collect::<Vec<&str>>()[0].to_string(),
                        seek_pos,
                    );

                    line.clear();
                }
            }
        }

        Ok(())
    }

    pub fn set(&mut self, inverted_index: InvertedIndex) -> io::Result<()> {
        if inverted_index.len() == 0 {
            return Ok(());
        }

        match &mut self.database {
            None => return Err(io::Error::other("Database not open")),
            Some(database) => {
                let mut temp_database = File::create(Path::new(&format!("{}.tmp", self.db_path)))?;

                let mut value_str: Vec<String>;

                for (key, value) in inverted_index.iter() {
                    value_str = value
                        .iter()
                        .map(|x| format!("{},{}", x.doc_id, x.tf_idf))
                        .collect();

                    let new_value;

                    if self.doc_index.contains_key(key) {
                        database.seek(SeekFrom::Start(self.doc_index[key]))?;
                        let mut old_value = String::new();
                        database.read_line(&mut old_value)?;
                        old_value = old_value.split("<>").collect::<Vec<&str>>()[1]
                            .trim()
                            .to_string();

                        let mut new_value_list: Vec<String> =
                            old_value.split("|").map(String::from).collect();

                        new_value_list.append(&mut value_str);
                        new_value = new_value_list.join("|");
                    } else {
                        new_value = value_str.join("|");
                    }

                    temp_database.write(format!("{key}<>{new_value}\n").as_bytes())?;
                }

                let mut line = String::new();
                for (key, value) in &self.doc_index {
                    if !inverted_index.contains_key(key) {
                        database.seek(SeekFrom::Start(*value))?;
                        database.read_line(&mut line)?;
                        temp_database.write(line.as_bytes())?;
                    }
                }
            }
        }

        self.database = None;
        remove_file(&self.db_path)?;
        rename(format!("{}.tmp", self.db_path), &self.db_path)?;

        self.database = Some(BufReader::new(File::open(&self.db_path)?));
        self.refresh_index()?;

        Ok(())
    }

    pub fn get(&mut self, key: &str) -> io::Result<Vec<IndexData>> {
        match &mut self.database {
            None => return Err(io::Error::other("Database Not Open")),
            Some(database) => {
                if !self.doc_index.contains_key(key) {
                    return Ok(Vec::new());
                }

                let seek_pos = self.doc_index.get(key).unwrap();
                database.seek(SeekFrom::Start(*seek_pos))?;

                let mut data = String::new();
                database.read_line(&mut data)?;
                data = data.split("<>").collect::<Vec<&str>>()[1].to_string();

                let data_list: Vec<&str> = data.split("|").collect();
                let mut res = Vec::new();
                for data in data_list {
                    let temp_tuple: Vec<&str> = data.split(",").map(|x| x.trim()).collect();
                    res.push(IndexData {
                        doc_id: temp_tuple[0].parse().unwrap(),
                        tf_idf: temp_tuple[1].parse().unwrap(),
                    })
                }

                return Ok(res);
            }
        };
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

        db.set(inverted_index).unwrap();

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

        db.set(inverted_index).unwrap();

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
