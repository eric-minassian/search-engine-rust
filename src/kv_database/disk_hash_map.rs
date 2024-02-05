use serde::{Deserialize, Serialize};

use std::io::Read;
use std::{
    collections::HashMap,
    fmt::Display,
    fs::{remove_file, rename, File},
    hash::Hash,
    io::{BufReader, BufWriter, Seek, SeekFrom, Write},
    marker::PhantomData,
    path::PathBuf,
};

use crate::error::{Error, Result};

use super::seek_pos_map::SeekPos;
use super::{constants::TEMP_FILE_SUFFIX, seek_pos_map::SeekPosMap};

#[derive(Debug)]
pub struct KVDatabase<K, V>
where
    K: Serialize + for<'de> Deserialize<'de> + Eq + Hash + Display + Clone,
    V: Serialize + for<'de> Deserialize<'de> + Clone,
{
    db_path: PathBuf,
    seek_path: PathBuf,
    pub seek_pos_map: SeekPosMap<K>,
    pub database: BufReader<File>,
    _marker: PhantomData<V>,
}

// impl<K, V> IntoIterator for KVDatabase<K, V>
// where
//     K: Serialize + for<'de> Deserialize<'de> + Eq + Hash + Display + Clone + From<String>,
//     V: Serialize + for<'de> Deserialize<'de> + Clone,
// {
// type Item = (K, V);
// type Item = (K, V);
// type IntoIter = KVDatabaseIterator<K, V>;

// fn next(&mut self) -> Option<Self::Item> {
//     let (key, seek_pos) = self.seek_pos_map.next()?;

// if self.iter_seek_pos >= self.seek_pos_map.pos {
//     return None;
// }

// self.database
//     .seek(SeekFrom::Start(self.iter_seek_pos))
//     .expect("Failed to seek to next position");

// let mut buffer = String::new();
// self.database
//     .read_line(&mut buffer)
//     .expect("Failed to read line from database");

// let (key, value) = match buffer.split_once(KEY_DELIMITER) {
//     Some((key_len, rest)) => {
//         let key_len = key_len
//             .parse::<usize>()
//             .expect("Failed to parse key length");
//         let key = K::from(rest[..key_len].to_string());
//         let value = &rest[key_len..];

//         (key, value)
//     }
//     None => {
//         panic!("KEY_DELIMITER not found");
//     }
// };

// self.iter_seek_pos = self
//     .database
//     .stream_position()
//     .expect("Failed to get stream position");

// Some((
//     key,
//     serde_json::from_str(value).expect("Failed to deserialize value"),
// ))
// }
// }

impl<K, V> KVDatabase<K, V>
where
    K: Serialize + for<'de> Deserialize<'de> + Eq + Hash + Display + Clone,
    V: Serialize + for<'de> Deserialize<'de> + Clone,
{
    pub fn new(db_path: PathBuf) -> Result<Self> {
        let seek_path = db_path.with_extension("seek");

        let seek_pos_map: SeekPosMap<K> = SeekPosMap::new();

        let serialized =
            bincode::serialize(&seek_pos_map).map_err(|e| Error::Generic(e.to_string()))?;
        let mut file = File::create(&seek_path)?;
        file.write_all(&serialized)?;

        Ok(Self {
            database: BufReader::new(File::create(&db_path)?),
            db_path,
            seek_path,
            seek_pos_map,
            _marker: PhantomData,
        })
    }

    pub fn from(db_path: PathBuf) -> Result<Self> {
        let seek_path = db_path.with_extension("seek");

        let mut file = File::open(&seek_path)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        let seek_pos_map: SeekPosMap<K> =
            bincode::deserialize(&buffer).map_err(|e| Error::Generic(e.to_string()))?;

        Ok(Self {
            database: BufReader::new(File::open(&db_path)?),
            db_path,
            seek_path,
            seek_pos_map,
            _marker: PhantomData,
        })
    }

    pub fn get(&mut self, key: &K) -> Result<Option<V>> {
        if let Some(seek_pos) = self.seek_pos_map.get(key) {
            self.database.seek(SeekFrom::Start(seek_pos.pos))?;

            // Read seek_pos.len bytes
            let mut buffer = vec![0; seek_pos.len as usize];
            self.database.read_exact(&mut buffer)?;
            let value: V =
                bincode::deserialize(&buffer).map_err(|e| Error::Generic(e.to_string()))?;

            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    pub fn insert(&mut self, hashmap: HashMap<K, V>) -> Result<()> {
        if hashmap.is_empty() {
            return Ok(());
        }

        let temp_db_path = self.db_path.with_extension(TEMP_FILE_SUFFIX);
        let mut temp_db_writer = BufWriter::new(File::create(&temp_db_path)?);

        let mut new_seek_pos_map: HashMap<K, SeekPos> = SeekPosMap::new();

        for (key, seek_pos) in &self.seek_pos_map {
            if !hashmap.contains_key(&key) {
                self.database.seek(SeekFrom::Start(seek_pos.pos))?;
                let mut buffer = vec![0; seek_pos.len as usize];
                self.database.read_exact(&mut buffer)?;
                new_seek_pos_map.insert(
                    key.clone(),
                    SeekPos::new(temp_db_writer.stream_position()?, seek_pos.len),
                );

                temp_db_writer.write_all(&buffer)?;
            }
        }

        for (key, value) in hashmap {
            let value = bincode::serialize(&value).map_err(|e| Error::Generic(e.to_string()))?;
            new_seek_pos_map.insert(
                key,
                SeekPos::new(temp_db_writer.stream_position()?, value.len() as u64),
            );

            temp_db_writer.write_all(&value)?;
        }

        temp_db_writer.flush()?;

        // Write the new seek_pos_map to the self.seek_path
        let serialized =
            bincode::serialize(&new_seek_pos_map).map_err(|e| Error::Generic(e.to_string()))?;
        let mut file = File::create(&self.seek_path)?;
        file.write_all(&serialized)?;

        remove_file(&self.db_path)?;
        rename(temp_db_path, &self.db_path)?;

        self.database = BufReader::new(File::open(&self.db_path)?);
        self.seek_pos_map = new_seek_pos_map;

        Ok(())
    }
}

impl<K, V, T> KVDatabase<K, V>
where
    K: Serialize + for<'de> Deserialize<'de> + Eq + Hash + Display + Clone,
    V: Serialize + Extend<T> + for<'de> Deserialize<'de> + IntoIterator<Item = T> + Clone,
{
    pub fn extend(&mut self, hashmap: HashMap<K, V>) -> Result<()> {
        if hashmap.is_empty() {
            return Ok(());
        }

        let temp_db_path = self.db_path.with_extension(TEMP_FILE_SUFFIX);
        let mut temp_db_writer = BufWriter::new(File::create(&temp_db_path)?);

        let mut new_seek_pos_map: HashMap<K, SeekPos> = SeekPosMap::new();

        for (key, seek_pos) in &self.seek_pos_map {
            if !hashmap.contains_key(&key) {
                self.database.seek(SeekFrom::Start(seek_pos.pos))?;
                let mut buffer = vec![0; seek_pos.len as usize];
                self.database.read_exact(&mut buffer)?;
                new_seek_pos_map.insert(
                    key.clone(),
                    SeekPos::new(temp_db_writer.stream_position()?, seek_pos.len),
                );

                temp_db_writer.write_all(&buffer)?;
            }
        }

        for (key, value) in hashmap {
            let new_value = if let Some(seek_pos) = self.seek_pos_map.get(&key) {
                self.database.seek(SeekFrom::Start(seek_pos.pos))?;
                let mut buffer = vec![0; seek_pos.len as usize];
                self.database.read_exact(&mut buffer)?;
                let mut old_value: V =
                    bincode::deserialize(&buffer).map_err(|e| Error::Generic(e.to_string()))?;
                old_value.extend(value);

                old_value
            } else {
                value
            };

            let value =
                bincode::serialize(&new_value).map_err(|e| Error::Generic(e.to_string()))?;
            new_seek_pos_map.insert(
                key,
                SeekPos::new(temp_db_writer.stream_position()?, value.len() as u64),
            );

            temp_db_writer.write_all(&value)?;
        }

        temp_db_writer.flush()?;

        // Write the new seek_pos_map to the self.seek_path
        let serialized =
            bincode::serialize(&new_seek_pos_map).map_err(|e| Error::Generic(e.to_string()))?;
        let mut file = File::create(&self.seek_path)?;
        file.write_all(&serialized)?;

        remove_file(&self.db_path)?;
        rename(temp_db_path, &self.db_path)?;

        self.database = BufReader::new(File::open(&self.db_path)?);
        self.seek_pos_map = new_seek_pos_map;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tests::KVDatabase;

    use super::*;

    #[test]
    fn basic_str() {
        let db_path = PathBuf::from("tests/basic_str.db");

        let mut db = KVDatabase::new(db_path.clone()).expect("Failed to create DiskHashMap");

        let mut hashmap = HashMap::new();
        hashmap.insert("hello".to_string(), vec![1, 2, 3]);
        hashmap.insert("world".to_string(), vec![4, 5, 6]);

        db.extend(hashmap).expect("Failed to insert hashmap");

        assert_eq!(
            db.get(&"hello".to_string()).expect("Failed to get value"),
            Some(vec![1, 2, 3])
        );
    }

    #[test]
    fn basic_int() {
        let db_path = PathBuf::from("tests/basic_int.db");

        let mut db = KVDatabase::new(db_path.clone()).expect("Failed to create DiskHashMap");

        let mut hashmap = HashMap::new();
        hashmap.insert(1, vec![1, 2, 3]);
        hashmap.insert(2, vec![4, 5, 6]);

        db.extend(hashmap).expect("Failed to insert hashmap");

        assert_eq!(
            db.get(&1).expect("Failed to get value"),
            Some(vec![1, 2, 3])
        );
    }

    #[test]
    fn extend_map() {
        let db_path = PathBuf::from("tests/extend_map.db");

        let mut db = KVDatabase::new(db_path.clone()).expect("Failed to create DiskHashMap");

        let mut hashmap = HashMap::new();
        hashmap.insert("hello".to_string(), vec![1, 2, 3]);
        hashmap.insert("world".to_string(), vec![4, 5, 6]);

        db.extend(hashmap.clone())
            .expect("Failed to insert hashmap");

        let mut hashmap2 = HashMap::new();
        hashmap2.insert("hello".to_string(), vec![7, 8, 9]);
        hashmap2.insert("world".to_string(), vec![10, 11, 12]);

        db.extend(hashmap2.clone())
            .expect("Failed to insert hashmap");

        assert_eq!(
            db.get(&"hello".to_string()).expect("Failed to get value"),
            Some(vec![1, 2, 3, 7, 8, 9])
        );
        assert_eq!(
            db.get(&"world".to_string()).expect("Failed to get value"),
            Some(vec![4, 5, 6, 10, 11, 12])
        );
    }

    #[test]
    fn restore_from_path() {
        let db_path = PathBuf::from("tests/restore_from_path.db");

        let mut db = KVDatabase::new(db_path.clone()).expect("Failed to create DiskHashMap");

        let mut hashmap = HashMap::new();
        hashmap.insert("hello".to_string(), vec![1, 2, 3]);
        hashmap.insert("world".to_string(), vec![4, 5, 6]);

        db.extend(hashmap.clone())
            .expect("Failed to insert hashmap");

        let mut db2 =
            KVDatabase::from(db_path.clone()).expect("Failed to restore DiskHashMap from path");

        assert_eq!(
            db2.get(&"hello".to_string()).expect("Failed to get value"),
            Some(vec![1, 2, 3])
        );
        assert_eq!(
            db2.get(&"world".to_string()).expect("Failed to get value"),
            Some(vec![4, 5, 6])
        );
    }

    #[test]
    fn extend() {
        let db_path = PathBuf::from("tests/extend.db");

        let mut db = KVDatabase::new(db_path.clone()).expect("Failed to create DiskHashMap");

        let mut hashmap = HashMap::new();
        hashmap.insert("hello".to_string(), vec![1, 2, 3]);
        hashmap.insert("world".to_string(), vec![4, 5, 6]);

        db.extend(hashmap.clone())
            .expect("Failed to insert hashmap");

        let mut hashmap2 = HashMap::new();
        hashmap2.insert("jeff".to_string(), vec![7, 8, 9]);

        db.extend(hashmap2.clone())
            .expect("Failed to insert hashmap");

        assert_eq!(
            db.get(&"jeff".to_string()).expect("Failed to get value"),
            Some(vec![7, 8, 9])
        );
        assert_eq!(
            db.get(&"hello".to_string()).expect("Failed to get value"),
            Some(vec![1, 2, 3])
        );
        assert_eq!(
            db.get(&"world".to_string()).expect("Failed to get value"),
            Some(vec![4, 5, 6])
        );
    }

    #[test]
    fn iterator() {
        let db_path = PathBuf::from("tests/iterator.db");

        let mut db = KVDatabase::new(db_path.clone()).expect("Failed to create DiskHashMap");

        let mut hashmap = HashMap::new();
        hashmap.insert("hello".to_string(), vec![1, 2, 3]);
        hashmap.insert("world".to_string(), vec![4, 5, 6]);

        db.extend(hashmap.clone())
            .expect("Failed to insert hashmap");

        let mut iter = db.into_iter();

        let first_value = iter
            .next()
            .expect("Failed to get next value")
            .expect("Failed to get value from disk");

        assert!(
            first_value == ("hello".to_string(), vec![1, 2, 3])
                || first_value == ("world".to_string(), vec![4, 5, 6])
        );

        let second_value = iter
            .next()
            .expect("Failed to get next value")
            .expect("Failed to get value from disk");
        let expected = if first_value.0 == "hello" {
            ("world".to_string(), vec![4, 5, 6])
        } else {
            ("hello".to_string(), vec![1, 2, 3])
        };

        assert_eq!(second_value, expected);

        assert!(iter.next().is_none());
    }

    #[test]
    fn insert() {
        let db_path = PathBuf::from("tests/insert.db");

        let mut db = KVDatabase::new(db_path.clone()).expect("Failed to create DiskHashMap");

        let mut hashmap = HashMap::new();
        hashmap.insert("hello".to_string(), vec![1, 2, 3]);
        hashmap.insert("world".to_string(), vec![4, 5, 6]);

        db.insert(hashmap).expect("Failed to insert hashmap");

        assert_eq!(
            db.get(&"hello".to_string()).expect("Failed to get value"),
            Some(vec![1, 2, 3])
        );
        assert_eq!(
            db.get(&"world".to_string()).expect("Failed to get value"),
            Some(vec![4, 5, 6])
        );

        let mut hashmap2 = HashMap::new();
        hashmap2.insert("jeff".to_string(), vec![7, 8, 9]);
        hashmap2.insert("hello".to_string(), vec![10, 11, 12]);

        db.insert(hashmap2).expect("Failed to insert hashmap");

        assert_eq!(
            db.get(&"jeff".to_string()).expect("Failed to get value"),
            Some(vec![7, 8, 9])
        );
        assert_eq!(
            db.get(&"hello".to_string()).expect("Failed to get value"),
            Some(vec![10, 11, 12])
        );
        assert_eq!(
            db.get(&"world".to_string()).expect("Failed to get value"),
            Some(vec![4, 5, 6])
        );
    }

    #[test]
    fn insert_struct() {
        #[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
        struct TestStruct {
            a: u64,
            b: Vec<String>,
        }

        let db_path = PathBuf::from("tests/insert_struct.db");

        let mut db = KVDatabase::new(db_path.clone()).expect("Failed to create DiskHashMap");

        let mut hashmap = HashMap::new();
        hashmap.insert("hello".to_string(), TestStruct { a: 1, b: vec![] });
        hashmap.insert(
            "world".to_string(),
            TestStruct {
                a: 2,
                b: vec!["a".to_string(), "b".to_string()],
            },
        );

        db.insert(hashmap).expect("Failed to insert hashmap");

        assert_eq!(
            db.get(&"hello".to_string()).expect("Failed to get value"),
            Some(TestStruct { a: 1, b: vec![] })
        );
        assert_eq!(
            db.get(&"world".to_string()).expect("Failed to get value"),
            Some(TestStruct {
                a: 2,
                b: vec!["a".to_string(), "b".to_string()]
            })
        );
    }
}
