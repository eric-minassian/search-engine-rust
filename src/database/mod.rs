use std::{
    collections::HashMap,
    fmt::Display,
    fs::{remove_file, rename, File},
    hash::Hash,
    io::{self, BufRead, BufReader, BufWriter, Seek, SeekFrom, Write},
    marker::PhantomData,
    path::PathBuf,
};

use rev_buf_reader::RevBufReader;
use serde::{Deserialize, Serialize};
use serde_json::to_string;

pub(crate) const KEY_DELIMITER: &str = ":";
pub(crate) const TEMP_FILE_SUFFIX: &str = "tmp";

#[derive(Serialize, Deserialize, Debug)]
pub struct SeekPosMap<K>
where
    K: Hash + Eq,
{
    pub map: HashMap<K, u64>,
    pub map_pos: u64,
}

#[derive(Debug)]
pub struct DiskHashMap<K, V>
where
    K: Serialize + for<'de> Deserialize<'de> + Eq + Hash + Display + Clone,
    V: Serialize + for<'de> Deserialize<'de> + Clone,
{
    db_path: PathBuf,
    seek_pos_map: SeekPosMap<K>,
    database: BufReader<File>,
    iter_seek_pos: u64,
    _marker: PhantomData<V>,
}

impl<K, V> Iterator for DiskHashMap<K, V>
where
    K: Serialize + for<'de> Deserialize<'de> + Eq + Hash + Display + Clone + From<String>,
    V: Serialize + for<'de> Deserialize<'de> + Clone,
{
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        if self.iter_seek_pos >= self.seek_pos_map.map_pos {
            return None;
        }

        self.database
            .seek(SeekFrom::Start(self.iter_seek_pos))
            .expect("Failed to seek to next position");

        let mut buffer = String::new();
        self.database
            .read_line(&mut buffer)
            .expect("Failed to read line from database");

        let (key_len, key, value) = match buffer.split_once(KEY_DELIMITER) {
            Some((key_len, rest)) => {
                let key_len = key_len.parse::<u64>().expect("Failed to parse key length");
                let key = &rest[..key_len as usize];
                let value = &rest[key_len as usize..];

                (key_len, key, value)
            }
            None => {
                panic!("KEY_DELIMITER not found");
            }
        };

        self.iter_seek_pos = self
            .database
            .stream_position()
            .expect("Failed to get stream position");

        Some((
            key.to_string().into(),
            serde_json::from_str(value).expect("Failed to deserialize value"),
        ))
    }
}

impl<K, V> DiskHashMap<K, V>
where
    K: Serialize + for<'de> Deserialize<'de> + Eq + Hash + Display + Clone,
    V: Serialize + for<'de> Deserialize<'de> + Clone,
{
    pub fn new(db_path: PathBuf) -> io::Result<Self> {
        let db_file = File::create(&db_path)?;
        let mut writer = BufWriter::new(db_file);
        let empty_map: HashMap<K, V> = HashMap::new();
        serde_json::to_writer(&mut writer, &empty_map)?;

        Ok(Self {
            seek_pos_map: SeekPosMap {
                map: HashMap::new(),
                map_pos: 0,
            },
            database: BufReader::new(File::create(&db_path)?),
            db_path,
            iter_seek_pos: 0,
            _marker: PhantomData,
        })
    }

    pub fn from_path(db_path: PathBuf) -> io::Result<Self> {
        let mut database = RevBufReader::new(File::open(&db_path)?);
        let mut buffer = String::new();
        database.read_line(&mut buffer)?;
        let seek_pos_map: SeekPosMap<K> = serde_json::from_str(&buffer)?;

        Ok(Self {
            database: BufReader::new(File::open(&db_path)?),
            seek_pos_map,
            db_path,
            iter_seek_pos: 0,
            _marker: PhantomData,
        })
    }

    pub fn get(&mut self, key: &K) -> io::Result<Option<V>> {
        if let Some(seek_pos) = self.seek_pos_map.map.get(key) {
            let key_len = key.to_string().len();

            self.database.seek(SeekFrom::Start(*seek_pos))?;

            let mut data = String::new();
            self.database.read_line(&mut data)?;
            let value_str = match data.split_once(KEY_DELIMITER) {
                Some((_, value)) => &value[key_len..],
                None => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "KEY_DELIMITER not found",
                    ))
                }
            }
            .trim();

            Ok(Some(serde_json::from_str(value_str)?))
        } else {
            Ok(None)
        }
    }

    pub fn insert(&mut self, hashmap: HashMap<K, V>) -> io::Result<()> {
        if hashmap.is_empty() {
            return Ok(());
        }

        let temp_db_path = self.db_path.with_extension(TEMP_FILE_SUFFIX);
        let mut temp_db_writer = BufWriter::new(File::create(&temp_db_path)?);

        let mut new_seek_pos_map = SeekPosMap {
            map: HashMap::new(),
            map_pos: 0,
        };

        for (key, &offset) in &self.seek_pos_map.map {
            if !hashmap.contains_key(key) {
                self.database.seek(SeekFrom::Start(offset))?;
                let mut line = String::new();
                self.database.read_line(&mut line)?;
                new_seek_pos_map
                    .map
                    .insert(key.clone(), temp_db_writer.stream_position()?);
                write!(temp_db_writer, "{}", line)?;
            }
        }

        for (key, value) in hashmap.into_iter() {
            let key_len = key.to_string().len();

            let new_value_str = to_string(&value)?;

            let stream_pos = temp_db_writer.stream_position()?;

            writeln!(
                temp_db_writer,
                "{}{}{}{}",
                key_len, KEY_DELIMITER, &key, new_value_str
            )?;

            new_seek_pos_map.map.insert(key, stream_pos);
        }

        // Write the new seek_pos_map to the end of the file
        new_seek_pos_map.map_pos = temp_db_writer.stream_position()?;
        serde_json::to_writer(&mut temp_db_writer, &new_seek_pos_map)?;

        temp_db_writer.flush()?;

        remove_file(&self.db_path)?;
        rename(temp_db_path, &self.db_path)?;

        self.database = BufReader::new(File::open(&self.db_path)?);
        self.seek_pos_map = new_seek_pos_map;

        Ok(())
    }
}

impl<K, V, T> DiskHashMap<K, V>
where
    K: Serialize + for<'de> Deserialize<'de> + Eq + Hash + Display + Clone,
    V: Serialize + Extend<T> + for<'de> Deserialize<'de> + IntoIterator<Item = T> + Clone,
{
    pub fn extend(&mut self, hashmap: HashMap<K, V>) -> io::Result<()> {
        if hashmap.is_empty() {
            return Ok(());
        }

        let temp_db_path = self.db_path.with_extension(TEMP_FILE_SUFFIX);
        let mut temp_db_writer = BufWriter::new(File::create(&temp_db_path)?);

        let mut new_seek_pos_map = SeekPosMap {
            map: HashMap::new(),
            map_pos: 0,
        };

        for (key, &offset) in &self.seek_pos_map.map {
            if !hashmap.contains_key(key) {
                self.database.seek(SeekFrom::Start(offset))?;
                let mut line = String::new();
                self.database.read_line(&mut line)?;

                new_seek_pos_map
                    .map
                    .insert(key.clone(), temp_db_writer.stream_position()?);
                write!(temp_db_writer, "{}", line)?;
            }
        }

        for (key, value) in hashmap.into_iter() {
            let key_len = key.to_string().len();

            let new_value = if let Some(&offset) = self.seek_pos_map.map.get(&key) {
                self.database.seek(SeekFrom::Start(offset))?;
                let mut old_value_str = String::new();
                self.database.read_line(&mut old_value_str)?;
                let mut old_value = serde_json::from_str::<V>(
                    &old_value_str.split_once(KEY_DELIMITER).unwrap().1[key_len..].trim(),
                )?;
                old_value.extend(value);

                old_value
            } else {
                value
            };

            let new_value_str = to_string(&new_value)?;

            let stream_pos = temp_db_writer.stream_position()?;

            writeln!(
                temp_db_writer,
                "{}{}{}{}",
                key_len, KEY_DELIMITER, &key, new_value_str
            )?;

            new_seek_pos_map.map.insert(key, stream_pos);
        }

        // Write the new seek_pos_map to the end of the file
        new_seek_pos_map.map_pos = temp_db_writer.stream_position()?;
        serde_json::to_writer(&mut temp_db_writer, &new_seek_pos_map)?;

        temp_db_writer.flush()?;

        remove_file(&self.db_path)?;
        rename(temp_db_path, &self.db_path)?;

        self.database = BufReader::new(File::open(&self.db_path)?);
        self.seek_pos_map = new_seek_pos_map;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::hash;

    use tests::DiskHashMap;

    use super::*;

    #[test]
    fn basic_str() {
        let db_path = PathBuf::from("tests/basic_str.db");

        let mut db = DiskHashMap::new(db_path.clone()).expect("Failed to create DiskHashMap");

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

        let mut db = DiskHashMap::new(db_path.clone()).expect("Failed to create DiskHashMap");

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

        let mut db = DiskHashMap::new(db_path.clone()).expect("Failed to create DiskHashMap");

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

        let mut db = DiskHashMap::new(db_path.clone()).expect("Failed to create DiskHashMap");

        let mut hashmap = HashMap::new();
        hashmap.insert("hello".to_string(), vec![1, 2, 3]);
        hashmap.insert("world".to_string(), vec![4, 5, 6]);

        db.extend(hashmap.clone())
            .expect("Failed to insert hashmap");

        let mut db2 = DiskHashMap::from_path(db_path.clone())
            .expect("Failed to restore DiskHashMap from path");

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

        let mut db = DiskHashMap::new(db_path.clone()).expect("Failed to create DiskHashMap");

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

        let mut db = DiskHashMap::new(db_path.clone()).expect("Failed to create DiskHashMap");

        let mut hashmap = HashMap::new();
        hashmap.insert("hello".to_string(), vec![1, 2, 3]);
        hashmap.insert("world".to_string(), vec![4, 5, 6]);

        db.extend(hashmap.clone())
            .expect("Failed to insert hashmap");

        let mut iter = db.into_iter();

        let first_value = iter.next().expect("Failed to get next value");

        assert!(
            first_value == ("hello".to_string(), vec![1, 2, 3])
                || first_value == ("world".to_string(), vec![4, 5, 6])
        );

        let second_value = iter.next().expect("Failed to get next value");
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

        let mut db = DiskHashMap::new(db_path.clone()).expect("Failed to create DiskHashMap");

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
    }
}
