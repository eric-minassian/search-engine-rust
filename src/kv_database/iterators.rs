use serde::{Deserialize, Serialize};
use std::collections::hash_map::Iter as HashMapIter;
use std::fmt::Display;
use std::fs::File;
use std::hash::Hash;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::marker::PhantomData;

use super::disk_hash_map::KVDatabase;
use super::seek_pos_map::SeekPos;

use crate::error::{Error, Result};

pub struct KVDbIterator<'a, K, V> {
    seek_pos_iter: HashMapIter<'a, K, SeekPos>,
    database: &'a mut BufReader<File>,
    _marker: PhantomData<*const V>, // Use PhantomData to mark V's usage without ownership
}

impl<'a, K, V> Iterator for KVDbIterator<'a, K, V>
where
    K: Serialize + for<'de> Deserialize<'de> + Eq + Hash + Display + Clone,
    V: Serialize + for<'de> Deserialize<'de> + Clone,
{
    type Item = Result<(K, V)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.seek_pos_iter.next().map(|(key, seek_pos)| {
            self.database
                .seek(SeekFrom::Start(seek_pos.pos))
                .map_err(|e| Error::Generic(e.to_string()))?;

            let mut buffer = vec![0; seek_pos.len as usize];
            self.database
                .read_exact(&mut buffer)
                .map_err(|e| Error::Generic(e.to_string()))?;

            let value: V =
                bincode::deserialize(&buffer).map_err(|e| Error::Generic(e.to_string()))?;

            Ok((key.clone(), value))
        })
    }
}

impl<'a, K, V> IntoIterator for &'a mut KVDatabase<K, V>
where
    K: Serialize + for<'de> Deserialize<'de> + Eq + Hash + Display + Clone,
    V: Serialize + for<'de> Deserialize<'de> + Clone,
{
    type Item = Result<(K, V)>;
    type IntoIter = KVDbIterator<'a, K, V>;

    fn into_iter(self) -> Self::IntoIter {
        KVDbIterator {
            seek_pos_iter: self.seek_pos_map.iter(),
            database: &mut self.database,
            _marker: PhantomData,
        }
    }
}
