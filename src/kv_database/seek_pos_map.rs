use serde::{Deserialize, Serialize};
use std::{collections::HashMap, hash::Hash};

#[derive(Serialize, Deserialize, Debug)]
pub struct SeekPosMap<K>
where
    K: Hash + Eq,
{
    pub map: HashMap<K, u64>,
    pub pos: u64,
}
