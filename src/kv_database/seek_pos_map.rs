use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Debug)]
pub struct SeekPos {
    pub pos: u64,
    pub len: u64,
}

impl SeekPos {
    pub const fn new(pos: u64, len: u64) -> Self {
        Self { pos, len }
    }
}

pub type SeekPosMap<K> = HashMap<K, SeekPos>;
