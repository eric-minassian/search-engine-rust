use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct Doc {
    pub url: String,
}

impl Doc {
    pub const fn new(url: String) -> Self {
        Self { url }
    }
}

pub type DocID = u64;
pub type TF = u32;
pub type TFIDF = f64;

pub type DocMap = HashMap<DocID, Doc>;
