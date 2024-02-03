use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct Doc {
    pub url: String,
}

impl Doc {
    pub fn new(url: String) -> Self {
        Doc { url }
    }
}

type DocID = u64;

pub type DocMap = HashMap<DocID, Doc>;
