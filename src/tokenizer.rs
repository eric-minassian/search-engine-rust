use crate::error::{Error, Result};
use regex::Regex;
use rust_stemmers::Stemmer;

pub struct Tokenizer {
    stemmer: Stemmer,
    regex: Regex,
}

impl Tokenizer {
    pub fn new() -> Result<Self> {
        Ok(Self {
            stemmer: Stemmer::create(rust_stemmers::Algorithm::English),
            regex: Regex::new(r"\b\w+\b")
                .map_err(|e| Error::Generic(format!("Failed to compile regex: {e}")))?,
        })
    }

    pub fn tokenize(&self, text: &str) -> Vec<String> {
        self.regex
            .find_iter(text)
            .map(|token| self.stemmer.stem(token.as_str()).to_lowercase())
            .collect()
    }
}
