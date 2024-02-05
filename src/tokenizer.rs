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
            .map(|token| {
                self.stemmer
                    .stem(&token.as_str().to_lowercase())
                    .to_string()
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokenize() {
        let tokenizer = Tokenizer::new().expect("Failed to create tokenizer");
        let tokens = tokenizer.tokenize("I am a test sentence");
        assert_eq!(tokens, vec!["i", "am", "a", "test", "sentenc"]);
    }

    #[test]
    fn test_tokenize_empty() {
        let tokenizer = Tokenizer::new().expect("Failed to create tokenizer");
        let tokens = tokenizer.tokenize("");
        assert_eq!(tokens, Vec::<String>::new());
    }

    #[test]
    fn test_tokenize_punctuation() {
        let tokenizer = Tokenizer::new().expect("Failed to create tokenizer");
        let tokens = tokenizer.tokenize("I am a test sentence!?");
        assert_eq!(tokens, vec!["i", "am", "a", "test", "sentenc"]);
    }

    #[test]
    fn test_tokenize_uppercase() {
        let tokenizer = Tokenizer::new().expect("Failed to create tokenizer");
        let tokens = tokenizer.tokenize("I AM A TEST SENTENCE");
        assert_eq!(tokens, vec!["i", "am", "a", "test", "sentenc"]);
    }

    #[test]
    fn test_stemmer() {
        let tokenizer = Tokenizer::new().expect("Failed to create tokenizer");

        let plurals = vec![
            "caresses",
            "flies",
            "dies",
            "mules",
            "denied",
            "died",
            "agreed",
            "owned",
            "humbled",
            "sized",
            "meeting",
            "stating",
            "siezing",
            "itemization",
            "sensational",
            "traditional",
            "reference",
            "colonizer",
            "plotted",
        ];

        let expected_result = vec![
            "caress", "fli", "die", "mule", "deni", "die", "agre", "own", "humbl", "size", "meet",
            "state", "siez", "item", "sensat", "tradit", "refer", "colon", "plot",
        ];

        let stems: Vec<String> = plurals
            .iter()
            .map(|word| tokenizer.stemmer.stem(&word.to_lowercase()).to_string())
            .collect();

        assert_eq!(expected_result, stems);
    }
}
