#[derive(Debug, Clone)]
pub struct SearchResult {
    pub url: String,
    pub score: f64,
}

impl SearchResult {
    pub const fn new(url: String, score: f64) -> Self {
        Self { url, score }
    }
}
