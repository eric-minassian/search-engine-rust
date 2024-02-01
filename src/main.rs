use search_engine::inverted_index_db::InvertedIndexDatabase;

fn main() {
    // println!("Hello, world!");
    let mut db = InvertedIndexDatabase::new(
        "database.db".to_string(),
        "doc_index.json".to_string(),
        "url_map.json".to_string(),
        true,
    )
    .unwrap();

    db.initialize()
}
