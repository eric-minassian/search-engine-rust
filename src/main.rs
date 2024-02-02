use std::{io, path::PathBuf};

use clap::Parser;
use search_engine::{inverted_index_db::InvertedIndexDatabase, search_engine::SearchEngine};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Restarts the database
    #[arg(short, long, default_value = "false")]
    restart: bool,

    /// Path to the crawled data
    #[arg(long, default_value = "data")]
    crawled_data_path: PathBuf,

    /// Path to the database
    #[arg(long, default_value = "database.db")]
    db_path: PathBuf,

    /// Path to the document index
    #[arg(long, default_value = "doc_index.json")]
    doc_index_path: PathBuf,

    /// Path to the URL map
    #[arg(long, default_value = "url_map.json")]
    url_map_path: PathBuf,
}

fn main() {
    let args = Args::parse();

    let db = if args.restart {
        InvertedIndexDatabase::from_crawled_data(
            args.crawled_data_path,
            args.db_path,
            args.doc_index_path,
            args.url_map_path,
        )
        .unwrap()
    } else {
        InvertedIndexDatabase::from_cache(args.db_path, args.doc_index_path, args.url_map_path)
            .unwrap()
    };

    let mut search = SearchEngine::new(db);
    let mut buffer = String::new();

    loop {
        io::stdin().read_line(&mut buffer).unwrap();

        if buffer.trim() == "exit" {
            break;
        }

        println!("Results for '{}':", buffer.trim());
        // Time the search
        let start = std::time::Instant::now();
        println!("Number of results: {}", search.search(&buffer.trim()).len());
        println!("Time taken: {:?}", start.elapsed());
        buffer.clear();
    }
}
