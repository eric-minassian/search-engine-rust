use std::{collections::HashMap, io, path::PathBuf};

use clap::Parser;
use search_engine::{
    database::DiskHashMap,
    inverted_index::{DiskInvertedIndex, TermIndex},
    search_engine::SearchEngine,
};
use serde::{Deserialize, Serialize};

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

#[derive(Serialize, Deserialize, Debug)]
struct Data {
    hello: i32,
    list: Vec<i32>,
}

fn main() {
    let args = Args::parse();

    let db = if args.restart {
        DiskInvertedIndex::new(args.db_path, args.url_map_path, args.crawled_data_path).unwrap()
    } else {
        DiskInvertedIndex::from_path(args.db_path, args.url_map_path).unwrap()
    };

    let mut search = SearchEngine::new(db);
    let mut buffer = String::new();

    println!("Enter Search Query:");

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
