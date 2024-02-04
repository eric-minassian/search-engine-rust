use std::{io, path::PathBuf};

use clap::{Parser, ValueHint};
use search_engine::{
    inverted_index::disk_inverted_index::DiskInvertedIndex, search_engine::SearchEngine,
};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Restarts the database
    #[arg(short, long, default_value_t = false, requires = "crawled_data_path")]
    restart: bool,

    /// Path to the crawled data
    #[arg(short, long, value_hint = ValueHint::FilePath)]
    crawled_data: Option<PathBuf>,

    /// Path to the database
    #[arg(short, long, value_hint = ValueHint::FilePath)]
    db: PathBuf,

    /// Path to the URL map
    #[arg(short, long, value_hint = ValueHint::FilePath)]
    url_map: PathBuf,
}

fn main() {
    let args = Args::parse();

    let db = if args.restart {
        DiskInvertedIndex::new(args.db, args.url_map, args.crawled_data.unwrap()).unwrap()
    } else {
        DiskInvertedIndex::from_path(args.db, args.url_map).unwrap()
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
