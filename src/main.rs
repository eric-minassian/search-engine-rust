use clap::{Parser, ValueHint};
use search_engine::{
    error::{Error, Result},
    inverted_index::disk_inverted_index::DiskInvertedIndex,
    search_engine::SearchEngine,
};
use std::{io, path::PathBuf};

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

fn main() -> Result<()> {
    let args = Args::parse();

    let db = if args.restart {
        DiskInvertedIndex::new(
            args.db,
            args.url_map,
            args.crawled_data
                .ok_or_else(|| Error::Generic("Crawled data path is required".to_string()))?,
        )?
    } else {
        DiskInvertedIndex::from_path(args.db, args.url_map)?
    };

    let mut search = SearchEngine::new(db)?;
    let mut buffer = String::new();

    println!("Enter Search Query:");

    loop {
        io::stdin()
            .read_line(&mut buffer)
            .expect("Failed to read line");

        if buffer.trim() == "exit" {
            break;
        }

        println!("Results for '{}':", buffer.trim());
        // Time the search
        let start = std::time::Instant::now();
        println!("Number of results: {}", search.search(buffer.trim())?.len());
        println!("Time taken: {:?}", start.elapsed());
        // Print top 10 results
        for (i, result) in search.search(buffer.trim())?.iter().take(10).enumerate() {
            println!("{}. {:?}", i + 1, result);
        }
        buffer.clear();
    }

    Ok(())
}
