use clap::{Parser, ValueHint};
use search_engine::{
    error::{Error, Result},
    inverted_index::disk_inverted_index::DiskInvertedIndex,
    search::engine::SearchEngine,
};
use std::{io, path::PathBuf};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Restarts the database
    #[arg(short, long, default_value_t = false, requires = "crawled_data_path")]
    restart: bool,

    /// Path to the crawled data
    #[arg(short, long, default_value = "data", value_hint = ValueHint::FilePath)]
    crawled_data: Option<PathBuf>,

    /// Path to the database
    #[arg(short, long, default_value = "database.db", value_hint = ValueHint::FilePath)]
    db: PathBuf,

    // Path to the seek position file
    #[arg(short, long, default_value = "database.seek", value_hint = ValueHint::FilePath)]
    db_seek: PathBuf,

    /// Path to the URL map
    #[arg(short, long, default_value = "url_map.db", value_hint = ValueHint::FilePath)]
    url_map: PathBuf,

    /// Path to the URL map seek position file
    #[arg(short, long, default_value = "url_map.seek", value_hint = ValueHint::FilePath)]
    url_map_seek: PathBuf,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let db = if args.restart {
        DiskInvertedIndex::new(
            args.db,
            args.db_seek,
            args.url_map,
            args.url_map_seek,
            args.crawled_data
                .ok_or_else(|| Error::Generic("Crawled data path is required".to_string()))?,
        )?
    } else {
        DiskInvertedIndex::from(args.db, args.db_seek, args.url_map, args.url_map_seek)?
    };

    let mut search_engine = SearchEngine::new(db)?;
    let mut input_buffer = String::new();

    loop {
        println!("Enter a search query (type 'exit' to quit):");
        io::stdin()
            .read_line(&mut input_buffer)
            .expect("Failed to read line");

        if input_buffer.trim() == "exit" {
            break;
        }

        let start_time = std::time::Instant::now();

        let search_results = search_engine.search(input_buffer.trim())?;

        println!(
            "Found {} results in {:?}",
            search_results.len(),
            start_time.elapsed()
        );

        println!("Top 10 results:");
        for (i, result) in search_engine
            .search(input_buffer.trim())?
            .iter()
            .take(10)
            .enumerate()
        {
            println!("{}. {:?}", i + 1, result);
        }
        input_buffer.clear();
    }

    Ok(())
}
