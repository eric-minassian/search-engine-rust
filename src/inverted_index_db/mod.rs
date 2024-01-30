use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io,
    path::Path,
};

struct InvertedIndexDatabase {
    path: String,
    index_path: String,
    index: HashMap<String, u64>,
    restart: bool,
    database: Option<File>,
}

impl InvertedIndexDatabase {
    fn new(path: String, index_path: String, restart: bool) -> Self {
        InvertedIndexDatabase {
            path,
            index_path,
            restart,
            index: HashMap::new(),
            database: None,
        }
    }

    fn open(&mut self) -> io::Result<()> {
        if self.restart {
            if Path::new(&self.path).exists() {
                std::fs::remove_file(&self.path)?;
            }
            if Path::new(&self.index_path).exists() {
                std::fs::remove_file(&self.index_path)?;
            }
            self.database = Some(File::create(&self.path)?);
        } else {
            self.database = Some(OpenOptions::new().append(true).open(&self.path)?);
        }

        Ok(())
    }
}
