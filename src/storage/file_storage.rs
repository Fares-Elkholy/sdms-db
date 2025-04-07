// This file will be replaced by the runner

use std::{
    fs::{create_dir_all, OpenOptions},
    io::{BufReader, BufWriter, Read, Seek, Write},
    path::PathBuf,
};

use uuid::Uuid;

use crate::storage::{FileBasedStorage, FileHandle};
use crate::DatabaseError;

impl FileHandle {
    pub fn new(file_uuid: Uuid) -> Self {
        FileHandle { file_uuid }
    }

    pub fn default() -> Self {
        FileHandle {
            file_uuid: Uuid::new_v4(),
        }
    }

    pub fn to_path_buf(&self) -> String {
        let mut file = self.file_uuid.simple().to_string();
        file.insert_str(2, "/");
        file.insert_str(5, "/");
        file.push_str(".bin");

        file
    }
}

impl FileBasedStorage {
    pub fn new(base_path: PathBuf) -> Self {
        FileBasedStorage { base_path }
    }

    pub fn read_file(&self, file: &FileHandle) -> Result<impl Read + Seek, DatabaseError> {
        let mut path = PathBuf::from(&self.base_path);
        path.push(file.to_path_buf());

        let file = OpenOptions::new().read(true).open(path)?;
        Ok(BufReader::new(file))
    }

    pub fn write_file(&self, data: &[u8]) -> Result<FileHandle, DatabaseError> {
        let file_handle = FileHandle {
            file_uuid: Uuid::new_v4(),
        };

        let mut path = PathBuf::from(&self.base_path);
        path.push(file_handle.to_path_buf());
        create_dir_all(
            path.parent()
                .expect("valid parent directories should exist"),
        )?;

        let mut file = BufWriter::new(OpenOptions::new().create_new(true).write(true).open(path)?);
        file.write_all(data)?;

        Ok(file_handle)
    }
}

impl Default for FileBasedStorage {
    fn default() -> Self {
        FileBasedStorage {
            base_path: PathBuf::from("./target/sdms"),
        }
    }
}
