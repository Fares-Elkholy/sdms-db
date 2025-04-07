use std::{io::{Read, Write, Seek, SeekFrom}, str, string, vec, fs::File};
use super::{Columns, DataFile, DataFileHeader};
use crate::{DatabaseError, RowID, TableChunk, TypeID, Value};

impl DataFile {
    pub fn new(header: DataFileHeader, data: TableChunk) -> Self {
        DataFile { header, data }
    }

    /// Serializes [`DataFile`] as specified in lab slides.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut result: Vec<u8> = vec![];
        // add header
        result.extend(self.header.to_bytes());

        let _no_rows = self.header.rows;
        let _no_cols = self.header.columns;

        // initialize start idx of the column
        let mut col_start_idx: u64 = 24 + (16*self.header.columns);
        
        // iterate over each column
        for col in 0..self.data.len() { // col access
            // debugging, ignore
            let tail = &result[result.len().saturating_sub(100)..];
            println!("Tail: {:?}", tail);

            // represents the idx in result where start_idx bytes should be placed
            let idx_start_idx = 32 + (16*col);
            // update found start_idx of respective column
            result[idx_start_idx..idx_start_idx+8].copy_from_slice(&col_start_idx.to_le_bytes());
            
            match &self.data[col][0]    {
                Value::Int(_) => {
                    for row in 0..self.data[col].len() { // row access
                        // simply add int
                        if let Value::Int(int) = self.data[col][row] {
                            result.extend(int.to_le_bytes());
                            col_start_idx = col_start_idx + 4;
                        }
                        else {
                            panic!("error")
                        }
                    }
                    // at the end, increment col_start_idx by adding 4*no. of rows
                }

                Value::UInt(_) => {
                    for row in 0..self.data[col].len() { // row access

                        // simply add UInt
                        if let Value::UInt(uint) = self.data[col][row] {
                            result.extend(uint.to_le_bytes());
                            col_start_idx = col_start_idx + 4;
                        }
                        else {
                            panic!("error")
                        }
                    }
                    // at the end, increment col_start_idx by adding 4*no. of rows
                }
                
                Value::Varchar(_) => {
                    for row in 0..self.data[col].len() { // row access
                    if let Value::Varchar(s) = &self.data[col][row] {
                    let length_bytes: [u8; 8] = s.to_string().len().to_le_bytes();
                    // first add length bytes for string
                    result.extend(length_bytes);  
                    let length= u64::from_le_bytes(length_bytes);

                    // then add string decoded in utf8                 
                    result.extend(s.to_string().as_bytes()); // TODO: Possible error here as string is in wrong ordering (little/big endianness)

                    // for each string, add length + 8 (bytes of char length) to col_start_idx
                    col_start_idx = col_start_idx + length + 8; 
                    }
                    else {
                        panic!("error!!!!")
                    }
                }
                }
                Value::RowID(_) => {
                    for row in 0..self.data[col].len() { // row access

                        // simply add RowID
                        if let Value::RowID(rowid) = self.data[col][row] {
                            result.extend(rowid.0.to_le_bytes());
                            col_start_idx = col_start_idx + 8; // 8 bytes added as each row id is 8 bytes long
                        }
                        else {
                            panic!("error")
                        }
                    }
                } 
            }    
        }
        result
    }

    /// Deserialize all columns into [`DataFile`] as specified in lab slides.
    pub fn parse<F: Seek + Read>(file: &mut F) -> Result<Self, DatabaseError> {
        // read file
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;

        // parse header
        let mut header = DataFileHeader::parse(&bytes)?;

        let mut chunk: Vec<Vec<Value>> = vec![];

        // parse data per column, assumes start index is
        for (type_id, mut start_idx) in &header.column_info {
            // used to save values in this column 
            let mut column: Vec<Value> = vec![]; 
            match type_id {
                TypeID::Int => {
                    for idx in 0..header.rows as usize {
                        let pos  = start_idx + 4*idx;
                        let int_bytes: [u8; 4] = bytes[pos..pos+4].try_into().expect("Slice should be exactly 4 bytes");
                        // add int to column
                        column.push(Value::Int(i32::from_le_bytes(int_bytes)));
                    }  
                }
                TypeID::UInt => {
                    for idx in 0..header.rows as usize {
                        let pos = start_idx + 4*idx;
                        let uint_bytes: [u8; 4] = bytes[pos..pos+4].try_into().expect("Slice should be exactly 4 bytes");
                        // add uint to column
                        column.push(Value::UInt(u32::from_le_bytes(uint_bytes)));
                    }
                }
                TypeID::RowID => {
                    for idx in 0..header.rows as usize {
                        let pos = start_idx + 8*idx;
                        let rowid_bytes: [u8; 8] = bytes[pos..pos+8].try_into().expect("Slice should be exactly 8 bytes");
                        // add rowid to column
                        column.push(Value::RowID(RowID(u64::from_le_bytes(rowid_bytes))));
                    }
                }
                TypeID::Varchar => {
                    for _ in 0..header.rows as usize {
                        // calculate size of this string
                        let size_bytes: [u8; 8] = bytes[start_idx..start_idx+8]
                            .try_into()
                            .expect("Slice for String size should be exactly 8 bytes");
                        let size = u64::from_le_bytes(size_bytes) as usize;

                        let string_start = start_idx + 8;

                        let str_bytes = &bytes[string_start..string_start+size];
                        let s = std::str::from_utf8(str_bytes)
                            .expect("Invalid UTF8 string")
                            .to_string();
                        column.push(Value::Varchar(std::rc::Rc::new(s)));
                        
                        // set start_idx to next string
                        start_idx += 8 + size;
                    }
                }
            }
            // push materialized column
            chunk.push(column);
        }

        header.columns = chunk.len() as u64;
        Ok(DataFile::new(header, chunk))
    }

    /// Deserialize selected columns into [`DataFile`] as specified in lab slides.
    pub fn parse_columns<F: Seek + Read>(
        file: &mut F,
        column_indexes: Columns,
    ) -> Result<Self, DatabaseError> {
        const COLUMN_INF0_START: usize = 24;

        // // read no of rows
        // let row_bytes: [u8; 8] = bytes[8..16].try_into().expect("Slice for row bytes should be exactly 8 bytes");
        // let rows = u64::from_le_bytes(row_bytes);

        match column_indexes {
            Columns::Selection(vec) => {


                // read file
                let mut bytes = Vec::new();
                file.read_to_end(&mut bytes)?;

                // parse header
                let header = DataFileHeader::parse(&bytes)?;  

                // result chunk
                let mut chunk: TableChunk = vec![vec![]; header.columns as usize]; 
                // chunk with selected columns
                let mut selected_chunk: TableChunk = vec![];

                for col in &vec {
                    let mut column: Vec<Value> = vec![]; 


                    let (type_id, mut start_idx) = header.column_info[*col];

                    // // get type_id
                    // let type_id_bytes: [u8; 8] = bytes[col_info_start..col_info_start+8].try_into().expect("Slice for Column type should be exactly 8 bytes");
                    // let type_id = TypeID::try_from(u64::from_le_bytes(type_id_bytes)).unwrap();
                    
                    // // get start idx
                    // let start_idx_bytes: [u8; 8] = bytes[col_info_start+8..col_info_start+16].try_into().expect("Slice for Start idx should be exactly 8 bytes");
                    // let mut start_idx = u64::from_le_bytes(start_idx_bytes) as usize;
                    
                    // parse column depending on type_id and start_idx
                    match type_id {
                        TypeID::Int => {
                            for idx in 0..header.rows as usize {
                                let pos= start_idx + 4*idx;
                                let int_bytes: [u8; 4] = bytes[pos..pos+4].try_into().expect("Slice should be exactly 4 bytes");
                                // add int to column
                                column.push(Value::Int(i32::from_le_bytes(int_bytes)));
                            }  
                        }
                        TypeID::UInt => {
                            for idx in 0..header.rows as usize {
                                let pos = start_idx + 4*idx;
                                let uint_bytes: [u8; 4] = bytes[pos..pos+4].try_into().expect("Slice should be exactly 4 bytes");
                                // add uint to column
                                column.push(Value::UInt(u32::from_le_bytes(uint_bytes)));
                            }
                        }
                        TypeID::Varchar => {
                            for _ in 0..header.rows as usize {
                                // calculate size of this string
                                let size_bytes: [u8; 8] = bytes[start_idx..start_idx+8]
                                    .try_into()
                                    .expect("Slice for String size should be exactly 8 bytes");
                                let size = u64::from_le_bytes(size_bytes) as usize;
        
                                let string_start = start_idx + 8;
        
                                let str_bytes = &bytes[string_start..string_start+size];
                                let s = std::str::from_utf8(str_bytes)
                                    .expect("Invalid UTF8 string")
                                    .to_string();
                                column.push(Value::Varchar(std::rc::Rc::new(s)));
                                
                                // set start_idx to next string
                                start_idx += 8 + size;
                            }
                        }
                        TypeID::RowID => {
                            for idx in 0..header.rows as usize {
                                let pos = start_idx + 8*idx;
                                let rowid_bytes: [u8; 8] = bytes[pos..pos+8].try_into().expect("Slice should be exactly 8 bytes");
                                // add uint to column
                                column.push(Value::RowID(RowID(u64::from_le_bytes(rowid_bytes))));
                            }
                        }
                    }
                    selected_chunk.push(column);
                }
                
                let mut selected_idx = 0;
                for col in 0..header.columns as usize {
                    if vec.contains(&col) {
                        chunk[col] = selected_chunk[selected_idx].clone();
                        selected_idx += 1;
                    }
                }

                Ok(DataFile::new(header, chunk))
            }
            Columns::All => {
                DataFile::parse(file)
            }
        }
    }
}

impl DataFileHeader {
    const MAGIC: [u8; 8] = [0x53, 0x44, 0x4d, 0x53, 0x19, 0x03, 0x4a, 0x53];
    const HDR_LEN: usize = Self::MAGIC.len() + 16; // 8 byte for rows, 8 bytes for columns
    const COLUMN_INFO_LEN: usize = 16;

    pub fn new(rows: u64, columns: u64, column_info: Vec<(TypeID, usize)>) -> Self {
        DataFileHeader {
            rows,
            columns,
            column_info,
        }
    }

    fn to_bytes(&self) -> Vec<u8> {
        let mut result: Vec<u8> = vec![];

        // START HEADER

        // add magic number
        result.extend(DataFileHeader::MAGIC);
        // add 8 bytes each for rows and column length
        result.extend(self.rows.to_le_bytes()); 
        result.extend(self.columns.to_le_bytes());
        
        // END HEADER

        // add coloumn info
        for col in self.column_info.clone() {
            // add typeid
            result.extend((col.0 as u64).to_le_bytes());
            
            // add dummy start indizes
            result.extend((col.1 as u64).to_le_bytes());
        }

        result
    }

    fn parse(bytes: &[u8]) -> Result<Self, DatabaseError> {
        // assert magic is there
        if &bytes[0..8] != &DataFileHeader::MAGIC {
            panic!("Header is incorrect!")
        }
        
        // set dummy no. of rows/cols
        let rows = u64::from_le_bytes(bytes[8..16].try_into().expect("slice with incorrect length"));
        let cols = u64::from_le_bytes(bytes[16..24].try_into().expect("slice with incorrect length"));    
        
        // add column infos
        let mut col_info: Vec<(TypeID, usize)> = vec![];
        // go to first int
        let mut start_string: usize = 24 + (16*cols as usize);
        for i in 0..cols {
            let pos = 24 + (i * 16) as usize;

            let type_id: TypeID = TypeID::try_from(u64::from_le_bytes(bytes[pos..pos+8].try_into().unwrap())).unwrap();
            // if let TypeID::Varchar = type_id {
            //     // find the length of the string
            //     let length_bytes: [u8; 8] = bytes[start_string..start_string+8].try_into().expect("error");
            //     let length = u64::from_le_bytes(length_bytes) as usize; 
            //     col_info.push((type_id, start_string));
                
            //     // add length and 8 to start string
            //     start_string += length + 8;
            // } else {
                let start = usize::from_le_bytes(bytes[pos+8..pos+16].try_into().unwrap()); // fill start index
                col_info.push((type_id, start));
        }

        Ok(DataFileHeader::new(rows, cols, col_info))

    } 
}

fn write_binary_datafile(bytes: Vec<u8>, file_name: &str) -> std::io::Result<()> {
    // Create a new String to hold the hex representation.
    let mut hex_output = String::new();
    
    for byte in bytes {
        // Convert each byte to a two-digit hex string.
        // "{:02x}" ensures a leading zero if needed.
        hex_output.push_str(&format!("{:02x}", byte));
        // Optionally add a space between each hex value for readability.
        hex_output.push(' ');
    }
    
    // Remove the trailing space, if any.
    let hex_output = hex_output.trim();
    
    // Create or overwrite the file and write the hex string (as ASCII text) into it.
    let mut file = File::create(file_name)?;
    file.write_all(hex_output.as_bytes())?;
    
    Ok(())
}

#[cfg(test)]
mod tests {

    use std::{
        fs::OpenOptions,
        io::{BufReader, Cursor, Read, Seek},
        rc::Rc,
    };

    use crate::storage::{data::write_binary_datafile, DataFile, DataFileHeader};
    use crate::{DatabaseError, TypeID, Value};
    use rand::{
        distributions::{Standard, Uniform},
        prelude::Distribution,
        rngs::StdRng,
        thread_rng, Rng, RngCore, SeedableRng,
    };
    use rand_utf8::rand_utf8;

    #[test]
    fn serialize_deserialize_varchar() -> Result<(), DatabaseError> {
        let mut seed_rng: rand::prelude::ThreadRng = thread_rng();
        let mut seed = [0u8; 32];
        seed_rng.fill_bytes(&mut seed);
        println!("Seed: {seed:?}");
        let mut rng = StdRng::from_seed(seed);

        let distribution = Uniform::new_inclusive(10usize, 80);
        let length: Vec<_> = (0..20_000).map(|_| distribution.sample(&mut rng)).collect();

        let mut keys = vec![];
        for l in length {
            keys.push(Value::Varchar(Rc::new(rand_utf8(&mut rng, l).to_string())));
        }

        let binding = keys.clone();
        let mut original_keys = binding.chunks_exact(1_000);

        let chunks = keys.chunks_exact(1_000);

        let mut files = vec![];

        for c in chunks {
            let hdr = DataFileHeader::new(1_000, 1, vec![(TypeID::Varchar, 0)]);
            let data = vec![Vec::from(c)];
            let datafile = DataFile::new(hdr, data);

            files.push(datafile.to_bytes());
        }

        for f in files {
            write_binary_datafile(f.clone(), "output.bin")?;
            let mut file = Cursor::new(f);
            let chunk = DataFile::parse(&mut file)?;

            assert_eq! {chunk.data, vec![original_keys.next().expect("there should be original keys")]};
        }

        Ok(())
    }

    #[test]
    fn serialize_deserialize_multicolumn() -> Result<(), DatabaseError> {
        let mut seed_rng: rand::prelude::ThreadRng = thread_rng();
        let mut seed = [0u8; 32];
        seed_rng.fill_bytes(&mut seed);
        println!("Seed: {seed:?}");
        let mut rng = StdRng::from_seed(seed);

        let distribution = Uniform::new_inclusive(10usize, 80);
        let length: Vec<_> = (0..20_000).map(|_| distribution.sample(&mut rng)).collect();

        let mut column_0 = vec![];
        for l in length {
            // push string of random length between 10-80 chars
            column_0.push(Value::Varchar(Rc::new(rand_utf8(&mut rng, l).to_string())));
        }

        let column_1: Vec<_> = (0..20_000)
            .map(|_| {
                // add random UInts
                let r = rng.sample::<u32, _>(Standard);
                Value::UInt(r)
            })
            .collect();

        let binding_column_0 = column_0.clone();
        let mut original_column_0 = binding_column_0.chunks_exact(1_000);

        let binding_column_1 = column_1.clone();
        let mut original_column_1 = binding_column_1.chunks_exact(1_000);

        let chunks_column_0 = column_0.chunks_exact(1_000);
        let chunks_column_1 = column_1.chunks_exact(1_000);

        let chunk_iter = chunks_column_0.zip(chunks_column_1);

        let mut files = vec![];
        for (c0, c1) in chunk_iter {
            let hdr = DataFileHeader::new(1_000, 2, vec![(TypeID::Varchar, 0), (TypeID::UInt, 0)]);
            let data = vec![Vec::from(c0), Vec::from(c1)];
            let datafile = DataFile::new(hdr, data);

            files.push(datafile.to_bytes()); // serialize
        }

        for f in files {
            let mut file = Cursor::new(f);
            let chunk = DataFile::parse(&mut file)?; // deserialize

            assert_eq! {chunk.data,vec![original_column_0.next().expect("there should be original keys"),original_column_1.next().expect("there should be original keys")]};
        }

        Ok(())
    }

    #[test]
    #[should_panic]
    fn deserialize_invalid() {
        let Ok(file) = OpenOptions::new()
            .read(true)
            .open("./test_files/invalid_test_file.example")
        else {
            return;
        };

        let mut reader = BufReader::new(file);

        let Ok(_) = DataFile::parse(&mut reader) else {
            return;
        };
    }

    #[test]
    fn deserialize_serialize() -> Result<(), DatabaseError> {
        let file = OpenOptions::new()
            .read(true)
            .open("./test_files/valid_test_file.example")?;

        let mut reader = BufReader::new(file);

        let mut original_byte_stream = Vec::new();
        reader.read_to_end(&mut original_byte_stream)?;
        reader.seek(std::io::SeekFrom::Start(0))?;

        let data_file = DataFile::parse(&mut reader)?;
        let byte_stream = data_file.to_bytes();

        assert_eq! {original_byte_stream, byte_stream};

        Ok(())
    }

    #[test]
    fn deserialize_serialize_multicolumn() -> Result<(), DatabaseError> {
        let file = OpenOptions::new()
            .read(true)
            .open("./test_files/multi_column_file.example")?;

        let mut reader = BufReader::new(file);

        let mut original_byte_stream = Vec::new();
        reader.read_to_end(&mut original_byte_stream)?;
        reader.seek(std::io::SeekFrom::Start(0))?;

        let data_file = DataFile::parse(&mut reader)?;
        let byte_stream = data_file.to_bytes();

        assert_eq! {original_byte_stream, byte_stream};

        Ok(())
    }
}

// DO NOT REMOVE OR EDIT THE FOLLOWING LINES!
#[cfg(test)]
mod private_tests_data;
