use parquet::file::reader::SerializedFileReader;
use std::{fs::File, path::Path};

#[no_mangle]
pub extern "C" fn read_parquet_int32(filename: *const i8, row_index: i32) -> i32 {
    // Convert C string to Rust string
    let filename_rs = unsafe {
        std::ffi::CStr::from_ptr(filename)
            .to_str()
            .unwrap()
    };

    let path = Path::new(filename_rs);
    let file = File::open(path).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();

    // Assuming a single row group and a single int32 column
    let mut iter = reader.get_row_iter(None).unwrap();
    let row = iter.nth(row_index as usize).unwrap();

    // This should be a match statement of all possible schema types
    if let parquet::record::Row::Single(parquet::record::Field::Int(Some(value))) = row {
        return value;
    }
    return 0;
}

