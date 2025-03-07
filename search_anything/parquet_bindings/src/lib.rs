use parquet::file::reader::{SerializedFileReader, FileReader};
use std::{fs::File, path::Path};

// #[no_mangle]
// pub extern "C" fn read_parquet_int32(filename: *const i8, row_index: i32) -> i32 {
    // // Convert C string to Rust string
    // let filename_rs = unsafe {
        // std::ffi::CStr::from_ptr(filename)
            // .to_str()
            // .unwrap()
    // };
// 
    // let path = Path::new(filename_rs);
    // let file = File::open(path).unwrap();
    // let reader = SerializedFileReader::new(file).unwrap();
// 
    // // Assuming a single row group and a single int32 column
    // let mut iter = reader.get_row_iter(None).unwrap();
    // let row = iter.nth(row_index as usize).unwrap();
// 
    // // This should be a match statement of all possible schema types
    // if let parquet::record::Row::Single(parquet::record::Field::Int(Some(value))) = row {
        // return value;
    // }
    // return 0;
// }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_rows() {
        // Paths to the Parquet files
        let paths = vec![
            "../../data/mb.parquet",
        ];

        // Create a reader for each file and flat map rows
        let rows = paths.iter()
            .map(|p| SerializedFileReader::try_from(*p).unwrap())
            .flat_map(|r| r.into_iter());

        // for row in rows {
        for row in rows.take(10) {
            println!("{}", row.unwrap());
        }
    }

    #[test]
    fn test_read_column() {
        // Paths to the Parquet files
        let path = "../../data/mb.parquet";

        // Create a reader for each file and flat map rows
        let reader = SerializedFileReader::try_from(path).unwrap();

        println!("{:?} row groups", reader.num_row_groups());
        let metadata = reader.metadata();

        let schema = metadata.row_group();
        println!("{:?}", schema);
    }
}
