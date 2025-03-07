use parquet::file::reader::{SerializedFileReader, FileReader};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use arrow_array::LargeStringArray;
use parquet::arrow::ProjectionMask; 

use std::fs::File;
use std::path::PathBuf;

use std::ffi::CStr;
use std::mem::transmute;
/*
pub fn read_parquet_row_group_column_utf8(
    filename: &str,
    row_group_index: usize,
    column_index: usize,
    values: &mut Vec<String>,
) {
    // Paths to the Parquet files
    let path = PathBuf::from(filename);
    let file = File::open(path).unwrap();
    let rdr = SerializedFileReader::try_from(filename).unwrap();
    let schema_desc = rdr.metadata().file_metadata().schema_descr();

    let mask = ProjectionMask::leaves(schema_desc, vec![column_index]);

    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("Failed to build reader.")
        .with_batch_size(1 << 26)
        .with_row_groups(vec![row_group_index])
        .with_projection(mask);
    let reader = builder.build().unwrap();

    for batch in reader {
        let rb = batch.unwrap();
        let col = rb.column(0);
        let col_arr = col.as_any().downcast_ref::<LargeStringArray>().unwrap();
        println!("{:?}", col.len());
        for val in col_arr.iter() {
            match val {
                Some(v) => values.push(v.to_string()),
                None => values.push("".to_string()),
            }
        }
    }
}
*/

pub fn read_parquet_row_group_column_utf8_null_terminated(
    filename: &str,
    row_group_index: usize,
    column_index: usize,
    values: &mut Vec<u8>,
) {
    // Paths to the Parquet files
    let path = PathBuf::from(filename);
    let file = File::open(path).unwrap();
    let rdr = SerializedFileReader::try_from(filename).unwrap();
    let schema_desc = rdr.metadata().file_metadata().schema_descr();

    println!("{:?}", schema_desc);

    let mask = ProjectionMask::leaves(schema_desc, vec![column_index]);

    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("Failed to build reader.")
        .with_batch_size(1 << 26)
        .with_row_groups(vec![row_group_index])
        .with_projection(mask);
    let reader = builder.build().unwrap();

    for batch in reader {
        let rb = batch.unwrap();
        let col = rb.column(0);
        let col_arr = col.as_any().downcast_ref::<LargeStringArray>().unwrap();
        println!("{:?}", col.len());
        for val in col_arr.iter() {
            match val {
                Some(v) => values.extend_from_slice(v.as_bytes()),
                None => values.push(0),
            }
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn read_parquet_row_group_column_utf8_null_terminated_c(
    filename: *const u8,
    row_group_index: usize,
    column_index: usize,
    values: *mut u8,
    values_len: *mut usize,
) {
    if filename.is_null() {
        eprintln!("Error: Filename pointer is null");
        // return 0; // Or another appropriate error value
    }

    // Convert C string to Rust string safely
    let filename_rs_result = unsafe {
        let fname: *const i8 = transmute(filename);
        CStr::from_ptr(fname).to_str()
    };

    let mut values_rs: Vec<u8> = Vec::new();
    read_parquet_row_group_column_utf8_null_terminated(
        &filename_rs_result.unwrap(), 
        row_group_index, 
        column_index, 
        &mut values_rs,
        );

    unsafe {
        *values_len = values_rs.len();
        std::ptr::copy_nonoverlapping(values_rs.as_ptr(), values, values_rs.len());
    }
}

pub fn get_num_row_groups(filename: &str) -> usize {
    let rdr = SerializedFileReader::try_from(filename).unwrap();
    rdr.num_row_groups()
}

#[unsafe(no_mangle)]
pub extern "C" fn get_num_row_groups_c(filename: *const u8) -> usize {
    if filename.is_null() {
        eprintln!("Error: Filename pointer is null");
        return 0; // Or another appropriate error value
    }

    // Convert C string to Rust string safely
    let filename_rs_result = unsafe {
        let fname: *const i8 = transmute(filename);
        CStr::from_ptr(fname).to_str()
    };

    get_num_row_groups(&filename_rs_result.unwrap())
}

pub fn get_col_names(filename: &str) -> Vec<String> {
    let rdr = SerializedFileReader::try_from(filename).unwrap();
    let schema = rdr.metadata().file_metadata().schema_descr();
    schema.columns().iter().map(|c| c.name().to_string()).collect()
}

#[unsafe(no_mangle)]
pub extern "C" fn get_col_names_c(
    filename: *const u8,
    col_names: *mut u8,
    ) {
    // Convert C string to Rust string
    let filename_rs = unsafe {
        let fname: *const i8 = transmute(filename);
        CStr::from_ptr(fname).to_str()
    };

    let col_names_rs = get_col_names(&filename_rs.unwrap());
    let mut col_names_ptrs = Vec::new();
    for s in col_names_rs {
        col_names_ptrs.extend_from_slice(s.as_bytes());
        col_names_ptrs.push(0);
    }
    col_names_ptrs.push(0);

    unsafe {
        std::ptr::copy_nonoverlapping(col_names_ptrs.as_ptr(), col_names, col_names_ptrs.len());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /*
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
    */

    /*
    #[test]
    fn test_read_column() {
        // Paths to the Parquet files
        let path = "../../data/mb.parquet";

        // Create a reader for each file and flat map rows
        let reader = SerializedFileReader::try_from(path).unwrap();

        println!("{:?} row groups", reader.num_row_groups());
        let metadata = reader.metadata();

        let rg = reader.get_row_group(0).unwrap();

        let schema = metadata.file_metadata().schema();
        println!("{:?}", schema);

        // Get list of columns
        let col_reader = rg.get_column_reader(0).unwrap();
        const N: usize = 10;

        // Determine the type of column reader and downcast
        match col_reader {
            ColumnReader::Int64ColumnReader(mut _reader) => {
                let mut def_levels_buffer: Vec<i16> = vec![0; N];
                let mut rep_levels_buffer: Vec<i16> = vec![0; N];
                let mut values_buffer: Vec<i64> = vec![0; N];

                // Read the records
                let (records_read, values_read, levels_read) = _reader.read_records(
                    N,
                    Some(&mut def_levels_buffer),
                    Some(&mut rep_levels_buffer),
                    // None, None,
                    &mut values_buffer,
                ).unwrap();

                println!("Records read: {}", records_read);
                println!("Values read: {}", values_read);
                println!("Levels read: {}", levels_read);

                println!("Def levels: {:?}", &def_levels_buffer[..levels_read]);
                println!("Rep levels: {:?}", &rep_levels_buffer[..levels_read]);
                println!("Values: {:?}", &values_buffer[..values_read]);
            }
            _ => println!("Column is not an Int64ColumnReader"),
        }
    }
    */

    /*
    #[test]
    fn test_read_column() {
        // Paths to the Parquet files
        let _file = "../../data/mb.parquet";
        let path = PathBuf::from(_file);
        let file = File::open(path).unwrap();
        let rdr = SerializedFileReader::try_from(_file).unwrap();
        let schema_desc = rdr.metadata().file_metadata().schema_descr();

        let mask = ProjectionMask::leaves(schema_desc, vec![0, 7]);

        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .expect("Failed to build reader.")
            .with_row_groups(vec![0])
            // .with_row_selection(Some(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]))
            .with_projection(mask);
        // println!("Converted arrow schema is: {}", builder.schema());
        let reader = builder.build().unwrap();

        for batch in reader {
            let batch = batch.unwrap();
            // println!("{:?}", batch);
            break;
        }
    }
    */

    #[test]
    fn test_read_string_col() {
        // Paths to the Parquet files
        let path = "../../data/mb.parquet";
        // let mut values: Vec<String> = Vec::new();
        let mut values: Vec<u8> = Vec::new();
        // read_parquet_row_group_column_utf8(path, 0, 7, &mut values);
        read_parquet_row_group_column_utf8_null_terminated(path, 0, 7, &mut values);
        println!("{:?}", values.len());

        let num_row_groups = get_num_row_groups(path);
        println!("{:?}", num_row_groups);
    }
}
