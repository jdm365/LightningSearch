use parquet::file::reader::{SerializedFileReader, FileReader};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use arrow_array::LargeStringArray;
use parquet::arrow::ProjectionMask; 

use std::fs::File;
use std::path::PathBuf;

use std::ffi::CStr;
use serde_json::Value;

fn stringify_json_value(value: &Value) -> Vec<u8> {
    match value {
        Value::String(s) => s.as_bytes().to_vec(),
        Value::Number(n) => n.to_string().as_bytes().to_vec(),
        Value::Bool(b) => b.to_string().as_bytes().to_vec(),
        Value::Null => "null".as_bytes().to_vec(),
        Value::Array(arr) => {
            serde_json::to_string(arr).unwrap().as_bytes().to_vec()
        }
        Value::Object(obj) => {
            serde_json::to_string(obj).unwrap().as_bytes().to_vec()
        }
    }
}

#[inline]
pub fn vbyte_encode(value: u64, buffer: &mut Vec<u8>) {
    let mut value = value;
    while value >= 0x80 {
        buffer.push((value as u8) | 0x80);
        value >>= 7;
    }
    buffer.push(value as u8);
}

pub fn read_parquet_row_group_column_utf8_null_terminated(
    filename: &str,
    row_group_index: usize,
    column_index: usize,
    values: &mut Vec<u8>,
) {
    // Paths to the Parquet files
    let path = PathBuf::from(filename);
    let file = match File::open(path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("Failed to open file {:?} {:?}", filename, e);
            return;
        }
    };
    let rdr = match SerializedFileReader::try_from(filename) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Failed to create reader for file {:?} {:?}", filename, e);
            return;
        }
    };
    let schema_desc = rdr.metadata().file_metadata().schema_descr();

    assert_eq!(schema_desc.column(column_index).logical_type(), Some(parquet::basic::LogicalType::String));

    let mask = ProjectionMask::leaves(schema_desc, vec![column_index]);

    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("Failed to build reader.")
        .with_batch_size(1 << 26)
        .with_row_groups(vec![row_group_index])
        .with_projection(mask);
    let reader = builder.build().expect("Failed to build reader");

    for batch in reader {
        let rb = batch.expect("Reading batch failed");
        let col = rb.column(0);
        let col_arr = match col.as_any().downcast_ref::<LargeStringArray>() {
            Some(arr) => arr,
            None => {
                eprintln!("Failed to downcast column to LargeStringArray");
                return;
            }
        };
        for val in col_arr.iter() {
            match val {
                Some(v) => {
                    values.extend_from_slice(v.as_bytes());
                    values.push(0);
                },
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
    values_len: *mut usize,
) -> *mut u8 {
    if filename.is_null() {
        eprintln!("Error: Filename pointer is null");
        return std::ptr::null_mut();
    }

    // Convert C string to Rust string safely
    let filename_rs = unsafe {
        CStr::from_ptr(filename as *const i8)
            .to_string_lossy()
            .into_owned()
    };

    let mut values_rs: Vec<u8> = Vec::new();
    read_parquet_row_group_column_utf8_null_terminated(
        &filename_rs, 
        row_group_index, 
        column_index, 
        &mut values_rs,
        );

    unsafe {
        *values_len = values_rs.len();
    }

    let values_ptr = values_rs.as_mut_ptr();
    std::mem::forget(values_rs);
    values_ptr
}

pub fn read_parquet_row_group_column_utf8_vbyte(
    filename: &str,
    row_group_index: usize,
    column_index: usize,
    values: &mut Vec<u8>,
) {
    // Paths to the Parquet files
    let path = PathBuf::from(filename);
    let file = match File::open(path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("Failed to open file {:?} {:?}", filename, e);
            return;
        }
    };
    let rdr = match SerializedFileReader::try_from(filename) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Failed to create reader for file {:?} {:?}", filename, e);
            return;
        }
    };
    let schema_desc = rdr.metadata().file_metadata().schema_descr();

    assert_eq!(schema_desc.column(column_index).logical_type(), Some(parquet::basic::LogicalType::String));

    let mask = ProjectionMask::leaves(schema_desc, vec![column_index]);

    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("Failed to build reader.")
        .with_batch_size(1 << 26)
        .with_row_groups(vec![row_group_index])
        .with_projection(mask);
    let reader = builder.build().expect("Failed to build reader");

    for batch in reader {
        let rb = batch.expect("Reading batch failed");
        let col = rb.column(0);
        let col_arr = match col.as_any().downcast_ref::<LargeStringArray>() {
            Some(arr) => arr,
            None => {
                eprintln!("Failed to downcast column to LargeStringArray");
                return;
            }
        };
        for val in col_arr.iter() {
            match val {
                Some(v) => {
                    let bytes: &[u8] = v.as_bytes();
                    vbyte_encode(bytes.len() as u64, values);
                    values.extend_from_slice(bytes);
                },
                None => values.push(0),
            }
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn read_parquet_row_group_column_utf8_vbyte_c(
    filename: *const u8,
    row_group_index: usize,
    column_index: usize,
    values_len: *mut usize,
) -> *mut u8 {
    if filename.is_null() {
        eprintln!("Error: Filename pointer is null");
        return std::ptr::null_mut();
    }

    // Convert C string to Rust string safely
    let filename_rs = unsafe {
        CStr::from_ptr(filename as *const i8)
            .to_string_lossy()
            .into_owned()
    };

    let mut values_rs: Vec<u8> = Vec::new();
    read_parquet_row_group_column_utf8_vbyte(
        &filename_rs, 
        row_group_index, 
        column_index, 
        &mut values_rs,
        );

    unsafe {
        *values_len = values_rs.len();
    }

    let values_ptr = values_rs.as_mut_ptr();
    std::mem::forget(values_rs);
    values_ptr
}

#[unsafe(no_mangle)]
pub extern "C" fn free_vec(ptr: *mut u8, size: usize) {
    if !ptr.is_null() {
        unsafe {
            let _ = Vec::from_raw_parts(ptr, size, size);
        }
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
    let filename_rs = unsafe {
        CStr::from_ptr(filename as *const i8)
            .to_string_lossy()
            .into_owned()
    };

    get_num_row_groups(&filename_rs)
}

pub fn get_num_rows(filename: &str) -> usize {
    let rdr = SerializedFileReader::try_from(filename).unwrap();
    let metadata = rdr.metadata();
    let mut num_rows = 0;
    for i in 0..rdr.num_row_groups() {
        num_rows += metadata.row_group(i).num_rows();
    }
    num_rows as usize
}

#[unsafe(no_mangle)]
pub extern "C" fn get_num_rows_c(filename: *const u8) -> usize {
    if filename.is_null() {
        eprintln!("Error: Filename pointer is null");
        return 0; // Or another appropriate error value
    }

    // Convert C string to Rust string safely
    let filename_rs = unsafe {
        CStr::from_ptr(filename as *const i8)
            .to_string_lossy()
            .into_owned()
    };

    get_num_rows(&filename_rs)
}

pub fn get_num_rows_in_row_group(filename: &str, row_group_index: usize) -> usize {
    let rdr = SerializedFileReader::try_from(filename).unwrap();
    let metadata = rdr.metadata();
    metadata.row_group(row_group_index).num_rows() as usize
}

#[unsafe(no_mangle)]
pub extern "C" fn get_num_rows_in_row_group_c(filename: *const u8, row_group_index: usize) -> usize {
    if filename.is_null() {
        eprintln!("Error: Filename pointer is null");
        return 0; // Or another appropriate error value
    }

    // Convert C string to Rust string safely
    let filename_rs = unsafe {
        CStr::from_ptr(filename as *const i8)
            .to_string_lossy()
            .into_owned()
    };

    get_num_rows_in_row_group(&filename_rs, row_group_index)
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
        CStr::from_ptr(filename as *const i8)
            .to_string_lossy()
            .into_owned()
    };

    let col_names_rs = get_col_names(&filename_rs);
    let mut col_names_ptrs = Vec::new();
    for s in col_names_rs {
        col_names_ptrs.extend_from_slice(s.as_bytes());
        col_names_ptrs.push(0);
    }
    col_names_ptrs.push(0);

    unsafe {
        std::ptr::copy_nonoverlapping(
            col_names_ptrs.as_ptr(), 
            col_names, 
            col_names_ptrs.len(),
            );
    }
}

#[allow(dead_code)]
#[repr(packed)]
#[derive(Clone)]
pub struct Field {
    start_position: u32,
    length: u32,
}

pub fn fetch_row_from_row_group(
    filename: &str,
    row_group_index: usize,
    row_index: usize,
    values: &mut Vec<u8>,
    // field_lengths: *mut usize,
    // field_start_positions: *mut usize,
    result_positions_ptr: *mut Field,
    ) {
    let rdr = match SerializedFileReader::try_from(filename) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Failed to create reader for file {:?} {:?}", filename, e);
            return;
        }
    };
    let rg = rdr.get_row_group(row_group_index).expect("Failed to get row group");

    let mut row_iter = rg.get_row_iter(None).expect("Failed to get row iterator");

    let _row = row_iter.nth(row_index).expect("Failed to get row").unwrap();

    let json_row = _row.to_json_value();

    let mut idx: usize = 0;
    for (_, value) in json_row.as_object().unwrap() {
        let value_bytes = stringify_json_value(value);

        let field = Field {
            start_position: values.len() as u32,
            length: value_bytes.len() as u32,
        };
        unsafe {
            result_positions_ptr.add(idx).write(field);
        }

        values.extend_from_slice(value_bytes.as_slice());

        idx += 1;
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn fetch_row_from_row_group_c(
    filename: *const u8,
    row_group_index: usize,
    row_index: usize,
    values: *mut u8,
    result_positions_ptr: *mut Field,
    ) {
    // Convert C string to Rust string
    let filename_rs = unsafe {
        CStr::from_ptr(filename as *const i8)
            .to_string_lossy()
            .into_owned()
    };

    let mut values_rs: Vec<u8> = Vec::new();

    fetch_row_from_row_group(
        &filename_rs,
        row_group_index,
        row_index,
        &mut values_rs,
        result_positions_ptr,
        );

    unsafe {
        std::ptr::copy_nonoverlapping(
            values_rs.as_ptr(), 
            values, 
            values_rs.len(),
            );
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

    #[test]
    fn test_read_string_col_c() {
        use std::ffi::CString;
        // Paths to the Parquet files
        let path = "../../data/mb.parquet";
        // let c_path = path.as_bytes();
        let c_path = CString::new(path).expect("Failed to create CString");

        let mut values_len: usize = 0;
        read_parquet_row_group_column_utf8_null_terminated_c(
            c_path.as_ptr() as *const u8, 
            0, 
            7, 
            &mut values_len,
            );


        read_parquet_row_group_column_utf8_vbyte_c(
            c_path.as_ptr() as *const u8, 
            0, 
            7, 
            &mut values_len,
            );
        free_vec(std::ptr::null_mut(), values_len);

        let num_rows = get_num_rows_c(c_path.as_ptr() as *const u8);
        println!("{:?}", num_rows);

        let num_rows_in_rg_0 = get_num_rows_in_row_group_c(c_path.as_ptr() as *const u8, 0);
        println!("{:?}", num_rows_in_rg_0);

        // Alloc with 100 elements
        let mut values: Vec<u8> = vec![0; 1 << 16];
        let mut result_positions: Vec<Field> = vec![Field { start_position: 0, length: 0 }; 100];

        fetch_row_from_row_group_c(
            c_path.as_ptr() as *const u8,
            0,
            0,
            values.as_mut_ptr(),
            result_positions.as_mut_ptr(),
            );
    }
}
