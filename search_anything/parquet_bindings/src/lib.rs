use parquet::file::reader::{SerializedFileReader, FileReader};
use parquet::column::reader::ColumnReader;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::data_type::{ByteArray, FixedLenByteArray};
use arrow_array::LargeStringArray;
use parquet::arrow::ProjectionMask; 

use std::fs::{File, OpenOptions};
use std::path::PathBuf;
use std::io::{Write, Read, Seek};

use std::ffi::CStr;
use serde_json::Value;

use std::ptr;
use std::sync::Arc;
use std::slice;

use rayon::prelude::*;
use zstd::bulk::{Compressor, Decompressor};
// 
// use huffman::Tree;
// use huffman::decode;

use std::path::Path;
use polars::prelude::*;

#[inline]
pub fn compress_zstd(
    compressor: &mut Compressor,
    input: &[u8],
    output: &mut [u8],
    ) -> usize {
    match compressor.compress_to_buffer(input, output) {
        Ok(bytes_written) => bytes_written,
        Err(e) => {
            eprintln!("Failed to compress: {:?}", e);
            0
        }
    }
}

#[inline]
pub fn decompress_zstd(
    decompressor: &mut Decompressor,
    input: &[u8],
    output: &mut [u8],
    ) -> usize {
    match decompressor.decompress_to_buffer(input, output) {
        Ok(bytes_read) => bytes_read,
        Err(e) => {
            eprintln!("Failed to decompress: {:?}", e);
            0
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn convert_parquet_to_csv(_parquet_path: *const u8) {
    let parquet_path = unsafe {
        CStr::from_ptr(_parquet_path as *const i8)
            .to_string_lossy()
            .into_owned()
    };

    let input_path = Path::new(&parquet_path);
    let output_path = {
        let mut path = PathBuf::from(&parquet_path);
        path.set_extension("csv");
        path
    };
 

    let mut df = ParquetReader::new(File::open(input_path).unwrap()).finish().unwrap();
    let mut file = File::create(output_path).unwrap();
    CsvWriter::new(&mut file).finish(&mut df).unwrap();
}



pub struct RowGroupHandler {
    pub column_readers: Vec<ColumnReader>,
}

// TODO: impl read_row
impl RowGroupHandler {
    pub fn read_row(
        &mut self, 
        row_index: usize,
        col_index: usize,
        ) -> Result<Vec<u8>, parquet::errors::ParquetError> {
        let mut values: Vec<u8> = Vec::new();
        let col_reader = &mut self.column_readers[col_index];

        match col_reader {
            ColumnReader::Int32ColumnReader(rdr) => {
                let _rows_skipped = rdr.skip_records(row_index).expect("Failed to skip records");

                let mut def_levels = Vec::with_capacity(1);
                let mut rep_levels = Vec::with_capacity(1);

                let mut value_buffer: Vec<i32> = Vec::with_capacity(1);
                let (_records_read, _non_null_count, _levels_read) = rdr.read_records(
                    1, 
                    Some(&mut def_levels),
                    Some(&mut rep_levels),
                    &mut value_buffer,
                )?;

                values.extend_from_slice(&value_buffer[0].to_le_bytes());

                Ok(values)
            },
            ColumnReader::Int64ColumnReader(rdr) => {
                let _rows_skipped = rdr.skip_records(row_index).expect("Failed to skip records");

                let mut def_levels = Vec::with_capacity(1);
                let mut rep_levels = Vec::with_capacity(1);

                let mut value_buffer: Vec<i64> = Vec::with_capacity(1);
                let (_records_read, _non_null_count, _levels_read) = rdr.read_records(
                    1, 
                    Some(&mut def_levels),
                    Some(&mut rep_levels),
                    &mut value_buffer,
                )?;

                values.extend_from_slice(&value_buffer[0].to_le_bytes());

                Ok(values)
            },
            ColumnReader::Int96ColumnReader(_) => {
                return Err(parquet::errors::ParquetError::General("Int96 not supported".to_string()));
            },
            ColumnReader::FloatColumnReader(rdr) => {
                let _rows_skipped = rdr.skip_records(row_index).expect("Failed to skip records");

                let mut def_levels = Vec::with_capacity(1);
                let mut rep_levels = Vec::with_capacity(1);

                let mut value_buffer: Vec<f32> = Vec::with_capacity(1);
                let (_records_read, _non_null_count, _levels_read) = rdr.read_records(
                    1, 
                    Some(&mut def_levels),
                    Some(&mut rep_levels),
                    &mut value_buffer,
                )?;

                values.extend_from_slice(&value_buffer[0].to_le_bytes());

                Ok(values)
            },
            ColumnReader::DoubleColumnReader(rdr) => {
                let _rows_skipped = rdr.skip_records(row_index).expect("Failed to skip records");

                let mut def_levels = Vec::with_capacity(1);
                let mut rep_levels = Vec::with_capacity(1);

                let mut value_buffer: Vec<f64> = Vec::with_capacity(1);
                let (_records_read, _non_null_count, _levels_read) = rdr.read_records(
                    1, 
                    Some(&mut def_levels),
                    Some(&mut rep_levels),
                    &mut value_buffer,
                )?;

                values.extend_from_slice(&value_buffer[0].to_le_bytes());

                Ok(values)
            },
            ColumnReader::BoolColumnReader(rdr) => {
                let _rows_skipped = rdr.skip_records(row_index).expect("Failed to skip records");

                let mut def_levels = Vec::with_capacity(1);
                let mut rep_levels = Vec::with_capacity(1);

                let mut value_buffer: Vec<bool> = Vec::with_capacity(1);
                let (_records_read, _non_null_count, _levels_read) = rdr.read_records(
                    1, 
                    Some(&mut def_levels),
                    Some(&mut rep_levels),
                    &mut value_buffer,
                )?;

                values.push(value_buffer[0] as u8);

                Ok(values)
            },
            ColumnReader::ByteArrayColumnReader(rdr) => {
                let start = std::time::Instant::now();
                let _rows_skipped = rdr.skip_records(row_index).expect("Failed to skip records");
                let duration = start.elapsed();
                println!("Time elapsed in skip_records() is: {:?}", duration);

                let mut def_levels = Vec::with_capacity(1);
                let mut rep_levels = Vec::with_capacity(1);

                let mut value_buffer: Vec<ByteArray> = Vec::with_capacity(1);
                let start = std::time::Instant::now();
                let (_records_read, _non_null_count, _levels_read) = rdr.read_records(
                    1, 
                    Some(&mut def_levels),
                    Some(&mut rep_levels),
                    &mut value_buffer,
                )?;
                let duration = start.elapsed();
                println!("Time elapsed in read_records() is: {:?}", duration);

                if value_buffer.len() == 0 {
                    // TODO: Revisit this logic.
                    return Ok(values);
                }

                for byte in value_buffer[0].data() {
                    values.push(*byte);
                }
                Ok(values)
            },
            ColumnReader::FixedLenByteArrayColumnReader(rdr) => {
                let _rows_skipped = rdr.skip_records(row_index).expect("Failed to skip records");

                let mut def_levels = Vec::with_capacity(1);
                let mut rep_levels = Vec::with_capacity(1);

                let mut value_buffer: Vec<FixedLenByteArray> = Vec::with_capacity(1);
                let (_records_read, _non_null_count, _levels_read) = rdr.read_records(
                    1, 
                    Some(&mut def_levels),
                    Some(&mut rep_levels),
                    &mut value_buffer,
                )?;

                for byte in value_buffer[0].data() {
                    values.push(*byte);
                }
                Ok(values)
            },
        }

    }
}

pub struct FileColumnHandler {
    pub row_groups: Vec<RowGroupHandler>,
}

impl FileColumnHandler {
    pub fn new(
        filename: &str,
        ) -> Result<Self, parquet::errors::ParquetError> {
        let rdr = SerializedFileReader::try_from(filename)?;
        let num_row_groups = rdr.num_row_groups();
        let mut row_groups = Vec::with_capacity(num_row_groups);

        for rg_idx in 0..num_row_groups {
            let rg = rdr.get_row_group(rg_idx)?;
            let num_columns = rg.num_columns();
            let mut column_readers = Vec::with_capacity(num_columns);

            for col_idx in 0..num_columns {
                let col_reader = rg.get_column_reader(col_idx)?;
                column_readers.push(col_reader);
            }

            row_groups.push(RowGroupHandler {
                column_readers,
            });
        }


        Ok(Self {
            row_groups,
        })
    }

    pub fn read_row(
        &mut self, 
        row_group_index: usize, 
        row_index: usize,
        col_index: usize,
        ) -> Result<Vec<u8>, parquet::errors::ParquetError> {
        self.row_groups[row_group_index].read_row(row_index, col_index)
    }
}

/*
pub fn fetch_row_parallel(
    handle: *mut ParquetReaderHandle,
    row_group_index: usize,
    row_index: usize,
    col_indices: &[usize],
    data: &mut Vec<u8>,
    result_positions_ptr: *mut Field,
) -> Result<(), parquet::errors::ParquetError> {

    let results: Result<Vec<(Vec<u8>, Field)>, parquet::errors::ParquetError> = col_indices
        .par_iter()
        .map(|&col_index| {
            // Call the already implemented per‑column row fetcher.
            fetch_row_from_column_result(handle, row_group_index, row_index, col_index)
        })
        .collect();
    let mut results = results?;

    for (i, (mut col_bytes, mut field)) in results.into_iter().enumerate() {
        field.start_position = data.len() as u32;
        data.append(&mut col_bytes);

        // Instead of always writing to add(0), write to the i-th Field.
        unsafe {
            result_positions_ptr.add(i).write(field);
        }
    }

    Ok(())
}

pub fn fetch_row_from_column_result(
    handle: *mut ParquetReaderHandle,
    row_group_index: usize,
    row_index: usize,
    col_index: usize,
) -> Result<(Vec<u8>, Field), parquet::errors::ParquetError> {
    let local_values = fetch_row_from_column(
        handle,
        row_group_index,
        row_index,
        col_index,
    )?;
    let field = Field {
        start_position: 0,
        length: local_values.len() as u32,
    };

    Ok((local_values, field))
}
*/


// Define an opaque structure to wrap the SerializedFileReader.
// By using #[repr(C)] we guarantee that it can be passed as an opaque pointer.
// #[repr(C)]
pub struct ParquetReaderHandle {
    // Using Arc to allow shared ownership if needed. If not, you can simply
    // store a Box<SerializedFileReader<File>>.
    reader: Arc<SerializedFileReader<File>>,
}

/// Create a new Parquet reader and return a handle to it.
///
/// # Safety
/// The `filename` must be a valid, null-terminated C string.
#[unsafe(no_mangle)]
pub extern "C" fn create_parquet_reader(
    filename: *const u8,
) -> *mut ParquetReaderHandle {
    if filename.is_null() {
        eprintln!("create_parquet_reader: filename pointer is null.");
        return ptr::null_mut();
    }

    // Convert C string to Rust string.
    let filename_rs = unsafe {
        CStr::from_ptr(filename as *const i8)
            .to_string_lossy()
            .into_owned()
    };

    // Open the file.
    let file = match File::open(filename_rs) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("create_parquet_reader: failed to open file: {:?}", e);
            return ptr::null_mut();
        }
    };

    // Create the Parquet reader.
    let reader = match SerializedFileReader::new(file) {
        Ok(r) => Arc::new(r),
        Err(e) => {
            eprintln!("create_parquet_reader: error initializing reader: {:?}", e);
            return ptr::null_mut();
        }
    };

    // Box the handle and return a raw pointer.
    let handle = Box::new(ParquetReaderHandle { reader });
    Box::into_raw(handle)
}

fn get_reader_from_handle(handle: *mut ParquetReaderHandle) -> Option<Arc<SerializedFileReader<File>>> {
    if handle.is_null() {
        None
    } else {
        // Safety: we assume the pointer is valid.
        let h = unsafe { &*handle };
        Some(Arc::clone(&h.reader))
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn free_parquet_reader(handle: *mut ParquetReaderHandle) {
    if !handle.is_null() {
        // Convert the raw pointer back into a Box and drop it.
        unsafe {
            _ = Box::from_raw(handle);
        }
    }
}



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
    // filename: &str,
    handle: *mut ParquetReaderHandle,
    row_group_index: usize,
    row_index: usize,
    values: &mut Vec<u8>,
    result_positions_ptr: *mut Field,
    ) {
    let rdr = get_reader_from_handle(handle).expect("Failed to get reader");
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
    // filename: *const u8,
    pr: *mut ParquetReaderHandle,
    row_group_index: usize,
    row_index: usize,
    values: *mut u8,
    result_positions_ptr: *mut Field,
    ) {
    let mut values_rs: Vec<u8> = Vec::new();

    fetch_row_from_row_group(
        // &filename_rs,
        pr,
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


pub unsafe fn write_row_group_get_line_offsets(
    input_filename: &str,
    row_group_index: usize,
    line_offsets: *mut u32,
    file: &mut File,
    ) -> std::io::Result<()> {
    println!("Writing row group {} to file", row_group_index);

    let rdr = SerializedFileReader::try_from(input_filename).unwrap();
    let rg = rdr.get_row_group(row_group_index).unwrap();
    let row_iter = rg.get_row_iter(None).unwrap().enumerate();

    let mut buffer_idx: u32 = 0;
    let mut offset: usize = 0;
    unsafe {
        line_offsets.add(0).write(buffer_idx);
    }

    let mut compressor = Compressor::new(3).unwrap();

    let mut scratch_buffer: Vec<u8> = Vec::with_capacity(65536);

    const FLUSH_SIZE: usize = 1 << 24;
    let mut buffer: Vec<u8> = vec![0; 2 * FLUSH_SIZE];

    let num_rows = rg.metadata().num_rows() as usize;

    for (idx, row) in row_iter {
        scratch_buffer.clear();

        for (_, field) in row.unwrap().get_column_iter() {
            let str_val = field.to_string();
            let num_bytes = str_val.as_bytes().len();
            vbyte_encode(num_bytes as u64, &mut scratch_buffer);
            scratch_buffer.extend_from_slice(str_val.as_bytes());
        }
        let compressed_size = compress_zstd(
            &mut compressor,
            scratch_buffer.as_slice(),
            &mut buffer[buffer_idx as usize..],
            );

        offset     += compressed_size;
        buffer_idx += compressed_size as u32;

        unsafe {
            line_offsets.add(idx).write(offset as u32);
        }

        if buffer_idx as usize >= FLUSH_SIZE {
            file.write_all(&buffer[..buffer_idx as usize])?;
            buffer_idx = 0;
        }
    }

    if buffer_idx > 0 {
        file.write_all(&buffer[..buffer_idx as usize])?;
    }
    unsafe {
        line_offsets.add(num_rows).write(offset as u32);
    }

    Ok(())
}

pub struct DocStore<'a> {
    dir: String,
    file_handles: Vec<File>,
    decompressor: Decompressor<'a>,
    scratch_buffer: [u8; 1 << 16],
}

impl<'a> DocStore<'a> {
    pub fn create(dir: &str) -> Self {
        Self {
            dir: dir.to_string(),
            file_handles: Vec::new(),
            decompressor: Decompressor::new().unwrap(),
            scratch_buffer: [0; 1 << 16],
        }
    }

    pub unsafe fn write_all_row_groups(
        &mut self,
        input_filename: &str,
    ) -> Result<*mut *mut u32, std::io::Error> {
        let rdr = SerializedFileReader::try_from(input_filename)?;
        let num_row_groups = rdr.num_row_groups();

        if std::fs::metadata(&self.dir).is_err() {
            std::fs::create_dir(&self.dir)?;
        }

        for i in 0..num_row_groups {
            self.file_handles.push(
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(format!("{}/rg{}", self.dir, i))
                    .expect("Failed to create file"),
            );
        }

        // Create a vector of line_offsets (one Vec<u32> per row group)
        let mut line_offsets: Vec<Vec<u32>> =
            (0..num_row_groups).map(|_| Vec::new()).collect();

        for idx in 0..num_row_groups {
            let num_rows = rdr.metadata().row_group(idx).num_rows();
            line_offsets[idx].resize(num_rows as usize + 1, 0);
        }


        let input_filename_arc = Arc::new(input_filename.to_string());
        self.file_handles
            .par_iter_mut()
            .zip(line_offsets.par_iter_mut())
            .enumerate()
            .try_for_each(|(i, (file, _line_offsets))| -> Result<(), std::io::Error> {
                let local_filename = Arc::clone(&input_filename_arc);

                unsafe {
                    write_row_group_get_line_offsets(
                        &local_filename,
                        i,
                        _line_offsets.as_mut_ptr(),
                        file,
                    )?;
                }
                Ok(())
            })?;

        let mut line_offsets_ptrs: Vec<*mut u32> =
            line_offsets.iter_mut().map(|v| v.as_mut_ptr()).collect();

        std::mem::forget(line_offsets);

        Ok(line_offsets_ptrs.as_mut_ptr())
    }


    #[unsafe(no_mangle)]
    pub extern "C" fn write_row_group_get_line_offsets_c(
        &mut self,
        input_filename: *const u8,
        row_group_index: usize,
        line_offsets: *mut u32,
        ) {
        unsafe {
            let _ = write_row_group_get_line_offsets(
                &CStr::from_ptr(input_filename as *const i8).to_string_lossy(),
                row_group_index, 
                line_offsets,
                self.file_handles.get_mut(row_group_index).unwrap(),
                );
        }
    }


    pub unsafe fn fetch_row(
        &mut self,
        row_group_index: usize,
        offset: u32,
        bytes_to_read: u32,
        str_bytes: *mut u8,
        str_bytes_capacity: usize,
        ) -> std::io::Result<()> {
        self.file_handles[row_group_index].seek(std::io::SeekFrom::Start(offset as u64)).expect("Failed to seek");

        let bytes_read = self.file_handles[row_group_index].read(&mut self.scratch_buffer[..bytes_to_read as usize])?;

        if str_bytes.is_null() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Null pointer",
                ));
        }

        let out_buffer: &mut [u8] = unsafe {
            slice::from_raw_parts_mut(str_bytes, str_bytes_capacity)
        };

        _ = decompress_zstd(
            &mut self.decompressor,
            &self.scratch_buffer[..bytes_read],
            out_buffer,
            );

        Ok(())
    }

    #[unsafe(no_mangle)]
    pub extern "C" fn fetch_row_c(
        &mut self,
        row_group_index: usize,
        offset: u32,
        bytes_to_read: u32,
        str_bytes: *mut u8,
        str_bytes_capacity: usize,
        ) {
        unsafe {
            let _ = self.fetch_row(row_group_index, offset, bytes_to_read, str_bytes, str_bytes_capacity);
        }
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

    /*
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
        let path = "../../data/mb_smallrg.parquet";
        let c_path = CString::new(path).expect("Failed to create CString");

        let mut values_len: usize = 0;
        // read_parquet_row_group_column_utf8_null_terminated_c(
            // c_path.as_ptr() as *const u8, 
            // 0, 
            // 7, 
            // &mut values_len,
            // );
// 

        read_parquet_row_group_column_utf8_vbyte_c(
            c_path.as_ptr() as *const u8, 
            0, 
            7, 
            &mut values_len,
            );
        free_vec(std::ptr::null_mut(), values_len);

        let num_rows = get_num_rows_c(c_path.as_ptr() as *const u8);
        println!("Num rows in file: {:?}", num_rows);

        let num_rows_in_rg_0 = get_num_rows_in_row_group_c(c_path.as_ptr() as *const u8, 0);
        println!("Num rows in row group 0: {:?}", num_rows_in_rg_0);

        // Alloc with 100 elements
        let mut values: Vec<u8> = vec![0; 1 << 16];
        let mut result_positions: Vec<Field> = vec![Field { start_position: 0, length: 0 }; 100];

        let pr = create_parquet_reader(c_path.as_ptr() as *const u8);
        println!("{:?}", pr);

        let mut file_handler = FileColumnHandler::new(path).unwrap();

        let start = std::time::Instant::now();
        file_handler.read_row(400, 100000, 7).expect("Failed to read row");
        /*
        let _ = fetch_row_parallel(
            pr,
            0,
            0,
            vec![0, 7].as_slice(),
            &mut values,
            result_positions.as_mut_ptr(),
            );
        */
        let duration = start.elapsed();
        println!("Time elapsed in fetch_row_from_column() is: {:?}", duration);

        let min_rg = 0;
        let max_rg = file_handler.row_groups.len();

        let min_row_selection = 0;
        let max_row_selection = 4096;

        let n = 50;

        let start = std::time::Instant::now();
        // fetch_row_from_row_group_c(
            // // c_path.as_ptr() as *const u8,
            // pr,
            // 0,
            // 0,
            // values.as_mut_ptr(),
            // result_positions.as_mut_ptr(),
            // );
        for sample_idx in 0..n {
            let rg_idx = min_rg + (sample_idx % (max_rg - min_rg));
            let row_idx = min_row_selection + (sample_idx % (max_row_selection - min_row_selection));
            let _ = fetch_row_from_row_group_c(
                pr,
                rg_idx,
                row_idx,
                values.as_mut_ptr(),
                result_positions.as_mut_ptr(),
                );
        }
        let duration = start.elapsed();
        println!("Time elapsed in fetch_row_from_row_group_c() is: {:?}", duration);

        println!("{:?}", values[0]);
        println!("{:?}", values.len());
    }
    */


        /*
    #[test]
    fn zstd_compress() {
        let mut test_bytes = vec![0; 125];
        test_bytes.copy_from_slice(b"2,1733714,2,3,36995712MB-01,16,Lookin' in the Eyes of My Melanie - De allerbeste van The Classics,2.867,Classics,,'04,English");

        let mut compressed = Vec::with_capacity(test_bytes.len());
        compressed.resize(test_bytes.len() * 2, 0);

        let level = 3;
        let mut compressor = Compressor::new(level).unwrap();

        let bytes_written = compress_zstd(&mut compressor, test_bytes.as_mut_slice(), &mut compressed);

        println!("Uncompressed size: {:?}", test_bytes.len());
        println!("Compressed size:   {:?}", bytes_written);
        println!("Unompressed: {:?}", test_bytes);
        println!("Compressed:  {:?}", compressed);

        let mut decompressor = Decompressor::new().unwrap();
        decompress_zstd(&mut decompressor, &compressed[0..bytes_written], &mut test_bytes[..]);

        println!("Decompressed: {:?}", test_bytes);
    }

    #[test]
    fn doc_store() {
        let mut doc_store = DocStore::create("test");
        // let path = "../../data/mb_smallrg.parquet";
        let path = "../../data/mb.parquet";
        // let path = "../../data/mb_duckdb.parquet";
        let num_rows = 309380;

        unsafe {
            let start = std::time::Instant::now();
            let _line_offsets = doc_store.write_all_row_groups(path).unwrap();
            let duration = start.elapsed();

            println!("Time elapsed in write_row_group_get_line_offsets() is: {:?}", duration);
            println!("Rows per second: {:?}", num_rows as f64 / duration.as_secs_f64());

            let mut str_bytes: Vec<u8> = vec![0; 1 << 16];

            let start = std::time::Instant::now();
            doc_store.fetch_row(0, 0, 100, str_bytes.as_mut_ptr(), str_bytes.len()).unwrap();
            let duration = start.elapsed();
            println!("Fetch time: {:?}", duration);

            println!("{:?}", str_bytes[..100].iter().map(|&b| b as char).collect::<String>());
        }
    }
    */

    #[test]
    fn parquet_to_csv() {
        let path = "../../data/mb.parquet\0";

        let start = std::time::Instant::now();
        convert_parquet_to_csv(
            path.as_ptr() as *const u8,
        );
        let duration = start.elapsed();
        println!("Time elapsed in convert_parquet_to_csv() is: {:?}", duration);
    }
}
