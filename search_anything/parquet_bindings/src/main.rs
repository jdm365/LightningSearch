use parquet::file::reader::SerializedFileReader;
use std::convert::TryFrom;

let paths = vec![
    "/path/to/sample.parquet/part-1.snappy.parquet",
    "/path/to/sample.parquet/part-2.snappy.parquet"
];
// Create a reader for each file and flat map rows
let rows = paths.iter()
    .map(|p| SerializedFileReader::try_from(*p).unwrap())
    .flat_map(|r| r.into_iter());

for row in rows {
    println!("{}", row.unwrap());
}
