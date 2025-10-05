use arrow::array::{ArrayRef, Int64Array, UInt64Array, RecordBatch};
use arrow::datatypes::{Schema, Field, DataType};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use parquet::basic::Compression;
use databento::dbn;
use anyhow::Result;
use std::sync::Arc;
use std::fs::File;

pub fn write_parquet(records: &[dbn::OhlcvMsg], file_path: &str) -> Result<()> {
    let mut timestamps = Vec::new();
    let mut opens = Vec::new();
    let mut highs = Vec::new();
    let mut lows = Vec::new();
    let mut closes = Vec::new();
    let mut volumes = Vec::new();

    for record in records {
        timestamps.push(record.hd.ts_event as i64);
        opens.push(record.open);
        highs.push(record.high);
        lows.push(record.low);
        closes.push(record.close);
        volumes.push(record.volume);
    }

    let schema = Schema::new(vec![
        Field::new("timestamp", DataType::Int64, false),
        Field::new("open", DataType::Int64, false),
        Field::new("high", DataType::Int64, false),
        Field::new("low", DataType::Int64, false),
        Field::new("close", DataType::Int64, false),
        Field::new("volume", DataType::UInt64, false),
    ]);

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(timestamps)),
        Arc::new(Int64Array::from(opens)),
        Arc::new(Int64Array::from(highs)),
        Arc::new(Int64Array::from(lows)),
        Arc::new(Int64Array::from(closes)),
        Arc::new(UInt64Array::from(volumes)),
    ];

    let batch = RecordBatch::try_new(Arc::new(schema), arrays)?;

    let file = File::create(file_path)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(parquet::basic::ZstdLevel::default()))
        .build();

    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    println!("Data saved to {} with Zstd compression!", file_path);
    Ok(())
}
