use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use anyhow::Result;
use arrow::array::{Int64Array, UInt64Array};
use std::fs::File;

pub async fn create_hypertable(database_url: &str) -> Result<PgPool> {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(database_url)
        .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS ohlcv_data (
            timestamp TIMESTAMPTZ NOT NULL,
            open BIGINT NOT NULL,
            high BIGINT NOT NULL,
            low BIGINT NOT NULL,
            close BIGINT NOT NULL,
            volume BIGINT NOT NULL
        );
        "#
    )
    .execute(&pool)
    .await?;

    sqlx::query(
        r#"
        SELECT create_hypertable('ohlcv_data', 'timestamp', if_not_exists => TRUE);
        "#
    )
    .execute(&pool)
    .await?;

    println!("Hypertable created successfully");
    Ok(pool)
}

pub async fn insert_parquet_data(pool: &PgPool, file_path: &str) -> Result<()> {
    println!("Loading data into PostgreSQL hypertable...");

    let parquet_file = File::open(file_path)?;
    let arrow_reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(parquet_file)?
        .build()?;

    let mut total_count = 0;

    for batch_result in arrow_reader {
        let batch = batch_result?;

        let timestamps = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let opens = batch.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        let highs = batch.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
        let lows = batch.column(3).as_any().downcast_ref::<Int64Array>().unwrap();
        let closes = batch.column(4).as_any().downcast_ref::<Int64Array>().unwrap();
        let volumes = batch.column(5).as_any().downcast_ref::<UInt64Array>().unwrap();

        let mut query = String::from("INSERT INTO ohlcv_data (timestamp, open, high, low, close, volume) VALUES ");

        for i in 0..batch.num_rows() {
            let timestamp_ns = timestamps.value(i);
            let timestamp_us = timestamp_ns / 1000;

            if i > 0 {
                query.push_str(", ");
            }
            query.push_str(&format!(
                "(to_timestamp({}::double precision / 1000000), {}, {}, {}, {}, {})",
                timestamp_us,
                opens.value(i),
                highs.value(i),
                lows.value(i),
                closes.value(i),
                volumes.value(i) as i64
            ));
        }

        query.push_str(" ON CONFLICT DO NOTHING");

        sqlx::query(&query).execute(pool).await?;
        total_count += batch.num_rows();
    }

    println!("Inserted {} records into hypertable", total_count);
    Ok(())
}
