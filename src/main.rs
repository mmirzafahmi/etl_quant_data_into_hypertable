mod databento_client;
mod parquet_writer;
mod s3_uploader;
mod database;

use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    // Load environment variables
    dotenvy::dotenv().ok();

    // Configuration
    let databento_config = databento_client::DatabentoConfig {
        api_key: std::env::var("DATABENTO_API_KEY")?,
        dataset: "GLBX.MDP3".to_string(),
        symbol: "ESH5".to_string(),
    };

    let s3_config = s3_uploader::S3Config {
        endpoint: std::env::var("AWS_URL")?,
        region: std::env::var("AWS_DEFAULT_REGION")?,
        bucket: std::env::var("S3_BUCKET")?,
    };

    let database_url = std::env::var("DATABASE_URL")?;
    let parquet_file = "ohlcv_data.parquet";
    let s3_key = "databento/ohlcv_data.parquet";

    // Step 1: Fetch data from Databento
    let records = databento_client::fetch_ohlcv_data(databento_config).await?;

    // Step 2: Write to Parquet
    parquet_writer::write_parquet(&records, parquet_file)?;

    // Step 3: Upload to S3
    s3_uploader::upload_to_s3(parquet_file, s3_key, s3_config).await?;

    // Step 4: Create hypertable and load data
    let pool = database::create_hypertable(&database_url).await?;
    database::insert_parquet_data(&pool, parquet_file).await?;

    println!("ETL pipeline completed successfully!");
    Ok(())
}
