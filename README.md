# ETL Databento OHLCV-1m

A Rust ETL pipeline that fetches OHLCV (Open, High, Low, Close, Volume) 1-minute data from Databento API, stores it as Parquet files with Zstd compression, uploads to S3, and loads into a PostgreSQL TimescaleDB hypertable.

## Features

- 📊 Fetch historical OHLCV-1m data from Databento (GLBX.MDP3 dataset)
- 💾 Save data as Parquet files with Zstd compression
- ☁️ Upload to S3-compatible object storage
- 🗄️ Load data into PostgreSQL TimescaleDB hypertable
- ⚡ Batch inserts for optimal database performance

## Prerequisites

- Rust 1.70+
- PostgreSQL with TimescaleDB extension
- Databento API key
- S3-compatible storage credentials

## Setup

1. Copy the example environment file:
```bash
cp .env.example .env
```

2. Update `.env` with your credentials:
```env
DATABENTO_API_KEY=your_databento_api_key
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_DEFAULT_REGION=your_region
AWS_URL=https://your-s3-endpoint.com
S3_BUCKET=your-bucket-name
DATABASE_URL=postgresql://user:password@localhost:5432/dbname
```

3. Install dependencies:
```bash
cargo build
```

## Usage

Run the ETL pipeline:
```bash
cargo run
```

The pipeline will:
1. Fetch OHLCV-1m data from Databento (ESH5 futures, Jan 2-3, 2025)
2. Save to `ohlcv_data.parquet` locally
3. Upload to S3 at `databento/ohlcv_data.parquet`
4. Create TimescaleDB hypertable `ohlcv_data`
5. Insert all records into the database

## Project Structure

```
src/
├── main.rs              # Main ETL pipeline orchestration
├── databento_client.rs  # Databento API integration
├── parquet_writer.rs    # Parquet file writer with compression
├── s3_uploader.rs       # S3 upload functionality
└── database.rs          # PostgreSQL/TimescaleDB operations
```

## Configuration

Modify the configuration in `src/main.rs`:

- **Dataset**: `GLBX.MDP3` (CME Globex MDP3)
- **Symbol**: `ESH5` (ES March 2025 futures)
- **Date Range**: Adjust start/end dates as needed
- **S3 Key**: Change upload path if needed

## Database Schema

```sql
CREATE TABLE ohlcv_data (
    timestamp TIMESTAMPTZ NOT NULL,
    open BIGINT NOT NULL,
    high BIGINT NOT NULL,
    low BIGINT NOT NULL,
    close BIGINT NOT NULL,
    volume BIGINT NOT NULL
);

SELECT create_hypertable('ohlcv_data', 'timestamp');
```

## License

MIT
