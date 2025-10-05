use databento::historical::timeseries::GetRangeParams;
use databento::{HistoricalClient, dbn};
use anyhow::Result;
use time::macros::datetime;

pub struct DatabentoConfig {
    pub api_key: String,
    pub dataset: String,
    pub symbol: String,
}

pub async fn fetch_ohlcv_data(config: DatabentoConfig) -> Result<Vec<dbn::OhlcvMsg>> {
    let mut client = HistoricalClient::builder()
        .key(&config.api_key)?
        .build()?;

    let params = GetRangeParams::builder()
        .dataset(config.dataset.as_str())
        .schema(dbn::Schema::Ohlcv1M)
        .symbols(config.symbol.as_str())
        .date_time_range((
            datetime!(2025-01-02 00:00 UTC),
            datetime!(2025-01-03 00:00 UTC),
        ))
        .build();

    println!("Fetching OHLCV-1m data from {}...", config.dataset);

    let mut decoder = client
        .timeseries()
        .get_range(&params)
        .await?;

    let mut records = Vec::new();
    while let Some(record) = decoder.decode_record::<dbn::OhlcvMsg>().await? {
        records.push(record.clone());
    }

    println!("Fetched {} records", records.len());
    Ok(records)
}
