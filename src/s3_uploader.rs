use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use aws_config::BehaviorVersion;
use anyhow::Result;
use std::path::Path;

pub struct S3Config {
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
}

pub async fn upload_to_s3(file_path: &str, s3_key: &str, config: S3Config) -> Result<()> {
    println!("Uploading to S3...");

    let aws_config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let s3_config = aws_sdk_s3::config::Builder::from(&aws_config)
        .endpoint_url(&config.endpoint)
        .region(aws_sdk_s3::config::Region::new(config.region))
        .force_path_style(true)
        .build();
    let s3_client = Client::from_conf(s3_config);

    let body = ByteStream::from_path(Path::new(file_path)).await?;

    s3_client
        .put_object()
        .bucket(&config.bucket)
        .key(s3_key)
        .body(body)
        .send()
        .await?;

    println!("Successfully uploaded to s3://{}/{}", config.bucket, s3_key);
    Ok(())
}
