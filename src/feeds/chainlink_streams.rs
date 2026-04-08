use anyhow::{Context, Result};
use chainlink_data_streams_report::feed_id::ID;
use chainlink_data_streams_report::report::{decode_full_report, v3::ReportDataV3};
use chainlink_data_streams_sdk::config::Config;
use chainlink_data_streams_sdk::stream::{Stream, WebSocketReport};
use num_bigint::BigInt;
use num_traits::ToPrimitive;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::{error, info, warn};

use super::PriceFeeds;

static DS_MSG_COUNT: AtomicU64 = AtomicU64::new(0);

pub fn messages_received() -> u64 {
    DS_MSG_COUNT.load(Ordering::Relaxed)
}

/// Map from feed_id bytes → our Chainlink price key (e.g. "btc/usd").
type FeedMap = HashMap<[u8; 32], String>;

/// Asset → known feed ID hex suffix (last 4 hex chars from Chainlink registry).
const KNOWN_FEEDS: &[(&str, &str)] = &[
    ("BTC", "75b8"),
    ("ETH", "3ae9"),
    ("SOL", "c24f"),
    ("XRP", "fc45"),
    ("DOGE", "8fdc"),
    ("BNB", "21fe"),
];

fn asset_to_chainlink_key(asset: &str) -> String {
    format!("{}/usd", asset.to_lowercase())
}

/// Discover feed IDs at startup via the REST API, matching by asset suffix.
async fn discover_feeds(
    config: &Config,
    assets: &[String],
) -> Result<(Vec<ID>, FeedMap)> {
    use chainlink_data_streams_sdk::client::Client;

    let client = Client::new(config.clone())
        .map_err(|e| anyhow::anyhow!("Client creation failed: {e}"))?;
    let feeds_response = client.get_feeds().await
        .map_err(|e| anyhow::anyhow!("get_feeds failed: {e}"))?;

    let mut feed_ids = Vec::new();
    let mut feed_map = FeedMap::new();

    for asset in assets {
        let asset_upper = asset.to_uppercase();
        let suffix = KNOWN_FEEDS.iter()
            .find(|(a, _)| *a == asset_upper)
            .map(|(_, s)| *s);

        if let Some(suffix) = suffix {
            for feed in &feeds_response {
                let hex_id = feed.feed_id.to_hex_string();
                if hex_id.ends_with(suffix) {
                    let key = asset_to_chainlink_key(&asset_upper);
                    info!(
                        asset = %asset_upper,
                        feed_id = %hex_id,
                        chainlink_key = %key,
                        "Discovered Data Streams feed"
                    );
                    feed_map.insert(feed.feed_id.0, key);
                    feed_ids.push(feed.feed_id);
                    break;
                }
            }
        }

        if !feed_map.values().any(|k| k == &asset_to_chainlink_key(&asset_upper)) {
            warn!(asset = %asset_upper, "No Data Streams feed found");
        }
    }

    Ok((feed_ids, feed_map))
}

/// Convert a BigInt (18 decimal places) to a Decimal USD price.
fn bigint_to_decimal(val: &BigInt) -> Option<Decimal> {
    // benchmark_price can be 8 or 18 decimals depending on the feed.
    // V3 crypto streams use 18 decimals.
    let divisor = BigInt::from(1_000_000_000_000_000_000i128); // 10^18
    let whole = val / &divisor;
    let remainder = val % &divisor;

    let whole_i64 = whole.to_i64()?;
    let remainder_i64 = remainder.to_i64()?.unsigned_abs();
    let price_str = format!("{}.{:018}", whole_i64, remainder_i64);
    price_str.parse::<Decimal>().ok().map(|d| d.normalize())
}

/// Connect to Chainlink Data Streams WebSocket and stream prices into PriceFeeds.
pub async fn run_chainlink_streams_feed(
    feeds: PriceFeeds,
    assets: Vec<String>,
) -> Result<()> {
    let api_key = std::env::var("CHAINLINK_DS_API_KEY")
        .context("CHAINLINK_DS_API_KEY env var required for Data Streams")?;
    let user_secret = std::env::var("CHAINLINK_DS_USER_SECRET")
        .context("CHAINLINK_DS_USER_SECRET env var required for Data Streams")?;

    let rest_url = "https://api.dataengine.chain.link".to_owned();
    let ws_url = "wss://ws.dataengine.chain.link".to_owned();

    let config = Config::new(api_key, user_secret, rest_url, ws_url)
        .with_ws_max_reconnect(100)
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to build Data Streams config: {e}"))?;

    info!(assets = ?assets, "Discovering Chainlink Data Streams feed IDs...");
    let (feed_ids, feed_map) = discover_feeds(&config, &assets).await?;

    if feed_ids.is_empty() {
        anyhow::bail!("No Data Streams feeds discovered for assets: {:?}", assets);
    }

    info!(
        feed_count = feed_ids.len(),
        feeds = ?feed_map.values().collect::<Vec<_>>(),
        "Starting Chainlink Data Streams WebSocket"
    );

    loop {
        match run_stream(&config, &feed_ids, &feed_map, &feeds).await {
            Ok(()) => {
                warn!("Data Streams stream ended normally, reconnecting in 2s...");
            }
            Err(e) => {
                error!(error = %e, "Data Streams stream error, reconnecting in 5s...");
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                continue;
            }
        }
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}

async fn run_stream(
    config: &Config,
    feed_ids: &[ID],
    feed_map: &FeedMap,
    feeds: &PriceFeeds,
) -> Result<()> {
    let mut stream = Stream::new(config, feed_ids.to_vec())
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create WS stream: {e}"))?;

    stream.listen().await
        .map_err(|e| anyhow::anyhow!("Failed to start listening: {e}"))?;
    info!("Chainlink Data Streams WebSocket connected and listening");

    loop {
        match stream.read().await {
            Ok(ws_report) => {
                DS_MSG_COUNT.fetch_add(1, Ordering::Relaxed);

                if let Err(e) = process_report(&ws_report, feed_map, feeds).await {
                    warn!(error = %e, "Failed to process Data Streams report");
                }
            }
            Err(e) => {
                error!(error = %e, "Data Streams read error");
                let _ = stream.close().await;
                return Err(anyhow::anyhow!("Stream read error: {e}"));
            }
        }
    }
}

async fn process_report(
    ws_report: &WebSocketReport,
    feed_map: &FeedMap,
    feeds: &PriceFeeds,
) -> Result<()> {
    let report = &ws_report.report;

    // full_report is a hex-encoded string, possibly with "0x" prefix
    let hex_str = report.full_report.strip_prefix("0x")
        .or_else(|| report.full_report.strip_prefix("0X"))
        .unwrap_or(&report.full_report);
    let payload = hex::decode(hex_str)
        .with_context(|| format!(
            "Failed to hex-decode full_report (len={}, first20={})",
            report.full_report.len(),
            &report.full_report[..report.full_report.len().min(20)]
        ))?;

    let (_report_context, report_blob) = decode_full_report(&payload)
        .map_err(|e| anyhow::anyhow!("decode_full_report failed: {e}"))?;

    let data = ReportDataV3::decode(&report_blob)
        .map_err(|e| anyhow::anyhow!("V3 decode failed: {e}"))?;

    let price = bigint_to_decimal(&data.benchmark_price)
        .context("Failed to convert benchmark_price to Decimal")?;

    let feed_id_bytes: [u8; 32] = data.feed_id.0;
    if let Some(chainlink_key) = feed_map.get(&feed_id_bytes) {
        feeds.set_price(chainlink_key, price).await;

        let count = DS_MSG_COUNT.load(Ordering::Relaxed);
        if count % 100 == 1 {
            info!(
                key = %chainlink_key,
                price = %price,
                ts = data.observations_timestamp,
                total_msgs = count,
                "Data Streams price update (sample)"
            );
        }
    } else {
        warn!(feed_id = %data.feed_id, "Received report for unknown feed ID");
    }

    Ok(())
}
