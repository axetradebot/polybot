use std::time::{SystemTime, UNIX_EPOCH};

const WINDOW_SECONDS: u64 = 300;

fn epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn current_window_ts() -> u64 {
    let now = epoch_secs();
    now - (now % WINDOW_SECONDS)
}

fn seconds_until_close() -> u64 {
    let now = epoch_secs();
    let window_ts = now - (now % WINDOW_SECONDS);
    let close_time = window_ts + WINDOW_SECONDS;
    close_time.saturating_sub(now)
}

fn market_slug(window_ts: u64) -> String {
    format!("btc-updown-5m-{window_ts}")
}

#[test]
fn test_market_slug_format() {
    let slug = market_slug(1711036800);
    assert_eq!(slug, "btc-updown-5m-1711036800");
}

#[test]
fn test_market_slug_deterministic() {
    let slug1 = market_slug(1711036800);
    let slug2 = market_slug(1711036800);
    assert_eq!(slug1, slug2);
}

#[test]
fn test_window_ts_alignment() {
    let ts = current_window_ts();
    assert_eq!(ts % 300, 0, "Window timestamp must be 5-minute aligned");
}

#[test]
fn test_seconds_until_close_range() {
    let secs = seconds_until_close();
    assert!(secs <= 300, "Seconds until close must be <= 300");
}

#[test]
fn test_window_info_consistency() {
    let window_ts = current_window_ts();
    let slug = market_slug(window_ts);
    let remaining = seconds_until_close();

    assert_eq!(slug, format!("btc-updown-5m-{window_ts}"));
    assert!(remaining <= 300);
    assert_eq!(window_ts % 300, 0);
}
