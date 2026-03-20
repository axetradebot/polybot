# Polymarket BTC 5-Minute Maker Bot

Rust-based maker bot for Polymarket's 5-minute BTC Up/Down prediction markets.

Places maker limit orders on the directionally-correct side in the final seconds before resolution, earning $0.05–$0.20 per correct contract with zero maker fees plus USDC rebates.

## Quick Start

```bash
# 1. Copy and configure environment
cp .env.example .env
# Edit .env with your private key and API credentials

# 2. Review config.toml for trading parameters

# 3. Build
cargo build --release

# 4. Run in paper mode (default)
./target/release/polymarket-btc-bot

# 5. Run in live mode (after paper validation)
BOT_MODE=live ./target/release/polymarket-btc-bot
```

## How It Works

1. Every 5 minutes, a new BTC Up/Down market opens (288/day)
2. At **T-10s** before window close, compares current BTC price vs window open price
3. If the delta exceeds a threshold, places a **maker limit BUY** on the winning outcome
4. If filled and correct: profit = $1.00 − entry price per contract
5. Auto-settles and tracks P&L

## Configuration

- **`.env`** — Secrets (private key, API keys, Telegram token)
- **`config.toml`** — All tunable parameters (bet sizing, signal thresholds, pricing tiers)

## Pre-Flight Checklist

- [ ] Fund Polymarket wallet with $500+ USDC on Polygon
- [ ] Run token approvals (one-time)
- [ ] Generate API credentials via SDK
- [ ] Run paper mode for 48+ hours
- [ ] Verify win rate >80% on trades where delta >0.05%
- [ ] Go live with $5 max bet, monitor for 24 hours

## Architecture

- **Binance WebSocket** — real-time BTC/USDT price via `tokio-tungstenite`
- **Polymarket SDK** — CLOB orders, Gamma market resolution, WS orderbook
- **Signal Engine** — delta-based direction + pricing tier model
- **Risk Manager** — daily loss limits, consecutive loss circuit breaker, flat sizing
- **SQLite** — persistent trade log
- **Telegram** — real-time notifications

## Building

Requires Rust 1.88+.

```bash
cargo build --release
cargo test
```
