# Polymarket Whale & Insider Detector

Standalone Python service that polls public Polymarket trade data, detects large (“whale”) and clustered fills, scores wallets for insider-like behaviour, and sends alerts to Telegram while persisting rows to Supabase for PolyEdgeAI.com.

This project is independent of the Rust `polybot` trading bot: separate directory, venv, and systemd unit.

## Data sources

| Purpose | Endpoint |
|--------|----------|
| Recent fills (no auth) | `GET https://data-api.polymarket.com/trades` |
| Active markets | `GET https://gamma-api.polymarket.com/markets?closed=false&limit=500` (paginated) |
| Order book / mid price | `GET https://clob.polymarket.com/book?token_id=…` |

The CLOB `GET /trades` route is authenticated in current Polymarket docs; this service uses the **Data API** for global recent trades instead. Wallet history uses `GET …/trades?user=<address>` on the same Data API.

## Setup

1. Copy `.env.example` to `.env` and fill secrets (Telegram, Supabase).
2. In Supabase, run `supabase/whale_alerts.sql` to create `whale_alerts`.
3. Python 3.11+:

```bash
python -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate
pip install -r requirements.txt
python -m src.main
```

## Deployment (Lightsail)

Paths match `systemd/whale-detector.service`: install under `/home/ubuntu/polymarket-whale-detector`, copy the unit to `/etc/systemd/system/`, `daemon-reload`, `enable`, `start`. Logs: `journalctl -u whale-detector -f`.

## Behaviour notes

- First poll after startup only seeds seen trade IDs (no alerts) to avoid a backlog burst.
- Insider scoring uses cached wallet trade counts (1 hour TTL), at most 10 full analyses per poll.
- Known market makers: optional manual list in `known_wallets.json`; auto-tag when `>200` trades across `>20` markets.
- Telegram queue caps at 20 messages per minute; duplicate suppression per market+wallet every 10 minutes.

## Requirements stack

Includes `py-clob-client` for alignment with Polymarket tooling; live reads use `httpx` against public HTTP APIs as above.
