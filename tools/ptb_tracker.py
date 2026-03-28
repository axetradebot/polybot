#!/usr/bin/env python3
"""
PriceToBeat Timing Tracker (Binance edition)

Logs Binance BTC/USDT price every second throughout a 5-minute window,
then fetches priceToBeat from Polymarket's Gamma API and finds which
second's price best matches — revealing when Polymarket snapshots.

Usage:
    pip install websockets requests
    python tools/ptb_tracker.py [--windows N] [--symbol btcusdt]
"""

import argparse
import asyncio
import csv
import json
import time
from datetime import datetime, timezone

import requests
import websockets

BINANCE_WS = "wss://stream.binance.com:9443/ws"
GAMMA_API = "https://gamma-api.polymarket.com/markets"
WINDOW_SECS = 300


def current_window_ts() -> int:
    now = int(time.time())
    return now - (now % WINDOW_SECS)


def secs_into_window() -> float:
    return time.time() - current_window_ts()


def slug_for_symbol(binance_sym: str, window_ts: int) -> str:
    asset = binance_sym.replace("usdt", "").lower()
    return f"{asset}-updown-5m-{window_ts}"


def fetch_price_to_beat(slug: str, retries: int = 5) -> float | None:
    for attempt in range(retries):
        try:
            resp = requests.get(f"{GAMMA_API}?slug={slug}", timeout=10)
            resp.raise_for_status()
            data = resp.json()
            if not data:
                return None
            events = data[0].get("events")
            if not events:
                return None
            meta = events[0].get("event_metadata", {})
            ptb = meta.get("priceToBeat")
            if ptb is not None:
                return float(ptb)
        except Exception as e:
            print(f"    Gamma API attempt {attempt+1}: {e}")
        if attempt < retries - 1:
            time.sleep(10)
    return None


async def track_window(symbol: str, window_num: int, total_windows: int):
    wts = current_window_ts()
    slug = slug_for_symbol(symbol, wts)
    csv_file = f"ptb_log_{symbol}_{wts}.csv"

    print(f"\n{'='*60}")
    print(f"Window {window_num}/{total_windows}: {slug}")
    print(f"  Window start: {datetime.fromtimestamp(wts, tz=timezone.utc).isoformat()}")
    print(f"  Currently {secs_into_window():.1f}s into window")
    print(f"  Logging Binance {symbol} every second...")
    print(f"{'='*60}")

    # Per-second price samples: {secs_into_window_int: (epoch, price)}
    samples: dict[int, tuple[float, float]] = {}
    last_price = None

    stream_url = f"{BINANCE_WS}/{symbol}@trade"

    async with websockets.connect(stream_url) as ws:
        end_time = wts + WINDOW_SECS + 2
        last_log_sec = -1

        while time.time() < end_time:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=2)
            except asyncio.TimeoutError:
                continue

            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue

            price = float(msg.get("p", 0))
            if price <= 0:
                continue

            now = time.time()
            sec = int(now - wts)
            last_price = price

            if sec not in samples and 0 <= sec <= WINDOW_SECS:
                samples[sec] = (now, price)

            if sec != last_log_sec and sec % 10 == 0 and sec >= 0:
                print(f"    [{sec:4d}s] {symbol} = {price:.2f}")
                last_log_sec = sec

    # Write CSV
    with open(csv_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["secs_into_window", "epoch", "price"])
        for sec in sorted(samples.keys()):
            ep, pr = samples[sec]
            writer.writerow([sec, f"{ep:.3f}", f"{pr:.8f}"])

    print(f"\n  Logged {len(samples)} per-second samples to {csv_file}")

    # Fetch priceToBeat
    print("  Waiting 20s for Gamma API to populate priceToBeat...")
    await asyncio.sleep(20)
    ptb = fetch_price_to_beat(slug)

    if ptb is None:
        print("  [!] Could not fetch priceToBeat. Try manually:")
        print(f"      curl '{GAMMA_API}?slug={slug}' | python -m json.tool")
        return

    print(f"\n  priceToBeat from Polymarket: {ptb}")

    # Find closest match
    best_sec = -1
    best_diff = float("inf")
    matches = []

    for sec in sorted(samples.keys()):
        _, price = samples[sec]
        diff = abs(price - ptb)
        pct = diff / ptb * 100 if ptb else 0
        if diff < best_diff:
            best_diff = diff
            best_sec = sec
        if pct < 0.005:  # within 0.005%
            matches.append((sec, price, diff, pct))

    print(f"\n  {'='*60}")
    print(f"  RESULTS")
    print(f"  {'='*60}")
    print(f"  priceToBeat:     {ptb}")
    print(f"  Closest match:   second {best_sec} into window")
    print(f"    Price:         {samples[best_sec][1]:.8f}")
    print(f"    Diff:          {best_diff:.8f} ({best_diff/ptb*100:.6f}%)")

    if matches:
        print(f"\n  All matches within 0.005%:")
        for sec, price, diff, pct in matches:
            marker = " <<<" if sec == best_sec else ""
            print(f"    [{sec:4d}s] {price:.8f}  diff={diff:.8f} ({pct:.6f}%){marker}")

    # Show prices around match and around key timestamps
    print(f"\n  Prices around closest match (second {best_sec}):")
    for sec in range(max(0, best_sec - 5), min(WINDOW_SECS, best_sec + 6)):
        if sec in samples:
            _, price = samples[sec]
            diff = price - ptb
            marker = " >>>" if sec == best_sec else "    "
            print(f"  {marker} [{sec:4d}s] {price:.8f}  diff={diff:+.8f}")

    # Key timing regions
    for label, region_start, region_end in [
        ("Window start (0-5s)", 0, 6),
        ("T-240 region (55-70s)", 55, 71),
        ("T-180 region (115-125s)", 115, 126),
    ]:
        print(f"\n  {label}:")
        for sec in range(region_start, region_end):
            if sec in samples:
                _, price = samples[sec]
                diff = price - ptb
                marker = " >>>" if sec == best_sec else "    "
                print(f"  {marker} [{sec:4d}s] {price:.8f}  diff={diff:+.8f}")

    print()


async def main():
    parser = argparse.ArgumentParser(description="Track priceToBeat timing")
    parser.add_argument("--windows", type=int, default=2, help="Windows to track (default: 2)")
    parser.add_argument("--symbol", type=str, default="btcusdt", help="Binance symbol (default: btcusdt)")
    args = parser.parse_args()

    print(f"PriceToBeat Timing Tracker (Binance)")
    print(f"  Symbol: {args.symbol}")
    print(f"  Windows to track: {args.windows}")
    print(f"  Current time: {datetime.now(timezone.utc).isoformat()}")

    for i in range(args.windows):
        secs_in = secs_into_window()
        if secs_in > 10:
            wait = WINDOW_SECS - secs_in + 1
            print(f"\n  Waiting {wait:.0f}s for next window to start...")
            await asyncio.sleep(wait)

        await track_window(args.symbol, i + 1, args.windows)


if __name__ == "__main__":
    asyncio.run(main())
