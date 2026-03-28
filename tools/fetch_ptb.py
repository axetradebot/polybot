import requests, json, sys, csv

slug = sys.argv[1] if len(sys.argv) > 1 else "btc-updown-5m-1774713300"
r = requests.get(f"https://gamma-api.polymarket.com/markets?slug={slug}")
data = r.json()
evt = data[0].get("events", [])
evtm = evt[0] if evt else {}
meta = evtm.get("eventMetadata", {})
ptb = meta.get("priceToBeat")
final_price = meta.get("finalPrice")
print(f"slug: {slug}")
print(f"priceToBeat: {ptb}")
print(f"finalPrice:  {final_price}")

if ptb and len(sys.argv) > 2:
    csv_file = sys.argv[2]
    print(f"\nSearching {csv_file} for closest match to priceToBeat={ptb}...")
    best_sec, best_diff, best_price = -1, float("inf"), 0
    with open(csv_file) as f:
        reader = csv.DictReader(f)
        for row in reader:
            sec = int(row["secs_into_window"])
            price = float(row["price"])
            diff = abs(price - ptb)
            if diff < best_diff:
                best_diff = diff
                best_sec = sec
                best_price = price

    print(f"Closest match: second {best_sec}")
    print(f"  Binance price: {best_price:.8f}")
    print(f"  priceToBeat:   {ptb}")
    print(f"  Diff:          {best_diff:.8f} ({best_diff/ptb*100:.6f}%)")

    print(f"\nPrices around match:")
    with open(csv_file) as f:
        reader = csv.DictReader(f)
        for row in reader:
            sec = int(row["secs_into_window"])
            price = float(row["price"])
            if abs(sec - best_sec) <= 5:
                diff = price - ptb
                marker = " >>>" if sec == best_sec else "    "
                print(f"{marker} [{sec:4d}s] {price:.8f}  diff={diff:+.8f}")

    print(f"\nPrices at key timestamps:")
    with open(csv_file) as f:
        reader = csv.DictReader(f)
        for row in reader:
            sec = int(row["secs_into_window"])
            price = float(row["price"])
            if sec <= 5 or (55 <= sec <= 70) or (115 <= sec <= 125):
                diff = price - ptb
                print(f"    [{sec:4d}s] {price:.8f}  diff={diff:+.8f}")
