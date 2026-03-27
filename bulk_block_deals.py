import cloudscraper
import pandas as pd
import time
from datetime import datetime
from supabase import create_client
import os

# ───────── CONFIG ─────────
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ───────────────── NSE FETCH (CLOUDSCRAPER) ─────────────────
def fetch_nse():
    today = datetime.today().strftime("%d-%m-%Y")

    bulk_url = f"https://www.nseindia.com/api/bulk-deals?from={today}&to={today}"
    block_url = f"https://www.nseindia.com/api/block-deals?from={today}&to={today}"

    scraper = cloudscraper.create_scraper(
        browser={
            "browser": "chrome",
            "platform": "windows",
            "mobile": False
        }
    )

    print("Initializing NSE session...")

    # Warm-up (CRITICAL)
    res = scraper.get("https://www.nseindia.com", timeout=20)
    if res.status_code != 200:
        raise Exception(f"NSE warmup failed: {res.status_code}")

    time.sleep(2)

    # ───── BULK DEALS ─────
    bulk_resp = scraper.get(bulk_url, timeout=20)
    if bulk_resp.status_code != 200:
        raise Exception(f"NSE bulk failed: {bulk_resp.status_code}")

    try:
        bulk_json = bulk_resp.json()
    except:
        raise Exception(f"NSE bulk invalid JSON: {bulk_resp.text[:200]}")

    bulk_data = bulk_json.get("data", [])

    time.sleep(1)

    # ───── BLOCK DEALS ─────
    block_resp = scraper.get(block_url, timeout=20)
    if block_resp.status_code != 200:
        raise Exception(f"NSE block failed: {block_resp.status_code}")

    try:
        block_json = block_resp.json()
    except:
        raise Exception(f"NSE block invalid JSON: {block_resp.text[:200]}")

    block_data = block_json.get("data", [])

    # Tag categories
    for row in bulk_data:
        row["deal_category"] = "BULK"

    for row in block_data:
        row["deal_category"] = "BLOCK"

    total = len(bulk_data) + len(block_data)

    print(f"NSE fetched → Bulk: {len(bulk_data)}, Block: {len(block_data)}")

    if total == 0:
        raise Exception("NSE returned empty data")

    return bulk_data + block_data


# ───────────────── FETCH ROUTER ─────────────────
def fetch_data():
    for i in range(3):
        try:
            print(f"NSE Attempt {i+1}")
            data = fetch_nse()
            return data, "NSE"
        except Exception as e:
            print("NSE failed:", e)
            time.sleep(3)

    print("All NSE attempts failed — returning empty dataset")
    return [], "NONE"


# ───────────────── TRANSFORM ─────────────────
def transform_data(raw):
    if not raw:
        return []

    df = pd.DataFrame(raw)

    df = df.rename(columns={
        "symbol": "symbol",
        "securityName": "security_name",
        "clientName": "client_name",
        "buySell": "deal_type",
        "quantityTraded": "quantity",
        "tradePrice": "price",
        "date": "trade_date"
    })

    df["exchange"] = "NSE"
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date

    df = df[
        [
            "symbol",
            "security_name",
            "client_name",
            "deal_type",
            "deal_category",
            "quantity",
            "price",
            "trade_date",
            "exchange",
        ]
    ]

    print("Transformed rows:", len(df))

    return df.to_dict(orient="records")


# ───────────────── LOAD ─────────────────
def load_to_supabase(records):
    if not records:
        print("No records to insert (safe exit)")
        return

    try:
        supabase.table("bulk_block_deals").upsert(
            records,
            on_conflict="symbol,client_name,trade_date,deal_type,deal_category"
        ).execute()

        print("Inserted:", len(records))

    except Exception as e:
        print("Supabase insert failed:", e)


# ───────────────── MAIN ─────────────────
def main():
    if datetime.today().weekday() > 4:
        print("Weekend — skipping")
        return

    print("Starting NSE bulk/block deals job...")

    raw_data, source = fetch_data()

    print(f"Source: {source}")
    print(f"Raw records: {len(raw_data)}")

    records = transform_data(raw_data)

    load_to_supabase(records)

    print("Job completed")


# ───────────────── RUN ─────────────────
if __name__ == "__main__":
    main()
