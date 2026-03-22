import requests
import pandas as pd
import time
from datetime import datetime
from supabase import create_client
import os

# ───────────────── CONFIG ─────────────────
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "application/json",
    "Referer": "https://www.nseindia.com/report-detail/display-bulk-and-block-deals"
}

# ───────────────── NSE FETCH (BULK + BLOCK) ─────────────────
def fetch_nse():
    today = datetime.today().strftime("%d-%m-%Y")

    bulk_url = f"https://www.nseindia.com/api/bulk-deals?from={today}&to={today}"
    block_url = f"https://www.nseindia.com/api/block-deals?from={today}&to={today}"

    session = requests.Session()

    # Step 1: Initialize cookies
    homepage = session.get("https://www.nseindia.com", headers=HEADERS, timeout=10)
    if homepage.status_code != 200:
        raise Exception("NSE session init failed")

    time.sleep(1.5)

    # Step 2: Fetch BULK deals
    bulk_resp = session.get(bulk_url, headers=HEADERS, timeout=10)
    if bulk_resp.status_code != 200:
        raise Exception("Bulk deals fetch failed")

    bulk_data = bulk_resp.json().get("data", [])

    time.sleep(1)

    # Step 3: Fetch BLOCK deals
    block_resp = session.get(block_url, headers=HEADERS, timeout=10)
    if block_resp.status_code != 200:
        raise Exception("Block deals fetch failed")

    block_data = block_resp.json().get("data", [])

    print(f"NSE Bulk: {len(bulk_data)}, Block: {len(block_data)}")

    # Tag deal category
    for row in bulk_data:
        row["deal_category"] = "BULK"

    for row in block_data:
        row["deal_category"] = "BLOCK"

    return bulk_data + block_data


# ───────────────── BSE FETCH (FALLBACK) ─────────────────
def fetch_bse():
    print("Fetching from BSE...")

    today = datetime.today().strftime("%Y%m%d")

    url = f"https://api.bseindia.com/BseIndiaAPI/api/BulkBlockDeals/w?fdate={today}&segment=equity"

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
        "Referer": "https://www.bseindia.com/"
    }

    response = requests.get(url, headers=headers, timeout=10)

    if response.status_code != 200:
        raise Exception("BSE fetch failed")

    data = response.json().get("Table", [])

    # Tag category (BSE gives both together)
    for row in data:
        deal_type = row.get("BUY_SELL", "").upper()
        row["deal_category"] = "BLOCK" if "BLOCK" in deal_type else "BULK"

    return data


# ───────────────── RETRY ─────────────────
def fetch_with_retry():
    for i in range(3):
        try:
            print(f"Attempt {i+1}: Fetching NSE...")
            return fetch_nse()
        except Exception as e:
            print("NSE attempt failed:", e)
            time.sleep(2)

    raise Exception("All NSE retries failed")


# ───────────────── FETCH ROUTER ─────────────────
def fetch_data():
    try:
        return fetch_with_retry(), "NSE"
    except:
        print("Switching to BSE fallback...")
        return fetch_bse(), "BSE"


# ───────────────── TRANSFORM ─────────────────
def transform_data(raw, source):
    if not raw:
        return []

    df = pd.DataFrame(raw)

    if source == "NSE":
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

    else:  # BSE
        df = df.rename(columns={
            "SCRIP_CODE": "symbol",
            "SCRIP_NAME": "security_name",
            "CLIENT_NAME": "client_name",
            "BUY_SELL": "deal_type",
            "QTY": "quantity",
            "PRICE": "price",
            "DEAL_DATE": "trade_date"
        })

        df["exchange"] = "BSE"

    # Ensure deal_category exists
    if "deal_category" not in df.columns:
        df["deal_category"] = "UNKNOWN"

    # Convert date
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date

    # Final columns
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


# ───────────────── LOAD TO SUPABASE ─────────────────
def load_to_supabase(records):
    if not records:
        print("No records to insert")
        return

    response = supabase.table("bulk_block_deals").upsert(
        records,
        on_conflict="symbol,client_name,trade_date,deal_type,deal_category"
    ).execute()

    print("Inserted/Upserted:", len(records))


# ───────────────── MAIN ─────────────────
def main():
    # Skip weekends
    if datetime.today().weekday() > 4:
        print("Weekend — skipping job")
        return

    print("Starting bulk/block deals job...")

    raw_data, source = fetch_data()

    print(f"Source used: {source}")
    print(f"Raw records: {len(raw_data)}")

    records = transform_data(raw_data, source)

    load_to_supabase(records)

    print("Job completed successfully")


if __name__ == "__main__":
    main()
