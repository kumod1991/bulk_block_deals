import requests
import pandas as pd
import time
from datetime import datetime
from supabase import create_client
import os

# ───────── CONFIG ─────────
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Strong browser-like headers
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "application/json",
    "Referer": "https://www.nseindia.com/get-quotes/equity",
    "Connection": "keep-alive"
}

# ───────────────── NSE SESSION INIT ─────────────────
def init_nse_session(session):
    base_headers = {
        "User-Agent": HEADERS["User-Agent"],
        "Accept-Language": HEADERS["Accept-Language"],
        "Accept": "text/html,application/xhtml+xml",
        "Connection": "keep-alive"
    }

    res = session.get("https://www.nseindia.com", headers=base_headers, timeout=10)
    if res.status_code != 200:
        raise Exception(f"NSE homepage failed: {res.status_code}")

    time.sleep(2)

    session.get(
        "https://www.nseindia.com/api",
        headers={**base_headers, "Accept": "application/json"},
        timeout=10
    )

# ───────────────── NSE FETCH ─────────────────
def fetch_nse():
    today = datetime.today().strftime("%d-%m-%Y")

    bulk_url = f"https://www.nseindia.com/api/bulk-deals?from={today}&to={today}"
    block_url = f"https://www.nseindia.com/api/block-deals?from={today}&to={today}"

    session = requests.Session()
    init_nse_session(session)

    time.sleep(1)

    # BULK
    bulk_resp = session.get(bulk_url, headers=HEADERS, timeout=10)
    if bulk_resp.status_code != 200:
        raise Exception("NSE bulk fetch failed")

    try:
        bulk_json = bulk_resp.json()
    except:
        raise Exception(f"NSE bulk invalid response: {bulk_resp.text[:200]}")

    bulk_data = bulk_json.get("data", [])

    time.sleep(1)

    # BLOCK
    block_resp = session.get(block_url, headers=HEADERS, timeout=10)
    if block_resp.status_code != 200:
        raise Exception("NSE block fetch failed")

    try:
        block_json = block_resp.json()
    except:
        raise Exception(f"NSE block invalid response: {block_resp.text[:200]}")

    block_data = block_json.get("data", [])

    print(f"NSE Bulk: {len(bulk_data)}, Block: {len(block_data)}")

    for row in bulk_data:
        row["deal_category"] = "BULK"

    for row in block_data:
        row["deal_category"] = "BLOCK"

    return bulk_data + block_data

# ───────────────── BSE FETCH ─────────────────
def fetch_bse():
    print("Fetching from BSE...")

    session = requests.Session()

    headers = {
        "User-Agent": HEADERS["User-Agent"],
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://www.bseindia.com/",
        "Origin": "https://www.bseindia.com",
        "Connection": "keep-alive"
    }

    # Init session
    session.get("https://www.bseindia.com/", headers=headers, timeout=10)
    time.sleep(1.5)

    today = datetime.today().strftime("%Y%m%d")
    url = f"https://api.bseindia.com/BseIndiaAPI/api/BulkBlockDeals/w?fdate={today}&segment=equity"

    response = session.get(url, headers=headers, timeout=10)

    print("BSE status:", response.status_code)
    print("BSE content-type:", response.headers.get("Content-Type"))

    if "application/json" not in response.headers.get("Content-Type", ""):
        raise Exception(f"BSE blocked: {response.text[:200]}")

    try:
        json_data = response.json()
    except:
        raise Exception(f"BSE JSON parse failed: {response.text[:200]}")

    data = json_data.get("Table", [])

    for row in data:
        deal_type = row.get("BUY_SELL", "").upper()
        if "BLOCK" in deal_type:
            row["deal_category"] = "BLOCK"
        else:
            row["deal_category"] = "BULK"

    print(f"BSE records: {len(data)}")

    return data

# ───────────────── FETCH ROUTER ─────────────────
def fetch_data():
    # NSE first
    for i in range(3):
        try:
            print(f"NSE Attempt {i+1}")
            return fetch_nse(), "NSE"
        except Exception as e:
            print("NSE failed:", e)
            time.sleep(2)

    # BSE fallback
    print("Switching to BSE fallback...")
    for i in range(3):
        try:
            print(f"BSE Attempt {i+1}")
            return fetch_bse(), "BSE"
        except Exception as e:
            print("BSE failed:", e)
            time.sleep(2)

    raise Exception("Both NSE and BSE failed")

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

    else:
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
        print("No records to insert")
        return

    supabase.table("bulk_block_deals").upsert(
        records,
        on_conflict="symbol,client_name,trade_date,deal_type,deal_category"
    ).execute()

    print("Inserted:", len(records))

# ───────────────── MAIN ─────────────────
def main():
    if datetime.today().weekday() > 4:
        print("Weekend — skipping")
        return

    print("Starting job...")

    raw_data, source = fetch_data()

    print(f"Source: {source}")
    print(f"Raw records: {len(raw_data)}")

    records = transform_data(raw_data, source)

    load_to_supabase(records)

    print("Job completed successfully")

if __name__ == "__main__":
    main()
