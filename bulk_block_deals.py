import requests
import pandas as pd
from datetime import datetime
from supabase import create_client
import os

# ─── CONFIG ─────────────────────────────────────────────
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# NSE headers (VERY IMPORTANT)
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
    "Referer": "https://www.nseindia.com/"
}

# ─── FETCH DATA ─────────────────────────────────────────
def fetch_bulk_deals():
    today = datetime.today().strftime("%d-%m-%Y")

    url = f"https://www.nseindia.com/api/bulk-deals?from={today}&to={today}"

    session = requests.Session()
    session.get("https://www.nseindia.com", headers=HEADERS)

    response = session.get(url, headers=HEADERS)

    if response.status_code != 200:
        raise Exception("Failed to fetch NSE data")

    data = response.json()

    return data.get("data", [])


# ─── TRANSFORM ─────────────────────────────────────────
def transform_data(raw_data):
    df = pd.DataFrame(raw_data)

    if df.empty:
        return []

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

    return df.to_dict(orient="records")


# ─── LOAD TO SUPABASE ───────────────────────────────────
def load_to_supabase(records):
    if not records:
        print("No data to insert")
        return

    response = supabase.table("bulk_block_deals").upsert(
        records,
        on_conflict="symbol,client_name,trade_date,deal_type"
    ).execute()

    print("Inserted:", len(records))


# ─── MAIN ───────────────────────────────────────────────
def main():
    print("Fetching bulk deals...")
    raw = fetch_bulk_deals()

    print("Transforming...")
    records = transform_data(raw)

    print("Uploading to Supabase...")
    load_to_supabase(records)

    print("Done.")


if __name__ == "__main__":
    main()