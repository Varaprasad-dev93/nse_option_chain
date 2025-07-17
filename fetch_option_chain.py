import time
import requests
import pandas as pd
from datetime import datetime
from pymongo import MongoClient
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
# from apscheduler.schedulers.background import BackgroundScheduler

# --- MongoDB Setup ---
client = MongoClient("mongodb+srv://varaprasadyoyo:JXlWJPUxAJiVB7Gx@cluster0.eviy31f.mongodb.net/AlgoSaaS?retryWrites=true&w=majority&appName=Cluster0"
                     )
db = client["nse_data"]
ce_col = db["nifty_ce"]
pe_col = db["nifty_pe"]

# --- HTTP Session with Retry ---
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept-Language": "en-US,en;q=0.9",
}
session = requests.Session()
session.mount("https://", HTTPAdapter(
    max_retries=Retry(total=3, backoff_factor=1, status_forcelist=[429,500,502,503,504])
))

def refresh_cookies():
    session.get("https://www.nseindia.com/", headers=HEADERS, timeout=5)
    session.get("https://www.nseindia.com/option-chain", headers=HEADERS, timeout=5)
    time.sleep(1)

def fetch_option_chain(symbol="NIFTY"):
    url = f"https://www.nseindia.com/api/option-chain-indices?symbol={symbol}"
    for _ in range(3):
        refresh_cookies()
        res = session.get(url, headers=HEADERS, timeout=10)
        if res.status_code != 200 or not res.text.strip():
            continue
        try:
            return res.json()
        except ValueError:
            continue
    raise RuntimeError("Failed to fetch valid JSON")

def flatten_chain(json_data):
    ce_docs = []
    pe_docs = []
    ts = datetime.utcnow()
    for entry in json_data["records"]["data"]:
        strike = entry["strikePrice"]
        expiry = entry["expiryDate"]
        if (ce := entry.get("CE")):
            ce.update({
                "strikePrice": strike,
                "expiryDate": expiry,
                "type": "CE",
                "fetchedAt": ts
            })
            ce_docs.append(ce)
        if (pe := entry.get("PE")):
            pe.update({
                "strikePrice": strike,
                "expiryDate": expiry,
                "type": "PE",
                "fetchedAt": ts
            })
            pe_docs.append(pe)
    return ce_docs, pe_docs

def store_to_mongo(ce_docs, pe_docs):
    ce_col.create_index(
        [("strikePrice",1),("expiryDate",1),("fetchedAt",1)],
        unique=True
    )
    pe_col.create_index(
        [("strikePrice",1),("expiryDate",1),("fetchedAt",1)],
        unique=True
    )

    # Insert CE docs
    try:
        res = ce_col.insert_many(ce_docs, ordered=False)
        print(f"Inserted {len(res.inserted_ids)} CE docs")
    except Exception:
        print("CE insert: skipped duplicates")

    # Insert PE docs
    try:
        res = pe_col.insert_many(pe_docs, ordered=False)
        print(f"Inserted {len(res.inserted_ids)} PE docs")
    except Exception:
        print("PE insert: skipped duplicates")

def fetch_main(symbol="NIFTY"):
    data = fetch_option_chain(symbol)
    ce_docs, pe_docs = flatten_chain(data)
    # print(ce_docs[:2], pe_docs[:2])  # Print first 2 entries for debugging
    store_to_mongo(ce_docs, pe_docs)
    print(f"✅ Data appended separately to `nifty_ce` and `nifty_pe`")

