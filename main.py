from fastapi import FastAPI, Query
from pydantic import BaseModel
from fetch_option_chain import fetch_main
from pymongo import MongoClient
from threading import Thread
import requests
import time
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import pandas as pd

app = FastAPI()

# Allow CORS for frontend (e.g., localhost:5173 if using Vite)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],  # or "*" for all origins (not recommended in production)
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# MongoDB Atlas Connection
client = MongoClient("mongodb+srv://varaprasadyoyo:JXlWJPUxAJiVB7Gx@cluster0.eviy31f.mongodb.net/AlgoSaaS?retryWrites=true&w=majority&appName=Cluster0",
                      tls=True,
                      tlsAllowInvalidCertificates=False
                     )
db = client["option_chain"]
calls_col = db["option_chain_calls"]
puts_col = db["option_chain_puts"]

# Global session for NSE scraping
session = requests.session()

# Headers to mimic browser
headers = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/87.0.4280.88 Safari/537.36 ",
    "accept-language": "en-US,en;q=0.9",
    "accept": "application/json",
    "referer": "https://www.nseindia.com/option-chain"
}

# Function to fetch symbol by company name
def getId(name):
    search_url = 'https://www.nseindia.com/api/search/autocomplete?q={}'
    get_details = 'https://www.nseindia.com/api/quote-equity?symbol={}'

    session.get('https://www.nseindia.com/', headers=headers)
    search_response = session.get(search_url.format(name), headers=headers)
    search_result = search_response.json()['symbols'][0]['symbol']
    session.get(get_details.format(search_result), headers=headers)

    return search_result

# Background thread for periodic scraping
symbols_to_track = []

def periodic_scraper():
    while True:
        for symbol in symbols_to_track:
            try:
                fetch_main(symbol)
                print(f"[✅] Updated data for {symbol} at {time.ctime()}")
            except Exception as e:
                print(f"[❌] Error updating {symbol}: {e}")
        time.sleep(5) 

Thread(target=periodic_scraper, daemon=True).start()


@app.get("/option-chain")
def option_chain(company_name: str = Query(..., description="Name of the company")):
    try:
        symbol = getId(company_name)
        if symbol not in symbols_to_track:
            symbols_to_track.append(symbol)
        fetch_main(symbol)
        return {"status": "success", "symbol": symbol, "message": "Data fetched and stored to MongoDB."}
    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.get("/option-chain/data")
def get_data(symbol: str = Query(..., description="NSE Symbol (e.g. TCS, INFY)")):
    try:
        calls_cursor = list(calls_col.find({"symbol": symbol.upper()}).sort("fetched_at", -1).limit(1))
        puts_cursor = list(puts_col.find({"symbol": symbol.upper()}).sort("fetched_at", -1).limit(1))

        calls = calls_cursor[0] if calls_cursor else {}
        puts = puts_cursor[0] if puts_cursor else {}

        return {"calls": calls, "puts": puts}
    except Exception as e:
        return {"status": "error", "message": f"Data not found for symbol {symbol}: {e}"}

@app.get("/api/option-chain-plot")
async def get_option_chain():
    df = pd.read_csv('option-chain.csv', header=1)
    df.columns = df.columns.str.strip()
    df = df.dropna(subset=["STRIKE"])
    for col in ["STRIKE", "LTP", "CHNG", "VOLUME", "IV", "OI", "OI.1"]:
        if col in df:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace(",", ""),
                errors="coerce"
            )
    df = df.rename(columns={"OI": "Call OI", "OI.1": "Put OI"})
    df = df.sort_values("STRIKE")

    records = (
        df[["STRIKE", "Call OI", "Put OI"]]
        .melt(
            id_vars=["STRIKE"],
            value_vars=["Call OI", "Put OI"],
            var_name="Option Type",
            value_name="Open Interest"
        )
        .to_dict(orient="records")
    )

    return JSONResponse(content=records)