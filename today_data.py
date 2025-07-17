import os
import csv
import pandas as pd
import requests

session = requests.session()

HEADERS = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36"
}


def makeDataset(csv_url):
    print(f"⬇️ Downloading CSV from: {csv_url}")
    local_file = "dataset.csv"

    # Download CSV and save temporarily
    with open(local_file, "w", encoding="utf-8") as f:
        f.write(session.get(csv_url, headers=HEADERS).text)

    # Read the CSV back
    with open(local_file, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        nifty_data, stock_data = [], []

        for idx, row in enumerate(reader):
            if 8 <= idx <= 67:
                nifty_data.append(row)
            elif idx >= 120:
                stock_data.append(row)

    # Remove temp file
    os.remove(local_file)

    # Convert to DataFrames
    df_nifty = pd.DataFrame(nifty_data[1:], columns=nifty_data[0]) if nifty_data else pd.DataFrame()
    df_stocks = pd.DataFrame(stock_data[1:], columns=stock_data[0]) if stock_data else pd.DataFrame()

    return df_nifty, df_stocks


def getTodayData():
    print("📦 Fetching today's merged daily report metadata...")

    response = session.get("https://www.nseindia.com/api/merged-daily-reports?key=favCapital", headers=HEADERS)

    if response.status_code != 200:
        raise Exception(f"❌ Failed to fetch metadata: {response.status_code}")

    try:
        csv_url = response.json()[1]['link']
        return makeDataset(csv_url)
    except Exception as e:
        print("❌ Error parsing JSON or downloading CSV:", e)
        return pd.DataFrame(), pd.DataFrame()


# ✅ Example usage
if __name__ == "__main__":
    nifty_df, companies_df = getTodayData()
    print("\n📈 NIFTY Summary:")
    print(nifty_df.head())

    print("\n🏢 Company Data:")
    print(companies_df.head())
