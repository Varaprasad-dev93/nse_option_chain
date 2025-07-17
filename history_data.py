from io import StringIO
import requests
import pandas as pd
from datetime import datetime, timedelta
import bs4
import os

session = requests.session()

HEADERS = {
    'user-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36",
    'accept-language': 'en-US,en;q=0.9',
    'referer': 'https://www.nseindia.com'
}


def getHistoryData(company: str,
                   from_date: str = datetime.today().strftime("%d-%m-%Y"),
                   to_date: str = datetime(datetime.today().year - 1, datetime.today().month, datetime.today().day).strftime("%d-%m-%Y"),
                   save=False) -> pd.DataFrame:
    """
    Fetches historical stock data (OHLCV) for a specific company from NSE.

    :param company: NSE equity symbol (e.g., 'TCS', 'RELIANCE')
    :param from_date: Start date in DD-MM-YYYY
    :param to_date: End date in DD-MM-YYYY
    :param save: If True, saves to CSV
    :return: DataFrame of historical stock data
    """
    try:
        print(f"📦 Fetching historical data for {company} from {from_date} to {to_date}...")
        session.get("https://www.nseindia.com", headers=HEADERS)
        session.get(f"https://www.nseindia.com/get-quotes/equity?symbol={company}", headers=HEADERS)
        session.get(f"https://www.nseindia.com/api/historical/cm/equity?symbol={company}", headers=HEADERS)

        url = f"https://www.nseindia.com/api/historical/cm/equity?symbol={company}&series=[%22EQ%22]&from={from_date}&to={to_date}&csv=true"
        response = session.get(url, headers=HEADERS)

        if response.status_code != 200:
            raise Exception(f"❌ Error: Status code {response.status_code}")

        # Skip BOM (\ufeff) if present
        csv_data = response.text.lstrip('\ufeff')
        df = pd.read_csv(StringIO(csv_data))

        if save:
            filename = f"{company}_history.csv"
            df.to_csv(filename, index=False)
            print(f"✅ Saved to {filename}")

        return df

    except Exception as e:
        print(f"❌ Failed to fetch history for {company}: {e}")
        return pd.DataFrame()


def niftyHistoryData(index_name: str,
                     from_date: str = (datetime(datetime.today().year - 1, datetime.today().month, datetime.today().day) + timedelta(days=2)).strftime("%d-%m-%Y"),
                     to_date: str = datetime.today().strftime("%d-%m-%Y"),
                     save=False) -> pd.DataFrame:
    """
    Fetches historical index data (e.g., NIFTY 50) from NSE.

    :param index_name: Name of the index (e.g., 'NIFTY 50')
    :param from_date: Start date
    :param to_date: End date
    :param save: If True, saves to CSV
    :return: DataFrame of historical index data
    """
    try:
        print(f"📊 Fetching index data for {index_name} from {from_date} to {to_date}...")
        formatted_name = index_name.upper().replace(' ', '%20').replace('-', '%20')

        url = f"https://www1.nseindia.com/products/dynaContent/equities/indices/historicalindices.jsp?indexType={formatted_name}&fromDate={from_date}&toDate={to_date}"
        response = session.get(url, headers=HEADERS)

        soup = bs4.BeautifulSoup(response.text, 'html5lib')
        csv_div = soup.find('div', {'id': 'csvContentDiv'})

        if not csv_div:
            raise Exception("❌ CSV content not found in response")

        csv_data = csv_div.contents[0].replace(':', '\n')
        df = pd.read_csv(StringIO(csv_data))

        if save:
            filename = f"{index_name.replace(' ', '_')}_index_history.csv"
            df.to_csv(filename, index=False)
            print(f"✅ Saved to {filename}")

        return df

    except Exception as e:
        print(f"❌ Failed to fetch index data for {index_name}: {e}")
        return pd.DataFrame()


# Example usage:
df1 = getHistoryData('TCS', from_date='01-07-2023', to_date='30-06-2024', save=True)
df2 = niftyHistoryData('NIFTY 50', save=True)
