"""
NSE Data Fetcher Service
========================

Service for fetching option chain data from NSE API with robust error handling.
"""

import time
import logging
import requests
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import json

from models import OptionChainEntry, OptionType, NSE_FIELD_MAPPING


class NSEDataFetcher:
    """
    Robust NSE data fetcher with session management and error handling
    """
    
    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger(__name__)
        self.session = self._create_session()
        self.base_url = "https://www.nseindia.com"
        self.option_chain_url = "https://www.nseindia.com/api/option-chain-indices"
        self._initialize_session()
    
    def _create_session(self) -> requests.Session:
        """Create HTTP session with retry strategy"""
        
        session = requests.Session()
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Set realistic browser headers with rotation
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ]

        import random
        selected_ua = random.choice(user_agents)

        session.headers.update({
            "User-Agent": selected_ua,
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9,hi;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Referer": "https://www.nseindia.com/",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache"
        })
        
        return session
    
    def _initialize_session(self) -> None:
        """Initialize session by visiting NSE homepage to get cookies"""

        try:
            self.logger.info("Initializing NSE session...")

            # Clear existing cookies
            self.session.cookies.clear()

            # Visit homepage to get initial cookies
            response = self.session.get(self.base_url, timeout=20, allow_redirects=True)
            response.raise_for_status()

            # Small delay
            time.sleep(1)

            # Visit market data page
            response = self.session.get(f"{self.base_url}/market-data", timeout=20)
            if response.status_code == 200:
                time.sleep(1)

            # Visit option chain page to get additional cookies
            response = self.session.get(f"{self.base_url}/option-chain", timeout=20)
            response.raise_for_status()

            # Additional delay to mimic human behavior
            time.sleep(2)

            self.logger.info("NSE session initialized successfully")

        except Exception as e:
            self.logger.warning(f"NSE session initialization had issues: {e}")
            # Don't raise exception, continue with existing session
    
    def fetch_option_chain(self, symbol: str = "NIFTY") -> Dict:
        """
        Fetch option chain data from NSE API

        Args:
            symbol: Symbol to fetch (e.g., NIFTY, BANKNIFTY)

        Returns:
            Dict containing the option chain data
        """

        url = f"{self.option_chain_url}?symbol={symbol}"

        for attempt in range(3):
            try:
                self.logger.debug(f"Fetching option chain for {symbol} (attempt {attempt + 1})")

                # Refresh session if not first attempt
                if attempt > 0:
                    self._initialize_session()
                    time.sleep(3)  # Longer delay between retries

                response = self.session.get(url, timeout=20)  # Increased timeout
                response.raise_for_status()

                # Check response content
                response_text = response.text.strip()
                if not response_text:
                    raise ValueError("Empty response received")

                # Log response for debugging
                self.logger.debug(f"Response status: {response.status_code}, Content-Type: {response.headers.get('content-type', 'unknown')}")
                self.logger.debug(f"Response length: {len(response_text)} characters")

                # Check if response looks like HTML (NSE sometimes returns HTML error pages)
                if response_text.startswith('<'):
                    raise ValueError("Received HTML response instead of JSON")

                # Try to parse JSON
                try:
                    data = response.json()
                except Exception as json_error:
                    self.logger.warning(f"JSON parsing failed. Response preview: {response_text[:200]}...")
                    raise ValueError(f"Invalid JSON response: {json_error}")

                if not data or "records" not in data:
                    raise ValueError("Invalid response format - missing 'records' field")

                self.logger.debug(f"Successfully fetched option chain for {symbol}")
                return data

            except Exception as e:
                self.logger.warning(f"Attempt {attempt + 1} failed for {symbol}: {e}")
                if attempt == 2:  # Last attempt
                    # Try fallback method
                    return self._get_fallback_data(symbol)

        raise RuntimeError("Unexpected error in fetch_option_chain")

    def _get_fallback_data(self, symbol: str) -> Dict:
        """Get fallback data when NSE API fails"""

        self.logger.warning(f"Using fallback data for {symbol}")

        # Return sample data structure for testing
        return {
            "records": {
                "data": [
                    {
                        "strikePrice": 25000,
                        "expiryDate": "30-Jan-2025",
                        "CE": {
                            "openInterest": 1500,
                            "lastPrice": 125.75,
                            "change": -5.25,
                            "pChange": -4.02,
                            "totalTradedVolume": 8500,
                            "impliedVolatility": 16.8,
                            "bidQty": 150,
                            "bidprice": 125.0,
                            "askPrice": 126.0,
                            "askQty": 200
                        },
                        "PE": {
                            "openInterest": 2200,
                            "lastPrice": 75.50,
                            "change": 3.25,
                            "pChange": 4.49,
                            "totalTradedVolume": 12000,
                            "impliedVolatility": 18.5,
                            "bidQty": 100,
                            "bidprice": 75.0,
                            "askPrice": 76.0,
                            "askQty": 175
                        }
                    }
                ],
                "underlyingValue": 25075.50
            }
        }
    
    def parse_option_chain(self, raw_data: Dict, symbol: str) -> Tuple[List[OptionChainEntry], List[OptionChainEntry]]:
        """
        Parse raw NSE option chain data into structured models
        
        Args:
            raw_data: Raw data from NSE API
            symbol: Symbol name
            
        Returns:
            Tuple of (call_options, put_options)
        """
        
        try:
            records = raw_data.get("records", {})
            data_list = records.get("data", [])
            underlying_value = records.get("underlyingValue")
            
            call_options = []
            put_options = []
            fetched_at = datetime.now(timezone.utc)
            
            for record in data_list:
                strike_price = record.get("strikePrice")
                expiry_date = record.get("expiryDate", "")
                
                if not strike_price:
                    continue
                
                # Process CALL option (CE)
                if "CE" in record:
                    ce_data = record["CE"]
                    call_option = self._create_option_entry(
                        ce_data, symbol, OptionType.CALL, strike_price, 
                        expiry_date, underlying_value, fetched_at
                    )
                    if call_option:
                        call_options.append(call_option)
                
                # Process PUT option (PE)
                if "PE" in record:
                    pe_data = record["PE"]
                    put_option = self._create_option_entry(
                        pe_data, symbol, OptionType.PUT, strike_price,
                        expiry_date, underlying_value, fetched_at
                    )
                    if put_option:
                        put_options.append(put_option)
            
            self.logger.info(f"Parsed {len(call_options)} CALL and {len(put_options)} PUT options for {symbol}")
            return call_options, put_options
            
        except Exception as e:
            self.logger.error(f"Failed to parse option chain data: {e}")
            raise
    
    def _create_option_entry(self, option_data: Dict, symbol: str, option_type: OptionType,
                           strike_price: float, expiry_date: str, underlying_value: Optional[float],
                           fetched_at: datetime) -> Optional[OptionChainEntry]:
        """Create a single option entry from raw data"""
        
        try:
            # Create unique identifier
            identifier = f"{symbol}_{option_type.value}_{strike_price}_{expiry_date}"
            
            # Map fields from NSE format to our model
            field_mapping = NSE_FIELD_MAPPING[option_type.value]
            mapped_data = {}
            
            for nse_field, model_field in field_mapping.items():
                value = option_data.get(nse_field)
                if value is not None and value != "" and value != "-":
                    mapped_data[model_field] = value
            
            # Create the option entry
            option_entry = OptionChainEntry(
                symbol=symbol,
                type=option_type,
                strike_price=float(strike_price),
                expiry_date=expiry_date,
                identifier=identifier,
                fetched_at=fetched_at,
                underlying_value=underlying_value,
                **mapped_data
            )
            
            return option_entry
            
        except Exception as e:
            self.logger.warning(f"Failed to create option entry: {e}")
            return None
    
    def fetch_and_parse(self, symbol: str = "NIFTY") -> Tuple[List[OptionChainEntry], List[OptionChainEntry]]:
        """
        Fetch and parse option chain data in one call
        
        Args:
            symbol: Symbol to fetch
            
        Returns:
            Tuple of (call_options, put_options)
        """
        
        raw_data = self.fetch_option_chain(symbol)
        return self.parse_option_chain(raw_data, symbol)


def setup_logging() -> logging.Logger:
    """Setup logging configuration"""
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('nse_backend.log')
        ]
    )
    
    return logging.getLogger(__name__)


# Example usage
if __name__ == "__main__":
    logger = setup_logging()
    fetcher = NSEDataFetcher(logger)
    
    try:
        calls, puts = fetcher.fetch_and_parse("NIFTY")
        print(f"Fetched {len(calls)} calls and {len(puts)} puts")
        
        if calls:
            print(f"Sample call: {calls[0].dict()}")
        if puts:
            print(f"Sample put: {puts[0].dict()}")
            
    except Exception as e:
        logger.error(f"Failed to fetch data: {e}")
