"""
NSE Backend Configuration
========================

Configuration settings and utilities for handling NSE API issues.
"""

import logging
import sys
from dataclasses import dataclass
from typing import List, Dict


@dataclass
class NSEConfig:
    """Configuration for NSE backend system"""
    
    # Database settings
    MONGO_URI: str = "mongodb+srv://varaprasadyoyo:JXlWJPUxAJiVB7Gx@cluster0.eviy31f.mongodb.net/AlgoSaaS?retryWrites=true&w=majority&appName=Cluster0"
    DATABASE_NAME: str = "AlgoSaaS"
    COLLECTION_NAME: str = "option_chain"
    
    # Pipeline settings
    FETCH_INTERVAL: int = 10  # Increased from 5 to reduce API pressure
    SYMBOLS: List[str] = None
    MAX_RETRIES: int = 3
    REQUEST_TIMEOUT: int = 25  # Increased timeout
    
    # NSE API settings
    USE_FALLBACK_DATA: bool = True  # Enable fallback when API fails
    FALLBACK_INTERVAL: int = 30  # Use fallback data for 30 seconds after failure
    
    # Logging settings
    LOG_LEVEL: str = "INFO"
    LOG_FORMAT: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    ENABLE_FILE_LOGGING: bool = True
    LOG_FILE: str = "nse_backend.log"
    
    def __post_init__(self):
        if self.SYMBOLS is None:
            self.SYMBOLS = ["NIFTY"]


def setup_logging(config: NSEConfig) -> logging.Logger:
    """Setup logging with proper encoding to handle all characters"""
    
    # Configure logging with UTF-8 encoding
    handlers = []
    
    # Console handler with UTF-8 encoding
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, config.LOG_LEVEL))
    console_formatter = logging.Formatter(config.LOG_FORMAT)
    console_handler.setFormatter(console_formatter)
    handlers.append(console_handler)
    
    # File handler with UTF-8 encoding
    if config.ENABLE_FILE_LOGGING:
        try:
            file_handler = logging.FileHandler(
                config.LOG_FILE, 
                mode='a', 
                encoding='utf-8'  # Explicit UTF-8 encoding
            )
            file_handler.setLevel(getattr(logging, config.LOG_LEVEL))
            file_formatter = logging.Formatter(config.LOG_FORMAT)
            file_handler.setFormatter(file_formatter)
            handlers.append(file_handler)
        except Exception as e:
            print(f"Warning: Could not setup file logging: {e}")
    
    # Configure root logger
    logging.basicConfig(
        level=getattr(logging, config.LOG_LEVEL),
        handlers=handlers,
        force=True  # Override existing configuration
    )
    
    logger = logging.getLogger('nse_backend')
    logger.info("=" * 60)
    logger.info("NSE Option Chain Backend - Starting Up")
    logger.info("=" * 60)
    
    return logger


def get_fallback_option_data(symbol: str = "NIFTY") -> Dict:
    """Get fallback option chain data when NSE API is unavailable"""
    
    import random
    from datetime import datetime, timedelta
    
    # Generate realistic sample data
    base_price = 25000 if symbol == "NIFTY" else 50000
    strikes = [base_price + (i * 50) for i in range(-10, 11)]
    
    data_entries = []
    
    for strike in strikes:
        # Generate realistic option data
        ce_oi = random.randint(500, 5000)
        pe_oi = random.randint(500, 5000)
        
        ce_ltp = max(1, random.uniform(10, 200) * (base_price / strike))
        pe_ltp = max(1, random.uniform(10, 200) * (strike / base_price))
        
        entry = {
            "strikePrice": strike,
            "expiryDate": (datetime.now() + timedelta(days=7)).strftime("%d-%b-%Y"),
            "CE": {
                "openInterest": ce_oi,
                "changeinOpenInterest": random.randint(-100, 100),
                "pchangeinOpenInterest": random.uniform(-5, 5),
                "lastPrice": round(ce_ltp, 2),
                "change": random.uniform(-10, 10),
                "pChange": random.uniform(-5, 5),
                "totalTradedVolume": random.randint(1000, 10000),
                "impliedVolatility": random.uniform(10, 25),
                "bidQty": random.randint(50, 500),
                "bidprice": round(ce_ltp - 0.5, 2),
                "askPrice": round(ce_ltp + 0.5, 2),
                "askQty": random.randint(50, 500)
            },
            "PE": {
                "openInterest": pe_oi,
                "changeinOpenInterest": random.randint(-100, 100),
                "pchangeinOpenInterest": random.uniform(-5, 5),
                "lastPrice": round(pe_ltp, 2),
                "change": random.uniform(-10, 10),
                "pChange": random.uniform(-5, 5),
                "totalTradedVolume": random.randint(1000, 10000),
                "impliedVolatility": random.uniform(10, 25),
                "bidQty": random.randint(50, 500),
                "bidprice": round(pe_ltp - 0.5, 2),
                "askPrice": round(pe_ltp + 0.5, 2),
                "askQty": random.randint(50, 500)
            }
        }
        data_entries.append(entry)
    
    return {
        "records": {
            "data": data_entries,
            "underlyingValue": base_price + random.uniform(-100, 100)
        },
        "filtered": {
            "data": data_entries
        }
    }


def check_nse_api_health() -> bool:
    """Check if NSE API is accessible"""
    
    import requests
    
    try:
        response = requests.get(
            "https://www.nseindia.com",
            timeout=10,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }
        )
        return response.status_code == 200
    except:
        return False


def get_recommended_settings() -> Dict:
    """Get recommended settings based on current conditions"""
    
    api_healthy = check_nse_api_health()
    
    if api_healthy:
        return {
            "fetch_interval": 5,
            "use_fallback": False,
            "max_retries": 3,
            "timeout": 15
        }
    else:
        return {
            "fetch_interval": 15,  # Slower when API is having issues
            "use_fallback": True,
            "max_retries": 2,
            "timeout": 25
        }


# Default configuration instance
DEFAULT_CONFIG = NSEConfig()


if __name__ == "__main__":
    # Test configuration
    config = NSEConfig()
    logger = setup_logging(config)
    
    logger.info("Configuration test successful")
    logger.info(f"Database: {config.DATABASE_NAME}")
    logger.info(f"Symbols: {config.SYMBOLS}")
    logger.info(f"Fetch interval: {config.FETCH_INTERVAL}s")
    
    # Test NSE API health
    api_health = check_nse_api_health()
    logger.info(f"NSE API Health: {'OK' if api_health else 'Issues detected'}")
    
    # Get recommendations
    recommendations = get_recommended_settings()
    logger.info(f"Recommended settings: {recommendations}")
    
    print("Configuration test completed successfully!")
