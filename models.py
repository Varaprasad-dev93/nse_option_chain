"""
Data Models for NSE Option Chain Backend
========================================

Pydantic models for option chain data validation and serialization.
"""

from datetime import datetime
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field, validator
from enum import Enum


class OptionType(str, Enum):
    """Option type enumeration"""
    CALL = "CE"
    PUT = "PE"


class OptionChainEntry(BaseModel):
    """
    Complete option chain entry model matching the required schema
    """
    symbol: str = Field(..., description="Underlying symbol (e.g., NIFTY)")
    type: OptionType = Field(..., description="Option type: CE (Call) or PE (Put)")
    strike_price: float = Field(..., description="Strike price of the option")
    expiry_date: str = Field(..., description="Expiry date in DD-MMM-YYYY format")
    identifier: str = Field(..., description="Unique identifier for the option")
    fetched_at: datetime = Field(..., description="Timestamp when data was fetched")
    
    # Market data fields
    open_interest: Optional[float] = Field(None, description="Open Interest")
    change_in_oi: Optional[float] = Field(None, description="Change in Open Interest")
    pchange_in_oi: Optional[float] = Field(None, description="Percentage change in Open Interest")
    last_price: Optional[float] = Field(None, description="Last traded price")
    change: Optional[float] = Field(None, description="Change in price")
    pchange: Optional[float] = Field(None, description="Percentage change in price")
    total_traded_volume: Optional[float] = Field(None, description="Total traded volume")
    implied_volatility: Optional[float] = Field(None, description="Implied Volatility")
    
    # Bid/Ask data
    bid_qty: Optional[float] = Field(None, description="Bid quantity")
    bid_price: Optional[float] = Field(None, description="Bid price")
    ask_price: Optional[float] = Field(None, description="Ask price")
    ask_qty: Optional[float] = Field(None, description="Ask quantity")
    
    # Underlying data
    underlying_value: Optional[float] = Field(None, description="Underlying asset value")

    @validator('strike_price', 'last_price', 'bid_price', 'ask_price', pre=True)
    def validate_prices(cls, v):
        """Validate and convert price fields"""
        if v is None or v == "" or v == "-":
            return None
        try:
            return float(v)
        except (ValueError, TypeError):
            return None

    @validator('open_interest', 'change_in_oi', 'total_traded_volume', 'bid_qty', 'ask_qty', pre=True)
    def validate_quantities(cls, v):
        """Validate and convert quantity fields"""
        if v is None or v == "" or v == "-":
            return None
        try:
            return float(v)
        except (ValueError, TypeError):
            return None

    @validator('pchange_in_oi', 'change', 'pchange', 'implied_volatility', pre=True)
    def validate_percentages(cls, v):
        """Validate and convert percentage fields"""
        if v is None or v == "" or v == "-":
            return None
        try:
            return float(v)
        except (ValueError, TypeError):
            return None

    class Config:
        """Pydantic configuration"""
        use_enum_values = True
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class OptionChainResponse(BaseModel):
    """Response model for option chain API"""
    symbol: str
    underlying_value: Optional[float]
    timestamp: datetime
    total_calls: int
    total_puts: int
    calls: List[OptionChainEntry]
    puts: List[OptionChainEntry]


class OptionChainFilter(BaseModel):
    """Filter model for option chain queries"""
    symbol: Optional[str] = None
    option_type: Optional[OptionType] = None
    strike_price_min: Optional[float] = None
    strike_price_max: Optional[float] = None
    expiry_date: Optional[str] = None
    limit: int = Field(default=100, le=1000, description="Maximum number of records to return")
    offset: int = Field(default=0, ge=0, description="Number of records to skip")


class NSEOptionChainRaw(BaseModel):
    """Model for raw NSE API response"""
    records: Dict[str, Any]
    filtered: Dict[str, Any]


class APIResponse(BaseModel):
    """Standard API response wrapper"""
    success: bool
    message: str
    data: Optional[Any] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class HealthCheck(BaseModel):
    """Health check response model"""
    status: str
    timestamp: datetime
    database_connected: bool
    last_data_fetch: Optional[datetime] = None
    total_records: int = 0


class StatsResponse(BaseModel):
    """Statistics response model"""
    total_records: int
    records_by_symbol: Dict[str, int]
    records_by_type: Dict[str, int]
    latest_fetch_time: Optional[datetime]
    data_freshness_seconds: Optional[float]


# Field mapping from NSE API to our model
NSE_FIELD_MAPPING = {
    # Call option fields (CE)
    "CE": {
        "openInterest": "open_interest",
        "changeinOpenInterest": "change_in_oi", 
        "pchangeinOpenInterest": "pchange_in_oi",
        "lastPrice": "last_price",
        "change": "change",
        "pChange": "pchange",
        "totalTradedVolume": "total_traded_volume",
        "impliedVolatility": "implied_volatility",
        "bidQty": "bid_qty",
        "bidprice": "bid_price",
        "askPrice": "ask_price",
        "askQty": "ask_qty"
    },
    # Put option fields (PE)
    "PE": {
        "openInterest": "open_interest",
        "changeinOpenInterest": "change_in_oi",
        "pchangeinOpenInterest": "pchange_in_oi", 
        "lastPrice": "last_price",
        "change": "change",
        "pChange": "pchange",
        "totalTradedVolume": "total_traded_volume",
        "impliedVolatility": "implied_volatility",
        "bidQty": "bid_qty",
        "bidprice": "bid_price",
        "askPrice": "ask_price",
        "askQty": "ask_qty"
    }
}
