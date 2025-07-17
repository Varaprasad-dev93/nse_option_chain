"""
NSE Option Chain Backend API
============================

FastAPI application for serving NSE option chain data with background data pipeline.
"""

import logging
from datetime import datetime, timezone
from typing import List, Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from models import (
    OptionChainResponse, OptionChainFilter, APIResponse, 
    HealthCheck, StatsResponse, OptionType
)
from database_service import DatabaseService
from data_pipeline import PipelineManager


# Global variables
pipeline_manager: Optional[PipelineManager] = None
db_service: Optional[DatabaseService] = None
logger: logging.Logger = None

# Import configuration
from nse_config import NSEConfig, setup_logging, get_recommended_settings

# Configuration
config = NSEConfig()
# Apply recommended settings based on current conditions
recommended = get_recommended_settings()
config.FETCH_INTERVAL = recommended.get('fetch_interval', config.FETCH_INTERVAL)

DB_CONNECTION_STRING = config.MONGO_URI
SYMBOLS_TO_TRACK = config.SYMBOLS
FETCH_INTERVAL = config.FETCH_INTERVAL


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    
    global pipeline_manager, db_service, logger
    
    # Startup
    logger = setup_logging(config)
    logger.info("Starting NSE Option Chain Backend...")
    logger.info(f"Configuration: Fetch interval={FETCH_INTERVAL}s, Symbols={SYMBOLS_TO_TRACK}")
    
    try:
        # Initialize database service
        db_service = DatabaseService(
            connection_string=DB_CONNECTION_STRING,
            logger=logger
        )
        
        # Initialize and start pipeline manager
        pipeline_manager = PipelineManager(
            db_connection_string=DB_CONNECTION_STRING,
            symbols=SYMBOLS_TO_TRACK,
            fetch_interval=FETCH_INTERVAL,
            logger=logger
        )
        
        pipeline_manager.start()
        logger.info("NSE Option Chain Backend started successfully")
        
    except Exception as e:
        logger.error(f"Failed to start backend: {e}")
        raise
    
    yield
    
    # Shutdown
    logger.info("Shutting down NSE Option Chain Backend...")
    
    if pipeline_manager:
        pipeline_manager.stop()
    
    if db_service:
        db_service.close()
    
    logger.info("NSE Option Chain Backend shutdown complete")


# Create FastAPI app
app = FastAPI(
    title="NSE Option Chain Backend",
    description="Production-grade backend for NSE option chain data with real-time updates",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_db_service() -> DatabaseService:
    """Dependency to get database service"""
    if not db_service:
        raise HTTPException(status_code=503, detail="Database service not available")
    return db_service


def get_pipeline_manager() -> PipelineManager:
    """Dependency to get pipeline manager"""
    if not pipeline_manager:
        raise HTTPException(status_code=503, detail="Pipeline manager not available")
    return pipeline_manager


@app.get("/", response_model=APIResponse)
async def root():
    """Root endpoint"""
    return APIResponse(
        success=True,
        message="NSE Option Chain Backend API is running",
        data={
            "version": "1.0.0",
            "endpoints": [
                "/option-chain/{symbol}",
                "/option-chain/filter",
                "/health",
                "/stats",
                "/pipeline/status"
            ]
        }
    )


@app.get("/health", response_model=HealthCheck)
async def health_check(db: DatabaseService = Depends(get_db_service),
                      pipeline: PipelineManager = Depends(get_pipeline_manager)):
    """Health check endpoint"""
    
    try:
        # Check database connection
        db_healthy = db.health_check()
        
        # Get pipeline status
        pipeline_status = pipeline.get_status()
        
        # Get latest data info
        stats = db.get_statistics()
        
        return HealthCheck(
            status="healthy" if db_healthy and pipeline.is_running() else "unhealthy",
            timestamp=datetime.now(timezone.utc),
            database_connected=db_healthy,
            last_data_fetch=stats.get("latest_fetch_time"),
            total_records=stats.get("total_records", 0)
        )
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail=f"Health check failed: {str(e)}")


@app.get("/stats", response_model=StatsResponse)
async def get_statistics(db: DatabaseService = Depends(get_db_service)):
    """Get database statistics"""
    
    try:
        stats = db.get_statistics()
        
        return StatsResponse(
            total_records=stats["total_records"],
            records_by_symbol=stats["records_by_symbol"],
            records_by_type=stats["records_by_type"],
            latest_fetch_time=stats["latest_fetch_time"],
            data_freshness_seconds=stats["data_freshness_seconds"]
        )
        
    except Exception as e:
        logger.error(f"Failed to get statistics: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get statistics: {str(e)}")


@app.get("/pipeline/status")
async def get_pipeline_status(pipeline: PipelineManager = Depends(get_pipeline_manager)):
    """Get pipeline status"""
    
    try:
        status = pipeline.get_status()
        
        return APIResponse(
            success=True,
            message="Pipeline status retrieved successfully",
            data=status
        )
        
    except Exception as e:
        logger.error(f"Failed to get pipeline status: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get pipeline status: {str(e)}")


@app.get("/option-chain/{symbol}", response_model=OptionChainResponse)
async def get_option_chain(
    symbol: str,
    limit: int = Query(default=100, le=500, description="Maximum number of records per option type"),
    db: DatabaseService = Depends(get_db_service)
):
    """
    Get latest option chain data for a symbol
    
    Args:
        symbol: Symbol to fetch (e.g., NIFTY, BANKNIFTY)
        limit: Maximum number of records per option type
    """
    
    try:
        symbol = symbol.upper()
        
        # Get latest data
        calls, puts = db.get_latest_option_data(symbol, limit)
        
        if not calls and not puts:
            raise HTTPException(status_code=404, detail=f"No data found for symbol {symbol}")
        
        # Get underlying value from first available record
        underlying_value = None
        if calls:
            underlying_value = calls[0].get("underlying_value")
        elif puts:
            underlying_value = puts[0].get("underlying_value")
        
        # Get timestamp from first record
        timestamp = None
        if calls:
            timestamp = calls[0].get("fetched_at")
        elif puts:
            timestamp = puts[0].get("fetched_at")
        
        return OptionChainResponse(
            symbol=symbol,
            underlying_value=underlying_value,
            timestamp=timestamp or datetime.now(timezone.utc),
            total_calls=len(calls),
            total_puts=len(puts),
            calls=calls,
            puts=puts
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get option chain for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get option chain: {str(e)}")


@app.post("/option-chain/filter")
async def get_filtered_option_chain(
    filter_params: OptionChainFilter,
    db: DatabaseService = Depends(get_db_service)
):
    """
    Get filtered option chain data
    
    Args:
        filter_params: Filter parameters
    """
    
    try:
        # Get filtered data
        results = db.get_filtered_data(filter_params)
        
        return APIResponse(
            success=True,
            message=f"Retrieved {len(results)} filtered records",
            data={
                "total_records": len(results),
                "filter_params": filter_params.dict(),
                "records": results
            }
        )
        
    except Exception as e:
        logger.error(f"Failed to get filtered data: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get filtered data: {str(e)}")


@app.get("/option-chain/{symbol}/strikes")
async def get_strike_prices(
    symbol: str,
    option_type: Optional[OptionType] = Query(None, description="Filter by option type"),
    db: DatabaseService = Depends(get_db_service)
):
    """Get available strike prices for a symbol"""
    
    try:
        symbol = symbol.upper()
        
        # Build query
        query = {"symbol": symbol}
        if option_type:
            query["type"] = option_type.value
        
        # Get distinct strike prices
        strikes = db.collection.distinct("strike_price", query)
        strikes.sort()
        
        return APIResponse(
            success=True,
            message=f"Retrieved {len(strikes)} strike prices for {symbol}",
            data={
                "symbol": symbol,
                "option_type": option_type.value if option_type else "ALL",
                "strike_prices": strikes,
                "total_strikes": len(strikes)
            }
        )
        
    except Exception as e:
        logger.error(f"Failed to get strike prices for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get strike prices: {str(e)}")


@app.get("/option-chain/{symbol}/expiry-dates")
async def get_expiry_dates(
    symbol: str,
    db: DatabaseService = Depends(get_db_service)
):
    """Get available expiry dates for a symbol"""
    
    try:
        symbol = symbol.upper()
        
        # Get distinct expiry dates
        expiry_dates = db.collection.distinct("expiry_date", {"symbol": symbol})
        expiry_dates.sort()
        
        return APIResponse(
            success=True,
            message=f"Retrieved {len(expiry_dates)} expiry dates for {symbol}",
            data={
                "symbol": symbol,
                "expiry_dates": expiry_dates,
                "total_expiry_dates": len(expiry_dates)
            }
        )
        
    except Exception as e:
        logger.error(f"Failed to get expiry dates for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get expiry dates: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "nse_backend_app:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
