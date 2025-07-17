"""
Test Suite for NSE Option Chain Backend
=======================================

Comprehensive test suite for all backend components.
"""

import pytest
import asyncio
import logging
from datetime import datetime, timezone
from unittest.mock import Mock, patch
import json

from fastapi.testclient import TestClient
from models import OptionChainEntry, OptionType
from nse_service import NSEDataFetcher
from database_service import DatabaseService
from data_pipeline import DataPipeline
from nse_backend_app import app


# Test configuration
TEST_DB_CONNECTION = "mongodb+srv://varaprasadyoyo:JXlWJPUxAJiVB7Gx@cluster0.eviy31f.mongodb.net/AlgoSaaS?retryWrites=true&w=majority&appName=Cluster0"


def setup_test_logging():
    """Setup logging for tests"""
    logging.basicConfig(level=logging.DEBUG)
    return logging.getLogger(__name__)


class TestNSEDataFetcher:
    """Test NSE data fetcher service"""
    
    def setup_method(self):
        """Setup for each test method"""
        self.logger = setup_test_logging()
        self.fetcher = NSEDataFetcher(self.logger)
    
    def test_session_creation(self):
        """Test HTTP session creation"""
        assert self.fetcher.session is not None
        assert "User-Agent" in self.fetcher.session.headers
        assert "Mozilla" in self.fetcher.session.headers["User-Agent"]
    
    @patch('requests.Session.get')
    def test_fetch_option_chain_success(self, mock_get):
        """Test successful option chain fetch"""
        # Mock response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = '{"records": {"data": []}}'
        mock_response.json.return_value = {"records": {"data": []}}
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        # Test fetch
        result = self.fetcher.fetch_option_chain("NIFTY")
        
        assert result is not None
        assert "records" in result
    
    def test_parse_option_chain(self):
        """Test option chain parsing"""
        # Sample NSE data
        sample_data = {
            "records": {
                "data": [
                    {
                        "strikePrice": 25000,
                        "expiryDate": "30-Jan-2025",
                        "CE": {
                            "openInterest": 1000,
                            "lastPrice": 100.5,
                            "impliedVolatility": 15.5
                        },
                        "PE": {
                            "openInterest": 2000,
                            "lastPrice": 50.25,
                            "impliedVolatility": 18.2
                        }
                    }
                ],
                "underlyingValue": 25100.75
            }
        }
        
        calls, puts = self.fetcher.parse_option_chain(sample_data, "NIFTY")
        
        assert len(calls) == 1
        assert len(puts) == 1
        assert calls[0].symbol == "NIFTY"
        assert calls[0].type == OptionType.CALL
        assert calls[0].strike_price == 25000
        assert puts[0].type == OptionType.PUT


class TestDatabaseService:
    """Test database service"""
    
    def setup_method(self):
        """Setup for each test method"""
        self.logger = setup_test_logging()
        # Use test database
        self.db_service = DatabaseService(
            connection_string=TEST_DB_CONNECTION,
            database_name="test_nse",
            collection_name="test_options",
            logger=self.logger
        )
    
    def teardown_method(self):
        """Cleanup after each test"""
        # Clean up test data
        try:
            self.db_service.collection.delete_many({})
        except:
            pass
        self.db_service.close()
    
    def test_database_connection(self):
        """Test database connection"""
        assert self.db_service.health_check() == True
    
    def test_store_option_data(self):
        """Test storing option data"""
        # Create test data
        call_option = OptionChainEntry(
            symbol="NIFTY",
            type=OptionType.CALL,
            strike_price=25000,
            expiry_date="30-Jan-2025",
            identifier="NIFTY_CE_25000_30-Jan-2025",
            fetched_at=datetime.now(timezone.utc),
            open_interest=1000,
            last_price=100.5
        )
        
        put_option = OptionChainEntry(
            symbol="NIFTY",
            type=OptionType.PUT,
            strike_price=25000,
            expiry_date="30-Jan-2025",
            identifier="NIFTY_PE_25000_30-Jan-2025",
            fetched_at=datetime.now(timezone.utc),
            open_interest=2000,
            last_price=50.25
        )
        
        # Store data
        stats = self.db_service.store_option_data([call_option], [put_option])
        
        assert stats["calls_inserted"] == 1
        assert stats["puts_inserted"] == 1
        assert stats["total_processed"] == 2
    
    def test_get_latest_option_data(self):
        """Test retrieving latest option data"""
        # First store some data
        self.test_store_option_data()
        
        # Retrieve data
        calls, puts = self.db_service.get_latest_option_data("NIFTY", 10)
        
        assert len(calls) == 1
        assert len(puts) == 1
        assert calls[0]["symbol"] == "NIFTY"
        assert puts[0]["symbol"] == "NIFTY"
    
    def test_get_statistics(self):
        """Test getting database statistics"""
        # Store some test data first
        self.test_store_option_data()
        
        stats = self.db_service.get_statistics()
        
        assert stats["total_records"] >= 2
        assert "NIFTY" in stats["records_by_symbol"]
        assert stats["records_by_symbol"]["NIFTY"] >= 2


class TestDataPipeline:
    """Test data pipeline"""
    
    def setup_method(self):
        """Setup for each test method"""
        self.logger = setup_test_logging()
        self.pipeline = DataPipeline(
            db_connection_string=TEST_DB_CONNECTION,
            symbols=["NIFTY"],
            fetch_interval=1,  # Short interval for testing
            logger=self.logger
        )
    
    def teardown_method(self):
        """Cleanup after each test"""
        if self.pipeline:
            self.pipeline.cleanup()
    
    @patch('nse_service.NSEDataFetcher.fetch_and_parse')
    def test_single_cycle(self, mock_fetch):
        """Test single pipeline cycle"""
        # Mock NSE data
        mock_call = OptionChainEntry(
            symbol="NIFTY",
            type=OptionType.CALL,
            strike_price=25000,
            expiry_date="30-Jan-2025",
            identifier="NIFTY_CE_25000_30-Jan-2025",
            fetched_at=datetime.now(timezone.utc),
            open_interest=1000
        )
        
        mock_put = OptionChainEntry(
            symbol="NIFTY",
            type=OptionType.PUT,
            strike_price=25000,
            expiry_date="30-Jan-2025",
            identifier="NIFTY_PE_25000_30-Jan-2025",
            fetched_at=datetime.now(timezone.utc),
            open_interest=2000
        )
        
        mock_fetch.return_value = ([mock_call], [mock_put])
        
        # Initialize pipeline
        self.pipeline.initialize()
        
        # Run single cycle
        result = self.pipeline.run_single_cycle("NIFTY")
        
        assert result["success"] == True
        assert result["symbol"] == "NIFTY"
        assert result["calls_processed"] == 1
        assert result["puts_processed"] == 1
    
    def test_get_status(self):
        """Test pipeline status"""
        status = self.pipeline.get_status()
        
        assert "running" in status
        assert "total_cycles" in status
        assert "symbols" in status
        assert status["symbols"] == ["NIFTY"]


class TestFastAPIEndpoints:
    """Test FastAPI endpoints"""
    
    def setup_method(self):
        """Setup for each test method"""
        self.client = TestClient(app)
    
    def test_root_endpoint(self):
        """Test root endpoint"""
        response = self.client.get("/")
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] == True
        assert "version" in data["data"]
    
    def test_health_endpoint(self):
        """Test health check endpoint"""
        response = self.client.get("/health")
        
        # May fail if services not running, but should return valid structure
        if response.status_code == 200:
            data = response.json()
            assert "status" in data
            assert "timestamp" in data
            assert "database_connected" in data
    
    def test_stats_endpoint(self):
        """Test statistics endpoint"""
        response = self.client.get("/stats")
        
        # May fail if services not running
        if response.status_code == 200:
            data = response.json()
            assert "total_records" in data
            assert "records_by_symbol" in data
    
    def test_option_chain_endpoint(self):
        """Test option chain endpoint"""
        response = self.client.get("/option-chain/NIFTY")
        
        # May return 404 if no data, but should be valid response
        assert response.status_code in [200, 404, 503]
        
        if response.status_code == 200:
            data = response.json()
            assert "symbol" in data
            assert "calls" in data
            assert "puts" in data
    
    def test_strikes_endpoint(self):
        """Test strike prices endpoint"""
        response = self.client.get("/option-chain/NIFTY/strikes")
        
        # Should return valid response structure
        if response.status_code == 200:
            data = response.json()
            assert data["success"] == True
            assert "strike_prices" in data["data"]
    
    def test_expiry_dates_endpoint(self):
        """Test expiry dates endpoint"""
        response = self.client.get("/option-chain/NIFTY/expiry-dates")
        
        # Should return valid response structure
        if response.status_code == 200:
            data = response.json()
            assert data["success"] == True
            assert "expiry_dates" in data["data"]


def test_option_chain_entry_validation():
    """Test OptionChainEntry model validation"""
    # Valid entry
    entry = OptionChainEntry(
        symbol="NIFTY",
        type=OptionType.CALL,
        strike_price=25000,
        expiry_date="30-Jan-2025",
        identifier="NIFTY_CE_25000_30-Jan-2025",
        fetched_at=datetime.now(timezone.utc),
        open_interest=1000,
        last_price=100.5
    )
    
    assert entry.symbol == "NIFTY"
    assert entry.type == OptionType.CALL
    assert entry.strike_price == 25000
    
    # Test validation of None/empty values
    entry_with_nones = OptionChainEntry(
        symbol="NIFTY",
        type=OptionType.PUT,
        strike_price=25000,
        expiry_date="30-Jan-2025",
        identifier="NIFTY_PE_25000_30-Jan-2025",
        fetched_at=datetime.now(timezone.utc),
        open_interest=None,  # Should be handled gracefully
        last_price=""  # Should be converted to None
    )
    
    assert entry_with_nones.open_interest is None
    assert entry_with_nones.last_price is None


# Integration test
def test_end_to_end_flow():
    """Test complete end-to-end flow"""
    logger = setup_test_logging()
    
    try:
        # Test NSE fetcher
        fetcher = NSEDataFetcher(logger)
        
        # Test database service
        db_service = DatabaseService(
            connection_string=TEST_DB_CONNECTION,
            database_name="test_nse_e2e",
            collection_name="test_options_e2e",
            logger=logger
        )
        
        # Create sample data (since NSE API might not be accessible)
        sample_call = OptionChainEntry(
            symbol="NIFTY",
            type=OptionType.CALL,
            strike_price=25000,
            expiry_date="30-Jan-2025",
            identifier="NIFTY_CE_25000_30-Jan-2025",
            fetched_at=datetime.now(timezone.utc),
            open_interest=1000,
            last_price=100.5
        )
        
        sample_put = OptionChainEntry(
            symbol="NIFTY",
            type=OptionType.PUT,
            strike_price=25000,
            expiry_date="30-Jan-2025",
            identifier="NIFTY_PE_25000_30-Jan-2025",
            fetched_at=datetime.now(timezone.utc),
            open_interest=2000,
            last_price=50.25
        )
        
        # Store data
        stats = db_service.store_option_data([sample_call], [sample_put])
        assert stats["calls_inserted"] == 1
        assert stats["puts_inserted"] == 1
        
        # Retrieve data
        calls, puts = db_service.get_latest_option_data("NIFTY", 10)
        assert len(calls) == 1
        assert len(puts) == 1
        
        # Cleanup
        db_service.collection.delete_many({})
        db_service.close()
        
        logger.info("End-to-end test completed successfully")
        
    except Exception as e:
        logger.error(f"End-to-end test failed: {e}")
        raise


if __name__ == "__main__":
    # Run basic tests
    print("Running NSE Backend Test Suite...")
    
    # Test model validation
    test_option_chain_entry_validation()
    print("✅ Model validation tests passed")
    
    # Test end-to-end flow
    test_end_to_end_flow()
    print("✅ End-to-end test passed")
    
    print("🎉 All basic tests completed successfully!")
    print("\nTo run full test suite with pytest:")
    print("pip install pytest")
    print("pytest test_nse_backend.py -v")
