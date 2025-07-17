#!/usr/bin/env python3
"""
Backend Validation Script
=========================

Simple validation script to test core backend functionality.
"""

import sys
import logging
from datetime import datetime, timezone

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def test_models():
    """Test data models"""
    print("\n" + "="*50)
    print("TESTING DATA MODELS")
    print("="*50)
    
    try:
        from models import OptionChainEntry, OptionType, NSE_FIELD_MAPPING
        
        # Test model creation
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
        
        print(f"✅ Created OptionChainEntry: {entry.symbol} {entry.type} {entry.strike_price}")
        print(f"✅ Field mapping loaded: {len(NSE_FIELD_MAPPING)} option types")
        
        return True
        
    except Exception as e:
        print(f"❌ Models test failed: {e}")
        return False


def test_nse_service():
    """Test NSE service"""
    print("\n" + "="*50)
    print("TESTING NSE SERVICE")
    print("="*50)
    
    try:
        from nse_service import NSEDataFetcher
        
        # Create fetcher
        fetcher = NSEDataFetcher(logger)
        print("✅ NSE fetcher created successfully")
        
        # Test session creation
        assert fetcher.session is not None
        print("✅ HTTP session created")
        
        # Test parsing with sample data
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
        
        calls, puts = fetcher.parse_option_chain(sample_data, "NIFTY")
        print(f"✅ Parsed sample data: {len(calls)} calls, {len(puts)} puts")
        
        if calls:
            print(f"   Sample call: {calls[0].symbol} {calls[0].strike_price}")
        if puts:
            print(f"   Sample put: {puts[0].symbol} {puts[0].strike_price}")
        
        return True
        
    except Exception as e:
        print(f"❌ NSE service test failed: {e}")
        return False


def test_database_service():
    """Test database service"""
    print("\n" + "="*50)
    print("TESTING DATABASE SERVICE")
    print("="*50)
    
    try:
        from database_service import DatabaseService
        from models import OptionChainEntry, OptionType
        
        # Create database service
        db_service = DatabaseService(
            connection_string="mongodb+srv://varaprasadyoyo:JXlWJPUxAJiVB7Gx@cluster0.eviy31f.mongodb.net/AlgoSaaS?retryWrites=true&w=majority&appName=Cluster0",
            database_name="test_validation",
            collection_name="test_options",
            logger=logger
        )
        
        print("✅ Database service created")
        
        # Test connection
        if db_service.health_check():
            print("✅ Database connection healthy")
        else:
            print("⚠️  Database connection issues")
            return False
        
        # Test data storage
        test_call = OptionChainEntry(
            symbol="NIFTY",
            type=OptionType.CALL,
            strike_price=25000,
            expiry_date="30-Jan-2025",
            identifier=f"TEST_NIFTY_CE_25000_{datetime.now().timestamp()}",
            fetched_at=datetime.now(timezone.utc),
            open_interest=1000,
            last_price=100.5
        )
        
        test_put = OptionChainEntry(
            symbol="NIFTY",
            type=OptionType.PUT,
            strike_price=25000,
            expiry_date="30-Jan-2025",
            identifier=f"TEST_NIFTY_PE_25000_{datetime.now().timestamp()}",
            fetched_at=datetime.now(timezone.utc),
            open_interest=2000,
            last_price=50.25
        )
        
        stats = db_service.store_option_data([test_call], [test_put])
        print(f"✅ Stored test data: {stats}")
        
        # Test retrieval
        calls, puts = db_service.get_latest_option_data("NIFTY", 5)
        print(f"✅ Retrieved data: {len(calls)} calls, {len(puts)} puts")
        
        # Cleanup test data
        db_service.collection.delete_many({"identifier": {"$regex": "^TEST_"}})
        db_service.close()
        
        return True
        
    except Exception as e:
        print(f"❌ Database service test failed: {e}")
        return False


def test_data_pipeline():
    """Test data pipeline"""
    print("\n" + "="*50)
    print("TESTING DATA PIPELINE")
    print("="*50)
    
    try:
        from data_pipeline import DataPipeline
        
        # Create pipeline
        pipeline = DataPipeline(
            db_connection_string="mongodb+srv://varaprasadyoyo:JXlWJPUxAJiVB7Gx@cluster0.eviy31f.mongodb.net/AlgoSaaS?retryWrites=true&w=majority&appName=Cluster0",
            symbols=["NIFTY"],
            fetch_interval=10,
            logger=logger
        )
        
        print("✅ Data pipeline created")
        
        # Test status
        status = pipeline.get_status()
        print(f"✅ Pipeline status: {status['running']}")
        
        # Test initialization
        pipeline.initialize()
        print("✅ Pipeline initialized")
        
        # Cleanup
        pipeline.cleanup()
        
        return True
        
    except Exception as e:
        print(f"❌ Data pipeline test failed: {e}")
        return False


def test_fastapi_app():
    """Test FastAPI application"""
    print("\n" + "="*50)
    print("TESTING FASTAPI APPLICATION")
    print("="*50)
    
    try:
        from nse_backend_app import app
        
        print("✅ FastAPI app imported successfully")
        
        # Test app configuration
        assert app.title == "NSE Option Chain Backend"
        print("✅ App configuration validated")
        
        return True
        
    except Exception as e:
        print(f"❌ FastAPI app test failed: {e}")
        return False


def run_validation():
    """Run all validation tests"""
    print("🚀 Starting NSE Backend Validation")
    print("=" * 60)
    
    tests = [
        ("Data Models", test_models),
        ("NSE Service", test_nse_service),
        ("Database Service", test_database_service),
        ("Data Pipeline", test_data_pipeline),
        ("FastAPI Application", test_fastapi_app)
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        try:
            print(f"\n🧪 Running {test_name} validation...")
            results[test_name] = test_func()
        except Exception as e:
            print(f"❌ {test_name} validation crashed: {e}")
            results[test_name] = False
    
    # Print summary
    print("\n" + "="*60)
    print("VALIDATION SUMMARY")
    print("="*60)
    
    passed = 0
    total = len(tests)
    
    for test_name, result in results.items():
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"{test_name:<25} {status}")
        if result:
            passed += 1
    
    print(f"\nOverall: {passed}/{total} validations passed")
    
    if passed == total:
        print("🎉 All validations passed! Backend is ready for use.")
        print("\nNext steps:")
        print("1. Run the backend: python nse_backend_app.py")
        print("2. Access API docs: http://localhost:8000/docs")
        print("3. Test endpoints: http://localhost:8000/health")
        return True
    else:
        print("⚠️  Some validations failed. Please fix issues before deployment.")
        return False


if __name__ == "__main__":
    success = run_validation()
    sys.exit(0 if success else 1)
