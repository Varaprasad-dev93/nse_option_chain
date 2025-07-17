#!/usr/bin/env python3
"""
Test Script for NSE Option Chain Pipeline
=========================================

This script tests the production-grade NSE Option Chain pipeline to ensure
all components work correctly before running in production.

Usage:
    python test_pipeline.py
"""

import sys
import time
import logging
from datetime import datetime, timezone
from nse_option_chain_pipeline import (
    PipelineConfig, 
    NSEOptionChainPipeline,
    NSEHttpClient,
    OptionChainParser,
    MongoDBStorage,
    setup_logging
)


def test_http_client():
    """Test NSE HTTP client functionality"""
    print("\n" + "="*50)
    print("TESTING HTTP CLIENT")
    print("="*50)
    
    config = PipelineConfig()
    logger = setup_logging(config)
    
    try:
        client = NSEHttpClient(config, logger)
        
        # Test fetching CSV data
        print("Fetching NIFTY option chain CSV...")
        csv_data = client.fetch_option_chain_csv("NIFTY")
        
        if csv_data and len(csv_data) > 100:
            print(f"✅ Successfully fetched CSV data ({len(csv_data)} characters)")
            print(f"First 200 characters: {csv_data[:200]}...")
            return True
        else:
            print("❌ Failed to fetch valid CSV data")
            return False
            
    except Exception as e:
        print(f"❌ HTTP Client test failed: {e}")
        return False


def test_csv_parser():
    """Test CSV parser functionality"""
    print("\n" + "="*50)
    print("TESTING CSV PARSER")
    print("="*50)
    
    # Sample CSV data for testing
    sample_csv = """CALLS,,PUTS
,OI,CHNG IN OI,VOLUME,IV,LTP,CHNG,BID QTY,BID,ASK,ASK QTY,STRIKE,BID QTY,BID,ASK,ASK QTY,CHNG,LTP,IV,VOLUME,CHNG IN OI,OI,
,76,-,-,-,-,-,750,"2,490.75","2,566.70","1,125","22,950.00","5,88,225",0.60,0.65,"3,73,125",-0.10,0.65,49.70,"68,667","4,452","46,139",
,395,-27,61,77.08,"2,492.10",34.10,75,"2,471.45","2,479.50",225,"23,000.00","1,06,950",0.65,0.70,"2,63,850",-0.20,0.65,49.13,"67,390","-6,478","24,472",
,"7,129",45,"68,429",13.24,290.45,-9.85,150,290.55,291.00,300,"25,200.00","7,800",13.65,13.70,"5,475",-19.65,13.65,12.42,"6,17,406","9,108","70,405","""
    
    logger = logging.getLogger('Test')
    parser = OptionChainParser(logger)
    
    try:
        call_docs, put_docs = parser.parse_csv_to_documents(sample_csv, "NIFTY")
        
        print(f"✅ Parsed {len(call_docs)} CALL documents")
        print(f"✅ Parsed {len(put_docs)} PUT documents")
        
        if call_docs:
            print(f"Sample CALL document: {call_docs[0]}")
        
        if put_docs:
            print(f"Sample PUT document: {put_docs[0]}")
        
        return len(call_docs) > 0 and len(put_docs) > 0
        
    except Exception as e:
        print(f"❌ CSV Parser test failed: {e}")
        return False


def test_mongodb_connection():
    """Test MongoDB connection and operations"""
    print("\n" + "="*50)
    print("TESTING MONGODB CONNECTION")
    print("="*50)
    
    config = PipelineConfig()
    logger = setup_logging(config)
    
    try:
        storage = MongoDBStorage(config, logger)
        
        # Test connection
        print("✅ Successfully connected to MongoDB Atlas")
        
        # Test data count functionality
        counts = storage.get_latest_data_count("NIFTY")
        print(f"✅ Current NIFTY data count: {counts}")
        
        storage.close()
        print("✅ MongoDB connection closed successfully")
        
        return True
        
    except Exception as e:
        print(f"❌ MongoDB test failed: {e}")
        return False


def test_single_pipeline_cycle():
    """Test a single pipeline cycle"""
    print("\n" + "="*50)
    print("TESTING SINGLE PIPELINE CYCLE")
    print("="*50)
    
    try:
        config = PipelineConfig()
        pipeline = NSEOptionChainPipeline(config)
        
        # Initialize pipeline
        pipeline.initialize()
        print("✅ Pipeline initialized successfully")
        
        # Run single cycle
        print("Running single data cycle...")
        stats = pipeline.run_single_cycle("NIFTY")
        
        print(f"✅ Single cycle completed successfully")
        print(f"Stats: {stats}")
        
        # Cleanup
        pipeline.cleanup()
        print("✅ Pipeline cleanup completed")
        
        return True
        
    except Exception as e:
        print(f"❌ Single cycle test failed: {e}")
        return False


def test_data_validation():
    """Test data validation in MongoDB"""
    print("\n" + "="*50)
    print("TESTING DATA VALIDATION")
    print("="*50)
    
    config = PipelineConfig()
    logger = setup_logging(config)
    
    try:
        storage = MongoDBStorage(config, logger)
        
        # Get latest data
        latest_doc = storage.collection.find_one(
            {"symbol": "NIFTY"},
            sort=[("fetched_at", -1)]
        )
        
        if latest_doc:
            print("✅ Found latest data in MongoDB")
            print(f"Latest document timestamp: {latest_doc.get('fetched_at')}")
            print(f"Document keys: {list(latest_doc.keys())}")
            
            # Validate required fields
            required_fields = ["symbol", "type", "strike_price", "fetched_at"]
            missing_fields = [field for field in required_fields if field not in latest_doc]
            
            if not missing_fields:
                print("✅ All required fields present")
            else:
                print(f"❌ Missing required fields: {missing_fields}")
                return False
        else:
            print("⚠️  No data found in MongoDB - run pipeline first")
            return False
        
        storage.close()
        return True
        
    except Exception as e:
        print(f"❌ Data validation test failed: {e}")
        return False


def run_all_tests():
    """Run all tests and provide summary"""
    print("🚀 Starting NSE Option Chain Pipeline Tests")
    print("=" * 60)
    
    tests = [
        ("HTTP Client", test_http_client),
        ("CSV Parser", test_csv_parser),
        ("MongoDB Connection", test_mongodb_connection),
        ("Single Pipeline Cycle", test_single_pipeline_cycle),
        ("Data Validation", test_data_validation)
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        try:
            print(f"\n🧪 Running {test_name} test...")
            results[test_name] = test_func()
        except Exception as e:
            print(f"❌ {test_name} test crashed: {e}")
            results[test_name] = False
    
    # Print summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    
    passed = 0
    total = len(tests)
    
    for test_name, result in results.items():
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"{test_name:<25} {status}")
        if result:
            passed += 1
    
    print(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! Pipeline is ready for production.")
        return True
    else:
        print("⚠️  Some tests failed. Please fix issues before production deployment.")
        return False


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
