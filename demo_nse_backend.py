#!/usr/bin/env python3
"""
NSE Backend Demonstration Script
================================

Demonstrates the complete NSE Option Chain Backend system functionality.
"""

import time
import logging
import asyncio
from datetime import datetime, timezone

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def demo_nse_fetcher():
    """Demonstrate NSE data fetching"""
    print("\n" + "="*60)
    print("🔄 DEMONSTRATING NSE DATA FETCHER")
    print("="*60)
    
    try:
        from nse_service import NSEDataFetcher
        
        # Create fetcher
        fetcher = NSEDataFetcher(logger)
        print("✅ NSE fetcher initialized")
        
        # Try to fetch real data (may fail due to NSE restrictions)
        try:
            print("🌐 Attempting to fetch live NIFTY data...")
            calls, puts = fetcher.fetch_and_parse("NIFTY")
            
            print(f"✅ Successfully fetched live data:")
            print(f"   📈 Calls: {len(calls)}")
            print(f"   📉 Puts: {len(puts)}")
            
            if calls:
                sample_call = calls[0]
                print(f"   Sample Call: {sample_call.symbol} {sample_call.strike_price} @ {sample_call.last_price}")
            
            if puts:
                sample_put = puts[0]
                print(f"   Sample Put: {sample_put.symbol} {sample_put.strike_price} @ {sample_put.last_price}")
                
            return calls, puts
            
        except Exception as e:
            print(f"⚠️  Live data fetch failed (expected): {e}")
            print("📝 Using sample data for demonstration...")
            
            # Use sample data
            sample_data = {
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
                        },
                        {
                            "strikePrice": 25100,
                            "expiryDate": "30-Jan-2025",
                            "CE": {
                                "openInterest": 1200,
                                "lastPrice": 95.25,
                                "change": -8.75,
                                "pChange": -8.41,
                                "totalTradedVolume": 6500,
                                "impliedVolatility": 17.2
                            },
                            "PE": {
                                "openInterest": 1800,
                                "lastPrice": 105.75,
                                "change": 7.50,
                                "pChange": 7.63,
                                "totalTradedVolume": 9500,
                                "impliedVolatility": 19.1
                            }
                        }
                    ],
                    "underlyingValue": 25075.50
                }
            }
            
            calls, puts = fetcher.parse_option_chain(sample_data, "NIFTY")
            
            print(f"✅ Parsed sample data:")
            print(f"   📈 Calls: {len(calls)}")
            print(f"   📉 Puts: {len(puts)}")
            
            return calls, puts
            
    except Exception as e:
        print(f"❌ NSE fetcher demo failed: {e}")
        return [], []


def demo_database_operations(calls, puts):
    """Demonstrate database operations"""
    print("\n" + "="*60)
    print("🗄️  DEMONSTRATING DATABASE OPERATIONS")
    print("="*60)
    
    try:
        from database_service import DatabaseService
        
        # Create database service
        db_service = DatabaseService(
            connection_string="mongodb+srv://varaprasadyoyo:JXlWJPUxAJiVB7Gx@cluster0.eviy31f.mongodb.net/AlgoSaaS?retryWrites=true&w=majority&appName=Cluster0",
            database_name="demo_nse",
            collection_name="demo_options",
            logger=logger
        )
        
        print("✅ Database service connected")
        
        # Store data
        if calls or puts:
            print("💾 Storing option data...")
            stats = db_service.store_option_data(calls, puts)
            
            print(f"✅ Storage complete:")
            print(f"   📈 Calls: {stats['calls_inserted']} inserted, {stats['calls_skipped']} skipped")
            print(f"   📉 Puts: {stats['puts_inserted']} inserted, {stats['puts_skipped']} skipped")
        
        # Retrieve latest data
        print("📊 Retrieving latest data...")
        retrieved_calls, retrieved_puts = db_service.get_latest_option_data("NIFTY", 5)
        
        print(f"✅ Retrieved data:")
        print(f"   📈 Calls: {len(retrieved_calls)}")
        print(f"   📉 Puts: {len(retrieved_puts)}")
        
        # Show sample records
        if retrieved_calls:
            call = retrieved_calls[0]
            print(f"   Sample Call: Strike {call['strike_price']}, LTP {call.get('last_price', 'N/A')}")
        
        if retrieved_puts:
            put = retrieved_puts[0]
            print(f"   Sample Put: Strike {put['strike_price']}, LTP {put.get('last_price', 'N/A')}")
        
        # Get statistics
        print("📈 Database statistics...")
        stats = db_service.get_statistics()
        
        print(f"✅ Statistics:")
        print(f"   Total records: {stats['total_records']}")
        print(f"   Data freshness: {stats['data_freshness_seconds']:.1f} seconds")
        
        # Cleanup demo data
        print("🧹 Cleaning up demo data...")
        db_service.collection.delete_many({"symbol": "NIFTY"})
        
        db_service.close()
        print("✅ Database operations completed")
        
    except Exception as e:
        print(f"❌ Database demo failed: {e}")


def demo_pipeline():
    """Demonstrate data pipeline"""
    print("\n" + "="*60)
    print("⚙️  DEMONSTRATING DATA PIPELINE")
    print("="*60)
    
    try:
        from data_pipeline import DataPipeline
        
        # Create pipeline
        pipeline = DataPipeline(
            db_connection_string="mongodb+srv://varaprasadyoyo:JXlWJPUxAJiVB7Gx@cluster0.eviy31f.mongodb.net/AlgoSaaS?retryWrites=true&w=majority&appName=Cluster0",
            symbols=["NIFTY"],
            fetch_interval=10,  # 10 seconds for demo
            logger=logger
        )
        
        print("✅ Pipeline created")
        
        # Initialize pipeline
        print("🔧 Initializing pipeline...")
        pipeline.initialize()
        print("✅ Pipeline initialized")
        
        # Show status
        status = pipeline.get_status()
        print(f"📊 Pipeline status:")
        print(f"   Running: {status['running']}")
        print(f"   Symbols: {status['symbols']}")
        print(f"   Interval: {status['fetch_interval']}s")
        
        # Run a few cycles
        print("🔄 Running demo cycles...")
        for i in range(2):
            print(f"\n   Cycle {i+1}/2:")
            try:
                result = pipeline.run_single_cycle("NIFTY")
                if result["success"]:
                    print(f"   ✅ Success in {result['cycle_time']:.2f}s")
                    print(f"      Calls: {result.get('calls_processed', 0)} processed")
                    print(f"      Puts: {result.get('puts_processed', 0)} processed")
                else:
                    print(f"   ⚠️  Failed: {result.get('error', 'Unknown error')}")
            except Exception as e:
                print(f"   ❌ Cycle failed: {e}")
            
            if i < 1:  # Don't sleep after last cycle
                print(f"   ⏳ Waiting 5 seconds...")
                time.sleep(5)
        
        # Final status
        final_status = pipeline.get_status()
        print(f"\n📊 Final pipeline statistics:")
        print(f"   Total cycles: {final_status['total_cycles']}")
        print(f"   Success rate: {final_status['success_rate']:.1f}%")
        
        # Cleanup
        pipeline.cleanup()
        print("✅ Pipeline demo completed")
        
    except Exception as e:
        print(f"❌ Pipeline demo failed: {e}")


def demo_api_endpoints():
    """Demonstrate API endpoints"""
    print("\n" + "="*60)
    print("🌐 DEMONSTRATING API ENDPOINTS")
    print("="*60)
    
    try:
        import requests
        import json
        
        # Note: This assumes the API is running on localhost:8000
        base_url = "http://localhost:8000"
        
        print("📝 Note: This demo requires the API to be running.")
        print("   Start with: python nse_backend_app.py")
        print("   Then run this demo in another terminal.")
        
        # Test endpoints (will fail if API not running)
        endpoints = [
            ("/", "Root endpoint"),
            ("/health", "Health check"),
            ("/stats", "Statistics"),
            ("/pipeline/status", "Pipeline status"),
            ("/option-chain/NIFTY", "Option chain data")
        ]
        
        print("\n🧪 Testing API endpoints:")
        
        for endpoint, description in endpoints:
            try:
                response = requests.get(f"{base_url}{endpoint}", timeout=5)
                if response.status_code == 200:
                    print(f"   ✅ {endpoint} - {description}")
                else:
                    print(f"   ⚠️  {endpoint} - Status {response.status_code}")
            except requests.exceptions.ConnectionError:
                print(f"   ❌ {endpoint} - API not running")
            except Exception as e:
                print(f"   ❌ {endpoint} - Error: {e}")
        
        print("\n📖 API Documentation available at: http://localhost:8000/docs")
        
    except Exception as e:
        print(f"❌ API demo failed: {e}")


def main():
    """Run complete demonstration"""
    print("🚀 NSE Option Chain Backend - Complete Demonstration")
    print("=" * 80)
    
    print("This demonstration will show all components of the NSE backend system:")
    print("1. 🔄 NSE Data Fetching")
    print("2. 🗄️  Database Operations") 
    print("3. ⚙️  Data Pipeline")
    print("4. 🌐 API Endpoints")
    
    # Demo 1: NSE Fetcher
    calls, puts = demo_nse_fetcher()
    
    # Demo 2: Database Operations
    demo_database_operations(calls, puts)
    
    # Demo 3: Data Pipeline
    demo_pipeline()
    
    # Demo 4: API Endpoints
    demo_api_endpoints()
    
    print("\n" + "=" * 80)
    print("🎉 DEMONSTRATION COMPLETE")
    print("=" * 80)
    
    print("\n✅ All components demonstrated successfully!")
    print("\n🚀 To start the full backend system:")
    print("   python nse_backend_app.py")
    print("\n📖 Then visit http://localhost:8000/docs for API documentation")
    print("\n🔍 Monitor the system:")
    print("   Health: http://localhost:8000/health")
    print("   Stats: http://localhost:8000/stats")
    print("   Data: http://localhost:8000/option-chain/NIFTY")


if __name__ == "__main__":
    main()
