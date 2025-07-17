#!/usr/bin/env python3
"""
Example Usage of NSE Option Chain Pipeline
==========================================

This script demonstrates various ways to use the NSE Option Chain Pipeline
for different use cases.
"""

import time
from nse_option_chain_pipeline import NSEOptionChainPipeline, PipelineConfig


def example_single_fetch():
    """Example: Fetch data once for a specific symbol"""
    print("🔄 Example: Single Data Fetch")
    print("-" * 40)
    
    # Create pipeline
    pipeline = NSEOptionChainPipeline()
    
    try:
        # Initialize
        pipeline.initialize()
        
        # Fetch data once for NIFTY
        stats = pipeline.run_single_cycle("NIFTY")
        
        print(f"✅ Data fetched successfully!")
        print(f"   Calls inserted: {stats['calls_inserted']}")
        print(f"   Puts inserted: {stats['puts_inserted']}")
        print(f"   Calls skipped: {stats['calls_skipped']}")
        print(f"   Puts skipped: {stats['puts_skipped']}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        pipeline.cleanup()


def example_multiple_symbols():
    """Example: Fetch data for multiple symbols"""
    print("\n🔄 Example: Multiple Symbols")
    print("-" * 40)
    
    # Create pipeline
    pipeline = NSEOptionChainPipeline()
    
    try:
        # Initialize
        pipeline.initialize()
        
        # Fetch data for multiple symbols
        symbols = ["NIFTY", "BANKNIFTY"]
        
        for symbol in symbols:
            print(f"Fetching data for {symbol}...")
            stats = pipeline.run_single_cycle(symbol)
            print(f"   {symbol}: {stats['calls_inserted']} calls, {stats['puts_inserted']} puts")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        pipeline.cleanup()


def example_custom_config():
    """Example: Using custom configuration"""
    print("\n🔄 Example: Custom Configuration")
    print("-" * 40)
    
    # Create custom configuration
    config = PipelineConfig()
    config.FETCH_INTERVAL_SECONDS = 10  # Fetch every 10 seconds instead of 5
    config.MAX_RETRIES = 5              # More retries
    config.REQUEST_TIMEOUT = 20         # Longer timeout
    
    # Create pipeline with custom config
    pipeline = NSEOptionChainPipeline(config)
    
    try:
        pipeline.initialize()
        
        print(f"Using custom config:")
        print(f"   Fetch interval: {config.FETCH_INTERVAL_SECONDS} seconds")
        print(f"   Max retries: {config.MAX_RETRIES}")
        print(f"   Request timeout: {config.REQUEST_TIMEOUT} seconds")
        
        # Run single cycle
        stats = pipeline.run_single_cycle()
        print(f"✅ Fetched with custom config: {stats}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        pipeline.cleanup()


def example_continuous_limited():
    """Example: Run continuous pipeline for limited time"""
    print("\n🔄 Example: Continuous Pipeline (Limited Time)")
    print("-" * 40)
    
    # Create pipeline
    pipeline = NSEOptionChainPipeline()
    
    try:
        pipeline.initialize()
        
        print("Running pipeline for 30 seconds...")
        
        # Start pipeline in a separate thread
        import threading
        
        def run_pipeline():
            pipeline.run_continuous(symbols=["NIFTY"])
        
        pipeline_thread = threading.Thread(target=run_pipeline, daemon=True)
        pipeline_thread.start()
        
        # Let it run for 30 seconds
        time.sleep(30)
        
        # Stop pipeline
        pipeline.running = False
        print("✅ Pipeline stopped after 30 seconds")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        pipeline.cleanup()


def example_data_monitoring():
    """Example: Monitor data in MongoDB"""
    print("\n🔄 Example: Data Monitoring")
    print("-" * 40)
    
    from nse_option_chain_pipeline import MongoDBStorage
    
    config = PipelineConfig()
    
    try:
        # Connect to MongoDB
        storage = MongoDBStorage(config, None)
        
        # Get data counts
        symbols = ["NIFTY", "BANKNIFTY"]
        
        for symbol in symbols:
            counts = storage.get_latest_data_count(symbol)
            print(f"{symbol}: {counts['calls']} calls, {counts['puts']} puts")
        
        # Get latest document
        latest_doc = storage.collection.find_one(
            {"symbol": "NIFTY"},
            sort=[("fetched_at", -1)]
        )
        
        if latest_doc:
            print(f"\nLatest NIFTY data timestamp: {latest_doc['fetched_at']}")
            print(f"Sample strike price: {latest_doc.get('strike_price', 'N/A')}")
        
        storage.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")


def main():
    """Run all examples"""
    print("🚀 NSE Option Chain Pipeline - Usage Examples")
    print("=" * 60)
    
    # Run examples
    example_single_fetch()
    example_multiple_symbols()
    example_custom_config()
    example_data_monitoring()
    
    # Note: Uncomment the line below to run continuous example
    # example_continuous_limited()
    
    print("\n✅ All examples completed!")
    print("\nTo run the full continuous pipeline:")
    print("   python nse_option_chain_pipeline.py")


if __name__ == "__main__":
    main()
