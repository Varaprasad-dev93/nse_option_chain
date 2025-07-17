#!/usr/bin/env python3
"""
Demo Script for NSE Option Chain Pipeline
=========================================

This script demonstrates the production-grade NSE Option Chain pipeline
by running a few cycles and showing the results.
"""

import time
from nse_option_chain_pipeline import NSEOptionChainPipeline, PipelineConfig


def demo_pipeline():
    """Demonstrate the pipeline with a few cycles"""
    
    print("🚀 NSE Option Chain Pipeline - Live Demo")
    print("=" * 60)
    
    # Create pipeline with custom config for demo
    config = PipelineConfig()
    config.FETCH_INTERVAL_SECONDS = 10  # 10 seconds for demo
    
    pipeline = NSEOptionChainPipeline(config)
    
    try:
        # Initialize pipeline
        print("🔧 Initializing pipeline components...")
        pipeline.initialize()
        print("✅ Pipeline initialized successfully!")
        
        # Run a few cycles
        print(f"\n📊 Running demo cycles (fetching every {config.FETCH_INTERVAL_SECONDS} seconds)")
        print("-" * 60)
        
        total_stats = {"calls_inserted": 0, "puts_inserted": 0, "calls_skipped": 0, "puts_skipped": 0}
        
        for cycle in range(3):  # Run 3 cycles for demo
            print(f"\n🔄 Cycle {cycle + 1}/3")
            
            cycle_start = time.time()
            stats = pipeline.run_single_cycle("NIFTY")
            cycle_time = time.time() - cycle_start
            
            # Update totals
            for key in total_stats:
                total_stats[key] += stats.get(key, 0)
            
            print(f"   ⏱️  Completed in {cycle_time:.2f} seconds")
            print(f"   📈 Calls: {stats['calls_inserted']} inserted, {stats['calls_skipped']} skipped")
            print(f"   📉 Puts: {stats['puts_inserted']} inserted, {stats['puts_skipped']} skipped")
            
            if cycle < 2:  # Don't sleep after last cycle
                print(f"   ⏳ Waiting {config.FETCH_INTERVAL_SECONDS} seconds...")
                time.sleep(config.FETCH_INTERVAL_SECONDS)
        
        # Show summary
        print("\n" + "=" * 60)
        print("📊 DEMO SUMMARY")
        print("=" * 60)
        print(f"Total Calls Inserted: {total_stats['calls_inserted']}")
        print(f"Total Puts Inserted: {total_stats['puts_inserted']}")
        print(f"Total Calls Skipped: {total_stats['calls_skipped']}")
        print(f"Total Puts Skipped: {total_stats['puts_skipped']}")
        
        # Show current data count
        try:
            counts = pipeline.storage.get_latest_data_count("NIFTY")
            print(f"\n📈 Current NIFTY data in MongoDB:")
            print(f"   Calls: {counts['calls']}")
            print(f"   Puts: {counts['puts']}")
        except Exception as e:
            print(f"⚠️  Could not retrieve data count: {e}")
        
        print("\n✅ Demo completed successfully!")
        print("\nTo run the full continuous pipeline:")
        print("   python nse_option_chain_pipeline.py")
        
    except KeyboardInterrupt:
        print("\n⏹️  Demo interrupted by user")
    except Exception as e:
        print(f"\n❌ Demo failed: {e}")
    finally:
        pipeline.cleanup()
        print("🧹 Cleanup completed")


if __name__ == "__main__":
    demo_pipeline()
