"""
Background Data Pipeline Service
================================

Background service that continuously fetches and stores NSE option chain data.
"""

import asyncio
import logging
import signal
import time
from datetime import datetime, timezone
from typing import List, Optional
import threading

from nse_service import NSEDataFetcher
from database_service import DatabaseService


class DataPipeline:
    """
    Background service for continuous data fetching and storage
    """
    
    def __init__(self, db_connection_string: str, symbols: List[str] = None, 
                 fetch_interval: int = 5, logger: Optional[logging.Logger] = None):
        self.db_connection_string = db_connection_string
        self.symbols = symbols or ["NIFTY"]
        self.fetch_interval = fetch_interval
        self.logger = logger or logging.getLogger(__name__)
        
        self.running = False
        self.nse_fetcher = None
        self.db_service = None
        
        # Statistics
        self.total_cycles = 0
        self.successful_cycles = 0
        self.failed_cycles = 0
        self.last_successful_fetch = None
        self.stats = {
            "total_calls_inserted": 0,
            "total_puts_inserted": 0,
            "total_calls_skipped": 0,
            "total_puts_skipped": 0
        }
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        self.logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self.running = False
    
    def initialize(self) -> None:
        """Initialize pipeline components"""
        
        try:
            self.logger.info("Initializing data pipeline components...")
            
            # Initialize NSE fetcher
            self.nse_fetcher = NSEDataFetcher(self.logger)
            
            # Initialize database service
            self.db_service = DatabaseService(
                connection_string=self.db_connection_string,
                logger=self.logger
            )
            
            self.logger.info("Data pipeline components initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize pipeline: {e}")
            raise
    
    def run_single_cycle(self, symbol: str) -> dict:
        """
        Run a single data fetch and store cycle
        
        Args:
            symbol: Symbol to fetch data for
            
        Returns:
            Dictionary with cycle statistics
        """
        
        cycle_start = time.time()
        
        try:
            self.logger.debug(f"Starting data cycle for {symbol}")
            
            # Fetch data from NSE
            call_options, put_options = self.nse_fetcher.fetch_and_parse(symbol)
            
            # Store to database
            storage_stats = self.db_service.store_option_data(call_options, put_options)
            
            # Calculate cycle time
            cycle_time = time.time() - cycle_start
            
            # Update statistics
            self.stats["total_calls_inserted"] += storage_stats["calls_inserted"]
            self.stats["total_puts_inserted"] += storage_stats["puts_inserted"]
            self.stats["total_calls_skipped"] += storage_stats["calls_skipped"]
            self.stats["total_puts_skipped"] += storage_stats["puts_skipped"]
            
            self.last_successful_fetch = datetime.now(timezone.utc)
            
            self.logger.info(
                f"[SUCCESS] Cycle completed for {symbol} in {cycle_time:.2f}s - "
                f"Calls: {storage_stats['calls_inserted']} inserted, {storage_stats['calls_skipped']} skipped | "
                f"Puts: {storage_stats['puts_inserted']} inserted, {storage_stats['puts_skipped']} skipped"
            )
            
            return {
                "symbol": symbol,
                "success": True,
                "cycle_time": cycle_time,
                "calls_processed": len(call_options),
                "puts_processed": len(put_options),
                **storage_stats
            }
            
        except Exception as e:
            cycle_time = time.time() - cycle_start
            self.logger.error(f"[FAILED] Cycle failed for {symbol} after {cycle_time:.2f}s: {e}")

            return {
                "symbol": symbol,
                "success": False,
                "cycle_time": cycle_time,
                "error": str(e)
            }
    
    def run_continuous(self) -> None:
        """Run continuous data pipeline"""
        
        self.running = True
        self.logger.info(f"Starting continuous data pipeline for symbols: {self.symbols}")
        self.logger.info(f"Fetch interval: {self.fetch_interval} seconds")
        
        try:
            while self.running:
                cycle_start = time.time()
                self.total_cycles += 1
                
                self.logger.info(f"--- Cycle #{self.total_cycles} ---")
                
                cycle_success = True
                
                for symbol in self.symbols:
                    if not self.running:
                        break
                    
                    try:
                        result = self.run_single_cycle(symbol)
                        if not result["success"]:
                            cycle_success = False
                    except Exception as e:
                        self.logger.error(f"Failed to process {symbol}: {e}")
                        cycle_success = False
                        continue
                
                # Update cycle statistics
                if cycle_success:
                    self.successful_cycles += 1
                else:
                    self.failed_cycles += 1
                
                # Log periodic summary
                if self.total_cycles % 10 == 0:
                    self._log_summary()
                
                # Calculate sleep time
                cycle_time = time.time() - cycle_start
                sleep_time = max(0, self.fetch_interval - cycle_time)
                
                if sleep_time > 0:
                    self.logger.debug(f"Sleeping for {sleep_time:.2f} seconds")
                    time.sleep(sleep_time)
                else:
                    self.logger.warning(f"Cycle took {cycle_time:.2f}s, longer than interval {self.fetch_interval}s")
        
        except KeyboardInterrupt:
            self.logger.info("Pipeline interrupted by user")
        except Exception as e:
            self.logger.error(f"Pipeline failed with error: {e}")
            raise
        finally:
            self.cleanup()
    
    def _log_summary(self) -> None:
        """Log periodic summary statistics"""
        
        success_rate = (self.successful_cycles / self.total_cycles * 100) if self.total_cycles > 0 else 0
        
        self.logger.info(
            f"[SUMMARY] After {self.total_cycles} cycles - "
            f"Success rate: {success_rate:.1f}% ({self.successful_cycles}/{self.total_cycles}) | "
            f"Total inserted: {self.stats['total_calls_inserted']} calls, {self.stats['total_puts_inserted']} puts | "
            f"Total skipped: {self.stats['total_calls_skipped']} calls, {self.stats['total_puts_skipped']} puts"
        )
        
        # Log database statistics
        try:
            db_stats = self.db_service.get_statistics()
            self.logger.info(
                f"[DATABASE] Stats: {db_stats['total_records']} total records, "
                f"freshness: {db_stats['data_freshness_seconds']:.1f}s"
            )
        except Exception as e:
            self.logger.warning(f"Failed to get database statistics: {e}")
    
    def get_status(self) -> dict:
        """Get current pipeline status"""
        
        return {
            "running": self.running,
            "total_cycles": self.total_cycles,
            "successful_cycles": self.successful_cycles,
            "failed_cycles": self.failed_cycles,
            "success_rate": (self.successful_cycles / self.total_cycles * 100) if self.total_cycles > 0 else 0,
            "last_successful_fetch": self.last_successful_fetch,
            "symbols": self.symbols,
            "fetch_interval": self.fetch_interval,
            "statistics": self.stats.copy()
        }
    
    def cleanup(self) -> None:
        """Cleanup resources"""
        
        self.logger.info("Cleaning up pipeline resources...")
        
        if self.db_service:
            self.db_service.close()
        
        self.logger.info("Pipeline shutdown complete")


class PipelineManager:
    """
    Manager for running the data pipeline in a separate thread
    """
    
    def __init__(self, db_connection_string: str, symbols: List[str] = None, 
                 fetch_interval: int = 5, logger: Optional[logging.Logger] = None):
        self.pipeline = DataPipeline(db_connection_string, symbols, fetch_interval, logger)
        self.thread = None
        self.logger = logger or logging.getLogger(__name__)
    
    def start(self) -> None:
        """Start the pipeline in a background thread"""
        
        if self.thread and self.thread.is_alive():
            self.logger.warning("Pipeline is already running")
            return
        
        try:
            self.pipeline.initialize()
            
            self.thread = threading.Thread(
                target=self.pipeline.run_continuous,
                daemon=True,
                name="DataPipeline"
            )
            self.thread.start()
            
            self.logger.info("Data pipeline started in background thread")
            
        except Exception as e:
            self.logger.error(f"Failed to start pipeline: {e}")
            raise
    
    def stop(self) -> None:
        """Stop the pipeline"""
        
        if self.pipeline:
            self.pipeline.running = False
        
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=10)
            
        self.logger.info("Data pipeline stopped")
    
    def get_status(self) -> dict:
        """Get pipeline status"""
        
        if self.pipeline:
            return self.pipeline.get_status()
        
        return {"running": False}
    
    def is_running(self) -> bool:
        """Check if pipeline is running"""
        
        return self.thread and self.thread.is_alive() and self.pipeline.running


def setup_logging() -> logging.Logger:
    """Setup logging configuration"""
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('data_pipeline.log')
        ]
    )
    
    return logging.getLogger(__name__)


# Example usage
if __name__ == "__main__":
    logger = setup_logging()
    
    # MongoDB connection string
    db_connection = "mongodb+srv://varaprasadyoyo:JXlWJPUxAJiVB7Gx@cluster0.eviy31f.mongodb.net/AlgoSaaS?retryWrites=true&w=majority&appName=Cluster0"
    
    # Create and run pipeline
    pipeline = DataPipeline(
        db_connection_string=db_connection,
        symbols=["NIFTY"],
        fetch_interval=5,
        logger=logger
    )
    
    try:
        pipeline.initialize()
        pipeline.run_continuous()
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
