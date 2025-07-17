"""
MongoDB Database Service
========================

Service for storing and retrieving option chain data from MongoDB Atlas.
"""

import logging
from datetime import datetime, timezone
from typing import List, Dict, Optional, Tuple
from pymongo import MongoClient, errors as mongo_errors
from pymongo.collection import Collection
import time

from models import OptionChainEntry, OptionType, OptionChainFilter


class DatabaseService:
    """
    MongoDB service for option chain data storage and retrieval
    """
    
    def __init__(self, connection_string: str, database_name: str = "AlgoSaaS", 
                 collection_name: str = "option_chain", logger: Optional[logging.Logger] = None):
        self.connection_string = connection_string
        self.database_name = database_name
        self.collection_name = collection_name
        self.logger = logger or logging.getLogger(__name__)
        
        self.client = None
        self.db = None
        self.collection = None
        
        self._connect()
        self._setup_indexes()
    
    def _connect(self) -> None:
        """Connect to MongoDB Atlas"""
        
        try:
            self.logger.info("Connecting to MongoDB Atlas...")
            
            self.client = MongoClient(
                self.connection_string,
                serverSelectionTimeoutMS=30000,
                connectTimeoutMS=30000,
                socketTimeoutMS=30000,
                tls=True,
                tlsAllowInvalidCertificates=False,
                retryWrites=True,
                w='majority'
            )
            
            # Test connection with retry
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    self.client.admin.command('ping')
                    break
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise
                    self.logger.warning(f"Connection attempt {attempt + 1} failed: {e}")
                    time.sleep(2)
            
            self.db = self.client[self.database_name]
            self.collection = self.db[self.collection_name]
            
            self.logger.info("Successfully connected to MongoDB Atlas")
            
        except Exception as e:
            self.logger.error(f"Failed to connect to MongoDB: {e}")
            raise
    
    def _setup_indexes(self) -> None:
        """Setup MongoDB indexes for optimal performance"""
        
        try:
            self.logger.info("Setting up MongoDB indexes...")
            
            # Unique index for deduplication (identifier + fetched_at)
            self.collection.create_index([
                ("identifier", 1),
                ("fetched_at", 1)
            ], unique=True, name="dedup_index")
            
            # Index for efficient querying by symbol and type
            self.collection.create_index([
                ("symbol", 1),
                ("type", 1),
                ("fetched_at", -1)
            ], name="symbol_type_time_index")
            
            # Index for strike price queries
            self.collection.create_index([
                ("symbol", 1),
                ("strike_price", 1),
                ("expiry_date", 1)
            ], name="strike_expiry_index")
            
            # Index for time-based queries
            self.collection.create_index([
                ("fetched_at", -1)
            ], name="time_index")
            
            # Index for identifier queries
            self.collection.create_index([
                ("identifier", 1)
            ], name="identifier_index")
            
            self.logger.info("MongoDB indexes created successfully")
            
        except Exception as e:
            self.logger.warning(f"Index creation warning: {e}")
    
    def store_option_data(self, call_options: List[OptionChainEntry], 
                         put_options: List[OptionChainEntry]) -> Dict[str, int]:
        """
        Store option chain data with deduplication
        
        Args:
            call_options: List of call option entries
            put_options: List of put option entries
            
        Returns:
            Dictionary with insertion statistics
        """
        
        stats = {
            "calls_inserted": 0,
            "puts_inserted": 0,
            "calls_skipped": 0,
            "puts_skipped": 0,
            "total_processed": 0
        }
        
        # Store CALL options
        if call_options:
            call_stats = self._bulk_insert_with_dedup(call_options, "CALL")
            stats["calls_inserted"] = call_stats["inserted"]
            stats["calls_skipped"] = call_stats["skipped"]
        
        # Store PUT options
        if put_options:
            put_stats = self._bulk_insert_with_dedup(put_options, "PUT")
            stats["puts_inserted"] = put_stats["inserted"]
            stats["puts_skipped"] = put_stats["skipped"]
        
        stats["total_processed"] = len(call_options) + len(put_options)
        
        self.logger.info(
            f"Storage complete - Calls: {stats['calls_inserted']} inserted, {stats['calls_skipped']} skipped | "
            f"Puts: {stats['puts_inserted']} inserted, {stats['puts_skipped']} skipped"
        )
        
        return stats
    
    def _bulk_insert_with_dedup(self, options: List[OptionChainEntry], option_type: str) -> Dict[str, int]:
        """Bulk insert with duplicate handling"""
        
        if not options:
            return {"inserted": 0, "skipped": 0}
        
        # Convert to dictionaries for MongoDB
        documents = [option.dict() for option in options]
        
        inserted_count = 0
        skipped_count = 0
        
        try:
            # Use ordered=False to continue on duplicate key errors
            result = self.collection.insert_many(documents, ordered=False)
            inserted_count = len(result.inserted_ids)
            
        except mongo_errors.BulkWriteError as e:
            # Handle duplicate key errors
            inserted_count = e.details.get('nInserted', 0)
            skipped_count = len(documents) - inserted_count
            
            self.logger.debug(f"Bulk insert for {option_type}: {inserted_count} inserted, {skipped_count} duplicates skipped")
            
        except Exception as e:
            self.logger.error(f"Failed to insert {option_type} documents: {e}")
            raise
        
        return {"inserted": inserted_count, "skipped": skipped_count}
    
    def get_latest_option_data(self, symbol: str, limit: int = 100) -> Tuple[List[Dict], List[Dict]]:
        """
        Get latest option chain data for a symbol
        
        Args:
            symbol: Symbol to query
            limit: Maximum number of records per type
            
        Returns:
            Tuple of (call_options, put_options)
        """
        
        try:
            # Get latest timestamp for the symbol
            latest_doc = self.collection.find_one(
                {"symbol": symbol},
                sort=[("fetched_at", -1)]
            )
            
            if not latest_doc:
                return [], []
            
            latest_time = latest_doc["fetched_at"]
            
            # Get calls for latest timestamp
            calls = list(self.collection.find(
                {
                    "symbol": symbol,
                    "type": "CE",
                    "fetched_at": latest_time
                },
                {"_id": 0}  # Exclude MongoDB _id field
            ).sort("strike_price", 1).limit(limit))
            
            # Get puts for latest timestamp
            puts = list(self.collection.find(
                {
                    "symbol": symbol,
                    "type": "PE", 
                    "fetched_at": latest_time
                },
                {"_id": 0}  # Exclude MongoDB _id field
            ).sort("strike_price", 1).limit(limit))
            
            return calls, puts
            
        except Exception as e:
            self.logger.error(f"Failed to get latest option data: {e}")
            raise
    
    def get_filtered_data(self, filter_params: OptionChainFilter) -> List[Dict]:
        """
        Get filtered option chain data
        
        Args:
            filter_params: Filter parameters
            
        Returns:
            List of matching documents
        """
        
        try:
            # Build query
            query = {}
            
            if filter_params.symbol:
                query["symbol"] = filter_params.symbol
            
            if filter_params.option_type:
                query["type"] = filter_params.option_type.value
            
            if filter_params.strike_price_min is not None or filter_params.strike_price_max is not None:
                strike_query = {}
                if filter_params.strike_price_min is not None:
                    strike_query["$gte"] = filter_params.strike_price_min
                if filter_params.strike_price_max is not None:
                    strike_query["$lte"] = filter_params.strike_price_max
                query["strike_price"] = strike_query
            
            if filter_params.expiry_date:
                query["expiry_date"] = filter_params.expiry_date
            
            # Execute query
            cursor = self.collection.find(
                query,
                {"_id": 0}  # Exclude MongoDB _id field
            ).sort("fetched_at", -1).skip(filter_params.offset).limit(filter_params.limit)
            
            return list(cursor)
            
        except Exception as e:
            self.logger.error(f"Failed to get filtered data: {e}")
            raise
    
    def get_statistics(self) -> Dict:
        """Get database statistics"""
        
        try:
            # Total records
            total_records = self.collection.count_documents({})
            
            # Records by symbol
            symbol_pipeline = [
                {"$group": {"_id": "$symbol", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}}
            ]
            symbol_stats = {doc["_id"]: doc["count"] for doc in self.collection.aggregate(symbol_pipeline)}
            
            # Records by type
            type_pipeline = [
                {"$group": {"_id": "$type", "count": {"$sum": 1}}}
            ]
            type_stats = {doc["_id"]: doc["count"] for doc in self.collection.aggregate(type_pipeline)}
            
            # Latest fetch time
            latest_doc = self.collection.find_one({}, sort=[("fetched_at", -1)])
            latest_fetch = latest_doc["fetched_at"] if latest_doc else None
            
            # Data freshness
            freshness = None
            if latest_fetch:
                freshness = (datetime.now(timezone.utc) - latest_fetch).total_seconds()
            
            return {
                "total_records": total_records,
                "records_by_symbol": symbol_stats,
                "records_by_type": type_stats,
                "latest_fetch_time": latest_fetch,
                "data_freshness_seconds": freshness
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get statistics: {e}")
            raise
    
    def health_check(self) -> bool:
        """Check database connection health"""
        
        try:
            self.client.admin.command('ping')
            return True
        except Exception as e:
            self.logger.error(f"Database health check failed: {e}")
            return False
    
    def close(self) -> None:
        """Close database connection"""
        
        if self.client:
            self.client.close()
            self.logger.info("Database connection closed")
