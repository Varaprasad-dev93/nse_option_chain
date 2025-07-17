# NSE Option Chain Data Pipeline

A **production-grade, enterprise-level** real-time data ingestion pipeline for NSE Option Chain CSV data with MongoDB Atlas storage.

## 🎯 Features

- **Real-time Data Ingestion**: Fetches NSE option chain data every 5 seconds
- **Comprehensive CSV Parsing**: Handles all columns (OI, CHNG IN OI, VOLUME, IV, LTP, CHNG, BID, ASK, STRIKE, etc.)
- **Intelligent Deduplication**: Prevents duplicate entries based on symbol+type+strike+timestamp
- **Production-Grade Error Handling**: Robust retry logic, session management, graceful failures
- **MongoDB Atlas Integration**: Optimized storage with proper indexing
- **Comprehensive Logging**: Structured logging with operation counts and performance metrics
- **Scalable Architecture**: Modular design supporting multiple symbols (NIFTY, BANKNIFTY, FINNIFTY)

## 🏗️ Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   NSE API       │    │   CSV Parser     │    │   MongoDB       │
│   (CSV Data)    │───▶│   (Pandas)       │───▶│   Atlas         │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         ▲                        ▲                       ▲
         │                        │                       │
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│ HTTP Client     │    │ Data Transform   │    │ Deduplication   │
│ (Session Mgmt)  │    │ (Clean & Map)    │    │ (Unique Index)  │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

## 📦 Installation

1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure MongoDB**: Update the MongoDB URI in `PipelineConfig` class if needed.

## 🚀 Quick Start

### Run the Pipeline
```bash
python nse_option_chain_pipeline.py
```

### Test the Pipeline
```bash
python test_pipeline.py
```

## 📊 Data Schema

Each document stored in MongoDB contains:

```json
{
  "_id": "ObjectId",
  "symbol": "NIFTY",
  "type": "CALL" | "PUT", 
  "strike_price": 25000.0,
  "fetched_at": "2025-07-17T10:30:00.000Z",
  "open_interest": 1234.0,
  "change_in_oi": 56.0,
  "volume": 7890.0,
  "implied_volatility": 15.25,
  "last_traded_price": 123.45,
  "change": -2.30,
  "bid_qty": 100,
  "bid_price": 123.00,
  "ask_price": 124.00,
  "ask_qty": 150
}
```

## ⚙️ Configuration

Modify `PipelineConfig` class in `nse_option_chain_pipeline.py`:

```python
@dataclass
class PipelineConfig:
    # MongoDB Configuration
    MONGO_URI: str = "your_mongodb_uri"
    DATABASE_NAME: str = "nse_data"
    COLLECTION_NAME: str = "nifty_option_chain"
    
    # Pipeline Configuration  
    FETCH_INTERVAL_SECONDS: int = 5
    DEFAULT_SYMBOL: str = "NIFTY"
    MAX_RETRIES: int = 3
    REQUEST_TIMEOUT: int = 15
```

## 🔧 Advanced Usage

### Multiple Symbols
```python
# Run for multiple symbols
pipeline.run_continuous(symbols=["NIFTY", "BANKNIFTY", "FINNIFTY"])
```

### Single Cycle (Testing)
```python
# Run single data fetch cycle
stats = pipeline.run_single_cycle("NIFTY")
print(f"Inserted: {stats['calls_inserted']} calls, {stats['puts_inserted']} puts")
```

## 📈 Monitoring & Logging

The pipeline provides comprehensive logging:

- **INFO Level**: Cycle completion, insertion counts, periodic summaries
- **DEBUG Level**: Detailed operation logs, timing information
- **ERROR Level**: Failures, retry attempts, connection issues

### Log Output Example:
```
2025-07-17 10:30:15 - NSE_Pipeline - INFO - ✅ Cycle completed for NIFTY in 2.34s - Calls: 45 inserted, 0 skipped | Puts: 43 inserted, 0 skipped
2025-07-17 10:30:25 - NSE_Pipeline - INFO - 📊 Summary after 10 cycles - Total Calls: 450 inserted, 12 skipped | Total Puts: 430 inserted, 8 skipped
```

## 🛡️ Error Handling

- **HTTP Failures**: Automatic retry with exponential backoff
- **CSV Parsing Errors**: Graceful handling with detailed error logs
- **MongoDB Errors**: Duplicate key handling, connection recovery
- **Network Issues**: Session refresh, timeout management

## 🔍 Testing

Run comprehensive tests before production:

```bash
python test_pipeline.py
```

Tests include:
- HTTP client functionality
- CSV parsing accuracy
- MongoDB connection
- Single pipeline cycle
- Data validation

## 📋 Production Deployment

1. **Environment Setup**:
   ```bash
   # Create virtual environment
   python -m venv venv
   source venv/bin/activate  # Linux/Mac
   # or
   venv\Scripts\activate     # Windows
   
   # Install dependencies
   pip install -r requirements.txt
   ```

2. **Run as Service** (Linux):
   ```bash
   # Create systemd service file
   sudo nano /etc/systemd/system/nse-pipeline.service
   
   # Enable and start service
   sudo systemctl enable nse-pipeline.service
   sudo systemctl start nse-pipeline.service
   ```

3. **Monitor Logs**:
   ```bash
   tail -f nse_pipeline.log
   ```

## 🔧 Troubleshooting

### Common Issues:

1. **NSE API Access Denied**:
   - Check internet connection
   - Verify NSE website accessibility
   - Review HTTP headers and user agent

2. **MongoDB Connection Failed**:
   - Verify MongoDB URI
   - Check network connectivity
   - Validate credentials

3. **CSV Parsing Errors**:
   - Check NSE CSV format changes
   - Review column mappings
   - Validate data types

## 📊 Performance Metrics

- **Fetch Time**: ~1-3 seconds per symbol
- **Parse Time**: ~0.1-0.5 seconds
- **Storage Time**: ~0.2-1 seconds
- **Total Cycle Time**: ~2-5 seconds
- **Memory Usage**: ~50-100 MB
- **Storage Growth**: ~1-2 MB per hour per symbol

## 🤝 Contributing

1. Fork the repository
2. Create feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit pull request

