# NSE Option Chain Backend System

A **production-grade, enterprise-level** Python backend system for fetching, storing, and serving NSE Option Chain data with real-time updates and comprehensive API endpoints.

## 🎯 Features

- **Real-time Data Ingestion**: Fetches NSE option chain JSON data every 5 seconds
- **Complete Data Processing**: Parses all fields (OI, change_in_OI, pChange, volume, IV, bid/ask, etc.)
- **MongoDB Atlas Integration**: Stores each CE/PE entry as separate documents with proper schema
- **Intelligent Deduplication**: Uses `identifier + fetched_at` for duplicate prevention
- **FastAPI REST API**: Comprehensive endpoints for data access and filtering
- **Background Pipeline**: Continuous data processing with error handling and monitoring
- **Production-Ready**: Comprehensive logging, health checks, and statistics

## 🏗️ System Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   NSE API       │    │   Data Pipeline  │    │   MongoDB       │
│   (JSON Data)   │───▶│   (Background)   │───▶│   Atlas         │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         ▲                        ▲                       ▲
         │                        │                       │
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│ NSE Service     │    │ Database Service │    │ FastAPI App     │
│ (HTTP Client)   │    │ (Storage Layer)  │    │ (REST API)      │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

## 📊 Data Schema

Each MongoDB document follows this schema:

```json
{
  "_id": "ObjectId",
  "symbol": "NIFTY",
  "type": "CE" | "PE",
  "strike_price": 25000.0,
  "expiry_date": "30-Jan-2025",
  "identifier": "NIFTY_CE_25000_30-Jan-2025",
  "fetched_at": "2025-07-17T10:30:00.000Z",
  "open_interest": 1234.0,
  "change_in_oi": 56.0,
  "pchange_in_oi": 4.5,
  "last_price": 123.45,
  "change": -2.30,
  "pchange": -1.83,
  "total_traded_volume": 7890.0,
  "implied_volatility": 15.25,
  "bid_qty": 100,
  "bid_price": 123.00,
  "ask_price": 124.00,
  "ask_qty": 150,
  "underlying_value": 25100.75
}
```

## 🚀 Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements_nse_backend.txt
```

### 2. Run the Backend
```bash
python nse_backend_app.py
```

### 3. Access the API
- **API Documentation**: http://localhost:8000/docs
- **Health Check**: http://localhost:8000/health
- **Option Chain Data**: http://localhost:8000/option-chain/NIFTY

## 📡 API Endpoints

### Core Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | API information and available endpoints |
| `/health` | GET | Health check with database and pipeline status |
| `/stats` | GET | Database statistics and data freshness |
| `/pipeline/status` | GET | Background pipeline status and metrics |

### Data Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/option-chain/{symbol}` | GET | Latest option chain data for symbol |
| `/option-chain/filter` | POST | Filtered option chain data with parameters |
| `/option-chain/{symbol}/strikes` | GET | Available strike prices for symbol |
| `/option-chain/{symbol}/expiry-dates` | GET | Available expiry dates for symbol |

### Example API Calls

```bash
# Get latest NIFTY option chain
curl http://localhost:8000/option-chain/NIFTY

# Get health status
curl http://localhost:8000/health

# Get statistics
curl http://localhost:8000/stats

# Filter data
curl -X POST http://localhost:8000/option-chain/filter \
  -H "Content-Type: application/json" \
  -d '{
    "symbol": "NIFTY",
    "option_type": "CE",
    "strike_price_min": 25000,
    "strike_price_max": 26000,
    "limit": 50
  }'
```

## 🧪 Testing

### Run Test Suite
```bash
# Install pytest
pip install pytest pytest-asyncio

# Run all tests
pytest test_nse_backend.py -v

# Run specific test class
pytest test_nse_backend.py::TestNSEDataFetcher -v
```

### Manual Testing
```bash
# Test basic functionality
python test_nse_backend.py
```

## 📁 Project Structure

```
nse-backend/
├── models.py                 # Pydantic data models and validation
├── nse_service.py            # NSE API client and data fetching
├── database_service.py       # MongoDB storage and retrieval
├── data_pipeline.py          # Background data processing pipeline
├── nse_backend_app.py        # FastAPI application and endpoints
├── test_nse_backend.py       # Comprehensive test suite
├── requirements_nse_backend.txt  # Python dependencies
└── README_NSE_Backend.md     # This documentation
```

## ⚙️ Configuration

### Environment Variables (Optional)
```bash
export NSE_FETCH_INTERVAL=5
export NSE_SYMBOLS="NIFTY,BANKNIFTY"
export MONGODB_URI="your_mongodb_connection_string"
export LOG_LEVEL="INFO"
```

### Code Configuration
Modify constants in `nse_backend_app.py`:
```python
DB_CONNECTION_STRING = "your_mongodb_uri"
SYMBOLS_TO_TRACK = ["NIFTY", "BANKNIFTY"]
FETCH_INTERVAL = 5  # seconds
```

## 🔧 Components Deep Dive

### 1. NSE Service (`nse_service.py`)
- **Robust HTTP Client**: Session management with retry logic
- **Data Fetching**: Fetches JSON data from NSE API
- **Data Parsing**: Converts NSE format to structured models
- **Error Handling**: Comprehensive error recovery

### 2. Database Service (`database_service.py`)
- **MongoDB Integration**: Optimized connection and operations
- **Indexing Strategy**: Efficient indexes for queries and deduplication
- **Batch Operations**: High-performance bulk inserts
- **Query Methods**: Flexible data retrieval with filtering

### 3. Data Pipeline (`data_pipeline.py`)
- **Background Processing**: Continuous data fetching every 5 seconds
- **Pipeline Management**: Start/stop controls and status monitoring
- **Statistics Tracking**: Comprehensive metrics and success rates
- **Error Recovery**: Graceful handling of failures

### 4. FastAPI Application (`nse_backend_app.py`)
- **REST API**: Comprehensive endpoints for data access
- **Dependency Injection**: Clean service management
- **Error Handling**: Proper HTTP status codes and error messages
- **Documentation**: Auto-generated OpenAPI/Swagger docs

## 📊 Monitoring & Logging

### Log Files
- `nse_backend.log` - Main application logs
- `data_pipeline.log` - Background pipeline logs

### Key Metrics
- **Data Freshness**: Time since last successful fetch
- **Success Rate**: Percentage of successful pipeline cycles
- **Record Counts**: Total records by symbol and type
- **Performance**: Cycle times and processing rates

### Health Monitoring
```bash
# Check system health
curl http://localhost:8000/health

# Monitor pipeline status
curl http://localhost:8000/pipeline/status

# View statistics
curl http://localhost:8000/stats
```

## 🚀 Production Deployment

### 1. Docker Deployment (Recommended)
```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY requirements_nse_backend.txt .
RUN pip install -r requirements_nse_backend.txt

COPY . .
EXPOSE 8000

CMD ["python", "nse_backend_app.py"]
```

### 2. Systemd Service (Linux)
```ini
[Unit]
Description=NSE Option Chain Backend
After=network.target

[Service]
Type=simple
User=nse-backend
WorkingDirectory=/opt/nse-backend
ExecStart=/opt/nse-backend/venv/bin/python nse_backend_app.py
Restart=always

[Install]
WantedBy=multi-user.target
```

### 3. Process Manager (PM2)
```bash
pm2 start nse_backend_app.py --name nse-backend --interpreter python3
```

## 🔒 Security Considerations

- **Database Security**: Use MongoDB Atlas with proper authentication
- **API Security**: Implement rate limiting and authentication as needed
- **Network Security**: Use HTTPS in production
- **Environment Variables**: Store sensitive data in environment variables

## 📈 Performance Optimization

- **Database Indexes**: Optimized for common query patterns
- **Connection Pooling**: Efficient database connection management
- **Batch Operations**: Bulk inserts for better performance
- **Caching**: Consider Redis for frequently accessed data

## 🛠️ Troubleshooting

### Common Issues

1. **NSE API Access Denied**
   - Check internet connectivity
   - Verify NSE website accessibility
   - Review HTTP headers and user agent

2. **MongoDB Connection Failed**
   - Verify connection string
   - Check network connectivity
   - Validate credentials and permissions

3. **Data Not Updating**
   - Check pipeline status: `/pipeline/status`
   - Review logs for errors
   - Verify NSE API response format

### Debug Mode
```python
# Enable debug logging
import logging
logging.basicConfig(level=logging.DEBUG)
```

## 🤝 Contributing

1. Fork the repository
2. Create feature branch: `git checkout -b feature/new-feature`
3. Add tests for new functionality
4. Ensure all tests pass: `pytest test_nse_backend.py -v`
5. Submit pull request

## 📄 License

This project is licensed under the MIT License.

---

**Built with ❤️ by Augment AI for enterprise-grade financial data processing.**
