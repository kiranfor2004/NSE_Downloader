# 🚀 NSE Data Downloader & Analysis Tool

A comprehensive Python toolkit for downloading, storing, and analyzing NSE (National Stock Exchange) stock market data. This project includes automated data collection, local database management, and advanced analysis tools for Indian stock market research.

## 📊 Project Overview

**Data Coverage:**
- **55,780+ stock records** across August 2025
- **19 trading days** of complete market data
- **3,121 unique stocks** with full OHLCV data
- **Complete Bhavcopy & Security Deliverable data**

## 🎯 Features

### 📈 Data Collection
- **Automated NSE Data Download** - Downloads complete Bhavcopy data
- **Smart Error Handling** - Retries failed downloads with multiple URL fallbacks
- **Progress Tracking** - Real-time download progress with detailed status
- **Data Validation** - Ensures data integrity and completeness

### 🗄️ Database Management
- **SQLite Integration** - Fast local database with optimized schema
- **Bulk Import** - Efficiently imports thousands of records
- **Data Deduplication** - Prevents duplicate entries
- **Performance Optimization** - Indexed columns for fast queries

### 📊 Analysis Tools
- **Market Movers** - Top gainers/losers analysis
- **Volume Analytics** - Trading volume and turnover insights
- **Delivery Analysis** - High delivery percentage stock identification
- **Individual Stock Tracking** - Complete performance history
- **Market Summaries** - Daily market overview statistics
- **Export Capabilities** - CSV export for further analysis

### ☁️ Cloud Integration
- **Supabase Support** - Cloud database integration for remote access
- **Scalable Architecture** - Handle large datasets efficiently
- **API Integration** - REST API access to your data

## 🚀 Quick Start

### Prerequisites
```bash
Python 3.8+
pip install requests pandas
```

### Installation
1. Clone this repository:
```bash
git clone https://github.com/kiranfor2004/NSE_Downloader.git
cd NSE_Downloader
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

### Usage

#### 1. Download NSE Data
```bash
python nse_august_2025_downloader.py
```
This downloads all August 2025 trading data (19 files, ~40MB)

#### 2. Setup Database
```bash
python setup_database.py
```
Creates SQLite database and imports all CSV files

#### 3. Start Analysis
#### 4. (New) Download F&O UDiFF Derivatives Bhavcopy
Fetches "F&O - UDiFF Common Bhavcopy Final" ZIP archives discovered via the merged daily reports API.

Run:
```
python nse_fo_udiff_real_endpoint_downloader.py
```
Outputs ZIP files to `fo_udiff_downloads/` named:
`BhavCopy_NSE_FO_0_0_0_YYYYMMDD_F_0000.csv.zip`

Next planned improvements:
- Add CLI options for arbitrary month/year
- Retry logic for transient small responses
- PostgreSQL importer (table: fo_udiff_bhavcopy)
- Cross-check missing days vs holiday calendar

```bash
python nse_query_tool.py
```
Interactive analysis tool with multiple query options

## 📁 Project Structure (Step-Oriented)

```
NSE_Downloader/
├── step01_equity_downloads/            # Wrappers for equity daily download scripts
├── step02_monthly_analysis/            # Wrappers & docs for monthly unique symbol analysis
├── step03_monthly_comparisons/         # Wrappers for increases-only monthly comparisons
├── step04_derivatives_udiff/           # F&O UDiFF (Derivatives) downloader wrappers & docs
│
├── 01_Data_Downloads/                  # Original equity download scripts (legacy)
├── 02_Monthly_Analysis/                # Original analysis scripts
├── 03_Comparison_Analysis/             # Original comparison scripts
├── 04_Archive/                         # Experimental/deprecated utilities
│
├── Database
│   ├── nse_database.py
│   ├── nse_query_tool.py
│   └── setup_database.py
│
├── Derivatives Core
│   └── nse_fo_udiff_real_endpoint_downloader.py
│
├── Data
│   ├── NSE_August_2025_Data/
│   ├── fo_udiff_downloads/
│   └── nse_data.db
│
├── Documentation
│   ├── NSE_Download_Checklist.md
│   ├── STEP_FILE_RENAMING_PLAN.md
│   ├── README_SUPABASE.md
│   ├── SUPABASE_STEP_BY_STEP.md
│   ├── SETUP_CHECKLIST.md
│   └── API_KEY_HELP.md
│
└── Config
    ├── requirements.txt
    ├── requirements_supabase.txt
    └── .env.example
```

## 📈 Analysis Capabilities

### Market Analytics
- **Daily Top Gainers/Losers** - Identify best and worst performers
- **Volume Leaders** - Highest traded stocks by volume and value
- **High Delivery Stocks** - Stocks with high institutional interest
- **Price Range Analysis** - Support/resistance level identification

### Individual Stock Analysis
- **Complete OHLCV History** - Open, High, Low, Close, Volume data
- **Delivery Percentage Trends** - Track institutional vs retail interest
- **Performance Metrics** - Returns, volatility, and trading statistics
- **Historical Comparisons** - Compare performance across dates

### Market Overview
- **Daily Market Summary** - Overall market statistics
- **Breadth Analysis** - Advance/decline ratios
- **Turnover Analysis** - Market activity measurements
- **Sector Performance** - Industry-wise performance tracking

## 🛠️ Technical Features

### Database Schema
```sql
- Optimized SQLite schema with proper indexing
- Foreign key relationships for data integrity
- Efficient storage with appropriate data types
- Full-text search capabilities for stock symbols
```

### Performance Optimization
- **Batch Processing** - Handles large datasets efficiently
- **Memory Management** - Optimized for large CSV file processing
- **Query Optimization** - Indexed columns for fast searches
- **Connection Pooling** - Efficient database connections

### Error Handling
- **Comprehensive Logging** - Detailed error tracking and reporting
- **Graceful Degradation** - Continues operation despite individual failures
- **Data Validation** - Ensures data quality and consistency
- **Recovery Mechanisms** - Automatic retry logic for failed operations

## 📊 Sample Analysis Results

### Top Gainers (Sample)
```
Rank  Symbol      Open    Close   High    Change%  Volume
1     EXAMPLE1    245.50  267.80  270.00  +9.08%   2,456,789
2     EXAMPLE2    156.20  169.45  171.30  +8.47%   1,234,567
...
```

### High Delivery Analysis
```
Rank  Symbol      Price   Volume      Delivery    Delivery%
1     EXAMPLE1    267.80  2,456,789   2,234,567   90.95%
2     EXAMPLE2    169.45  1,234,567   1,111,234   89.99%
...
```

## 🌐 Cloud Integration

### Supabase Setup
For remote access and team collaboration:

1. **Create Supabase Account** - Free tier supports your data size
2. **Configure Database** - Run provided SQL scripts
3. **Upload Data** - Bulk upload your CSV files
4. **Start Analysis** - Access from anywhere with internet

See `SUPABASE_STEP_BY_STEP.md` for detailed setup instructions.

## 📋 Data Sources

- **NSE Official Archives** - `https://archives.nseindia.com/`
- **Complete Bhavcopy Data** - Full market data for each trading day
- **Security Deliverable Data** - Delivery percentage for all stocks
- **Trading Statistics** - Volume, turnover, and trade count data

## 🔒 Data Privacy & Security

- **Local-First Architecture** - Your data stays on your computer
- **Optional Cloud Sync** - Cloud integration is completely optional
- **No Personal Data** - Only public market data is processed
- **Secure API Keys** - Environment-based configuration for cloud services

## 🤝 Contributing

Contributions are welcome! Please feel free to submit:
- **Bug Reports** - Help improve stability
- **Feature Requests** - Suggest new analysis capabilities
- **Code Improvements** - Optimize performance and functionality
- **Documentation** - Improve guides and examples

## 📝 License

This project is open source and available under the MIT License.

## 🆘 Support

If you encounter any issues:

1. **Check Documentation** - Comprehensive guides available
2. **Review Error Messages** - Most issues have clear error descriptions
3. **Open an Issue** - Report bugs or request features
4. **Share Logs** - Include error logs for faster resolution

## 🎯 Future Enhancements

- **Real-time Data Integration** - Live market data feeds
- **Advanced Analytics** - Technical indicators and patterns
- **Portfolio Tracking** - Investment performance analysis
- **Alerts System** - Price and volume-based notifications
- **Web Dashboard** - Browser-based analysis interface
- **Mobile App** - iOS/Android market analysis
- **Derivatives DB Integration** - Unified Equity + F&O analytical layer
- **Automated UDiFF Backfill** - Historical month crawling with holiday calendar

## 📞 Contact

**GitHub:** [kiranfor2004](https://github.com/kiranfor2004)
**Project:** [NSE_Downloader](https://github.com/kiranfor2004/NSE_Downloader)

---

**⭐ Star this repository if you find it useful!**

**📈 Happy Stock Market Analysis! 🚀**
