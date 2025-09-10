# 🚀 NSE Supabase Cloud Database Setup

Transform your NSE stock data into a powerful cloud-based analytics platform using Supabase!

## 🌟 Why Supabase for NSE Data?

✅ **Free cloud database** - No server maintenance  
✅ **Remote access** - Query from anywhere  
✅ **Real-time updates** - Instant data synchronization  
✅ **Powerful queries** - SQL-based analysis  
✅ **Scalable** - Handles large datasets efficiently  
✅ **Secure** - Built-in authentication and security  

## 📊 Your Current Data

- **Total Records:** 55,780 stock entries
- **Unique Stocks:** 3,121 companies  
- **Trading Days:** 19 days (August 2025)
- **Date Range:** Aug 1, 2025 to Aug 29, 2025
- **Local Database:** 13.4 MB

## 🚀 Quick Setup (5 minutes)

### Step 1: Create Supabase Account

1. Go to **[supabase.com](https://supabase.com)**
2. Click **"Start your project"**
3. Sign up with GitHub/Google/Email
4. Choose **Free Plan** (perfect for your data size)

### Step 2: Create Project

1. Click **"New Project"**
2. Name: `NSE-Stock-Analysis` 
3. Set database password (save it!)
4. Choose region closest to you
5. Click **"Create new project"**
6. ⏳ Wait 2-3 minutes for setup

### Step 3: Get API Credentials

1. Go to **Settings > API** (left sidebar)
2. Copy **Project URL** (e.g., `https://abcdef.supabase.co`)
3. Copy **anon public key** (long JWT token)

### Step 4: Setup Database Tables

1. Go to **SQL Editor** (left sidebar)
2. Click **"New query"**
3. Copy and paste this SQL:

```sql
-- NSE Stock Data Table
CREATE TABLE IF NOT EXISTS nse_stock_data (
    id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(50) NOT NULL,
    series VARCHAR(10),
    date DATE NOT NULL,
    prev_close DECIMAL(12,2),
    open_price DECIMAL(12,2),
    high_price DECIMAL(12,2),
    low_price DECIMAL(12,2),
    last_price DECIMAL(12,2),
    close_price DECIMAL(12,2),
    avg_price DECIMAL(12,2),
    total_traded_qty BIGINT,
    turnover DECIMAL(15,2),
    no_of_trades INTEGER,
    deliverable_qty BIGINT,
    deliverable_percentage DECIMAL(8,4),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Prevent duplicates
    UNIQUE(symbol, date, series)
);

-- Performance Indexes
CREATE INDEX IF NOT EXISTS idx_nse_symbol ON nse_stock_data(symbol);
CREATE INDEX IF NOT EXISTS idx_nse_date ON nse_stock_data(date);
CREATE INDEX IF NOT EXISTS idx_nse_symbol_date ON nse_stock_data(symbol, date);
CREATE INDEX IF NOT EXISTS idx_nse_deliverable_pct ON nse_stock_data(deliverable_percentage);
CREATE INDEX IF NOT EXISTS idx_nse_turnover ON nse_stock_data(turnover);

-- Upload Status Tracking
CREATE TABLE IF NOT EXISTS upload_status (
    id BIGSERIAL PRIMARY KEY,
    filename VARCHAR(100) NOT NULL UNIQUE,
    upload_date TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    records_count INTEGER,
    file_size_kb INTEGER,
    status VARCHAR(20) DEFAULT 'completed',
    error_message TEXT
);

-- Enable Row Level Security (optional)
ALTER TABLE nse_stock_data ENABLE ROW LEVEL SECURITY;
ALTER TABLE upload_status ENABLE ROW LEVEL SECURITY;

-- Public access policies (adjust as needed)
CREATE POLICY "Allow public read access" ON nse_stock_data FOR SELECT USING (true);
CREATE POLICY "Allow public read access" ON upload_status FOR SELECT USING (true);
```

4. Click **"Run"** to create tables

### Step 5: Configure Local Environment

1. Create `.env` file in your NSE_Downloader folder:

```env
# Supabase Configuration
SUPABASE_URL=your_project_url_here
SUPABASE_ANON_KEY=your_anon_key_here

# Example:
# SUPABASE_URL=https://abcdefghijk.supabase.co
# SUPABASE_ANON_KEY=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
```

2. Replace with your actual credentials from Step 3

## 📤 Upload Your Data

Run the upload script:

```bash
python supabase_nse_uploader.py
```

This will:
- ✅ Upload all 55,780 records to cloud
- ✅ Handle data validation and cleaning  
- ✅ Track upload progress and status
- ✅ Skip duplicates automatically

Expected upload time: **3-5 minutes**

## 📊 Start Analyzing

Run the analysis tool:

```bash
python supabase_analysis_tool.py
```

### Available Analysis:

🚀 **Market Movers**
- Top gainers/losers by date
- Volume leaders analysis
- Price range analysis

📦 **Delivery Analysis**  
- High delivery percentage stocks
- Delivery vs. price correlation
- Institutional vs. retail interest

🔍 **Individual Stock Analysis**
- Complete price history
- Volume trends
- Performance metrics

🌍 **Market Overview**
- Daily market summary
- Sectoral performance
- Market breadth analysis

## 🎯 Sample Queries You Can Run

### Find Top Gainers for Latest Date
```python
analyzer.get_top_gainers(limit=10)
```

### Analyze Specific Stock (e.g., RELIANCE)
```python
analyzer.analyze_stock("RELIANCE")
```

### High Delivery Stocks (≥80%)
```python
analyzer.get_high_delivery_stocks(min_delivery_percent=80)
```

### Market Summary
```python
analyzer.get_market_summary()
```

## 💡 Advanced Features

### 📊 Data Export
- Export analysis results to CSV
- Share reports with team members
- Integration with Excel/Google Sheets

### 🔄 Real-time Updates
- Add new data as it becomes available  
- Automatic deduplication
- Historical data preservation

### 🌐 Remote Access
- Query from any device
- Share database access with team
- API access for custom applications

## 🛠️ File Structure

```
NSE_Downloader/
├── 📊 Data Files
│   ├── nse_data.db (Local SQLite - 13.4 MB)
│   └── NSE_August_2025_Data/ (19 CSV files)
│
├── ☁️  Supabase Integration  
│   ├── supabase_nse_uploader.py (Upload to cloud)
│   ├── supabase_analysis_tool.py (Cloud analysis)
│   ├── supabase_setup.py (Setup helper)
│   └── .env (Your credentials)
│
├── 🗃️ Local Database (Backup)
│   ├── nse_database.py (Local SQLite manager)
│   ├── nse_query_tool.py (Local analysis)
│   └── setup_database.py (Local setup)
│
└── 📋 Configuration
    ├── requirements_supabase.txt (Cloud packages)
    └── README_SUPABASE.md (This guide)
```

## 🚨 Troubleshooting

### Connection Issues
```bash
# Test connection
python -c "from supabase_nse_uploader import NSESupabaseManager; NSESupabaseManager()"
```

### Upload Errors
- Check internet connection
- Verify Supabase credentials in `.env`
- Ensure tables are created in Supabase
- Check for API rate limits

### Query Issues  
- Verify data was uploaded successfully
- Check table names and column names
- Test with simple queries first

## 📞 Need Help?

1. **Check Supabase Dashboard** - View your data directly
2. **Review Error Messages** - Scripts provide detailed error info
3. **Test with Small Dataset** - Upload one CSV file first
4. **Check Internet Connection** - Required for cloud access

## 🎉 Next Steps

Once your data is in Supabase:

1. **🔗 Share Access** - Invite team members to your project
2. **📱 Build Dashboards** - Use Supabase dashboard features  
3. **🤖 API Integration** - Connect to other applications
4. **📈 Advanced Analytics** - Use SQL for complex queries
5. **⚡ Real-time Updates** - Set up automatic data refreshes

## 💰 Pricing (Free Tier Limits)

Your NSE data easily fits within Supabase free tier:

- ✅ **Database:** Up to 500MB (you need ~14MB)
- ✅ **API Requests:** 50,000/month (plenty for analysis)
- ✅ **Storage:** 1GB (more than enough)
- ✅ **Bandwidth:** 2GB/month (sufficient for regular use)

**Result:** Your NSE analysis is 100% free! 🎉

---

## 🚀 Ready to Get Started?

1. **Create Supabase account** → [supabase.com](https://supabase.com)
2. **Follow setup steps** above (5 minutes)  
3. **Run upload script** → Upload your 55K+ records
4. **Start analyzing** → Powerful cloud-based insights!

**Your stock market data deserves a cloud home! ☁️📈**
