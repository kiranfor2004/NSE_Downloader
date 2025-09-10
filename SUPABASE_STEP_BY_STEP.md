# 🚀 Supabase Setup - Step by Step Guide

**Goal:** Move your NSE data (55,780 records) to cloud database for better analysis

---

## 📋 **STEP 1: Create Supabase Account (2 minutes)**

### 1.1 Go to Supabase Website
- Open browser → Go to **https://supabase.com**
- Click **"Start your project"** (green button)

### 1.2 Sign Up
Choose one option:
- ✅ **GitHub** (recommended - fastest)
- ✅ **Google Account** 
- ✅ **Email + Password**

### 1.3 Verify Email
- Check your email inbox
- Click verification link (if using email signup)

---

## 🏗️ **STEP 2: Create New Project (3 minutes)**

### 2.1 Create Project
- After login, click **"New project"**
- Choose your organization (usually your username)

### 2.2 Project Settings
```
Project Name: NSE-Stock-Analysis
Database Password: [Create strong password - SAVE IT!]
Region: [Choose closest to you - e.g., Asia Pacific (Mumbai) for India]
Pricing Plan: Free
```

### 2.3 Create & Wait
- Click **"Create new project"**  
- ⏳ Wait 2-3 minutes (you'll see progress indicators)
- ☕ Get some coffee while it sets up!

---

## 🔑 **STEP 3: Get API Credentials (30 seconds)**

### 3.1 Navigate to API Settings
- In your project dashboard (left sidebar)
- Click **⚙️ Settings** 
- Click **📡 API**

### 3.2 Copy These Two Values
You'll see:

**Project URL** (looks like):
```
https://abcdefghijklmnop.supabase.co
```
📋 **Copy this** - you'll need it

**anon/public Key** (long text starting with `eyJ`):
```
eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFz...
```
📋 **Copy this too** - you'll need it

### 3.3 Keep These Safe
- Write them down or save in notepad
- Don't close this browser tab yet!

---

## 🗃️ **STEP 4: Create Database Tables (2 minutes)**

### 4.1 Open SQL Editor  
- In left sidebar, click **🔧 SQL Editor**
- Click **"New query"**

### 4.2 Copy & Paste This SQL Code
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
    
    -- Prevent duplicate records
    UNIQUE(symbol, date, series)
);

-- Create indexes for faster queries
CREATE INDEX IF NOT EXISTS idx_nse_symbol ON nse_stock_data(symbol);
CREATE INDEX IF NOT EXISTS idx_nse_date ON nse_stock_data(date);
CREATE INDEX IF NOT EXISTS idx_nse_symbol_date ON nse_stock_data(symbol, date);
CREATE INDEX IF NOT EXISTS idx_nse_deliverable_pct ON nse_stock_data(deliverable_percentage);
CREATE INDEX IF NOT EXISTS idx_nse_turnover ON nse_stock_data(turnover);

-- Upload tracking table
CREATE TABLE IF NOT EXISTS upload_status (
    id BIGSERIAL PRIMARY KEY,
    filename VARCHAR(100) NOT NULL UNIQUE,
    upload_date TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    records_count INTEGER,
    file_size_kb INTEGER,
    status VARCHAR(20) DEFAULT 'completed',
    error_message TEXT
);

-- Enable security (optional)
ALTER TABLE nse_stock_data ENABLE ROW LEVEL SECURITY;
ALTER TABLE upload_status ENABLE ROW LEVEL SECURITY;

-- Allow public read access
CREATE POLICY "Allow public read access" ON nse_stock_data FOR SELECT USING (true);
CREATE POLICY "Allow public read access" ON upload_status FOR SELECT USING (true);
```

### 4.3 Run the SQL
- Click **▶️ "Run"** button (bottom right)
- You should see ✅ "Success. No rows returned"
- If you see errors, copy the error message and ask for help

---

## 💻 **STEP 5: Configure Your Local Computer (1 minute)**

### 5.1 Create .env File
In your `NSE_Downloader` folder, create a new file called `.env` (note the dot at the beginning)

### 5.2 Add Your Credentials
Open the `.env` file and paste:

```env
# Replace these with YOUR actual values from Step 3
SUPABASE_URL=https://abcdefghijklmnop.supabase.co
SUPABASE_ANON_KEY=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFz...

# Example - DO NOT USE THESE VALUES:
# SUPABASE_URL=https://your-project-ref.supabase.co
# SUPABASE_ANON_KEY=your_long_api_key_starting_with_eyJ
```

⚠️ **Important:** Replace with YOUR actual URL and key from Step 3!

### 5.3 Save the File
- Save the `.env` file
- Make sure it's in the same folder as your Python scripts

---

## 📤 **STEP 6: Upload Your Data (3-5 minutes)**

### 6.1 Run the Upload Script
Open Command Prompt/PowerShell in your NSE_Downloader folder and run:

```bash
python supabase_nse_uploader.py
```

### 6.2 Follow the Menu
You'll see options:
1. Choose **"1"** first time (Create database tables) - SKIP if you did Step 4
2. Choose **"2"** (Upload all CSV files)

### 6.3 Watch the Progress
You'll see something like:
```
📤 Uploading sec_bhavdata_full_01082025.csv...
   📊 Found 2908 records
   ⏳ Progress: 100.0% (2908/2908)
   ✅ Uploaded 2908/2908 records
```

This will repeat for all 19 CSV files.

### 6.4 Completion
When done, you should see:
```
📊 UPLOAD SUMMARY
✅ Successful uploads: 19/19
❌ Failed uploads: 0/19
```

---

## 📊 **STEP 7: Test Your Data (30 seconds)**

### 7.1 Check in Supabase Dashboard
- Go back to your Supabase project
- Click **🗃️ Table Editor** (left sidebar)  
- You should see table: `nse_stock_data`
- Click on it to see your data!

### 7.2 Run Analysis Tool
```bash
python supabase_analysis_tool.py
```

Choose option **"3"** (Check database status) to verify everything works.

---

## ✅ **Verification Checklist**

Make sure you have:
- [ ] ✅ Supabase account created
- [ ] ✅ Project created (with name like `NSE-Stock-Analysis`)
- [ ] ✅ API credentials copied (URL + Key)
- [ ] ✅ SQL tables created (no errors when running SQL)
- [ ] ✅ `.env` file created with YOUR credentials
- [ ] ✅ Data uploaded successfully (19/19 files)
- [ ] ✅ Can see data in Supabase Table Editor
- [ ] ✅ Analysis tool connects successfully

---

## 🚨 **Common Issues & Solutions**

### Issue 1: "Can't connect to Supabase"
**Solution:** Check your `.env` file:
- Make sure URL starts with `https://`
- Make sure API key starts with `eyJ`
- No spaces around the `=` sign

### Issue 2: "Table does not exist"  
**Solution:** Run the SQL commands again in Step 4

### Issue 3: "Upload failed"
**Solution:** 
- Check internet connection
- Make sure tables were created (Step 4)
- Try uploading one file first

### Issue 4: ".env file not found"
**Solution:** 
- Make sure `.env` file is in the same folder as your Python scripts
- File name must be exactly `.env` (with the dot)

---

## 🎉 **What's Next?**

Once everything is working:

1. **Analyze Your Data:**
   ```bash
   python supabase_analysis_tool.py
   ```

2. **Available Analysis:**
   - Top gainers/losers
   - High delivery stocks  
   - Volume leaders
   - Individual stock analysis
   - Market summaries

3. **Export Results:**
   - All analysis can be exported to CSV
   - Share insights with others
   - Use in Excel or other tools

**Your 55,780 NSE records are now in the cloud! 🚀**

---

## 🆘 **Need Help?**

If you get stuck on any step:
1. Copy the exact error message
2. Tell me which step you're on
3. Show me what you see vs. what you expected

**I'm here to help you get this working! 💪**
