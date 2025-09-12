# NSE Data Download Checklist (Equities & F&O UDiFF)

## Goal
1. Download NSE Equities (Securities Bhav Copy) data for all trading days January–June 2025 (Step 1–3 legacy scope)
2. (NEW Step 4) Download F&O UDiFF Common Bhavcopy Final ZIP archives (Derivatives) for target months (starting with February 2025) using the discovered merged-daily-reports API pattern.

## Months to Download
- [ ] January 2025
- [ ] February 2025
- [ ] March 2025
- [ ] April 2025
- [ ] May 2025
- [ ] June 2025

## Download Sources (Equities)
1. **Official NSE Website** (Primary)
   - URL: https://www.nseindia.com/market-data/historical-data
   - Select: Securities Bhav Copy
   - Format: CSV
2. **NSE Archives** (Alternative)
   - URL: https://archives.nseindia.com/
   - Navigate: Historical Data > Equities

## Download Steps (Equities)
1. Visit NSE historical data page
2. Select 'Securities Bhav Copy'
3. Choose date range (month by month)
4. Download CSV files
5. Save to appropriate month folder
6. Rename files to: sec_bhavdata_full_DDMMYYYY.csv

## Folder Structure
- NSE_January_2025_Data/
- NSE_February_2025_Data/
- NSE_March_2025_Data/
- NSE_April_2025_Data/
- NSE_May_2025_Data/
- NSE_June_2025_Data/

## Verification (Equities)
After downloading each month:
1. Check file count matches trading days
2. Verify each CSV has data (not empty)
3. Run: python nse_data_manager.py to validate

## Expected Files per Month (Equities)
- January: ~21-23 files
- February: ~19-20 files
- March: ~21-22 files
- April: ~21-22 files
- May: ~21-22 files
- June: ~21-22 files

---

## Step 4: F&O UDiFF Common Bhavcopy Final (Derivatives)
Purpose: Automate retrieval of derivative segment daily “UDiFF Common Bhavcopy Final” ZIP files discovered via network capture.

### Endpoint Discovery
- API queried: https://www.nseindia.com/api/merged-daily-reports?key=favDerivatives
- JSON contains item: name: "F&O - UDiFF Common Bhavcopy Final (zip)" with link pattern
- Static archive file pattern validated:  
   https://nsearchives.nseindia.com/content/fo/BhavCopy_NSE_FO_0_0_0_YYYYMMDD_F_0000.csv.zip

### Script
Use: `python nse_fo_udiff_real_endpoint_downloader.py`

### What the Script Does
1. Primes session (root + allMarketStatus) to collect cookies
2. Fetches favDerivatives list (logs & stores JSON)
3. Validates current/yesterday file exists
4. Iterates target month trading days (Mon–Fri) constructing archive URLs
5. Downloads ZIP if HTTP 200 and size > 6 KB (rudimentary integrity check)
6. Logs all events to `fo_udiff_downloads/run_log.json`

### Output Directory
`fo_udiff_downloads/` containing downloaded `BhavCopy_NSE_FO_0_0_0_YYYYMMDD_F_0000.csv.zip` files and `favDerivatives_raw.json`.

### Current Coverage
- February 2025: 19/20 trading days downloaded (2025-02-26 returned 404 – pending verification if holiday / missing)

### Next Actions (Derivatives)
- [ ] Confirm missing 2025-02-26 (holiday/calendar cross-check)
- [ ] Extend script to accept CLI args: --year --month
- [ ] Add optional retry/backoff for transient <10 KB responses
- [ ] Parse one ZIP to derive column schema for DB import
- [ ] Create PostgreSQL importer (table: fo_udiff_bhavcopy)

### Suggested Table Columns (Preliminary)
`trade_date, instrument, symbol, expiry_date, strike, option_type, open, high, low, close, settle_price, contracts, value_lakhs, open_interest, change_in_oi, implied_vol, underlying_value`

### Validation Checklist (Derivatives)
- [ ] Number of ZIPs == trading days (excluding holidays)
- [ ] Each ZIP decompresses successfully
- [ ] Row count > 0 and consistent across adjacent days
- [ ] No duplicate (trade_date, instrument, symbol, expiry_date, strike, option_type)

---

Last Updated (Step 4 addition): 2025-09-12
