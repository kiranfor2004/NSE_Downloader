# NSE Data Download Checklist - January to June 2025

## Goal
Download NSE Bhav Copy data for all trading days from January-June 2025

## Months to Download
- [ ] January 2025
- [ ] February 2025
- [ ] March 2025
- [ ] April 2025
- [ ] May 2025
- [ ] June 2025

## Download Sources
1. **Official NSE Website** (Primary)
   - URL: https://www.nseindia.com/market-data/historical-data
   - Select: Securities Bhav Copy
   - Format: CSV

2. **NSE Archives** (Alternative)
   - URL: https://archives.nseindia.com/
   - Navigate: Historical Data > Equities

## Download Steps
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

## Verification
After downloading each month:
1. Check file count matches trading days
2. Verify each CSV has data (not empty)
3. Run: python nse_data_manager.py to validate

## Expected Files per Month
- January: ~21-23 files
- February: ~19-20 files
- March: ~21-22 files
- April: ~21-22 files
- May: ~21-22 files
- June: ~21-22 files
