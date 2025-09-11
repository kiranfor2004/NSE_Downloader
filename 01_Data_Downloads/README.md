# 📥 Data Downloads Folder

## Purpose
This folder contains all NSE data downloader scripts and manages raw data collection.

## Scripts Overview

### Monthly Downloaders
- `nse_january_2025_downloader.py` - January 2025 data
- `nse_february_2025_downloader.py` - February 2025 data  
- `nse_march_2025_downloader.py` - March 2025 data
- `nse_april_2025_downloader.py` - April 2025 data
- `nse_may_2025_downloader.py` - May 2025 data (⚡ Latest)
- `nse_june_2025_downloader.py` - June 2025 data
- `nse_july_2025_downloader.py` - July 2025 data

### Special Downloaders
- `nse_jan_june_2025_downloader.py` - Bulk download Jan-June
- `nse_smart_downloader.py` - Intelligent download manager

## Data Coverage
- **Months**: 8 months (January - August 2025)
- **Files**: 164 CSV files total
- **Records**: 460,000+ trading records
- **Format**: NSE Bhavcopy and Security Deliverable data

## Raw Data Locations
Data is stored in parallel folders:
- `../NSE_January_2025_Data/`
- `../NSE_February_2025_Data/`
- `../NSE_March_2025_Data/`
- `../NSE_April_2025_Data/`
- `../NSE_May_2025_Data/`
- `../NSE_June_2025_Data/`
- `../NSE_July_2025_Data/`
- `../NSE_August_2025_Data/`

## Usage
Run any monthly downloader to collect NSE data:
```bash
python nse_may_2025_downloader.py
```

## File Naming Convention
- Input: `sec_bhavdata_full_*.csv` (July-Aug) or `cm*.csv` (Jan-June)
- Output: Organized by month in separate folders
- Filter: Only SERIES='EQ' (Equity) data processed

*Last Updated: September 11, 2025*
