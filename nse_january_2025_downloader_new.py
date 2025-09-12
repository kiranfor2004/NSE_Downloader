#!/usr/bin/env python3
"""
🚀 NSE January 2025 Data Downloader
Download NSE Equity Bhavcopy CSV files for January 2025
"""

import requests
import os
from datetime import datetime, timedelta
import calendar
import time

def download_january_2025():
    """Download NSE equity data for January 2025"""
    
    # Create download folder
    download_folder = "NSE_January_2025_Data"
    if not os.path.exists(download_folder):
        os.makedirs(download_folder)
    
    print("🚀 NSE January 2025 Data Downloader")
    print("📊 Downloading Equity Bhavcopy CSV Files")
    print("=" * 60)
    print(f"📁 Download folder: {download_folder}")
    
    # NSE headers to mimic browser request
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }
    
    # Create session for better connection handling
    session = requests.Session()
    session.headers.update(headers)
    
    # January 2025 trading days (excluding weekends)
    trading_days = []
    year = 2025
    month = 1
    
    # Get all days in January 2025
    num_days = calendar.monthrange(year, month)[1]
    for day in range(1, num_days + 1):
        date_obj = datetime(year, month, day)
        # Skip weekends (Saturday=5, Sunday=6)
        if date_obj.weekday() < 5:
            trading_days.append(date_obj)
    
    print(f"📅 Total trading days to download: {len(trading_days)}")
    print()
    
    successful_downloads = []
    failed_downloads = []
    
    print("🔄 Starting downloads...")
    print("-" * 60)
    
    for i, date_obj in enumerate(trading_days, 1):
        date_str = date_obj.strftime("%d%m%Y")
        filename = f"cm{date_str}bhav.csv"
        filepath = os.path.join(download_folder, filename)
        
        # Skip if file already exists
        if os.path.exists(filepath):
            print(f"📊 [{i:2d}/{len(trading_days)}] {date_obj.strftime('%d-%m-%Y')}")
            print(f"  ✅ File already exists: {filename}")
            successful_downloads.append(filename)
            continue
        
        print(f"📊 [{i:2d}/{len(trading_days)}] {date_obj.strftime('%d-%m-%Y')}")
        
        # Try multiple URL patterns
        base_urls = [
            f"https://archives.nseindia.com/products/content/sec_bhavdata_full_{date_str}.csv",
            f"https://www1.nseindia.com/products/content/sec_bhavdata_full_{date_str}.csv",
            f"https://archives.nseindia.com/content/historical/EQUITIES/{year}/JAN/cm{date_str}bhav.csv.zip",
            f"https://www1.nseindia.com/content/historical/EQUITIES/{year}/JAN/cm{date_str}bhav.csv.zip"
        ]
        
        downloaded = False
        
        for attempt, url in enumerate(base_urls, 1):
            try:
                print(f"  Attempt {attempt}: {url}")
                response = session.get(url, timeout=30)
                
                if response.status_code == 200 and len(response.content) > 1000:
                    # Save the file
                    with open(filepath, 'wb') as f:
                        f.write(response.content)
                    
                    print(f"    ✅ Success! Downloaded {filename} ({len(response.content)} bytes)")
                    successful_downloads.append(filename)
                    downloaded = True
                    break
                else:
                    print(f"    ❌ Status: {response.status_code}, Size: {len(response.content)}")
                    
            except requests.exceptions.RequestException as e:
                print(f"    ❌ Error: {str(e)}")
        
        if not downloaded:
            print(f"    ❌ Download failed - No data found")
            failed_downloads.append(date_obj.strftime('%d-%m-%Y'))
            # Write failed URL to log
            with open(os.path.join(download_folder, "failed_urls_012025.txt"), "a") as f:
                f.write(f"{date_obj.strftime('%d-%m-%Y')}: All attempts failed\n")
        
        print(f"    ⏳ Waiting 3 seconds...")
        time.sleep(3)  # Be respectful to NSE servers
        print()
    
    # Summary
    print("📊 Download Summary:")
    print("=" * 30)
    print(f"✅ Successful downloads: {len(successful_downloads)}")
    print(f"❌ Failed downloads: {len(failed_downloads)}")
    print(f"📁 Files saved in: {os.path.abspath(download_folder)}")
    
    if successful_downloads:
        print(f"\n📄 Downloaded files:")
        for file in successful_downloads:
            print(f"   {file}")
    
    if failed_downloads:
        print(f"\n❌ Failed dates:")
        for date in failed_downloads:
            print(f"   {date}")
    
    # Create README
    readme_content = f"""# NSE January 2025 Equity Data

## Download Summary
- **Total Files**: {len(successful_downloads)}
- **Failed Downloads**: {len(failed_downloads)}
- **Download Date**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## File Format
- **File Pattern**: cm{'{date}'}bhav.csv
- **Date Format**: DDMMYYYY
- **Content**: NSE Equity Bhavcopy data

## Data Fields
Each CSV file contains stock trading data with fields like:
- SYMBOL, SERIES, OPEN, HIGH, LOW, CLOSE, LAST, PREVCLOSE
- TOTTRDQTY, TOTTRDVAL, TIMESTAMP, TOTALTRADES
- ISIN

## Usage
These files can be imported into databases or analysis tools for:
- Stock price analysis
- Volume analysis  
- Market trend studies
- Historical backtesting
"""
    
    with open(os.path.join(download_folder, "README.md"), "w") as f:
        f.write(readme_content)
    
    print(f"\n🎉 January 2025 download completed!")
    return len(successful_downloads), len(failed_downloads)

if __name__ == "__main__":
    download_january_2025()
