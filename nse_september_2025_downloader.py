#!/usr/bin/env python3
"""
🚀 NSE September 2025 Data Downloader (Step 1)
Download NSE Equity Bhavcopy data for September 2025
"""

import requests
import os
from datetime import datetime, timedelta
import calendar
import time

def download_nse_september_2025():
    """Download NSE Equity data for September 2025"""
    
    # Target month
    year = 2025
    month = 9  # September
    
    # Create download folder
    download_folder = "NSE_September_2025_Data"
    if not os.path.exists(download_folder):
        os.makedirs(download_folder)
        print(f"📁 Created folder: {download_folder}")
    
    # Print header
    print("🚀 NSE September 2025 Data Downloader")
    print("📊 Equity Bhavcopy (Equity Daily Reports)")
    print("=" * 60)
    print(f"📁 Download folder: {download_folder}")
    
    # Headers to mimic browser
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }
    
    # Get number of days in September 2025
    num_days = calendar.monthrange(year, month)[1]
    
    # Calculate trading days (exclude weekends)
    trading_days = []
    for day in range(1, num_days + 1):
        date_obj = datetime(year, month, day)
        if date_obj.weekday() < 5:  # Monday=0 to Friday=4
            trading_days.append(date_obj)
    
    print(f"📅 Total trading days to download: {len(trading_days)}")
    print()
    
    # Download counters
    successful_downloads = 0
    failed_downloads = 0
    
    print("🔄 Starting downloads...")
    print("-" * 60)
    
    for i, date_obj in enumerate(trading_days):
        day = date_obj.day
        date_str = date_obj.strftime("%d%m%Y")  # Format: DDMMYYYY
        
        print(f"\n📊 [{i+1:2d}/{len(trading_days)}] {date_obj.strftime('%d-%m-%Y')}")
        
        # File patterns that NSE uses for equity data
        file_patterns = [
            f"sec_bhavdata_full_{date_str}.csv",  # Current format used in Aug 2025
            f"cm{date_str}bhav.csv",             # Alternative format used in some months
            f"Eq{date_str}_CSV.zip",             # Zipped format
            f"sec_bhavdata_{date_str}.csv"       # Alternative naming
        ]
        
        # URL patterns where NSE hosts the files
        base_urls = [
            "https://archives.nseindia.com/products/content/sec_bhavdata/VIX/",
            "https://nsearchives.nseindia.com/products/content/sec_bhavdata/VIX/",
            "https://www1.nseindia.com/products/content/sec_bhavdata/VIX/",
            "https://archives.nseindia.com/content/historical/EQUITIES/2025/SEP/",
            "https://nsearchives.nseindia.com/content/historical/EQUITIES/2025/SEP/",
            "https://www1.nseindia.com/content/historical/EQUITIES/2025/SEP/"
        ]
        
        downloaded = False
        attempt = 0
        
        # Try different combinations
        for base_url in base_urls:
            for filename in file_patterns:
                if downloaded:
                    break
                    
                attempt += 1
                url = base_url + filename
                filepath = os.path.join(download_folder, filename)
                
                print(f"  Attempt {attempt}: {url}")
                
                try:
                    response = requests.get(url, headers=headers, timeout=15)
                    
                    if response.status_code == 200 and len(response.content) > 10000:
                        # Save file
                        with open(filepath, 'wb') as f:
                            f.write(response.content)
                        
                        file_size = len(response.content)
                        print(f"    ✅ Success! Downloaded {filename} ({file_size:,} bytes)")
                        successful_downloads += 1
                        downloaded = True
                        break
                    else:
                        print(f"    ❌ Status: {response.status_code}, Size: {len(response.content)}")
                        
                except requests.exceptions.RequestException as e:
                    print(f"    ❌ Error: {e.__class__.__name__}")
                
                time.sleep(0.5)  # Small delay between attempts
        
        if not downloaded:
            print(f"    ❌ Download failed - No data found")
            failed_downloads += 1
            
        # Longer delay between dates to be respectful to NSE servers
        if i < len(trading_days) - 1:
            print("    ⏳ Waiting 3 seconds...")
            time.sleep(3)
    
    # Final summary
    print("\n" + "=" * 60)
    print("📊 Download Summary:")
    print(f"✅ Successful downloads: {successful_downloads}")
    print(f"❌ Failed downloads: {failed_downloads}")
    print(f"📁 Files saved in: {os.path.abspath(download_folder)}")
    
    if successful_downloads > 0:
        print(f"\n🎉 Successfully downloaded {successful_downloads} files!")
        print("✅ You can now proceed to import this data into your database")
    else:
        print("\n⚠️  No files were downloaded. This might indicate:")
        print("   1. NSE has changed their URL structure")
        print("   2. September 2025 data is not yet available")
        print("   3. Network connectivity issues")

if __name__ == "__main__":
    download_nse_september_2025()
