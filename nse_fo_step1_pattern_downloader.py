#!/usr/bin/env python3
"""
🚀 NSE F&O February 2025 Downloader (Step 1 Pattern)
Using the SUCCESSFUL Step 1 URL pattern for F&O data
"""

import requests
import os
from datetime import datetime
import calendar
import time

def download_fo_february_2025_step1_pattern():
    """Download F&O data using Step 1's successful URL pattern"""
    
    # Create download folder
    download_folder = "NSE_FO_February_2025_Step1_Pattern"
    if not os.path.exists(download_folder):
        os.makedirs(download_folder)
    
    print("🚀 NSE F&O February 2025 Downloader (Step 1 Pattern)")
    print("📊 Using SUCCESSFUL Step 1 URL Structure")
    print("=" * 60)
    print(f"📁 Download folder: {download_folder}")
    
    # Headers matching Step 1
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }
    
    session = requests.Session()
    session.headers.update(headers)
    
    # February 2025 trading days
    trading_days = []
    year = 2025
    month = 2
    
    num_days = calendar.monthrange(year, month)[1]
    for day in range(1, num_days + 1):
        date_obj = datetime(year, month, day)
        if date_obj.weekday() < 5:  # Skip weekends
            trading_days.append(date_obj)
    
    print(f"📅 Total trading days to download: {len(trading_days)}")
    print()
    
    successful_downloads = []
    failed_downloads = []
    
    print("🔄 Starting downloads using Step 1 pattern...")
    print("-" * 60)
    
    for i, date_obj in enumerate(trading_days, 1):
        date_str = date_obj.strftime("%d%m%Y")
        print(f"📊 [{i:2d}/{len(trading_days)}] {date_obj.strftime('%d-%m-%Y')}")
        
        # Try F&O URLs following Step 1's SUCCESSFUL pattern
        test_urls = [
            # Pattern 1: Direct F&O file like equity (most likely to work)
            f"https://archives.nseindia.com/products/content/fo_bhavdata_full_{date_str}.csv",
            f"https://archives.nseindia.com/products/content/derivatives_bhavdata_full_{date_str}.csv",
            f"https://archives.nseindia.com/products/content/fo_udiff_{date_str}.csv",
            f"https://archives.nseindia.com/products/content/derivatives_udiff_{date_str}.csv",
            
            # Pattern 2: F&O with ZIP like equity pattern
            f"https://archives.nseindia.com/products/content/fo_bhavdata_full_{date_str}.zip",
            f"https://archives.nseindia.com/products/content/derivatives_bhavdata_full_{date_str}.zip",
            
            # Pattern 3: Different naming but same path structure
            f"https://archives.nseindia.com/products/content/fo{date_str}bhav.csv",
            f"https://archives.nseindia.com/products/content/udiff{date_str}.csv",
        ]
        
        downloaded = False
        
        for attempt, url in enumerate(test_urls, 1):
            filename = os.path.basename(url)
            filepath = os.path.join(download_folder, filename)
            
            try:
                print(f"  Attempt {attempt}: {url}")
                response = session.get(url, timeout=30)
                
                if response.status_code == 200 and len(response.content) > 1000:
                    with open(filepath, 'wb') as f:
                        f.write(response.content)
                    
                    print(f"    ✅ SUCCESS! Downloaded {filename} ({len(response.content)} bytes)")
                    successful_downloads.append(filename)
                    downloaded = True
                    break
                else:
                    print(f"    ❌ Status: {response.status_code}, Size: {len(response.content)}")
                    
            except requests.exceptions.RequestException as e:
                print(f"    ❌ Error: {str(e)}")
        
        if not downloaded:
            print(f"    ❌ All patterns failed for {date_obj.strftime('%d-%m-%Y')}")
            failed_downloads.append(date_obj.strftime('%d-%m-%Y'))
        
        print(f"    ⏳ Waiting 2 seconds...")
        time.sleep(2)
        print()
    
    # Summary
    print("📊 Download Summary:")
    print("=" * 30)
    print(f"✅ Successful downloads: {len(successful_downloads)}")
    print(f"❌ Failed downloads: {len(failed_downloads)}")
    print(f"📁 Files saved in: {os.path.abspath(download_folder)}")
    
    if successful_downloads:
        print(f"\n📄 Successfully downloaded:")
        for file in successful_downloads:
            print(f"   {file}")
        
        print(f"\n🎉 SUCCESS! Found working F&O URL pattern!")
        print(f"💡 The successful pattern can be used for other months")
    else:
        print(f"\n❌ No files downloaded - F&O data may not follow equity pattern")
        print(f"💡 NSE might use completely different structure for F&O data")
    
    if failed_downloads:
        print(f"\n❌ Failed dates:")
        for date in failed_downloads:
            print(f"   {date}")

if __name__ == "__main__":
    download_fo_february_2025_step1_pattern()
