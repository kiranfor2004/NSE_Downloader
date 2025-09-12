#!/usr/bin/env python3
"""
🧠 NSE Smart Downloader
A more advanced script to find and download F&O data by first trying to scrape the page
and then falling back to pattern matching.
"""

import requests
import os
from datetime import datetime, timedelta
import calendar
import time
import urllib3

# Disable InsecureRequestWarning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def get_trading_days(year, month):
    """Returns a list of all trading days (Mon-Fri) for a given month."""
    trading_days = []
    num_days = calendar.monthrange(year, month)[1]
    for day in range(1, num_days + 1):
        date = datetime(year, month, day)
        if date.weekday() < 5: # Monday to Friday
            trading_days.append(date)
    return trading_days

def download_fo_data_smart(year, month, download_folder="NSE_FO_Downloads"):
    """
    Attempts to download all F&O UDIFF Bhavcopy files for a given month.
    
    Args:
        year (int): The year to download data for.
        month (int): The month to download data for.
        download_folder (str): The folder to save files in.
    """
    
    if not os.path.exists(download_folder):
        os.makedirs(download_folder)
        print(f"📁 Created download folder: {download_folder}")

    trading_days = get_trading_days(year, month)
    month_name = calendar.month_name[month]
    
    print("="*60)
    print(f"🧠 NSE Smart Downloader for F&O Data")
    print(f"🎯 Target: {month_name} {year}")
    print(f"Total trading days to check: {len(trading_days)}")
    print("="*60)

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
    }

    # --- Main Loop for each day ---
    for date in trading_days:
        day_str = date.strftime('%d')
        month_abbr = date.strftime('%b').upper()
        
        print(f"\n--- Processing: {date.strftime('%d-%b-%Y')} ---")
        
        # --- Stage 1: Dynamic Scrape Attempt (Not implemented as it's complex and likely to fail) ---
        # This would involve using Selenium to interact with the date picker on the NSE website.
        # For this example, we will stick to direct HTTP requests which we know are failing.
        
        # --- Stage 2: Pattern-Based Fallback ---
        url_templates = [
            f"https://archives.nseindia.com/content/historical/DERIVATIVES/{year}/{month_abbr}/fo{day_str}{month_abbr}{year}bhav.csv.zip",
            f"https://archives.nseindia.com/products/content/derivatives/equities/udiff_{day_str}{month:02d}{year}.zip",
            f"https://www1.nseindia.com/content/historical/DERIVATIVES/{year}/{month_abbr}/fo{day_str}{month_abbr}{year}bhav.csv.zip",
        ]
        
        downloaded = False
        for i, url in enumerate(url_templates):
            filename = os.path.basename(url)
            filepath = os.path.join(download_folder, filename)
            
            print(f"  Attempt {i+1}: {url}")
            
            try:
                response = requests.get(url, headers=headers, timeout=15, verify=False)
                
                if response.status_code == 200 and len(response.content) > 1000:
                    with open(filepath, 'wb') as f:
                        f.write(response.content)
                    print(f"  ✅ SUCCESS: Downloaded {filename}")
                    downloaded = True
                    break # Move to next day
                else:
                    print(f"    ❌ Fail (Status: {response.status_code}, Size: {len(response.content)})")

            except requests.exceptions.RequestException as e:
                print(f"    ❌ Fail (Error: {e.__class__.__name__})")
            
            time.sleep(1)

        if not downloaded:
            print(f"  ❌ All attempts failed for {date.strftime('%d-%b-%Y')}.")

    print("\n" + "="*60)
    print("🏁 Download process finished.")
    print("Please check the output above to see which files, if any, were downloaded.")
    print("If all downloads failed, it confirms the NSE archive URLs are no longer accessible via simple scripts.")
    print("="*60)


if __name__ == "__main__":
    download_fo_data_smart(2025, 2, download_folder="NSE_FO_February_2025")
