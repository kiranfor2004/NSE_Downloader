#!/usr/bin/env python3
"""
🎯 NSE F&O Single Day Downloader
A targeted script to find and download F&O data for a specific day.
"""

import requests
import os
from datetime import datetime
import time
import urllib3

# Disable InsecureRequestWarning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def download_single_day_fo(year, month, day, download_folder="downloads"):
    """
    Tries multiple URL patterns to download NSE F&O UDIFF Bhavcopy for a single day.
    
    Args:
        year (int): Year (e.g., 2025)
        month (int): Month (1-12)
        day (int): Day of the month
        download_folder (str): Folder to save the downloaded file
    """
    
    # Create download folder if it doesn't exist
    if not os.path.exists(download_folder):
        os.makedirs(download_folder)
        print(f"📁 Created download folder: {download_folder}")
    
    # --- Date Formatting ---
    date_obj = datetime(year, month, day)
    month_abbr = date_obj.strftime('%b').upper()  # e.g., FEB
    month_name = date_obj.strftime('%B').upper() # e.g., FEBRUARY
    day_str = f"{day:02d}" # e.g., 03
    
    # --- URL and Filename Patterns ---
    # This is a comprehensive list of patterns NSE has used over the years.
    url_templates = [
        # Current primary archive URL
        "https://archives.nseindia.com/content/historical/DERIVATIVES/{year}/{month_abbr}/fo{day_str}{month_abbr}{year}bhav.csv.zip",
        # Older www1 URL (often has SSL issues but worth trying)
        "https://www1.nseindia.com/content/historical/DERIVATIVES/{year}/{month_abbr}/fo{day_str}{month_abbr}{year}bhav.csv.zip",
        # UDIFF specific patterns
        "https://archives.nseindia.com/products/content/derivatives/equities/udiff_{day_str}{month:02d}{year}.zip",
        "https://archives.nseindia.com/content/historical/DERIVATIVES/{year}/{month_abbr}/udiff{day_str}{month_abbr}{year}.zip",
        # Alternative naming conventions
        "https://archives.nseindia.com/content/historical/DERIVATIVES/{year}/{month_abbr}/cm{day_str}{month_abbr}{year}bhav.csv.zip",
    ]

    print(f"🎯 Attempting to download F&O data for: {date_obj.strftime('%d-%b-%Y')}")
    print("-" * 60)

    # --- Request Setup ---
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.5',
    }
    
    for i, template in enumerate(url_templates):
        url = template.format(year=year, month=month, day_str=day_str, month_abbr=month_abbr, month_name=month_name)
        filename = os.path.basename(url)
        filepath = os.path.join(download_folder, filename)
        
        print(f"  Attempt {i+1}/{len(url_templates)}: Trying URL -> {url}")
        
        try:
            # Make request with timeout and disabled SSL verification for older domains
            response = requests.get(url, headers=headers, timeout=15, verify=False)
            
            # --- Check Response ---
            if response.status_code == 200 and len(response.content) > 1000: # Check for valid file size
                with open(filepath, 'wb') as f:
                    f.write(response.content)
                
                print(f"\n🎉 SUCCESS! Downloaded: {filename} ({len(response.content)} bytes)")
                print(f"   Saved to: {os.path.abspath(filepath)}")
                print("-" * 60)
                return

            else:
                print(f"    ❌ FAILED: Status {response.status_code}, Size: {len(response.content)} bytes")

        except requests.exceptions.RequestException as e:
            print(f"    ❌ ERROR: {e.__class__.__name__}")
        
        time.sleep(1) # Small delay between attempts

    print(f"\n❌ All attempts failed for {date_obj.strftime('%d-%b-%Y')}.")
    print("   This could mean the data for this specific day is not available or the URL structure has changed again.")
    print("-" * 60)


if __name__ == "__main__":
    # --- Test with a recent date to confirm downloader logic ---
    print("--- DIAGNOSTIC TEST ---")
    print("First, attempting to download data for a recent date (Aug 29, 2025) to verify the script.")
    download_single_day_fo(2025, 8, 29, download_folder="NSE_FO_Downloads")
    
    # --- Retry the user's requested date ---
    print("\n--- USER REQUEST ---")
    print("Now, re-attempting the download for the originally requested date (Feb 03, 2025).")
    download_single_day_fo(2025, 2, 3, download_folder="NSE_FO_Downloads")
