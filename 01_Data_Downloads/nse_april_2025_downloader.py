#!/usr/bin/env python3
"""
NSE April 2025 Data Downloader
Downloads daily NSE equity data for April 2025 trading days.
"""

import os
import requests
from datetime import datetime, timedelta
import pandas as pd

def get_april_2025_trading_days():
    """Get all trading days for April 2025 (excluding weekends and holidays)"""
    trading_days = []
    
    # April 2025 trading days (excluding weekends and major holidays)
    april_dates = [
        "01042025",  # Apr 1
        "02042025",  # Apr 2
        "03042025",  # Apr 3
        "04042025",  # Apr 4
        "07042025",  # Apr 7
        "08042025",  # Apr 8
        "09042025",  # Apr 9
        "10042025",  # Apr 10
        "11042025",  # Apr 11
        "14042025",  # Apr 14 (Ram Navami might be holiday, but included)
        "15042025",  # Apr 15
        "16042025",  # Apr 16
        "17042025",  # Apr 17
        "18042025",  # Apr 18 (Good Friday might be holiday)
        "21042025",  # Apr 21
        "22042025",  # Apr 22
        "23042025",  # Apr 23
        "24042025",  # Apr 24
        "25042025",  # Apr 25
        "28042025",  # Apr 28
        "29042025",  # Apr 29
        "30042025",  # Apr 30
    ]
    
    return april_dates

def download_nse_data(date_str):
    """Download NSE data for a specific date with multiple URL attempts"""
    
    # Multiple NSE URL patterns to try
    urls = [
        f"https://archives.nseindia.com/content/historical/EQUITIES/2025/APR/cm{date_str}bhav.csv.zip",
        f"https://www1.nseindia.com/content/historical/EQUITIES/2025/APR/cm{date_str}bhav.csv.zip",
        f"https://nsearchives.nseindia.com/content/historical/EQUITIES/2025/APR/cm{date_str}bhav.csv.zip",
        f"https://archives.nseindia.com/products/content/sec_bhavdata_full_{date_str}.csv",
        f"https://www1.nseindia.com/products/content/sec_bhavdata_full_{date_str}.csv",
        f"https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{date_str}.csv"
    ]
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }
    
    for i, url in enumerate(urls, 1):
        try:
            print(f"  Attempt {i}: {url}")
            response = requests.get(url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                return response.content
            else:
                print(f"    Status: {response.status_code}")
                
        except Exception as e:
            print(f"    Error: {str(e)}")
            continue
    
    return None

def save_data(content, filename, folder):
    """Save downloaded content to file"""
    try:
        filepath = os.path.join(folder, filename)
        
        if filename.endswith('.zip'):
            # Save as zip file
            with open(filepath, 'wb') as f:
                f.write(content)
            print(f"    Saved ZIP: {filename}")
        else:
            # Save as CSV file
            with open(filepath, 'wb') as f:
                f.write(content)
            print(f"    Saved CSV: {filename}")
        
        return True
    except Exception as e:
        print(f"    Save error: {str(e)}")
        return False

def validate_csv_file(filepath):
    """Validate that the downloaded file is a proper CSV"""
    try:
        df = pd.read_csv(filepath, nrows=5)  # Read first 5 rows to validate
        if len(df.columns) > 5:  # NSE files typically have many columns
            print(f"    ✓ Valid CSV with {len(df.columns)} columns")
            return True
        else:
            print(f"    ✗ Invalid CSV - only {len(df.columns)} columns")
            return False
    except Exception as e:
        print(f"    ✗ CSV validation error: {str(e)}")
        return False

def main():
    """Main function to download April 2025 NSE data"""
    
    print("=" * 60)
    print("NSE APRIL 2025 DATA DOWNLOADER")
    print("=" * 60)
    
    # Create data folder
    data_folder = "NSE_April_2025_Data"
    if not os.path.exists(data_folder):
        os.makedirs(data_folder)
        print(f"Created folder: {data_folder}")
    
    # Get trading days
    trading_days = get_april_2025_trading_days()
    print(f"\nTotal trading days to download: {len(trading_days)}")
    
    successful_downloads = []
    failed_downloads = []
    
    # Download data for each trading day
    for i, date_str in enumerate(trading_days, 1):
        print(f"\n[{i}/{len(trading_days)}] Downloading data for {date_str}")
        
        # Try multiple filename patterns
        possible_filenames = [
            f"cm{date_str}bhav.csv",
            f"sec_bhavdata_full_{date_str}.csv"
        ]
        
        downloaded = False
        
        for filename in possible_filenames:
            content = download_nse_data(date_str)
            
            if content:
                if save_data(content, filename, data_folder):
                    filepath = os.path.join(data_folder, filename)
                    if validate_csv_file(filepath):
                        successful_downloads.append(date_str)
                        downloaded = True
                        break
                    else:
                        # Remove invalid file
                        if os.path.exists(filepath):
                            os.remove(filepath)
        
        if not downloaded:
            failed_downloads.append(date_str)
            print(f"    ✗ Failed to download data for {date_str}")
    
    # Summary
    print("\n" + "=" * 60)
    print("DOWNLOAD SUMMARY")
    print("=" * 60)
    print(f"Successful downloads: {len(successful_downloads)}")
    print(f"Failed downloads: {len(failed_downloads)}")
    
    if successful_downloads:
        print(f"\n✓ Successfully downloaded dates:")
        for date in successful_downloads:
            print(f"  - {date}")
    
    if failed_downloads:
        print(f"\n✗ Failed to download dates:")
        for date in failed_downloads:
            print(f"  - {date}")
        
        # Save failed URLs for manual download
        failed_file = os.path.join(data_folder, "failed_urls_apr2025.txt")
        with open(failed_file, 'w') as f:
            f.write("Failed downloads for April 2025:\n")
            f.write("=" * 40 + "\n")
            for date in failed_downloads:
                f.write(f"\nDate: {date}\n")
                f.write(f"Try these URLs manually:\n")
                f.write(f"- https://archives.nseindia.com/content/historical/EQUITIES/2025/APR/cm{date}bhav.csv.zip\n")
                f.write(f"- https://archives.nseindia.com/products/content/sec_bhavdata_full_{date}.csv\n")
        
        print(f"\n📝 Failed URLs saved to: {failed_file}")
    
    print(f"\n📁 Data saved in folder: {data_folder}")
    print("=" * 60)

if __name__ == "__main__":
    main()
