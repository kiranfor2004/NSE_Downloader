#!/usr/bin/env python3
"""
NSE March 2025 Data Downloader
Downloads daily NSE equity data for March 2025 trading days.
"""

import os
import requests
from datetime import datetime, timedelta
import pandas as pd

def get_march_2025_trading_days():
    """Get all trading days for March 2025 (excluding weekends and holidays)"""
    trading_days = []
    
    # March 2025 trading days (excluding weekends and major holidays)
    march_dates = [
        "03032025",  # Mar 3
        "04032025",  # Mar 4
        "05032025",  # Mar 5
        "06032025",  # Mar 6
        "07032025",  # Mar 7
        "10032025",  # Mar 10
        "11032025",  # Mar 11
        "12032025",  # Mar 12
        "13032025",  # Mar 13
        "14032025",  # Mar 14
        "17032025",  # Mar 17
        "18032025",  # Mar 18
        "19032025",  # Mar 19
        "20032025",  # Mar 20
        "21032025",  # Mar 21
        "24032025",  # Mar 24
        "25032025",  # Mar 25
        "26032025",  # Mar 26
        "27032025",  # Mar 27
        "28032025",  # Mar 28
        "31032025",  # Mar 31
    ]
    
    return march_dates

def download_nse_data(date_str):
    """Download NSE data for a specific date with multiple URL attempts"""
    
    # Multiple NSE URL patterns to try
    urls = [
        f"https://archives.nseindia.com/content/historical/EQUITIES/2025/MAR/cm{date_str}bhav.csv.zip",
        f"https://www1.nseindia.com/content/historical/EQUITIES/2025/MAR/cm{date_str}bhav.csv.zip",
        f"https://nsearchives.nseindia.com/content/historical/EQUITIES/2025/MAR/cm{date_str}bhav.csv.zip",
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
    """Main function to download March 2025 NSE data"""
    
    print("=" * 60)
    print("NSE MARCH 2025 DATA DOWNLOADER")
    print("=" * 60)
    
    # Create data folder
    data_folder = "NSE_March_2025_Data"
    if not os.path.exists(data_folder):
        os.makedirs(data_folder)
        print(f"Created folder: {data_folder}")
    
    # Get trading days
    trading_days = get_march_2025_trading_days()
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
        failed_file = os.path.join(data_folder, "failed_urls_mar2025.txt")
        with open(failed_file, 'w') as f:
            f.write("Failed downloads for March 2025:\n")
            f.write("=" * 40 + "\n")
            for date in failed_downloads:
                f.write(f"\nDate: {date}\n")
                f.write(f"Try these URLs manually:\n")
                f.write(f"- https://archives.nseindia.com/content/historical/EQUITIES/2025/MAR/cm{date}bhav.csv.zip\n")
                f.write(f"- https://archives.nseindia.com/products/content/sec_bhavdata_full_{date}.csv\n")
        
        print(f"\n📝 Failed URLs saved to: {failed_file}")
    
    print(f"\n📁 Data saved in folder: {data_folder}")
    print("=" * 60)

if __name__ == "__main__":
    main()
