#!/usr/bin/env python3
"""
🚀 NSE F&O Data Downloader for February 2025
Download Futures & Options data from NSE India
"""

import requests
import os
import json
import time
from datetime import datetime, date, timedelta
import calendar
import zipfile
import pandas as pd

class NSEFODownloader:
    def __init__(self):
        self.base_url = "https://nsearchives.nseindia.com/content/historical/DERIVATIVES"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        
    def get_february_2025_dates(self):
        """Get all trading dates for February 2025 (excluding weekends)"""
        year = 2025
        month = 2
        
        # Get all dates in February 2025
        cal = calendar.monthcalendar(year, month)
        trading_dates = []
        
        for week in cal:
            for day in week:
                if day != 0:  # Valid day
                    date_obj = date(year, month, day)
                    # Exclude weekends (Saturday=5, Sunday=6)
                    if date_obj.weekday() < 5:  # Monday=0 to Friday=4
                        trading_dates.append(date_obj)
        
        return trading_dates
    
    def format_date_for_url(self, date_obj):
        """Format date for NSE URL (DDMMMYYYY format)"""
        months = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN',
                 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']
        
        day = f"{date_obj.day:02d}"
        month = months[date_obj.month - 1]
        year = str(date_obj.year)
        
        return f"{day}{month}{year}"
    
    def download_fo_file(self, date_obj, file_type="fo"):
        """Download F&O file for a specific date"""
        date_str = self.format_date_for_url(date_obj)
        
        # Different file patterns NSE uses
        file_patterns = [
            f"fo{date_str}bhav.csv.zip",
            f"fo{date_str}.zip", 
            f"FO_{date_str}.zip",
            f"fo{date_obj.day:02d}{date_obj.month:02d}{date_obj.year}bhav.csv.zip"
        ]
        
        for file_pattern in file_patterns:
            try:
                # Construct URL
                year_month = f"{date_obj.year}/{date_obj.month:02d}"
                url = f"{self.base_url}/{year_month}/{file_pattern}"
                
                print(f"🔄 Trying: {url}")
                
                response = self.session.get(url, timeout=30)
                
                if response.status_code == 200:
                    # Create directory if it doesn't exist
                    os.makedirs("NSE_FO_February_2025_Data", exist_ok=True)
                    
                    file_path = os.path.join("NSE_FO_February_2025_Data", file_pattern)
                    
                    with open(file_path, 'wb') as f:
                        f.write(response.content)
                    
                    print(f"✅ Downloaded: {file_pattern} ({len(response.content)} bytes)")
                    
                    # Try to extract if it's a zip file
                    if file_pattern.endswith('.zip'):
                        try:
                            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                                zip_ref.extractall("NSE_FO_February_2025_Data")
                                print(f"📂 Extracted: {file_pattern}")
                        except Exception as e:
                            print(f"⚠️ Extraction failed: {str(e)}")
                    
                    return True
                    
                else:
                    print(f"❌ Failed: {response.status_code}")
                    
            except Exception as e:
                print(f"❌ Error downloading {file_pattern}: {str(e)}")
                continue
        
        return False
    
    def download_all_february_2025(self):
        """Download all F&O data for February 2025"""
        print("🚀 NSE F&O Data Downloader - February 2025")
        print("=" * 60)
        
        trading_dates = self.get_february_2025_dates()
        print(f"📅 Found {len(trading_dates)} potential trading dates in February 2025")
        
        successful_downloads = 0
        failed_downloads = 0
        
        for i, date_obj in enumerate(trading_dates, 1):
            print(f"\n📊 [{i}/{len(trading_dates)}] Processing {date_obj.strftime('%d-%b-%Y')}")
            
            if self.download_fo_file(date_obj):
                successful_downloads += 1
            else:
                failed_downloads += 1
                print(f"❌ No F&O data found for {date_obj.strftime('%d-%b-%Y')}")
            
            # Add delay to avoid overwhelming the server
            time.sleep(2)
        
        print(f"\n📊 Download Summary:")
        print(f"✅ Successful: {successful_downloads}")
        print(f"❌ Failed: {failed_downloads}")
        print(f"📁 Data saved in: NSE_FO_February_2025_Data/")
        
        return successful_downloads
    
    def list_alternative_sources(self):
        """List alternative data sources and URLs"""
        print("\n🔗 Alternative F&O Data Sources:")
        print("=" * 50)
        print("1. NSE Archives: https://nsearchives.nseindia.com/")
        print("2. NSE Historical Data: https://www1.nseindia.com/products/content/derivatives/equities/historical_fo.htm")
        print("3. Direct CSV Downloads: Try different date formats")
        print("4. Bhavcopy Archives: https://nsearchives.nseindia.com/content/historical/DERIVATIVES/")
        
        print("\n📝 Manual Download Instructions:")
        print("1. Visit: https://www.nseindia.com/all-reports-derivatives")
        print("2. Navigate to 'Historical Reports' > 'Derivatives'")
        print("3. Select 'Equity Derivatives' and choose February 2025")
        print("4. Download individual daily files")

def main():
    """Main function"""
    downloader = NSEFODownloader()
    
    print("🎯 NSE F&O February 2025 Downloader")
    print("Choose an option:")
    print("1. Download F&O data for February 2025")
    print("2. Show alternative sources")
    print("3. Download specific date")
    
    choice = input("\nEnter choice (1-3): ").strip()
    
    if choice == "1":
        result = downloader.download_all_february_2025()
        if result == 0:
            print("\n⚠️ No files downloaded. The data might not be available yet.")
            print("💡 February 2025 is in the future - data might not exist.")
            downloader.list_alternative_sources()
            
    elif choice == "2":
        downloader.list_alternative_sources()
        
    elif choice == "3":
        date_str = input("Enter date (DD-MM-YYYY): ").strip()
        try:
            date_obj = datetime.strptime(date_str, "%d-%m-%Y").date()
            if downloader.download_fo_file(date_obj):
                print("✅ Download successful!")
            else:
                print("❌ Download failed!")
        except ValueError:
            print("❌ Invalid date format! Use DD-MM-YYYY")
    
    else:
        print("❌ Invalid choice!")

if __name__ == "__main__":
    main()
