#!/usr/bin/env python3
"""
🔍 NSE F&O Data Availability Checker & Downloader
Check what F&O data is actually available and download it
"""

import requests
import os
import json
import time
from datetime import datetime, date, timedelta
import calendar
import zipfile

class NSEFOChecker:
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
    
    def check_available_data(self):
        """Check what F&O data is available for recent months"""
        print("🔍 Checking NSE F&O Data Availability")
        print("=" * 60)
        
        current_date = date.today()
        available_data = []
        
        # Check last 6 months
        for i in range(6):
            check_date = current_date - timedelta(days=30*i)
            year = check_date.year
            month = check_date.month
            
            print(f"\n📅 Checking {calendar.month_name[month]} {year}...")
            
            # Try a few dates in the month
            test_dates = [1, 15, 28]  # Beginning, middle, end of month
            month_has_data = False
            
            for day in test_dates:
                try:
                    test_date = date(year, month, min(day, calendar.monthrange(year, month)[1]))
                    if self.test_date_availability(test_date):
                        available_data.append((year, month))
                        month_has_data = True
                        print(f"   ✅ Data found for {test_date.strftime('%d-%b-%Y')}")
                        break
                except:
                    continue
            
            if not month_has_data:
                print(f"   ❌ No data found for {calendar.month_name[month]} {year}")
        
        return available_data
    
    def test_date_availability(self, date_obj):
        """Test if data is available for a specific date"""
        date_str = self.format_date_for_url(date_obj)
        
        # Try the most common pattern
        file_pattern = f"fo{date_str}bhav.csv.zip"
        year_month = f"{date_obj.year}/{date_obj.month:02d}"
        url = f"{self.base_url}/{year_month}/{file_pattern}"
        
        try:
            response = self.session.head(url, timeout=10)
            return response.status_code == 200
        except:
            return False
    
    def format_date_for_url(self, date_obj):
        """Format date for NSE URL (DDMMMYYYY format)"""
        months = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN',
                 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']
        
        day = f"{date_obj.day:02d}"
        month = months[date_obj.month - 1]
        year = str(date_obj.year)
        
        return f"{day}{month}{year}"
    
    def download_month_data(self, year, month):
        """Download all available F&O data for a specific month"""
        print(f"\n🚀 Downloading F&O data for {calendar.month_name[month]} {year}")
        print("=" * 60)
        
        # Create directory
        dir_name = f"NSE_FO_{calendar.month_name[month]}_{year}_Data"
        os.makedirs(dir_name, exist_ok=True)
        
        # Get all potential trading dates for the month
        cal = calendar.monthcalendar(year, month)
        trading_dates = []
        
        for week in cal:
            for day in week:
                if day != 0:
                    date_obj = date(year, month, day)
                    if date_obj.weekday() < 5:  # Exclude weekends
                        trading_dates.append(date_obj)
        
        successful = 0
        failed = 0
        
        for i, date_obj in enumerate(trading_dates, 1):
            print(f"📊 [{i}/{len(trading_dates)}] Downloading {date_obj.strftime('%d-%b-%Y')}...")
            
            if self.download_fo_file(date_obj, dir_name):
                successful += 1
            else:
                failed += 1
            
            time.sleep(1)  # Be nice to the server
        
        print(f"\n📊 Download Summary for {calendar.month_name[month]} {year}:")
        print(f"✅ Successful: {successful}")
        print(f"❌ Failed: {failed}")
        print(f"📁 Data saved in: {dir_name}/")
        
        return successful
    
    def download_fo_file(self, date_obj, output_dir):
        """Download F&O file for a specific date"""
        date_str = self.format_date_for_url(date_obj)
        
        file_patterns = [
            f"fo{date_str}bhav.csv.zip",
            f"fo{date_str}.zip",
            f"FO_{date_str}.zip"
        ]
        
        for file_pattern in file_patterns:
            try:
                year_month = f"{date_obj.year}/{date_obj.month:02d}"
                url = f"{self.base_url}/{year_month}/{file_pattern}"
                
                response = self.session.get(url, timeout=30)
                
                if response.status_code == 200:
                    file_path = os.path.join(output_dir, file_pattern)
                    
                    with open(file_path, 'wb') as f:
                        f.write(response.content)
                    
                    print(f"   ✅ Downloaded: {file_pattern} ({len(response.content)} bytes)")
                    
                    # Extract zip if possible
                    if file_pattern.endswith('.zip'):
                        try:
                            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                                zip_ref.extractall(output_dir)
                        except:
                            pass
                    
                    return True
                    
            except Exception as e:
                continue
        
        print(f"   ❌ No data for {date_obj.strftime('%d-%b-%Y')}")
        return False
    
    def check_current_month_availability(self):
        """Check if current month data is available"""
        today = date.today()
        print(f"\n🔍 Checking current month ({calendar.month_name[today.month]} {today.year}) data availability...")
        
        # Check last few days
        for i in range(10):
            check_date = today - timedelta(days=i)
            if check_date.weekday() < 5:  # Weekday only
                if self.test_date_availability(check_date):
                    print(f"✅ Latest available data: {check_date.strftime('%d-%b-%Y')}")
                    return check_date
        
        print("❌ No recent data found")
        return None

def main():
    """Main function"""
    checker = NSEFOChecker()
    
    print("🎯 NSE F&O Data Availability Checker")
    print("Choose an option:")
    print("1. Check data availability (last 6 months)")
    print("2. Download specific month")
    print("3. Check current month availability")
    print("4. Download latest available data")
    
    choice = input("\nEnter choice (1-4): ").strip()
    
    if choice == "1":
        available_data = checker.check_available_data()
        if available_data:
            print(f"\n📊 Available Data Summary:")
            for year, month in available_data:
                print(f"✅ {calendar.month_name[month]} {year}")
        else:
            print("❌ No F&O data found in recent months")
    
    elif choice == "2":
        year = int(input("Enter year (e.g., 2024): "))
        month = int(input("Enter month (1-12): "))
        
        if 1 <= month <= 12:
            checker.download_month_data(year, month)
        else:
            print("❌ Invalid month!")
    
    elif choice == "3":
        latest_date = checker.check_current_month_availability()
        if latest_date:
            download = input(f"Download data for {latest_date.strftime('%d-%b-%Y')}? (y/n): ")
            if download.lower() == 'y':
                dir_name = f"NSE_FO_Latest_Data"
                os.makedirs(dir_name, exist_ok=True)
                checker.download_fo_file(latest_date, dir_name)
    
    elif choice == "4":
        print("🔍 Finding latest available F&O data...")
        today = date.today()
        
        for i in range(30):  # Check last 30 days
            check_date = today - timedelta(days=i)
            if check_date.weekday() < 5:  # Weekday only
                if checker.test_date_availability(check_date):
                    print(f"✅ Latest data found: {check_date.strftime('%d-%b-%Y')}")
                    
                    download = input("Download this data? (y/n): ")
                    if download.lower() == 'y':
                        dir_name = f"NSE_FO_Latest_Data"
                        os.makedirs(dir_name, exist_ok=True)
                        checker.download_fo_file(check_date, dir_name)
                    break
        else:
            print("❌ No recent F&O data found")
    
    else:
        print("❌ Invalid choice!")

if __name__ == "__main__":
    main()
