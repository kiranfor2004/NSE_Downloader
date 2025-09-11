#!/usr/bin/env python3
"""
NSE India Data Availability Checker & Smart Downloader
Checks what data is available and downloads accordingly
"""

import requests
import os
import time
from datetime import datetime, timedelta
import pandas as pd

def test_nse_data_availability():
    """Test what NSE data is currently available"""
    
    print("🔍 Testing NSE Data Availability...")
    print("=" * 50)
    
    # Test different date ranges to see what's available
    test_dates = [
        "01012025",  # January 1, 2025
        "01072025",  # July 1, 2025 (we know this works)
        "01082025",  # August 1, 2025 (we know this works)
        "01062025",  # June 1, 2025
        "01052025",  # May 1, 2025
        "01042025",  # April 1, 2025
        "01032025",  # March 1, 2025
        "01022025",  # February 1, 2025
    ]
    
    available_months = []
    
    for date_str in test_dates:
        display_date = f"{date_str[:2]}-{date_str[2:4]}-{date_str[4:]}"
        month_name = datetime.strptime(date_str, "%d%m%Y").strftime("%B %Y")
        
        print(f"🔎 Testing {month_name} ({display_date})...", end=" ")
        
        urls = [
            f"https://www1.nseindia.com/content/historical/EQUITIES/{date_str[4:]}/{date_str[2:4].upper()}/cm{date_str}bhav.csv.zip",
            f"https://archives.nseindia.com/content/historical/EQUITIES/{date_str[4:]}/{date_str[2:4].upper()}/cm{date_str}bhav.csv.zip",
            f"https://nsearchives.nseindia.com/content/historical/EQUITIES/{date_str[4:]}/{date_str[2:4].upper()}/cm{date_str}bhav.csv.zip"
        ]
        
        found = False
        for url in urls:
            try:
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                }
                
                response = requests.get(url, headers=headers, timeout=10)
                if response.status_code == 200:
                    # Check if it contains actual data
                    if len(response.content) > 1000:  # Reasonable file size
                        print("✅ Available")
                        available_months.append((month_name, date_str[2:4], date_str[4:]))
                        found = True
                        break
                        
            except Exception as e:
                continue
        
        if not found:
            print("❌ Not available")
        
        time.sleep(0.5)  # Be respectful to servers
    
    print(f"\n📊 AVAILABILITY SUMMARY:")
    if available_months:
        print("✅ Available months:")
        for month, mm, yyyy in available_months:
            print(f"   • {month}")
    else:
        print("❌ No months are currently available for 2025")
    
    return available_months

def get_trading_days_smart(year, month):
    """Get trading days with better holiday handling"""
    
    # More comprehensive holiday list for 2025
    holidays_2025 = {
        # January
        (2025, 1, 1): "New Year's Day",
        (2025, 1, 26): "Republic Day", 
        # February
        (2025, 2, 26): "Maha Shivratri",
        # March  
        (2025, 3, 14): "Holi",
        (2025, 3, 31): "Ram Navami",
        # April
        (2025, 4, 14): "Dr. Ambedkar Jayanti", 
        (2025, 4, 18): "Good Friday",
        # May
        (2025, 5, 1): "Maharashtra Day",
        (2025, 5, 12): "Buddha Purnima",
        # June
        (2025, 6, 15): "Eid al-Adha (estimated)",
        # Add more as needed
    }
    
    start_date = datetime(year, month, 1)
    if month == 12:
        end_date = datetime(year + 1, 1, 1) - timedelta(days=1)
    else:
        end_date = datetime(year, month + 1, 1) - timedelta(days=1)
    
    trading_days = []
    current_date = start_date
    
    while current_date <= end_date:
        # Skip weekends
        if current_date.weekday() < 5:
            # Skip holidays
            date_tuple = (current_date.year, current_date.month, current_date.day)
            if date_tuple not in holidays_2025:
                trading_days.append(current_date.strftime("%d%m%Y"))
        current_date += timedelta(days=1)
    
    return trading_days

def download_available_months():
    """Download data for available months only"""
    
    print("\n🚀 Starting Smart NSE Data Download...")
    print("=" * 50)
    
    # Test availability first
    available_months = test_nse_data_availability()
    
    if not available_months:
        print("\n❌ No 2025 data appears to be available yet.")
        print("💡 This could be because:")
        print("   • NSE historical data has a delay")
        print("   • Data is published monthly with a lag")
        print("   • Server URLs have changed")
        print("\n✅ We already have July and August 2025 data!")
        return
    
    print(f"\n📥 Will download data for {len(available_months)} available months...")
    
    # Now download the available months
    month_mapping = {
        "01": "January", "02": "February", "03": "March",
        "04": "April", "05": "May", "06": "June"
    }
    
    for month_name, mm, yyyy in available_months:
        month_num = int(mm)
        year_num = int(yyyy)
        
        print(f"\n📅 Downloading {month_name}...")
        download_month_data_smart(year_num, month_num, month_name)

def download_month_data_smart(year, month, month_name):
    """Smart month data downloader"""
    
    folder_name = f"NSE_{month_name.replace(' ', '_')}_Data"
    os.makedirs(folder_name, exist_ok=True)
    
    trading_days = get_trading_days_smart(year, month)
    print(f"📊 Trading days in {month_name}: {len(trading_days)}")
    
    successful = 0
    failed = 0
    
    for i, date_str in enumerate(trading_days, 1):
        display_date = f"{date_str[:2]}-{date_str[2:4]}-{date_str[4:]}"
        print(f"[{i:2}/{len(trading_days)}] {display_date}: ", end="")
        
        success = download_nse_file(date_str, folder_name)
        if success:
            successful += 1
            print("✅")
        else:
            failed += 1  
            print("❌")
        
        time.sleep(1)  # Be respectful
    
    print(f"\n📊 {month_name} Summary: ✅ {successful} success, ❌ {failed} failed")
    return successful, failed

def download_nse_file(date_str, folder):
    """Download single NSE file with multiple URL attempts"""
    
    filename = f"sec_bhavdata_full_{date_str}.csv"
    filepath = os.path.join(folder, filename)
    
    if os.path.exists(filepath):
        return True
    
    urls = [
        f"https://www1.nseindia.com/content/historical/EQUITIES/{date_str[4:]}/{date_str[2:4].upper()}/cm{date_str}bhav.csv.zip",
        f"https://archives.nseindia.com/content/historical/EQUITIES/{date_str[4:]}/{date_str[2:4].upper()}/cm{date_str}bhav.csv.zip",
        f"https://nsearchives.nseindia.com/content/historical/EQUITIES/{date_str[4:]}/{date_str[2:4].upper()}/cm{date_str}bhav.csv.zip"
    ]
    
    for url in urls:
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
            response = requests.get(url, headers=headers, timeout=15)
            
            if response.status_code == 200 and len(response.content) > 1000:
                # Try saving as CSV or extracting from ZIP
                try:
                    import zipfile
                    import io
                    
                    zip_file = zipfile.ZipFile(io.BytesIO(response.content))
                    csv_files = [f for f in zip_file.namelist() if f.endswith('.csv')]
                    
                    if csv_files:
                        csv_content = zip_file.read(csv_files[0]).decode('utf-8')
                        with open(filepath, 'w', encoding='utf-8') as f:
                            f.write(csv_content)
                        return True
                        
                except:
                    # Maybe it's already CSV
                    try:
                        with open(filepath, 'w', encoding='utf-8') as f:
                            f.write(response.text)
                        if 'SYMBOL' in response.text:
                            return True
                    except:
                        continue
                        
        except:
            continue
    
    return False

def main():
    print("🔍 NSE India Smart Data Downloader")
    print("=" * 60)
    print("🧠 This will intelligently check what data is available")
    print("📅 And download only the accessible months from Jan-June 2025")
    print()
    
    # Check current folder status first
    print("📁 Current NSE Data Folders:")
    folders = [f for f in os.listdir('.') if f.startswith('NSE_') and f.endswith('_Data')]
    for folder in folders:
        csv_count = len([f for f in os.listdir(folder) if f.endswith('.csv')])
        print(f"   • {folder}: {csv_count} CSV files")
    
    if not folders:
        print("   (No NSE data folders found)")
    
    print()
    
    # Run the smart download
    download_available_months()
    
    print(f"\n🎉 Smart download complete!")
    print(f"💡 If some months failed, they may not be published by NSE yet")

if __name__ == "__main__":
    main()
