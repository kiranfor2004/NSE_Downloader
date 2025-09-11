#!/usr/bin/env python3
"""
NSE India Data Downloader - January to June 2025
Downloads NSE Bhav Copy data for all trading days from Jan-June 2025
Organizes data in month-wise folders
"""

import requests
import os
import time
from datetime import datetime, timedelta
import pandas as pd

def get_trading_days(year, month):
    """Get all trading days for a given month (excluding weekends and major holidays)"""
    
    # Known major holidays in 2025 (NSE closed days)
    holidays_2025 = {
        # January
        (2025, 1, 26): "Republic Day",
        # February  
        (2025, 2, 26): "Maha Shivratri",
        # March
        (2025, 3, 14): "Holi",
        (2025, 3, 31): "Ram Navami", 
        # April
        (2025, 4, 18): "Good Friday",
        # May
        (2025, 5, 1): "Maharashtra Day",
        (2025, 5, 12): "Buddha Purnima",
        # June - no major holidays
    }
    
    # Get all days in the month
    start_date = datetime(year, month, 1)
    if month == 12:
        end_date = datetime(year + 1, 1, 1) - timedelta(days=1)
    else:
        end_date = datetime(year, month + 1, 1) - timedelta(days=1)
    
    trading_days = []
    current_date = start_date
    
    while current_date <= end_date:
        # Skip weekends (Saturday=5, Sunday=6)
        if current_date.weekday() < 5:  # Monday=0 to Friday=4
            # Skip known holidays
            date_tuple = (current_date.year, current_date.month, current_date.day)
            if date_tuple not in holidays_2025:
                trading_days.append(current_date.strftime("%d%m%Y"))
        current_date += timedelta(days=1)
    
    return trading_days

def download_nse_data(date_str, output_folder):
    """Download NSE data for a specific date"""
    
    # Format: DDMMYYYY to DD-MM-YYYY for display
    display_date = f"{date_str[:2]}-{date_str[2:4]}-{date_str[4:]}"
    
    # Multiple URL patterns to try (NSE sometimes changes URLs)
    urls = [
        f"https://www1.nseindia.com/content/historical/EQUITIES/{date_str[4:]}/{date_str[2:4].upper()}/cm{date_str}bhav.csv.zip",
        f"https://archives.nseindia.com/content/historical/EQUITIES/{date_str[4:]}/{date_str[2:4].upper()}/cm{date_str}bhav.csv.zip",
        f"https://nsearchives.nseindia.com/content/historical/EQUITIES/{date_str[4:]}/{date_str[2:4].upper()}/cm{date_str}bhav.csv.zip"
    ]
    
    filename = f"sec_bhavdata_full_{date_str}.csv"
    filepath = os.path.join(output_folder, filename)
    
    # Skip if already downloaded
    if os.path.exists(filepath):
        print(f"   ✅ {display_date}: Already exists")
        return True
    
    print(f"   📥 {display_date}: Downloading...", end=" ")
    
    for url_index, url in enumerate(urls, 1):
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            response = requests.get(url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                # Try to save as CSV directly (some URLs return CSV directly)
                try:
                    # Check if it's a CSV file
                    content = response.text
                    if 'SYMBOL' in content and 'SERIES' in content:
                        with open(filepath, 'w', encoding='utf-8') as f:
                            f.write(content)
                        print(f"✅ Success (URL {url_index})")
                        return True
                except Exception as e:
                    pass
                
                # If not CSV, try to extract from ZIP
                try:
                    import zipfile
                    import io
                    
                    zip_file = zipfile.ZipFile(io.BytesIO(response.content))
                    csv_files = [f for f in zip_file.namelist() if f.endswith('.csv')]
                    
                    if csv_files:
                        csv_content = zip_file.read(csv_files[0]).decode('utf-8')
                        with open(filepath, 'w', encoding='utf-8') as f:
                            f.write(csv_content)
                        print(f"✅ Success (URL {url_index})")
                        return True
                        
                except Exception as e:
                    continue
                    
        except Exception as e:
            continue
    
    print(f"❌ Failed")
    return False

def validate_file(filepath):
    """Validate downloaded CSV file"""
    try:
        df = pd.read_csv(filepath)
        if len(df) > 0 and 'SYMBOL' in df.columns:
            return len(df)
        return 0
    except:
        return 0

def download_month_data(year, month):
    """Download data for entire month"""
    
    month_names = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August", 
        9: "September", 10: "October", 11: "November", 12: "December"
    }
    
    month_name = month_names[month]
    folder_name = f"NSE_{month_name}_{year}_Data"
    
    print(f"\n📅 {month_name} {year} Data Download")
    print("=" * 50)
    
    # Create output folder
    os.makedirs(folder_name, exist_ok=True)
    print(f"📁 Created folder: {folder_name}")
    
    # Get trading days for the month
    trading_days = get_trading_days(year, month)
    print(f"📊 Trading days in {month_name}: {len(trading_days)}")
    
    if not trading_days:
        print(f"⚠️  No trading days found for {month_name} {year}")
        return
    
    successful_downloads = 0
    failed_dates = []
    
    for i, date_str in enumerate(trading_days, 1):
        print(f"[{i:2}/{len(trading_days)}]", end=" ")
        
        success = download_nse_data(date_str, folder_name)
        if success:
            # Validate the downloaded file
            filepath = os.path.join(folder_name, f"sec_bhavdata_full_{date_str}.csv")
            record_count = validate_file(filepath)
            if record_count > 0:
                successful_downloads += 1
                print(f"         📊 {record_count:,} records")
            else:
                print(f"         ⚠️  Invalid file")
                failed_dates.append(date_str)
        else:
            failed_dates.append(date_str)
        
        # Small delay to be respectful to NSE servers
        time.sleep(1)
    
    # Summary for the month
    print(f"\n📊 {month_name} {year} Summary:")
    print(f"   ✅ Successfully downloaded: {successful_downloads}/{len(trading_days)} files")
    print(f"   📁 Saved in: {folder_name}")
    
    if failed_dates:
        print(f"   ❌ Failed dates: {len(failed_dates)}")
        failed_file = os.path.join(folder_name, f"failed_urls_{month:02d}{year}.txt")
        with open(failed_file, 'w') as f:
            f.write("Failed download dates:\n")
            for date in failed_dates:
                display_date = f"{date[:2]}-{date[2:4]}-{date[4:]}"
                f.write(f"{display_date}\n")
        print(f"   📝 Failed dates saved to: {failed_file}")
    else:
        print(f"   🎉 All files downloaded successfully!")

def main():
    print("🔍 NSE India Data Downloader - January to June 2025")
    print("=" * 60)
    print("📅 This will download NSE Bhav Copy data for all trading days")
    print("📁 Data will be organized in month-wise folders")
    print("⏱️  Please be patient, this may take some time...")
    print()
    
    # Define months to download
    months_to_download = [
        (2025, 1),   # January 2025
        (2025, 2),   # February 2025
        (2025, 3),   # March 2025
        (2025, 4),   # April 2025
        (2025, 5),   # May 2025
        (2025, 6),   # June 2025
    ]
    
    total_start_time = time.time()
    overall_summary = {}
    
    # Download data for each month
    for year, month in months_to_download:
        month_start_time = time.time()
        
        try:
            download_month_data(year, month)
            month_time = time.time() - month_start_time
            
            # Store summary info
            month_names = {1: "January", 2: "February", 3: "March", 4: "April", 5: "May", 6: "June"}
            month_name = month_names[month]
            folder_name = f"NSE_{month_name}_{year}_Data"
            
            # Count successful files
            csv_files = [f for f in os.listdir(folder_name) if f.endswith('.csv')]
            overall_summary[month_name] = {
                'files': len(csv_files),
                'time': month_time,
                'folder': folder_name
            }
            
            print(f"⏱️  {month_name} completed in {month_time:.1f} seconds")
            
        except Exception as e:
            print(f"❌ Error downloading {month_name}: {e}")
            continue
        
        print()  # Space between months
    
    # Final summary
    total_time = time.time() - total_start_time
    
    print("🎉 DOWNLOAD COMPLETE!")
    print("=" * 60)
    print("📊 OVERALL SUMMARY:")
    
    total_files = 0
    for month_name, info in overall_summary.items():
        print(f"   📅 {month_name:>9}: {info['files']:>3} files in {info['folder']}")
        total_files += info['files']
    
    print(f"\n📈 TOTALS:")
    print(f"   📄 Total files downloaded: {total_files}")
    print(f"   ⏱️  Total time: {total_time/60:.1f} minutes")
    print(f"   📁 Created {len(overall_summary)} month folders")
    
    print(f"\n✅ All NSE data from January-June 2025 has been downloaded!")
    print(f"🔍 You can now use this data for analysis alongside July-August data")

if __name__ == "__main__":
    main()
