"""
Download Missing F&O Data (March - August 2025)
Systematically download F&O UDiFF data for missing months to complete Step 4 dataset
"""
import requests
import zipfile
import pandas as pd
import os
import pyodbc
from datetime import datetime, timedelta
import json
import io

class MissingMonthsDownloader:
    def __init__(self):
        # Load database configuration
        with open('database_config.json', 'r') as f:
            self.db_config = json.load(f)
        
        # Set up directories
        self.download_dir = r"C:\Users\kiran\NSE_Downloader\fo_udiff_downloads"
        os.makedirs(self.download_dir, exist_ok=True)
        
        # Trading dates for each month (excluding weekends and holidays)
        self.trading_dates = {
            'March 2025': [
                '2025-03-03', '2025-03-04', '2025-03-05', '2025-03-06', '2025-03-07',
                '2025-03-10', '2025-03-11', '2025-03-12', '2025-03-13', '2025-03-14',
                '2025-03-17', '2025-03-18', '2025-03-19', '2025-03-20', '2025-03-21',
                '2025-03-24', '2025-03-25', '2025-03-26', '2025-03-27', '2025-03-28',
                '2025-03-31'
            ],
            'April 2025': [
                '2025-04-01', '2025-04-02', '2025-04-03', '2025-04-04', '2025-04-07',
                '2025-04-08', '2025-04-09', '2025-04-10', '2025-04-11', '2025-04-14',
                '2025-04-15', '2025-04-16', '2025-04-17', '2025-04-18', '2025-04-21',
                '2025-04-22', '2025-04-23', '2025-04-24', '2025-04-25', '2025-04-28',
                '2025-04-29', '2025-04-30'
            ],
            'May 2025': [
                '2025-05-01', '2025-05-02', '2025-05-05', '2025-05-06', '2025-05-07',
                '2025-05-08', '2025-05-09', '2025-05-12', '2025-05-13', '2025-05-14',
                '2025-05-15', '2025-05-16', '2025-05-19', '2025-05-20', '2025-05-21',
                '2025-05-22', '2025-05-23', '2025-05-26', '2025-05-27', '2025-05-28',
                '2025-05-29', '2025-05-30'
            ],
            'June 2025': [
                '2025-06-02', '2025-06-03', '2025-06-04', '2025-06-05', '2025-06-06',
                '2025-06-09', '2025-06-10', '2025-06-11', '2025-06-12', '2025-06-13',
                '2025-06-16', '2025-06-17', '2025-06-18', '2025-06-19', '2025-06-20',
                '2025-06-23', '2025-06-24', '2025-06-25', '2025-06-26', '2025-06-27',
                '2025-06-30'
            ],
            'July 2025': [
                '2025-07-01', '2025-07-02', '2025-07-03', '2025-07-04', '2025-07-07',
                '2025-07-08', '2025-07-09', '2025-07-10', '2025-07-11', '2025-07-14',
                '2025-07-15', '2025-07-16', '2025-07-17', '2025-07-18', '2025-07-21',
                '2025-07-22', '2025-07-23', '2025-07-24', '2025-07-25', '2025-07-28',
                '2025-07-29', '2025-07-30', '2025-07-31'
            ],
            'August 2025': [
                '2025-08-01', '2025-08-04', '2025-08-05', '2025-08-06', '2025-08-07',
                '2025-08-08', '2025-08-11', '2025-08-12', '2025-08-13', '2025-08-14',
                '2025-08-18', '2025-08-19', '2025-08-20', '2025-08-21', '2025-08-22',
                '2025-08-25', '2025-08-26', '2025-08-27', '2025-08-28', '2025-08-29'
            ]
        }
    
    def download_udiff_file(self, date_str):
        """Download UDiFF file for a specific date"""
        
        # Convert date format
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        formatted_date = date_obj.strftime('%d%m%Y')
        
        # NSE UDiFF URL
        base_url = "https://archives.nseindia.com/products/content/derivatives/equities/"
        filename = f"udiff_{formatted_date}.zip"
        url = base_url + filename
        
        # Check if already downloaded
        local_path = os.path.join(self.download_dir, f"BhavCopy_NSE_FO_0_0_0_{date_obj.strftime('%Y%m%d')}_F_0000.csv.zip")
        if os.path.exists(local_path):
            print(f"📁 Already exists: {os.path.basename(local_path)}")
            return True, local_path
        
        print(f"📥 Downloading: {filename} for {date_str}")
        
        # Headers to mimic browser
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
        }
        
        try:
            response = requests.get(url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                file_size = len(response.content)
                print(f"✅ Downloaded: {file_size:,} bytes")
                
                # Save with consistent naming convention
                with open(local_path, 'wb') as f:
                    f.write(response.content)
                
                # Quick validation
                try:
                    with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
                        csv_files = [f for f in zip_file.namelist() if f.endswith('.csv')]
                        if csv_files:
                            print(f"✅ Valid ZIP with CSV: {csv_files[0]}")
                            return True, local_path
                        else:
                            print(f"❌ No CSV found in ZIP")
                            return False, None
                except:
                    print(f"❌ Invalid ZIP file")
                    return False, None
                    
            elif response.status_code == 404:
                print(f"❌ File not found (404) - May be holiday/weekend: {date_str}")
                return False, None
            else:
                print(f"❌ HTTP Error {response.status_code}")
                return False, None
                
        except Exception as e:
            print(f"❌ Error: {e}")
            return False, None
    
    def get_connection(self):
        """Get database connection"""
        return pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={self.db_config['server']};"
            f"DATABASE={self.db_config['database']};"
            f"Trusted_Connection=yes;"
        )
    
    def check_existing_data(self, month_year):
        """Check what data already exists in database for a month"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Get year and month
        year, month = month_year.split()[1], month_year.split()[0]
        month_num = {
            'March': '03', 'April': '04', 'May': '05',
            'June': '06', 'July': '07', 'August': '08'
        }[month]
        
        # Check existing dates
        query = """
        SELECT DISTINCT trade_date, COUNT(*) as record_count
        FROM step04_fo_udiff_daily 
        WHERE trade_date LIKE ?
        GROUP BY trade_date
        ORDER BY trade_date
        """
        
        pattern = f"{year}{month_num}%"
        cursor.execute(query, pattern)
        
        existing_data = {}
        for row in cursor.fetchall():
            existing_data[row[0]] = row[1]
        
        conn.close()
        return existing_data
    
    def download_month_data(self, month_year):
        """Download all trading days for a specific month"""
        print(f"\n🗓️  Processing {month_year}")
        print(f"=" * 60)
        
        # Check existing data
        existing_data = self.check_existing_data(month_year)
        if existing_data:
            print(f"📊 Existing data in database:")
            for date, count in existing_data.items():
                print(f"   {date}: {count:,} records")
        else:
            print(f"📊 No existing data found for {month_year}")
        
        # Get trading dates for this month
        trading_dates = self.trading_dates.get(month_year, [])
        print(f"📅 Trading dates to process: {len(trading_dates)}")
        
        success_count = 0
        failure_count = 0
        downloaded_files = []
        
        for date_str in trading_dates:
            # Convert to database format to check
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
            db_date = date_obj.strftime('%Y%m%d')
            
            if db_date in existing_data:
                print(f"⏭️  Skipping {date_str} - Already in database ({existing_data[db_date]:,} records)")
                success_count += 1
                continue
            
            success, file_path = self.download_udiff_file(date_str)
            if success:
                success_count += 1
                downloaded_files.append(file_path)
                print(f"✅ {date_str}: Downloaded to {os.path.basename(file_path)}")
            else:
                failure_count += 1
                print(f"❌ {date_str}: Download failed")
        
        print(f"\n📊 {month_year} Summary:")
        print(f"   ✅ Successful: {success_count}")
        print(f"   ❌ Failed: {failure_count}")
        print(f"   📁 New files: {len(downloaded_files)}")
        
        return success_count, failure_count, downloaded_files
    
    def download_all_missing_months(self):
        """Download data for all missing months"""
        print("🚀 NSE F&O Missing Months Downloader")
        print("Goal: Download March - August 2025 F&O data")
        print("=" * 80)
        
        total_success = 0
        total_failure = 0
        all_downloaded_files = []
        
        # Process each month
        for month_year in ['March 2025', 'April 2025', 'May 2025', 'June 2025', 'July 2025', 'August 2025']:
            success, failure, files = self.download_month_data(month_year)
            total_success += success
            total_failure += failure
            all_downloaded_files.extend(files)
        
        print(f"\n🎯 OVERALL SUMMARY")
        print(f"=" * 80)
        print(f"✅ Total successful downloads: {total_success}")
        print(f"❌ Total failed downloads: {total_failure}")
        print(f"📁 Total new files: {len(all_downloaded_files)}")
        
        if all_downloaded_files:
            print(f"\n📂 Downloaded files location: {self.download_dir}")
            print(f"🔄 Next step: Run step04_fo_validation_loader.py to load into database")
        
        return total_success, total_failure, all_downloaded_files

def main():
    downloader = MissingMonthsDownloader()
    
    print("Options:")
    print("1. Download ALL missing months (March - August 2025)")
    print("2. Download specific month")
    print("3. Check current database status")
    
    choice = input("\nEnter choice (1-3): ").strip()
    
    if choice == '1':
        downloader.download_all_missing_months()
    elif choice == '2':
        print("\nAvailable months:")
        months = ['March 2025', 'April 2025', 'May 2025', 'June 2025', 'July 2025', 'August 2025']
        for i, month in enumerate(months, 1):
            print(f"{i}. {month}")
        
        month_choice = input("Select month (1-6): ").strip()
        try:
            month_index = int(month_choice) - 1
            if 0 <= month_index < len(months):
                selected_month = months[month_index]
                downloader.download_month_data(selected_month)
            else:
                print("Invalid choice")
        except:
            print("Invalid input")
    elif choice == '3':
        # Check database status for all months
        for month_year in ['March 2025', 'April 2025', 'May 2025', 'June 2025', 'July 2025', 'August 2025']:
            existing_data = downloader.check_existing_data(month_year)
            print(f"\n📊 {month_year}:")
            if existing_data:
                for date, count in existing_data.items():
                    print(f"   {date}: {count:,} records")
            else:
                print(f"   No data found")
    else:
        print("Invalid choice")

if __name__ == "__main__":
    main()