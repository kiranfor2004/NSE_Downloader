"""
NSE F&O UDiFF Common Bhavcopy Final Downloader
Downloads the correct F&O - UDiFF Common Bhavcopy Final (zip) files from NSE India
"""
import requests
import zipfile
import pandas as pd
import pyodbc
import os
import time
from datetime import datetime
import io

class NSE_FO_UDiFF_Downloader:
    def __init__(self):
        # Correct NSE UDiFF endpoint
        self.base_url = "https://archives.nseindia.com/products/content/derivatives/equities/"
        self.session = requests.Session()
        
        # Headers to mimic browser
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Referer': 'https://www.nseindia.com/'
        })
        
        # Database connection
        self.conn_string = (
            'Driver={ODBC Driver 17 for SQL Server};'
            'Server=SRIKIRANREDDY\\SQLEXPRESS;'
            'Database=master;'
            'Trusted_Connection=yes;'
            'TrustServerCertificate=yes;'
        )
        
        # Column mapping for UDiFF F&O files
        self.udiff_column_mapping = {
            'INSTRUMENT': 'instrument',
            'SYMBOL': 'symbol',
            'EXPIRY_DT': 'expiry_date',
            'STRIKE_PR': 'strike_price',
            'OPTION_TYP': 'option_type',
            'OPEN': 'open_price',
            'HIGH': 'high_price',
            'LOW': 'low_price',
            'CLOSE': 'close_price',
            'SETTLE_PR': 'settle_price',
            'CONTRACTS': 'contracts_traded',
            'VAL_INLAKH': 'value_in_lakh',
            'OPEN_INT': 'open_interest',
            'CHG_IN_OI': 'change_in_oi',
            'TIMESTAMP': 'trade_date'
        }

    def get_historical_dates(self):
        """Get available historical dates for testing (Aug-Sep 2025)"""
        historical_dates = [
            # August 2025 (should be available)
            '2025-08-01', '2025-08-04', '2025-08-05', '2025-08-06', '2025-08-07',
            '2025-08-08', '2025-08-11', '2025-08-12', '2025-08-13', '2025-08-14',
            '2025-08-15', '2025-08-18', '2025-08-19', '2025-08-20', '2025-08-21',
            '2025-08-22', '2025-08-25', '2025-08-26', '2025-08-27', '2025-08-28',
            '2025-08-29',
            
            # September 2025 (current month - partial)
            '2025-09-01', '2025-09-02', '2025-09-03', '2025-09-04', '2025-09-05',
            '2025-09-08', '2025-09-09', '2025-09-10', '2025-09-11', '2025-09-12'
        ]
        
        return historical_dates

    def download_udiff_file(self, date_str):
        """Download F&O UDiFF Common Bhavcopy Final file for a specific date"""
        try:
            # Convert date format
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
            formatted_date = date_obj.strftime('%d%m%Y')
            
            # Correct NSE F&O UDiFF file pattern
            filename = f"udiff_{formatted_date}.zip"
            url = self.base_url + filename
            
            print(f"📥 Downloading F&O UDiFF: {filename}")
            print(f"🔗 URL: {url}")
            
            response = self.session.get(url, timeout=30)
            
            if response.status_code == 200:
                print(f"✅ Downloaded: {filename} ({len(response.content)} bytes)")
                return response.content
            else:
                print(f"❌ Failed to download {filename}: HTTP {response.status_code}")
                if response.status_code == 404:
                    print(f"   File may not exist for this date")
                return None
                
        except Exception as e:
            print(f"❌ Error downloading {date_str}: {e}")
            return None

    def process_udiff_zip(self, zip_content, trade_date):
        """Process downloaded UDiFF ZIP file and extract F&O data"""
        try:
            with zipfile.ZipFile(io.BytesIO(zip_content)) as zip_file:
                csv_files = [f for f in zip_file.namelist() if f.endswith('.csv')]
                
                if not csv_files:
                    print(f"❌ No CSV file found in UDiFF ZIP")
                    return None
                
                csv_file = csv_files[0]
                print(f"📊 Processing UDiFF CSV: {csv_file}")
                
                # Read CSV from ZIP
                with zip_file.open(csv_file) as csv_data:
                    df = pd.read_csv(csv_data)
                
                print(f"📋 Raw UDiFF data shape: {df.shape}")
                print(f"📋 Columns: {list(df.columns)}")
                
                # Check for expected UDiFF columns
                expected_cols = ['INSTRUMENT', 'SYMBOL', 'EXPIRY_DT', 'OPEN', 'HIGH', 'LOW', 'CLOSE']
                missing_cols = [col for col in expected_cols if col not in df.columns]
                
                if missing_cols:
                    print(f"❌ Missing expected UDiFF columns: {missing_cols}")
                    print(f"Available columns: {list(df.columns)}")
                    return None
                
                # Filter for F&O derivatives only
                fo_instruments = ['FUTIDX', 'FUTSTK', 'OPTIDX', 'OPTSTK']
                fo_data = df[df['INSTRUMENT'].isin(fo_instruments)].copy()
                
                print(f"📊 F&O UDiFF records found: {len(fo_data)}")
                
                if len(fo_data) == 0:
                    print(f"❌ No F&O data found in UDiFF file for {trade_date}")
                    return None
                
                # Add source file info
                fo_data['source_file'] = csv_file
                fo_data['trade_date_formatted'] = trade_date.replace('-', '')
                
                return fo_data
                
        except Exception as e:
            print(f"❌ Error processing UDiFF ZIP file: {e}")
            return None

    def save_udiff_to_database(self, df, trade_date):
        """Save F&O UDiFF data to SQL Server database"""
        try:
            conn = pyodbc.connect(self.conn_string)
            cur = conn.cursor()
            
            print(f"💾 Saving {len(df)} UDiFF F&O records to database...")
            
            # Prepare data for insertion
            records = []
            
            for _, row in df.iterrows():
                # Handle date conversion for expiry
                expiry_date = ''
                if pd.notna(row.get('EXPIRY_DT')):
                    try:
                        expiry_dt = pd.to_datetime(row.get('EXPIRY_DT'))
                        expiry_date = expiry_dt.strftime('%Y%m%d')
                    except:
                        expiry_date = str(row.get('EXPIRY_DT', ''))
                
                record = (
                    row.get('trade_date_formatted', trade_date.replace('-', '')),
                    str(row.get('SYMBOL', '')),
                    str(row.get('INSTRUMENT', '')),
                    expiry_date,
                    float(row.get('STRIKE_PR', 0)) if pd.notna(row.get('STRIKE_PR')) else 0,
                    str(row.get('OPTION_TYP', '')),
                    float(row.get('OPEN', 0)) if pd.notna(row.get('OPEN')) else 0,
                    float(row.get('HIGH', 0)) if pd.notna(row.get('HIGH')) else 0,
                    float(row.get('LOW', 0)) if pd.notna(row.get('LOW')) else 0,
                    float(row.get('CLOSE', 0)) if pd.notna(row.get('CLOSE')) else 0,
                    float(row.get('SETTLE_PR', 0)) if pd.notna(row.get('SETTLE_PR')) else 0,
                    int(row.get('CONTRACTS', 0)) if pd.notna(row.get('CONTRACTS')) else 0,
                    float(row.get('VAL_INLAKH', 0)) if pd.notna(row.get('VAL_INLAKH')) else 0,
                    int(row.get('OPEN_INT', 0)) if pd.notna(row.get('OPEN_INT')) else 0,
                    int(row.get('CHG_IN_OI', 0)) if pd.notna(row.get('CHG_IN_OI')) else 0,
                    str(row.get('SYMBOL', '')),  # underlying symbol
                    f"udiff_{trade_date.replace('-', '')}.zip"
                )
                records.append(record)
            
            # Insert data
            insert_sql = """
            INSERT INTO step04_fo_udiff_daily 
            (trade_date, symbol, instrument, expiry_date, strike_price, option_type, 
             open_price, high_price, low_price, close_price, settle_price, 
             contracts_traded, value_in_lakh, open_interest, change_in_oi, underlying, source_file)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            cur.executemany(insert_sql, records)
            conn.commit()
            
            print(f"✅ Saved {len(records)} UDiFF F&O records for {trade_date}")
            
            conn.close()
            return len(records)
            
        except Exception as e:
            print(f"❌ Database error: {e}")
            return 0

    def clear_historical_data(self, month_pattern='202508'):
        """Clear existing historical data for the specified month"""
        try:
            conn = pyodbc.connect(self.conn_string)
            cur = conn.cursor()
            
            cur.execute(f"DELETE FROM step04_fo_udiff_daily WHERE trade_date LIKE '{month_pattern}%'")
            deleted = cur.rowcount
            conn.commit()
            conn.close()
            
            print(f"🗑️ Cleared {deleted} existing records for {month_pattern}")
            return True
            
        except Exception as e:
            print(f"❌ Error clearing data: {e}")
            return False

    def download_historical_udiff_data(self, target_month='2025-08'):
        """Main function to download historical F&O UDiFF data"""
        print("🚀 NSE F&O UDiFF Common Bhavcopy Final Downloader")
        print("=" * 60)
        print("📋 Downloading the CORRECT F&O UDiFF files from NSE India")
        print("🎯 File: F&O - UDiFF Common Bhavcopy Final (zip)")
        print("🔗 Source: archives.nseindia.com/products/content/derivatives/equities/")
        print("=" * 60)
        
        # Clear existing data for the month
        month_code = target_month.replace('-', '')[2:]  # 202508 from 2025-08
        self.clear_historical_data(f'2025{month_code[2:]}')
        
        # Get historical trading dates
        historical_dates = self.get_historical_dates()
        target_dates = [d for d in historical_dates if d.startswith(target_month)]
        
        print(f"📅 Processing {len(target_dates)} trading days for {target_month}")
        
        total_records = 0
        successful_dates = 0
        failed_dates = []
        
        for date_str in target_dates:
            print(f"\n📆 Processing {date_str}...")
            
            # Download UDiFF file
            zip_content = self.download_udiff_file(date_str)
            if zip_content is None:
                failed_dates.append(date_str)
                continue
            
            # Process UDiFF ZIP file
            udiff_data = self.process_udiff_zip(zip_content, date_str)
            if udiff_data is None:
                failed_dates.append(date_str)
                continue
            
            # Save to database
            records_saved = self.save_udiff_to_database(udiff_data, date_str)
            if records_saved > 0:
                total_records += records_saved
                successful_dates += 1
            else:
                failed_dates.append(date_str)
            
            # Small delay to be respectful to NSE servers
            time.sleep(1)
        
        print(f"\n" + "=" * 60)
        print(f"🎯 F&O UDiFF DOWNLOAD COMPLETE")
        print(f"=" * 60)
        print(f"✅ Successful dates: {successful_dates}/{len(target_dates)}")
        print(f"✅ Total F&O UDiFF records loaded: {total_records:,}")
        print(f"📊 Average records per day: {total_records//successful_dates if successful_dates > 0 else 0:,}")
        
        if failed_dates:
            print(f"❌ Failed dates: {len(failed_dates)}")
            for date in failed_dates[:5]:  # Show first 5 failed dates
                print(f"   • {date}")
            if len(failed_dates) > 5:
                print(f"   • ... and {len(failed_dates) - 5} more")
        
        print(f"\n🔍 Test queries in SSMS:")
        month_pattern = target_month.replace('-', '')[2:]
        print(f"SELECT COUNT(*) FROM step04_fo_udiff_daily WHERE trade_date LIKE '2025{month_pattern[2:]}%';")
        print(f"SELECT TOP 10 * FROM step04_fo_udiff_daily WHERE trade_date LIKE '2025{month_pattern[2:]}%' ORDER BY trade_date DESC;")
        print(f"SELECT DISTINCT instrument FROM step04_fo_udiff_daily WHERE trade_date LIKE '2025{month_pattern[2:]}%';")

def main():
    downloader = NSE_FO_UDiFF_Downloader()
    
    print("🎯 CHOOSE DOWNLOAD OPTION:")
    print("1. August 2025 (Recent historical data)")
    print("2. September 2025 (Current month - partial)")
    
    choice = input("Enter choice (1 or 2): ")
    
    if choice == "1":
        downloader.download_historical_udiff_data('2025-08')
    elif choice == "2":
        downloader.download_historical_udiff_data('2025-09')
    else:
        print("Defaulting to August 2025...")
        downloader.download_historical_udiff_data('2025-08')

if __name__ == "__main__":
    main()
