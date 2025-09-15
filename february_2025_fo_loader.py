"""
Download and load February 2025 F&O data from NSE archives
"""
import requests
import zipfile
import pandas as pd
import pyodbc
import os
import time
from datetime import datetime, timedelta
import io

class FebruaryFOLoader:
    def __init__(self):
        self.base_url = "https://nsearchives.nseindia.com/content/fo/"
        self.session = requests.Session()
        
        # Headers to mimic browser
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        
        # Database connection
        self.conn_string = (
            'Driver={ODBC Driver 17 for SQL Server};'
            'Server=SRIKIRANREDDY\\SQLEXPRESS;'
            'Database=master;'
            'Trusted_Connection=yes;'
            'TrustServerCertificate=yes;'
        )
        
        # Column mapping from NSE F&O file to our database
        self.column_mapping = {
            'TckrSymb': 'symbol',
            'Sgmt': 'instrument', 
            'Srs': 'instrument',
            'OpnPric': 'open_price',
            'HghPric': 'high_price', 
            'LwPric': 'low_price',
            'ClsPric': 'close_price',
            'SttlmntPric': 'settle_price',
            'TtlTradgVol': 'contracts_traded',
            'TtlTrfVal': 'value_in_lakh',
            'OpnIntrst': 'open_interest',
            'ChngInOpnIntrst': 'change_in_oi',
            'TradDt': 'trade_date',
            'XpryDt': 'expiry_date',
            'StrkPric': 'strike_price',
            'OptTp': 'option_type',
            'UndrlygSymb': 'underlying'
        }

    def get_february_2025_dates(self):
        """Generate list of trading days in February 2025"""
        trading_dates = []
        
        # February 2025 trading days (excluding weekends and holidays)
        february_dates = [
            '2025-02-03', '2025-02-04', '2025-02-05', '2025-02-06', '2025-02-07',
            '2025-02-10', '2025-02-11', '2025-02-12', '2025-02-13', '2025-02-14',
            '2025-02-17', '2025-02-18', '2025-02-19', '2025-02-20', '2025-02-21',
            '2025-02-24', '2025-02-25', '2025-02-26', '2025-02-27', '2025-02-28'
        ]
        
        return february_dates

    def download_fo_file(self, date_str):
        """Download F&O file for a specific date"""
        try:
            # Convert date format
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
            formatted_date = date_obj.strftime('%d%m%Y')
            
            # NSE F&O file URL pattern
            filename = f"BhavCopy_NSE_FO_0_0_0_{formatted_date}_F_0000.csv.zip"
            url = self.base_url + filename
            
            print(f"📥 Downloading: {filename}")
            
            response = self.session.get(url, timeout=30)
            
            if response.status_code == 200:
                print(f"✅ Downloaded: {filename} ({len(response.content)} bytes)")
                return response.content
            else:
                print(f"❌ Failed to download {filename}: HTTP {response.status_code}")
                return None
                
        except Exception as e:
            print(f"❌ Error downloading {date_str}: {e}")
            return None

    def process_fo_zip(self, zip_content, trade_date):
        """Process downloaded ZIP file and extract F&O data"""
        try:
            with zipfile.ZipFile(io.BytesIO(zip_content)) as zip_file:
                csv_files = [f for f in zip_file.namelist() if f.endswith('.csv')]
                
                if not csv_files:
                    print(f"❌ No CSV file found in ZIP")
                    return None
                
                csv_file = csv_files[0]
                print(f"📊 Processing CSV: {csv_file}")
                
                # Read CSV from ZIP
                with zip_file.open(csv_file) as csv_data:
                    df = pd.read_csv(csv_data)
                
                print(f"📋 Raw data shape: {df.shape}")
                print(f"📋 Columns: {list(df.columns)}")
                
                # Check for expected F&O columns
                if 'TckrSymb' not in df.columns:
                    print(f"❌ Invalid F&O file format - missing TckrSymb column")
                    return None
                
                # Filter for derivatives data (exclude equity)
                fo_data = df[df['Sgmt'].isin(['FUTIDX', 'FUTSTK', 'OPTIDX', 'OPTSTK'])].copy()
                
                print(f"📊 F&O records found: {len(fo_data)}")
                
                if len(fo_data) == 0:
                    print(f"❌ No F&O data found for {trade_date}")
                    return None
                
                # Add source file info
                fo_data['source_file'] = csv_file
                fo_data['trade_date_formatted'] = trade_date.replace('-', '')
                
                return fo_data
                
        except Exception as e:
            print(f"❌ Error processing ZIP file: {e}")
            return None

    def save_to_database(self, df, trade_date):
        """Save F&O data to SQL Server database"""
        try:
            conn = pyodbc.connect(self.conn_string)
            cur = conn.cursor()
            
            print(f"💾 Saving {len(df)} records to database...")
            
            # Prepare data for insertion
            records = []
            
            for _, row in df.iterrows():
                record = (
                    row.get('trade_date_formatted', trade_date.replace('-', '')),
                    str(row.get('TckrSymb', '')),
                    str(row.get('Sgmt', '')),
                    str(row.get('XpryDt', '')),
                    float(row.get('StrkPric', 0)) if pd.notna(row.get('StrkPric')) else 0,
                    str(row.get('OptTp', '')),
                    float(row.get('OpnPric', 0)) if pd.notna(row.get('OpnPric')) else 0,
                    float(row.get('HghPric', 0)) if pd.notna(row.get('HghPric')) else 0,
                    float(row.get('LwPric', 0)) if pd.notna(row.get('LwPric')) else 0,
                    float(row.get('ClsPric', 0)) if pd.notna(row.get('ClsPric')) else 0,
                    float(row.get('SttlmntPric', 0)) if pd.notna(row.get('SttlmntPric')) else 0,
                    int(row.get('TtlTradgVol', 0)) if pd.notna(row.get('TtlTradgVol')) else 0,
                    float(row.get('TtlTrfVal', 0)) if pd.notna(row.get('TtlTrfVal')) else 0,
                    int(row.get('OpnIntrst', 0)) if pd.notna(row.get('OpnIntrst')) else 0,
                    int(row.get('ChngInOpnIntrst', 0)) if pd.notna(row.get('ChngInOpnIntrst')) else 0,
                    str(row.get('UndrlygSymb', '')),
                    str(row.get('source_file', ''))
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
            
            print(f"✅ Saved {len(records)} F&O records for {trade_date}")
            
            conn.close()
            return len(records)
            
        except Exception as e:
            print(f"❌ Database error: {e}")
            return 0

    def clear_february_data(self):
        """Clear existing February 2025 data"""
        try:
            conn = pyodbc.connect(self.conn_string)
            cur = conn.cursor()
            
            cur.execute("DELETE FROM step04_fo_udiff_daily WHERE trade_date LIKE '202502%'")
            deleted = cur.rowcount
            conn.commit()
            conn.close()
            
            print(f"🗑️ Cleared {deleted} existing February 2025 records")
            return True
            
        except Exception as e:
            print(f"❌ Error clearing data: {e}")
            return False

    def load_february_2025(self):
        """Main function to load February 2025 F&O data"""
        print("🚀 Loading February 2025 F&O Data from NSE")
        print("=" * 50)
        
        # Clear existing data
        self.clear_february_data()
        
        # Get February trading dates
        trading_dates = self.get_february_2025_dates()
        print(f"📅 Processing {len(trading_dates)} trading days in February 2025")
        
        total_records = 0
        successful_dates = 0
        
        for date_str in trading_dates:
            print(f"\n📆 Processing {date_str}...")
            
            # Download file
            zip_content = self.download_fo_file(date_str)
            if zip_content is None:
                continue
            
            # Process ZIP file
            fo_data = self.process_fo_zip(zip_content, date_str)
            if fo_data is None:
                continue
            
            # Save to database
            records_saved = self.save_to_database(fo_data, date_str)
            if records_saved > 0:
                total_records += records_saved
                successful_dates += 1
            
            # Small delay to be respectful to NSE servers
            time.sleep(1)
        
        print(f"\n" + "=" * 50)
        print(f"🎯 FEBRUARY 2025 F&O DATA LOADING COMPLETE")
        print(f"=" * 50)
        print(f"✅ Successful dates: {successful_dates}/{len(trading_dates)}")
        print(f"✅ Total F&O records loaded: {total_records:,}")
        print(f"📊 Average records per day: {total_records//successful_dates if successful_dates > 0 else 0:,}")
        print(f"\n🔍 Test query in SSMS:")
        print(f"SELECT COUNT(*) FROM step04_fo_udiff_daily WHERE trade_date LIKE '202502%';")
        print(f"SELECT TOP 10 * FROM step04_fo_udiff_daily WHERE trade_date LIKE '202502%' ORDER BY created_at DESC;")

def main():
    loader = FebruaryFOLoader()
    loader.load_february_2025()

if __name__ == "__main__":
    main()
