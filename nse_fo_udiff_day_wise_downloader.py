"""
NSE F&O UDiFF Day-wise Downloader - Fast single day downloads
Downloads the correct "F&O - UDiFF Common Bhavcopy Final (zip)" files from NSE
"""
import requests
import zipfile
import pandas as pd
import pyodbc
import os
import time
from datetime import datetime
import io

class NSEFOUDiFFDayDownloader:
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
        })
        
        # Database connection
        self.conn_string = (
            'Driver={ODBC Driver 17 for SQL Server};'
            'Server=SRIKIRANREDDY\\SQLEXPRESS;'
            'Database=master;'
            'Trusted_Connection=yes;'
            'TrustServerCertificate=yes;'
        )

    def download_single_day_udiff(self, date_str):
        """Download UDiFF file for a single day (format: DDMMYYYY or YYYY-MM-DD)"""
        try:
            # Convert date format if needed
            if '-' in date_str:
                date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                formatted_date = date_obj.strftime('%d%m%Y')
            else:
                formatted_date = date_str
                
            # Correct UDiFF filename pattern
            filename = f"udiff_{formatted_date}.zip"
            url = self.base_url + filename
            
            print(f"🚀 Downloading: {filename}")
            print(f"📡 URL: {url}")
            
            response = self.session.get(url, timeout=30, stream=True)
            
            if response.status_code == 200:
                file_size = len(response.content)
                print(f"✅ Downloaded: {filename} ({file_size:,} bytes)")
                
                # Save to local file for inspection
                os.makedirs("UDiFF_Downloads", exist_ok=True)
                local_path = os.path.join("UDiFF_Downloads", filename)
                
                with open(local_path, 'wb') as f:
                    f.write(response.content)
                print(f"💾 Saved to: {local_path}")
                
                return response.content, formatted_date
            else:
                print(f"❌ Failed to download {filename}: HTTP {response.status_code}")
                if response.status_code == 404:
                    print(f"   File may not exist for this date")
                return None, None
                
        except Exception as e:
            print(f"❌ Error downloading {date_str}: {e}")
            return None, None

    def extract_and_analyze_udiff(self, zip_content, trade_date):
        """Extract and analyze UDiFF ZIP file"""
        try:
            print(f"📊 Analyzing UDiFF data for {trade_date}...")
            
            with zipfile.ZipFile(io.BytesIO(zip_content)) as zip_file:
                file_list = zip_file.namelist()
                print(f"📁 Files in ZIP: {file_list}")
                
                # Find CSV files
                csv_files = [f for f in file_list if f.endswith('.csv')]
                
                if not csv_files:
                    print(f"❌ No CSV files found in ZIP")
                    return None
                
                csv_file = csv_files[0]
                print(f"📋 Processing CSV: {csv_file}")
                
                # Read CSV from ZIP
                with zip_file.open(csv_file) as csv_data:
                    # Try different encodings
                    try:
                        df = pd.read_csv(csv_data, encoding='utf-8')
                    except:
                        csv_data.seek(0)
                        df = pd.read_csv(csv_data, encoding='latin-1')
                
                print(f"📊 Data shape: {df.shape}")
                print(f"📋 Columns: {list(df.columns)}")
                
                # Show first few rows
                print(f"📈 Sample data:")
                print(df.head(3).to_string())
                
                # Check for F&O data
                if 'SYMBOL' in df.columns or 'TckrSymb' in df.columns:
                    # Try to identify F&O instruments
                    if 'INSTRUMENT' in df.columns:
                        instruments = df['INSTRUMENT'].unique()
                        print(f"🔍 Instruments found: {instruments}")
                        
                        # Filter F&O data
                        fo_instruments = ['FUTIDX', 'FUTSTK', 'OPTIDX', 'OPTSTK']
                        fo_data = df[df['INSTRUMENT'].isin(fo_instruments)]
                        
                        if len(fo_data) > 0:
                            print(f"✅ F&O records found: {len(fo_data)}")
                            return fo_data
                        else:
                            print(f"⚠️ No F&O data found, showing all data")
                            return df
                    else:
                        print(f"⚠️ No INSTRUMENT column, showing all data")
                        return df
                else:
                    print(f"⚠️ Unknown column structure, showing all data")
                    return df
                
        except Exception as e:
            print(f"❌ Error processing ZIP file: {e}")
            return None

    def save_udiff_to_database(self, df, trade_date):
        """Save UDiFF F&O data to database"""
        try:
            if df is None or len(df) == 0:
                print(f"❌ No data to save")
                return False
                
            conn = pyodbc.connect(self.conn_string)
            cur = conn.cursor()
            
            print(f"💾 Saving {len(df)} UDiFF records to database...")
            
            # Clear existing data for this date
            cur.execute("DELETE FROM step04_fo_udiff_daily WHERE trade_date = ?", trade_date)
            deleted = cur.rowcount
            if deleted > 0:
                print(f"🗑️ Cleared {deleted} existing records for {trade_date}")
            
            # Prepare data for insertion
            records = []
            
            for _, row in df.iterrows():
                # Map UDiFF columns to our database schema
                # Adjust column names based on actual UDiFF structure
                record = (
                    trade_date,
                    str(row.get('SYMBOL', row.get('TckrSymb', ''))),
                    str(row.get('INSTRUMENT', row.get('Sgmt', ''))),
                    str(row.get('EXPIRY_DT', row.get('XpryDt', ''))),
                    float(row.get('STRIKE_PR', row.get('StrkPric', 0))) if pd.notna(row.get('STRIKE_PR', row.get('StrkPric'))) else 0,
                    str(row.get('OPTION_TYP', row.get('OptTp', ''))),
                    float(row.get('OPEN', row.get('OpnPric', 0))) if pd.notna(row.get('OPEN', row.get('OpnPric'))) else 0,
                    float(row.get('HIGH', row.get('HghPric', 0))) if pd.notna(row.get('HIGH', row.get('HghPric'))) else 0,
                    float(row.get('LOW', row.get('LwPric', 0))) if pd.notna(row.get('LOW', row.get('LwPric'))) else 0,
                    float(row.get('CLOSE', row.get('ClsPric', 0))) if pd.notna(row.get('CLOSE', row.get('ClsPric'))) else 0,
                    float(row.get('SETTLE_PR', row.get('SttlmntPric', 0))) if pd.notna(row.get('SETTLE_PR', row.get('SttlmntPric'))) else 0,
                    int(row.get('CONTRACTS', row.get('TtlTradgVol', 0))) if pd.notna(row.get('CONTRACTS', row.get('TtlTradgVol'))) else 0,
                    float(row.get('VAL_INLAKH', row.get('TtlTrfVal', 0))) if pd.notna(row.get('VAL_INLAKH', row.get('TtlTrfVal'))) else 0,
                    int(row.get('OPEN_INT', row.get('OpnIntrst', 0))) if pd.notna(row.get('OPEN_INT', row.get('OpnIntrst'))) else 0,
                    int(row.get('CHG_IN_OI', row.get('ChngInOpnIntrst', 0))) if pd.notna(row.get('CHG_IN_OI', row.get('ChngInOpnIntrst'))) else 0,
                    str(row.get('UNDERLYING', row.get('UndrlygSymb', ''))),
                    f'udiff_{trade_date}.zip'
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
            
            # Show summary
            cur.execute("""
                SELECT 
                    COUNT(*) as total,
                    COUNT(DISTINCT symbol) as symbols,
                    COUNT(DISTINCT instrument) as instruments
                FROM step04_fo_udiff_daily 
                WHERE trade_date = ?
            """, trade_date)
            
            stats = cur.fetchone()
            print(f"📊 Summary for {trade_date}: {stats[0]} records, {stats[1]} symbols, {stats[2]} instruments")
            
            conn.close()
            return True
            
        except Exception as e:
            print(f"❌ Database error: {e}")
            return False

    def download_single_day_fo_udiff(self, date_input):
        """Main function to download and process single day F&O UDiFF data"""
        print(f"🚀 NSE F&O UDiFF Single Day Downloader")
        print(f"=" * 50)
        print(f"📅 Target date: {date_input}")
        print(f"📁 File type: F&O - UDiFF Common Bhavcopy Final (zip)")
        print(f"🌐 Source: NSE Archives")
        
        # Download UDiFF file
        zip_content, formatted_date = self.download_single_day_udiff(date_input)
        
        if zip_content is None:
            print(f"❌ Failed to download UDiFF data for {date_input}")
            return False
        
        # Extract and analyze
        fo_data = self.extract_and_analyze_udiff(zip_content, formatted_date)
        
        if fo_data is None:
            print(f"❌ Failed to extract UDiFF data for {date_input}")
            return False
        
        # Save to database
        success = self.save_udiff_to_database(fo_data, formatted_date)
        
        if success:
            print(f"\n🎯 SUCCESS! UDiFF F&O data downloaded for {date_input}")
            print(f"✅ File: udiff_{formatted_date}.zip")
            print(f"✅ Records: {len(fo_data)}")
            print(f"✅ Location: master.dbo.step04_fo_udiff_daily")
            
            print(f"\n🔍 Test query in SSMS:")
            print(f"SELECT * FROM step04_fo_udiff_daily WHERE trade_date = '{formatted_date}';")
            return True
        else:
            print(f"❌ Failed to save UDiFF data to database")
            return False

def main():
    downloader = NSEFOUDiFFDayDownloader()
    
    # Example usage - download specific date
    print("📋 Choose download option:")
    print("1. Enter specific date (YYYY-MM-DD or DDMMYYYY)")
    print("2. Use example: August 1, 2025")
    
    choice = input("Enter choice (1 or 2): ").strip()
    
    if choice == "1":
        date_input = input("Enter date (YYYY-MM-DD or DDMMYYYY): ").strip()
    else:
        date_input = "2025-08-01"  # Example date
        print(f"Using example date: {date_input}")
    
    # Download single day
    downloader.download_single_day_fo_udiff(date_input)

if __name__ == "__main__":
    main()
