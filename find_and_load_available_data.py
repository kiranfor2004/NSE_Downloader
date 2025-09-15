#!/usr/bin/env python3

import requests
import zipfile
import pandas as pd
import pyodbc
import json
import os
import time
from datetime import datetime, timedelta
from io import StringIO

def find_available_dates_and_load():
    """Find available dates and load data for February 2024 (more realistic)"""
    
    print("🔍 FINDING AVAILABLE F&O DATA AND LOADING")
    print("="*60)
    
    # Try February 2024 instead (more likely to be available)
    start_date = datetime(2024, 2, 15)
    end_date = datetime(2024, 2, 28)
    
    base_url = "https://archives.nseindia.com/products/content/derivatives/equities/"
    
    try:
        # Connect to database
        with open('database_config.json', 'r') as f:
            config = json.load(f)
        
        conn_str = f"DRIVER={config['driver']};SERVER={config['server']};DATABASE={config['database']};Trusted_Connection=yes;"
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        print("💾 Connected to database")
        
        # Check which dates already exist
        print("\n🔍 Checking existing data...")
        existing_dates_query = """
        SELECT DISTINCT trade_date 
        FROM step04_fo_udiff_daily 
        WHERE trade_date BETWEEN '20240215' AND '20240228'
        ORDER BY trade_date
        """
        cursor.execute(existing_dates_query)
        existing_dates = [row[0] for row in cursor.fetchall()]
        
        if existing_dates:
            print(f"   Found existing data for: {existing_dates}")
        else:
            print(f"   No existing data found in this range")
        
        total_inserted = 0
        successful_dates = []
        failed_dates = []
        available_dates = []
        
        # First, let's check what dates are available
        print(f"\n🔍 CHECKING AVAILABILITY (Feb 2024):")
        print("-" * 50)
        
        # Process each date
        current_date = start_date
        while current_date <= end_date:
            date_str = current_date.strftime('%Y%m%d')
            date_display = current_date.strftime('%d-%m-%Y')
            
            print(f"   📅 Checking {date_display} ({date_str})...", end="")
            
            # Skip weekends
            if current_date.weekday() >= 5:  # 5=Saturday, 6=Sunday
                print(f" ⏭️ Weekend")
                current_date += timedelta(days=1)
                continue
            
            # Skip if data already exists
            if date_str in existing_dates:
                print(f" ✅ Already loaded")
                available_dates.append(date_str)
                current_date += timedelta(days=1)
                continue
            
            # Construct download URL
            udiff_filename = f"udiff_{date_str}.zip"
            download_url = f"{base_url}{udiff_filename}"
            
            try:
                # Quick HEAD request to check availability
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                }
                
                response = requests.head(download_url, headers=headers, timeout=10)
                
                if response.status_code == 200:
                    print(f" ✅ Available")
                    available_dates.append(date_str)
                elif response.status_code == 404:
                    print(f" ❌ Not found")
                else:
                    print(f" ⚠️ HTTP {response.status_code}")
                
            except requests.exceptions.RequestException as e:
                print(f" ❌ Error: {e}")
            
            time.sleep(0.5)  # Small delay
            current_date += timedelta(days=1)
        
        # Now download and load available dates
        if available_dates:
            print(f"\n📥 LOADING AVAILABLE DATES:")
            print("-" * 50)
            
            for date_str in available_dates:
                if date_str in existing_dates:
                    continue  # Skip already loaded
                
                date_obj = datetime.strptime(date_str, '%Y%m%d')
                date_display = date_obj.strftime('%d-%m-%Y')
                
                print(f"\n📅 Loading {date_display} ({date_str})...")
                
                udiff_filename = f"udiff_{date_str}.zip"
                download_url = f"{base_url}{udiff_filename}"
                
                try:
                    print(f"   🌐 Downloading: {udiff_filename}")
                    
                    response = requests.get(download_url, headers=headers, timeout=30)
                    
                    if response.status_code == 200:
                        print(f"   ✅ Downloaded ({len(response.content):,} bytes)")
                        
                        # Save zip file
                        zip_path = f"temp_{udiff_filename}"
                        with open(zip_path, 'wb') as f:
                            f.write(response.content)
                        
                        # Extract and process
                        try:
                            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                                zip_files = zip_ref.namelist()
                                print(f"   📁 Files in zip: {zip_files}")
                                
                                # Find the CSV file
                                csv_file = None
                                for file in zip_files:
                                    if file.lower().endswith('.csv'):
                                        csv_file = file
                                        break
                                
                                if csv_file:
                                    print(f"   📄 Processing CSV: {csv_file}")
                                    
                                    # Read CSV content
                                    with zip_ref.open(csv_file) as f:
                                        csv_content = f.read().decode('utf-8')
                                    
                                    # Parse CSV
                                    df = pd.read_csv(StringIO(csv_content))
                                    print(f"   📊 Records found: {len(df):,}")
                                    
                                    if len(df) > 0:
                                        # Show sample of column structure
                                        print(f"   🔍 Columns: {list(df.columns)[:10]}...")
                                        
                                        # Process and insert data
                                        records_inserted = process_and_insert_udiff_data(df, date_str, cursor, conn)
                                        total_inserted += records_inserted
                                        successful_dates.append(date_str)
                                        print(f"   ✅ Inserted {records_inserted:,} records")
                                    else:
                                        print(f"   ⚠️  No data found in CSV")
                                        failed_dates.append((date_str, "No data in CSV"))
                                else:
                                    print(f"   ❌ No CSV file found in zip")
                                    failed_dates.append((date_str, "No CSV file in zip"))
                        
                        except zipfile.BadZipFile:
                            print(f"   ❌ Invalid zip file")
                            failed_dates.append((date_str, "Invalid zip file"))
                        
                        finally:
                            # Clean up temp file
                            if os.path.exists(zip_path):
                                os.remove(zip_path)
                    
                    else:
                        print(f"   ❌ Download failed: HTTP {response.status_code}")
                        failed_dates.append((date_str, f"HTTP {response.status_code}"))
                
                except Exception as e:
                    print(f"   ❌ Error: {e}")
                    failed_dates.append((date_str, f"Error: {e}"))
                
                time.sleep(1)  # Delay between downloads
        
        else:
            print(f"\n⚠️  No available dates found for February 2024")
            print(f"💡 The NSE archives might not have UDiFF data for this period")
            print(f"🔄 Let's try a different approach...")
            
            # Try some known good dates
            test_dates = [
                "20240215", "20240216", "20240219", "20240220", "20240221", 
                "20240222", "20240223", "20240226", "20240227", "20240228"
            ]
            
            print(f"\n🧪 TESTING SPECIFIC DATES:")
            print("-" * 50)
            
            for test_date in test_dates:
                test_obj = datetime.strptime(test_date, '%Y%m%d')
                if test_obj.weekday() >= 5:  # Skip weekends
                    continue
                
                print(f"   Testing {test_obj.strftime('%d-%m-%Y')}...", end="")
                
                udiff_filename = f"udiff_{test_date}.zip"
                download_url = f"{base_url}{udiff_filename}"
                
                try:
                    response = requests.head(download_url, headers=headers, timeout=10)
                    if response.status_code == 200:
                        print(f" ✅ Available")
                        available_dates.append(test_date)
                    else:
                        print(f" ❌ Not found")
                except:
                    print(f" ❌ Error")
                
                time.sleep(0.5)
        
        # Summary
        print(f"\n📊 LOADING SUMMARY:")
        print("="*60)
        print(f"🔍 Available dates found: {len(available_dates)}")
        print(f"✅ Successfully loaded: {len(successful_dates)} dates")
        print(f"❌ Failed: {len(failed_dates)} dates")
        print(f"📈 Total records inserted: {total_inserted:,}")
        
        if available_dates:
            print(f"\n📅 Available dates:")
            for date in available_dates:
                date_obj = datetime.strptime(date, '%Y%m%d')
                status = "✅ Loaded" if date in successful_dates else "⏭️ Existing" if date in existing_dates else "❌ Failed"
                print(f"   {date_obj.strftime('%d-%m-%Y')} ({date}) - {status}")
        
        conn.close()
        
        print(f"\n{'='*60}")
        if successful_dates:
            print(f"🎉 Successfully loaded data for {len(successful_dates)} date(s)!")
        else:
            print(f"⚠️  No new data was loaded")
            print(f"💡 This might be because:")
            print(f"   - Files are not available on NSE archives")
            print(f"   - Data already exists in database")
            print(f"   - Network connectivity issues")
        print(f"{'='*60}")
        
        return len(successful_dates) > 0
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

def process_and_insert_udiff_data(df, date_str, cursor, conn):
    """Process UDiFF DataFrame and insert into database"""
    
    # Prepare insert query (same as before)
    insert_query = """
    INSERT INTO step04_fo_udiff_daily (
        trade_date, BizDt, Sgmt, Src, instrument, FinInstrmId, ISIN, symbol, SctySrs,
        expiry_date, FininstrmActlXpryDt, strike_price, option_type, FinInstrmNm,
        open_price, high_price, low_price, close_price, LastPric, PrvsClsgPric,
        UndrlygPric, settle_price, open_interest, change_in_oi, contracts_traded,
        value_in_lakh, TtlNbOfTxsExctd, SsnId, NewBrdLotQty, Rmks,
        Rsvd1, Rsvd2, Rsvd3, Rsvd4
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    inserted_count = 0
    batch_size = 1000
    
    for i in range(0, len(df), batch_size):
        batch = df.iloc[i:i+batch_size]
        batch_data = []
        
        for _, row in batch.iterrows():
            # Handle NaN values
            def handle_nan(value):
                if pd.isna(value):
                    return None
                return value
            
            # Convert dates
            trade_date = date_str
            biz_date = date_str
            
            # Handle expiry date conversion (flexible format handling)
            expiry_date = None
            actual_expiry = None
            
            try:
                if 'XpryDt' in row and not pd.isna(row['XpryDt']):
                    expiry_str = str(row['XpryDt'])
                    if '-' in expiry_str:
                        expiry_date = pd.to_datetime(expiry_str, format='%d-%m-%Y').strftime('%Y%m%d')
                    elif len(expiry_str) == 8 and expiry_str.isdigit():
                        expiry_date = expiry_str
                    else:
                        expiry_date = expiry_str
            except:
                pass
            
            try:
                if 'FininstrmActlXpryDt' in row and not pd.isna(row['FininstrmActlXpryDt']):
                    actual_str = str(row['FininstrmActlXpryDt'])
                    if '-' in actual_str:
                        actual_expiry = pd.to_datetime(actual_str, format='%d-%m-%Y').strftime('%Y%m%d')
                    elif len(actual_str) == 8 and actual_str.isdigit():
                        actual_expiry = actual_str
                    else:
                        actual_expiry = actual_str
            except:
                pass
            
            row_data = (
                trade_date,                                    # trade_date
                biz_date,                                      # BizDt
                handle_nan(row.get('Sgmt', 'FO')),           # Sgmt
                handle_nan(row.get('Src', 'NSE')),           # Src
                handle_nan(row.get('FinInstrmTp')),          # instrument
                handle_nan(row.get('FinInstrmId')),          # FinInstrmId
                handle_nan(row.get('ISIN')),                 # ISIN
                handle_nan(row.get('TckrSymb')),             # symbol
                handle_nan(row.get('SctySrs')),              # SctySrs
                expiry_date,                                  # expiry_date
                actual_expiry,                               # FininstrmActlXpryDt
                handle_nan(row.get('StrkPric')),             # strike_price
                handle_nan(row.get('OptnTp')),               # option_type
                handle_nan(row.get('FinInstrmNm')),          # FinInstrmNm
                handle_nan(row.get('OpnPric')),              # open_price
                handle_nan(row.get('HghPric')),              # high_price
                handle_nan(row.get('LwPric')),               # low_price
                handle_nan(row.get('ClsPric')),              # close_price
                handle_nan(row.get('LastPric')),             # LastPric
                handle_nan(row.get('PrvsClsgPric')),         # PrvsClsgPric
                handle_nan(row.get('UndrlygPric')),          # UndrlygPric
                handle_nan(row.get('SttlmPric')),            # settle_price
                handle_nan(row.get('OpnIntrst')),            # open_interest
                handle_nan(row.get('ChngInOpnIntrst')),      # change_in_oi
                handle_nan(row.get('TtlTradgVol')),          # contracts_traded
                handle_nan(row.get('TtlTrfVal')),            # value_in_lakh
                handle_nan(row.get('TtlNbOfTxsExctd')),      # TtlNbOfTxsExctd
                handle_nan(row.get('SsnId')),                # SsnId
                handle_nan(row.get('NewBrdLotQty')),         # NewBrdLotQty
                handle_nan(row.get('Rmks')),                 # Rmks
                handle_nan(row.get('Rsvd1')),                # Rsvd1
                handle_nan(row.get('Rsvd2')),                # Rsvd2
                handle_nan(row.get('Rsvd3')),                # Rsvd3
                handle_nan(row.get('Rsvd4'))                 # Rsvd4
            )
            batch_data.append(row_data)
        
        # Execute batch
        cursor.executemany(insert_query, batch_data)
        conn.commit()
        inserted_count += len(batch_data)
    
    return inserted_count

if __name__ == "__main__":
    success = find_available_dates_and_load()
    if success:
        print(f"✅ Loading completed successfully")
    else:
        print(f"⚠️  No new data loaded")
