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

def load_february_15_to_28_data():
    """Load F&O UDiFF data from Feb 15 to Feb 28, 2025"""
    
    print("📥 LOADING F&O DATA: FEB 15-28, 2025")
    print("="*60)
    
    # Date range setup
    start_date = datetime(2025, 2, 15)
    end_date = datetime(2025, 2, 28)
    
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
        WHERE trade_date BETWEEN '20250215' AND '20250228'
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
        
        # Process each date
        current_date = start_date
        while current_date <= end_date:
            date_str = current_date.strftime('%Y%m%d')
            date_display = current_date.strftime('%d-%m-%Y')
            
            print(f"\n📅 Processing {date_display} ({date_str})...")
            
            # Skip if data already exists
            if date_str in existing_dates:
                print(f"   ⏭️  Skipping - data already exists")
                current_date += timedelta(days=1)
                continue
            
            # Check if it's a trading day (skip Saturdays and Sundays)
            if current_date.weekday() >= 5:  # 5=Saturday, 6=Sunday
                print(f"   ⏭️  Skipping weekend")
                current_date += timedelta(days=1)
                continue
            
            # Construct download URL
            udiff_filename = f"udiff_{date_str}.zip"
            download_url = f"{base_url}{udiff_filename}"
            
            try:
                print(f"   🌐 Downloading: {download_url}")
                
                # Download with headers to mimic browser
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                }
                
                response = requests.get(download_url, headers=headers, timeout=30)
                
                if response.status_code == 200:
                    print(f"   ✅ Downloaded successfully ({len(response.content):,} bytes)")
                    
                    # Save zip file
                    zip_path = f"temp_{udiff_filename}"
                    with open(zip_path, 'wb') as f:
                        f.write(response.content)
                    
                    # Extract and process
                    try:
                        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                            # List files in zip
                            zip_files = zip_ref.namelist()
                            print(f"   📁 Files in zip: {zip_files}")
                            
                            # Find the CSV file (usually named like fo{date}bhav.csv or similar)
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
                                    # Process and insert data
                                    records_inserted = process_and_insert_data(df, date_str, cursor, conn)
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
                
                elif response.status_code == 404:
                    print(f"   ⚠️  File not found (404) - likely no trading day")
                    failed_dates.append((date_str, "File not found (404)"))
                else:
                    print(f"   ❌ Download failed: HTTP {response.status_code}")
                    failed_dates.append((date_str, f"HTTP {response.status_code}"))
            
            except requests.exceptions.RequestException as e:
                print(f"   ❌ Network error: {e}")
                failed_dates.append((date_str, f"Network error: {e}"))
            
            except Exception as e:
                print(f"   ❌ Processing error: {e}")
                failed_dates.append((date_str, f"Processing error: {e}"))
            
            # Add delay to avoid overwhelming the server
            time.sleep(1)
            
            current_date += timedelta(days=1)
        
        # Summary
        print(f"\n📊 LOADING SUMMARY:")
        print("="*60)
        print(f"✅ Successfully loaded: {len(successful_dates)} dates")
        print(f"❌ Failed: {len(failed_dates)} dates")
        print(f"📈 Total records inserted: {total_inserted:,}")
        
        if successful_dates:
            print(f"\n✅ Successful dates:")
            for date in successful_dates:
                date_obj = datetime.strptime(date, '%Y%m%d')
                print(f"   {date_obj.strftime('%d-%m-%Y')} ({date})")
        
        if failed_dates:
            print(f"\n❌ Failed dates:")
            for date, reason in failed_dates:
                date_obj = datetime.strptime(date, '%Y%m%d')
                print(f"   {date_obj.strftime('%d-%m-%Y')} ({date}): {reason}")
        
        # Final verification
        print(f"\n🧪 FINAL VERIFICATION:")
        print("-" * 50)
        
        verification_query = """
        SELECT 
            trade_date,
            COUNT(*) as record_count,
            COUNT(DISTINCT symbol) as unique_symbols,
            COUNT(DISTINCT instrument) as instrument_types
        FROM step04_fo_udiff_daily 
        WHERE trade_date BETWEEN '20250215' AND '20250228'
        GROUP BY trade_date
        ORDER BY trade_date
        """
        
        cursor.execute(verification_query)
        verification_results = cursor.fetchall()
        
        if verification_results:
            print("Date Range Summary:")
            total_records = 0
            for trade_date, count, symbols, instruments in verification_results:
                date_obj = datetime.strptime(trade_date, '%Y%m%d')
                print(f"  {date_obj.strftime('%d-%m-%Y')}: {count:,} records, {symbols} symbols, {instruments} instrument types")
                total_records += count
            
            print(f"\nGrand Total: {total_records:,} records across all dates")
        else:
            print("No data found in the specified range")
        
        conn.close()
        
        print(f"\n{'='*60}")
        print(f"🎉 Data loading completed!")
        print(f"📊 Range: Feb 15-28, 2025")
        print(f"📈 Total records: {total_inserted:,}")
        print(f"{'='*60}")
        
        return len(successful_dates) > 0
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

def process_and_insert_data(df, date_str, cursor, conn):
    """Process DataFrame and insert into database"""
    
    # Convert date format and prepare data
    df_processed = df.copy()
    
    # Map CSV columns to database columns based on UDiFF format
    column_mapping = {
        'TradDt': 'trade_date',
        'BizDt': 'BizDt', 
        'Sgmt': 'Sgmt',
        'Src': 'Src',
        'FinInstrmTp': 'instrument',
        'FinInstrmId': 'FinInstrmId',
        'ISIN': 'ISIN',
        'TckrSymb': 'symbol',
        'SctySrs': 'SctySrs',
        'XpryDt': 'expiry_date',
        'FininstrmActlXpryDt': 'FininstrmActlXpryDt',
        'StrkPric': 'strike_price',
        'OptnTp': 'option_type',
        'FinInstrmNm': 'FinInstrmNm',
        'OpnPric': 'open_price',
        'HghPric': 'high_price', 
        'LwPric': 'low_price',
        'ClsPric': 'close_price',
        'LastPric': 'LastPric',
        'PrvsClsgPric': 'PrvsClsgPric',
        'UndrlygPric': 'UndrlygPric',
        'SttlmPric': 'settle_price',
        'OpnIntrst': 'open_interest',
        'ChngInOpnIntrst': 'change_in_oi',
        'TtlTradgVol': 'contracts_traded',
        'TtlTrfVal': 'value_in_lakh',
        'TtlNbOfTxsExctd': 'TtlNbOfTxsExctd',
        'SsnId': 'SsnId',
        'NewBrdLotQty': 'NewBrdLotQty',
        'Rmks': 'Rmks',
        'Rsvd1': 'Rsvd1',
        'Rsvd2': 'Rsvd2', 
        'Rsvd3': 'Rsvd3',
        'Rsvd4': 'Rsvd4'
    }
    
    # Prepare insert query
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
            
            # Handle expiry date conversion
            try:
                if 'XpryDt' in row and not pd.isna(row['XpryDt']):
                    if isinstance(row['XpryDt'], str) and '-' in str(row['XpryDt']):
                        expiry_date = pd.to_datetime(row['XpryDt'], format='%d-%m-%Y').strftime('%Y%m%d')
                    else:
                        expiry_date = str(row['XpryDt'])
                else:
                    expiry_date = None
            except:
                expiry_date = None
            
            # Handle actual expiry date
            try:
                if 'FininstrmActlXpryDt' in row and not pd.isna(row['FininstrmActlXpryDt']):
                    if isinstance(row['FininstrmActlXpryDt'], str) and '-' in str(row['FininstrmActlXpryDt']):
                        actual_expiry = pd.to_datetime(row['FininstrmActlXpryDt'], format='%d-%m-%Y').strftime('%Y%m%d')
                    else:
                        actual_expiry = str(row['FininstrmActlXpryDt'])
                else:
                    actual_expiry = None
            except:
                actual_expiry = None
            
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
    success = load_february_15_to_28_data()
    if success:
        print(f"✅ Loading completed successfully")
    else:
        print(f"❌ Loading failed")
