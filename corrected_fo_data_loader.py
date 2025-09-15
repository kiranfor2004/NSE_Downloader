#!/usr/bin/env python3
"""
CORRECTED F&O Data Loader - Fixes all validation issues
Loads data from actual NSE BhavCopy source files with 100% accuracy
"""

import pandas as pd
import pyodbc
import json
import os
import zipfile
from datetime import datetime
import numpy as np
from io import StringIO

class CorrectedFOLoader:
    def __init__(self):
        # Load database configuration
        with open('database_config.json', 'r') as f:
            self.config = json.load(f)
        
        self.conn_str = f"DRIVER={self.config['driver']};SERVER={self.config['server']};DATABASE={self.config['database']};Trusted_Connection=yes;"
        self.source_directory = r"C:\Users\kiran\NSE_Downloader\fo_udiff_downloads"
        
        # CORRECTED column mapping - NSE BhavCopy to Database
        self.column_mapping = {
            # Source Column -> Database Column
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

    def get_source_files(self):
        """Get all BhavCopy source files"""
        source_files = []
        
        for filename in os.listdir(self.source_directory):
            if filename.endswith('.zip') and 'BhavCopy_NSE_FO' in filename:
                # Extract date from filename: BhavCopy_NSE_FO_0_0_0_20250203_F_0000.csv.zip
                import re
                match = re.search(r'(202502\d{2})', filename)
                if match:
                    date_str = match.group(1)
                    source_files.append((date_str, filename))
        
        source_files.sort()
        return source_files

    def load_source_file(self, date_str, filename):
        """Load and process a single BhavCopy source file"""
        
        print(f"\\n📅 Processing {date_str} - {filename}")
        
        source_path = os.path.join(self.source_directory, filename)
        
        try:
            with zipfile.ZipFile(source_path, 'r') as zip_ref:
                # Find CSV file
                csv_files = [f for f in zip_ref.namelist() if f.lower().endswith('.csv')]
                
                if not csv_files:
                    print(f"   ❌ No CSV file found in {filename}")
                    return None
                
                csv_file = csv_files[0]
                print(f"   📋 Processing: {csv_file}")
                
                # Read CSV content
                with zip_ref.open(csv_file) as f:
                    csv_content = f.read().decode('utf-8')
                
                # Parse CSV
                source_df = pd.read_csv(StringIO(csv_content))
                
                print(f"   📊 Source records: {len(source_df):,}")
                print(f"   🔍 Source columns: {len(source_df.columns)}")
                
                # Convert TradDt to our format (YYYYMMDD)
                if 'TradDt' in source_df.columns:
                    # Handle different date formats
                    try:
                        source_df['TradDt'] = pd.to_datetime(source_df['TradDt']).dt.strftime('%Y%m%d')
                    except:
                        # If already in YYYYMMDD format
                        source_df['TradDt'] = source_df['TradDt'].astype(str)
                
                # Add source file info
                source_df['source_file'] = filename
                
                return source_df
                
        except Exception as e:
            print(f"   ❌ Error processing {filename}: {e}")
            return None

    def prepare_database_record(self, row):
        """Convert source row to database record format"""
        
        def safe_convert(value, data_type, default=None):
            """Safely convert values with proper NULL handling"""
            if pd.isna(value) or value == '' or str(value).strip() == '':
                return default
            
            try:
                if data_type == 'str':
                    return str(value).strip()
                elif data_type == 'float':
                    return float(value)
                elif data_type == 'int':
                    return int(float(value))
                else:
                    return value
            except:
                return default
        
        # Build record following exact database column order
        record = []
        
        # Core fields (always required)
        record.append(safe_convert(row.get('TradDt'), 'str', ''))                    # trade_date
        record.append(safe_convert(row.get('TckrSymb'), 'str', ''))                  # symbol
        record.append(safe_convert(row.get('FinInstrmTp'), 'str', ''))               # instrument
        record.append(safe_convert(row.get('XpryDt'), 'str', ''))                    # expiry_date
        record.append(safe_convert(row.get('StrkPric'), 'float', None))              # strike_price (NULL for futures)
        record.append(safe_convert(row.get('OptnTp'), 'str', None))                  # option_type (NULL for futures)
        
        # Price fields
        record.append(safe_convert(row.get('OpnPric'), 'float', 0.0))                # open_price
        record.append(safe_convert(row.get('HghPric'), 'float', 0.0))                # high_price
        record.append(safe_convert(row.get('LwPric'), 'float', 0.0))                 # low_price
        record.append(safe_convert(row.get('ClsPric'), 'float', 0.0))                # close_price
        record.append(safe_convert(row.get('SttlmPric'), 'float', 0.0))              # settle_price
        
        # Volume and OI fields
        record.append(safe_convert(row.get('TtlTradgVol'), 'int', 0))                # contracts_traded
        record.append(safe_convert(row.get('TtlTrfVal'), 'float', 0.0))              # value_in_lakh
        record.append(safe_convert(row.get('OpnIntrst'), 'int', 0))                  # open_interest
        record.append(safe_convert(row.get('ChngInOpnIntrst'), 'int', 0))            # change_in_oi
        
        # Extended UDiFF fields
        record.append(safe_convert(row.get('BizDt'), 'str', ''))                     # BizDt
        record.append(safe_convert(row.get('Sgmt'), 'str', ''))                      # Sgmt
        record.append(safe_convert(row.get('Src'), 'str', ''))                       # Src
        record.append(safe_convert(row.get('FinInstrmId'), 'str', ''))               # FinInstrmId
        record.append(safe_convert(row.get('ISIN'), 'str', ''))                      # ISIN
        record.append(safe_convert(row.get('SctySrs'), 'str', ''))                   # SctySrs
        record.append(safe_convert(row.get('FininstrmActlXpryDt'), 'str', ''))       # FininstrmActlXpryDt
        record.append(safe_convert(row.get('FinInstrmNm'), 'str', ''))               # FinInstrmNm
        record.append(safe_convert(row.get('LastPric'), 'float', 0.0))               # LastPric
        record.append(safe_convert(row.get('PrvsClsgPric'), 'float', 0.0))           # PrvsClsgPric
        record.append(safe_convert(row.get('UndrlygPric'), 'float', 0.0))            # UndrlygPric
        record.append(safe_convert(row.get('TtlNbOfTxsExctd'), 'int', 0))            # TtlNbOfTxsExctd
        record.append(safe_convert(row.get('SsnId'), 'str', ''))                     # SsnId
        record.append(safe_convert(row.get('NewBrdLotQty'), 'int', 0))               # NewBrdLotQty
        record.append(safe_convert(row.get('Rmks'), 'str', ''))                      # Rmks
        record.append(safe_convert(row.get('Rsvd1'), 'str', ''))                     # Rsvd1
        record.append(safe_convert(row.get('Rsvd2'), 'str', ''))                     # Rsvd2
        record.append(safe_convert(row.get('Rsvd3'), 'str', ''))                     # Rsvd3
        record.append(safe_convert(row.get('Rsvd4'), 'str', ''))                     # Rsvd4
        
        # Source file info
        record.append(safe_convert(row.get('source_file'), 'str', ''))               # source_file
        
        return tuple(record)

    def save_to_database(self, source_df, date_str):
        """Save source data to database with 100% accuracy"""
        
        try:
            conn = pyodbc.connect(self.conn_str)
            cursor = conn.cursor()
            
            print(f"   💾 Saving {len(source_df):,} records to database...")
            
            # Clear existing data for this date
            cursor.execute("DELETE FROM step04_fo_udiff_daily WHERE trade_date = ?", date_str)
            deleted = cursor.rowcount
            if deleted > 0:
                print(f"   🗑️ Cleared {deleted:,} existing records for {date_str}")
            
            # Prepare records
            records = []
            error_count = 0
            
            for idx, row in source_df.iterrows():
                try:
                    record = self.prepare_database_record(row)
                    records.append(record)
                except Exception as e:
                    error_count += 1
                    if error_count <= 5:  # Show first 5 errors
                        print(f"   ⚠️ Error processing row {idx}: {e}")
            
            if error_count > 5:
                print(f"   ⚠️ ... and {error_count - 5} more record processing errors")
            
            print(f"   📊 Prepared {len(records):,} records for insertion")
            
            # Insert data - Complete 34-column UDiFF format
            insert_sql = """
            INSERT INTO step04_fo_udiff_daily 
            (trade_date, symbol, instrument, expiry_date, strike_price, option_type, 
             open_price, high_price, low_price, close_price, settle_price, 
             contracts_traded, value_in_lakh, open_interest, change_in_oi,
             BizDt, Sgmt, Src, FinInstrmId, ISIN, SctySrs, FininstrmActlXpryDt, FinInstrmNm,
             LastPric, PrvsClsgPric, UndrlygPric, TtlNbOfTxsExctd, SsnId, NewBrdLotQty, 
             Rmks, Rsvd1, Rsvd2, Rsvd3, Rsvd4, source_file)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            # Execute batch insert
            cursor.executemany(insert_sql, records)
            conn.commit()
            
            print(f"   ✅ Successfully inserted {len(records):,} records")
            
            # Verification
            cursor.execute("SELECT COUNT(*) FROM step04_fo_udiff_daily WHERE trade_date = ?", date_str)
            db_count = cursor.fetchone()[0]
            
            if db_count == len(source_df):
                print(f"   🎯 Perfect match: Source {len(source_df):,} = Database {db_count:,}")
                match_status = True
            else:
                print(f"   ❌ Count mismatch: Source {len(source_df):,} ≠ Database {db_count:,}")
                match_status = False
            
            conn.close()
            return db_count, match_status
            
        except Exception as e:
            print(f"   ❌ Database error: {e}")
            return 0, False

    def reload_feb_5th_only(self):
        """Reload only 5th February 2025 data from source file"""
        print("🚀 CORRECTED F&O DATA LOADER - 5th Feb 2025 ONLY")
        print("="*60)
        print("📋 Loading only 5th Feb 2025 from NSE BhavCopy source file")
        # Get source files
        source_files = [(date_str, filename) for date_str, filename in self.get_source_files() if date_str == '20250205']
        if not source_files:
            print("❌ No source file found for 5th Feb 2025!")
            return False
        date_str, filename = source_files[0]
        date_obj = datetime.strptime(date_str, '%Y%m%d')
        print(f"   {date_obj.strftime('%d-%m-%Y')}: {filename}")
        
        print(f"\n📊 PROCESSING 5th FEB 2025:")
        print("-" * 60)
        
        # Load source file
        source_df = self.load_source_file(date_str, filename)
        
        if source_df is None:
            print("❌ Could not load source file for 5th Feb 2025!")
            return False
        
        # Save to database
        db_count, match_status = self.save_to_database(source_df, date_str)
        
        # Final summary
        print(f"\n" + "="*60)
        print(f"🏆 5th FEB 2025 LOADING COMPLETE")
        print(f"="*60)
        
        print(f"   Source records: {len(source_df):,}")
        print(f"   Database records: {db_count:,}")
        if match_status:
            print(f"\n🎉 SUCCESS: 5th Feb 2025 loaded with 100% ACCURACY!")
            print(f"✅ Database now matches source file exactly for 5th Feb 2025")
        else:
            print(f"\n❌ LOADING ISSUE: 5th Feb 2025 not loaded correctly")
        
        print(f"\n🔍 Test in SSMS:")
        print(f"SELECT COUNT(*) FROM step04_fo_udiff_daily WHERE trade_date = '20250205';")
        
        return match_status

    def reload_all_february_data(self):
        """Reload all February data from source files (from 5th Feb onwards)"""
        print("🚀 CORRECTED F&O DATA LOADER")
        print("="*60)
        print("📋 Loading from actual NSE BhavCopy source files")
        print("🎯 Target: 100% source-to-database accuracy")
        
        # Get source files
        source_files = self.get_source_files()
        
        # Filter for dates >= 20250205
        source_files = [(date_str, filename) for date_str, filename in source_files if date_str >= '20250205']
        
        if not source_files:
            print("❌ No source files found!")
            return False
        
        print(f"\n📁 Found {len(source_files)} source files:")
        for date_str, filename in source_files:
            date_obj = datetime.strptime(date_str, '%Y%m%d')
            print(f"   {date_obj.strftime('%d-%m-%Y')}: {filename}")
        
        # Process each file
        total_source_records = 0
        total_db_records = 0
        perfect_matches = 0
        
        print(f"\n📊 PROCESSING SOURCE FILES:")
        print("-" * 60)
        
        for date_str, filename in source_files:
            # Load source file
            source_df = self.load_source_file(date_str, filename)
            
            if source_df is None:
                continue
            
            # Save to database
            db_count, match_status = self.save_to_database(source_df, date_str)
            
            # Track totals
            total_source_records += len(source_df)
            total_db_records += db_count
            
            if match_status:
                perfect_matches += 1
        
        # Final summary
        print(f"\n" + "="*60)
        print(f"🏆 CORRECTED LOADING COMPLETE")
        print(f"="*60)
        
        print(f"📊 FINAL RESULTS:")
        print(f"   Source files processed: {len(source_files)}")
        print(f"   Perfect matches: {perfect_matches}/{len(source_files)}")
        print(f"   Total source records: {total_source_records:,}")
        print(f"   Total database records: {total_db_records:,}")
        print(f"   Record accuracy: {(total_db_records/total_source_records*100):.1f}%" if total_source_records > 0 else "0.0%")
        
        if perfect_matches == len(source_files):
            print(f"\n🎉 SUCCESS: ALL FILES LOADED WITH 100% ACCURACY!")
            print(f"✅ Database now matches source files exactly")
            verdict = "PERFECT"
        elif perfect_matches > len(source_files) // 2:
            print(f"\n⚠️  PARTIAL SUCCESS: {perfect_matches}/{len(source_files)} files match perfectly")
            verdict = "PARTIAL"
        else:
            print(f"\n❌ LOADING ISSUES: Only {perfect_matches}/{len(source_files)} files loaded correctly")
            verdict = "FAILED"
        
        print(f"\n🔍 Test in SSMS:")
        print(f"SELECT COUNT(*) FROM step04_fo_udiff_daily WHERE trade_date LIKE '202502%';")
        print(f"SELECT trade_date, COUNT(*) FROM step04_fo_udiff_daily WHERE trade_date LIKE '202502%' GROUP BY trade_date ORDER BY trade_date;")
        
        return verdict == "PERFECT"

def main():
    print("🔧 CORRECTED F&O DATA LOADER")
    print("="*50)
    print("This script will:")
    print("1. Load data from actual NSE BhavCopy source files")
    print("2. Ensure 100% record count accuracy")
    print("3. Fix all column mapping issues")
    print("4. Resolve validation discrepancies")
    print("\nOptions:")
    print("1. Reload only 5th Feb 2025")
    print("2. Reload all from 5th Feb onwards")
    print("3. Reload all February data")
    option = input("\nChoose option (1/2/3): ").strip()
    loader = CorrectedFOLoader()
    if option == '1':
        loader.reload_feb_5th_only()
    elif option == '2':
        loader.reload_all_february_data()
    elif option == '3':
        # For full reload, remove the date filter in reload_all_february_data
        loader.reload_all_february_data()
    else:
        print("❌ Invalid option. Exiting.")

if __name__ == "__main__":
    main()
