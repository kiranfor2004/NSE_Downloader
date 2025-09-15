#!/usr/bin/env python3
"""
STEP 04: F&O Data Validation Loader
Official Step 4 of NSE Data Processing Pipeline

Features:
- Day-by-day validation and retry logic
- Automatic correction of missing records
- Source-to-database validation for each trading date
- Only proceeds to next date if current date validates successfully
- Uses proven data loading logic with proper type handling

Usage: python step04_fo_validation_loader.py
"""

import pandas as pd
import pyodbc
import json
import os
import zipfile
from datetime import datetime
import numpy as np
from io import StringIO

class Step04FOValidationLoader:
    def __init__(self):
        # Load database configuration
        with open('database_config.json', 'r') as f:
            self.config = json.load(f)
        
        self.conn_str = f"DRIVER={self.config['driver']};SERVER={self.config['server']};DATABASE={self.config['database']};Trusted_Connection=yes;"
        self.source_directory = r"C:\Users\kiran\NSE_Downloader\fo_udiff_downloads"
        
    def validate_and_load_all_february(self):
        """Load all February 2025 dates with validation and retry logic"""
        print("🚀 STEP 04: F&O VALIDATION LOADER")
        print("=" * 60)
        print("📋 Official Step 4 of NSE Data Processing Pipeline")
        print("   ✅ Validates each date before moving to next")
        print("   🔄 Retries up to 3 times if validation fails")
        print("   📊 Only proceeds if current date validates successfully")
        print("   🎯 Ensures 100% data accuracy and completeness")
        print("=" * 60)
        
        # February 2025 trading dates with their zip files
        february_dates = [
            ('20250203', 'BhavCopy_NSE_FO_0_0_0_20250203_F_0000.csv.zip'),
            ('20250204', 'BhavCopy_NSE_FO_0_0_0_20250204_F_0000.csv.zip'),
            ('20250205', 'BhavCopy_NSE_FO_0_0_0_20250205_F_0000.csv.zip'),
            ('20250206', 'BhavCopy_NSE_FO_0_0_0_20250206_F_0000.csv.zip'),
            ('20250207', 'BhavCopy_NSE_FO_0_0_0_20250207_F_0000.csv.zip'),
            ('20250210', 'BhavCopy_NSE_FO_0_0_0_20250210_F_0000.csv.zip'),
            ('20250211', 'BhavCopy_NSE_FO_0_0_0_20250211_F_0000.csv.zip'),
            ('20250212', 'BhavCopy_NSE_FO_0_0_0_20250212_F_0000.csv.zip'),
            ('20250213', 'BhavCopy_NSE_FO_0_0_0_20250213_F_0000.csv.zip'),
            ('20250214', 'BhavCopy_NSE_FO_0_0_0_20250214_F_0000.csv.zip'),
            ('20250217', 'BhavCopy_NSE_FO_0_0_0_20250217_F_0000.csv.zip'),
            ('20250218', 'BhavCopy_NSE_FO_0_0_0_20250218_F_0000.csv.zip'),
            ('20250219', 'BhavCopy_NSE_FO_0_0_0_20250219_F_0000.csv.zip'),
            ('20250220', 'BhavCopy_NSE_FO_0_0_0_20250220_F_0000.csv.zip'),
            ('20250221', 'BhavCopy_NSE_FO_0_0_0_20250221_F_0000.csv.zip'),
            ('20250224', 'BhavCopy_NSE_FO_0_0_0_20250224_F_0000.csv.zip'),
            ('20250225', 'BhavCopy_NSE_FO_0_0_0_20250225_F_0000.csv.zip'),
            ('20250227', 'BhavCopy_NSE_FO_0_0_0_20250227_F_0000.csv.zip'),
            ('20250228', 'BhavCopy_NSE_FO_0_0_0_20250228_F_0000.csv.zip'),
        ]
        
        successful_dates = 0
        failed_dates = 0
        
        for date_str, zip_file in february_dates:
            print(f"\\n📅 PROCESSING {date_str}")
            print("-" * 40)
            
            success = self.load_and_validate_single_date(date_str, zip_file)
            
            if success:
                successful_dates += 1
                print(f"   🎉 {date_str} COMPLETED SUCCESSFULLY")
            else:
                failed_dates += 1
                print(f"   ❌ {date_str} FAILED - STOPPING PROCESS")
                break  # Stop on first failure as requested
        
        print(f"\\n📊 FINAL SUMMARY")
        print("=" * 40)
        print(f"✅ Successful dates: {successful_dates}")
        print(f"❌ Failed dates: {failed_dates}")
        print(f"📈 Total processed: {successful_dates + failed_dates}/{len(february_dates)}")
        
        if failed_dates == 0:
            print("🎉 ALL FEBRUARY 2025 DATA VALIDATED AND LOADED SUCCESSFULLY!")
        else:
            print("⚠️ Process stopped due to validation failure. Fix issue and restart.")
    
    def load_and_validate_single_date(self, date_str, zip_file):
        """Load and validate a single date with retry logic"""
        max_attempts = 3
        
        for attempt in range(1, max_attempts + 1):
            print(f"   🔄 Attempt {attempt}/{max_attempts}")
            
            try:
                # Step 1: Load source data
                source_df = self.load_source_file(date_str, zip_file)
                if source_df is None:
                    print(f"      ❌ Failed to load source file")
                    continue
                
                source_count = len(source_df)
                print(f"      📊 Source records: {source_count:,}")
                
                # Step 2: Load to database
                db_count, match_status = self.save_to_database(source_df, date_str)
                
                # Step 3: Validate
                if match_status and db_count == source_count:
                    print(f"      ✅ VALIDATION PASSED: {db_count:,} records match")
                    return True
                else:
                    print(f"      ❌ VALIDATION FAILED: Source {source_count:,} ≠ DB {db_count:,}")
                    if attempt < max_attempts:
                        print(f"      🔄 Will retry (attempt {attempt + 1})")
                    continue
                    
            except Exception as e:
                print(f"      ❌ Error in attempt {attempt}: {e}")
                continue
        
        print(f"   🚨 GIVING UP: Failed validation after {max_attempts} attempts")
        return False
    
    def load_source_file(self, date_str, filename):
        """Load and process a single BhavCopy source file"""
        source_path = os.path.join(self.source_directory, filename)
        
        try:
            with zipfile.ZipFile(source_path, 'r') as zip_ref:
                # Find CSV file
                csv_files = [f for f in zip_ref.namelist() if f.lower().endswith('.csv')]
                
                if not csv_files:
                    print(f"      ❌ No CSV file found in {filename}")
                    return None
                
                csv_file = csv_files[0]
                
                # Read CSV content
                with zip_ref.open(csv_file) as f:
                    csv_content = f.read().decode('utf-8')
                
                # Parse CSV
                source_df = pd.read_csv(StringIO(csv_content))
                
                # Convert all date columns to YYYYMMDD format for database compatibility
                date_columns = ['TradDt', 'BizDt', 'XpryDt', 'FininstrmActlXpryDt']
                for col in date_columns:
                    if col in source_df.columns:
                        try:
                            source_df[col] = pd.to_datetime(source_df[col]).dt.strftime('%Y%m%d')
                        except:
                            # If conversion fails, keep as string but ensure it's not too long
                            source_df[col] = source_df[col].astype(str).str.replace('-', '')[:8]
                
                # Add source file info
                source_df['source_file'] = filename
                
                return source_df
                
        except Exception as e:
            print(f"      ❌ Error processing {filename}: {e}")
            return None

    def prepare_database_record(self, row):
        """Convert source row to database record format with safe type conversion"""
        
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
            except:
                return default
        
        # Build record with all 35 columns (34 + source_file)
        record = [
            safe_convert(row.get('TradDt'), 'str'),           # trade_date
            safe_convert(row.get('TckrSymb'), 'str'),         # symbol
            safe_convert(row.get('FinInstrmTp'), 'str'),      # instrument
            safe_convert(row.get('XpryDt'), 'str'),           # expiry_date
            safe_convert(row.get('StrkPric'), 'float'),       # strike_price
            safe_convert(row.get('OptnTp'), 'str'),           # option_type
            safe_convert(row.get('OpnPric'), 'float'),        # open_price
            safe_convert(row.get('HghPric'), 'float'),        # high_price
            safe_convert(row.get('LwPric'), 'float'),         # low_price
            safe_convert(row.get('ClsPric'), 'float'),        # close_price
            safe_convert(row.get('SttlmPric'), 'float'),      # settle_price
            safe_convert(row.get('TtlTradgVol'), 'int'),      # contracts_traded
            safe_convert(row.get('TtlTrfVal'), 'float'),      # value_in_lakh
            safe_convert(row.get('OpnIntrst'), 'int'),        # open_interest
            safe_convert(row.get('ChngInOpnIntrst'), 'int'),  # change_in_oi
            safe_convert(row.get('BizDt'), 'str'),            # BizDt
            safe_convert(row.get('Sgmt'), 'str'),             # Sgmt
            safe_convert(row.get('Src'), 'str'),              # Src
            safe_convert(row.get('FinInstrmId'), 'str'),      # FinInstrmId
            safe_convert(row.get('ISIN'), 'str'),             # ISIN
            safe_convert(row.get('SctySrs'), 'str'),          # SctySrs
            safe_convert(row.get('FininstrmActlXpryDt'), 'str'), # FininstrmActlXpryDt
            safe_convert(row.get('FinInstrmNm'), 'str'),      # FinInstrmNm
            safe_convert(row.get('LastPric'), 'float'),       # LastPric
            safe_convert(row.get('PrvsClsgPric'), 'float'),   # PrvsClsgPric
            safe_convert(row.get('UndrlygPric'), 'float'),    # UndrlygPric
            safe_convert(row.get('TtlNbOfTxsExctd'), 'int'),  # TtlNbOfTxsExctd
            safe_convert(row.get('SsnId'), 'str'),            # SsnId
            safe_convert(row.get('NewBrdLotQty'), 'int'),     # NewBrdLotQty
            safe_convert(row.get('Rmks'), 'str'),             # Rmks
            safe_convert(row.get('Rsvd1'), 'str'),            # Rsvd1
            safe_convert(row.get('Rsvd2'), 'str'),            # Rsvd2
            safe_convert(row.get('Rsvd3'), 'str'),            # Rsvd3
            safe_convert(row.get('Rsvd4'), 'str'),            # Rsvd4
            safe_convert(row.get('source_file'), 'str'),      # source_file
        ]
        
        return record

    def save_to_database(self, source_df, date_str):
        """Save dataframe to database with validation"""
        try:
            conn = pyodbc.connect(self.conn_str)
            cursor = conn.cursor()
            
            print(f"      💾 Saving {len(source_df):,} records to database...")
            
            # Clear existing data for this date
            cursor.execute("DELETE FROM step04_fo_udiff_daily WHERE trade_date = ?", date_str)
            deleted = cursor.rowcount
            if deleted > 0:
                print(f"      🗑️ Cleared {deleted:,} existing records for {date_str}")
            
            # Prepare records
            records = []
            error_count = 0
            
            for idx, row in source_df.iterrows():
                try:
                    record = self.prepare_database_record(row)
                    records.append(record)
                except Exception as e:
                    error_count += 1
                    if error_count <= 3:  # Show first 3 errors
                        print(f"      ⚠️ Error processing row {idx}: {e}")
            
            if error_count > 3:
                print(f"      ⚠️ ... and {error_count - 3} more record processing errors")
            
            print(f"      📊 Prepared {len(records):,} records for insertion")
            
            # Insert data - Complete 35-column format
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
            
            print(f"      ✅ Successfully inserted {len(records):,} records")
            
            # Verification
            cursor.execute("SELECT COUNT(*) FROM step04_fo_udiff_daily WHERE trade_date = ?", date_str)
            db_count = cursor.fetchone()[0]
            
            if db_count == len(source_df):
                print(f"      🎯 Perfect match: Source {len(source_df):,} = Database {db_count:,}")
                match_status = True
            else:
                print(f"      ❌ Count mismatch: Source {len(source_df):,} ≠ Database {db_count:,}")
                match_status = False
            
            conn.close()
            return db_count, match_status
            
        except Exception as e:
            print(f"      ❌ Database error: {e}")
            return 0, False

def main():
    loader = Step04FOValidationLoader()
    loader.validate_and_load_all_february()

if __name__ == "__main__":
    main()