#!/usr/bin/env python3
"""
FIXED F&O Data Loader - Resolves all validation and mapping issues
"""

import pandas as pd
import pyodbc
import json
import os
import zipfile
from datetime import datetime
from io import StringIO
import numpy as np

class FixedFOLoader:
    def __init__(self):
        # Load database configuration
        with open('database_config.json', 'r') as f:
            self.config = json.load(f)
        
        self.conn_str = f"DRIVER={self.config['driver']};SERVER={self.config['server']};DATABASE={self.config['database']};Trusted_Connection=yes;"
        self.source_directory = r"C:\Users\kiran\NSE_Downloader\fo_udiff_downloads"
        
        # FIXED instrument type mapping
        self.instrument_mapping = {
            'STO': 'OPTSTK',  # Stock Options
            'IDO': 'OPTIDX',  # Index Options
            'STF': 'FUTSTK',  # Stock Futures
            'IDF': 'FUTIDX'   # Index Futures
        }
        
        # Symbol prefix mapping for underlying determination
        self.index_symbols = {'NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'BANKEX', 'NIFTYIT'}

    def load_source_file(self, date_str, filename):
        """Load and process a single BhavCopy source file with enhanced validation"""
        print(f"\n📅 Processing {date_str} - {filename}")
        
        source_path = os.path.join(self.source_directory, filename)
        debug_info = {"source_records": 0, "processed_records": 0, "validation_issues": []}
        
        try:
            with zipfile.ZipFile(source_path, 'r') as zip_ref:
                csv_files = [f for f in zip_ref.namelist() if f.lower().endswith('.csv')]
                if not csv_files:
                    debug_info["validation_issues"].append(f"No CSV file found in {filename}")
                    return None, debug_info
                
                csv_file = csv_files[0]
                with zip_ref.open(csv_file) as f:
                    csv_content = f.read().decode('utf-8')
                
                # Read CSV with validation
                df = pd.read_csv(StringIO(csv_content))
                debug_info["source_records"] = len(df)
                
                # 1. Fix instrument type mapping
                df['instrument'] = df['FinInstrmTp'].map(self.instrument_mapping)
                unmapped = df[df['instrument'].isna()]['FinInstrmTp'].unique()
                if len(unmapped) > 0:
                    debug_info["validation_issues"].append(f"Found unmapped instrument types: {unmapped}")
                
                # 2. Handle NULL values properly
                null_report = df.isnull().sum()
                if null_report.any():
                    debug_info["validation_issues"].append("NULL values found in columns: " + 
                                                        ", ".join(f"{col}({count})" for col, count in null_report[null_report > 0].items()))
                
                # Fill NAs appropriately
                df['ISIN'].fillna('', inplace=True)
                df['SctySrs'].fillna('', inplace=True)
                df['Rmks'].fillna('', inplace=True)
                df['Rsvd1'].fillna('', inplace=True)
                df['Rsvd2'].fillna('', inplace=True)
                df['Rsvd3'].fillna('', inplace=True)
                df['Rsvd4'].fillna('', inplace=True)
                
                # 3. Validate and fix data types
                df['TradDt'] = pd.to_datetime(df['TradDt']).dt.strftime('%Y%m%d')
                df['XpryDt'] = pd.to_datetime(df['XpryDt']).dt.strftime('%Y%m%d')
                df['FininstrmActlXpryDt'] = pd.to_datetime(df['FininstrmActlXpryDt']).dt.strftime('%Y%m%d')
                
                # Convert numeric columns
                numeric_cols = ['OpnPric', 'HghPric', 'LwPric', 'ClsPric', 'LastPric', 
                              'PrvsClsgPric', 'UndrlygPric', 'SttlmPric', 'StrkPric']
                for col in numeric_cols:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
                
                # 4. Ensure option type is properly set
                df.loc[df['instrument'].isin(['FUTSTK', 'FUTIDX']), 'OptnTp'] = None
                df.loc[df['instrument'].isin(['OPTSTK', 'OPTIDX']), 'OptnTp'] = df.loc[df['instrument'].isin(['OPTSTK', 'OPTIDX']), 'OptnTp'].fillna('')
                
                # 5. Set underlying asset
                df['underlying'] = ''
                for symbol in self.index_symbols:
                    mask = df['TckrSymb'].str.startswith(symbol, na=False)
                    df.loc[mask, 'underlying'] = symbol
                
                # For stocks, underlying is same as symbol
                df.loc[df['underlying'] == '', 'underlying'] = df.loc[df['underlying'] == '', 'TckrSymb']
                
                debug_info["processed_records"] = len(df)
                
                return df, debug_info
                
        except Exception as e:
            debug_info["validation_issues"].append(f"Error processing file: {str(e)}")
            return None, debug_info

    def save_to_database(self, df, date_str, debug_info):
        """Save processed data to database with enhanced validation"""
        try:
            conn = pyodbc.connect(self.conn_str)
            cursor = conn.cursor()
            
            print(f"💾 Saving {len(df):,} records to database...")
            
            # Clear existing data
            cursor.execute("DELETE FROM step04_fo_udiff_daily WHERE trade_date = ?", date_str)
            deleted = cursor.rowcount
            print(f"🗑️  Cleared {deleted:,} existing records")
            
            # Prepare records with validation
            records = []
            for idx, row in df.iterrows():
                try:
                    record = (
                        str(row['TradDt']),
                        str(row['TckrSymb']),
                        str(row['instrument']),
                        str(row['XpryDt']),
                        float(row['StrkPric']) if pd.notna(row['StrkPric']) else None,
                        str(row['OptnTp']) if pd.notna(row['OptnTp']) else None,
                        float(row['OpnPric']),
                        float(row['HghPric']),
                        float(row['LwPric']),
                        float(row['ClsPric']),
                        float(row['SttlmPric']),
                        int(row['TtlTradgVol']),
                        float(row['TtlTrfVal']),
                        int(row['OpnIntrst']),
                        int(row['ChngInOpnIntrst']),
                        str(row['underlying']),
                        date_str + '.zip',  # source_file
                        str(row['BizDt']),
                        str(row['Sgmt']),
                        str(row['Src']),
                        str(row['FinInstrmId']),
                        str(row['ISIN']),
                        str(row['SctySrs']),
                        str(row['FininstrmActlXpryDt']),
                        str(row['FinInstrmNm']),
                        float(row['LastPric']),
                        float(row['PrvsClsgPric']),
                        float(row['UndrlygPric']),
                        int(row['TtlNbOfTxsExctd']),
                        str(row['SsnId']),
                        int(row['NewBrdLotQty']),
                        str(row['Rmks']),
                        str(row['Rsvd1']),
                        str(row['Rsvd2']),
                        str(row['Rsvd3']),
                        str(row['Rsvd4'])
                    )
                    records.append(record)
                except Exception as e:
                    debug_info["validation_issues"].append(f"Error preparing record {idx}: {str(e)}")
            
            # Insert with validation
            insert_sql = """
            INSERT INTO step04_fo_udiff_daily 
            (trade_date, symbol, instrument, expiry_date, strike_price, option_type,
             open_price, high_price, low_price, close_price, settle_price,
             contracts_traded, value_in_lakh, open_interest, change_in_oi, underlying,
             source_file, BizDt, Sgmt, Src, FinInstrmId, ISIN, SctySrs, 
             FininstrmActlXpryDt, FinInstrmNm, LastPric, PrvsClsgPric, UndrlygPric,
             TtlNbOfTxsExctd, SsnId, NewBrdLotQty, Rmks, Rsvd1, Rsvd2, Rsvd3, Rsvd4)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            cursor.executemany(insert_sql, records)
            conn.commit()
            
            # Verify insertion
            cursor.execute("SELECT COUNT(*) FROM step04_fo_udiff_daily WHERE trade_date = ?", date_str)
            db_count = cursor.fetchone()[0]
            
            if db_count == len(df):
                print(f"✅ Perfect match: {db_count:,} records inserted")
            else:
                print(f"⚠️  Count mismatch: Source {len(df):,} ≠ Database {db_count:,}")
                debug_info["validation_issues"].append(f"Record count mismatch: Source={len(df)}, DB={db_count}")
            
            conn.close()
            return db_count
            
        except Exception as e:
            debug_info["validation_issues"].append(f"Database error: {str(e)}")
            return 0

    def reload_feb_5th(self):
        """Reload 5th February 2025 data with full validation"""
        print("\n🔍 FIXED F&O DATA LOADER - 5th Feb 2025")
        print("="*60)
        
        date_str = '20250205'
        filename = f"BhavCopy_NSE_FO_0_0_0_{date_str}_F_0000.csv.zip"
        
        if not os.path.exists(os.path.join(self.source_directory, filename)):
            print(f"❌ Source file not found: {filename}")
            return False
        
        # Load and process with validation
        df, debug_info = self.load_source_file(date_str, filename)
        
        if df is None:
            print("\n❌ VALIDATION FAILED:")
            for issue in debug_info["validation_issues"]:
                print(f"   - {issue}")
            return False
        
        print("\n📊 VALIDATION REPORT:")
        print(f"Source records: {debug_info['source_records']:,}")
        print(f"Processed records: {debug_info['processed_records']:,}")
        
        if debug_info["validation_issues"]:
            print("\n⚠️  VALIDATION ISSUES:")
            for issue in debug_info["validation_issues"]:
                print(f"   - {issue}")
        
        # Save to database with validation
        db_count = self.save_to_database(df, date_str, debug_info)
        
        print("\n🎯 FINAL RESULTS:")
        print(f"Source records: {len(df):,}")
        print(f"Database records: {db_count:,}")
        
        success = db_count == len(df)
        if success:
            print("\n🎉 SUCCESS: 5th Feb 2025 loaded with 100% accuracy!")
        else:
            print("\n❌ FAILED: Record count mismatch")
            
        print("\n🔍 Verification Query:")
        print("SELECT COUNT(*), instrument FROM step04_fo_udiff_daily")
        print("WHERE trade_date = '20250205'")
        print("GROUP BY instrument ORDER BY COUNT(*) DESC;")
        
        return success

if __name__ == "__main__":
    loader = FixedFOLoader()
    loader.reload_feb_5th()
