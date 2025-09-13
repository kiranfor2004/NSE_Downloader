#!/usr/bin/env python3
"""
Step 01 Data Loader - Complete NSE Daily Bhavcopy to Database

Purpose:
  Load all NSE daily CSV files into step01_equity_daily table.
  Includes ALL market segments: EQ (Equity), SM (SME), BE (Bonds), 
  ST (Structured), GB (Govt Bonds), GS (Govt Securities), BZ, IV, etc.

Usage:
  python step01_equity_data_loader.py --data-pattern "NSE_*_2025_Data/cm*.csv"
  python step01_equity_data_loader.py --month January --year 2025
"""

import pandas as pd
import glob
import os
import argparse
from datetime import datetime
from nse_database_integration import NSEDatabaseManager

COLUMN_MAPPING = {
    'symbol': 'SYMBOL', 'SYMBOL': 'SYMBOL',
    'date': 'DATE', 'DATE': 'DATE', 'DATE1': 'DATE',
    'series': 'SERIES', 'SERIES': 'SERIES',
    'prev_close': 'PREV_CLOSE', 'PREV_CLOSE': 'PREV_CLOSE',
    'open_price': 'OPEN_PRICE', 'OPEN_PRICE': 'OPEN_PRICE', 'open': 'OPEN_PRICE',
    'high_price': 'HIGH_PRICE', 'HIGH_PRICE': 'HIGH_PRICE', 'high': 'HIGH_PRICE',
    'low_price': 'LOW_PRICE', 'LOW_PRICE': 'LOW_PRICE', 'low': 'LOW_PRICE',
    'last_price': 'LAST_PRICE', 'LAST_PRICE': 'LAST_PRICE', 'last': 'LAST_PRICE',
    'close_price': 'CLOSE_PRICE', 'CLOSE_PRICE': 'CLOSE_PRICE', 'close': 'CLOSE_PRICE',
    'avg_price': 'AVG_PRICE', 'AVG_PRICE': 'AVG_PRICE',
    'TTL_TRD_QNTY': 'TTL_TRD_QNTY', 'total_traded_qty': 'TTL_TRD_QNTY', 'volume': 'TTL_TRD_QNTY',
    'turnover_lacs': 'TURNOVER_LACS', 'TURNOVER_LACS': 'TURNOVER_LACS', 'turnover': 'TURNOVER_LACS',
    'no_of_trades': 'NO_OF_TRADES', 'NO_OF_TRADES': 'NO_OF_TRADES', 'trades': 'NO_OF_TRADES',
    'DELIV_QTY': 'DELIV_QTY', 'delivery_qty': 'DELIV_QTY', 'deliv_qty': 'DELIV_QTY',
    'deliv_per': 'DELIV_PER', 'DELIV_PER': 'DELIV_PER', 'delivery_percentage': 'DELIV_PER'
}

def parse_args():
    p = argparse.ArgumentParser(description='Load complete NSE daily data (all segments) into database')
    p.add_argument('--data-pattern', default='NSE_*_2025_Data/cm*.csv', help='Glob pattern for CSV files')
    p.add_argument('--month', help='Specific month (e.g., January)')
    p.add_argument('--year', help='Specific year (e.g., 2025)')
    p.add_argument('--batch-size', type=int, default=1000, help='Batch size for database inserts')
    p.add_argument('--skip-existing', action='store_true', help='Skip files already loaded')
    return p.parse_args()

def load_and_clean_csv(file_path: str) -> pd.DataFrame:
    """Load and standardize a single CSV file"""
    try:
        df = pd.read_csv(file_path)
        df.columns = df.columns.str.strip()
        df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
        df = df.rename(columns=COLUMN_MAPPING)
        
        # Load ALL series data (EQ, SM, BE, ST, GB, GS, BZ, etc.)
        # No filtering - include complete NSE market data
        print(f"   📊 Series found: {df['SERIES'].value_counts().to_dict() if 'SERIES' in df.columns else 'No SERIES column'}")
        
        # Standardize numeric columns
        numeric_cols = ['PREV_CLOSE', 'OPEN_PRICE', 'HIGH_PRICE', 'LOW_PRICE', 'LAST_PRICE', 
                       'CLOSE_PRICE', 'AVG_PRICE', 'TTL_TRD_QNTY', 'TURNOVER_LACS', 
                       'NO_OF_TRADES', 'DELIV_QTY', 'DELIV_PER']
        
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Add source file tracking
        df['SOURCE_FILE'] = os.path.basename(file_path)
        
        # Parse date from filename if DATE column missing
        if 'DATE' not in df.columns:
            base = os.path.basename(file_path)
            # Try to extract date from filename
            df['DATE'] = base
        
        return df
        
    except Exception as e:
        print(f"❌ Error loading {file_path}: {e}")
        return pd.DataFrame()

def insert_batch_to_db(db_manager: NSEDatabaseManager, df_batch: pd.DataFrame):
    """Insert a batch of records to step01_equity_daily table"""
    if df_batch.empty:
        return 0
    
    cursor = db_manager.connection.cursor()
    
    insert_sql = """
    INSERT INTO step01_equity_daily 
    (trade_date, symbol, series, prev_close, open_price, high_price, low_price, 
     last_price, close_price, avg_price, ttl_trd_qnty, turnover_lacs, no_of_trades, 
     deliv_qty, deliv_per, source_file)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    # Prepare data for insertion
    records = []
    for _, row in df_batch.iterrows():
        def safe_get(key):
            """Get value and convert NaN to None for database"""
            val = row.get(key)
            if pd.isna(val):
                return None
            return val
        
        record = (
            safe_get('DATE'),
            safe_get('SYMBOL'),
            safe_get('SERIES') or 'EQ',
            safe_get('PREV_CLOSE'),
            safe_get('OPEN_PRICE'),
            safe_get('HIGH_PRICE'),
            safe_get('LOW_PRICE'),
            safe_get('LAST_PRICE'),
            safe_get('CLOSE_PRICE'),
            safe_get('AVG_PRICE'),
            safe_get('TTL_TRD_QNTY'),
            safe_get('TURNOVER_LACS'),
            safe_get('NO_OF_TRADES'),
            safe_get('DELIV_QTY'),
            safe_get('DELIV_PER'),
            safe_get('SOURCE_FILE')
        )
        records.append(record)
    
    cursor.executemany(insert_sql, records)
    db_manager.connection.commit()
    return len(records)

def main():
    args = parse_args()
    
    # Build file pattern
    if args.month and args.year:
        pattern = f"NSE_{args.month}_{args.year}_Data/cm*.csv"
    else:
        pattern = args.data_pattern
    
    files = sorted(glob.glob(pattern))
    if not files:
        print(f"❌ No files found matching pattern: {pattern}")
        return
    
    print(f"📁 Found {len(files)} CSV files to process")
    
    # Initialize database
    db_manager = NSEDatabaseManager()
    db_manager.initialize_database()
    
    total_records = 0
    processed_files = 0
    
    for file_path in files:
        print(f"📖 Processing: {os.path.basename(file_path)}")
        
        # Check if file already processed (if skip_existing enabled)
        if args.skip_existing:
            cursor = db_manager.connection.cursor()
            cursor.execute("SELECT COUNT(*) FROM step01_equity_daily WHERE source_file = ?", 
                          (os.path.basename(file_path),))
            if cursor.fetchone()[0] > 0:
                print(f"   ⏭️  Skipping (already loaded)")
                continue
        
        # Load and process file
        df = load_and_clean_csv(file_path)
        if df.empty:
            continue
        
        # Insert in batches
        file_records = 0
        for i in range(0, len(df), args.batch_size):
            batch = df.iloc[i:i + args.batch_size]
            batch_count = insert_batch_to_db(db_manager, batch)
            file_records += batch_count
        
        print(f"   ✅ Loaded {file_records:,} records")
        total_records += file_records
        processed_files += 1
    
    print(f"\n🎉 Loading complete!")
    print(f"   📁 Files processed: {processed_files}")
    print(f"   📊 Total records loaded: {total_records:,}")
    
    # Display final summary
    db_manager.get_database_summary()
    db_manager.close()

if __name__ == "__main__":
    main()
