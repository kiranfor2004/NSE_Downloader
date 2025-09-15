#!/usr/bin/env python3
"""
Debug script for analyzing 5th February 2025 data discrepancies
"""

import pandas as pd
import pyodbc
import json
import os
import zipfile
from datetime import datetime
from io import StringIO

def debug_feb_5th():
    print("🔍 DEBUGGING 5th FEB 2025 DATA")
    print("="*60)
    
    # Load database config
    with open('database_config.json', 'r') as f:
        config = json.load(f)
    
    conn_str = f"DRIVER={config['driver']};SERVER={config['server']};DATABASE={config['database']};Trusted_Connection=yes;"
    source_dir = r"C:\Users\kiran\NSE_Downloader\fo_udiff_downloads"
    
    # 1. Analyze source file
    print("\n📂 ANALYZING SOURCE FILE:")
    filename = "BhavCopy_NSE_FO_0_0_0_20250205_F_0000.csv.zip"
    source_path = os.path.join(source_dir, filename)
    
    try:
        with zipfile.ZipFile(source_path, 'r') as zip_ref:
            csv_file = [f for f in zip_ref.namelist() if f.lower().endswith('.csv')][0]
            print(f"📋 Found CSV: {csv_file}")
            
            with zip_ref.open(csv_file) as f:
                source_df = pd.read_csv(StringIO(f.read().decode('utf-8')))
                
            print("\n📊 SOURCE FILE ANALYSIS:")
            print(f"Total records: {len(source_df):,}")
            print(f"\nInstrument types:")
            print(source_df['FinInstrmTp'].value_counts().to_string())
            
            print(f"\nTop symbols by record count:")
            print(source_df['TckrSymb'].value_counts().head(10).to_string())
            
            print(f"\nSample records:")
            print(source_df.head(3).to_string())
    except Exception as e:
        print(f"❌ Error reading source file: {e}")
        return
    
    # 2. Analyze database data
    print("\n💾 ANALYZING DATABASE DATA:")
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        # Get record count
        cursor.execute("SELECT COUNT(*) FROM step04_fo_udiff_daily WHERE trade_date = '20250205'")
        db_count = cursor.fetchone()[0]
        print(f"Database records: {db_count:,}")
        
        # Get instrument breakdown
        cursor.execute("""
            SELECT instrument, COUNT(*) as count 
            FROM step04_fo_udiff_daily 
            WHERE trade_date = '20250205'
            GROUP BY instrument 
            ORDER BY count DESC
        """)
        print("\nInstrument types in database:")
        for row in cursor.fetchall():
            print(f"{row[0]}: {row[1]:,}")
            
        # Get top symbols
        cursor.execute("""
            SELECT symbol, COUNT(*) as count 
            FROM step04_fo_udiff_daily 
            WHERE trade_date = '20250205'
            GROUP BY symbol 
            ORDER BY count DESC
        """)
        print("\nTop symbols in database:")
        results = cursor.fetchmany(10)
        for row in results:
            print(f"{row[0]}: {row[1]:,}")
            
        # Sample records
        cursor.execute("""
            SELECT TOP 3 * 
            FROM step04_fo_udiff_daily 
            WHERE trade_date = '20250205'
        """)
        columns = [column[0] for column in cursor.description]
        print("\nSample database records:")
        for row in cursor.fetchall():
            print(dict(zip(columns, row)))
            
    except Exception as e:
        print(f"❌ Database error: {e}")
        return
    
    # 3. Compare and analyze differences
    print("\n🔍 ANALYZING DIFFERENCES:")
    print(f"Source records: {len(source_df):,}")
    print(f"Database records: {db_count:,}")
    print(f"Missing records: {len(source_df) - db_count:,}")
    
    # Compare instrument types
    db_instruments = set()
    cursor.execute("SELECT DISTINCT instrument FROM step04_fo_udiff_daily WHERE trade_date = '20250205'")
    for row in cursor.fetchall():
        db_instruments.add(row[0])
    
    source_instruments = set(source_df['FinInstrmTp'].unique())
    
    print("\nInstrument type comparison:")
    print(f"Source unique instruments: {len(source_instruments)}")
    print(f"Database unique instruments: {len(db_instruments)}")
    
    missing_instruments = source_instruments - db_instruments
    if missing_instruments:
        print(f"\n❌ Missing instrument types in database:")
        for inst in missing_instruments:
            count = len(source_df[source_df['FinInstrmTp'] == inst])
            print(f"- {inst}: {count:,} records in source")
    
    # Analyze specific patterns
    print("\n🔍 POTENTIAL ISSUES:")
    
    # Check for NULL values in key fields
    null_counts = source_df.isnull().sum()
    if null_counts.any():
        print("\nNULL values in source:")
        print(null_counts[null_counts > 0].to_string())
    
    # Check for date format issues
    if 'TradDt' in source_df.columns:
        invalid_dates = source_df[pd.to_datetime(source_df['TradDt'], errors='coerce').isnull()]
        if len(invalid_dates) > 0:
            print(f"\n❌ Found {len(invalid_dates)} records with invalid dates")
            print(invalid_dates['TradDt'].head().to_string())
    
    print("\n✅ Analysis complete")
    conn.close()

if __name__ == "__main__":
    debug_feb_5th()
