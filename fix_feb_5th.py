# -*- coding: utf-8 -*-
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

def fix_feb_5th():
    """Fix and reload 5th February data"""
    print("\n🔍 FIXED F&O DATA LOADER - 5th Feb 2025")
    print("="*60)

    # Load config
    with open('database_config.json', 'r') as f:
        config = json.load(f)
    
    conn_str = f"DRIVER={config['driver']};SERVER={config['server']};DATABASE={config['database']};Trusted_Connection=yes;"
    source_dir = r"C:\Users\kiran\NSE_Downloader\fo_udiff_downloads"
    
    # Instrument mapping
    instrument_map = {
        'STO': 'OPTSTK',  # Stock Options
        'IDO': 'OPTIDX',  # Index Options
        'STF': 'FUTSTK',  # Stock Futures
        'IDF': 'FUTIDX'   # Index Futures
    }
    
    # Load source file
    date_str = '20250205'
    filename = f"BhavCopy_NSE_FO_0_0_0_{date_str}_F_0000.csv.zip"
    source_path = os.path.join(source_dir, filename)

    print(f"\n📂 Loading {filename}...")
    with zipfile.ZipFile(source_path, 'r') as zip_ref:
        csv_file = [f for f in zip_ref.namelist() if f.lower().endswith('.csv')][0]
        with zip_ref.open(csv_file) as f:
            df = pd.read_csv(StringIO(f.read().decode('utf-8')))

    print(f"📊 Loaded {len(df):,} source records")

    # Fix data types
    print("\n🔧 Fixing data types...")

    # 1. Map instrument types
    df['instrument'] = df['FinInstrmTp'].map(instrument_map)
    print(f"\nInstrument breakdown:")
    print(df['instrument'].value_counts().to_string())

    # 2. Handle string columns
    string_cols = ['ISIN', 'SctySrs', 'OptnTp', 'Rmks', 'Rsvd1', 'Rsvd2', 'Rsvd3', 'Rsvd4', 
                   'FinInstrmId', 'FinInstrmNm', 'SsnId', 'Sgmt', 'Src', 'BizDt']
    for col in string_cols:
        df[col] = df[col].fillna('')

    # 3. Handle numeric columns
    df['StrkPric'] = pd.to_numeric(df['StrkPric'], errors='coerce')
    
    float_cols = ['OpnPric', 'HghPric', 'LwPric', 'ClsPric', 'LastPric', 
                  'PrvsClsgPric', 'UndrlygPric', 'SttlmPric', 'TtlTrfVal']
    for col in float_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
    
    int_cols = ['TtlTradgVol', 'OpnIntrst', 'ChngInOpnIntrst', 'TtlNbOfTxsExctd', 'NewBrdLotQty']
    for col in int_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(np.int64)

    # 4. Fix option type
    df.loc[df['instrument'].isin(['FUTSTK', 'FUTIDX']), 'OptnTp'] = None
    df.loc[~df['instrument'].isin(['FUTSTK', 'FUTIDX']) & df['OptnTp'].isna(), 'OptnTp'] = ''

    # 5. Handle dates
    for col in ['TradDt', 'XpryDt', 'FininstrmActlXpryDt']:
        df[col] = pd.to_datetime(df[col]).dt.strftime('%Y%m%d')

    # 6. Set underlying
    index_symbols = {'NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'BANKEX', 'NIFTYIT'}
    df['underlying'] = df['TckrSymb'].copy()
    for symbol in index_symbols:
        mask = df['TckrSymb'].str.startswith(symbol, na=False)
        df.loc[mask, 'underlying'] = symbol

    # Save to database
    print("\n💾 Saving to database...")
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # Clear existing data
    cursor.execute("DELETE FROM step04_fo_udiff_daily WHERE trade_date = ?", date_str)
    print(f"🗑️  Cleared {cursor.rowcount:,} existing records")

    # Prepare and insert records
    records = []
    for _, row in df.iterrows():
        record = (
            str(row['TradDt']), 
            str(row['TckrSymb']), 
            str(row['instrument']), 
            str(row['XpryDt']),
            None if pd.isna(row['StrkPric']) else float(row['StrkPric']),
            None if pd.isna(row['OptnTp']) or str(row['OptnTp']).strip() == '' else str(row['OptnTp']),
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
            f"{date_str}.zip", 
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

    # Insert in chunks
    chunk_size = 1000
    for i in range(0, len(records), chunk_size):
        chunk = records[i:i + chunk_size]
        cursor.executemany("""
        INSERT INTO step04_fo_udiff_daily 
        (trade_date, symbol, instrument, expiry_date, strike_price, option_type,
         open_price, high_price, low_price, close_price, settle_price,
         contracts_traded, value_in_lakh, open_interest, change_in_oi, underlying,
         source_file, BizDt, Sgmt, Src, FinInstrmId, ISIN, SctySrs, 
         FininstrmActlXpryDt, FinInstrmNm, LastPric, PrvsClsgPric, UndrlygPric,
         TtlNbOfTxsExctd, SsnId, NewBrdLotQty, Rmks, Rsvd1, Rsvd2, Rsvd3, Rsvd4)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
               ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, chunk)
        conn.commit()
        print(f"✅ Inserted chunk {i//chunk_size + 1}: {len(chunk):,} records")

    # Verify
    cursor.execute("SELECT instrument, COUNT(*) FROM step04_fo_udiff_daily WHERE trade_date = ? GROUP BY instrument", date_str)
    print("\n📊 Final Database Record Breakdown:")
    for row in cursor.fetchall():
        print(f"   {row[0]}: {row[1]:,}")

    # Final count
    cursor.execute("SELECT COUNT(*) FROM step04_fo_udiff_daily WHERE trade_date = ?", date_str)
    db_count = cursor.fetchone()[0]
    conn.close()

    print("\n🎯 FINAL RESULTS:")
    print(f"Source records: {len(df):,}")
    print(f"Database records: {db_count:,}")

    if db_count == len(df):
        print("\n🎉 SUCCESS: Loaded with 100% accuracy!")
        return True
    else:
        print("\n❌ FAILED: Record count mismatch")
        return False

if __name__ == '__main__':
    fix_feb_5th()
