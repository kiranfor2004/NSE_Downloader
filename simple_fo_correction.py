#!/usr/bin/env python3
"""
Simple F&O Data Correction Tool

Purpose: Correct missing records by reloading data from source files
"""

import pandas as pd
import zipfile
import os
import pyodbc

def correct_single_date(date, zip_file):
    """Correct data for a single date"""
    print(f"\n🔧 Correcting {date}...")
    
    # Connect to database
    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=SRIKIRANREDDY\\SQLEXPRESS;DATABASE=master;Trusted_Connection=yes;')
    cursor = conn.cursor()
    
    try:
        # Step 1: Delete existing data for this date
        cursor.execute("DELETE FROM step04_fo_udiff_daily WHERE trade_date = ?", date)
        deleted = cursor.rowcount
        print(f"   🗑️ Deleted {deleted:,} existing records")
        
        # Step 2: Load fresh data from source
        zip_path = os.path.join("fo_udiff_downloads", zip_file)
        with zipfile.ZipFile(zip_path, 'r') as z:
            csv_file = z.namelist()[0]
            with z.open(csv_file) as f:
                df = pd.read_csv(f)
                
        print(f"   📊 Source file has {len(df):,} records")
        
        # Step 3: Insert data using direct SQL (much faster for large datasets)
        insert_sql = """
        INSERT INTO step04_fo_udiff_daily (
            trade_date, BizDt, Sgmt, Src, FininstrmActlXpryDt, FinInstrmId, ISIN, SctySrs, FinInstrmNm,
            LastPric, PrvsClsgPric, UndrlygPric, TtlNbOfTxsExctd, SsnId, NewBrdLotQty, Rmks, Rsvd1, Rsvd2, Rsvd3, Rsvd4
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        # Prepare data for bulk insert
        data_to_insert = []
        for _, row in df.iterrows():
            data_to_insert.append([
                date,                                    # trade_date
                row.get('BizDt', None),                 # BizDt
                row.get('Sgmt', None),                  # Sgmt
                row.get('Src', None),                   # Src
                row.get('FininstrmActlXpryDt', None),   # FininstrmActlXpryDt
                row.get('FinInstrmId', None),           # FinInstrmId
                row.get('ISIN', None),                  # ISIN
                row.get('SctySrs', None),               # SctySrs
                row.get('FinInstrmNm', None),           # FinInstrmNm
                row.get('LastPric', None),              # LastPric
                row.get('PrvsClsgPric', None),          # PrvsClsgPric
                row.get('UndrlygPric', None),           # UndrlygPric
                row.get('TtlNbOfTxsExctd', None),       # TtlNbOfTxsExctd
                row.get('SsnId', None),                 # SsnId
                row.get('NewBrdLotQty', None),          # NewBrdLotQty
                row.get('Rmks', None),                  # Rmks
                row.get('Rsvd1', None),                 # Rsvd1
                row.get('Rsvd2', None),                 # Rsvd2
                row.get('Rsvd3', None),                 # Rsvd3
                row.get('Rsvd4', None),                 # Rsvd4
            ])
        
        # Insert in batches
        batch_size = 1000
        total_inserted = 0
        for i in range(0, len(data_to_insert), batch_size):
            batch = data_to_insert[i:i+batch_size]
            cursor.executemany(insert_sql, batch)
            total_inserted += len(batch)
            if total_inserted % 5000 == 0:
                print(f"   📝 Inserted {total_inserted:,} records...")
        
        print(f"   ✅ Loaded {total_inserted:,} fresh records")
        
        # Step 4: Commit changes
        conn.commit()
        
        # Step 5: Verify
        cursor.execute("SELECT COUNT(*) FROM step04_fo_udiff_daily WHERE trade_date = ?", date)
        new_count = cursor.fetchone()[0]
        
        if new_count == len(df):
            print(f"   🎉 Verification successful: {new_count:,} records")
            return True
        else:
            print(f"   ❌ Verification failed: Expected {len(df):,}, Got {new_count:,}")
            return False
            
    except Exception as e:
        print(f"   ❌ Error correcting {date}: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

def main():
    # List of dates with discrepancies (from previous validation)
    discrepancies = [
        ('20250203', 'BhavCopy_NSE_FO_0_0_0_20250203_F_0000.csv.zip'),
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
    
    print("🔧 CORRECTING F&O DATA DISCREPANCIES")
    print("=" * 50)
    
    successful = 0
    failed = 0
    
    for date, zip_file in discrepancies:
        if correct_single_date(date, zip_file):
            successful += 1
        else:
            failed += 1
    
    print(f"\n📊 CORRECTION SUMMARY")
    print(f"✅ Successful: {successful}")
    print(f"❌ Failed: {failed}")
    print(f"📈 Total: {len(discrepancies)}")

if __name__ == "__main__":
    main()