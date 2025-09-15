#!/usr/bin/env python3

import pandas as pd
import pyodbc
import json
import os
from datetime import datetime

def fix_feb_4th_data_based_on_csv():
    """Fix Feb 4th data to match the CSV source format exactly"""
    
    csv_file_path = r"C:\Users\kiran\Downloads\4th month.csv"
    
    print("🔧 FIXING FEB 4TH DATA TO MATCH CSV SOURCE")
    print("="*60)
    
    try:
        # Step 1: Read and analyze CSV
        print("📄 Reading CSV source file...")
        df_csv = pd.read_csv(csv_file_path)
        print(f"CSV Records: {len(df_csv):,}")
        
        # Step 2: Understand instrument type mapping
        print(f"\n🔍 CSV Instrument Type Analysis:")
        csv_instrument_analysis = df_csv.groupby(['FinInstrmTp', 'OptnTp']).size().reset_index(name='count')
        print(csv_instrument_analysis)
        
        # Step 3: Connect to database
        print(f"\n💾 Connecting to database...")
        with open('database_config.json', 'r') as f:
            config = json.load(f)
        
        conn_str = f"DRIVER={config['driver']};SERVER={config['server']};DATABASE={config['database']};Trusted_Connection=yes;"
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        # Step 4: Clear existing Feb 4th data
        print(f"\n🗑️ Clearing existing Feb 4th data...")
        delete_query = "DELETE FROM step04_fo_udiff_daily WHERE trade_date = '20250204'"
        cursor.execute(delete_query)
        deleted_count = cursor.rowcount
        print(f"   Deleted {deleted_count:,} existing records")
        
        # Step 5: Prepare CSV data for insertion
        print(f"\n🔄 Preparing CSV data for database insertion...")
        
        # Convert date format from DD-MM-YYYY to YYYYMMDD
        df_csv['TradDt_db'] = pd.to_datetime(df_csv['TradDt'], format='%d-%m-%Y').dt.strftime('%Y%m%d')
        df_csv['BizDt_db'] = pd.to_datetime(df_csv['BizDt'], format='%d-%m-%Y').dt.strftime('%Y%m%d')
        
        # Convert expiry dates
        df_csv['XpryDt_db'] = pd.to_datetime(df_csv['XpryDt'], format='%d-%m-%Y').dt.strftime('%Y%m%d')
        df_csv['FininstrmActlXpryDt_db'] = pd.to_datetime(df_csv['FininstrmActlXpryDt'], format='%d-%m-%Y').dt.strftime('%Y%m%d')
        
        print(f"   Converted date formats")
        print(f"   Sample: {df_csv['TradDt'].iloc[0]} → {df_csv['TradDt_db'].iloc[0]}")
        
        # Step 6: Insert data using exact CSV structure
        print(f"\n📥 Inserting CSV data into database...")
        
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
        
        for i in range(0, len(df_csv), batch_size):
            batch = df_csv.iloc[i:i+batch_size]
            batch_data = []
            
            for _, row in batch.iterrows():
                # Handle NaN values
                def handle_nan(value):
                    if pd.isna(value):
                        return None
                    return value
                
                row_data = (
                    row['TradDt_db'],           # trade_date
                    row['BizDt_db'],            # BizDt
                    handle_nan(row['Sgmt']),    # Sgmt
                    handle_nan(row['Src']),     # Src
                    handle_nan(row['FinInstrmTp']),  # instrument (keep CSV format: STO, IDO, etc.)
                    handle_nan(row['FinInstrmId']),  # FinInstrmId
                    handle_nan(row['ISIN']),    # ISIN
                    handle_nan(row['TckrSymb']), # symbol
                    handle_nan(row['SctySrs']), # SctySrs
                    row['XpryDt_db'],           # expiry_date
                    row['FininstrmActlXpryDt_db'], # FininstrmActlXpryDt
                    handle_nan(row['StrkPric']), # strike_price
                    handle_nan(row['OptnTp']),  # option_type
                    handle_nan(row['FinInstrmNm']), # FinInstrmNm
                    handle_nan(row['OpnPric']), # open_price
                    handle_nan(row['HghPric']), # high_price
                    handle_nan(row['LwPric']),  # low_price
                    handle_nan(row['ClsPric']), # close_price
                    handle_nan(row['LastPric']), # LastPric
                    handle_nan(row['PrvsClsgPric']), # PrvsClsgPric
                    handle_nan(row['UndrlygPric']), # UndrlygPric
                    handle_nan(row['SttlmPric']), # settle_price
                    handle_nan(row['OpnIntrst']), # open_interest
                    handle_nan(row['ChngInOpnIntrst']), # change_in_oi
                    handle_nan(row['TtlTradgVol']), # contracts_traded
                    handle_nan(row['TtlTrfVal']), # value_in_lakh
                    handle_nan(row['TtlNbOfTxsExctd']), # TtlNbOfTxsExctd
                    handle_nan(row['SsnId']),   # SsnId
                    handle_nan(row['NewBrdLotQty']), # NewBrdLotQty
                    handle_nan(row['Rmks']),    # Rmks
                    handle_nan(row['Rsvd1']),   # Rsvd1
                    handle_nan(row['Rsvd2']),   # Rsvd2
                    handle_nan(row['Rsvd3']),   # Rsvd3
                    handle_nan(row['Rsvd4'])    # Rsvd4
                )
                batch_data.append(row_data)
            
            # Execute batch
            cursor.executemany(insert_query, batch_data)
            inserted_count += len(batch_data)
            
            if i % (batch_size * 10) == 0:  # Progress every 10 batches
                print(f"   Processed: {inserted_count:,}/{len(df_csv):,} records")
        
        # Commit changes
        conn.commit()
        print(f"✅ Successfully inserted {inserted_count:,} records")
        
        # Step 7: Verify the insertion
        print(f"\n🧪 Verifying insertion...")
        verify_query = """
        SELECT 
            COUNT(*) as total_count,
            COUNT(DISTINCT instrument) as instrument_types,
            COUNT(DISTINCT symbol) as unique_symbols
        FROM step04_fo_udiff_daily 
        WHERE trade_date = '20250204'
        """
        
        cursor.execute(verify_query)
        result = cursor.fetchone()
        
        print(f"   Total records: {result[0]:,}")
        print(f"   Instrument types: {result[1]}")
        print(f"   Unique symbols: {result[2]}")
        
        # Check instrument type distribution
        inst_query = """
        SELECT instrument, COUNT(*) as count
        FROM step04_fo_udiff_daily 
        WHERE trade_date = '20250204'
        GROUP BY instrument
        ORDER BY count DESC
        """
        
        cursor.execute(inst_query)
        inst_results = cursor.fetchall()
        
        print(f"\n📊 Instrument type distribution:")
        for inst_type, count in inst_results:
            print(f"   {inst_type}: {count:,}")
        
        # Step 8: Final validation
        print(f"\n✅ FINAL VALIDATION:")
        print("-" * 50)
        
        if result[0] == len(df_csv):
            print(f"🎯 SUCCESS: Record count matches exactly!")
            print(f"   CSV: {len(df_csv):,}")
            print(f"   DB:  {result[0]:,}")
        else:
            print(f"⚠️  Record count mismatch:")
            print(f"   CSV: {len(df_csv):,}")
            print(f"   DB:  {result[0]:,}")
        
        conn.close()
        
        print(f"\n{'='*60}")
        print(f"🎉 Feb 4th data successfully updated from CSV source!")
        print(f"📊 Using exact CSV instrument types: STO, IDO, STF, IDF")
        print(f"📅 Date formats converted to database format")
        print(f"{'='*60}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = fix_feb_4th_data_based_on_csv()
    if success:
        print(f"\n✅ Fix completed successfully")
    else:
        print(f"❌ Fix failed")
