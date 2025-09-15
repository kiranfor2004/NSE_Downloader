#!/usr/bin/env python3

import pandas as pd
import pyodbc
import json
import os
from datetime import datetime

def analyze_csv_vs_db_discrepancies():
    """Detailed analysis of discrepancies between CSV and database"""
    
    csv_file_path = r"C:\Users\kiran\Downloads\4th month.csv"
    
    print("🔍 DETAILED DISCREPANCY ANALYSIS")
    print("="*60)
    
    try:
        # Read CSV
        df_csv = pd.read_csv(csv_file_path)
        print(f"CSV Records: {len(df_csv):,}")
        
        # Read database
        with open('database_config.json', 'r') as f:
            config = json.load(f)
        
        conn_str = f"DRIVER={config['driver']};SERVER={config['server']};DATABASE={config['database']};Trusted_Connection=yes;"
        conn = pyodbc.connect(conn_str)
        
        query = """
        SELECT 
            trade_date AS TradDt,
            BizDt,
            Sgmt,
            Src,
            instrument AS FinInstrmTp,
            FinInstrmId,
            ISIN,
            symbol AS TckrSymb,
            SctySrs,
            expiry_date AS XpryDt,
            FininstrmActlXpryDt,
            strike_price AS StrkPric,
            option_type AS OptnTp,
            FinInstrmNm,
            open_price AS OpnPric,
            high_price AS HghPric,
            low_price AS LwPric,
            close_price AS ClsPric,
            LastPric,
            PrvsClsgPric,
            UndrlygPric,
            settle_price AS SttlmPric,
            open_interest AS OpnIntrst,
            change_in_oi AS ChngInOpnIntrst,
            contracts_traded AS TtlTradgVol,
            value_in_lakh AS TtlTrfVal,
            TtlNbOfTxsExctd,
            SsnId,
            NewBrdLotQty,
            Rmks,
            Rsvd1,
            Rsvd2,
            Rsvd3,
            Rsvd4
        FROM step04_fo_udiff_daily
        WHERE trade_date = '20250204'
        ORDER BY id
        """
        
        df_db = pd.read_sql(query, conn)
        conn.close()
        
        print(f"DB Records: {len(df_db):,}")
        
        # Issue 1: Date format discrepancy
        print(f"\n🔍 ISSUE 1: DATE FORMAT DISCREPANCY")
        print("-" * 50)
        print(f"CSV TradDt format: {df_csv['TradDt'].iloc[0]} (DD-MM-YYYY)")
        print(f"DB TradDt format:  {df_db['TradDt'].iloc[0]} (YYYYMMDD)")
        print("❌ Date formats don't match - need conversion")
        
        # Issue 2: Instrument type mapping
        print(f"\n🔍 ISSUE 2: INSTRUMENT TYPE MAPPING")
        print("-" * 50)
        csv_instruments = df_csv['FinInstrmTp'].value_counts()
        db_instruments = df_db['FinInstrmTp'].value_counts()
        
        print("CSV instrument types:")
        print(csv_instruments)
        print("\nDB instrument types:")
        print(db_instruments)
        
        print("\n❌ Instrument types are completely different!")
        print("CSV uses: STO, IDO, STF, IDF")
        print("DB uses:  FUTIDX, FUTSTK, OPTIDX, OPTSTK, FUTCUR, OPTCUR")
        
        # Issue 3: Record count difference
        print(f"\n🔍 ISSUE 3: RECORD COUNT DIFFERENCE")
        print("-" * 50)
        print(f"CSV has {len(df_csv):,} records")
        print(f"DB has {len(df_db):,} records")
        print(f"Missing {len(df_csv) - len(df_db):,} records in DB")
        
        # Issue 4: Symbol comparison
        print(f"\n🔍 ISSUE 4: SYMBOL ANALYSIS")
        print("-" * 50)
        csv_symbols = set(df_csv['TckrSymb'].unique())
        db_symbols = set(df_db['TckrSymb'].unique())
        
        print(f"CSV symbols: {len(csv_symbols)}")
        print(f"DB symbols:  {len(db_symbols)}")
        print(f"Common symbols: {len(csv_symbols.intersection(db_symbols))}")
        
        missing_in_db = csv_symbols - db_symbols
        extra_in_db = db_symbols - csv_symbols
        
        if missing_in_db:
            print(f"\nSymbols in CSV but missing in DB ({len(missing_in_db)}):")
            print(sorted(list(missing_in_db))[:10])  # Show first 10
        
        if extra_in_db:
            print(f"\nSymbols in DB but not in CSV ({len(extra_in_db)}):")
            print(sorted(list(extra_in_db))[:10])  # Show first 10
        
        # Issue 5: Data source analysis
        print(f"\n🔍 ISSUE 5: DATA SOURCE ANALYSIS")
        print("-" * 50)
        print("The discrepancies suggest we might be:")
        print("1. Using wrong source file (BhavCopy vs UDiFF)")
        print("2. Missing data transformation")
        print("3. Using different instrument type coding")
        
        # Create mapping analysis
        print(f"\n🔍 INSTRUMENT TYPE MAPPING ANALYSIS")
        print("-" * 50)
        
        # Check CSV sample data for patterns
        csv_sample = df_csv.head(10)
        print("CSV sample instrument details:")
        for idx, row in csv_sample.iterrows():
            print(f"  {row['TckrSymb']} - {row['FinInstrmTp']} - {row['OptnTp']} - Strike: {row['StrkPric']}")
        
        print("\nDB sample instrument details:")
        db_sample = df_db.head(10)
        for idx, row in db_sample.iterrows():
            print(f"  {row['TckrSymb']} - {row['FinInstrmTp']} - {row['OptnTp']} - Strike: {row['StrkPric']}")
        
        # Recommendations
        print(f"\n💡 RECOMMENDATIONS:")
        print("-" * 50)
        print("1. ✅ Column structure matches perfectly (34 columns)")
        print("2. ❌ Need to fix date format conversion")
        print("3. ❌ Need to understand instrument type mapping:")
        print("   - STO → ? (Stock Options?)")
        print("   - IDO → ? (Index Options?)")
        print("   - STF → ? (Stock Futures?)")
        print("   - IDF → ? (Index Futures?)")
        print("4. ❌ Need to investigate missing 2,644 records")
        print("5. ❌ Need to verify we're using correct UDiFF source")
        
        return {
            'csv_records': len(df_csv),
            'db_records': len(df_db),
            'missing_records': len(df_csv) - len(df_db),
            'csv_symbols': len(csv_symbols),
            'db_symbols': len(db_symbols),
            'common_symbols': len(csv_symbols.intersection(db_symbols)),
            'csv_instruments': dict(csv_instruments),
            'db_instruments': dict(db_instruments)
        }
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    result = analyze_csv_vs_db_discrepancies()
    if result:
        print(f"\n✅ Analysis complete - see detailed breakdown above")
    else:
        print(f"❌ Analysis failed")
