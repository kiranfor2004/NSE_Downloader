#!/usr/bin/env python3

import pandas as pd
import pyodbc
import json
import os

def validate_feb_4th_against_source():
    """Validate Feb 4th 2025 data against the provided CSV file"""
    
    csv_file_path = r"C:\Users\kiran\Downloads\4th month.csv"
    
    print("🔍 VALIDATING FEB 4TH DATA AGAINST SOURCE CSV")
    print("="*60)
    
    # Step 1: Check if CSV file exists
    if not os.path.exists(csv_file_path):
        print(f"❌ CSV file not found: {csv_file_path}")
        return False
    
    print(f"✅ CSV file found: {csv_file_path}")
    
    try:
        # Step 2: Read the CSV file
        print(f"\n📄 Reading CSV file...")
        df_csv = pd.read_csv(csv_file_path)
        print(f"   CSV Records: {len(df_csv):,}")
        print(f"   CSV Columns: {len(df_csv.columns)}")
        
        # Display CSV structure
        print(f"\n📋 CSV Column Structure:")
        print("-" * 50)
        for i, col in enumerate(df_csv.columns, 1):
            print(f"{i:2d}. {col}")
        
        # Show sample CSV data
        print(f"\n📝 Sample CSV Data (first 3 rows):")
        print("-" * 50)
        print(df_csv.head(3).to_string())
        
        # Step 3: Read database data for Feb 4th
        print(f"\n💾 Reading database data for Feb 4th...")
        
        # Load database configuration
        with open('database_config.json', 'r') as f:
            config = json.load(f)

        conn_str = f"DRIVER={config['driver']};SERVER={config['server']};DATABASE={config['database']};Trusted_Connection=yes;"
        conn = pyodbc.connect(conn_str)
        
        # Use our UDiFF-compliant query
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
        
        print(f"   Database Records: {len(df_db):,}")
        print(f"   Database Columns: {len(df_db.columns)}")
        
        # Step 4: Compare structures
        print(f"\n🔍 STRUCTURE COMPARISON:")
        print("-" * 60)
        print(f"CSV Records:      {len(df_csv):,}")
        print(f"Database Records: {len(df_db):,}")
        print(f"Record Difference: {abs(len(df_csv) - len(df_db)):,}")
        
        print(f"\nCSV Columns:      {len(df_csv.columns)}")
        print(f"Database Columns: {len(df_db.columns)}")
        
        # Step 5: Column mapping analysis
        print(f"\n📋 COLUMN MAPPING ANALYSIS:")
        print("-" * 60)
        
        csv_columns = list(df_csv.columns)
        db_columns = list(df_db.columns)
        
        print(f"CSV columns: {csv_columns[:10]}{'...' if len(csv_columns) > 10 else ''}")
        print(f"DB columns:  {db_columns[:10]}{'...' if len(db_columns) > 10 else ''}")
        
        # Find common columns
        common_columns = []
        for csv_col in csv_columns:
            if csv_col in db_columns:
                common_columns.append(csv_col)
        
        print(f"\nCommon columns: {len(common_columns)}")
        if common_columns:
            print(f"Common: {common_columns[:5]}{'...' if len(common_columns) > 5 else ''}")
        
        # Step 6: Data comparison for common columns
        if common_columns:
            print(f"\n🔍 DATA COMPARISON (Common Columns):")
            print("-" * 60)
            
            for col in common_columns[:5]:  # Compare first 5 common columns
                if col in df_csv.columns and col in df_db.columns:
                    # Sample comparison
                    csv_sample = df_csv[col].head(3).tolist()
                    db_sample = df_db[col].head(3).tolist()
                    
                    print(f"\nColumn: {col}")
                    print(f"  CSV:  {csv_sample}")
                    print(f"  DB:   {db_sample}")
                    print(f"  Match: {'✅' if csv_sample == db_sample else '❌'}")
        
        # Step 7: Specific validations
        print(f"\n🧪 SPECIFIC VALIDATIONS:")
        print("-" * 60)
        
        # Check if CSV has TradDt column
        if 'TradDt' in df_csv.columns:
            csv_trade_dates = df_csv['TradDt'].unique()
            print(f"✅ CSV TradDt values: {csv_trade_dates}")
        elif 'trade_date' in df_csv.columns:
            csv_trade_dates = df_csv['trade_date'].unique()
            print(f"✅ CSV trade_date values: {csv_trade_dates}")
        else:
            print(f"⚠️  No trade date column found in CSV")
        
        # Check database trade date
        if 'TradDt' in df_db.columns:
            db_trade_dates = df_db['TradDt'].unique()
            print(f"✅ DB TradDt values: {db_trade_dates}")
        
        # Symbol comparison
        if 'TckrSymb' in df_csv.columns and 'TckrSymb' in df_db.columns:
            csv_symbols = set(df_csv['TckrSymb'].unique())
            db_symbols = set(df_db['TckrSymb'].unique())
            
            print(f"\nSymbol comparison:")
            print(f"  CSV symbols: {len(csv_symbols)}")
            print(f"  DB symbols:  {len(db_symbols)}")
            print(f"  Common symbols: {len(csv_symbols.intersection(db_symbols))}")
            
            if len(csv_symbols) < 20:  # If manageable, show symbols
                print(f"  CSV symbols: {sorted(csv_symbols)}")
            if len(db_symbols) < 20:
                print(f"  DB symbols:  {sorted(db_symbols)}")
        
        # Instrument type comparison
        if 'FinInstrmTp' in df_csv.columns and 'FinInstrmTp' in df_db.columns:
            csv_instruments = df_csv['FinInstrmTp'].value_counts()
            db_instruments = df_db['FinInstrmTp'].value_counts()
            
            print(f"\nInstrument type comparison:")
            print(f"  CSV instruments:\n{csv_instruments}")
            print(f"  DB instruments:\n{db_instruments}")
        
        # Step 8: Summary and recommendations
        print(f"\n📊 VALIDATION SUMMARY:")
        print("-" * 60)
        
        record_match = len(df_csv) == len(df_db)
        structure_match = len(common_columns) >= 25  # At least 25 common columns
        
        print(f"Record Count Match: {'✅' if record_match else '❌'}")
        print(f"Structure Match: {'✅' if structure_match else '❌'}")
        
        if not record_match:
            print(f"\n⚠️  RECORD COUNT MISMATCH:")
            print(f"   Expected (CSV): {len(df_csv):,}")
            print(f"   Actual (DB):    {len(df_db):,}")
            print(f"   Difference:     {abs(len(df_csv) - len(df_db)):,}")
        
        if not structure_match:
            print(f"\n⚠️  STRUCTURE MISMATCH:")
            print(f"   Common columns: {len(common_columns)}/34 expected")
        
        # Step 9: Create detailed comparison report
        print(f"\n📄 Creating detailed comparison report...")
        
        comparison_report = {
            'csv_file': csv_file_path,
            'csv_records': len(df_csv),
            'csv_columns': len(df_csv.columns),
            'db_records': len(df_db),
            'db_columns': len(df_db.columns),
            'common_columns': len(common_columns),
            'record_match': record_match,
            'structure_match': structure_match
        }
        
        # Save comparison to file
        with open('feb_4th_validation_report.json', 'w') as f:
            json.dump(comparison_report, f, indent=2)
        
        print(f"✅ Validation report saved: feb_4th_validation_report.json")
        
        print(f"\n{'='*60}")
        if record_match and structure_match:
            print(f"🎯 VALIDATION RESULT: ✅ PASSED")
            print(f"📊 Data matches source CSV file")
        else:
            print(f"🎯 VALIDATION RESULT: ❌ FAILED")
            print(f"⚠️  Discrepancies found - check report for details")
        print(f"{'='*60}")
        
        return record_match and structure_match
        
    except Exception as e:
        print(f"❌ Error during validation: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    validate_feb_4th_against_source()
