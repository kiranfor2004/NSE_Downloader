#!/usr/bin/env python3

import pandas as pd
import numpy as np

def analyze_csv_data_types():
    """Analyze CSV data types to identify overflow issues"""
    
    csv_file_path = r"C:\Users\kiran\Downloads\4th month.csv"
    
    print("🔍 ANALYZING CSV DATA TYPES FOR OVERFLOW ISSUES")
    print("="*60)
    
    try:
        df_csv = pd.read_csv(csv_file_path)
        
        # Check numeric columns for large values
        numeric_columns = [
            'FinInstrmId', 'StrkPric', 'OpnPric', 'HghPric', 'LwPric', 'ClsPric', 
            'LastPric', 'PrvsClsgPric', 'UndrlygPric', 'SttlmPric', 'OpnIntrst', 
            'ChngInOpnIntrst', 'TtlTradgVol', 'TtlTrfVal', 'TtlNbOfTxsExctd', 
            'NewBrdLotQty'
        ]
        
        print("📊 NUMERIC COLUMN ANALYSIS:")
        print("-" * 50)
        
        for col in numeric_columns:
            if col in df_csv.columns:
                series = pd.to_numeric(df_csv[col], errors='coerce')
                
                print(f"\n{col}:")
                print(f"  Data type: {series.dtype}")
                print(f"  Min: {series.min()}")
                print(f"  Max: {series.max()}")
                print(f"  NaN count: {series.isna().sum()}")
                
                # Check for integer overflow (SQL Server int max: 2,147,483,647)
                if series.max() > 2147483647:
                    print(f"  ⚠️  OVERFLOW: Values exceed SQL Server int limit!")
                
                # Show sample of large values
                if series.max() > 1000000:
                    large_values = series[series > 1000000].head(5)
                    print(f"  Large values sample: {large_values.tolist()}")
        
        # Check specific problematic values
        print(f"\n🔍 CHECKING SPECIFIC PROBLEMATIC COLUMNS:")
        print("-" * 50)
        
        # Check FinInstrmId
        if 'FinInstrmId' in df_csv.columns:
            fin_id = pd.to_numeric(df_csv['FinInstrmId'], errors='coerce')
            max_fin_id = fin_id.max()
            print(f"FinInstrmId max: {max_fin_id:,}")
            if max_fin_id > 2147483647:
                print(f"❌ FinInstrmId overflow detected!")
        
        # Check open interest
        if 'OpnIntrst' in df_csv.columns:
            oi = pd.to_numeric(df_csv['OpnIntrst'], errors='coerce')
            max_oi = oi.max()
            print(f"OpnIntrst max: {max_oi:,}")
            if max_oi > 2147483647:
                print(f"❌ OpnIntrst overflow detected!")
        
        # Check TtlTradgVol
        if 'TtlTradgVol' in df_csv.columns:
            vol = pd.to_numeric(df_csv['TtlTradgVol'], errors='coerce')
            max_vol = vol.max()
            print(f"TtlTradgVol max: {max_vol:,}")
            if max_vol > 2147483647:
                print(f"❌ TtlTradgVol overflow detected!")
        
        # Check NewBrdLotQty
        if 'NewBrdLotQty' in df_csv.columns:
            lot = pd.to_numeric(df_csv['NewBrdLotQty'], errors='coerce')
            max_lot = lot.max()
            print(f"NewBrdLotQty max: {max_lot:,}")
            if max_lot > 2147483647:
                print(f"❌ NewBrdLotQty overflow detected!")
        
        print(f"\n💡 RECOMMENDATIONS:")
        print("-" * 50)
        print("SQL Server data type limits:")
        print("  int: -2,147,483,648 to 2,147,483,647")
        print("  bigint: -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807")
        print("  float: Approximate numeric data")
        
        print(f"\nSuggested fixes:")
        print("1. Change int columns to bigint for large numbers")
        print("2. Use float for decimal values")
        print("3. Handle NaN values properly")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    analyze_csv_data_types()
