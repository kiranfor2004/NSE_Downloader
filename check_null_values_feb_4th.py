#!/usr/bin/env python3

import pyodbc
import json
import pandas as pd

def check_null_values_feb_4th():
    """Check for NULL values in Feb 4th data and compare with CSV"""
    
    print("🔍 CHECKING NULL VALUES IN FEB 4TH DATA")
    print("="*60)
    
    try:
        # Connect to database
        with open('database_config.json', 'r') as f:
            config = json.load(f)
        
        conn_str = f"DRIVER={config['driver']};SERVER={config['server']};DATABASE={config['database']};Trusted_Connection=yes;"
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        # Check NULL values in database
        print("💾 Checking NULL values in database...")
        null_check_query = """
        SELECT 
            'trade_date' as column_name, COUNT(*) - COUNT(trade_date) as null_count, COUNT(*) as total_count,
            CAST((COUNT(*) - COUNT(trade_date)) * 100.0 / COUNT(*) AS DECIMAL(5,2)) as null_percentage
        FROM step04_fo_udiff_daily WHERE trade_date = '20250204'
        UNION ALL
        SELECT 'strike_price', COUNT(*) - COUNT(strike_price), COUNT(*), 
            CAST((COUNT(*) - COUNT(strike_price)) * 100.0 / COUNT(*) AS DECIMAL(5,2))
        FROM step04_fo_udiff_daily WHERE trade_date = '20250204'
        UNION ALL
        SELECT 'expiry_date', COUNT(*) - COUNT(expiry_date), COUNT(*), 
            CAST((COUNT(*) - COUNT(expiry_date)) * 100.0 / COUNT(*) AS DECIMAL(5,2))
        FROM step04_fo_udiff_daily WHERE trade_date = '20250204'
        UNION ALL
        SELECT 'option_type', COUNT(*) - COUNT(option_type), COUNT(*), 
            CAST((COUNT(*) - COUNT(option_type)) * 100.0 / COUNT(*) AS DECIMAL(5,2))
        FROM step04_fo_udiff_daily WHERE trade_date = '20250204'
        UNION ALL
        SELECT 'open_price', COUNT(*) - COUNT(open_price), COUNT(*), 
            CAST((COUNT(*) - COUNT(open_price)) * 100.0 / COUNT(*) AS DECIMAL(5,2))
        FROM step04_fo_udiff_daily WHERE trade_date = '20250204'
        UNION ALL
        SELECT 'high_price', COUNT(*) - COUNT(high_price), COUNT(*), 
            CAST((COUNT(*) - COUNT(high_price)) * 100.0 / COUNT(*) AS DECIMAL(5,2))
        FROM step04_fo_udiff_daily WHERE trade_date = '20250204'
        UNION ALL
        SELECT 'low_price', COUNT(*) - COUNT(low_price), COUNT(*), 
            CAST((COUNT(*) - COUNT(low_price)) * 100.0 / COUNT(*) AS DECIMAL(5,2))
        FROM step04_fo_udiff_daily WHERE trade_date = '20250204'
        UNION ALL
        SELECT 'close_price', COUNT(*) - COUNT(close_price), COUNT(*), 
            CAST((COUNT(*) - COUNT(close_price)) * 100.0 / COUNT(*) AS DECIMAL(5,2))
        FROM step04_fo_udiff_daily WHERE trade_date = '20250204'
        UNION ALL
        SELECT 'open_interest', COUNT(*) - COUNT(open_interest), COUNT(*), 
            CAST((COUNT(*) - COUNT(open_interest)) * 100.0 / COUNT(*) AS DECIMAL(5,2))
        FROM step04_fo_udiff_daily WHERE trade_date = '20250204'
        ORDER BY null_count DESC
        """
        
        cursor.execute(null_check_query)
        db_nulls = cursor.fetchall()
        
        print("📊 NULL Value Analysis (Database):")
        print("-" * 50)
        for column_name, null_count, total_count, null_percentage in db_nulls:
            status = "❌" if null_count > 0 else "✅"
            print(f"  {column_name}: {null_count:,} NULLs out of {total_count:,} ({null_percentage}%) {status}")
        
        # Check CSV for comparison
        csv_file_path = r"C:\Users\kiran\Downloads\4th month.csv"
        print(f"\n📄 Checking NULL values in CSV...")
        
        df_csv = pd.read_csv(csv_file_path)
        
        key_columns = ['StrkPric', 'XpryDt', 'OptnTp', 'OpnPric', 'HghPric', 'LwPric', 'ClsPric', 'OpnIntrst']
        
        print("📊 NULL Value Analysis (CSV):")
        print("-" * 50)
        for col in key_columns:
            if col in df_csv.columns:
                null_count = df_csv[col].isna().sum()
                total_count = len(df_csv)
                null_percentage = (null_count * 100.0) / total_count
                status = "❌" if null_count > 0 else "✅"
                print(f"  {col}: {null_count:,} NULLs out of {total_count:,} ({null_percentage:.2f}%) {status}")
        
        # Sample records with NULLs
        print(f"\n🔍 Sample records with NULL strike_price from database:")
        sample_nulls_query = """
        SELECT TOP 5 
            symbol, instrument, strike_price, expiry_date, option_type, 
            open_price, high_price, low_price, close_price
        FROM step04_fo_udiff_daily 
        WHERE trade_date = '20250204' AND strike_price IS NULL
        """
        
        cursor.execute(sample_nulls_query)
        null_samples = cursor.fetchall()
        
        if null_samples:
            print("Database records with NULL strike_price:")
            for row in null_samples:
                print(f"  {row[0]} ({row[1]}) - Strike: {row[2]}, Expiry: {row[3]}, Type: {row[4]}")
        else:
            print("No NULL strike_price records found")
        
        # Sample records from CSV
        print(f"\n📄 Sample CSV records for comparison:")
        csv_sample = df_csv[['TckrSymb', 'FinInstrmTp', 'StrkPric', 'XpryDt', 'OptnTp']].head(5)
        for idx, row in csv_sample.iterrows():
            print(f"  {row['TckrSymb']} ({row['FinInstrmTp']}) - Strike: {row['StrkPric']}, Expiry: {row['XpryDt']}, Type: {row['OptnTp']}")
        
        conn.close()
        
        # Analysis and recommendation
        print(f"\n💡 ANALYSIS & RECOMMENDATION:")
        print("-" * 50)
        
        db_has_nulls = any(row[1] > 0 for row in db_nulls)  # Check if any NULL count > 0
        csv_has_nulls = any(df_csv[col].isna().sum() > 0 for col in key_columns if col in df_csv.columns)
        
        print(f"Database has NULLs: {'Yes' if db_has_nulls else 'No'}")
        print(f"CSV has NULLs: {'Yes' if csv_has_nulls else 'No'}")
        
        if db_has_nulls and not csv_has_nulls:
            print(f"\n🎯 YOUR UNDERSTANDING IS CORRECT!")
            print(f"✅ The CSV source file has no NULL values")
            print(f"❌ The database has NULL values due to data loading issues")
            print(f"🔧 SOLUTION: Delete Feb 4th data and reload from CSV")
            print(f"\nRecommended actions:")
            print(f"1. Delete all Feb 4th data from database")
            print(f"2. Reload using the exact CSV structure and mapping")
            print(f"3. Ensure proper data type handling during insertion")
            return True
        else:
            print(f"No significant NULL value discrepancy found")
            return False
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    check_null_values_feb_4th()
