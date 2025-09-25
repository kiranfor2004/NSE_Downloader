"""
Load September 11th Data
"""
import pyodbc
import zipfile
import pandas as pd
import json
import os

# Load database configuration
with open('database_config.json', 'r') as f:
    db_config = json.load(f)

conn = pyodbc.connect(
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={db_config['server']};"
    f"DATABASE={db_config['database']};"
    f"Trusted_Connection=yes;"
)

# Load the September 11th file
zip_file = 'fo_udiff_downloads/BhavCopy_NSE_FO_0_0_0_20250911_F_0000.csv.zip'
if os.path.exists(zip_file):
    print(f'Loading September 11th: {zip_file}')
    
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        csv_files = [f for f in zip_ref.namelist() if f.endswith('.csv')]
        if csv_files:
            with zip_ref.open(csv_files[0]) as csv_file:
                df = pd.read_csv(csv_file)
                
            print(f'Records to load: {len(df):,}')
            
            # Insert into database
            cursor = conn.cursor()
            
            # Clear any existing Sep 11th data
            cursor.execute("DELETE FROM step04_fo_udiff_daily WHERE trade_date = '20250911'")
            print(f'Cleared existing records: {cursor.rowcount}')
            
            insert_sql = """
            INSERT INTO step04_fo_udiff_daily 
            (trade_date, symbol, instrument, expiry_date, strike_price, option_type, 
             open_price, high_price, low_price, close_price, settle_price, 
             contracts_traded, value_in_lakh, open_interest, change_in_oi, underlying, source_file)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            records = []
            for _, row in df.iterrows():
                record = (
                    '20250911',
                    str(row.get('SYMBOL', '')),
                    str(row.get('INSTRUMENT', '')),
                    str(row.get('EXPIRY_DT', '')).replace('-', ''),
                    float(row.get('STRIKE_PR', 0)) if pd.notna(row.get('STRIKE_PR')) else 0,
                    str(row.get('OPTION_TYP', '')),
                    float(row.get('OPEN', 0)) if pd.notna(row.get('OPEN')) else 0,
                    float(row.get('HIGH', 0)) if pd.notna(row.get('HIGH')) else 0,
                    float(row.get('LOW', 0)) if pd.notna(row.get('LOW')) else 0,
                    float(row.get('CLOSE', 0)) if pd.notna(row.get('CLOSE')) else 0,
                    float(row.get('SETTLE_PR', 0)) if pd.notna(row.get('SETTLE_PR')) else 0,
                    int(row.get('CONTRACTS', 0)) if pd.notna(row.get('CONTRACTS')) else 0,
                    float(row.get('VAL_INLAKH', 0)) if pd.notna(row.get('VAL_INLAKH')) else 0,
                    int(row.get('OPEN_INT', 0)) if pd.notna(row.get('OPEN_INT')) else 0,
                    int(row.get('CHG_IN_OI', 0)) if pd.notna(row.get('CHG_IN_OI')) else 0,
                    str(row.get('SYMBOL', '')),
                    os.path.basename(zip_file)
                )
                records.append(record)
            
            cursor.executemany(insert_sql, records)
            conn.commit()
            print(f'✅ Loaded {len(records):,} records for September 11th')
        else:
            print(f'❌ No CSV files found in {zip_file}')
else:
    print(f'❌ File not found: {zip_file}')

conn.close()
print("✅ September data loading completed!")