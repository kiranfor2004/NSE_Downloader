#!/usr/bin/env python3

import pyodbc
import json

def update_table_structure():
    """Update the table to include all missing UDiFF columns"""
    
    # Load database configuration
    with open('database_config.json', 'r') as f:
        config = json.load(f)

    try:
        conn_str = f"DRIVER={config['driver']};SERVER={config['server']};DATABASE={config['database']};Trusted_Connection=yes;"
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        print("🔍 Current table structure check...")
        
        # Get current columns
        cursor.execute("""
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'step04_fo_udiff_daily' 
        ORDER BY ORDINAL_POSITION
        """)
        
        current_columns = [row[0] for row in cursor.fetchall()]
        print(f"Current columns: {len(current_columns)}")
        
        # Define the missing columns to add (using exact UDiFF names)
        missing_columns = [
            ('BizDt', 'VARCHAR(8)', 'Business Date'),
            ('Sgmt', 'VARCHAR(10)', 'Segment'), 
            ('Src', 'VARCHAR(10)', 'Source'),
            ('FinInstrmActlXpryDt', 'VARCHAR(10)', 'Financial Instrument Actual Expiry Date'),
            ('FinInstrmId', 'VARCHAR(50)', 'Financial Instrument ID'),
            ('ISIN', 'VARCHAR(12)', 'ISIN Code'),
            ('SctySrs', 'VARCHAR(10)', 'Security Series'),
            ('FinInstrmNm', 'VARCHAR(200)', 'Financial Instrument Name'),
            ('LastPric', 'FLOAT', 'Last Price'),
            ('PrvsClsgPric', 'FLOAT', 'Previous Closing Price'),
            ('UndrlygPric', 'FLOAT', 'Underlying Price'),
            ('TtlNbOfTxsExctd', 'INT', 'Total Number of Transactions Executed'),
            ('SsnId', 'VARCHAR(20)', 'Session ID'),
            ('NewBrdLotQty', 'INT', 'New Board Lot Quantity'),
            ('Rmks', 'VARCHAR(500)', 'Remarks'),
            ('Rsvd01', 'VARCHAR(50)', 'Reserved 01'),
            ('Rsvd02', 'VARCHAR(50)', 'Reserved 02'),
            ('Rsvd03', 'VARCHAR(50)', 'Reserved 03'),
            ('Rsvd04', 'VARCHAR(50)', 'Reserved 04')
        ]
        
        print(f"\\n📝 Adding {len(missing_columns)} missing columns...")
        
        # Add each missing column
        for col_name, col_type, description in missing_columns:
            if col_name not in current_columns:
                try:
                    alter_sql = f"ALTER TABLE step04_fo_udiff_daily ADD {col_name} {col_type} NULL"
                    print(f"  Adding: {col_name} ({col_type}) - {description}")
                    cursor.execute(alter_sql)
                    conn.commit()
                    print(f"  ✅ Added {col_name}")
                except Exception as e:
                    print(f"  ❌ Error adding {col_name}: {e}")
            else:
                print(f"  ⏭️  {col_name} already exists")
        
        print(f"\\n🔍 Updated table structure check...")
        
        # Get updated columns
        cursor.execute("""
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            CHARACTER_MAXIMUM_LENGTH,
            IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'step04_fo_udiff_daily' 
        ORDER BY ORDINAL_POSITION
        """)
        
        updated_columns = cursor.fetchall()
        print(f"Updated total columns: {len(updated_columns)}")
        
        print("\\nFinal table structure:")
        print("Column Name".ljust(25) + "Data Type".ljust(15) + "Length".ljust(10) + "Nullable")
        print("-" * 65)
        
        for col in updated_columns:
            length = str(col[2]) if col[2] else 'N/A'
            nullable = 'YES' if col[3] == 'YES' else 'NO'
            print(f"{col[0].ljust(25)}{col[1].ljust(15)}{length.ljust(10)}{nullable}")
        
        conn.close()
        
        print(f"\\n✅ Table structure update completed!")
        print(f"📊 Total columns: {len(updated_columns)} (increased from {len(current_columns)})")
        
        return True
        
    except Exception as e:
        print(f"❌ Error updating table structure: {e}")
        return False

def verify_udiff_column_mapping():
    """Show the mapping between UDiFF source columns and table columns"""
    
    print("\\n" + "="*60)
    print("UDiFF COLUMN MAPPING")
    print("="*60)
    
    # Complete mapping of UDiFF columns to table columns
    column_mapping = {
        # Original mapped columns
        'TradDt': 'trade_date',
        'TckrSymb': 'symbol', 
        'FinInstrmTp': 'instrument',
        'XpryDt': 'expiry_date',
        'StrkPric': 'strike_price',
        'OptnTp': 'option_type',
        'OpnPric': 'open_price',
        'HghPric': 'high_price',
        'LwPric': 'low_price',
        'ClsPric': 'close_price',
        'SttlmPric': 'settle_price',
        'TtlTrdgVol': 'contracts_traded',
        'TtlTrfVal': 'value_in_lakh',
        'OpnIntrst': 'open_interest',
        'ChngInOpnIntrst': 'change_in_oi',
        
        # Newly added columns (keeping UDiFF names)
        'BizDt': 'BizDt',
        'Sgmt': 'Sgmt',
        'Src': 'Src',
        'FinInstrmActlXpryDt': 'FinInstrmActlXpryDt',
        'FinInstrmId': 'FinInstrmId',
        'ISIN': 'ISIN',
        'SctySrs': 'SctySrs',
        'FinInstrmNm': 'FinInstrmNm',
        'LastPric': 'LastPric',
        'PrvsClsgPric': 'PrvsClsgPric',
        'UndrlygPric': 'UndrlygPric',
        'TtlNbOfTxsExctd': 'TtlNbOfTxsExctd',
        'SsnId': 'SsnId',
        'NewBrdLotQty': 'NewBrdLotQty',
        'Rmks': 'Rmks',
        'Rsvd01': 'Rsvd01',
        'Rsvd02': 'Rsvd02',
        'Rsvd03': 'Rsvd03',
        'Rsvd04': 'Rsvd04'
    }
    
    print("\\nUDiFF Source -> Table Column Mapping:")
    for udiff_col, table_col in column_mapping.items():
        if udiff_col == table_col:
            print(f"  {udiff_col} -> {table_col} (direct mapping)")
        else:
            print(f"  {udiff_col} -> {table_col} (renamed)")
    
    print(f"\\nTotal mapped columns: {len(column_mapping)}")
    
    # Table-specific columns
    table_specific = ['id', 'underlying', 'source_file', 'created_at']
    print(f"\\nTable-specific columns (not from UDiFF):")
    for col in table_specific:
        print(f"  - {col}")
    
    return column_mapping

if __name__ == "__main__":
    print("🚀 Starting table structure update for complete UDiFF support...")
    
    success = update_table_structure()
    
    if success:
        verify_udiff_column_mapping()
        print(f"\\n✅ Table is now ready to handle complete UDiFF data!")
        print(f"📋 Next step: Update data loading scripts to use all UDiFF columns")
    else:
        print(f"\\n❌ Table update failed. Please check error messages above.")
