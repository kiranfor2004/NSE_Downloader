#!/usr/bin/env python3

import pyodbc
import json

def update_table_for_large_values():
    """Update table structure to handle large values from CSV"""
    
    print("🔧 UPDATING TABLE STRUCTURE FOR LARGE VALUES")
    print("="*60)
    
    try:
        # Connect to database
        with open('database_config.json', 'r') as f:
            config = json.load(f)
        
        conn_str = f"DRIVER={config['driver']};SERVER={config['server']};DATABASE={config['database']};Trusted_Connection=yes;"
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        print("💾 Connected to database")
        
        # Step 1: Alter columns that can have large values
        alterations = [
            # Open Interest can be very large (3+ billion)
            "ALTER TABLE step04_fo_udiff_daily ALTER COLUMN open_interest BIGINT",
            
            # Change in Open Interest can also be large
            "ALTER TABLE step04_fo_udiff_daily ALTER COLUMN change_in_oi BIGINT", 
            
            # Value in lakh can be very large (trillions)
            "ALTER TABLE step04_fo_udiff_daily ALTER COLUMN value_in_lakh FLOAT",
            
            # TtlTrfVal - Total Transfer Value can be very large
            "ALTER TABLE step04_fo_udiff_daily ALTER COLUMN TtlTrfVal FLOAT"
        ]
        
        print(f"\n🔄 Applying table alterations...")
        
        for i, alter_sql in enumerate(alterations, 1):
            try:
                print(f"   {i}. {alter_sql}")
                cursor.execute(alter_sql)
                conn.commit()
                print(f"      ✅ Success")
            except Exception as e:
                print(f"      ⚠️  Warning: {e}")
        
        # Step 2: Check current table structure
        print(f"\n📋 Checking updated table structure...")
        
        structure_query = """
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            CHARACTER_MAXIMUM_LENGTH,
            NUMERIC_PRECISION,
            NUMERIC_SCALE,
            IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'step04_fo_udiff_daily'
        AND COLUMN_NAME IN ('open_interest', 'change_in_oi', 'value_in_lakh', 'TtlTrfVal')
        ORDER BY ORDINAL_POSITION
        """
        
        cursor.execute(structure_query)
        columns = cursor.fetchall()
        
        print("Updated columns:")
        for col in columns:
            col_name, data_type, char_len, num_prec, num_scale, nullable = col
            print(f"  {col_name}: {data_type}" + 
                  (f"({char_len})" if char_len else "") +
                  (f"({num_prec},{num_scale})" if num_prec and data_type in ['decimal', 'numeric'] else "") +
                  (" NULL" if nullable == "YES" else " NOT NULL"))
        
        conn.close()
        
        print(f"\n✅ Table structure updated successfully!")
        print(f"📊 Ready to handle large values:")
        print(f"   - Open Interest: up to 9+ quintillion")
        print(f"   - Change in OI: up to 9+ quintillion") 
        print(f"   - Value fields: floating point precision")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    update_table_for_large_values()
