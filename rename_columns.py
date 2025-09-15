import pyodbc
import sys

def rename_table_columns():
    """Execute column renaming for step03_compare_monthvspreviousmonth table"""
    try:
        connection_string = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=SRIKIRANREDDY\\SQLEX;DATABASE=master;Trusted_Connection=yes;'
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()

        print('🔄 Starting column renaming for step03_compare_monthvspreviousmonth table...')
        print('=' * 70)

        # Define the column mappings
        feb_columns = [
            'feb_trade_date', 'feb_prev_close', 'feb_open_price', 'feb_high_price', 
            'feb_low_price', 'feb_last_price', 'feb_close_price', 'feb_avg_price',
            'feb_ttl_trd_qnty', 'feb_turnover_lacs', 'feb_no_of_trades', 
            'feb_deliv_qty', 'feb_deliv_per', 'feb_source_file'
        ]
        
        jan_columns = [
            'jan_baseline_date', 'jan_prev_close', 'jan_open_price', 'jan_high_price',
            'jan_low_price', 'jan_last_price', 'jan_close_price', 'jan_avg_price',
            'jan_ttl_trd_qnty', 'jan_turnover_lacs', 'jan_no_of_trades',
            'jan_deliv_qty', 'jan_deliv_per', 'jan_source_file'
        ]

        # Rename feb_ columns to current_
        print('🔄 Renaming feb_ columns to current_...')
        for col in feb_columns:
            new_name = col.replace('feb_', 'current_')
            try:
                sql = f"EXEC sp_rename 'step03_compare_monthvspreviousmonth.{col}', '{new_name}', 'COLUMN'"
                cursor.execute(sql)
                print(f"   ✅ {col} → {new_name}")
            except Exception as e:
                if "does not exist" in str(e) or "already exists" in str(e):
                    print(f"   ⚠️ {col} → {new_name} (already renamed or doesn't exist)")
                else:
                    print(f"   ❌ Error renaming {col}: {e}")

        # Rename jan_ columns to previous_
        print('\n🔄 Renaming jan_ columns to previous_...')
        for col in jan_columns:
            new_name = col.replace('jan_', 'previous_')
            try:
                sql = f"EXEC sp_rename 'step03_compare_monthvspreviousmonth.{col}', '{new_name}', 'COLUMN'"
                cursor.execute(sql)
                print(f"   ✅ {col} → {new_name}")
            except Exception as e:
                if "does not exist" in str(e) or "already exists" in str(e):
                    print(f"   ⚠️ {col} → {new_name} (already renamed or doesn't exist)")
                else:
                    print(f"   ❌ Error renaming {col}: {e}")

        # Verify the changes
        print('\n📋 Verifying new column names...')
        cursor.execute("""
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'step03_compare_monthvspreviousmonth'
        AND (COLUMN_NAME LIKE 'current_%' OR COLUMN_NAME LIKE 'previous_%')
        ORDER BY COLUMN_NAME
        """)
        
        renamed_columns = cursor.fetchall()
        print(f'\n✅ Found {len(renamed_columns)} renamed columns:')
        current_cols = []
        previous_cols = []
        
        for row in renamed_columns:
            col_name = row[0]
            if col_name.startswith('current_'):
                current_cols.append(col_name)
            elif col_name.startswith('previous_'):
                previous_cols.append(col_name)
        
        print(f'\n📊 current_ columns ({len(current_cols)}):')
        for col in sorted(current_cols):
            print(f"   {col}")
            
        print(f'\n📊 previous_ columns ({len(previous_cols)}):')
        for col in sorted(previous_cols):
            print(f"   {col}")

        connection.commit()
        connection.close()
        
        print('\n🎉 Column renaming completed successfully!')
        print('✅ feb_ columns are now current_ columns')
        print('✅ jan_ columns are now previous_ columns')
        
        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    success = rename_table_columns()
    if success:
        print('\n💡 Next step: Update the Python scripts to use new column names')
    else:
        print('\n❌ Column renaming failed. Please check the error messages above.')