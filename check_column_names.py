import pyodbc
import sys

def check_table_columns():
    """Check all column names in step03_compare_monthvspreviousmonth table"""
    try:
        connection_string = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=SRIKIRANREDDY\\SQLEX;DATABASE=master;Trusted_Connection=yes;'
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()

        # Get all column names
        cursor.execute("""
        SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'step03_compare_monthvspreviousmonth'
        ORDER BY ORDINAL_POSITION
        """)

        print('📋 Current Columns in step03_compare_monthvspreviousmonth:')
        print('=' * 70)
        print(f"{'Column Name':<35} {'Type':<15} {'Nullable':<10} {'Length':<10}")
        print('-' * 70)
        
        feb_columns = []
        jan_columns = []
        other_columns = []
        
        for row in cursor.fetchall():
            col_name, data_type, is_nullable, max_length = row
            length_str = str(max_length) if max_length else 'N/A'
            print(f"{col_name:<35} {data_type:<15} {is_nullable:<10} {length_str:<10}")
            
            if col_name.startswith('feb_'):
                feb_columns.append(col_name)
            elif col_name.startswith('jan_'):
                jan_columns.append(col_name)
            else:
                other_columns.append(col_name)

        print(f'\n📊 Summary:')
        print(f'  feb_ columns (to rename to current_): {len(feb_columns)}')
        print(f'  jan_ columns (to rename to previous_): {len(jan_columns)}')
        print(f'  Other columns (no change): {len(other_columns)}')
        
        print(f'\n🔄 feb_ columns to rename:')
        for col in feb_columns:
            new_name = col.replace('feb_', 'current_')
            print(f'  {col} → {new_name}')
            
        print(f'\n🔄 jan_ columns to rename:')
        for col in jan_columns:
            new_name = col.replace('jan_', 'previous_')
            print(f'  {col} → {new_name}')

        connection.close()
        return feb_columns, jan_columns, other_columns

    except Exception as e:
        print(f"❌ Error: {e}")
        return [], [], []

if __name__ == "__main__":
    check_table_columns()