import pyodbc
import sys

def check_table_constraints():
    """Check which columns in step03_compare_monthvspreviousmonth don't allow NULL"""
    try:
        connection_string = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=SRIKIRANREDDY\\SQLEX;DATABASE=master;Trusted_Connection=yes;'
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()

        # Get column constraints
        cursor.execute("""
        SELECT 
            c.COLUMN_NAME,
            c.IS_NULLABLE,
            c.DATA_TYPE,
            c.COLUMN_DEFAULT
        FROM INFORMATION_SCHEMA.COLUMNS c
        WHERE c.TABLE_NAME = 'step03_compare_monthvspreviousmonth'
            AND c.IS_NULLABLE = 'NO'
        ORDER BY c.ORDINAL_POSITION
        """)

        print('NON-NULL Columns in step03_compare_monthvspreviousmonth:')
        print('=' * 60)
        non_null_columns = []
        for row in cursor.fetchall():
            col_name, is_nullable, data_type, default_val = row
            print(f'{col_name:<30} {is_nullable:<10} {data_type:<15} {default_val or "No Default"}')
            non_null_columns.append(col_name)

        print(f'\nTotal non-null columns: {len(non_null_columns)}')
        
        # Also get sample data to understand the structure
        cursor.execute("SELECT TOP 2 * FROM step03_compare_monthvspreviousmonth")
        sample_data = cursor.fetchall()
        if sample_data:
            print('\nSample existing data structure:')
            print('-' * 40)
            for i, row in enumerate(sample_data):
                print(f'Row {i+1}: First 10 values = {row[:10]}')

        connection.close()
        return non_null_columns

    except Exception as e:
        print(f"Error: {e}")
        return []

if __name__ == "__main__":
    check_table_constraints()