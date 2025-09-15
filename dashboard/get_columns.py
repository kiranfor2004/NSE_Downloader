import pyodbc
import json

# Database connection
server = 'SRIKIRANREDDY\\SQLEXPRESS'
database = 'NSE_STEP01'
table_name = 'step03_compare_monthvspreviousmonth'

try:
    conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;'
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    
    # Get column information
    cursor.execute(f"""
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            IS_NULLABLE,
            CHARACTER_MAXIMUM_LENGTH,
            NUMERIC_PRECISION,
            NUMERIC_SCALE
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = '{table_name}'
        ORDER BY ORDINAL_POSITION
    """)
    
    columns = cursor.fetchall()
    
    print('Database Table Structure:')
    print(f'Table: {table_name}')
    print('-' * 80)
    print(f"{'Column Name':<25} {'Data Type':<15} {'Nullable':<10} {'Length/Precision':<20}")
    print('-' * 80)
    
    for col in columns:
        col_name = col[0]
        data_type = col[1]
        nullable = col[2]
        max_length = col[3] if col[3] else ''
        precision = col[4] if col[4] else ''
        scale = col[5] if col[5] else ''
        
        if precision and scale:
            type_detail = f'{precision},{scale}'
        elif max_length:
            type_detail = str(max_length)
        else:
            type_detail = ''
            
        print(f'{col_name:<25} {data_type:<15} {nullable:<10} {type_detail:<20}')
    
    conn.close()
    
except Exception as e:
    print(f'Error: {e}')