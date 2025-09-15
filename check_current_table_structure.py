#!/usr/bin/env python3

import pyodbc
import json

def check_table_structure():
    # Load database configuration
    with open('database_config.json', 'r') as f:
        config = json.load(f)

    try:
        conn_str = f"DRIVER={config['driver']};SERVER={config['server']};DATABASE={config['database']};Trusted_Connection=yes;"
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        # Get current table structure
        cursor.execute("""
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            CHARACTER_MAXIMUM_LENGTH,
            IS_NULLABLE,
            COLUMN_DEFAULT
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'step04_fo_udiff_daily' 
        ORDER BY ORDINAL_POSITION
        """)
        
        columns = cursor.fetchall()
        print('Current table structure:')
        print('Column Name'.ljust(30) + 'Data Type'.ljust(15) + 'Length'.ljust(10) + 'Nullable')
        print('-' * 70)
        
        current_columns = []
        for col in columns:
            length = str(col[2]) if col[2] else 'N/A'
            nullable = 'YES' if col[3] == 'YES' else 'NO'
            print(f'{col[0].ljust(30)}{col[1].ljust(15)}{length.ljust(10)}{nullable}')
            current_columns.append(col[0])
        
        print(f'\nTotal columns: {len(columns)}')
        print(f'\nCurrent columns: {current_columns}')
        
        conn.close()
        return current_columns
        
    except Exception as e:
        print(f'Error: {e}')
        return []

if __name__ == "__main__":
    check_table_structure()
