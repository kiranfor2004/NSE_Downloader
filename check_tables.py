"""
Check available tables in NSE database
"""

import pyodbc
import json
import sys

def main():
    try:
        with open('database_config.json', 'r') as f:
            config = json.load(f)
        
        # Build connection string for Windows Authentication
        conn_str = (
            f"DRIVER={{{config['driver']}}};"
            f"SERVER={config['server']};"
            f"DATABASE={config['database']};"
            f"Trusted_Connection=yes;"
        )
        
        connection = pyodbc.connect(conn_str)
        print(f"✅ Connected to database: {config['database']}")
        
        cursor = connection.cursor()
        
        # Get all user tables
        cursor.execute("""
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_TYPE = 'BASE TABLE' 
            ORDER BY TABLE_NAME
        """)
        
        tables = cursor.fetchall()
        
        print("\n📋 Available Tables:")
        print("-" * 40)
        for table in tables:
            print(f"  - {table[0]}")
        
        # Look for tables with step03 or monthly comparison related names
        cursor.execute("""
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_TYPE = 'BASE TABLE' 
            AND (TABLE_NAME LIKE '%step03%' OR TABLE_NAME LIKE '%month%' OR TABLE_NAME LIKE '%compare%')
            ORDER BY TABLE_NAME
        """)
        
        step03_tables = cursor.fetchall()
        
        if step03_tables:
            print("\n🔍 Step03/Monthly Comparison Related Tables:")
            print("-" * 50)
            for table in step03_tables:
                print(f"  - {table[0]}")
                
                # Get sample data from each table
                try:
                    cursor.execute(f"SELECT TOP 3 * FROM {table[0]}")
                    sample_data = cursor.fetchall()
                    if sample_data:
                        print(f"    Sample columns: {[desc[0] for desc in cursor.description]}")
                except Exception as e:
                    print(f"    Error reading table: {e}")
        
        cursor.close()
        connection.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()