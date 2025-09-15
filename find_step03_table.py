"""
Find the step03_compare_monthvspreviousmonth table across all databases
"""

import pyodbc
import json
import sys

def main():
    try:
        with open('database_config.json', 'r') as f:
            config = json.load(f)
        
        # Connect to master database first to check all databases
        conn_str = (
            f"DRIVER={{{config['driver']}}};"
            f"SERVER={config['server']};"
            f"DATABASE=master;"
            f"Trusted_Connection=yes;"
        )
        
        connection = pyodbc.connect(conn_str)
        print(f"✅ Connected to server: {config['server']}")
        
        cursor = connection.cursor()
        
        # Get all databases
        cursor.execute("SELECT name FROM sys.databases WHERE name NOT IN ('master', 'model', 'msdb', 'tempdb') ORDER BY name")
        databases = cursor.fetchall()
        
        print("\n📋 Available Databases:")
        print("-" * 30)
        for db in databases:
            print(f"  - {db[0]}")
        
        cursor.close()
        connection.close()
        
        # Check each database for the table
        for db in databases:
            db_name = db[0]
            try:
                print(f"\n🔍 Checking database: {db_name}")
                
                conn_str = (
                    f"DRIVER={{{config['driver']}}};"
                    f"SERVER={config['server']};"
                    f"DATABASE={db_name};"
                    f"Trusted_Connection=yes;"
                )
                
                connection = pyodbc.connect(conn_str)
                cursor = connection.cursor()
                
                # Look for step03 or monthly comparison tables
                cursor.execute("""
                    SELECT TABLE_NAME 
                    FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_TYPE = 'BASE TABLE' 
                    AND (TABLE_NAME LIKE '%step03%' OR TABLE_NAME LIKE '%month%' OR TABLE_NAME LIKE '%compare%')
                    ORDER BY TABLE_NAME
                """)
                
                step03_tables = cursor.fetchall()
                
                if step03_tables:
                    print(f"  ✅ Found tables:")
                    for table in step03_tables:
                        print(f"    - {table[0]}")
                        
                        # Get table info
                        try:
                            cursor.execute(f"SELECT COUNT(*) FROM {table[0]}")
                            count = cursor.fetchone()[0]
                            print(f"      Records: {count}")
                            
                            # Get column info
                            cursor.execute(f"SELECT TOP 1 * FROM {table[0]}")
                            columns = [desc[0] for desc in cursor.description]
                            print(f"      Columns: {', '.join(columns[:5])}{'...' if len(columns) > 5 else ''}")
                            
                        except Exception as e:
                            print(f"      Error reading table: {e}")
                else:
                    print(f"  ❌ No step03/monthly tables found")
                
                cursor.close()
                connection.close()
                
            except Exception as e:
                print(f"  ❌ Error accessing database {db_name}: {e}")
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()