"""
Check NSE_Analysis database for step03 table
"""

import pyodbc
import json
import sys

def main():
    try:
        with open('database_config.json', 'r') as f:
            config = json.load(f)
        
        # Try NSE_Analysis database
        databases_to_check = ['NSE_Analysis', 'NSE', 'master']
        
        for db_name in databases_to_check:
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
                
                # Look for step03 table
                cursor.execute("""
                    SELECT TABLE_NAME 
                    FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_TYPE = 'BASE TABLE' 
                    AND TABLE_NAME = 'step03_compare_monthvspreviousmonth'
                """)
                
                table_found = cursor.fetchall()
                
                if table_found:
                    print(f"  ✅ Found step03_compare_monthvspreviousmonth table!")
                    
                    # Get table info
                    cursor.execute("SELECT COUNT(*) FROM step03_compare_monthvspreviousmonth")
                    count = cursor.fetchone()[0]
                    print(f"      Records: {count}")
                    
                    # Get column info
                    cursor.execute("""
                        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
                        FROM INFORMATION_SCHEMA.COLUMNS 
                        WHERE TABLE_NAME = 'step03_compare_monthvspreviousmonth'
                        ORDER BY ORDINAL_POSITION
                    """)
                    columns = cursor.fetchall()
                    print(f"      Columns ({len(columns)}):")
                    for col in columns:
                        max_len = f"({col[2]})" if col[2] else ""
                        print(f"        - {col[0]} {col[1]}{max_len}")
                    
                    # Get sample data
                    cursor.execute("""
                        SELECT TOP 3 symbol, comparison_month, percentage_increase
                        FROM step03_compare_monthvspreviousmonth
                        ORDER BY percentage_increase DESC
                    """)
                    sample_data = cursor.fetchall()
                    if sample_data:
                        print(f"      Sample data:")
                        for row in sample_data:
                            print(f"        {row[0]} | {row[1]} | {row[2]:.2f}%")
                    
                    # Update the config file to use this database
                    if db_name != config['database']:
                        config['database'] = db_name
                        with open('database_config.json', 'w') as f:
                            json.dump(config, f, indent=4)
                        print(f"  📝 Updated database_config.json to use {db_name}")
                    
                    cursor.close()
                    connection.close()
                    return db_name
                    
                else:
                    print(f"  ❌ step03_compare_monthvspreviousmonth table not found")
                
                cursor.close()
                connection.close()
                
            except Exception as e:
                print(f"  ❌ Error accessing database {db_name}: {e}")
        
        print("\n❌ step03_compare_monthvspreviousmonth table not found in any database")
        return None
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

if __name__ == "__main__":
    found_db = main()
    if found_db:
        print(f"\n✅ Ready to proceed with database: {found_db}")
    else:
        print(f"\n⚠️ You may need to run one of the step03 analysis scripts first to create the table")