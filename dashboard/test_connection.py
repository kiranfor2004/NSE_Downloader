"""
Test database connection for the dashboard API
"""
import pyodbc
import json

def test_connection():
    try:
        # Load config
        with open('database_config.json', 'r') as f:
            config = json.load(f)
        
        master_config = config.get('master_database', {})
        server_name = master_config.get('server', 'SRIKIRANREDDY\\SQLEXPRESS')
        database_name = master_config.get('database', 'master')
        driver_name = master_config.get('driver', 'ODBC Driver 17 for SQL Server')
        
        connection_string = (
            f"DRIVER={{{driver_name}}};"
            f"SERVER={server_name};"
            f"DATABASE={database_name};"
            f"Trusted_Connection=yes;"
        )
        
        print(f"Testing connection with: {connection_string}")
        
        # Test connection
        conn = pyodbc.connect(connection_string)
        print("✅ Database connection successful!")
        
        # Test table query
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM step03_compare_monthvspreviousmonth")
        count = cursor.fetchone()[0]
        print(f"✅ Table step03_compare_monthvspreviousmonth found with {count} records")
        
        cursor.close()
        conn.close()
        print("✅ Connection test completed successfully")
        
    except Exception as e:
        print(f"❌ Connection test failed: {e}")

if __name__ == '__main__':
    test_connection()