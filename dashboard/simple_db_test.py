import pyodbc
import json

def test_connection():
    try:
        # Test connection with the database config
        connection_string = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=SRIKIRANREDDY\\SQLEXPRESS;"
            "DATABASE=master;"
            "Integrated Security=yes;"
        )
        
        print("Testing database connection...")
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        
        # Test basic query
        cursor.execute("SELECT 1 as test")
        result = cursor.fetchone()
        print(f"✅ Database connection successful: {result[0]}")
        
        # Test step03 table
        cursor.execute("SELECT COUNT(*) FROM step03_compare_monthvspreviousmonth")
        count = cursor.fetchone()[0]
        print(f"✅ step03_compare_monthvspreviousmonth table found with {count} records")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False

if __name__ == "__main__":
    test_connection()