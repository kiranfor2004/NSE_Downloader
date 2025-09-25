import pyodbc
import pandas as pd

def check_step03_structure():
    try:
        connection_string = (
            'DRIVER={ODBC Driver 17 for SQL Server};'
            'SERVER=SRIKIRANREDDY\\SQLEXPRESS;'
            'DATABASE=master;'
            'Trusted_Connection=yes;'
        )
        
        conn = pyodbc.connect(connection_string)
        print("Database connected successfully")
        
        # Check if step03 table exists
        table_check = """
        SELECT COUNT(*) as table_exists 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_NAME = 'step03_compare_monthvspreviousmonth'
        """
        
        result = conn.execute(table_check).fetchone()
        print(f"step03_compare_monthvspreviousmonth table exists: {result[0] > 0}")
        
        if result[0] > 0:
            # Check table structure
            structure_query = """
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = 'step03_compare_monthvspreviousmonth'
            ORDER BY ORDINAL_POSITION
            """
            
            print("step03_compare_monthvspreviousmonth table structure:")
            for row in conn.execute(structure_query):
                print(f"   {row[0]}: {row[1]} ({'NULL' if row[2] == 'YES' else 'NOT NULL'})")
            
            # Sample data
            sample_query = "SELECT TOP 5 * FROM step03_compare_monthvspreviousmonth"
            df = pd.read_sql(sample_query, conn)
            print("\nSample data:")
            print(df.head())
        
        # Also check step04 table columns we can use for current price
        print("\nChecking step04_fo_udiff_daily for available price columns:")
        step04_structure = """
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = 'step04_fo_udiff_daily'
        AND COLUMN_NAME LIKE '%price%'
        ORDER BY COLUMN_NAME
        """
        
        print("Price-related columns in step04:")
        for row in conn.execute(step04_structure):
            print(f"   {row[0]}")
        
        conn.close()
        
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    check_step03_structure()