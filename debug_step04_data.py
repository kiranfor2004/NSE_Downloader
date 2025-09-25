import pyodbc
import pandas as pd
from datetime import datetime

def debug_step04_data():
    try:
        # Database connection
        server = 'SRIKIRANREDDY\\SQLEXPRESS'
        database = 'master'
        
        connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;'
        
        conn = pyodbc.connect(connection_string)
        print("✅ Database connection successful")
        
        # Check table exists
        table_check = """
        SELECT COUNT(*) as table_exists 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_NAME = 'step04_fo_udiff_daily'
        """
        
        result = conn.execute(table_check).fetchone()
        print(f"📊 Table exists: {result[0] > 0}")
        
        if result[0] == 0:
            print("❌ Table step04_fo_udiff_daily does not exist!")
            return
        
        # Check total records
        count_query = "SELECT COUNT(*) FROM step04_fo_udiff_daily"
        total_records = conn.execute(count_query).fetchone()[0]
        print(f"📈 Total records in table: {total_records}")
        
        # Check date range
        date_range_query = """
        SELECT 
            MIN(Trade_date) as min_date,
            MAX(Trade_date) as max_date,
            COUNT(DISTINCT Trade_date) as unique_dates
        FROM step04_fo_udiff_daily
        """
        
        date_info = conn.execute(date_range_query).fetchone()
        print(f"📅 Date range: {date_info[0]} to {date_info[1]}")
        print(f"📅 Unique trading dates: {date_info[2]}")
        
        # Check February 2025 data specifically
        feb_query = """
        SELECT 
            COUNT(*) as feb_records,
            COUNT(DISTINCT Trade_date) as feb_dates,
            COUNT(DISTINCT Symbol) as feb_symbols
        FROM step04_fo_udiff_daily 
        WHERE Trade_date >= 20250201 AND Trade_date <= 20250228
        """
        
        feb_info = conn.execute(feb_query).fetchone()
        print(f"\n🎯 FEBRUARY 2025 DATA:")
        print(f"   Records: {feb_info[0]}")
        print(f"   Unique dates: {feb_info[1]}")
        print(f"   Unique symbols: {feb_info[2]}")
        
        # Sample February dates
        sample_dates_query = """
        SELECT DISTINCT TOP 10 Trade_date 
        FROM step04_fo_udiff_daily 
        WHERE Trade_date >= 20250201 AND Trade_date <= 20250228
        ORDER BY Trade_date
        """
        
        print(f"\n📋 Sample February dates:")
        for row in conn.execute(sample_dates_query):
            date_str = str(row[0])
            formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
            print(f"   {row[0]} ({formatted_date})")
        
        # Check available symbols for one date
        if feb_info[1] > 0:
            sample_symbol_query = """
            SELECT TOP 5 Symbol, COUNT(*) as records
            FROM step04_fo_udiff_daily 
            WHERE Trade_date = (
                SELECT MIN(Trade_date) 
                FROM step04_fo_udiff_daily 
                WHERE Trade_date >= 20250201 AND Trade_date <= 20250228
            )
            GROUP BY Symbol
            ORDER BY Symbol
            """
            
            print(f"\n🏷️ Sample symbols for first February date:")
            for row in conn.execute(sample_symbol_query):
                print(f"   {row[0]}: {row[1]} records")
        
        # Check table structure
        structure_query = """
        SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = 'step04_fo_udiff_daily'
        ORDER BY ORDINAL_POSITION
        """
        
        print(f"\n🏗️ Table structure:")
        for row in conn.execute(structure_query):
            print(f"   {row[0]}: {row[1]} ({'NULL' if row[2] == 'YES' else 'NOT NULL'})")
        
        conn.close()
        print("\n✅ Diagnostic complete!")
        
    except Exception as e:
        print(f"❌ Error during diagnosis: {str(e)}")

if __name__ == "__main__":
    debug_step04_data()