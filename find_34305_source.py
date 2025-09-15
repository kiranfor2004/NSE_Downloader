"""
Search for the source of 34,305 records
"""
import pyodbc

def find_34305_records():
    conn_string = (
        'Driver={ODBC Driver 17 for SQL Server};'
        'Server=SRIKIRANREDDY\\SQLEXPRESS;'
        'Database=master;'
        'Trusted_Connection=yes;'
        'TrustServerCertificate=yes;'
    )
    
    try:
        conn = pyodbc.connect(conn_string)
        cur = conn.cursor()
        
        print("🔍 SEARCHING FOR 34,305 RECORDS SOURCE")
        print("=" * 50)
        
        # Check different date formats for Feb 3
        print("\n📅 Different date formats for Feb 3, 2025:")
        date_variations = ['20250203', '2025-02-03', '03/02/2025', '03-02-2025', '2025/02/03', '03022025']
        
        for date_format in date_variations:
            try:
                cur.execute("SELECT COUNT(*) FROM step04_fo_udiff_daily WHERE trade_date = ?", date_format)
                count = cur.fetchone()[0]
                if count > 0:
                    print(f"  {date_format}: {count:,} records")
            except:
                pass
        
        # Check total February
        cur.execute("SELECT COUNT(*) FROM step04_fo_udiff_daily WHERE trade_date LIKE '202502%'")
        total_feb = cur.fetchone()[0]
        print(f"\n📊 Total February 2025: {total_feb:,} records")
        
        # Check if all data in table is 34,305
        cur.execute("SELECT COUNT(*) FROM step04_fo_udiff_daily")
        total_all = cur.fetchone()[0]
        print(f"📊 Total all data in table: {total_all:,} records")
        
        # Check for any single date with large counts
        print(f"\n🔍 Dates with highest record counts:")
        cur.execute("""
            SELECT TOP 10
                trade_date, 
                COUNT(*) as count 
            FROM step04_fo_udiff_daily 
            GROUP BY trade_date 
            ORDER BY COUNT(*) DESC
        """)
        
        top_dates = cur.fetchall()
        for row in top_dates:
            print(f"  {row[0]}: {row[1]:,} records")
        
        # Check for records near 34,305
        print(f"\n🎯 Looking for counts near 34,305:")
        target = 34305
        tolerance = 100
        
        cur.execute(f"""
            SELECT trade_date, COUNT(*) as count 
            FROM step04_fo_udiff_daily 
            GROUP BY trade_date 
            HAVING COUNT(*) BETWEEN {target - tolerance} AND {target + tolerance}
            ORDER BY COUNT(*) DESC
        """)
        
        near_matches = cur.fetchall()
        if near_matches:
            print("Found near matches:")
            for row in near_matches:
                print(f"  {row[0]}: {row[1]:,} records")
        else:
            print("  No dates with counts near 34,305")
        
        # Check if you might be looking at a cumulative count
        print(f"\n🧮 Cumulative analysis:")
        print(f"  If you saw 34,305 records for Feb 3:")
        print(f"  - Single day actual: {1962:,}")
        print(f"  - Maybe cumulative up to Feb 3? Let's check...")
        
        cur.execute("""
            SELECT 
                trade_date,
                COUNT(*) as daily_count,
                SUM(COUNT(*)) OVER (ORDER BY trade_date) as cumulative_count
            FROM step04_fo_udiff_daily 
            WHERE trade_date LIKE '202502%'
            GROUP BY trade_date 
            ORDER BY trade_date
        """)
        
        cumulative = cur.fetchall()
        print(f"\nCumulative counts:")
        for row in cumulative:
            print(f"  {row[0]}: {row[1]:,} daily, {row[2]:,} cumulative")
            if abs(row[2] - 34305) < 1000:  # Close to 34,305
                print(f"    ⭐ CLOSE TO 34,305!")
        
        # Check if maybe there was old data
        print(f"\n🗂️ Checking for other data sources:")
        
        # List all tables that might contain F&O data
        cur.execute("""
            SELECT TABLE_NAME, TABLE_SCHEMA 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME LIKE '%fo%' OR TABLE_NAME LIKE '%deriv%' OR TABLE_NAME LIKE '%option%' OR TABLE_NAME LIKE '%future%'
            ORDER BY TABLE_NAME
        """)
        
        fo_tables = cur.fetchall()
        if fo_tables:
            print("Found potential F&O tables:")
            for table in fo_tables:
                try:
                    cur.execute(f"SELECT COUNT(*) FROM {table[1]}.{table[0]}")
                    count = cur.fetchone()[0]
                    print(f"  {table[1]}.{table[0]}: {count:,} records")
                except:
                    print(f"  {table[1]}.{table[0]}: (error accessing)")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    find_34305_records()
