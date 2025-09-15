#!/usr/bin/env python3
"""
Debug script to check February and March 2025 data availability
"""

from nse_database_integration import NSEDatabaseManager

def check_data_availability():
    db = NSEDatabaseManager()
    cursor = db.connection.cursor()
    
    print("📊 Checking data availability...")
    
    # Check February 2025 data
    print("\n🔍 February 2025 EQ series data:")
    cursor.execute("""
        SELECT COUNT(*) as count
        FROM step01_equity_daily 
        WHERE YEAR(trade_date) = 2025 
            AND MONTH(trade_date) = 2 
            AND series = 'EQ'
    """)
    feb_count = cursor.fetchone()[0]
    print(f"   📈 February 2025 EQ records: {feb_count:,}")
    
    # Check March 2025 data
    print("\n🔍 March 2025 EQ series data:")
    cursor.execute("""
        SELECT COUNT(*) as count
        FROM step01_equity_daily 
        WHERE YEAR(trade_date) = 2025 
            AND MONTH(trade_date) = 3 
            AND series = 'EQ'
    """)
    mar_count = cursor.fetchone()[0]
    print(f"   📈 March 2025 EQ records: {mar_count:,}")
    
    # Check date ranges available
    print("\n📅 Available date ranges:")
    cursor.execute("""
        SELECT MIN(trade_date) as min_date, MAX(trade_date) as max_date
        FROM step01_equity_daily 
        WHERE series = 'EQ'
    """)
    row = cursor.fetchone()
    print(f"   📊 Date range: {row[0]} to {row[1]}")
    
    # Check February sample
    if feb_count > 0:
        print("\n📋 February 2025 sample symbols:")
        cursor.execute("""
            SELECT TOP 5 symbol, trade_date, ttl_trd_qnty, deliv_qty
            FROM step01_equity_daily 
            WHERE YEAR(trade_date) = 2025 
                AND MONTH(trade_date) = 2 
                AND series = 'EQ'
            ORDER BY trade_date, symbol
        """)
        for row in cursor.fetchall():
            print(f"     {row[0]} | {row[1]} | Vol: {row[2]:,} | Del: {row[3]:,}")
    
    # Check March sample
    if mar_count > 0:
        print("\n📋 March 2025 sample symbols:")
        cursor.execute("""
            SELECT TOP 5 symbol, trade_date, ttl_trd_qnty, deliv_qty
            FROM step01_equity_daily 
            WHERE YEAR(trade_date) = 2025 
                AND MONTH(trade_date) = 3 
                AND series = 'EQ'
            ORDER BY trade_date, symbol
        """)
        for row in cursor.fetchall():
            print(f"     {row[0]} | {row[1]} | Vol: {row[2]:,} | Del: {row[3]:,}")
    
    db.close()

if __name__ == '__main__':
    check_data_availability()