#!/usr/bin/env python3
"""
Simple check for February 2025 data
"""

from nse_database_integration import NSEDatabaseManager

def simple_check():
    db = NSEDatabaseManager()
    cursor = db.connection.cursor()
    
    # Check what months we have in 2025
    print("📊 Checking what 2025 data we have...")
    cursor.execute("""
        SELECT YEAR(trade_date) as year, MONTH(trade_date) as month, COUNT(*) as count
        FROM step01_equity_daily 
        WHERE YEAR(trade_date) = 2025
        GROUP BY YEAR(trade_date), MONTH(trade_date)
        ORDER BY year, month
    """)
    
    for row in cursor.fetchall():
        print(f"   📅 {row[0]}-{row[1]:02d}: {row[2]:,} records")
    
    # Check specifically February EQ series
    print("\n🔍 February 2025 EQ series details...")
    cursor.execute("""
        SELECT COUNT(*) as count
        FROM step01_equity_daily 
        WHERE YEAR(trade_date) = 2025 
            AND MONTH(trade_date) = 2 
            AND series = 'EQ'
    """)
    feb_eq_count = cursor.fetchone()[0]
    print(f"   📈 February 2025 EQ records: {feb_eq_count:,}")
    
    if feb_eq_count > 0:
        # Show some sample data
        cursor.execute("""
            SELECT TOP 3 trade_date, symbol, ttl_trd_qnty, deliv_qty
            FROM step01_equity_daily 
            WHERE YEAR(trade_date) = 2025 
                AND MONTH(trade_date) = 2 
                AND series = 'EQ'
            ORDER BY trade_date
        """)
        print("   📋 Sample February data:")
        for row in cursor.fetchall():
            print(f"     {row[1]} | {row[0]} | Vol: {row[2]} | Del: {row[3]}")
    
    db.close()

if __name__ == '__main__':
    simple_check()