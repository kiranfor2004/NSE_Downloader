#!/usr/bin/env python3
"""
Check Step 3 results table
"""

from nse_database_integration import NSEDatabaseManager

def check_step3_table():
    db = NSEDatabaseManager()
    cursor = db.connection.cursor()
    
    print("📊 STEP 3 RESULTS TABLE INFORMATION")
    print("=" * 50)
    
    # Check table name and record count
    try:
        cursor.execute('SELECT COUNT(*) FROM step03_february_vs_march_analysis')
        count = cursor.fetchone()[0]
        print(f"📋 Table Name: step03_february_vs_march_analysis")
        print(f"📈 Total Records: {count:,}")
        print()
        
        # Show top 5 results
        print("🔝 Top 5 Delivery Exceedances:")
        cursor.execute("""
            SELECT TOP 5 symbol, trade_date, march_deliv_qty, feb_peak_delivery, 
                   delivery_increase_pct, exceedance_tier 
            FROM step03_february_vs_march_analysis 
            ORDER BY delivery_increase_pct DESC
        """)
        
        for row in cursor.fetchall():
            print(f"  {row[0]} ({row[1]}) | March: {row[2]:,} | Feb: {row[3]:,} | +{row[4]:.1f}% [{row[5]}]")
        
        # Show tier distribution
        print(f"\n🏆 Exceedance Tier Distribution:")
        cursor.execute("""
            SELECT exceedance_tier, COUNT(*) 
            FROM step03_february_vs_march_analysis 
            GROUP BY exceedance_tier 
            ORDER BY COUNT(*) DESC
        """)
        
        for row in cursor.fetchall():
            print(f"  {row[0]}: {row[1]:,} records")
            
        # Show date range
        cursor.execute("""
            SELECT MIN(trade_date) as earliest, MAX(trade_date) as latest
            FROM step03_february_vs_march_analysis
        """)
        
        date_range = cursor.fetchone()
        print(f"\n📅 Date Range: {date_range[0]} to {date_range[1]}")
        
        # Show unique symbols
        cursor.execute("""
            SELECT COUNT(DISTINCT symbol) as unique_symbols
            FROM step03_february_vs_march_analysis
        """)
        
        unique_count = cursor.fetchone()[0]
        print(f"🏢 Unique Symbols: {unique_count:,}")
        
    except Exception as e:
        print(f"❌ Error accessing table: {e}")
    
    db.close()

if __name__ == '__main__':
    check_step3_table()