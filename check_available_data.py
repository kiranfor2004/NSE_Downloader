#!/usr/bin/env python3
"""
Quick script to check available data for Step 3 implementation
"""

from nse_database_integration import NSEDatabaseManager

def check_data():
    db = NSEDatabaseManager()
    cursor = db.connection.cursor()
    
    print("📊 Checking available data for Step 3...")
    
    # Check available months for EQ series
    cursor.execute("""
        SELECT DISTINCT YEAR(trade_date), MONTH(trade_date), COUNT(*) 
        FROM step01_equity_daily 
        WHERE series='EQ' 
        GROUP BY YEAR(trade_date), MONTH(trade_date) 
        ORDER BY YEAR(trade_date), MONTH(trade_date)
    """)
    
    print("\n📅 Available EQ data by month:")
    result = cursor.fetchall()
    for row in result:
        print(f"  {row[0]}-{row[1]:02d}: {row[2]:,} records")
    
    # Check if we have February and March 2025 data
    feb_2025 = any(row[0] == 2025 and row[1] == 2 for row in result)
    mar_2025 = any(row[0] == 2025 and row[1] == 3 for row in result)
    
    print(f"\n🎯 Step 3 Requirements:")
    print(f"  February 2025 EQ data: {'✅ Available' if feb_2025 else '❌ Missing'}")
    print(f"  March 2025 EQ data: {'✅ Available' if mar_2025 else '❌ Missing'}")
    
    if feb_2025 and mar_2025:
        print(f"\n✅ Ready to implement Step 3: March vs February delivery analysis")
    else:
        print(f"\n⚠️ Missing required data for Step 3")
        if not feb_2025:
            print(f"  - Need February 2025 data for peak baselines")
        if not mar_2025:
            print(f"  - Need March 2025 data for daily comparisons")
    
    db.close()

if __name__ == '__main__':
    check_data()