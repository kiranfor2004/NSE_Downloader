#!/usr/bin/env python3
"""
Script to analyze available data in step01_equity_daily table
"""

from nse_database_integration import NSEDatabaseManager

def analyze_step01_data():
    db = NSEDatabaseManager()
    cursor = db.connection.cursor()
    
    print("📊 STEP 01 DATA ANALYSIS - step01_equity_daily")
    print("=" * 60)
    
    # Overall summary by month
    print("\n📅 Available Data by Month:")
    cursor.execute("""
        SELECT DISTINCT YEAR(trade_date), MONTH(trade_date), COUNT(*) 
        FROM step01_equity_daily 
        GROUP BY YEAR(trade_date), MONTH(trade_date) 
        ORDER BY YEAR(trade_date), MONTH(trade_date)
    """)
    
    result = cursor.fetchall()
    total_records = 0
    for row in result:
        month_name = {1: 'January', 2: 'February', 3: 'March', 4: 'April', 
                     5: 'May', 6: 'June', 7: 'July', 8: 'August', 
                     9: 'September', 10: 'October', 11: 'November', 12: 'December'}
        print(f"  📅 {month_name[row[1]]} {row[0]}: {row[2]:,} records")
        total_records += row[2]
    
    print(f"\n📊 Total: {len(result)} months, {total_records:,} records")
    
    # EQ series specific analysis
    print("\n📈 EQ (Equity) Series Analysis:")
    cursor.execute("""
        SELECT DISTINCT YEAR(trade_date), MONTH(trade_date), COUNT(*) 
        FROM step01_equity_daily 
        WHERE series = 'EQ'
        GROUP BY YEAR(trade_date), MONTH(trade_date) 
        ORDER BY YEAR(trade_date), MONTH(trade_date)
    """)
    
    eq_result = cursor.fetchall()
    eq_total = 0
    for row in eq_result:
        month_name = {1: 'January', 2: 'February', 3: 'March', 4: 'April', 
                     5: 'May', 6: 'June', 7: 'July', 8: 'August', 
                     9: 'September', 10: 'October', 11: 'November', 12: 'December'}
        print(f"  📈 {month_name[row[1]]} {row[0]} (EQ): {row[2]:,} records")
        eq_total += row[2]
    
    print(f"\n🎯 EQ Total: {eq_total:,} records")
    
    # Series distribution
    print("\n📊 Series Distribution (All Data):")
    cursor.execute("""
        SELECT series, COUNT(*) as count
        FROM step01_equity_daily 
        GROUP BY series 
        ORDER BY COUNT(*) DESC
    """)
    
    series_result = cursor.fetchall()
    for row in series_result[:10]:  # Show top 10 series
        print(f"  📋 {row[0]}: {row[1]:,} records")
    
    if len(series_result) > 10:
        print(f"  ... and {len(series_result)-10} more series")
    
    # Date range
    print("\n📅 Date Range:")
    cursor.execute("""
        SELECT MIN(trade_date) as earliest, MAX(trade_date) as latest
        FROM step01_equity_daily
    """)
    
    date_result = cursor.fetchone()
    print(f"  📅 Earliest: {date_result[0]}")
    print(f"  📅 Latest: {date_result[1]}")
    
    # Sample symbols for each month
    print("\n🔍 Sample Symbols by Month:")
    for row in result[:5]:  # Show first 5 months
        month_name = {1: 'January', 2: 'February', 3: 'March', 4: 'April', 
                     5: 'May', 6: 'June', 7: 'July', 8: 'August'}
        cursor.execute("""
            SELECT TOP 5 symbol, series
            FROM step01_equity_daily 
            WHERE YEAR(trade_date) = ? AND MONTH(trade_date) = ?
            AND series = 'EQ'
            ORDER BY symbol
        """, (row[0], row[1]))
        
        symbols = cursor.fetchall()
        symbol_list = [f"{s[0]}({s[1]})" for s in symbols]
        print(f"  📅 {month_name[row[1]]} {row[0]}: {', '.join(symbol_list[:3])}{', ...' if len(symbols) > 3 else ''}")
    
    print(f"\n✅ Step 1 Data Analysis Complete!")
    print(f"🎯 Ready for Steps 2, 3, and 4 analysis")
    
    db.close()

if __name__ == '__main__':
    analyze_step01_data()