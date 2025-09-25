#!/usr/bin/env python3
"""
Analyze the current data scope for implementing full month 50% reduction analysis
"""
import pyodbc

def analyze_data_scope():
    """Analyze current data availability for full month implementation"""
    
    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=SRIKIRANREDDY\\SQLEXPRESS;DATABASE=master;Trusted_Connection=yes;')
    cursor = conn.cursor()

    print('📊 FULL MONTH IMPLEMENTATION - DATA SCOPE ANALYSIS')
    print('='*60)

    # Check Step05 base data
    print('\n🎯 STEP05 BASE DATA ANALYSIS:')
    try:
        cursor.execute('SELECT COUNT(*) FROM Step05_strikepriceAnalysisderived')
        base_count = cursor.fetchone()[0]
        print(f'   Total base records: {base_count:,}')
        
        cursor.execute('SELECT COUNT(DISTINCT Symbol) FROM Step05_strikepriceAnalysisderived')
        base_symbols = cursor.fetchone()[0]
        print(f'   Unique symbols: {base_symbols:,}')
        
        cursor.execute('SELECT COUNT(DISTINCT Strike_price) FROM Step05_strikepriceAnalysisderived')
        base_strikes = cursor.fetchone()[0]
        print(f'   Unique strikes: {base_strikes:,}')
        
        cursor.execute('SELECT COUNT(DISTINCT option_type) FROM Step05_strikepriceAnalysisderived')
        base_options = cursor.fetchone()[0]
        print(f'   Option types: {base_options:,}')
        
        cursor.execute('SELECT MIN(Current_trade_date), MAX(Current_trade_date) FROM Step05_strikepriceAnalysisderived')
        date_range = cursor.fetchone()
        print(f'   Date range: {date_range[0]} to {date_range[1]}')
        
        # Sample data
        cursor.execute('SELECT TOP 5 Symbol, Strike_price, option_type, close_price FROM Step05_strikepriceAnalysisderived ORDER BY Symbol')
        samples = cursor.fetchall()
        print(f'   Sample records:')
        for sample in samples:
            print(f'     {sample[0]} {sample[1]:.0f} {sample[2]} ₹{sample[3]:.2f}')
            
    except Exception as e:
        print(f'   ❌ Error: {e}')

    # Check F&O trading data
    print('\n📈 F&O TRADING DATA ANALYSIS:')
    try:
        cursor.execute('SELECT COUNT(*) FROM step04_fo_udiff_daily')
        fo_count = cursor.fetchone()[0]
        print(f'   Total F&O records: {fo_count:,}')
        
        cursor.execute('SELECT COUNT(DISTINCT symbol) FROM step04_fo_udiff_daily')
        fo_symbols = cursor.fetchone()[0]
        print(f'   Unique symbols: {fo_symbols:,}')
        
        cursor.execute('SELECT MIN(trade_date), MAX(trade_date) FROM step04_fo_udiff_daily')
        fo_dates = cursor.fetchone()
        print(f'   Date range: {fo_dates[0]} to {fo_dates[1]}')
        
        # February 2025 specific analysis
        cursor.execute("""
            SELECT COUNT(*) as total_records,
                   COUNT(DISTINCT symbol) as unique_symbols,
                   COUNT(DISTINCT trade_date) as trading_days
            FROM step04_fo_udiff_daily 
            WHERE trade_date BETWEEN '20250201' AND '20250228'
        """)
        feb_stats = cursor.fetchone()
        print(f'   February 2025: {feb_stats[0]:,} records, {feb_stats[1]} symbols, {feb_stats[2]} trading days')
        
        # Check data overlap
        cursor.execute("""
            SELECT COUNT(DISTINCT s4.symbol) as matching_symbols
            FROM step04_fo_udiff_daily s4
            INNER JOIN Step05_strikepriceAnalysisderived s5 
                ON s4.symbol = s5.Symbol 
            WHERE s4.trade_date BETWEEN '20250201' AND '20250228'
        """)
        overlap = cursor.fetchone()[0]
        print(f'   Symbol overlap: {overlap} symbols match between tables')
        
    except Exception as e:
        print(f'   ❌ Error: {e}')

    # Trading days in February 2025
    print('\n📅 FEBRUARY 2025 TRADING DAYS:')
    try:
        cursor.execute("""
            SELECT DISTINCT trade_date
            FROM step04_fo_udiff_daily 
            WHERE trade_date BETWEEN '20250201' AND '20250228'
            ORDER BY trade_date
        """)
        trading_days = cursor.fetchall()
        print(f'   Trading days count: {len(trading_days)}')
        print(f'   Trading days: {", ".join([day[0] for day in trading_days])}')
        
    except Exception as e:
        print(f'   ❌ Error: {e}')

    # Estimate processing volume
    print('\n🔢 PROCESSING VOLUME ESTIMATION:')
    if 'base_count' in locals() and 'feb_stats' in locals():
        estimated_comparisons = base_count * len(trading_days) if 'trading_days' in locals() else 0
        print(f'   Base strikes to analyze: {base_count:,}')
        print(f'   Trading days per strike: {len(trading_days) if "trading_days" in locals() else "Unknown"}')
        print(f'   Estimated total comparisons: {estimated_comparisons:,}')
        print(f'   Estimated processing time: {estimated_comparisons // 1000:.0f}-{estimated_comparisons // 500:.0f} minutes')

    conn.close()
    print('\n✅ Analysis completed!')

if __name__ == "__main__":
    analyze_data_scope()