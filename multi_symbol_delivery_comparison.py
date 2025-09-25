#!/usr/bin/env python3
"""
Multi-Symbol Delivery Quantity Comparison Analysis
=================================================
Test multiple symbols to find examples where January 2025 delivery quantity
is greater than February 2025 delivery quantity.

Tables used:
- step02_monthly_analysis: For January 2025 delivery data
- step01_equity_daily: For February 2025 daily delivery data
"""

import pyodbc
import pandas as pd
from datetime import datetime

def get_january_symbols_with_delivery():
    """Get all symbols with delivery data from January 2025"""
    conn = pyodbc.connect('Driver={SQL Server};Server=SRIKIRANREDDY\\SQLEXPRESS;Database=master;Trusted_Connection=yes;')
    
    query = """
    SELECT 
        symbol,
        deliv_qty as jan_delivery_qty,
        close_price as jan_close_price,
        peak_date as jan_peak_date
    FROM step02_monthly_analysis 
    WHERE analysis_month = '2025-01'
    AND deliv_qty IS NOT NULL
    ORDER BY deliv_qty DESC
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def get_february_max_delivery(symbol):
    """Get maximum February 2025 delivery for a specific symbol"""
    conn = pyodbc.connect('Driver={SQL Server};Server=SRIKIRANREDDY\\SQLEXPRESS;Database=master;Trusted_Connection=yes;')
    
    query = f"""
    SELECT 
        MAX(deliv_qty) as max_feb_delivery,
        trade_date,
        close_price
    FROM step01_equity_daily 
    WHERE symbol = '{symbol}'
    AND trade_date >= '2025-02-01'
    AND trade_date <= '2025-02-28'
    AND deliv_qty IS NOT NULL
    GROUP BY trade_date, close_price
    ORDER BY MAX(deliv_qty) DESC
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    if len(df) > 0:
        return df.iloc[0]['max_feb_delivery'], df.iloc[0]['trade_date'], df.iloc[0]['close_price']
    else:
        return None, None, None

def analyze_multiple_symbols(limit=10):
    """Analyze multiple symbols to find Jan > Feb examples"""
    print("🔍 MULTI-SYMBOL DELIVERY QUANTITY COMPARISON")
    print("=" * 60)
    print("Finding symbols where January 2025 > February 2025 delivery\n")
    
    # Get January data
    print("1️⃣ Getting January 2025 delivery data...")
    jan_df = get_january_symbols_with_delivery()
    print(f"✅ Found {len(jan_df)} symbols with January delivery data")
    
    # Test top symbols
    test_symbols = jan_df.head(limit)
    results = []
    
    print(f"\n2️⃣ Testing top {limit} symbols by January delivery...")
    print("-" * 60)
    
    for idx, row in test_symbols.iterrows():
        symbol = row['symbol']
        jan_delivery = row['jan_delivery_qty']
        jan_price = row['jan_close_price']
        jan_date = row['jan_peak_date']
        
        # Get February max for this symbol
        feb_delivery, feb_date, feb_price = get_february_max_delivery(symbol)
        
        if feb_delivery is not None:
            jan_higher = jan_delivery > feb_delivery
            difference = jan_delivery - feb_delivery if feb_delivery else jan_delivery
            difference_pct = (difference / jan_delivery * 100) if jan_delivery > 0 else 0
            
            result = {
                'symbol': symbol,
                'jan_delivery': jan_delivery,
                'jan_price': jan_price,
                'jan_date': jan_date,
                'feb_delivery': feb_delivery,
                'feb_price': feb_price,
                'feb_date': feb_date,
                'jan_higher': jan_higher,
                'difference': difference,
                'difference_pct': difference_pct
            }
            results.append(result)
            
            status = "✅ JAN > FEB" if jan_higher else "❌ FEB >= JAN"
            print(f"{symbol:10} | {status} | Jan: {jan_delivery:>12,} | Feb: {feb_delivery:>12,}")
        else:
            print(f"{symbol:10} | ⚠️  NO FEB DATA | Jan: {jan_delivery:>12,} | Feb: No data")
    
    return results

def display_detailed_results(results):
    """Display detailed results with examples"""
    jan_higher_examples = [r for r in results if r['jan_higher']]
    feb_higher_examples = [r for r in results if not r['jan_higher']]
    
    print(f"\n3️⃣ DETAILED ANALYSIS RESULTS")
    print("=" * 40)
    print(f"Total symbols tested: {len(results)}")
    print(f"Jan > Feb examples: {len(jan_higher_examples)}")
    print(f"Feb >= Jan examples: {len(feb_higher_examples)}")
    
    if jan_higher_examples:
        print(f"\n🎯 EXAMPLES WHERE JANUARY > FEBRUARY:")
        print("-" * 50)
        
        # Sort by difference percentage (highest first)
        jan_higher_examples.sort(key=lambda x: x['difference_pct'], reverse=True)
        
        for i, result in enumerate(jan_higher_examples[:5], 1):
            print(f"\n{i}. **{result['symbol']}** - January Dominance")
            print(f"   📅 January ({result['jan_date']}): {result['jan_delivery']:,} shares at ₹{result['jan_price']:.2f}")
            print(f"   📅 February ({result['feb_date']}): {result['feb_delivery']:,} shares at ₹{result['feb_price']:.2f}")
            print(f"   📊 Difference: +{result['difference']:,} shares ({result['difference_pct']:.1f}% higher)")
            print(f"   🎯 January delivery was {result['jan_delivery']/result['feb_delivery']:.1f}x higher than February")
    
    if feb_higher_examples:
        print(f"\n📉 EXAMPLES WHERE FEBRUARY >= JANUARY:")
        print("-" * 50)
        
        # Sort by how much February exceeded January
        feb_higher_examples.sort(key=lambda x: abs(x['difference']), reverse=True)
        
        for i, result in enumerate(feb_higher_examples[:3], 1):
            if result['difference'] < 0:  # February was actually higher
                feb_advantage = abs(result['difference'])
                feb_advantage_pct = (feb_advantage / result['feb_delivery'] * 100)
                print(f"\n{i}. **{result['symbol']}** - February Advantage")
                print(f"   📅 January ({result['jan_date']}): {result['jan_delivery']:,} shares at ₹{result['jan_price']:.2f}")
                print(f"   📅 February ({result['feb_date']}): {result['feb_delivery']:,} shares at ₹{result['feb_price']:.2f}")
                print(f"   📊 February Advantage: +{feb_advantage:,} shares ({feb_advantage_pct:.1f}% higher)")

def main():
    """Main analysis function"""
    # Test with top 15 symbols
    results = analyze_multiple_symbols(limit=15)
    
    if results:
        display_detailed_results(results)
        
        # Summary statistics
        jan_higher_count = sum(1 for r in results if r['jan_higher'])
        total_tested = len(results)
        
        print(f"\n4️⃣ FINAL SUMMARY")
        print("=" * 25)
        print(f"📊 Pattern Analysis:")
        print(f"   • {jan_higher_count}/{total_tested} symbols had January > February delivery")
        print(f"   • {total_tested - jan_higher_count}/{total_tested} symbols had February >= January delivery")
        print(f"   • Success Rate: {jan_higher_count/total_tested*100:.1f}% for January dominance")
        
        if jan_higher_count > 0:
            print(f"\n✅ CONFIRMED: Multiple examples found where January 2025 delivery > February 2025")
            print(f"🎯 This validates your hypothesis about January delivery patterns!")
        else:
            print(f"\n⚠️  No examples found where January > February in tested symbols")
    else:
        print("❌ No valid comparisons could be made")

if __name__ == "__main__":
    main()