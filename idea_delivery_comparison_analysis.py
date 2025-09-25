#!/usr/bin/env python3
"""
IDEA Delivery Quantity Comparison Analysis
==========================================
Compare IDEA's highest delivery quantity from January 2025 with February 2025 daily data
to identify any February days that exceeded January's peak delivery.

Tables used:
- step02_monthly_analysis: For January 2025 highest delivery qty
- step01_equity_daily: For February 2025 daily delivery data
"""

import pyodbc
import pandas as pd
from datetime import datetime

def get_january_peak_delivery():
    """Get IDEA's highest delivery quantity from January 2025"""
    conn = pyodbc.connect('Driver={SQL Server};Server=SRIKIRANREDDY\\SQLEXPRESS;Database=master;Trusted_Connection=yes;')
    
    query = """
    SELECT 
        symbol,
        analysis_month,
        deliv_qty,
        close_price,
        peak_date
    FROM step02_monthly_analysis 
    WHERE symbol = 'IDEA' 
    AND analysis_month = '2025-01'
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    if len(df) > 0:
        jan_data = df.iloc[0]
        return {
            'symbol': jan_data['symbol'],
            'month': jan_data['analysis_month'],
            'delivery_qty': jan_data['deliv_qty'],
            'close_price': jan_data['close_price'],
            'peak_date': jan_data['peak_date']
        }
    else:
        return None

def get_february_daily_deliveries(jan_peak_qty):
    """Get February 2025 daily delivery data for IDEA and compare with January peak"""
    conn = pyodbc.connect('Driver={SQL Server};Server=SRIKIRANREDDY\\SQLEXPRESS;Database=master;Trusted_Connection=yes;')
    
    query = """
    SELECT 
        trade_date,
        symbol,
        deliv_qty,
        close_price,
        deliv_per,
        ttl_trd_qnty
    FROM step01_equity_daily 
    WHERE symbol = 'IDEA'
    AND trade_date >= '2025-02-01'
    AND trade_date <= '2025-02-28'
    AND deliv_qty IS NOT NULL
    ORDER BY deliv_qty DESC
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    if len(df) > 0:
        # Find days where deliv_qty > jan_peak_qty
        exceeded_days = df[df['deliv_qty'] > jan_peak_qty].copy()
        
        # The trade_date is already in YYYY-MM-DD format, so we can use it directly
        df['formatted_date'] = df['trade_date']
        exceeded_days['formatted_date'] = exceeded_days['trade_date']
        
        return df, exceeded_days
    else:
        return pd.DataFrame(), pd.DataFrame()

def main():
    """Main analysis function"""
    print("🔍 IDEA DELIVERY QUANTITY COMPARISON ANALYSIS")
    print("=" * 60)
    print("Comparing January 2025 peak vs February 2025 daily deliveries\n")
    
    # Step 1: Get January peak delivery
    print("1️⃣ Getting January 2025 peak delivery data...")
    jan_data = get_january_peak_delivery()
    
    if not jan_data:
        print("❌ No January 2025 data found for IDEA in step02_monthly_analysis")
        return
    
    print(f"✅ January 2025 Peak Delivery Data:")
    print(f"   Symbol: {jan_data['symbol']}")
    print(f"   Peak Delivery Qty: {jan_data['delivery_qty']:,}")
    print(f"   Closing Price: ₹{jan_data['close_price']:.2f}")
    print(f"   Peak Date: {jan_data['peak_date']}")
    print(f"   Month: {jan_data['month']}")
    
    # Step 2: Get February daily deliveries
    print(f"\n2️⃣ Getting February 2025 daily delivery data...")
    feb_df, exceeded_days = get_february_daily_deliveries(jan_data['delivery_qty'])
    
    if feb_df.empty:
        print("❌ No February 2025 data found for IDEA in step01_equity_daily")
        return
    
    print(f"✅ Found {len(feb_df)} February trading days with delivery data")
    
    # Step 3: Analysis Results
    print(f"\n3️⃣ COMPARISON RESULTS:")
    print("=" * 40)
    
    if len(exceeded_days) > 0:
        print(f"🎯 FOUND {len(exceeded_days)} FEBRUARY DAYS EXCEEDING JANUARY PEAK!")
        print(f"   January Peak: {jan_data['delivery_qty']:,}")
        print()
        
        print("📈 February days with higher delivery:")
        print("-" * 50)
        for idx, row in exceeded_days.iterrows():
            excess = row['deliv_qty'] - jan_data['delivery_qty']
            excess_pct = (excess / jan_data['delivery_qty']) * 100
            
            print(f"📅 Date: {row['formatted_date']} ({row['trade_date']})")
            print(f"   💰 Closing Price: ₹{row['close_price']:.2f}")
            print(f"   📦 Delivery Qty: {row['deliv_qty']:,}")
            print(f"   📊 Excess over Jan: {excess:,} ({excess_pct:.1f}% higher)")
            print(f"   🔄 Delivery %: {row['deliv_per']:.2f}%")
            print()
    else:
        print("📊 NO FEBRUARY DAYS EXCEEDED JANUARY PEAK")
        print(f"   January Peak: {jan_data['delivery_qty']:,}")
        
        # Show top 5 February days
        top_feb = feb_df.head(5)
        print(f"\n📈 Top 5 February delivery days (vs Jan peak):")
        print("-" * 50)
        for idx, row in top_feb.iterrows():
            gap = jan_data['delivery_qty'] - row['deliv_qty']
            gap_pct = (gap / jan_data['delivery_qty']) * 100
            
            print(f"📅 {row['formatted_date']}: {row['deliv_qty']:,} ")
            print(f"   💰 Price: ₹{row['close_price']:.2f} | Gap: -{gap:,} (-{gap_pct:.1f}%)")
    
    # Step 4: Summary Statistics
    print(f"\n4️⃣ SUMMARY STATISTICS:")
    print("=" * 30)
    print(f"January 2025 Peak Delivery: {jan_data['delivery_qty']:,}")
    print(f"February 2025 Max Delivery: {feb_df['deliv_qty'].max():,}")
    print(f"February 2025 Avg Delivery: {feb_df['deliv_qty'].mean():,.0f}")
    print(f"February Trading Days: {len(feb_df)}")
    print(f"Days Exceeding Jan Peak: {len(exceeded_days)}")
    
    if len(exceeded_days) > 0:
        print(f"\n🎯 CONCLUSION: February had delivery spikes exceeding January!")
        print(f"   Best February Day: {exceeded_days.iloc[0]['formatted_date']}")
        print(f"   Best February Price: ₹{exceeded_days.iloc[0]['close_price']:.2f}")
        print(f"   Best February Delivery: {exceeded_days.iloc[0]['deliv_qty']:,}")
    else:
        print(f"\n📊 CONCLUSION: January peak remains the highest delivery quantity.")

if __name__ == "__main__":
    main()