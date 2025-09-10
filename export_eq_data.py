#!/usr/bin/env python3
"""
NSE EQ Series Data Exporter
Exports only SERIES=EQ stocks to Excel for analysis
"""

import sqlite3
import pandas as pd
from datetime import datetime
import os

def export_eq_data_to_excel():
    """Export only EQ series stocks to Excel file"""
    
    print('📊 NSE EQ SERIES DATA EXPORTER')
    print('=' * 40)
    
    # Connect to database
    try:
        conn = sqlite3.connect('nse_data.db')
        print('✅ Connected to database')
    except Exception as e:
        print(f'❌ Database connection failed: {e}')
        return
    
    # Check available data
    eq_count = conn.execute("SELECT COUNT(*) FROM stock_data WHERE series = 'EQ'").fetchone()[0]
    total_count = conn.execute("SELECT COUNT(*) FROM stock_data").fetchone()[0]
    
    print(f'📈 EQ Series Records: {eq_count:,}')
    print(f'📊 Total Records: {total_count:,}')
    print(f'📋 EQ Percentage: {eq_count/total_count*100:.1f}%')
    print()
    
    if eq_count == 0:
        print('❌ No EQ series data found!')
        return
    
    # Menu for export options
    print('📄 Export Options:')
    print('1. Latest day EQ data only')
    print('2. All EQ data (all 19 days)')
    print('3. Top EQ stocks by volume (latest day)')
    print('4. Top EQ stocks by price (latest day)')
    print('5. High delivery EQ stocks (latest day)')
    
    choice = input('\n🎯 Choose export option (1-5): ').strip()
    
    if choice == '1':
        export_latest_eq_data(conn)
    elif choice == '2':
        export_all_eq_data(conn)
    elif choice == '3':
        export_top_volume_eq(conn)
    elif choice == '4':
        export_top_price_eq(conn)
    elif choice == '5':
        export_high_delivery_eq(conn)
    else:
        print('❌ Invalid choice!')
    
    conn.close()

def export_latest_eq_data(conn):
    """Export latest day EQ data to Excel"""
    
    # Get latest date
    latest_date = conn.execute("SELECT MAX(date) FROM stock_data").fetchone()[0]
    
    print(f'📅 Exporting EQ data for: {latest_date}')
    
    # Query for EQ data
    query = """
    SELECT 
        symbol,
        series,
        date,
        prev_close,
        open_price,
        high_price,
        low_price,
        close_price,
        avg_price,
        total_traded_qty as volume,
        turnover_lacs,
        no_of_trades,
        delivery_qty,
        delivery_percentage,
        ROUND(((close_price - prev_close) / prev_close * 100), 2) as change_percent
    FROM stock_data 
    WHERE series = 'EQ' AND date = ?
    ORDER BY turnover_lacs DESC
    """
    
    df = pd.read_sql_query(query, conn, params=[latest_date])
    
    # Create filename
    filename = f'NSE_EQ_Data_{latest_date.replace("-", "")}.xlsx'
    
    # Export to Excel
    try:
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            # Main data sheet
            df.to_excel(writer, sheet_name='EQ_Stocks', index=False)
            
            # Summary sheet
            summary_data = {
                'Metric': [
                    'Total EQ Stocks',
                    'Average Price (₹)',
                    'Total Volume',
                    'Total Turnover (Lacs)',
                    'Average Delivery %',
                    'Gainers',
                    'Losers',
                    'Unchanged'
                ],
                'Value': [
                    len(df),
                    f'₹{df["close_price"].mean():.2f}',
                    f'{df["volume"].sum():,}',
                    f'₹{df["turnover_lacs"].sum():,.1f}',
                    f'{df["delivery_percentage"].mean():.2f}%',
                    len(df[df["change_percent"] > 0]),
                    len(df[df["change_percent"] < 0]),
                    len(df[df["change_percent"] == 0])
                ]
            }
            
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
        
        print(f'✅ Excel file created: {filename}')
        print(f'📊 Records exported: {len(df):,}')
        print(f'📁 File location: {os.path.abspath(filename)}')
        
    except Exception as e:
        print(f'❌ Export failed: {e}')

def export_all_eq_data(conn):
    """Export all EQ data (19 days) to Excel"""
    
    print('📅 Exporting ALL EQ data (19 trading days)')
    print('⏳ This may take a moment...')
    
    query = """
    SELECT 
        symbol,
        date,
        prev_close,
        open_price,
        high_price,
        low_price,
        close_price,
        total_traded_qty as volume,
        turnover_lacs,
        delivery_percentage
    FROM stock_data 
    WHERE series = 'EQ'
    ORDER BY date DESC, turnover_lacs DESC
    """
    
    df = pd.read_sql_query(query, conn)
    
    filename = 'NSE_EQ_All_Data_Aug2025.xlsx'
    
    try:
        # Split by date for easier analysis
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            # All data in one sheet
            df.to_excel(writer, sheet_name='All_EQ_Data', index=False)
            
            # Summary by date
            daily_summary = df.groupby('date').agg({
                'symbol': 'count',
                'close_price': 'mean',
                'volume': 'sum',
                'turnover_lacs': 'sum',
                'delivery_percentage': 'mean'
            }).round(2)
            
            daily_summary.columns = ['Stocks_Count', 'Avg_Price', 'Total_Volume', 'Total_Turnover_Lacs', 'Avg_Delivery_Pct']
            daily_summary.to_excel(writer, sheet_name='Daily_Summary')
        
        print(f'✅ Excel file created: {filename}')
        print(f'📊 Records exported: {len(df):,}')
        print(f'📁 File location: {os.path.abspath(filename)}')
        
    except Exception as e:
        print(f'❌ Export failed: {e}')

def export_top_volume_eq(conn):
    """Export top volume EQ stocks"""
    
    latest_date = conn.execute("SELECT MAX(date) FROM stock_data").fetchone()[0]
    
    query = """
    SELECT 
        symbol,
        close_price,
        total_traded_qty as volume,
        turnover_lacs,
        delivery_percentage,
        ROUND(((close_price - prev_close) / prev_close * 100), 2) as change_percent
    FROM stock_data 
    WHERE series = 'EQ' AND date = ?
    ORDER BY total_traded_qty DESC
    LIMIT 100
    """
    
    df = pd.read_sql_query(query, conn, params=[latest_date])
    filename = f'NSE_EQ_Top_Volume_{latest_date.replace("-", "")}.xlsx'
    
    df.to_excel(filename, index=False)
    print(f'✅ Top 100 volume EQ stocks exported: {filename}')

def export_top_price_eq(conn):
    """Export top priced EQ stocks"""
    
    latest_date = conn.execute("SELECT MAX(date) FROM stock_data").fetchone()[0]
    
    query = """
    SELECT 
        symbol,
        close_price,
        total_traded_qty as volume,
        turnover_lacs,
        delivery_percentage
    FROM stock_data 
    WHERE series = 'EQ' AND date = ?
    ORDER BY close_price DESC
    LIMIT 100
    """
    
    df = pd.read_sql_query(query, conn, params=[latest_date])
    filename = f'NSE_EQ_Top_Priced_{latest_date.replace("-", "")}.xlsx'
    
    df.to_excel(filename, index=False)
    print(f'✅ Top 100 priced EQ stocks exported: {filename}')

def export_high_delivery_eq(conn):
    """Export high delivery EQ stocks"""
    
    latest_date = conn.execute("SELECT MAX(date) FROM stock_data").fetchone()[0]
    
    query = """
    SELECT 
        symbol,
        close_price,
        total_traded_qty as volume,
        delivery_qty,
        delivery_percentage,
        turnover_lacs
    FROM stock_data 
    WHERE series = 'EQ' AND date = ? AND delivery_percentage >= 80
    ORDER BY delivery_percentage DESC
    """
    
    df = pd.read_sql_query(query, conn, params=[latest_date])
    filename = f'NSE_EQ_High_Delivery_{latest_date.replace("-", "")}.xlsx'
    
    df.to_excel(filename, index=False)
    print(f'✅ High delivery EQ stocks exported: {filename}')
    print(f'📦 {len(df)} stocks with ≥80% delivery')

if __name__ == "__main__":
    export_eq_data_to_excel()
