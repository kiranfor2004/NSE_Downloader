#!/usr/bin/env python3
"""
🚀 FAST EQ Series Data Export to Excel
Export only SERIES='EQ' stocks from NSE database to Excel format
Optimized for speed with progress tracking
"""

import sqlite3
import pandas as pd
import os
from datetime import datetime

def export_eq_to_excel():
    """Export EQ series data to Excel with progress tracking"""
    
    print("🚀 NSE EQ Series Export Tool")
    print("=" * 50)
    
    # Database connection
    db_path = "nse_data.db"
    if not os.path.exists(db_path):
        print("❌ Database not found! Run setup_database.py first")
        return
    
    try:
        conn = sqlite3.connect(db_path)
        
        # First, get count of EQ records for progress
        print("📊 Checking EQ series data...")
        count_query = "SELECT COUNT(*) FROM stock_data WHERE series = 'EQ'"
        eq_count = conn.execute(count_query).fetchone()[0]
        print(f"✅ Found {eq_count:,} EQ series records")
        
        if eq_count == 0:
            print("❌ No EQ series data found!")
            return
        
        print("⚡ Exporting data (this will be fast)...")
        
        # Optimized query - select only EQ series with essential columns
        query = """
        SELECT 
            symbol,
            date,
            prev_close,
            open_price as open,
            high_price as high,
            low_price as low,
            last_price as last,
            close_price as close,
            avg_price as avg_price,
            total_traded_qty as volume,
            turnover_lacs,
            no_of_trades as trades,
            delivery_qty,
            delivery_percentage
        FROM stock_data 
        WHERE series = 'EQ'
        ORDER BY date DESC, symbol ASC
        """
        
        # Read data efficiently
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        print(f"✅ Loaded {len(df):,} EQ records")
        
        # Create filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"NSE_EQ_Data_{timestamp}.xlsx"
        
        print(f"💾 Saving to: {filename}")
        
        # Save to Excel with formatting
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            # Main data sheet
            df.to_excel(writer, sheet_name='EQ_Data', index=False)
            
            # Summary sheet
            summary_data = {
                'Metric': [
                    'Total EQ Records',
                    'Unique Stocks',
                    'Date Range',
                    'Avg Daily Volume (Cr)',
                    'Avg Daily Turnover (Cr)',
                    'Export Date'
                ],
                'Value': [
                    f"{len(df):,}",
                    f"{df['symbol'].nunique():,}",
                    f"{df['date'].min()} to {df['date'].max()}",
                    f"{df['volume'].mean()/10000000:.2f}",
                    f"{df['turnover_lacs'].mean()/100:.2f}",
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                ]
            }
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
            
            # Top 20 by volume
            top_volume = df.nlargest(20, 'volume')[['symbol', 'date', 'volume', 'turnover_lacs']]
            top_volume.to_excel(writer, sheet_name='Top_Volume', index=False)
        
        print("🎉 SUCCESS!")
        print("=" * 50)
        print(f"✅ File created: {filename}")
        print(f"📊 Records exported: {len(df):,}")
        print(f"🏢 Unique stocks: {df['symbol'].nunique():,}")
        print(f"📈 Date range: {df['date'].min()} to {df['date'].max()}")
        print("💡 File contains 3 sheets: EQ_Data, Summary, Top_Volume")
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    export_eq_to_excel()
