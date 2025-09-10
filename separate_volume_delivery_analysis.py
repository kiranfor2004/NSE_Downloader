#!/usr/bin/env python3
"""
📊 NSE Separate Analysis Tool
Create separate sheets for TTL_TRD_QNTY and DELIV_QTY analysis
Focus on SERIES=EQ stocks with detailed breakdowns
"""

import pandas as pd
import sqlite3
import os
from datetime import datetime

def create_separate_analysis():
    """Create Excel with separate sheets for volume and delivery analysis"""
    
    print("📊 NSE Separate Volume & Delivery Analysis")
    print("=" * 60)
    
    # Database connection
    db_path = "nse_data.db"
    if not os.path.exists(db_path):
        print("❌ Database not found! Run setup_database.py first")
        return
    
    try:
        conn = sqlite3.connect(db_path)
        
        print("📖 Loading EQ series data from database...")
        
        # Get all EQ series data with proper column names
        query = """
        SELECT 
            symbol,
            date,
            prev_close,
            open_price,
            high_price,
            low_price,
            last_price,
            close_price,
            avg_price,
            total_traded_qty,
            turnover_lacs,
            no_of_trades,
            delivery_qty,
            delivery_percentage
        FROM stock_data 
        WHERE series = 'EQ'
        ORDER BY date ASC, symbol ASC
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        print(f"✅ Loaded {len(df):,} EQ records")
        print(f"🏢 Unique symbols: {df['symbol'].nunique():,}")
        print(f"📅 Date range: {df['date'].min()} to {df['date'].max()}")
        
        # Create timestamp for filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"NSE_Volume_Delivery_Analysis_{timestamp}.xlsx"
        
        print(f"💾 Creating analysis file: {filename}")
        
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            
            # SHEET 1: TTL_TRD_QNTY (Volume) Analysis
            print("📈 Creating TTL_TRD_QNTY analysis sheet...")
            
            # Sort by volume (highest first) and create volume analysis
            volume_analysis = df.copy()
            volume_analysis = volume_analysis.sort_values('total_traded_qty', ascending=False)
            
            # Add volume in crores for better readability
            volume_analysis['volume_crores'] = volume_analysis['total_traded_qty'] / 10000000
            volume_analysis['turnover_crores'] = volume_analysis['turnover_lacs'] / 100
            
            # Select relevant columns for volume analysis
            volume_cols = [
                'symbol', 'date', 'total_traded_qty', 'volume_crores', 
                'turnover_lacs', 'turnover_crores', 'no_of_trades',
                'open_price', 'high_price', 'low_price', 'close_price'
            ]
            
            volume_sheet = volume_analysis[volume_cols].copy()
            volume_sheet.to_excel(writer, sheet_name='TTL_TRD_QNTY_Analysis', index=False)
            
            # SHEET 2: DELIV_QTY Analysis
            print("📦 Creating DELIV_QTY analysis sheet...")
            
            # Sort by delivery quantity (highest first)
            delivery_analysis = df.copy()
            delivery_analysis = delivery_analysis.sort_values('delivery_qty', ascending=False)
            
            # Add delivery in crores for better readability
            delivery_analysis['delivery_crores'] = delivery_analysis['delivery_qty'] / 10000000
            delivery_analysis['volume_crores'] = delivery_analysis['total_traded_qty'] / 10000000
            
            # Select relevant columns for delivery analysis
            delivery_cols = [
                'symbol', 'date', 'delivery_qty', 'delivery_crores', 
                'delivery_percentage', 'total_traded_qty', 'volume_crores',
                'open_price', 'high_price', 'low_price', 'close_price'
            ]
            
            delivery_sheet = delivery_analysis[delivery_cols].copy()
            delivery_sheet.to_excel(writer, sheet_name='DELIV_QTY_Analysis', index=False)
            
            # SHEET 3: Top Performers Summary
            print("🏆 Creating summary sheet...")
            
            # Top 50 by volume
            top_volume_summary = volume_analysis.head(50)[['symbol', 'date', 'volume_crores', 'turnover_crores']]
            top_volume_summary.columns = ['Symbol', 'Date', 'Volume_Cr', 'Turnover_Cr']
            
            # Top 50 by delivery
            top_delivery_summary = delivery_analysis.head(50)[['symbol', 'date', 'delivery_crores', 'delivery_percentage']]
            top_delivery_summary.columns = ['Symbol', 'Date', 'Delivery_Cr', 'Delivery_%']
            
            # Create summary sheet with both
            summary_data = {
                'Analysis_Type': ['TTL_TRD_QNTY (Volume)', 'DELIV_QTY (Delivery)', '', 'Top Volume Leader', 'Top Delivery Leader', '', 'Records Analyzed', 'Unique Symbols', 'Date Range'],
                'Details': [
                    f"Sorted by highest trading volume",
                    f"Sorted by highest delivery quantity",
                    '',
                    f"{volume_analysis.iloc[0]['symbol']} - {volume_analysis.iloc[0]['volume_crores']:.1f} Cr on {volume_analysis.iloc[0]['date']}",
                    f"{delivery_analysis.iloc[0]['symbol']} - {delivery_analysis.iloc[0]['delivery_crores']:.1f} Cr on {delivery_analysis.iloc[0]['date']}",
                    '',
                    f"{len(df):,}",
                    f"{df['symbol'].nunique():,}",
                    f"{df['date'].min()} to {df['date'].max()}"
                ]
            }
            
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
            
            # SHEET 4: Symbol-wise Statistics
            print("📊 Creating symbol-wise statistics...")
            
            # Calculate statistics for each symbol
            symbol_stats = df.groupby('symbol').agg({
                'total_traded_qty': ['sum', 'mean', 'max'],
                'delivery_qty': ['sum', 'mean', 'max'],
                'delivery_percentage': 'mean',
                'date': 'count'
            }).round(2)
            
            # Flatten column names
            symbol_stats.columns = ['Total_Volume', 'Avg_Volume', 'Max_Volume', 'Total_Delivery', 'Avg_Delivery', 'Max_Delivery', 'Avg_Delivery_%', 'Trading_Days']
            
            # Convert to crores
            for col in ['Total_Volume', 'Avg_Volume', 'Max_Volume', 'Total_Delivery', 'Avg_Delivery', 'Max_Delivery']:
                symbol_stats[col] = symbol_stats[col] / 10000000
            
            # Sort by total volume
            symbol_stats = symbol_stats.sort_values('Total_Volume', ascending=False)
            
            symbol_stats.to_excel(writer, sheet_name='Symbol_Statistics', index=True)
        
        print("🎉 SUCCESS!")
        print("=" * 60)
        print(f"✅ File created: {filename}")
        print()
        print("📋 Excel file contains 4 sheets:")
        print("   1. TTL_TRD_QNTY_Analysis - All records sorted by trading volume")
        print("   2. DELIV_QTY_Analysis    - All records sorted by delivery quantity")
        print("   3. Summary               - Key statistics and top performers")
        print("   4. Symbol_Statistics     - Symbol-wise aggregated data")
        print()
        print("📊 Data Summary:")
        print(f"   📈 Top Volume: {volume_analysis.iloc[0]['symbol']} - {volume_analysis.iloc[0]['volume_crores']:.1f} Cr")
        print(f"   📦 Top Delivery: {delivery_analysis.iloc[0]['symbol']} - {delivery_analysis.iloc[0]['delivery_crores']:.1f} Cr")
        print(f"   📅 Records: {len(df):,} EQ series entries")
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    create_separate_analysis()
