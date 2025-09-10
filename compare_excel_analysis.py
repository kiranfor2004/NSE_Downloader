#!/usr/bin/env python3
"""
📊 NSE Excel Comparison & Analysis Tool
Compare multiple Excel files to find symbols with highest TTL_TRD_QNTY and DELIV_QTY
Focus on SERIES=EQ stocks across all dates
"""

import pandas as pd
import os
import glob
from datetime import datetime

def analyze_excel_files():
    """Analyze all Excel files and find top performing EQ symbols"""
    
    print("📊 NSE Excel Files Comparison Analysis")
    print("=" * 60)
    
    # Find all Excel files in directory
    excel_files = glob.glob("*.xlsx")
    if not excel_files:
        print("❌ No Excel files found in current directory")
        return
    
    print(f"📁 Found {len(excel_files)} Excel files:")
    for file in excel_files:
        print(f"   • {file}")
    print()
    
    all_data = []
    
    # Read and combine data from all Excel files
    for file in excel_files:
        try:
            print(f"📖 Reading: {file}")
            
            # Try to read the EQ_Data sheet first, then default sheet
            try:
                df = pd.read_excel(file, sheet_name='EQ_Data')
                print(f"   ✅ Read EQ_Data sheet: {len(df)} records")
            except:
                df = pd.read_excel(file)
                print(f"   ✅ Read default sheet: {len(df)} records")
                
            # Filter for EQ series only
            if 'SERIES' in df.columns:
                df_eq = df[df['SERIES'] == 'EQ'].copy()
            elif 'series' in df.columns:
                df_eq = df[df['series'] == 'EQ'].copy()
            else:
                # Assume all data is EQ if no series column
                df_eq = df.copy()
            
            # Standardize column names
            column_mapping = {
                'SYMBOL': 'symbol',
                'DATE': 'date', 
                'TTL_TRD_QNTY': 'volume',
                'DELIV_QTY': 'delivery_qty',
                'DELIV_PER': 'delivery_percentage',
                'total_traded_qty': 'volume',
                'volume': 'volume'  # Keep if already correct
            }
            
            df_eq = df_eq.rename(columns=column_mapping)
            df_eq['source_file'] = file
            
            all_data.append(df_eq)
            print(f"   ✅ EQ records added: {len(df_eq)}")
            
        except Exception as e:
            print(f"   ❌ Error reading {file}: {e}")
    
    if not all_data:
        print("❌ No valid data found in Excel files")
        return
    
    # Combine all data
    combined_df = pd.concat(all_data, ignore_index=True)
    print(f"\n📊 Combined Analysis:")
    print(f"   Total EQ records: {len(combined_df):,}")
    print(f"   Unique symbols: {combined_df['symbol'].nunique():,}")
    print(f"   Date range: {combined_df['date'].min()} to {combined_df['date'].max()}")
    
    # Analysis 1: Top symbols by trading volume (TTL_TRD_QNTY)
    print("\n🔥 TOP 20 SYMBOLS BY TRADING VOLUME")
    print("=" * 60)
    
    volume_analysis = combined_df.groupby(['symbol', 'date']).agg({
        'volume': 'max',
        'delivery_qty': 'max' if 'delivery_qty' in combined_df.columns else 'first',
        'delivery_percentage': 'max' if 'delivery_percentage' in combined_df.columns else 'first'
    }).reset_index()
    
    # Get top 20 by volume
    top_volume = volume_analysis.nlargest(20, 'volume')
    
    for idx, row in top_volume.iterrows():
        vol_cr = row['volume'] / 10000000  # Convert to crores
        deliv_cr = row.get('delivery_qty', 0) / 10000000 if pd.notna(row.get('delivery_qty', 0)) else 0
        deliv_pct = row.get('delivery_percentage', 0) if pd.notna(row.get('delivery_percentage', 0)) else 0
        
        print(f"{row['symbol']:12} | {row['date']} | Vol: {vol_cr:8.2f} Cr | Deliv: {deliv_cr:8.2f} Cr ({deliv_pct:5.1f}%)")
    
    # Analysis 2: Top symbols by delivery quantity (DELIV_QTY)
    if 'delivery_qty' in combined_df.columns:
        print("\n📦 TOP 20 SYMBOLS BY DELIVERY QUANTITY")
        print("=" * 60)
        
        top_delivery = volume_analysis.nlargest(20, 'delivery_qty')
        
        for idx, row in top_delivery.iterrows():
            vol_cr = row['volume'] / 10000000
            deliv_cr = row['delivery_qty'] / 10000000
            deliv_pct = row.get('delivery_percentage', 0) if pd.notna(row.get('delivery_percentage', 0)) else 0
            
            print(f"{row['symbol']:12} | {row['date']} | Vol: {vol_cr:8.2f} Cr | Deliv: {deliv_cr:8.2f} Cr ({deliv_pct:5.1f}%)")
    
    # Analysis 3: Symbols with consistent high volume across dates
    print("\n📈 SYMBOLS WITH HIGHEST AVERAGE VOLUME ACROSS ALL DATES")
    print("=" * 60)
    
    avg_volume = combined_df.groupby('symbol').agg({
        'volume': ['mean', 'max', 'count'],
        'delivery_qty': 'mean' if 'delivery_qty' in combined_df.columns else 'first'
    }).round(0)
    
    # Flatten column names
    avg_volume.columns = ['avg_volume', 'max_volume', 'trading_days', 'avg_delivery']
    avg_volume = avg_volume.sort_values('avg_volume', ascending=False).head(15)
    
    for symbol, row in avg_volume.iterrows():
        avg_vol_cr = row['avg_volume'] / 10000000
        max_vol_cr = row['max_volume'] / 10000000
        days = int(row['trading_days'])
        avg_deliv_cr = row.get('avg_delivery', 0) / 10000000 if pd.notna(row.get('avg_delivery', 0)) else 0
        
        print(f"{symbol:12} | Avg: {avg_vol_cr:6.1f} Cr | Max: {max_vol_cr:6.1f} Cr | Days: {days:2d} | Avg Deliv: {avg_deliv_cr:6.1f} Cr")
    
    # Analysis 4: Date-wise top performers
    print("\n📅 TOP PERFORMER BY DATE")
    print("=" * 60)
    
    daily_top = combined_df.loc[combined_df.groupby('date')['volume'].idxmax()]
    daily_top = daily_top.sort_values('date')
    
    for idx, row in daily_top.iterrows():
        vol_cr = row['volume'] / 10000000
        deliv_cr = row.get('delivery_qty', 0) / 10000000 if pd.notna(row.get('delivery_qty', 0)) else 0
        
        print(f"{row['date']} | {row['symbol']:12} | Volume: {vol_cr:8.2f} Cr | Delivery: {deliv_cr:6.2f} Cr")
    
    # Export detailed analysis
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"NSE_Comparison_Analysis_{timestamp}.xlsx"
    
    print(f"\n💾 Exporting detailed analysis to: {output_file}")
    
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        # Top volume performers
        top_volume.to_excel(writer, sheet_name='Top_Volume', index=False)
        
        # Top delivery performers
        if 'delivery_qty' in combined_df.columns:
            top_delivery.to_excel(writer, sheet_name='Top_Delivery', index=False)
        
        # Average volume analysis
        avg_volume.to_excel(writer, sheet_name='Avg_Volume', index=True)
        
        # Daily top performers
        daily_top[['date', 'symbol', 'volume', 'delivery_qty', 'delivery_percentage']].to_excel(
            writer, sheet_name='Daily_Top', index=False)
        
        # Combined raw data (sample)
        combined_df.head(1000).to_excel(writer, sheet_name='Sample_Data', index=False)
    
    print("🎉 Analysis Complete!")
    print("=" * 60)
    print(f"✅ Processed {len(excel_files)} Excel files")
    print(f"📊 Analyzed {len(combined_df):,} EQ series records")
    print(f"🏢 Covered {combined_df['symbol'].nunique():,} unique symbols")
    print(f"📈 Exported detailed analysis to: {output_file}")

if __name__ == "__main__":
    analyze_excel_files()
