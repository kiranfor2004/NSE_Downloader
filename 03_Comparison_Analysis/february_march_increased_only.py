#!/usr/bin/env python3
"""
🔍 February 2025 vs March 2025 NSE Data Comparison - INCREASED VALUES ONLY
Compare each symbol and show only those with increases
"""

import pandas as pd
import glob
import os
from datetime import datetime

def february_march_increased_only():
    """Compare February 2025 analysis symbols with March 2025 daily data - Show only increases"""
    
    print("🔍 FEBRUARY vs MARCH 2025 - INCREASED VALUES ONLY")
    print("=" * 60)
    
    # Load February 2025 analysis results
    february_file = "NSE_FEBRUARY2025_data_Unique_Symbol_Analysis_20250911_113430.xlsx"
    
    if not os.path.exists(february_file):
        print(f"❌ February analysis file not found: {february_file}")
        return
    
    print(f"📖 Loading February 2025 analysis: {february_file}")
    
    # Read February analysis sheets
    try:
        feb_volume_df = pd.read_excel(february_file, sheet_name='Unique_TTL_TRD_QNTY')
        feb_delivery_df = pd.read_excel(february_file, sheet_name='Unique_DELIV_QTY')
        print(f"   ✅ February Volume Analysis: {len(feb_volume_df)} symbols")
        print(f"   ✅ February Delivery Analysis: {len(feb_delivery_df)} symbols")
    except Exception as e:
        print(f"❌ Error reading February analysis: {str(e)}")
        return
    
    # Load March 2025 CSV files
    march_csv_pattern = "NSE_March_2025_Data/cm*.csv"
    march_csv_files = glob.glob(march_csv_pattern)
    
    if not march_csv_files:
        print(f"❌ No March 2025 CSV files found!")
        return
    
    print(f"📁 Found {len(march_csv_files)} March 2025 CSV files")
    
    # Read and combine all March data
    print("🔄 Loading March 2025 daily data...")
    march_all_data = []
    
    for file in sorted(march_csv_files):
        try:
            df = pd.read_csv(file)
            df.columns = df.columns.str.strip()
            df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
            
            if 'SERIES' in df.columns:
                df = df[df['SERIES'] == 'EQ'].copy()
                if len(df) > 0:
                    # Handle DATE1 vs DATE
                    if 'DATE1' in df.columns and 'DATE' not in df.columns:
                        df['DATE'] = df['DATE1']
                    df['TTL_TRD_QNTY'] = pd.to_numeric(df['TTL_TRD_QNTY'], errors='coerce')
                    df['DELIV_QTY'] = pd.to_numeric(df['DELIV_QTY'], errors='coerce')
                    march_all_data.append(df)
                    
        except Exception as e:
            continue
    
    if not march_all_data:
        print("❌ No valid March data found!")
        return
    
    march_combined = pd.concat(march_all_data, ignore_index=True)
    print(f"   ✅ Combined March data: {len(march_combined):,} records")
    
    # Process Volume Comparison - Only increases
    print("\n📈 Finding Volume Increases...")
    volume_increases = []
    
    for idx, feb_row in feb_volume_df.iterrows():
        symbol = feb_row['SYMBOL']
        feb_volume = feb_row['TTL_TRD_QNTY']
        feb_date = feb_row['DATE']
        
        march_symbol_data = march_combined[march_combined['SYMBOL'] == symbol].copy()
        march_symbol_data = march_symbol_data.dropna(subset=['TTL_TRD_QNTY'])
        
        if len(march_symbol_data) > 0:
            march_max_volume = march_symbol_data['TTL_TRD_QNTY'].max()
            march_max_idx = march_symbol_data['TTL_TRD_QNTY'].idxmax()
            march_date = march_symbol_data.loc[march_max_idx, 'DATE']
            
            if march_max_volume > feb_volume and feb_volume > 0:
                increase = march_max_volume - feb_volume
                pct_increase = ((march_max_volume - feb_volume) / feb_volume) * 100
                
                volume_increases.append({
                    'SYMBOL': symbol,
                    'FEBRUARY_VOLUME': feb_volume,
                    'FEBRUARY_DATE': feb_date,
                    'MARCH_VOLUME': march_max_volume,
                    'MARCH_DATE': march_date,
                    'VOLUME_INCREASE': increase,
                    'INCREASE_PERCENTAGE': f'{pct_increase:.1f}%',
                    'TIMES_HIGHER': f'{march_max_volume/feb_volume:.1f}x',
                    'COMPARISON': f'February: {feb_volume:,} → March: {march_max_volume:,} ({pct_increase:.1f}% ↗)'
                })
    
    # Process Delivery Comparison - Only increases
    print("📦 Finding Delivery Increases...")
    delivery_increases = []
    
    for idx, feb_row in feb_delivery_df.iterrows():
        symbol = feb_row['SYMBOL']
        feb_delivery = feb_row['DELIV_QTY']
        feb_date = feb_row['DATE']
        
        march_symbol_data = march_combined[march_combined['SYMBOL'] == symbol].copy()
        march_symbol_data = march_symbol_data.dropna(subset=['DELIV_QTY'])
        
        if len(march_symbol_data) > 0:
            march_max_delivery = march_symbol_data['DELIV_QTY'].max()
            march_max_idx = march_symbol_data['DELIV_QTY'].idxmax()
            march_date = march_symbol_data.loc[march_max_idx, 'DATE']
            
            if march_max_delivery > feb_delivery and feb_delivery > 0:
                increase = march_max_delivery - feb_delivery
                pct_increase = ((march_max_delivery - feb_delivery) / feb_delivery) * 100
                
                delivery_increases.append({
                    'SYMBOL': symbol,
                    'FEBRUARY_DELIVERY': feb_delivery,
                    'FEBRUARY_DATE': feb_date,
                    'MARCH_DELIVERY': march_max_delivery,
                    'MARCH_DATE': march_date,
                    'DELIVERY_INCREASE': increase,
                    'INCREASE_PERCENTAGE': f'{pct_increase:.1f}%',
                    'TIMES_HIGHER': f'{march_max_delivery/feb_delivery:.1f}x',
                    'COMPARISON': f'February: {feb_delivery:,} → March: {march_max_delivery:,} ({pct_increase:.1f}% ↗)'
                })
    
    print(f"📊 Results: {len(volume_increases)} volume increases, {len(delivery_increases)} delivery increases")
    
    # Create Excel file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"February_March_Increases_Only_{timestamp}.xlsx"
    
    print(f"\n💾 Creating Excel: {output_filename}")
    
    try:
        with pd.ExcelWriter(output_filename, engine='openpyxl') as writer:
            
            # Volume increases sheet
            if volume_increases:
                volume_df = pd.DataFrame(volume_increases)
                volume_df = volume_df.sort_values('VOLUME_INCREASE', ascending=False)
                volume_df.to_excel(writer, sheet_name='Volume_Increases', index=False)
                print(f"   ✅ Volume Increases: {len(volume_df)} symbols")
            
            # Delivery increases sheet
            if delivery_increases:
                delivery_df = pd.DataFrame(delivery_increases)
                delivery_df = delivery_df.sort_values('DELIVERY_INCREASE', ascending=False)
                delivery_df.to_excel(writer, sheet_name='Delivery_Increases', index=False)
                print(f"   ✅ Delivery Increases: {len(delivery_df)} symbols")
            
            # If no increases, create summary
            if not volume_increases and not delivery_increases:
                summary_df = pd.DataFrame({
                    'Result': ['No increases found'],
                    'Details': ['All March values were ≤ February values']
                })
                summary_df.to_excel(writer, sheet_name='Summary', index=False)
                print("   ✅ Summary sheet (no increases)")
        
        print("\n🎉 ANALYSIS COMPLETE!")
        print("=" * 50)
        print(f"✅ File: {output_filename}")
        
        if volume_increases:
            top_vol = max(volume_increases, key=lambda x: x['VOLUME_INCREASE'])
            print(f"🥇 Top Volume Gainer: {top_vol['SYMBOL']} (+{top_vol['VOLUME_INCREASE']:,.0f})")
        
        if delivery_increases:
            top_del = max(delivery_increases, key=lambda x: x['DELIVERY_INCREASE'])
            print(f"🥇 Top Delivery Gainer: {top_del['SYMBOL']} (+{top_del['DELIVERY_INCREASE']:,.0f})")
    
    except Exception as e:
        print(f"❌ Error creating Excel: {str(e)}")

if __name__ == "__main__":
    february_march_increased_only()
