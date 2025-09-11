#!/usr/bin/env python3
"""
🔍 March 2025 vs April 2025 NSE Data Comparison - INCREASED VALUES ONLY
Compare each symbol and show only those with increases
"""

import pandas as pd
import glob
import os
from datetime import datetime

def march_april_increased_only():
    """Compare March 2025 analysis symbols with April 2025 daily data - Show only increases"""
    
    print("🔍 MARCH vs APRIL 2025 - INCREASED VALUES ONLY")
    print("=" * 60)
    
    # Load March 2025 analysis results
    march_file = "NSE_MARCH2025_data_Unique_Symbol_Analysis_20250911_113959.xlsx"
    
    if not os.path.exists(march_file):
        print(f"❌ March analysis file not found: {march_file}")
        return
    
    print(f"📖 Loading March 2025 analysis: {march_file}")
    
    # Read March analysis sheets
    try:
        march_volume_df = pd.read_excel(march_file, sheet_name='Unique_TTL_TRD_QNTY')
        march_delivery_df = pd.read_excel(march_file, sheet_name='Unique_DELIV_QTY')
        print(f"   ✅ March Volume Analysis: {len(march_volume_df)} symbols")
        print(f"   ✅ March Delivery Analysis: {len(march_delivery_df)} symbols")
    except Exception as e:
        print(f"❌ Error reading March analysis: {str(e)}")
        return
    
    # Load April 2025 CSV files
    april_csv_pattern = "NSE_April_2025_Data/cm*.csv"
    april_csv_files = glob.glob(april_csv_pattern)
    
    if not april_csv_files:
        print(f"❌ No April 2025 CSV files found!")
        return
    
    print(f"📁 Found {len(april_csv_files)} April 2025 CSV files")
    
    # Read and combine all April data
    print("🔄 Loading April 2025 daily data...")
    april_all_data = []
    
    for file in sorted(april_csv_files):
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
                    april_all_data.append(df)
                    
        except Exception as e:
            continue
    
    if not april_all_data:
        print("❌ No valid April data found!")
        return
    
    april_combined = pd.concat(april_all_data, ignore_index=True)
    print(f"   ✅ Combined April data: {len(april_combined):,} records")
    
    # Process Volume Comparison - Only increases
    print("\n📈 Finding Volume Increases...")
    volume_increases = []
    
    for idx, march_row in march_volume_df.iterrows():
        symbol = march_row['SYMBOL']
        march_volume = march_row['TTL_TRD_QNTY']
        march_date = march_row['DATE']
        
        april_symbol_data = april_combined[april_combined['SYMBOL'] == symbol].copy()
        april_symbol_data = april_symbol_data.dropna(subset=['TTL_TRD_QNTY'])
        
        if len(april_symbol_data) > 0:
            april_max_volume = april_symbol_data['TTL_TRD_QNTY'].max()
            april_max_idx = april_symbol_data['TTL_TRD_QNTY'].idxmax()
            april_date = april_symbol_data.loc[april_max_idx, 'DATE']
            
            if april_max_volume > march_volume and march_volume > 0:
                increase = april_max_volume - march_volume
                pct_increase = ((april_max_volume - march_volume) / march_volume) * 100
                
                volume_increases.append({
                    'SYMBOL': symbol,
                    'MARCH_VOLUME': march_volume,
                    'MARCH_DATE': march_date,
                    'APRIL_VOLUME': april_max_volume,
                    'APRIL_DATE': april_date,
                    'VOLUME_INCREASE': increase,
                    'INCREASE_PERCENTAGE': f'{pct_increase:.1f}%',
                    'TIMES_HIGHER': f'{april_max_volume/march_volume:.1f}x',
                    'COMPARISON': f'March: {march_volume:,} → April: {april_max_volume:,} ({pct_increase:.1f}% ↗)'
                })
    
    # Process Delivery Comparison - Only increases
    print("📦 Finding Delivery Increases...")
    delivery_increases = []
    
    for idx, march_row in march_delivery_df.iterrows():
        symbol = march_row['SYMBOL']
        march_delivery = march_row['DELIV_QTY']
        march_date = march_row['DATE']
        
        april_symbol_data = april_combined[april_combined['SYMBOL'] == symbol].copy()
        april_symbol_data = april_symbol_data.dropna(subset=['DELIV_QTY'])
        
        if len(april_symbol_data) > 0:
            april_max_delivery = april_symbol_data['DELIV_QTY'].max()
            april_max_idx = april_symbol_data['DELIV_QTY'].idxmax()
            april_date = april_symbol_data.loc[april_max_idx, 'DATE']
            
            if april_max_delivery > march_delivery and march_delivery > 0:
                increase = april_max_delivery - march_delivery
                pct_increase = ((april_max_delivery - march_delivery) / march_delivery) * 100
                
                delivery_increases.append({
                    'SYMBOL': symbol,
                    'MARCH_DELIVERY': march_delivery,
                    'MARCH_DATE': march_date,
                    'APRIL_DELIVERY': april_max_delivery,
                    'APRIL_DATE': april_date,
                    'DELIVERY_INCREASE': increase,
                    'INCREASE_PERCENTAGE': f'{pct_increase:.1f}%',
                    'TIMES_HIGHER': f'{april_max_delivery/march_delivery:.1f}x',
                    'COMPARISON': f'March: {march_delivery:,} → April: {april_max_delivery:,} ({pct_increase:.1f}% ↗)'
                })
    
    print(f"📊 Results: {len(volume_increases)} volume increases, {len(delivery_increases)} delivery increases")
    
    # Create Excel file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"March_April_Increases_Only_{timestamp}.xlsx"
    
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
                    'Details': ['All April values were ≤ March values']
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
    march_april_increased_only()
