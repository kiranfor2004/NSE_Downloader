#!/usr/bin/env python3
"""
🔍 April 2025 vs May 2025 NSE Data Comparison - INCREASED VALUES ONLY
Compare each symbol and show only those with increases
"""

import pandas as pd
import glob
import os
from datetime import datetime

def april_may_increased_only():
    """Compare April 2025 analysis symbols with May 2025 daily data - Show only increases"""
    
    print("🔍 APRIL vs MAY 2025 - INCREASED VALUES ONLY")
    print("=" * 60)
    
    # Load April 2025 analysis results
    april_file = "NSE_APRIL2025_data_Unique_Symbol_Analysis_20250911_121027.xlsx"
    
    if not os.path.exists(april_file):
        print(f"❌ April analysis file not found: {april_file}")
        return
    
    print(f"📖 Loading April 2025 analysis: {april_file}")
    
    # Read April analysis sheets
    try:
        april_volume_df = pd.read_excel(april_file, sheet_name='Unique_TTL_TRD_QNTY')
        april_delivery_df = pd.read_excel(april_file, sheet_name='Unique_DELIV_QTY')
        print(f"   ✅ April Volume Analysis: {len(april_volume_df)} symbols")
        print(f"   ✅ April Delivery Analysis: {len(april_delivery_df)} symbols")
    except Exception as e:
        print(f"❌ Error reading April analysis: {str(e)}")
        return
    
    # Load May 2025 CSV files
    may_csv_pattern = "NSE_May_2025_Data/cm*.csv"
    may_csv_files = glob.glob(may_csv_pattern)
    
    if not may_csv_files:
        print(f"❌ No May 2025 CSV files found!")
        return
    
    print(f"📁 Found {len(may_csv_files)} May 2025 CSV files")
    
    # Read and combine all May data
    print("🔄 Loading May 2025 daily data...")
    may_all_data = []
    
    for file in sorted(may_csv_files):
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
                    may_all_data.append(df)
                    
        except Exception as e:
            continue
    
    if not may_all_data:
        print("❌ No valid May data found!")
        return
    
    may_combined = pd.concat(may_all_data, ignore_index=True)
    print(f"   ✅ Combined May data: {len(may_combined):,} records")
    
    # Process Volume Comparison - Only increases
    print("\n📈 Finding Volume Increases...")
    volume_increases = []
    
    for idx, april_row in april_volume_df.iterrows():
        symbol = april_row['SYMBOL']
        april_volume = april_row['TTL_TRD_QNTY']
        april_date = april_row['DATE']
        
        may_symbol_data = may_combined[may_combined['SYMBOL'] == symbol].copy()
        may_symbol_data = may_symbol_data.dropna(subset=['TTL_TRD_QNTY'])
        
        if len(may_symbol_data) > 0:
            may_max_volume = may_symbol_data['TTL_TRD_QNTY'].max()
            may_max_idx = may_symbol_data['TTL_TRD_QNTY'].idxmax()
            may_date = may_symbol_data.loc[may_max_idx, 'DATE']
            
            if may_max_volume > april_volume and april_volume > 0:
                increase = may_max_volume - april_volume
                pct_increase = ((may_max_volume - april_volume) / april_volume) * 100
                
                volume_increases.append({
                    'SYMBOL': symbol,
                    'APRIL_VOLUME': april_volume,
                    'APRIL_DATE': april_date,
                    'MAY_VOLUME': may_max_volume,
                    'MAY_DATE': may_date,
                    'VOLUME_INCREASE': increase,
                    'INCREASE_PERCENTAGE': f'{pct_increase:.1f}%',
                    'TIMES_HIGHER': f'{may_max_volume/april_volume:.1f}x',
                    'COMPARISON': f'April: {april_volume:,} → May: {may_max_volume:,} ({pct_increase:.1f}% ↗)'
                })
    
    # Process Delivery Comparison - Only increases
    print("📦 Finding Delivery Increases...")
    delivery_increases = []
    
    for idx, april_row in april_delivery_df.iterrows():
        symbol = april_row['SYMBOL']
        april_delivery = april_row['DELIV_QTY']
        april_date = april_row['DATE']
        
        may_symbol_data = may_combined[may_combined['SYMBOL'] == symbol].copy()
        may_symbol_data = may_symbol_data.dropna(subset=['DELIV_QTY'])
        
        if len(may_symbol_data) > 0:
            may_max_delivery = may_symbol_data['DELIV_QTY'].max()
            may_max_idx = may_symbol_data['DELIV_QTY'].idxmax()
            may_date = may_symbol_data.loc[may_max_idx, 'DATE']
            
            if may_max_delivery > april_delivery and april_delivery > 0:
                increase = may_max_delivery - april_delivery
                pct_increase = ((may_max_delivery - april_delivery) / april_delivery) * 100
                
                delivery_increases.append({
                    'SYMBOL': symbol,
                    'APRIL_DELIVERY': april_delivery,
                    'APRIL_DATE': april_date,
                    'MAY_DELIVERY': may_max_delivery,
                    'MAY_DATE': may_date,
                    'DELIVERY_INCREASE': increase,
                    'INCREASE_PERCENTAGE': f'{pct_increase:.1f}%',
                    'TIMES_HIGHER': f'{may_max_delivery/april_delivery:.1f}x',
                    'COMPARISON': f'April: {april_delivery:,} → May: {may_max_delivery:,} ({pct_increase:.1f}% ↗)'
                })
    
    print(f"📊 Results: {len(volume_increases)} volume increases, {len(delivery_increases)} delivery increases")
    
    # Create Excel file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"April_May_Increases_Only_{timestamp}.xlsx"
    
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
                    'Details': ['All May values were ≤ April values']
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
    april_may_increased_only()
