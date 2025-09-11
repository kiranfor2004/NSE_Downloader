#!/usr/bin/env python3
"""
🔍 May 2025 vs June 2025 NSE Data Comparison - INCREASED VALUES ONLY
Compare each symbol and show only those with increases
"""

import pandas as pd
import glob
import os
from datetime import datetime

def may_june_increased_only():
    """Compare May 2025 analysis symbols with June 2025 daily data - Show only increases"""
    
    print("🔍 MAY vs JUNE 2025 - INCREASED VALUES ONLY")
    print("=" * 60)
    
    # Load May 2025 analysis results
    may_file = "NSE_MAY2025_data_Unique_Symbol_Analysis_20250911_121541.xlsx"
    
    if not os.path.exists(may_file):
        print(f"❌ May analysis file not found: {may_file}")
        return
    
    print(f"📖 Loading May 2025 analysis: {may_file}")
    
    # Read May analysis sheets
    try:
        may_volume_df = pd.read_excel(may_file, sheet_name='Unique_TTL_TRD_QNTY')
        may_delivery_df = pd.read_excel(may_file, sheet_name='Unique_DELIV_QTY')
        print(f"   ✅ May Volume Analysis: {len(may_volume_df)} symbols")
        print(f"   ✅ May Delivery Analysis: {len(may_delivery_df)} symbols")
    except Exception as e:
        print(f"❌ Error reading May analysis: {str(e)}")
        return
    
    # Load June 2025 CSV files
    june_csv_pattern = "NSE_June_2025_Data/cm*.csv"
    june_csv_files = glob.glob(june_csv_pattern)
    
    if not june_csv_files:
        print(f"❌ No June 2025 CSV files found!")
        return
    
    print(f"📁 Found {len(june_csv_files)} June 2025 CSV files")
    
    # Read and combine all June data
    print("🔄 Loading June 2025 daily data...")
    june_all_data = []
    
    for file in sorted(june_csv_files):
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
                    june_all_data.append(df)
                    
        except Exception as e:
            continue
    
    if not june_all_data:
        print("❌ No valid June data found!")
        return
    
    june_combined = pd.concat(june_all_data, ignore_index=True)
    print(f"   ✅ Combined June data: {len(june_combined):,} records")
    
    # Process Volume Comparison - Only increases
    print("\n📈 Finding Volume Increases...")
    volume_increases = []
    
    for idx, may_row in may_volume_df.iterrows():
        symbol = may_row['SYMBOL']
        may_volume = may_row['TTL_TRD_QNTY']
        may_date = may_row['DATE']
        
        june_symbol_data = june_combined[june_combined['SYMBOL'] == symbol].copy()
        june_symbol_data = june_symbol_data.dropna(subset=['TTL_TRD_QNTY'])
        
        if len(june_symbol_data) > 0:
            june_max_volume = june_symbol_data['TTL_TRD_QNTY'].max()
            june_max_idx = june_symbol_data['TTL_TRD_QNTY'].idxmax()
            june_date = june_symbol_data.loc[june_max_idx, 'DATE']
            
            if june_max_volume > may_volume and may_volume > 0:
                increase = june_max_volume - may_volume
                pct_increase = ((june_max_volume - may_volume) / may_volume) * 100
                
                volume_increases.append({
                    'SYMBOL': symbol,
                    'MAY_VOLUME': may_volume,
                    'MAY_DATE': may_date,
                    'JUNE_VOLUME': june_max_volume,
                    'JUNE_DATE': june_date,
                    'VOLUME_INCREASE': increase,
                    'INCREASE_PERCENTAGE': f'{pct_increase:.1f}%',
                    'TIMES_HIGHER': f'{june_max_volume/may_volume:.1f}x',
                    'COMPARISON': f'May: {may_volume:,} → June: {june_max_volume:,} ({pct_increase:.1f}% ↗)'
                })
    
    # Process Delivery Comparison - Only increases
    print("📦 Finding Delivery Increases...")
    delivery_increases = []
    
    for idx, may_row in may_delivery_df.iterrows():
        symbol = may_row['SYMBOL']
        may_delivery = may_row['DELIV_QTY']
        may_date = may_row['DATE']
        
        june_symbol_data = june_combined[june_combined['SYMBOL'] == symbol].copy()
        june_symbol_data = june_symbol_data.dropna(subset=['DELIV_QTY'])
        
        if len(june_symbol_data) > 0:
            june_max_delivery = june_symbol_data['DELIV_QTY'].max()
            june_max_idx = june_symbol_data['DELIV_QTY'].idxmax()
            june_date = june_symbol_data.loc[june_max_idx, 'DATE']
            
            if june_max_delivery > may_delivery and may_delivery > 0:
                increase = june_max_delivery - may_delivery
                pct_increase = ((june_max_delivery - may_delivery) / may_delivery) * 100
                
                delivery_increases.append({
                    'SYMBOL': symbol,
                    'MAY_DELIVERY': may_delivery,
                    'MAY_DATE': may_date,
                    'JUNE_DELIVERY': june_max_delivery,
                    'JUNE_DATE': june_date,
                    'DELIVERY_INCREASE': increase,
                    'INCREASE_PERCENTAGE': f'{pct_increase:.1f}%',
                    'TIMES_HIGHER': f'{june_max_delivery/may_delivery:.1f}x',
                    'COMPARISON': f'May: {may_delivery:,} → June: {june_max_delivery:,} ({pct_increase:.1f}% ↗)'
                })
    
    print(f"📊 Results: {len(volume_increases)} volume increases, {len(delivery_increases)} delivery increases")
    
    # Create Excel file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"May_June_Increases_Only_{timestamp}.xlsx"
    
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
                    'Details': ['All June values were ≤ May values']
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
    may_june_increased_only()
