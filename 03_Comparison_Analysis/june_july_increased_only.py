#!/usr/bin/env python3
"""
🔍 June 2025 vs July 2025 NSE Data Comparison - INCREASED VALUES ONLY
Compare each symbol and show only those with increases
"""

import pandas as pd
import glob
import os
from datetime import datetime

def june_july_increased_only():
    """Compare June 2025 analysis symbols with July 2025 daily data - Show only increases"""
    
    print("🔍 JUNE vs JULY 2025 - INCREASED VALUES ONLY")
    print("=" * 60)
    
    # Load June 2025 analysis results
    june_file = "NSE_JUNE2025_data_Unique_Symbol_Analysis_20250911_151422.xlsx"
    
    if not os.path.exists(june_file):
        print(f"❌ June analysis file not found: {june_file}")
        return
    
    print(f"📖 Loading June 2025 analysis: {june_file}")
    
    # Read June analysis sheets
    try:
        june_volume_df = pd.read_excel(june_file, sheet_name='Unique_TTL_TRD_QNTY')
        june_delivery_df = pd.read_excel(june_file, sheet_name='Unique_DELIV_QTY')
        print(f"   ✅ June Volume Analysis: {len(june_volume_df)} symbols")
        print(f"   ✅ June Delivery Analysis: {len(june_delivery_df)} symbols")
    except Exception as e:
        print(f"❌ Error reading June analysis: {str(e)}")
        return
    
    # Load July 2025 CSV files
    july_csv_pattern = "NSE_July_2025_Data/sec_bhavdata_full_*.csv"
    july_csv_files = glob.glob(july_csv_pattern)
    
    if not july_csv_files:
        print(f"❌ No July 2025 CSV files found!")
        return
    
    print(f"📁 Found {len(july_csv_files)} July 2025 CSV files")
    
    # Read and combine all July data
    print("🔄 Loading July 2025 daily data...")
    july_all_data = []
    
    for file in sorted(july_csv_files):
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
                    july_all_data.append(df)
                    
        except Exception as e:
            continue
    
    if not july_all_data:
        print("❌ No valid July data found!")
        return
    
    july_combined = pd.concat(july_all_data, ignore_index=True)
    print(f"   ✅ Combined July data: {len(july_combined):,} records")
    
    # Process Volume Comparison - Only increases
    print("\n📈 Finding Volume Increases...")
    volume_increases = []
    
    for idx, june_row in june_volume_df.iterrows():
        symbol = june_row['SYMBOL']
        june_volume = june_row['TTL_TRD_QNTY']
        june_date = june_row['DATE']
        
        july_symbol_data = july_combined[july_combined['SYMBOL'] == symbol].copy()
        july_symbol_data = july_symbol_data.dropna(subset=['TTL_TRD_QNTY'])
        
        if len(july_symbol_data) > 0:
            july_max_volume = july_symbol_data['TTL_TRD_QNTY'].max()
            july_max_idx = july_symbol_data['TTL_TRD_QNTY'].idxmax()
            july_date = july_symbol_data.loc[july_max_idx, 'DATE']
            
            if july_max_volume > june_volume and june_volume > 0:
                increase = july_max_volume - june_volume
                pct_increase = ((july_max_volume - june_volume) / june_volume) * 100
                
                volume_increases.append({
                    'SYMBOL': symbol,
                    'JUNE_VOLUME': june_volume,
                    'JUNE_DATE': june_date,
                    'JULY_VOLUME': july_max_volume,
                    'JULY_DATE': july_date,
                    'VOLUME_INCREASE': increase,
                    'INCREASE_PERCENTAGE': f'{pct_increase:.1f}%',
                    'TIMES_HIGHER': f'{july_max_volume/june_volume:.1f}x',
                    'COMPARISON': f'June: {june_volume:,} → July: {july_max_volume:,} ({pct_increase:.1f}% ↗)'
                })
    
    # Process Delivery Comparison - Only increases
    print("📦 Finding Delivery Increases...")
    delivery_increases = []
    
    for idx, june_row in june_delivery_df.iterrows():
        symbol = june_row['SYMBOL']
        june_delivery = june_row['DELIV_QTY']
        june_date = june_row['DATE']
        
        july_symbol_data = july_combined[july_combined['SYMBOL'] == symbol].copy()
        july_symbol_data = july_symbol_data.dropna(subset=['DELIV_QTY'])
        
        if len(july_symbol_data) > 0:
            july_max_delivery = july_symbol_data['DELIV_QTY'].max()
            july_max_idx = july_symbol_data['DELIV_QTY'].idxmax()
            july_date = july_symbol_data.loc[july_max_idx, 'DATE']
            
            if july_max_delivery > june_delivery and june_delivery > 0:
                increase = july_max_delivery - june_delivery
                pct_increase = ((july_max_delivery - june_delivery) / june_delivery) * 100
                
                delivery_increases.append({
                    'SYMBOL': symbol,
                    'JUNE_DELIVERY': june_delivery,
                    'JUNE_DATE': june_date,
                    'JULY_DELIVERY': july_max_delivery,
                    'JULY_DATE': july_date,
                    'DELIVERY_INCREASE': increase,
                    'INCREASE_PERCENTAGE': f'{pct_increase:.1f}%',
                    'TIMES_HIGHER': f'{july_max_delivery/june_delivery:.1f}x',
                    'COMPARISON': f'June: {june_delivery:,} → July: {july_max_delivery:,} ({pct_increase:.1f}% ↗)'
                })
    
    print(f"📊 Results: {len(volume_increases)} volume increases, {len(delivery_increases)} delivery increases")
    
    # Create Excel file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"June_July_Increases_Only_{timestamp}.xlsx"
    
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
                    'Details': ['All July values were ≤ June values']
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
    june_july_increased_only()
