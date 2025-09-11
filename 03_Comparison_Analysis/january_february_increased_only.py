#!/usr/bin/env python3
"""
🔍 January 2025 vs February 2025 NSE Data Comparison - INCREASED VALUES ONLY
Compare each symbol and show only those with increases
"""

import pandas as pd
import glob
import os
from datetime import datetime

def january_february_increased_only():
    """Compare January 2025 analysis symbols with February 2025 daily data - Show only increases"""
    
    print("🔍 JANUARY vs FEBRUARY 2025 - INCREASED VALUES ONLY")
    print("=" * 60)
    
    # Load January 2025 analysis results
    january_file = "NSE_JANUARY2025_data_Unique_Symbol_Analysis_20250911_112727.xlsx"
    
    if not os.path.exists(january_file):
        print(f"❌ January analysis file not found: {january_file}")
        return
    
    print(f"📖 Loading January 2025 analysis: {january_file}")
    
    # Read January analysis sheets
    try:
        jan_volume_df = pd.read_excel(january_file, sheet_name='Unique_TTL_TRD_QNTY')
        jan_delivery_df = pd.read_excel(january_file, sheet_name='Unique_DELIV_QTY')
        print(f"   ✅ January Volume Analysis: {len(jan_volume_df)} symbols")
        print(f"   ✅ January Delivery Analysis: {len(jan_delivery_df)} symbols")
    except Exception as e:
        print(f"❌ Error reading January analysis: {str(e)}")
        return
    
    # Load February 2025 CSV files
    feb_csv_pattern = "NSE_February_2025_Data/cm*.csv"
    feb_csv_files = glob.glob(feb_csv_pattern)
    
    if not feb_csv_files:
        print(f"❌ No February 2025 CSV files found!")
        return
    
    print(f"📁 Found {len(feb_csv_files)} February 2025 CSV files")
    
    # Read and combine all February data
    print("🔄 Loading February 2025 daily data...")
    feb_all_data = []
    
    for file in sorted(feb_csv_files):
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
                    feb_all_data.append(df)
                    
        except Exception as e:
            continue
    
    if not feb_all_data:
        print("❌ No valid February data found!")
        return
    
    feb_combined = pd.concat(feb_all_data, ignore_index=True)
    print(f"   ✅ Combined February data: {len(feb_combined):,} records")
    
    # Process Volume Comparison - Only increases
    print("\n📈 Finding Volume Increases...")
    volume_increases = []
    
    for idx, jan_row in jan_volume_df.iterrows():
        symbol = jan_row['SYMBOL']
        jan_volume = jan_row['TTL_TRD_QNTY']
        jan_date = jan_row['DATE']
        
        feb_symbol_data = feb_combined[feb_combined['SYMBOL'] == symbol].copy()
        feb_symbol_data = feb_symbol_data.dropna(subset=['TTL_TRD_QNTY'])
        
        if len(feb_symbol_data) > 0:
            feb_max_volume = feb_symbol_data['TTL_TRD_QNTY'].max()
            feb_max_idx = feb_symbol_data['TTL_TRD_QNTY'].idxmax()
            feb_date = feb_symbol_data.loc[feb_max_idx, 'DATE']
            
            if feb_max_volume > jan_volume and jan_volume > 0:
                increase = feb_max_volume - jan_volume
                pct_increase = ((feb_max_volume - jan_volume) / jan_volume) * 100
                
                volume_increases.append({
                    'SYMBOL': symbol,
                    'JANUARY_VOLUME': jan_volume,
                    'JANUARY_DATE': jan_date,
                    'FEBRUARY_VOLUME': feb_max_volume,
                    'FEBRUARY_DATE': feb_date,
                    'VOLUME_INCREASE': increase,
                    'INCREASE_PERCENTAGE': f'{pct_increase:.1f}%',
                    'TIMES_HIGHER': f'{feb_max_volume/jan_volume:.1f}x',
                    'COMPARISON': f'January: {jan_volume:,} → February: {feb_max_volume:,} ({pct_increase:.1f}% ↗)'
                })
    
    # Process Delivery Comparison - Only increases
    print("📦 Finding Delivery Increases...")
    delivery_increases = []
    
    for idx, jan_row in jan_delivery_df.iterrows():
        symbol = jan_row['SYMBOL']
        jan_delivery = jan_row['DELIV_QTY']
        jan_date = jan_row['DATE']
        
        feb_symbol_data = feb_combined[feb_combined['SYMBOL'] == symbol].copy()
        feb_symbol_data = feb_symbol_data.dropna(subset=['DELIV_QTY'])
        
        if len(feb_symbol_data) > 0:
            feb_max_delivery = feb_symbol_data['DELIV_QTY'].max()
            feb_max_idx = feb_symbol_data['DELIV_QTY'].idxmax()
            feb_date = feb_symbol_data.loc[feb_max_idx, 'DATE']
            
            if feb_max_delivery > jan_delivery and jan_delivery > 0:
                increase = feb_max_delivery - jan_delivery
                pct_increase = ((feb_max_delivery - jan_delivery) / jan_delivery) * 100
                
                delivery_increases.append({
                    'SYMBOL': symbol,
                    'JANUARY_DELIVERY': jan_delivery,
                    'JANUARY_DATE': jan_date,
                    'FEBRUARY_DELIVERY': feb_max_delivery,
                    'FEBRUARY_DATE': feb_date,
                    'DELIVERY_INCREASE': increase,
                    'INCREASE_PERCENTAGE': f'{pct_increase:.1f}%',
                    'TIMES_HIGHER': f'{feb_max_delivery/jan_delivery:.1f}x',
                    'COMPARISON': f'January: {jan_delivery:,} → February: {feb_max_delivery:,} ({pct_increase:.1f}% ↗)'
                })
    
    print(f"📊 Results: {len(volume_increases)} volume increases, {len(delivery_increases)} delivery increases")
    
    # Create Excel file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"January_February_Increases_Only_{timestamp}.xlsx"
    
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
                    'Details': ['All February values were ≤ January values']
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
    january_february_increased_only()
