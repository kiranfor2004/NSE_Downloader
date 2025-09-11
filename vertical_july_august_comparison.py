#!/usr/bin/env python3
"""
Vertical July vs August Comparison
Shows each August date as separate rows for better readability
"""

import pandas as pd
import os
from datetime import datetime
import glob

def main():
    print("🔍 Vertical July vs August Comparison")
    print("=" * 60)
    
    # Load July analysis
    july_file = "NSE_JULY2025_data_Unique_Symbol_Analysis_20250911_003120.xlsx"
    
    if not os.path.exists(july_file):
        print(f"❌ July analysis file not found: {july_file}")
        return
    
    print("📁 Loading July analysis...")
    july_df = pd.read_excel(july_file, sheet_name='Unique_TTL_TRD_QNTY')
    july_df.columns = july_df.columns.str.strip()
    print(f"   ✅ {len(july_df)} symbols for volume analysis")
    
    # Load August CSV files
    august_folder = "NSE_August_2025_Data"
    august_files = glob.glob(f"{august_folder}/*.csv")
    
    if not august_files:
        print(f"❌ No August CSV files found in {august_folder}")
        return
    
    print(f"📖 Loading {len(august_files)} August CSV files...")
    august_data = []
    
    for file in august_files:
        # Extract date from filename
        filename = os.path.basename(file)
        date_part = filename.replace('sec_bhavdata_full_', '').replace('.csv', '')
        formatted_date = f"{date_part[:2]}-{date_part[2:4]}-{date_part[4:]}"
        
        try:
            df = pd.read_csv(file)
            df.columns = df.columns.str.strip()
            
            # Clean data
            for col in df.columns:
                if df[col].dtype == 'object':
                    df[col] = df[col].astype(str).str.strip()
            
            # Convert numeric columns
            numeric_cols = ['TTL_TRD_QNTY', 'DELIV_QTY']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
            # Filter for EQ series
            if 'SERIES' in df.columns:
                df = df[df['SERIES'] == 'EQ'].copy()
            
            df['AUGUST_DATE'] = formatted_date
            august_data.append(df[['SYMBOL', 'TTL_TRD_QNTY', 'DELIV_QTY', 'AUGUST_DATE']])
            
        except Exception as e:
            print(f"⚠️  Error reading {file}: {e}")
    
    if not august_data:
        print("❌ No August data loaded successfully")
        return
    
    august_combined = pd.concat(august_data, ignore_index=True)
    print(f"   ✅ Combined: {len(august_combined):,} August records")
    
    # Process Volume Comparison - VERTICAL FORMAT
    print("\n🔍 Processing volume comparison (vertical format)...")
    volume_results = []
    
    for _, july_row in july_df.iterrows():
        symbol = july_row['SYMBOL']
        july_volume = july_row['TTL_TRD_QNTY']
        july_date = july_row.get('DATE', 'Unknown')
        
        # Find this symbol in August data
        august_symbol = august_combined[august_combined['SYMBOL'] == symbol]
        
        if not august_symbol.empty:
            # Find August dates where volume is higher than July
            higher_august = august_symbol[august_symbol['TTL_TRD_QNTY'] > july_volume]
            
            if not higher_august.empty:
                # Create SEPARATE ROW for EACH August winning date
                for _, aug_row in higher_august.iterrows():
                    august_volume = aug_row['TTL_TRD_QNTY']
                    august_date = aug_row['AUGUST_DATE']
                    
                    # Calculate improvements
                    increase = august_volume - july_volume
                    increase_pct = (increase / july_volume * 100) if july_volume > 0 else 0
                    times_higher = august_volume / july_volume if july_volume > 0 else 0
                    
                    volume_results.append({
                        'SYMBOL': symbol,
                        'JULY_VOLUME': f"{july_volume:,}",
                        'JULY_DATE': july_date,
                        'AUGUST_VOLUME': f"{august_volume:,}",
                        'AUGUST_DATE': august_date,
                        'VOLUME_INCREASE': f"{increase:,}",
                        'INCREASE_PERCENTAGE': f"{increase_pct:.1f}%",
                        'TIMES_HIGHER': f"{times_higher:.1f}x",
                        'COMPARISON': f"July: {july_volume:,} → Aug: {august_volume:,} ({increase_pct:.1f}% ↗️)"
                    })
    
    # Process Delivery Comparison - VERTICAL FORMAT
    print("🔍 Processing delivery comparison (vertical format)...")
    delivery_results = []
    
    # Load July delivery analysis
    july_delivery_df = pd.read_excel(july_file, sheet_name='Unique_DELIV_QTY')
    july_delivery_df.columns = july_delivery_df.columns.str.strip()
    
    for _, july_row in july_delivery_df.iterrows():
        symbol = july_row['SYMBOL']
        july_delivery = july_row['DELIV_QTY']
        july_date = july_row.get('DATE', 'Unknown')
        
        # Find this symbol in August data
        august_symbol = august_combined[august_combined['SYMBOL'] == symbol]
        
        if not august_symbol.empty:
            # Find August dates where delivery is higher than July
            higher_august = august_symbol[august_symbol['DELIV_QTY'] > july_delivery]
            
            if not higher_august.empty:
                # Create SEPARATE ROW for EACH August winning date
                for _, aug_row in higher_august.iterrows():
                    august_delivery = aug_row['DELIV_QTY']
                    august_date = aug_row['AUGUST_DATE']
                    
                    # Calculate improvements
                    increase = august_delivery - july_delivery
                    increase_pct = (increase / july_delivery * 100) if july_delivery > 0 else 0
                    times_higher = august_delivery / july_delivery if july_delivery > 0 else 0
                    
                    delivery_results.append({
                        'SYMBOL': symbol,
                        'JULY_DELIVERY': f"{july_delivery:,}",
                        'JULY_DATE': july_date,
                        'AUGUST_DELIVERY': f"{august_delivery:,}",
                        'AUGUST_DATE': august_date,
                        'DELIVERY_INCREASE': f"{increase:,}",
                        'INCREASE_PERCENTAGE': f"{increase_pct:.1f}%",
                        'TIMES_HIGHER': f"{times_higher:.1f}x",
                        'COMPARISON': f"July: {july_delivery:,} → Aug: {august_delivery:,} ({increase_pct:.1f}% ↗️)"
                    })
    
    # Convert to DataFrames and sort
    if volume_results:
        volume_df = pd.DataFrame(volume_results)
        # Sort by percentage increase (descending)
        volume_df['SORT_PCT'] = volume_df['INCREASE_PERCENTAGE'].str.rstrip('%').astype(float)
        volume_df = volume_df.sort_values('SORT_PCT', ascending=False).drop('SORT_PCT', axis=1)
        print(f"✅ Found {len(volume_df)} volume improvement records")
    else:
        volume_df = pd.DataFrame()
        print("❌ No volume improvement records found")
    
    if delivery_results:
        delivery_df = pd.DataFrame(delivery_results)
        # Sort by percentage increase (descending)
        delivery_df['SORT_PCT'] = delivery_df['INCREASE_PERCENTAGE'].str.rstrip('%').astype(float)
        delivery_df = delivery_df.sort_values('SORT_PCT', ascending=False).drop('SORT_PCT', axis=1)
        print(f"✅ Found {len(delivery_df)} delivery improvement records")
    else:
        delivery_df = pd.DataFrame()
        print("❌ No delivery improvement records found")
    
    # Export to Excel with readable format
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"Vertical_July_August_Comparison_{timestamp}.xlsx"
    
    print(f"\n💾 Creating vertical comparison: {output_file}")
    
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        # Volume sheet - Each row = one August win
        if not volume_df.empty:
            volume_df.to_excel(writer, sheet_name='Volume_Vertical', index=False)
            print(f"   📈 Volume: {len(volume_df)} improvement rows written")
            
        # Delivery sheet - Each row = one August win  
        if not delivery_df.empty:
            delivery_df.to_excel(writer, sheet_name='Delivery_Vertical', index=False)
            print(f"   📦 Delivery: {len(delivery_df)} improvement rows written")
            
        # Create summary by symbol (how many August wins per symbol)
        if not volume_df.empty:
            symbol_summary = volume_df.groupby('SYMBOL').agg({
                'AUGUST_DATE': 'count',
                'INCREASE_PERCENTAGE': lambda x: x.str.rstrip('%').astype(float).max()
            }).reset_index()
            symbol_summary.columns = ['SYMBOL', 'AUGUST_WIN_COUNT', 'MAX_INCREASE_PCT']
            symbol_summary['MAX_INCREASE_PCT'] = symbol_summary['MAX_INCREASE_PCT'].apply(lambda x: f"{x:.1f}%")
            symbol_summary = symbol_summary.sort_values('AUGUST_WIN_COUNT', ascending=False)
            symbol_summary.to_excel(writer, sheet_name='Symbol_Summary', index=False)
            print(f"   📊 Symbol summary created")
        
        # Overall summary
        summary_data = {
            'Metric': [
                'Total Volume Improvement Records',
                'Unique Symbols with Volume Wins',
                'Total Delivery Improvement Records', 
                'Unique Symbols with Delivery Wins',
                'Top Volume Performer (Symbol)',
                'Top Delivery Performer (Symbol)'
            ],
            'Value': [
                len(volume_df),
                len(volume_df['SYMBOL'].unique()) if not volume_df.empty else 0,
                len(delivery_df),
                len(delivery_df['SYMBOL'].unique()) if not delivery_df.empty else 0,
                volume_df.iloc[0]['SYMBOL'] if not volume_df.empty else 'None',
                delivery_df.iloc[0]['SYMBOL'] if not delivery_df.empty else 'None'
            ]
        }
        summary_df = pd.DataFrame(summary_data)
        summary_df.to_excel(writer, sheet_name='Overall_Summary', index=False)
        print(f"   📈 Overall summary created")
    
    print(f"\n✅ VERTICAL COMPARISON COMPLETE!")
    print(f"📄 Output: {output_file}")
    print(f"📋 Each row shows one specific August date that beat July")
    print(f"📖 Much more readable than horizontal columns!")
    
    # Show top 5 examples
    if not volume_df.empty:
        print(f"\n🏆 TOP 5 VOLUME IMPROVEMENTS (most readable format):")
        for i, (_, row) in enumerate(volume_df.head().iterrows(), 1):
            print(f"  {i}. {row['SYMBOL']} on {row['AUGUST_DATE']}: {row['COMPARISON']}")
    
    if not delivery_df.empty:
        print(f"\n📦 TOP 5 DELIVERY IMPROVEMENTS (most readable format):")
        for i, (_, row) in enumerate(delivery_df.head().iterrows(), 1):
            print(f"  {i}. {row['SYMBOL']} on {row['AUGUST_DATE']}: {row['COMPARISON']}")

if __name__ == "__main__":
    main()
