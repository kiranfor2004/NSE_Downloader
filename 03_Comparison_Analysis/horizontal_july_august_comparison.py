#!/usr/bin/env python3
"""
Horizontal July vs August Comparison
Shows each August date as separate columns with exact values
"""

import pandas as pd
import os
from datetime import datetime
import glob

def main():
    print("🔍 Horizontal July vs August Comparison")
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
    
    # Process Volume Comparison
    print("\n🔍 Processing volume comparison...")
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
                # Create base record
                result = {
                    'SYMBOL': symbol,
                    'JULY_TTL_TRD_QNTY': f"{july_volume:,}",
                    'JULY_DATE': july_date,
                    'AUGUST_WIN_COUNT': len(higher_august)
                }
                
                # Add each August date as separate columns
                for i, (_, aug_row) in enumerate(higher_august.iterrows(), 1):
                    aug_volume = aug_row['TTL_TRD_QNTY']
                    aug_date = aug_row['AUGUST_DATE']
                    
                    result[f'AUG_DATE_{i}'] = aug_date
                    result[f'AUG_VOLUME_{i}'] = f"{aug_volume:,}"
                    result[f'AUG_INCREASE_{i}'] = f"{aug_volume - july_volume:,}"
                    result[f'AUG_PERCENT_{i}'] = f"{((aug_volume - july_volume) / july_volume * 100):.1f}%" if july_volume > 0 else "N/A"
                
                volume_results.append(result)
    
    # Process Delivery Comparison
    print("🔍 Processing delivery comparison...")
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
                # Create base record
                result = {
                    'SYMBOL': symbol,
                    'JULY_DELIV_QTY': f"{july_delivery:,}",
                    'JULY_DATE': july_date,
                    'AUGUST_WIN_COUNT': len(higher_august)
                }
                
                # Add each August date as separate columns
                for i, (_, aug_row) in enumerate(higher_august.iterrows(), 1):
                    aug_delivery = aug_row['DELIV_QTY']
                    aug_date = aug_row['AUGUST_DATE']
                    
                    result[f'AUG_DATE_{i}'] = aug_date
                    result[f'AUG_DELIVERY_{i}'] = f"{aug_delivery:,}"
                    result[f'AUG_INCREASE_{i}'] = f"{aug_delivery - july_delivery:,}"
                    result[f'AUG_PERCENT_{i}'] = f"{((aug_delivery - july_delivery) / july_delivery * 100):.1f}%" if july_delivery > 0 else "N/A"
                
                delivery_results.append(result)
    
    # Convert to DataFrames and sort
    if volume_results:
        volume_df = pd.DataFrame(volume_results)
        volume_df = volume_df.sort_values('AUGUST_WIN_COUNT', ascending=False)
        print(f"✅ Found {len(volume_df)} symbols with higher August volumes")
    else:
        volume_df = pd.DataFrame()
        print("❌ No symbols found with higher August volumes")
    
    if delivery_results:
        delivery_df = pd.DataFrame(delivery_results)
        delivery_df = delivery_df.sort_values('AUGUST_WIN_COUNT', ascending=False)
        print(f"✅ Found {len(delivery_df)} symbols with higher August deliveries")
    else:
        delivery_df = pd.DataFrame()
        print("❌ No symbols found with higher August deliveries")
    
    # Export to Excel
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"Horizontal_July_August_Comparison_{timestamp}.xlsx"
    
    print(f"\n💾 Creating horizontal comparison: {output_file}")
    
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        # Volume sheet
        if not volume_df.empty:
            volume_df.to_excel(writer, sheet_name='Volume_Horizontal', index=False)
            print(f"   📈 Volume: {len(volume_df)} symbols with horizontal layout")
        
        # Delivery sheet  
        if not delivery_df.empty:
            delivery_df.to_excel(writer, sheet_name='Delivery_Horizontal', index=False)
            print(f"   📦 Delivery: {len(delivery_df)} symbols with horizontal layout")
        
        # Create summary
        summary_data = {
            'Metric': [
                'Symbols with Volume Wins',
                'Symbols with Delivery Wins',
                'Max August Wins (Volume)', 
                'Max August Wins (Delivery)'
            ],
            'Count': [
                len(volume_df),
                len(delivery_df),
                volume_df['AUGUST_WIN_COUNT'].max() if not volume_df.empty else 0,
                delivery_df['AUGUST_WIN_COUNT'].max() if not delivery_df.empty else 0
            ]
        }
        summary_df = pd.DataFrame(summary_data)
        summary_df.to_excel(writer, sheet_name='Summary', index=False)
        print(f"   📊 Summary sheet created")
    
    print(f"\n✅ HORIZONTAL COMPARISON COMPLETE!")
    print(f"📄 Output: {output_file}")
    print(f"📊 Each August date is now a separate column with exact values")
    
    # Show example
    if not volume_df.empty:
        print(f"\n🏆 Top Volume Performer:")
        top_vol = volume_df.iloc[0]
        print(f"   {top_vol['SYMBOL']}: {top_vol['AUGUST_WIN_COUNT']} August wins")
        print(f"   July: {top_vol['JULY_TTL_TRD_QNTY']} on {top_vol['JULY_DATE']}")
        for i in range(1, min(4, top_vol['AUGUST_WIN_COUNT'] + 1)):  # Show first 3 August dates
            if f'AUG_DATE_{i}' in top_vol:
                print(f"   Aug {i}: {top_vol[f'AUG_VOLUME_{i}']} on {top_vol[f'AUG_DATE_{i}']} ({top_vol[f'AUG_PERCENT_{i}']} increase)")

if __name__ == "__main__":
    main()
