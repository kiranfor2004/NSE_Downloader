#!/usr/bin/env python3
"""
🔍 Simple July vs August Comparison
For each symbol in July analysis, find August dates with higher values
Simple output showing: Symbol | July Value | August Higher Dates
"""

import pandas as pd
import glob
import os
from datetime import datetime

def simple_july_august_comparison():
    """Simple comparison - find August dates where symbols exceeded July values"""
    
    print("🔍 Simple July vs August Comparison")
    print("=" * 50)
    
    # Load July analysis file
    july_file = "NSE_JULY2025_data_Unique_Symbol_Analysis_20250911_003120.xlsx"
    
    if not os.path.exists(july_file):
        print(f"❌ July file not found: {july_file}")
        return
    
    print(f"📁 Loading July analysis...")
    july_volume_df = pd.read_excel(july_file, sheet_name='Unique_TTL_TRD_QNTY')
    july_delivery_df = pd.read_excel(july_file, sheet_name='Unique_DELIV_QTY')
    print(f"   ✅ {len(july_volume_df)} symbols for volume")
    print(f"   ✅ {len(july_delivery_df)} symbols for delivery")
    
    # Load August CSV files
    august_files = glob.glob("NSE_August_2025_Data/sec_bhavdata_full_*.csv")
    if not august_files:
        print("❌ No August CSV files found!")
        return
    
    print(f"\n📖 Loading {len(august_files)} August CSV files...")
    august_all_data = []
    
    for file in sorted(august_files):
        try:
            df = pd.read_csv(file)
            df.columns = df.columns.str.strip()
            df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
            
            if 'SERIES' in df.columns:
                df = df[df['SERIES'] == 'EQ'].copy()
                if len(df) > 0:
                    # Convert numeric columns
                    df['TTL_TRD_QNTY'] = pd.to_numeric(df['TTL_TRD_QNTY'], errors='coerce')
                    df['DELIV_QTY'] = pd.to_numeric(df['DELIV_QTY'], errors='coerce')
                    
                    # Extract date from filename (DDMMYYYY format)
                    date_part = os.path.basename(file).replace('sec_bhavdata_full_', '').replace('.csv', '')
                    day = date_part[:2]
                    month = date_part[2:4]
                    year = date_part[4:]
                    august_date = f"{day}-{month}-{year}"
                    df['AUGUST_DATE'] = august_date
                    
                    august_all_data.append(df[['SYMBOL', 'TTL_TRD_QNTY', 'DELIV_QTY', 'AUGUST_DATE']])
        except Exception as e:
            print(f"   ❌ Error with {os.path.basename(file)}: {str(e)[:30]}")
    
    if not august_all_data:
        print("❌ No August data loaded!")
        return
    
    # Combine August data
    august_combined = pd.concat(august_all_data, ignore_index=True)
    print(f"   ✅ Combined: {len(august_combined):,} August records")
    
    # Results storage
    volume_results = []
    delivery_results = []
    
    print("\n🔍 Checking volume comparisons...")
    # Volume Comparison - Show ALL instances, not just summary
    for _, july_row in july_volume_df.iterrows():
        symbol = july_row['SYMBOL']
        july_volume = july_row['TTL_TRD_QNTY']
        july_date = july_row.get('DATE', 'Unknown')
        
        # Find this symbol in August data
        august_symbol = august_combined[august_combined['SYMBOL'] == symbol]
        
        if not august_symbol.empty:
            # Find August dates where volume is higher than July
            higher_august = august_symbol[august_symbol['TTL_TRD_QNTY'] > july_volume]
            
            if not higher_august.empty:
                # Create a separate row for EACH August date that exceeded July
                for _, aug_row in higher_august.iterrows():
                    august_volume = aug_row['TTL_TRD_QNTY']
                    august_date = aug_row['AUGUST_DATE']
                    
                    # Calculate improvement
                    increase = august_volume - july_volume
                    increase_pct = (increase / july_volume * 100) if july_volume > 0 else 0
                    
                    volume_results.append({
                        'SYMBOL': symbol,
                        'JULY_TTL_TRD_QNTY': f"{july_volume:,}",
                        'JULY_DATE': july_date,
                        'AUGUST_TTL_TRD_QNTY': f"{august_volume:,}",
                        'AUGUST_DATE': august_date,
                        'INCREASE': f"{increase:,}",
                        'INCREASE_PERCENTAGE': f"{increase_pct:.1f}%",
                        'TIMES_HIGHER': f"{august_volume/july_volume:.1f}x" if july_volume > 0 else "N/A"
                    })
    
    print(f"🔍 Checking delivery comparisons...")
    # Delivery Comparison - Show ALL instances, not just summary  
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
                # Create a separate row for EACH August date that exceeded July
                for _, aug_row in higher_august.iterrows():
                    august_delivery = aug_row['DELIV_QTY']
                    august_date = aug_row['AUGUST_DATE']
                    
                    # Calculate improvement
                    increase = august_delivery - july_delivery
                    increase_pct = (increase / july_delivery * 100) if july_delivery > 0 else 0
                    
                    delivery_results.append({
                        'SYMBOL': symbol,
                        'JULY_DELIV_QTY': f"{july_delivery:,}",
                        'JULY_DATE': july_date,
                        'AUGUST_DELIV_QTY': f"{august_delivery:,}",
                        'AUGUST_DATE': august_date,
                        'INCREASE': f"{increase:,}",
                        'INCREASE_PERCENTAGE': f"{increase_pct:.1f}%",
                        'TIMES_HIGHER': f"{august_delivery/july_delivery:.1f}x" if july_delivery > 0 else "N/A"
                    })
    
    # Convert to DataFrame and sort by improvement percentage
    if volume_results:
        volume_df = pd.DataFrame(volume_results)
        # Sort by increase percentage (descending)
        volume_df['INCREASE_PCT_NUM'] = volume_df['INCREASE_PERCENTAGE'].str.rstrip('%').astype(float)
        volume_df = volume_df.sort_values('INCREASE_PCT_NUM', ascending=False).drop('INCREASE_PCT_NUM', axis=1)
        print(f"✅ Found {len(volume_df)} August volume records that exceeded July peaks")
    else:
        volume_df = pd.DataFrame()
        print("❌ No August volume records exceeded July peaks")
    
    if delivery_results:
        delivery_df = pd.DataFrame(delivery_results)
        # Sort by increase percentage (descending)
        delivery_df['INCREASE_PCT_NUM'] = delivery_df['INCREASE_PERCENTAGE'].str.rstrip('%').astype(float)
        delivery_df = delivery_df.sort_values('INCREASE_PCT_NUM', ascending=False).drop('INCREASE_PCT_NUM', axis=1)
        print(f"✅ Found {len(delivery_df)} August delivery records that exceeded July peaks")
    else:
        delivery_df = pd.DataFrame()
        print("❌ No August delivery records exceeded July peaks")
    
    # Create summary statistics
    print("\n📊 DETAILED COMPARISON SUMMARY:")
    print(f"• Total July symbols analyzed: {len(july_volume_df)}")
    print(f"• August volume wins: {len(volume_df)} records across {len(volume_df['SYMBOL'].unique()) if not volume_df.empty else 0} unique symbols")
    print(f"• August delivery wins: {len(delivery_df)} records across {len(delivery_df['SYMBOL'].unique()) if not delivery_df.empty else 0} unique symbols")
    
    # Show top performers
    if not volume_df.empty:
        print(f"\n🏆 TOP 5 VOLUME IMPROVEMENTS:")
        for i, (_, row) in enumerate(volume_df.head().iterrows(), 1):
            print(f"  {i}. {row['SYMBOL']}: {row['TIMES_HIGHER']} ({row['INCREASE_PERCENTAGE']} increase)")
            print(f"     July {row['JULY_DATE']}: {row['JULY_TTL_TRD_QNTY']} → August {row['AUGUST_DATE']}: {row['AUGUST_TTL_TRD_QNTY']}")
    
    if not delivery_df.empty:
        print(f"\n🏆 TOP 5 DELIVERY IMPROVEMENTS:")
        for i, (_, row) in enumerate(delivery_df.head().iterrows(), 1):
            print(f"  {i}. {row['SYMBOL']}: {row['TIMES_HIGHER']} ({row['INCREASE_PERCENTAGE']} increase)")
            print(f"     July {row['JULY_DATE']}: {row['JULY_DELIV_QTY']} → August {row['AUGUST_DATE']}: {row['AUGUST_DELIV_QTY']}")
    
    # Export to Excel with better formatting
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"Detailed_July_August_Comparison_{timestamp}.xlsx"
    
    print(f"\n💾 Creating detailed comparison: {output_file}")
    
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        # Export all volume records
        if not volume_df.empty:
            volume_df.to_excel(writer, sheet_name='All_Volume_Improvements', index=False)
            print(f"   � Volume: {len(volume_df)} improvement records written")
            
        # Export all delivery records  
        if not delivery_df.empty:
            delivery_df.to_excel(writer, sheet_name='All_Delivery_Improvements', index=False)
            print(f"   📦 Delivery: {len(delivery_df)} improvement records written")
            
        # Create summary sheet
        summary_data = {
            'Metric': [
                'Total July Symbols Analyzed',
                'August Volume Win Records',
                'Unique Symbols with Volume Wins', 
                'August Delivery Win Records',
                'Unique Symbols with Delivery Wins',
                'Top Volume Performer',
                'Top Delivery Performer'
            ],
            'Value': [
                len(july_volume_df),
                len(volume_df),
                len(volume_df['SYMBOL'].unique()) if not volume_df.empty else 0,
                len(delivery_df), 
                len(delivery_df['SYMBOL'].unique()) if not delivery_df.empty else 0,
                volume_df.iloc[0]['SYMBOL'] if not volume_df.empty else 'None',
                delivery_df.iloc[0]['SYMBOL'] if not delivery_df.empty else 'None'
            ]
        }
        summary_df = pd.DataFrame(summary_data)
        summary_df.to_excel(writer, sheet_name='Summary', index=False)
        print(f"   📊 Summary sheet created")

    print(f"\n✅ DETAILED COMPARISON COMPLETE!")
    print(f"📄 Output: {output_file}")
    print(f"📋 This shows ALL instances where August exceeded July, not just summaries")
    print(f"💡 Each row represents one August date that beat the July peak for that symbol")

if __name__ == "__main__":
    simple_july_august_comparison()
