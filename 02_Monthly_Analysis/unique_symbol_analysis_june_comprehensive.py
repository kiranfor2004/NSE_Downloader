#!/usr/bin/env python3
"""
🚀 NSE JUNE 2025 Unique Symbol Analysis - No Duplicates
Find for each symbol the date with highest TTL_TRD_QNTY and DELIV_QTY
- ONE record per symbol (no duplicates)
- Separate sheets for volume and delivery analysis  
- Only SERIES=EQ stocks
- Complete data with all columns
- Prioritizes records with complete price data
- Matches July/January/February/March/April/May 2025 analysis format exactly
"""

import pandas as pd
import glob
import os
from datetime import datetime

def create_unique_symbol_analysis_june():
    """Create unique symbol analysis from June 2025 CSV files"""
    
    print("📊 NSE JUNE 2025 Unique Symbol Analysis - No Duplicates")
    print("=" * 60)
    
    # Check for June 2025 CSV files
    june_csv_pattern = "NSE_June_2025_Data/cm*.csv"
    csv_files = glob.glob(june_csv_pattern)
    
    if not csv_files:
        print("❌ No June 2025 CSV files found!")
        print(f"💡 Looking for files matching: {june_csv_pattern}")
        return
    
    print(f"📁 Found {len(csv_files)} CSV files from June 2025:")
    for i, file in enumerate(sorted(csv_files), 1):
        print(f"   {i}. {os.path.basename(file)}")
    print()
    
    all_data = []
    
    # Read and combine data from all CSV files
    for file in sorted(csv_files):
        try:
            print(f"📖 Processing: {os.path.basename(file)}")
            
            # Read CSV file
            df = pd.read_csv(file)
            print(f"   ✅ Read CSV: {len(df)} records")
            
            # Clean column names (remove leading/trailing spaces)
            df.columns = df.columns.str.strip()
            
            # Clean data values (remove leading/trailing spaces) using map instead of deprecated applymap
            df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
            
            # Filter for EQ series only
            if 'SERIES' in df.columns:
                df = df[df['SERIES'] == 'EQ'].copy()
                eq_count = len(df)
                print(f"   📊 EQ series records: {eq_count}")
                
                if eq_count == 0:
                    continue
                
                # Standardize column names to ensure consistency across all files
                column_mapping = {
                    # Symbol variations
                    'symbol': 'SYMBOL', 'SYMBOL': 'SYMBOL',
                    # Date variations  
                    'date': 'DATE', 'DATE': 'DATE', 'DATE1': 'DATE',
                    # Series variations
                    'series': 'SERIES', 'SERIES': 'SERIES',
                    # Price variations
                    'prev_close': 'PREV_CLOSE', 'PREV_CLOSE': 'PREV_CLOSE',
                    'open_price': 'OPEN_PRICE', 'OPEN_PRICE': 'OPEN_PRICE', 'open': 'OPEN_PRICE',
                    'high_price': 'HIGH_PRICE', 'HIGH_PRICE': 'HIGH_PRICE', 'high': 'HIGH_PRICE',
                    'low_price': 'LOW_PRICE', 'LOW_PRICE': 'LOW_PRICE', 'low': 'LOW_PRICE',
                    'last_price': 'LAST_PRICE', 'LAST_PRICE': 'LAST_PRICE', 'last': 'LAST_PRICE',
                    'close_price': 'CLOSE_PRICE', 'CLOSE_PRICE': 'CLOSE_PRICE', 'close': 'CLOSE_PRICE',
                    'avg_price': 'AVG_PRICE', 'AVG_PRICE': 'AVG_PRICE',
                    # Volume and trading variations
                    'TTL_TRD_QNTY': 'TTL_TRD_QNTY', 'total_traded_qty': 'TTL_TRD_QNTY', 'volume': 'TTL_TRD_QNTY',
                    'turnover_lacs': 'TURNOVER_LACS', 'TURNOVER_LACS': 'TURNOVER_LACS', 'turnover': 'TURNOVER_LACS',
                    'no_of_trades': 'NO_OF_TRADES', 'NO_OF_TRADES': 'NO_OF_TRADES', 'trades': 'NO_OF_TRADES',
                    # Delivery variations
                    'DELIV_QTY': 'DELIV_QTY', 'delivery_qty': 'DELIV_QTY', 'deliv_qty': 'DELIV_QTY',
                    'deliv_per': 'DELIV_PER', 'DELIV_PER': 'DELIV_PER', 'delivery_percentage': 'DELIV_PER'
                }
                
                # Apply column mapping and preserve original data
                df = df.rename(columns=column_mapping)
                
                # Ensure numeric columns are properly converted but preserve actual values
                numeric_columns = ['PREV_CLOSE', 'OPEN_PRICE', 'HIGH_PRICE', 'LOW_PRICE', 'LAST_PRICE', 
                                 'CLOSE_PRICE', 'AVG_PRICE', 'TTL_TRD_QNTY', 'TURNOVER_LACS', 
                                 'NO_OF_TRADES', 'DELIV_QTY', 'DELIV_PER']
                
                for col in numeric_columns:
                    if col in df.columns:
                        # Convert to numeric but keep NaN for missing values, don't default to 0
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                
                # Create helper columns for analysis
                if 'TTL_TRD_QNTY' in df.columns:
                    df['_volume_value'] = pd.to_numeric(df['TTL_TRD_QNTY'], errors='coerce')
                else:
                    df['_volume_value'] = 0
                
                if 'DELIV_QTY' in df.columns:
                    df['_delivery_value'] = pd.to_numeric(df['DELIV_QTY'], errors='coerce')
                else:
                    df['_delivery_value'] = 0
                
                if 'SYMBOL' in df.columns:
                    df['_symbol_value'] = df['SYMBOL'].astype(str)
                else:
                    df['_symbol_value'] = ''
                
                all_data.append(df)
                
            else:
                print(f"   ❌ No SERIES column found in {file}")
                
        except Exception as e:
            print(f"   ❌ Error processing {file}: {str(e)[:50]}")
            continue
    
    if not all_data:
        print("❌ No valid data found!")
        return
    
    # Combine all data
    print("🔄 Combining and deduplicating data...")
    combined_df = pd.concat(all_data, ignore_index=True)
    print(f"📊 Before deduplication: {len(combined_df):,} records")
    print(f"🏢 Unique symbols: {combined_df['SYMBOL'].nunique():,}")
    
    # Create unique analysis - keep only the highest volume record per symbol
    print("🎯 Creating unique symbol analysis...")
    
    # For Volume Analysis: Keep record with highest volume per symbol, prioritizing complete data
    volume_unique_list = []
    for symbol in combined_df['_symbol_value'].unique():
        if pd.isna(symbol) or symbol == '':
            continue
            
        symbol_data = combined_df[combined_df['_symbol_value'] == symbol].copy()
        
        # Find max volume for this symbol (ignoring NaN)
        max_volume = symbol_data['_volume_value'].max()
        if pd.isna(max_volume):
            continue
        
        # Get all records with max volume
        max_volume_records = symbol_data[symbol_data['_volume_value'] == max_volume]
        
        # If multiple records have same max volume, prioritize the one with most complete price data
        if len(max_volume_records) > 1:
            price_cols = ['CLOSE_PRICE', 'OPEN_PRICE', 'HIGH_PRICE', 'LOW_PRICE', 'LAST_PRICE']
            max_volume_records = max_volume_records.copy()
            max_volume_records['_completeness_score'] = 0
            
            for col in price_cols:
                if col in max_volume_records.columns:
                    # Count non-null, non-zero values
                    max_volume_records['_completeness_score'] += ((max_volume_records[col].notna()) & (max_volume_records[col] != 0)).astype(int)
            
            # Select the record with highest completeness score
            best_idx = max_volume_records['_completeness_score'].idxmax()
            best_record = max_volume_records.loc[best_idx]
        else:
            best_record = max_volume_records.iloc[0]
        
        volume_unique_list.append(best_record)
    
    volume_unique = pd.DataFrame(volume_unique_list).reset_index(drop=True) if volume_unique_list else pd.DataFrame()
    if not volume_unique.empty:
        print(f"📈 Volume analysis: {len(volume_unique)} unique symbols")
    
    # For Delivery Analysis: Keep record with highest delivery per symbol, prioritizing complete data
    delivery_unique_list = []
    for symbol in combined_df['_symbol_value'].unique():
        if pd.isna(symbol) or symbol == '':
            continue
            
        symbol_data = combined_df[combined_df['_symbol_value'] == symbol].copy()
        
        # Skip if no delivery data
        if '_delivery_value' not in symbol_data.columns or symbol_data['_delivery_value'].isna().all():
            continue
        
        # Find max delivery for this symbol (ignoring NaN)
        max_delivery = symbol_data['_delivery_value'].max()
        if pd.isna(max_delivery):
            continue
            
        # Get all records with max delivery
        max_delivery_records = symbol_data[symbol_data['_delivery_value'] == max_delivery]
        
        # If multiple records have same max delivery, prioritize the one with most complete price data
        if len(max_delivery_records) > 1:
            price_cols = ['CLOSE_PRICE', 'OPEN_PRICE', 'HIGH_PRICE', 'LOW_PRICE', 'LAST_PRICE']
            max_delivery_records = max_delivery_records.copy()
            max_delivery_records['_completeness_score'] = 0
            
            for col in price_cols:
                if col in max_delivery_records.columns:
                    # Count non-null, non-zero values
                    max_delivery_records['_completeness_score'] += ((max_delivery_records[col].notna()) & (max_delivery_records[col] != 0)).astype(int)
            
            # Select the record with highest completeness score
            best_idx = max_delivery_records['_completeness_score'].idxmax()
            best_record = max_delivery_records.loc[best_idx]
        else:
            best_record = max_delivery_records.iloc[0]
            
        delivery_unique_list.append(best_record)
    
    delivery_unique = pd.DataFrame(delivery_unique_list).reset_index(drop=True) if delivery_unique_list else pd.DataFrame()
    if not delivery_unique.empty:
        print(f"📦 Delivery analysis: {len(delivery_unique)} unique symbols")
    
    # Create output filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"NSE_JUNE2025_data_Unique_Symbol_Analysis_{timestamp}.xlsx"
    
    print(f"💾 Creating unique analysis: {filename}")
    
    # Create Excel file with multiple sheets
    try:
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            
            # Sheet 1: Unique Volume Analysis
            if not volume_unique.empty:
                print("📈 Creating unique TTL_TRD_QNTY sheet...")
                
                # Remove helper columns and sort by volume
                vol_output = volume_unique.drop(columns=['_volume_value', '_delivery_value', '_symbol_value', '_completeness_score'], errors='ignore')
                vol_output = vol_output.sort_values('TTL_TRD_QNTY', ascending=False)
                
                vol_output.to_excel(writer, sheet_name='Unique_TTL_TRD_QNTY', index=False)
                print(f"   ✅ {len(vol_output)} unique symbols by volume (all columns included: {len(vol_output.columns)} columns)")
            
            # Sheet 2: Unique Delivery Analysis
            if not delivery_unique.empty:
                print("📦 Creating unique DELIV_QTY sheet...")
                
                # Remove helper columns and sort by delivery
                del_output = delivery_unique.drop(columns=['_volume_value', '_delivery_value', '_symbol_value', '_completeness_score'], errors='ignore')
                del_output = del_output.sort_values('DELIV_QTY', ascending=False)
                
                del_output.to_excel(writer, sheet_name='Unique_DELIV_QTY', index=False)
                print(f"   ✅ {len(del_output)} unique symbols by delivery (all columns included: {len(del_output.columns)} columns)")
            
            # Sheet 3: Summary
            print("📋 Creating summary sheet...")
            
            summary_data = []
            
            # Analysis summary
            summary_data.append(['📊 JUNE 2025 ANALYSIS SUMMARY', ''])
            summary_data.append(['', ''])
            summary_data.append(['📁 CSV Files Processed', len(csv_files)])
            summary_data.append(['📊 Total Records Before Dedup', f"{len(combined_df):,}"])
            summary_data.append(['🏢 Total Unique Symbols', combined_df['SYMBOL'].nunique()])
            summary_data.append(['📈 Volume Analysis Symbols', len(volume_unique) if not volume_unique.empty else 0])
            summary_data.append(['📦 Delivery Analysis Symbols', len(delivery_unique) if not delivery_unique.empty else 0])
            summary_data.append(['', ''])
            
            # Top volume performer
            if not volume_unique.empty:
                top_vol = volume_unique.iloc[0]
                vol_value = top_vol.get('_volume_value', 0)
                vol_crores = vol_value / 10000000 if vol_value > 0 else 0
                symbol_value = top_vol.get('_symbol_value', 'N/A')
                date_value = top_vol.get('DATE', 'N/A')
                
                summary_data.append(['🥇 TOP VOLUME PERFORMER', ''])
                summary_data.append(['Symbol', symbol_value])
                summary_data.append(['Date', date_value])
                summary_data.append(['Volume (Crores)', f"{vol_crores:.1f}"])
                summary_data.append(['', ''])
            
            # Top delivery performer  
            if not delivery_unique.empty:
                top_del = delivery_unique.iloc[0]
                del_value = top_del.get('_delivery_value', 0) 
                del_crores = del_value / 10000000 if del_value > 0 else 0
                symbol_value = top_del.get('_symbol_value', 'N/A')
                date_value = top_del.get('DATE', 'N/A')
                
                summary_data.append(['🥇 TOP DELIVERY PERFORMER', ''])
                summary_data.append(['Symbol', symbol_value])
                summary_data.append(['Date', date_value])
                summary_data.append(['Delivery (Crores)', f"{del_crores:.1f}"])
                summary_data.append(['', ''])
            
            summary_data.append(['⏰ Generated On', datetime.now().strftime("%Y-%m-%d %H:%M:%S")])
            
            summary_df = pd.DataFrame(summary_data, columns=['Metric', 'Value'])
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
            
            # Sheet 4: Top 50 Combined (Top 25 Volume + Top 25 Delivery)
            print("🏆 Creating top performers sheet...")
            
            top_combined = []
            
            # Top 25 by volume
            if not volume_unique.empty:
                top_25_vol = volume_unique.head(25)
                for idx, row in top_25_vol.iterrows():
                    vol_value = row.get('_volume_value', 0)
                    vol_crores = vol_value / 10000000 if vol_value > 0 else 0
                    symbol_value = row.get('_symbol_value', 'N/A')
                    date_value = row.get('DATE', 'N/A')
                    
                    top_combined.append({
                        'Rank': f"V{len(top_combined)+1}",
                        'Type': 'Volume',
                        'Symbol': symbol_value,
                        'Date': date_value,
                        'Value_Crores': vol_crores,
                        'Metric': 'TTL_TRD_QNTY'
                    })
            
            # Top 25 by delivery
            if not delivery_unique.empty:
                top_25_del = delivery_unique.head(25)
                for idx, row in top_25_del.iterrows():
                    del_value = row.get('_delivery_value', 0)
                    del_crores = del_value / 10000000 if del_value > 0 else 0
                    symbol_value = row.get('_symbol_value', 'N/A')
                    date_value = row.get('DATE', 'N/A')
                    
                    top_combined.append({
                        'Rank': f"D{len([x for x in top_combined if x['Type'] == 'Delivery'])+1}",
                        'Type': 'Delivery', 
                        'Symbol': symbol_value,
                        'Date': date_value,
                        'Value_Crores': del_crores,
                        'Metric': 'DELIV_QTY'
                    })
            
            if top_combined:
                top_df = pd.DataFrame(top_combined)
                top_df.to_excel(writer, sheet_name='Top_50_Combined', index=False)
        
        print("\n🎉 UNIQUE SYMBOL ANALYSIS COMPLETE!")
        print("=" * 60)
        print(f"✅ Output file: {filename}")
        
        print(f"\n📋 Excel contains 4 sheets:")
        print(f"   1. Unique_TTL_TRD_QNTY - ONE record per symbol (highest volume)")
        print(f"   2. Unique_DELIV_QTY    - ONE record per symbol (highest delivery)")
        print(f"   3. Summary             - Analysis overview and top performers")
        print(f"   4. Top_50_Combined     - Top 25 volume + Top 25 delivery")
        
        print(f"\n📊 Final Results:")
        print(f"   📁 {len(csv_files)} CSV files processed")
        print(f"   🔄 {len(combined_df):,} total records before deduplication")
        print(f"   ✨ {combined_df['SYMBOL'].nunique():,} unique EQ symbols found")
        print(f"   📈 No duplicate symbols - each appears only once!")
        
        if not volume_unique.empty:
            top_vol = volume_unique.iloc[0]
            vol_value = top_vol.get('_volume_value', 0)
            vol_crores = vol_value / 10000000 if vol_value > 0 else 0
            symbol_value = top_vol.get('_symbol_value', 'N/A')
            date_value = top_vol.get('DATE', 'N/A')
            print(f"   🥇 Top Volume: {symbol_value} - {vol_crores:.1f} Cr on {date_value}")
        
        if not delivery_unique.empty:
            top_del = delivery_unique.iloc[0]
            del_value = top_del.get('_delivery_value', 0) 
            del_crores = del_value / 10000000 if del_value > 0 else 0
            symbol_value = top_del.get('_symbol_value', 'N/A')
            date_value = top_del.get('DATE', 'N/A')
            print(f"   🥇 Top Delivery: {symbol_value} - {del_crores:.1f} Cr on {date_value}")
    
    except Exception as e:
        print(f"❌ Error creating Excel file: {str(e)}")

if __name__ == "__main__":
    create_unique_symbol_analysis_june()
