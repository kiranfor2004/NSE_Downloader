#!/usr/bin/env python3
"""
📊 NSE Unique Symbol Analysis - No Duplicates
Create analysis with only ONE record per symbol for EQ series
Remove duplicates and show only the best/highest values per symbol
"""

import pandas as pd
import os
import glob
from datetime import datetime

def create_unique_symbol_analysis():
    """Create analysis with only one record per EQ symbol"""
    
    print("📊 NSE Unique Symbol Analysis - No Duplicates")
    print("=" * 60)
    
    # Find all Excel files in directory
    excel_files = glob.glob("*.xlsx")
    if not excel_files:
        print("❌ No Excel files found in current directory")
        return
    
    print(f"📁 Found {len(excel_files)} Excel files:")
    for i, file in enumerate(excel_files, 1):
        print(f"   {i}. {file}")
    print()
    
    all_data = []
    
    # Read and combine data from all Excel files
    for file in excel_files:
        try:
            print(f"📖 Processing: {file}")
            
            # Try to read different sheet names
            sheets_to_try = ['EQ_Data', 'TTL_TRD_QNTY_Analysis', 'DELIV_QTY_Analysis', None]
            df = None
            
            for sheet in sheets_to_try:
                try:
                    if sheet is None:
                        df = pd.read_excel(file)
                        sheet_name = "Default"
                    else:
                        df = pd.read_excel(file, sheet_name=sheet)
                        sheet_name = sheet
                    print(f"   ✅ Read {sheet_name} sheet: {len(df)} records")
                    break
                except:
                    continue
            
            if df is None:
                print(f"   ❌ Could not read {file}")
                continue
                
            # Filter for EQ series only
            if 'SERIES' in df.columns:
                df = df[df['SERIES'] == 'EQ'].copy()
            elif 'series' in df.columns:
                df = df[df['series'] == 'EQ'].copy()
            
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
            
            # Identify key columns for processing
            volume_col = 'TTL_TRD_QNTY' if 'TTL_TRD_QNTY' in df.columns else None
            delivery_col = 'DELIV_QTY' if 'DELIV_QTY' in df.columns else None
            symbol_col = 'SYMBOL' if 'SYMBOL' in df.columns else None
            date_col = 'DATE' if 'DATE' in df.columns else None
            
            # Store original column info for later use
            df['_volume_col'] = volume_col
            df['_delivery_col'] = delivery_col
            df['_symbol_col'] = symbol_col
            df['_date_col'] = date_col
            
            # Create internal processing columns (these won't be in final output)
            if volume_col and volume_col in df.columns:
                df['_volume_value'] = pd.to_numeric(df[volume_col], errors='coerce')
            else:
                df['_volume_value'] = 0
                
            if delivery_col and delivery_col in df.columns:
                df['_delivery_value'] = pd.to_numeric(df[delivery_col], errors='coerce')
            else:
                df['_delivery_value'] = 0
                
            if symbol_col and symbol_col in df.columns:
                df['_symbol_value'] = df[symbol_col].astype(str)
            else:
                df['_symbol_value'] = ''
                
            # Debug: Print sample data for IDEA to verify values are being read correctly
            if 'IDEA' in df['SYMBOL'].values:
                idea_sample = df[df['SYMBOL'] == 'IDEA'].head(1)
                print(f"   🔍 IDEA sample data: TTL_TRD_QNTY={idea_sample['TTL_TRD_QNTY'].values[0] if 'TTL_TRD_QNTY' in idea_sample.columns else 'N/A'}")
                print(f"                       CLOSE_PRICE={idea_sample['CLOSE_PRICE'].values[0] if 'CLOSE_PRICE' in idea_sample.columns else 'N/A'}")
                print(f"                       DATE={idea_sample['DATE'].values[0] if 'DATE' in idea_sample.columns else 'N/A'}")
            
            # Debug: Check IDEA data after column mapping
            if 'IDEA' in df['SYMBOL'].values if 'SYMBOL' in df.columns else []:
                idea_debug = df[df['SYMBOL'] == 'IDEA'].copy()
                if not idea_debug.empty:
                    print(f"   🔍 IDEA in {file} after mapping:")
                    for idx, row in idea_debug.head(2).iterrows():
                        print(f"      Row {idx}: DATE={row.get('DATE','N/A')}, TTL_TRD_QNTY={row.get('TTL_TRD_QNTY','N/A')}, CLOSE_PRICE={row.get('CLOSE_PRICE','N/A')}")
            
            all_data.append(df)
            
        except Exception as e:
            print(f"   ❌ Error processing {file}: {e}")
    
    if not all_data:
        print("❌ No valid EQ series data found")
        return
    
    # Combine all data
    print("\n🔄 Combining and deduplicating data...")
    combined_df = pd.concat(all_data, ignore_index=True)
    
    print(f"📊 Before deduplication: {len(combined_df):,} records")
    print(f"🏢 Unique symbols: {combined_df['SYMBOL'].nunique():,}")
    
    # Create unique analysis - keep only the highest volume record per symbol
    print("🎯 Creating unique symbol analysis...")
    
    # For Volume Analysis: Keep record with highest volume per symbol
    volume_unique = pd.DataFrame()
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
    
    # For Delivery Analysis: Keep record with highest delivery_qty per symbol  
    delivery_unique = pd.DataFrame()
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
    output_file = f"NSE_AUGUST2025_data_Unique_Symbol_Analysis_{timestamp}.xlsx"
    
    print(f"\n💾 Creating unique analysis: {output_file}")
    
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        
        def get_col(df, names):
            for n in names:
                if n in df.columns:
                    return n
            return None

        # Map possible column names for each required output
        symbol_names = ['SYMBOL', 'symbol']
        ttl_trd_qnty_names = ['TTL_TRD_QNTY', 'ttl_trd_qnty', 'total_traded_qty', 'volume']
        deliv_qty_names = ['DELIV_QTY', 'deliv_qty', 'delivery_qty']
        series_names = ['SERIES', 'series']
        date_names = ['DATE', 'date']

        # SHEET 1: Unique TTL_TRD_QNTY Analysis (One record per symbol - highest volume)
        if not volume_unique.empty:
            print("📈 Creating unique TTL_TRD_QNTY sheet...")
            volume_unique = volume_unique.sort_values('_volume_value', ascending=False)
            # Remove internal processing columns but keep ALL original columns
            volume_cols_to_remove = [col for col in volume_unique.columns if col.startswith('_')]
            volume_sheet = volume_unique.drop(columns=volume_cols_to_remove).copy()
            volume_sheet.to_excel(writer, sheet_name='Unique_TTL_TRD_QNTY', index=False)
            print(f"   ✅ {len(volume_sheet)} unique symbols by volume (all columns included: {len(volume_sheet.columns)} columns)")

        # SHEET 2: Unique DELIV_QTY Analysis (One record per symbol - highest delivery)
        if not delivery_unique.empty:
            print("📦 Creating unique DELIV_QTY sheet...")
            delivery_unique = delivery_unique.sort_values('_delivery_value', ascending=False)
            # Remove internal processing columns but keep ALL original columns
            delivery_cols_to_remove = [col for col in delivery_unique.columns if col.startswith('_')]
            delivery_sheet = delivery_unique.drop(columns=delivery_cols_to_remove).copy()
            delivery_sheet.to_excel(writer, sheet_name='Unique_DELIV_QTY', index=False)
            print(f"   ✅ {len(delivery_sheet)} unique symbols by delivery (all columns included: {len(delivery_sheet.columns)} columns)")
        
        # SHEET 3: Summary
        print("📋 Creating summary sheet...")
        
        summary_data = [
            ['ANALYSIS TYPE', 'UNIQUE SYMBOL ANALYSIS', '', ''],
            ['Deduplication Method', 'One record per symbol (highest value)', '', ''],
            ['', '', '', ''],
            ['RESULTS', '', '', ''],
            ['Excel Files Processed', len(excel_files), '', ''],
            ['Total Records Before Dedup', f"{len(combined_df):,}", '', ''],
            ['Unique Symbols Found', f"{combined_df['SYMBOL'].nunique():,}", '', '']
        ]
        
        if not volume_unique.empty:
            top_vol = volume_unique.iloc[0]
            vol_value = top_vol.get('_volume_value', 0)
            vol_cr = vol_value / 10000000 if pd.notna(vol_value) else 0
            symbol_value = top_vol.get('_symbol_value', 'N/A')
            date_value = top_vol.get(top_vol.get('_date_col', 'date'), 'N/A')
            summary_data.extend([
                ['', '', '', ''],
                ['VOLUME ANALYSIS', '', '', ''],
                ['Unique Symbols (Volume)', len(volume_unique), '', ''],
                ['Top Volume Symbol', symbol_value, f"{vol_cr:.1f} Cr", date_value]
            ])
        
        if not delivery_unique.empty:
            top_del = delivery_unique.iloc[0]
            del_value = top_del.get('_delivery_value', 0)
            del_cr = del_value / 10000000 if pd.notna(del_value) else 0
            symbol_value = top_del.get('_symbol_value', 'N/A')
            date_value = top_del.get(top_del.get('_date_col', 'date'), 'N/A')
            summary_data.extend([
                ['', '', '', ''],
                ['DELIVERY ANALYSIS', '', '', ''],
                ['Unique Symbols (Delivery)', len(delivery_unique), '', ''],
                ['Top Delivery Symbol', symbol_value, f"{del_cr:.1f} Cr", date_value]
            ])
        
        summary_df = pd.DataFrame(summary_data, columns=['Category', 'Value', 'Amount', 'Date'])
        summary_df.to_excel(writer, sheet_name='Summary', index=False)
        
        # SHEET 4: Top 50 Combined (Volume + Delivery leaders)
        print("🏆 Creating top performers sheet...")
        
        top_performers = []
        
        # Top 25 by volume
        if not volume_unique.empty:
            for idx, row in volume_unique.head(25).iterrows():
                vol_value = row.get('_volume_value', 0)
                vol_cr = vol_value / 10000000 if pd.notna(vol_value) else 0
                symbol_value = row.get('_symbol_value', 'N/A')
                date_value = row.get(row.get('_date_col', 'date'), 'N/A')
                top_performers.append(['VOLUME', symbol_value, date_value, f"{vol_cr:.2f}"])
        
        # Top 25 by delivery
        if not delivery_unique.empty:
            for idx, row in delivery_unique.head(25).iterrows():
                del_value = row.get('_delivery_value', 0)
                del_cr = del_value / 10000000 if pd.notna(del_value) else 0
                symbol_value = row.get('_symbol_value', 'N/A')
                date_value = row.get(row.get('_date_col', 'date'), 'N/A')
                top_performers.append(['DELIVERY', symbol_value, date_value, f"{del_cr:.2f}"])
        
        top_df = pd.DataFrame(top_performers, columns=['Type', 'Symbol', 'Date', 'Value (Cr)'])
        top_df.to_excel(writer, sheet_name='Top_50_Combined', index=False)
    
    print("\n🎉 UNIQUE SYMBOL ANALYSIS COMPLETE!")
    print("=" * 60)
    print(f"✅ Output file: {output_file}")
    print("\n📋 Excel contains 4 sheets:")
    print("   1. Unique_TTL_TRD_QNTY - ONE record per symbol (highest volume)")
    print("   2. Unique_DELIV_QTY    - ONE record per symbol (highest delivery)")
    print("   3. Summary             - Analysis overview and top performers")
    print("   4. Top_50_Combined     - Top 25 volume + Top 25 delivery")
    
    print(f"\n📊 Final Results:")
    print(f"   📁 {len(excel_files)} Excel files processed")
    print(f"   🔄 {len(combined_df):,} total records before deduplication")
    print(f"   ✨ {combined_df['SYMBOL'].nunique():,} unique EQ symbols found")
    print(f"   📈 No duplicate symbols - each appears only once!")
    
    if not volume_unique.empty:
        top_vol = volume_unique.iloc[0]
        vol_value = top_vol.get('_volume_value', 0)
        vol_cr = vol_value / 10000000 if pd.notna(vol_value) else 0
        symbol_value = top_vol.get('_symbol_value', 'N/A')
        date_value = top_vol.get(top_vol.get('_date_col', 'date'), 'N/A')
        print(f"   🥇 Top Volume: {symbol_value} - {vol_cr:.1f} Cr on {date_value}")
    
    if not delivery_unique.empty:
        top_del = delivery_unique.iloc[0]
        del_value = top_del.get('_delivery_value', 0)
        del_cr = del_value / 10000000 if pd.notna(del_value) else 0
        symbol_value = top_del.get('_symbol_value', 'N/A')
        date_value = top_del.get(top_del.get('_date_col', 'date'), 'N/A')
        print(f"   🥇 Top Delivery: {symbol_value} - {del_cr:.1f} Cr on {date_value}")

if __name__ == "__main__":
    create_unique_symbol_analysis()
