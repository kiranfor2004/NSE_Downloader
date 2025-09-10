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
            
            # Keep original column names but create standardized internal columns for processing
            # Identify volume column (keep original name)
            volume_col = None
            if 'TTL_TRD_QNTY' in df.columns:
                volume_col = 'TTL_TRD_QNTY'
            elif 'total_traded_qty' in df.columns:
                volume_col = 'total_traded_qty'
            elif 'volume' in df.columns:
                volume_col = 'volume'
            
            # Identify delivery column (keep original name)
            delivery_col = None
            if 'DELIV_QTY' in df.columns:
                delivery_col = 'DELIV_QTY'
            elif 'delivery_qty' in df.columns:
                delivery_col = 'delivery_qty'
            
            # Identify other key columns
            symbol_col = 'SYMBOL' if 'SYMBOL' in df.columns else 'symbol'
            date_col = 'DATE' if 'DATE' in df.columns else 'date'
            
            # Store original column info for later use
            df['_volume_col'] = volume_col
            df['_delivery_col'] = delivery_col
            df['_symbol_col'] = symbol_col
            df['_date_col'] = date_col
            
            # Create internal processing columns (these won't be in final output)
            if volume_col:
                df['_volume_value'] = df[volume_col]
            if delivery_col:
                df['_delivery_value'] = df[delivery_col]
            if symbol_col in df.columns:
                df['_symbol_value'] = df[symbol_col]
            
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
    print(f"🏢 Unique symbols: {combined_df['symbol'].nunique():,}")
    
    # Create unique analysis - keep only the highest volume record per symbol
    print("🎯 Creating unique symbol analysis...")
    
    # For Volume Analysis: Keep record with highest volume per symbol
    volume_unique = pd.DataFrame()
    if any('_volume_value' in df.columns for df in all_data if not df.empty):
        # Group by symbol and find highest volume record
        volume_unique = combined_df.loc[combined_df.groupby('_symbol_value')['_volume_value'].idxmax()].copy()
        print(f"📈 Volume analysis: {len(volume_unique)} unique symbols")
    
    # For Delivery Analysis: Keep record with highest delivery_qty per symbol  
    delivery_unique = pd.DataFrame()
    if any('_delivery_value' in df.columns for df in all_data if not df.empty):
        # Group by symbol and find highest delivery record
        delivery_unique = combined_df.loc[combined_df.groupby('_symbol_value')['_delivery_value'].idxmax()].copy()
        print(f"📦 Delivery analysis: {len(delivery_unique)} unique symbols")
    
    # Create output filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"NSE_Unique_Symbol_Analysis_{timestamp}.xlsx"
    
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
            volume_cols_to_remove = [col for col in volume_unique.columns if col.startswith('_')]
            volume_sheet = volume_unique.drop(columns=volume_cols_to_remove).copy()
            # For each row, extract the actual values for output columns
            output_rows = []
            for _, row in volume_sheet.iterrows():
                symbol_val = next((row[c] for c in symbol_names if c in row and pd.notna(row[c])), '')
                ttl_trd_qnty_val = next((row[c] for c in ttl_trd_qnty_names if c in row and pd.notna(row[c])), '')
                deliv_qty_val = next((row[c] for c in deliv_qty_names if c in row and pd.notna(row[c])), '')
                date_val = next((row[c] for c in date_names if c in row and pd.notna(row[c])), '')
                output_rows.append({
                    'SYMBOL': symbol_val,
                    'TTL_TRD_QNTY': ttl_trd_qnty_val,
                    'DELIV_QTY': deliv_qty_val,
                    'DATE': date_val
                })
            volume_out = pd.DataFrame(output_rows)
            volume_out.to_excel(writer, sheet_name='Unique_TTL_TRD_QNTY', index=False)
            print(f"   ✅ {len(volume_out)} unique symbols by volume (columns: ['SYMBOL', 'TTL_TRD_QNTY', 'DELIV_QTY', 'DATE'])")

        # SHEET 2: Unique DELIV_QTY Analysis (One record per symbol - highest delivery)
        if not delivery_unique.empty:
            print("📦 Creating unique DELIV_QTY sheet...")
            delivery_unique = delivery_unique.sort_values('_delivery_value', ascending=False)
            delivery_cols_to_remove = [col for col in delivery_unique.columns if col.startswith('_')]
            delivery_sheet = delivery_unique.drop(columns=delivery_cols_to_remove).copy()
            output_rows = []
            for _, row in delivery_sheet.iterrows():
                symbol_val = next((row[c] for c in symbol_names if c in row and pd.notna(row[c])), '')
                ttl_trd_qnty_val = next((row[c] for c in ttl_trd_qnty_names if c in row and pd.notna(row[c])), '')
                deliv_qty_val = next((row[c] for c in deliv_qty_names if c in row and pd.notna(row[c])), '')
                date_val = next((row[c] for c in date_names if c in row and pd.notna(row[c])), '')
                output_rows.append({
                    'SYMBOL': symbol_val,
                    'TTL_TRD_QNTY': ttl_trd_qnty_val,
                    'DELIV_QTY': deliv_qty_val,
                    'DATE': date_val
                })
            delivery_out = pd.DataFrame(output_rows)
            delivery_out.to_excel(writer, sheet_name='Unique_DELIV_QTY', index=False)
            print(f"   ✅ {len(delivery_out)} unique symbols by delivery (columns: ['SYMBOL', 'TTL_TRD_QNTY', 'DELIV_QTY', 'DATE'])")
        
        # SHEET 3: Summary
        print("📋 Creating summary sheet...")
        
        summary_data = [
            ['ANALYSIS TYPE', 'UNIQUE SYMBOL ANALYSIS', '', ''],
            ['Deduplication Method', 'One record per symbol (highest value)', '', ''],
            ['', '', '', ''],
            ['RESULTS', '', '', ''],
            ['Excel Files Processed', len(excel_files), '', ''],
            ['Total Records Before Dedup', f"{len(combined_df):,}", '', ''],
            ['Unique Symbols Found', f"{combined_df['symbol'].nunique():,}", '', '']
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
    print(f"   ✨ {combined_df['symbol'].nunique():,} unique EQ symbols found")
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
