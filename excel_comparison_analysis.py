#!/usr/bin/env python3
"""
📊 NSE Excel Files Comparison - Separate Analysis
Compare ALL Excel files to find highest TTL_TRD_QNTY and DELIV_QTY
Create separate sheets for volume and delivery analysis
Focus only on SERIES=EQ stocks
"""

import pandas as pd
import os
import glob
from datetime import datetime

def compare_all_excel_files():
    """Compare all Excel files and create separate analysis sheets"""
    
    print("📊 NSE Excel Files - Volume & Delivery Comparison")
    print("=" * 70)
    
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
    file_summary = []
    
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
                        df = pd.read_excel(file)  # Default sheet
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
            original_count = len(df)
            if 'SERIES' in df.columns:
                df = df[df['SERIES'] == 'EQ'].copy()
            elif 'series' in df.columns:
                df = df[df['series'] == 'EQ'].copy()
            # If no series column, assume all data is EQ
            
            eq_count = len(df)
            print(f"   📊 EQ series records: {eq_count} (from {original_count} total)")
            
            if eq_count == 0:
                print(f"   ⚠️  No EQ series data found in {file}")
                continue
            
            # Standardize column names
            column_mapping = {
                'SYMBOL': 'symbol',
                'DATE': 'date', 
                'TTL_TRD_QNTY': 'volume',
                'DELIV_QTY': 'delivery_qty',
                'DELIV_PER': 'delivery_percentage',
                'total_traded_qty': 'volume',
                'delivery_percentage': 'delivery_percentage',
                'volume_crores': 'volume_cr',
                'delivery_crores': 'delivery_cr'
            }
            
            # Apply column mapping
            df = df.rename(columns=column_mapping)
            
            # Ensure we have the key columns
            if 'volume' not in df.columns and 'TTL_TRD_QNTY' in df.columns:
                df['volume'] = df['TTL_TRD_QNTY']
            if 'delivery_qty' not in df.columns and 'DELIV_QTY' in df.columns:
                df['delivery_qty'] = df['DELIV_QTY']
            
            # Add source file info
            df['source_file'] = file
            
            # Calculate crores if not already present
            if 'volume_cr' not in df.columns and 'volume' in df.columns:
                df['volume_cr'] = df['volume'] / 10000000
            if 'delivery_cr' not in df.columns and 'delivery_qty' in df.columns:
                df['delivery_cr'] = df['delivery_qty'] / 10000000
            
            all_data.append(df)
            file_summary.append({
                'file': file,
                'records': eq_count,
                'symbols': df['symbol'].nunique() if 'symbol' in df.columns else 0
            })
            
        except Exception as e:
            print(f"   ❌ Error processing {file}: {e}")
    
    if not all_data:
        print("❌ No valid EQ series data found in any Excel files")
        return
    
    # Combine all data
    print("\n🔄 Combining data from all files...")
    combined_df = pd.concat(all_data, ignore_index=True)
    
    print(f"📊 Combined Analysis Summary:")
    print(f"   📁 Files processed: {len(file_summary)}")
    print(f"   📈 Total EQ records: {len(combined_df):,}")
    print(f"   🏢 Unique symbols: {combined_df['symbol'].nunique():,}")
    if 'date' in combined_df.columns:
        print(f"   📅 Date range: {combined_df['date'].min()} to {combined_df['date'].max()}")
    
    # Create output filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"NSE_Excel_Comparison_Analysis_{timestamp}.xlsx"
    
    print(f"\n💾 Creating comparison analysis: {output_file}")
    
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        
        # SHEET 1: TTL_TRD_QNTY Analysis (Volume)
        print("📈 Creating TTL_TRD_QNTY comparison sheet...")
        
        if 'volume' in combined_df.columns:
            # Sort by highest volume
            volume_analysis = combined_df.copy()
            volume_analysis = volume_analysis.sort_values('volume', ascending=False)
            
            # Select relevant columns for volume analysis
            volume_cols = ['symbol', 'date', 'volume', 'source_file']
            
            # Add calculated columns if available
            if 'volume_cr' in volume_analysis.columns:
                volume_cols.insert(3, 'volume_cr')
            if 'delivery_qty' in volume_analysis.columns:
                volume_cols.extend(['delivery_qty'])
            if 'delivery_cr' in volume_analysis.columns:
                volume_cols.extend(['delivery_cr'])
            if 'delivery_percentage' in volume_analysis.columns:
                volume_cols.extend(['delivery_percentage'])
                
            # Add price columns if available
            price_cols = ['open_price', 'high_price', 'low_price', 'close_price']
            for col in price_cols:
                if col in volume_analysis.columns:
                    volume_cols.append(col)
            
            volume_sheet = volume_analysis[volume_cols].copy()
            
            # Rename columns for clarity
            volume_sheet.columns = [col.replace('_', ' ').title() for col in volume_sheet.columns]
            
            volume_sheet.to_excel(writer, sheet_name='TTL_TRD_QNTY_Analysis', index=False)
            print(f"   ✅ Volume analysis: {len(volume_sheet)} records")
        else:
            print("   ⚠️  No volume data found")
        
        # SHEET 2: DELIV_QTY Analysis (Delivery)
        print("📦 Creating DELIV_QTY comparison sheet...")
        
        if 'delivery_qty' in combined_df.columns:
            # Sort by highest delivery quantity
            delivery_analysis = combined_df.copy()
            delivery_analysis = delivery_analysis.sort_values('delivery_qty', ascending=False)
            
            # Select relevant columns for delivery analysis
            delivery_cols = ['symbol', 'date', 'delivery_qty', 'source_file']
            
            # Add calculated columns if available
            if 'delivery_cr' in delivery_analysis.columns:
                delivery_cols.insert(3, 'delivery_cr')
            if 'delivery_percentage' in delivery_analysis.columns:
                delivery_cols.insert(-1, 'delivery_percentage')
            if 'volume' in delivery_analysis.columns:
                delivery_cols.insert(-1, 'volume')
            if 'volume_cr' in delivery_analysis.columns:
                delivery_cols.insert(-1, 'volume_cr')
                
            # Add price columns if available
            price_cols = ['open_price', 'high_price', 'low_price', 'close_price']
            for col in price_cols:
                if col in delivery_analysis.columns:
                    delivery_cols.append(col)
            
            delivery_sheet = delivery_analysis[delivery_cols].copy()
            
            # Rename columns for clarity
            delivery_sheet.columns = [col.replace('_', ' ').title() for col in delivery_sheet.columns]
            
            delivery_sheet.to_excel(writer, sheet_name='DELIV_QTY_Analysis', index=False)
            print(f"   ✅ Delivery analysis: {len(delivery_sheet)} records")
        else:
            print("   ⚠️  No delivery data found")
        
        # SHEET 3: Summary and Top Performers
        print("📋 Creating summary sheet...")
        
        summary_data = []
        
        # File processing summary
        summary_data.append(['FILES PROCESSED', '', '', ''])
        for fs in file_summary:
            summary_data.append([fs['file'], f"{fs['records']:,} records", f"{fs['symbols']:,} symbols", ''])
        
        summary_data.append(['', '', '', ''])
        summary_data.append(['OVERALL SUMMARY', '', '', ''])
        summary_data.append(['Total Files', len(file_summary), '', ''])
        summary_data.append(['Total Records', f"{len(combined_df):,}", '', ''])
        summary_data.append(['Unique Symbols', f"{combined_df['symbol'].nunique():,}", '', ''])
        
        if 'date' in combined_df.columns:
            summary_data.append(['Date Range', f"{combined_df['date'].min()} to {combined_df['date'].max()}", '', ''])
        
        # Top performers
        if 'volume' in combined_df.columns:
            top_vol = combined_df.loc[combined_df['volume'].idxmax()]
            vol_cr = top_vol.get('volume_cr', top_vol['volume'] / 10000000) if pd.notna(top_vol['volume']) else 0
            summary_data.append(['Top Volume', f"{top_vol['symbol']} - {vol_cr:.1f} Cr", top_vol.get('date', 'N/A'), top_vol['source_file']])
        
        if 'delivery_qty' in combined_df.columns:
            top_del = combined_df.loc[combined_df['delivery_qty'].idxmax()]
            del_cr = top_del.get('delivery_cr', top_del['delivery_qty'] / 10000000) if pd.notna(top_del['delivery_qty']) else 0
            summary_data.append(['Top Delivery', f"{top_del['symbol']} - {del_cr:.1f} Cr", top_del.get('date', 'N/A'), top_del['source_file']])
        
        summary_df = pd.DataFrame(summary_data, columns=['Category', 'Value', 'Date', 'Source File'])
        summary_df.to_excel(writer, sheet_name='Summary', index=False)
        
        # SHEET 4: Top 50 from each category
        print("🏆 Creating top performers sheet...")
        
        top_data = []
        
        if 'volume' in combined_df.columns:
            top_50_vol = combined_df.nlargest(50, 'volume')
            for idx, row in top_50_vol.iterrows():
                vol_cr = row.get('volume_cr', row['volume'] / 10000000) if pd.notna(row['volume']) else 0
                top_data.append([
                    'VOLUME', row['symbol'], row.get('date', 'N/A'), 
                    f"{vol_cr:.2f}", row['source_file']
                ])
        
        if 'delivery_qty' in combined_df.columns:
            top_50_del = combined_df.nlargest(50, 'delivery_qty')
            for idx, row in top_50_del.iterrows():
                del_cr = row.get('delivery_cr', row['delivery_qty'] / 10000000) if pd.notna(row['delivery_qty']) else 0
                top_data.append([
                    'DELIVERY', row['symbol'], row.get('date', 'N/A'), 
                    f"{del_cr:.2f}", row['source_file']
                ])
        
        top_df = pd.DataFrame(top_data, columns=['Type', 'Symbol', 'Date', 'Value (Cr)', 'Source File'])
        top_df.to_excel(writer, sheet_name='Top_Performers', index=False)
    
    print("\n🎉 COMPARISON ANALYSIS COMPLETE!")
    print("=" * 70)
    print(f"✅ Output file: {output_file}")
    print("\n📋 Excel contains 4 sheets:")
    print("   1. TTL_TRD_QNTY_Analysis - All EQ records sorted by trading volume")
    print("   2. DELIV_QTY_Analysis    - All EQ records sorted by delivery quantity") 
    print("   3. Summary               - Files processed and top performers")
    print("   4. Top_Performers        - Top 50 volume + Top 50 delivery combined")
    print(f"\n📊 Analysis covered:")
    print(f"   📁 {len(file_summary)} Excel files processed")
    print(f"   📈 {len(combined_df):,} EQ series records analyzed")
    print(f"   🏢 {combined_df['symbol'].nunique():,} unique symbols")
    
    if 'volume' in combined_df.columns:
        top_vol = combined_df.loc[combined_df['volume'].idxmax()]
        vol_cr = top_vol.get('volume_cr', top_vol['volume'] / 10000000)
        print(f"   🥇 Top Volume: {top_vol['symbol']} - {vol_cr:.1f} Cr on {top_vol.get('date', 'N/A')}")
    
    if 'delivery_qty' in combined_df.columns:
        top_del = combined_df.loc[combined_df['delivery_qty'].idxmax()]
        del_cr = top_del.get('delivery_cr', top_del['delivery_qty'] / 10000000)
        print(f"   🥇 Top Delivery: {top_del['symbol']} - {del_cr:.1f} Cr on {top_del.get('date', 'N/A')}")

if __name__ == "__main__":
    compare_all_excel_files()
