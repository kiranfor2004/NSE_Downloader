#!/usr/bin/env python3
"""
🔍 January 2025 vs February 2025 NSE Data Comparison
Compare each symbol from January analysis with February daily data
Find dates when February values exceeded January peak values
Output format matches the attached Excel structure
"""

import pandas as pd
import glob
import os
from datetime import datetime

def january_february_comparison():
    """Compare January 2025 analysis symbols with February 2025 daily data"""
    
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
        print(f"💡 Looking for files matching: {feb_csv_pattern}")
        return
    
    print(f"📁 Found {len(feb_csv_files)} February 2025 CSV files")
    
    # Read and combine all February data
    print("🔄 Loading February 2025 daily data...")
    feb_all_data = []
    
    for file in sorted(feb_csv_files):
        try:
            df = pd.read_csv(file)
            # Clean column names and data
            df.columns = df.columns.str.strip()
            df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
            
            # Filter for EQ series only
            if 'SERIES' in df.columns:
                df = df[df['SERIES'] == 'EQ'].copy()
                if len(df) > 0:
                    # Standardize column names - handle DATE1 vs DATE
                    if 'DATE1' in df.columns and 'DATE' not in df.columns:
                        df['DATE'] = df['DATE1']
                    # Ensure numeric columns
                    df['TTL_TRD_QNTY'] = pd.to_numeric(df['TTL_TRD_QNTY'], errors='coerce')
                    df['DELIV_QTY'] = pd.to_numeric(df['DELIV_QTY'], errors='coerce')
                    feb_all_data.append(df)
                    
        except Exception as e:
            print(f"   ❌ Error reading {os.path.basename(file)}: {str(e)[:50]}")
            continue
    
    if not feb_all_data:
        print("❌ No valid February data found!")
        return
    
    # Combine all February data
    feb_combined = pd.concat(feb_all_data, ignore_index=True)
    print(f"   ✅ Combined February data: {len(feb_combined):,} records")
    print(f"   🏢 Unique symbols in February: {feb_combined['SYMBOL'].nunique():,}")
    
    # Process Volume Comparison
    print("\n📈 VOLUME COMPARISON ANALYSIS")
    print("-" * 40)
    
    volume_comparison_results = []
    
    for idx, jan_row in jan_volume_df.iterrows():
        symbol = jan_row['SYMBOL']
        jan_volume = jan_row['TTL_TRD_QNTY']
        jan_date = jan_row['DATE']
        
        # Find all February records for this symbol
        feb_symbol_data = feb_combined[feb_combined['SYMBOL'] == symbol].copy()
        
        if len(feb_symbol_data) == 0:
            # Symbol not found in February
            volume_comparison_results.append({
                'SYMBOL': symbol,
                'JANUARY_VOLUME': jan_volume,
                'JANUARY_DATE': jan_date,
                'FEBRUARY_VOLUME': 'N/A',
                'FEBRUARY_DATE': 'N/A',
                'VOLUME_INCREASE': 'N/A',
                'INCREASE_PERCENTAGE': 'N/A',
                'TIMES_HIGHER': 'N/A',
                'COMPARISON': f'January: {jan_volume:,} (Symbol not in February)'
            })
            continue
        
        # Find the highest February volume for this symbol
        feb_symbol_data = feb_symbol_data.dropna(subset=['TTL_TRD_QNTY'])
        if len(feb_symbol_data) == 0:
            volume_comparison_results.append({
                'SYMBOL': symbol,
                'JANUARY_VOLUME': jan_volume,
                'JANUARY_DATE': jan_date,
                'FEBRUARY_VOLUME': 'N/A',
                'FEBRUARY_DATE': 'N/A',
                'VOLUME_INCREASE': 'N/A',
                'INCREASE_PERCENTAGE': 'N/A',
                'TIMES_HIGHER': 'N/A',
                'COMPARISON': f'January: {jan_volume:,} (No valid February data)'
            })
            continue
        
        # Get the maximum February volume
        max_feb_idx = feb_symbol_data['TTL_TRD_QNTY'].idxmax()
        max_feb_row = feb_symbol_data.loc[max_feb_idx]
        feb_volume = max_feb_row['TTL_TRD_QNTY']
        feb_date = max_feb_row['DATE']
        
        # Calculate comparison metrics - ONLY INCLUDE INCREASES
        if feb_volume > jan_volume and jan_volume > 0:
            volume_increase = feb_volume - jan_volume
            increase_percentage = ((feb_volume - jan_volume) / jan_volume) * 100
            times_higher = feb_volume / jan_volume
            comparison = f'January: {jan_volume:,} → February: {feb_volume:,} ({increase_percentage:.1f}% ↗)'
            
            # Only add to results if there's an actual increase
            volume_comparison_results.append({
                'SYMBOL': symbol,
                'JANUARY_VOLUME': jan_volume,
                'JANUARY_DATE': jan_date,
                'FEBRUARY_VOLUME': feb_volume,
                'FEBRUARY_DATE': feb_date,
                'VOLUME_INCREASE': volume_increase,
                'INCREASE_PERCENTAGE': f'{increase_percentage:.1f}%',
                'TIMES_HIGHER': f'{times_higher:.1f}x',
                'COMPARISON': comparison
            })
        # Skip symbols with no increase or decrease
    
    # Process Delivery Comparison
    print("📦 DELIVERY COMPARISON ANALYSIS")
    print("-" * 40)
    
    delivery_comparison_results = []
    
    for idx, jan_row in jan_delivery_df.iterrows():
        symbol = jan_row['SYMBOL']
        jan_delivery = jan_row['DELIV_QTY']
        jan_date = jan_row['DATE']
        
        # Find all February records for this symbol
        feb_symbol_data = feb_combined[feb_combined['SYMBOL'] == symbol].copy()
        
        if len(feb_symbol_data) == 0:
            # Symbol not found in February
            delivery_comparison_results.append({
                'SYMBOL': symbol,
                'JANUARY_DELIVERY': jan_delivery,
                'JANUARY_DATE': jan_date,
                'FEBRUARY_DELIVERY': 'N/A',
                'FEBRUARY_DATE': 'N/A',
                'DELIVERY_INCREASE': 'N/A',
                'INCREASE_PERCENTAGE': 'N/A',
                'TIMES_HIGHER': 'N/A',
                'COMPARISON': f'January: {jan_delivery:,} (Symbol not in February)'
            })
            continue
        
        # Find the highest February delivery for this symbol
        feb_symbol_data = feb_symbol_data.dropna(subset=['DELIV_QTY'])
        if len(feb_symbol_data) == 0:
            delivery_comparison_results.append({
                'SYMBOL': symbol,
                'JANUARY_DELIVERY': jan_delivery,
                'JANUARY_DATE': jan_date,
                'FEBRUARY_DELIVERY': 'N/A',
                'FEBRUARY_DATE': 'N/A',
                'DELIVERY_INCREASE': 'N/A',
                'INCREASE_PERCENTAGE': 'N/A',
                'TIMES_HIGHER': 'N/A',
                'COMPARISON': f'January: {jan_delivery:,} (No valid February data)'
            })
            continue
        
        # Get the maximum February delivery
        max_feb_idx = feb_symbol_data['DELIV_QTY'].idxmax()
        max_feb_row = feb_symbol_data.loc[max_feb_idx]
        feb_delivery = max_feb_row['DELIV_QTY']
        feb_date = max_feb_row['DATE']
        
        # Calculate comparison metrics - ONLY INCLUDE INCREASES
        if feb_delivery > jan_delivery and jan_delivery > 0:
            delivery_increase = feb_delivery - jan_delivery
            increase_percentage = ((feb_delivery - jan_delivery) / jan_delivery) * 100
            times_higher = feb_delivery / jan_delivery
            comparison = f'January: {jan_delivery:,} → February: {feb_delivery:,} ({increase_percentage:.1f}% ↗)'
            
            # Only add to results if there's an actual increase
            delivery_comparison_results.append({
                'SYMBOL': symbol,
                'JANUARY_DELIVERY': jan_delivery,
                'JANUARY_DATE': jan_date,
                'FEBRUARY_DELIVERY': feb_delivery,
                'FEBRUARY_DATE': feb_date,
                'DELIVERY_INCREASE': delivery_increase,
                'INCREASE_PERCENTAGE': f'{increase_percentage:.1f}%',
                'TIMES_HIGHER': f'{times_higher:.1f}x',
                'COMPARISON': comparison
            })
        # Skip symbols with no increase or decrease
    
    print(f"📊 Analysis completed:")
    print(f"   📈 Volume increases found: {len(volume_comparison_results)}")
    print(f"   📦 Delivery increases found: {len(delivery_comparison_results)}")
    
    if len(volume_comparison_results) == 0 and len(delivery_comparison_results) == 0:
        print("❌ No increases found! All February values were lower than or equal to January values.")
        return
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"January_February_Comparison_{timestamp}.xlsx"
    
    print(f"\n💾 Creating comparison Excel: {output_filename}")
    
    try:
        with pd.ExcelWriter(output_filename, engine='openpyxl') as writer:
            
            # Always create at least one sheet
            sheet_created = False
            
            # Volume Comparison Sheet
            if len(volume_comparison_results) > 0:
                volume_df = pd.DataFrame(volume_comparison_results)
                # Sort by volume increase (highest first)
                volume_df = volume_df.sort_values('VOLUME_INCREASE', ascending=False)
                volume_df.to_excel(writer, sheet_name='Volume_Comparison', index=False)
                print(f"   ✅ Volume Comparison: {len(volume_df)} symbols")
                sheet_created = True
            
            # Delivery Comparison Sheet
            if len(delivery_comparison_results) > 0:
                delivery_df = pd.DataFrame(delivery_comparison_results)
                # Sort by delivery increase (highest first)
                delivery_df = delivery_df.sort_values('DELIVERY_INCREASE', ascending=False)
                delivery_df.to_excel(writer, sheet_name='Delivery_Comparison', index=False)
                print(f"   ✅ Delivery Comparison: {len(delivery_df)} symbols")
                sheet_created = True
            
            # If no data sheets created, create a summary sheet
            if not sheet_created:
                summary_df = pd.DataFrame({
                    'Result': ['No increases found'],
                    'Explanation': ['All February values were lower than or equal to January peak values']
                })
                summary_df.to_excel(writer, sheet_name='Summary', index=False)
                print(f"   ✅ Summary sheet created (no increases found)")
        
        print("\n🎉 COMPARISON ANALYSIS COMPLETE!")
        print("=" * 60)
        print(f"✅ Output file: {output_filename}")
        print(f"\n📋 Excel contains 2 sheets (ONLY INCREASED VALUES):")
        print(f"   1. Volume_Comparison   - Symbols with higher TTL_TRD_QNTY in February")
        print(f"   2. Delivery_Comparison - Symbols with higher DELIV_QTY in February")
        
        # Show summary statistics
        print(f"\n📊 Summary:")
        print(f"   📈 Volume: {len(volume_comparison_results)} symbols showed increase in February")
        print(f"   📦 Delivery: {len(delivery_comparison_results)} symbols showed increase in February")
        
        if volume_comparison_results:
            top_volume_gainer = max(volume_comparison_results, key=lambda x: x['VOLUME_INCREASE'])
            print(f"   🥇 Top Volume Gainer: {top_volume_gainer['SYMBOL']} (+{top_volume_gainer['VOLUME_INCREASE']:,})")
        
        if delivery_comparison_results:
            top_delivery_gainer = max(delivery_comparison_results, key=lambda x: x['DELIVERY_INCREASE'])
            print(f"   🥇 Top Delivery Gainer: {top_delivery_gainer['SYMBOL']} (+{top_delivery_gainer['DELIVERY_INCREASE']:,})")
    
    except Exception as e:
        print(f"❌ Error creating Excel file: {str(e)}")

if __name__ == "__main__":
    january_february_comparison()
