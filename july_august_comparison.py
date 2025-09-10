#!/usr/bin/env python3
"""
🔄 July vs August 2025 NSE Data Comparison
Compare July 2025 analysis results with August 2025 raw CSV data
- Check if symbols achieved higher volumes/deliveries in August
- Only compare EQ series stocks
- Generate comprehensive comparison report
"""

import pandas as pd
import glob
import os
from datetime import datetime

def compare_july_august_data():
    """Compare July analysis results with August raw CSV data"""
    
    print("🔄 NSE July vs August 2025 Data Comparison")
    print("=" * 60)
    
    # Load July 2025 analysis file
    july_file = "NSE_JULY2025_data_Unique_Symbol_Analysis_20250911_003120.xlsx"
    
    if not os.path.exists(july_file):
        print(f"❌ July analysis file not found: {july_file}")
        return
    
    print(f"📁 Loading July 2025 analysis: {july_file}")
    
    try:
        # Load July analysis sheets
        july_volume_df = pd.read_excel(july_file, sheet_name='Unique_TTL_TRD_QNTY')
        july_delivery_df = pd.read_excel(july_file, sheet_name='Unique_DELIV_QTY')
        
        print(f"   ✅ July Volume Analysis: {len(july_volume_df)} symbols")
        print(f"   ✅ July Delivery Analysis: {len(july_delivery_df)} symbols")
        
    except Exception as e:
        print(f"❌ Error loading July file: {str(e)}")
        return
    
    # Load August 2025 CSV files
    august_csv_pattern = "NSE_August_2025_Data/sec_bhavdata_full_*.csv"
    august_csv_files = glob.glob(august_csv_pattern)
    
    if not august_csv_files:
        print(f"❌ No August CSV files found! Looking for: {august_csv_pattern}")
        return
    
    print(f"\n📁 Found {len(august_csv_files)} August CSV files:")
    for i, file in enumerate(sorted(august_csv_files), 1):
        print(f"   {i}. {os.path.basename(file)}")
    print()
    
    # Load and combine August data
    print("📖 Loading August 2025 CSV files...")
    august_all_data = []
    
    for file in sorted(august_csv_files):
        try:
            print(f"   Processing: {os.path.basename(file)}")
            
            # Read CSV file
            df = pd.read_csv(file)
            
            # Clean column names and data (remove spaces)
            df.columns = df.columns.str.strip()
            df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
            
            # Filter for EQ series only
            if 'SERIES' in df.columns:
                df = df[df['SERIES'] == 'EQ'].copy()
                eq_count = len(df)
                print(f"     ✅ EQ series records: {eq_count}")
                
                if eq_count > 0:
                    # Ensure numeric columns
                    numeric_columns = ['TTL_TRD_QNTY', 'DELIV_QTY', 'CLOSE_PRICE', 'OPEN_PRICE', 'HIGH_PRICE', 'LOW_PRICE']
                    for col in numeric_columns:
                        if col in df.columns:
                            df[col] = pd.to_numeric(df[col], errors='coerce')
                    
                    august_all_data.append(df)
            else:
                print(f"     ❌ No SERIES column found")
                
        except Exception as e:
            print(f"     ❌ Error: {str(e)[:50]}")
            continue
    
    if not august_all_data:
        print("❌ No valid August data found!")
        return
    
    # Combine all August data
    print("\n🔄 Combining August data...")
    august_combined_df = pd.concat(august_all_data, ignore_index=True)
    print(f"📊 Total August EQ records: {len(august_combined_df):,}")
    print(f"🏢 Unique August symbols: {august_combined_df['SYMBOL'].nunique():,}")
    
    # Comparison Analysis
    print("\n🎯 Starting Comparison Analysis...")
    
    comparison_results = []
    
    # Volume Comparison
    print("📈 Comparing Volume Performance...")
    
    for idx, july_row in july_volume_df.iterrows():
        symbol = july_row['SYMBOL']
        july_volume = july_row['TTL_TRD_QNTY']
        july_date = july_row.get('DATE', 'Unknown')
        july_close = july_row.get('CLOSE_PRICE', 0)
        
        # Find this symbol in August data
        august_symbol_data = august_combined_df[august_combined_df['SYMBOL'] == symbol]
        
        if not august_symbol_data.empty:
            # Find max volume in August for this symbol
            august_max_volume = august_symbol_data['TTL_TRD_QNTY'].max()
            august_best_row = august_symbol_data.loc[august_symbol_data['TTL_TRD_QNTY'].idxmax()]
            august_date = august_best_row.get('DATE', august_best_row.get('DATE1', 'Unknown'))
            august_close = august_best_row.get('CLOSE_PRICE', 0)
            
            # Compare volumes
            volume_increase = august_max_volume - july_volume if pd.notna(august_max_volume) and pd.notna(july_volume) else 0
            volume_increase_pct = (volume_increase / july_volume * 100) if july_volume > 0 else 0
            
            comparison_results.append({
                'Symbol': symbol,
                'Metric': 'Volume',
                'July_Value': july_volume,
                'July_Date': july_date,
                'July_Close_Price': july_close,
                'August_Value': august_max_volume,
                'August_Date': august_date,
                'August_Close_Price': august_close,
                'Difference': volume_increase,
                'Percentage_Change': volume_increase_pct,
                'Better_Month': 'August' if volume_increase > 0 else 'July' if volume_increase < 0 else 'Same'
            })
        else:
            # Symbol not found in August
            comparison_results.append({
                'Symbol': symbol,
                'Metric': 'Volume',
                'July_Value': july_volume,
                'July_Date': july_date,
                'July_Close_Price': july_close,
                'August_Value': 0,
                'August_Date': 'Not Found',
                'August_Close_Price': 0,
                'Difference': -july_volume,
                'Percentage_Change': -100,
                'Better_Month': 'July'
            })
    
    # Delivery Comparison
    print("📦 Comparing Delivery Performance...")
    
    for idx, july_row in july_delivery_df.iterrows():
        symbol = july_row['SYMBOL']
        july_delivery = july_row['DELIV_QTY']
        july_date = july_row.get('DATE', 'Unknown')
        july_close = july_row.get('CLOSE_PRICE', 0)
        
        # Find this symbol in August data
        august_symbol_data = august_combined_df[august_combined_df['SYMBOL'] == symbol]
        
        if not august_symbol_data.empty:
            # Find max delivery in August for this symbol
            august_max_delivery = august_symbol_data['DELIV_QTY'].max()
            august_best_row = august_symbol_data.loc[august_symbol_data['DELIV_QTY'].idxmax()]
            august_date = august_best_row.get('DATE', august_best_row.get('DATE1', 'Unknown'))
            august_close = august_best_row.get('CLOSE_PRICE', 0)
            
            # Compare deliveries
            delivery_increase = august_max_delivery - july_delivery if pd.notna(august_max_delivery) and pd.notna(july_delivery) else 0
            delivery_increase_pct = (delivery_increase / july_delivery * 100) if july_delivery > 0 else 0
            
            comparison_results.append({
                'Symbol': symbol,
                'Metric': 'Delivery',
                'July_Value': july_delivery,
                'July_Date': july_date,
                'July_Close_Price': july_close,
                'August_Value': august_max_delivery,
                'August_Date': august_date,
                'August_Close_Price': august_close,
                'Difference': delivery_increase,
                'Percentage_Change': delivery_increase_pct,
                'Better_Month': 'August' if delivery_increase > 0 else 'July' if delivery_increase < 0 else 'Same'
            })
        else:
            # Symbol not found in August
            comparison_results.append({
                'Symbol': symbol,
                'Metric': 'Delivery',
                'July_Value': july_delivery,
                'July_Date': july_date,
                'July_Close_Price': july_close,
                'August_Value': 0,
                'August_Date': 'Not Found',
                'August_Close_Price': 0,
                'Difference': -july_delivery,
                'Percentage_Change': -100,
                'Better_Month': 'July'
            })
    
    # Create results DataFrame
    comparison_df = pd.DataFrame(comparison_results)
    
    # Generate output filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"NSE_July_August_Comparison_Analysis_{timestamp}.xlsx"
    
    print(f"\n💾 Creating comparison report: {output_file}")
    
    # Create Excel file with detailed analysis
    try:
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            
            # Sheet 1: All Comparisons
            print("📊 Creating comprehensive comparison sheet...")
            comparison_df.to_excel(writer, sheet_name='All_Comparisons', index=False)
            
            # Sheet 2: Volume Comparisons Only
            print("📈 Creating volume comparison sheet...")
            volume_comparisons = comparison_df[comparison_df['Metric'] == 'Volume'].copy()
            volume_comparisons = volume_comparisons.sort_values('Percentage_Change', ascending=False)
            volume_comparisons.to_excel(writer, sheet_name='Volume_Comparisons', index=False)
            
            # Sheet 3: Delivery Comparisons Only
            print("📦 Creating delivery comparison sheet...")
            delivery_comparisons = comparison_df[comparison_df['Metric'] == 'Delivery'].copy()
            delivery_comparisons = delivery_comparisons.sort_values('Percentage_Change', ascending=False)
            delivery_comparisons.to_excel(writer, sheet_name='Delivery_Comparisons', index=False)
            
            # Sheet 4: August Winners (symbols that performed better in August)
            print("🏆 Creating August winners sheet...")
            august_winners = comparison_df[comparison_df['Better_Month'] == 'August'].copy()
            august_winners = august_winners.sort_values('Percentage_Change', ascending=False)
            august_winners.to_excel(writer, sheet_name='August_Winners', index=False)
            
            # Sheet 5: July Winners (symbols that performed better in July)
            print("🥇 Creating July winners sheet...")
            july_winners = comparison_df[comparison_df['Better_Month'] == 'July'].copy()
            july_winners = july_winners.sort_values('Percentage_Change', ascending=True)  # Most negative first
            july_winners.to_excel(writer, sheet_name='July_Winners', index=False)
            
            # Sheet 6: Summary Statistics
            print("📋 Creating summary statistics...")
            
            summary_data = []
            
            # Overall statistics
            summary_data.append(['📊 JULY vs AUGUST 2025 COMPARISON SUMMARY', ''])
            summary_data.append(['', ''])
            summary_data.append(['📁 July Analysis File', july_file])
            summary_data.append(['📁 August CSV Files Processed', len(august_csv_files)])
            summary_data.append(['📊 Total Comparisons', len(comparison_df)])
            summary_data.append(['📈 Volume Comparisons', len(volume_comparisons)])
            summary_data.append(['📦 Delivery Comparisons', len(delivery_comparisons)])
            summary_data.append(['', ''])
            
            # Performance statistics
            august_better_volume = len(volume_comparisons[volume_comparisons['Better_Month'] == 'August'])
            july_better_volume = len(volume_comparisons[volume_comparisons['Better_Month'] == 'July'])
            august_better_delivery = len(delivery_comparisons[delivery_comparisons['Better_Month'] == 'August'])
            july_better_delivery = len(delivery_comparisons[delivery_comparisons['Better_Month'] == 'July'])
            
            summary_data.append(['🏆 VOLUME PERFORMANCE', ''])
            summary_data.append(['August Better', august_better_volume])
            summary_data.append(['July Better', july_better_volume])
            summary_data.append(['August Win Rate', f"{august_better_volume/len(volume_comparisons)*100:.1f}%"])
            summary_data.append(['', ''])
            
            summary_data.append(['📦 DELIVERY PERFORMANCE', ''])
            summary_data.append(['August Better', august_better_delivery])
            summary_data.append(['July Better', july_better_delivery])
            summary_data.append(['August Win Rate', f"{august_better_delivery/len(delivery_comparisons)*100:.1f}%"])
            summary_data.append(['', ''])
            
            # Top performers
            if not august_winners.empty:
                top_august_volume = august_winners[august_winners['Metric'] == 'Volume'].head(1)
                if not top_august_volume.empty:
                    row = top_august_volume.iloc[0]
                    summary_data.append(['🥇 TOP AUGUST VOLUME WINNER', ''])
                    summary_data.append(['Symbol', row['Symbol']])
                    summary_data.append(['Improvement', f"{row['Percentage_Change']:.1f}%"])
                    summary_data.append(['', ''])
                
                top_august_delivery = august_winners[august_winners['Metric'] == 'Delivery'].head(1)
                if not top_august_delivery.empty:
                    row = top_august_delivery.iloc[0]
                    summary_data.append(['📦 TOP AUGUST DELIVERY WINNER', ''])
                    summary_data.append(['Symbol', row['Symbol']])
                    summary_data.append(['Improvement', f"{row['Percentage_Change']:.1f}%"])
                    summary_data.append(['', ''])
            
            summary_data.append(['⏰ Generated On', datetime.now().strftime("%Y-%m-%d %H:%M:%S")])
            
            summary_df = pd.DataFrame(summary_data, columns=['Metric', 'Value'])
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
        
        print("\n🎉 COMPARISON ANALYSIS COMPLETE!")
        print("=" * 60)
        print(f"✅ Output file: {output_file}")
        
        print(f"\n📋 Excel contains 6 sheets:")
        print(f"   1. All_Comparisons    - Complete comparison data")
        print(f"   2. Volume_Comparisons - Volume performance comparison")
        print(f"   3. Delivery_Comparisons - Delivery performance comparison")
        print(f"   4. August_Winners     - Symbols that performed better in August")
        print(f"   5. July_Winners       - Symbols that performed better in July")
        print(f"   6. Summary            - Statistical overview")
        
        print(f"\n📊 Quick Summary:")
        print(f"   📈 Volume: {august_better_volume} symbols better in August, {july_better_volume} better in July")
        print(f"   📦 Delivery: {august_better_delivery} symbols better in August, {july_better_delivery} better in July")
        print(f"   🏆 August Win Rate: Volume {august_better_volume/len(volume_comparisons)*100:.1f}%, Delivery {august_better_delivery/len(delivery_comparisons)*100:.1f}%")
    
    except Exception as e:
        print(f"❌ Error creating Excel file: {str(e)}")

if __name__ == "__main__":
    compare_july_august_data()
