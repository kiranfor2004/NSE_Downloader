#!/usr/bin/env python3
"""
NSE January 2025 Unique Symbol Analysis
Analyzes January 2025 NSE data to find dates with highest TTL_TRD_QNTY and DELIV_QTY
for SERIES=EQ stocks, similar to July 2025 analysis.
"""

import os
import pandas as pd
from datetime import datetime
import glob

def load_january_data():
    """Load all January 2025 NSE data files"""
    print("Loading January 2025 NSE data...")
    
    data_folder = "NSE_January_2025_Data"
    csv_files = glob.glob(os.path.join(data_folder, "cm*.csv"))
    
    all_data = []
    
    for file_path in sorted(csv_files):
        try:
            print(f"Processing: {os.path.basename(file_path)}")
            df = pd.read_csv(file_path)
            
            # Clean column names (remove extra spaces)
            df.columns = df.columns.str.strip()
            
            # Filter for EQ series only
            df_eq = df[df['SERIES'].str.strip() == 'EQ'].copy()
            
            if len(df_eq) > 0:
                all_data.append(df_eq)
                print(f"  Added {len(df_eq)} EQ records")
            else:
                print(f"  No EQ records found")
                
        except Exception as e:
            print(f"Error processing {file_path}: {str(e)}")
    
    if all_data:
        combined_data = pd.concat(all_data, ignore_index=True)
        print(f"\nTotal EQ records loaded: {len(combined_data)}")
        return combined_data
    else:
        print("No data loaded!")
        return pd.DataFrame()

def analyze_volume_data(df):
    """Analyze trading volume data to find dates with highest TTL_TRD_QNTY for each symbol"""
    print("\n" + "="*60)
    print("ANALYZING TRADING VOLUME (TTL_TRD_QNTY)")
    print("="*60)
    
    # Convert TTL_TRD_QNTY to numeric, handling any non-numeric values
    df['TTL_TRD_QNTY'] = pd.to_numeric(df['TTL_TRD_QNTY'], errors='coerce')
    
    # Group by SYMBOL and find the date with maximum TTL_TRD_QNTY for each symbol
    volume_winners = []
    
    for symbol in df['SYMBOL'].unique():
        symbol_data = df[df['SYMBOL'] == symbol].copy()
        
        # Find the row with maximum TTL_TRD_QNTY for this symbol
        max_volume_idx = symbol_data['TTL_TRD_QNTY'].idxmax()
        if pd.notna(max_volume_idx):
            winner_row = df.loc[max_volume_idx]
            volume_winners.append({
                'SYMBOL': winner_row['SYMBOL'],
                'WINNING_DATE': winner_row['DATE1'],
                'MAX_TTL_TRD_QNTY': winner_row['TTL_TRD_QNTY'],
                'CLOSE_PRICE': winner_row['CLOSE_PRICE'],
                'HIGH_PRICE': winner_row['HIGH_PRICE'],
                'LOW_PRICE': winner_row['LOW_PRICE'],
                'TURNOVER_LACS': winner_row['TURNOVER_LACS']
            })
    
    volume_df = pd.DataFrame(volume_winners)
    
    if len(volume_df) > 0:
        # Sort by volume descending
        volume_df = volume_df.sort_values('MAX_TTL_TRD_QNTY', ascending=False)
        
        print(f"Found {len(volume_df)} unique symbols with volume winners")
        print("\nTop 20 Volume Winners:")
        print(volume_df.head(20).to_string(index=False))
        
        # Date-wise analysis
        date_volume_counts = volume_df['WINNING_DATE'].value_counts().sort_values(ascending=False)
        print(f"\nDate-wise Volume Winners:")
        print(date_volume_counts.head(10))
        
    return volume_df

def analyze_delivery_data(df):
    """Analyze delivery data to find dates with highest DELIV_QTY for each symbol"""
    print("\n" + "="*60)
    print("ANALYZING DELIVERY QUANTITY (DELIV_QTY)")
    print("="*60)
    
    # Convert DELIV_QTY to numeric, handling any non-numeric values and '-' entries
    df['DELIV_QTY'] = df['DELIV_QTY'].replace('-', 0)
    df['DELIV_QTY'] = pd.to_numeric(df['DELIV_QTY'], errors='coerce')
    
    # Filter out records where DELIV_QTY is 0 or NaN
    df_delivery = df[df['DELIV_QTY'] > 0].copy()
    
    print(f"Records with valid delivery data: {len(df_delivery)}")
    
    # Group by SYMBOL and find the date with maximum DELIV_QTY for each symbol
    delivery_winners = []
    
    for symbol in df_delivery['SYMBOL'].unique():
        symbol_data = df_delivery[df_delivery['SYMBOL'] == symbol].copy()
        
        # Find the row with maximum DELIV_QTY for this symbol
        max_delivery_idx = symbol_data['DELIV_QTY'].idxmax()
        if pd.notna(max_delivery_idx):
            winner_row = df_delivery.loc[max_delivery_idx]
            delivery_winners.append({
                'SYMBOL': winner_row['SYMBOL'],
                'WINNING_DATE': winner_row['DATE1'],
                'MAX_DELIV_QTY': winner_row['DELIV_QTY'],
                'DELIV_PER': winner_row['DELIV_PER'],
                'CLOSE_PRICE': winner_row['CLOSE_PRICE'],
                'HIGH_PRICE': winner_row['HIGH_PRICE'],
                'LOW_PRICE': winner_row['LOW_PRICE'],
                'TTL_TRD_QNTY': winner_row['TTL_TRD_QNTY']
            })
    
    delivery_df = pd.DataFrame(delivery_winners)
    
    if len(delivery_df) > 0:
        # Sort by delivery quantity descending
        delivery_df = delivery_df.sort_values('MAX_DELIV_QTY', ascending=False)
        
        print(f"Found {len(delivery_df)} unique symbols with delivery winners")
        print("\nTop 20 Delivery Winners:")
        print(delivery_df.head(20).to_string(index=False))
        
        # Date-wise analysis
        date_delivery_counts = delivery_df['WINNING_DATE'].value_counts().sort_values(ascending=False)
        print(f"\nDate-wise Delivery Winners:")
        print(date_delivery_counts.head(10))
        
    return delivery_df

def save_results_to_excel(volume_df, delivery_df):
    """Save results to Excel file"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"NSE_JANUARY2025_data_Unique_Symbol_Analysis_{timestamp}.xlsx"
    
    try:
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            # Volume Winners Sheet
            if len(volume_df) > 0:
                volume_df.to_excel(writer, sheet_name='Volume_Winners', index=False)
                
                # Add summary to volume sheet
                volume_summary = pd.DataFrame({
                    'Metric': ['Total Unique Symbols', 'Highest Volume', 'Average Volume', 'Date Range'],
                    'Value': [
                        len(volume_df),
                        f"{volume_df['MAX_TTL_TRD_QNTY'].max():,.0f}",
                        f"{volume_df['MAX_TTL_TRD_QNTY'].mean():,.0f}",
                        'January 2025'
                    ]
                })
                volume_summary.to_excel(writer, sheet_name='Volume_Summary', index=False)
            
            # Delivery Winners Sheet
            if len(delivery_df) > 0:
                delivery_df.to_excel(writer, sheet_name='Delivery_Winners', index=False)
                
                # Add summary to delivery sheet
                delivery_summary = pd.DataFrame({
                    'Metric': ['Total Unique Symbols', 'Highest Delivery', 'Average Delivery', 'Date Range'],
                    'Value': [
                        len(delivery_df),
                        f"{delivery_df['MAX_DELIV_QTY'].max():,.0f}",
                        f"{delivery_df['MAX_DELIV_QTY'].mean():,.0f}",
                        'January 2025'
                    ]
                })
                delivery_summary.to_excel(writer, sheet_name='Delivery_Summary', index=False)
            
            print(f"\n✅ Results saved to: {filename}")
            
    except Exception as e:
        print(f"Error saving to Excel: {str(e)}")

def main():
    """Main function to run January 2025 analysis"""
    print("=" * 80)
    print("NSE JANUARY 2025 UNIQUE SYMBOL ANALYSIS")
    print("Finding dates with highest TTL_TRD_QNTY and DELIV_QTY for EQ series stocks")
    print("=" * 80)
    
    # Load data
    df = load_january_data()
    
    if df.empty:
        print("No data to analyze!")
        return
    
    print(f"\nDataset Info:")
    print(f"- Total EQ records: {len(df):,}")
    print(f"- Unique symbols: {df['SYMBOL'].nunique():,}")
    print(f"- Date range: {df['DATE1'].min()} to {df['DATE1'].max()}")
    print(f"- Unique dates: {df['DATE1'].nunique()}")
    
    # Analyze volume data
    volume_df = analyze_volume_data(df)
    
    # Analyze delivery data
    delivery_df = analyze_delivery_data(df)
    
    # Save results
    save_results_to_excel(volume_df, delivery_df)
    
    print("\n" + "=" * 80)
    print("JANUARY 2025 ANALYSIS COMPLETED!")
    print("=" * 80)
    print(f"Volume Winners: {len(volume_df)} unique symbols")
    print(f"Delivery Winners: {len(delivery_df)} unique symbols")
    print("\nThis analysis identifies the single best trading date for each stock")
    print("in January 2025, similar to the July 2025 analysis approach.")
    print("=" * 80)

if __name__ == "__main__":
    main()
