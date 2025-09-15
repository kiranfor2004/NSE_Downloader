#!/usr/bin/env python3

import requests
import zipfile
import pandas as pd
import os
from datetime import datetime, timedelta
import tempfile

def find_available_udiff_files():
    """Try to find available UDiFF files by checking recent dates"""
    
    # Try dates from recent past (since future dates won't exist)
    base_date = datetime(2024, 12, 31)  # Try end of 2024
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }
    
    session = requests.Session()
    session.headers.update(headers)
    
    # Try various date formats and recent dates
    test_dates = []
    
    # Try last few days of 2024
    for i in range(10):
        test_date = base_date - timedelta(days=i)
        # Skip weekends (Saturday=5, Sunday=6)
        if test_date.weekday() < 5:  # Monday=0 to Friday=4
            test_dates.append(test_date)
    
    print("Trying to find available UDiFF files...")
    
    for test_date in test_dates:
        date_str = test_date.strftime("%Y%m%d")
        filename = f"udiff_{date_str}.zip"
        url = f"https://archives.nseindia.com/products/content/derivatives/equities/{filename}"
        
        try:
            print(f"Trying: {filename} ({test_date.strftime('%Y-%m-%d %A')})")
            response = session.head(url, timeout=10)  # Use HEAD to check if file exists
            
            if response.status_code == 200:
                print(f"✅ Found: {filename}")
                return download_and_inspect_file(url, filename, session)
            else:
                print(f"❌ Not found: {response.status_code}")
                
        except Exception as e:
            print(f"❌ Error checking {filename}: {str(e)[:50]}...")
    
    print("\nNo UDiFF files found. Let me show the expected NSE F&O UDiFF format based on documentation...")
    return show_standard_udiff_format()

def download_and_inspect_file(url, filename, session):
    """Download and inspect a found UDiFF file"""
    try:
        print(f"\nDownloading: {filename}")
        response = session.get(url, timeout=30)
        response.raise_for_status()
        
        with tempfile.NamedTemporaryFile(delete=False, suffix='.zip') as temp_file:
            temp_file.write(response.content)
            temp_zip_path = temp_file.name
        
        print(f"Downloaded successfully. File size: {len(response.content)} bytes")
        
        # Extract and examine
        with zipfile.ZipFile(temp_zip_path, 'r') as zip_ref:
            file_list = zip_ref.namelist()
            print(f"Files in ZIP: {file_list}")
            
            for file_name in file_list:
                if file_name.endswith('.csv'):
                    print(f"\nExamining: {file_name}")
                    
                    with zip_ref.open(file_name) as csv_file:
                        content = csv_file.read().decode('utf-8')
                        lines = content.split('\n')
                        
                        if lines:
                            header = lines[0]
                            print(f"\nHeader: {header}")
                            
                            columns = [col.strip() for col in header.split(',')]
                            print(f"\nTotal columns: {len(columns)}")
                            print("\nColumn structure:")
                            for i, col in enumerate(columns, 1):
                                print(f"{i:2d}. {col}")
                            
                            # Show sample data
                            if len(lines) > 1:
                                print(f"\nSample data (first 2 rows):")
                                for i in range(1, min(3, len(lines))):
                                    if lines[i].strip():
                                        values = lines[i].split(',')
                                        print(f"\nRow {i}:")
                                        for j, val in enumerate(values[:len(columns)]):
                                            print(f"  {columns[j]}: {val}")
                            
                            os.unlink(temp_zip_path)
                            return columns
        
        os.unlink(temp_zip_path)
        
    except Exception as e:
        print(f"Error processing file: {e}")
        return None

def show_standard_udiff_format():
    """Show the standard NSE F&O UDiFF format based on documentation"""
    
    print("\n" + "="*60)
    print("STANDARD NSE F&O UDiFF FORMAT (based on NSE documentation)")
    print("="*60)
    
    # Based on NSE F&O UDiFF specification
    udiff_columns = [
        'TradDt',          # Trade Date
        'BizDt',           # Business Date  
        'Sgmt',            # Segment
        'Src',             # Source
        'FinInstrmTp',     # Financial Instrument Type
        'FinInstrmActlXpryDt',  # Financial Instrument Actual Expiry Date
        'FinInstrmId',     # Financial Instrument ID
        'ISIN',            # ISIN
        'TckrSymb',        # Ticker Symbol
        'SctySrs',         # Security Series
        'XpryDt',          # Expiry Date
        'StrkPric',        # Strike Price
        'OptnTp',          # Option Type
        'FinInstrmNm',     # Financial Instrument Name
        'OpnPric',         # Open Price
        'HghPric',         # High Price
        'LwPric',          # Low Price
        'ClsPric',         # Close Price
        'LastPric',        # Last Price
        'PrvsClsgPric',    # Previous Closing Price
        'UndrlygPric',     # Underlying Price
        'SttlmPric',       # Settlement Price
        'OpnIntrst',       # Open Interest
        'ChngInOpnIntrst', # Change in Open Interest
        'TtlTrdgVol',      # Total Trading Volume
        'TtlTrfVal',       # Total Turnover Value
        'TtlNbOfTxsExctd', # Total Number of Transactions Executed
        'SsnId',           # Session ID
        'NewBrdLotQty',    # New Board Lot Quantity
        'Rmks',            # Remarks
        'Rsvd01',          # Reserved 01
        'Rsvd02',          # Reserved 02
        'Rsvd03',          # Reserved 03
        'Rsvd04'           # Reserved 04
    ]
    
    print(f"\nTotal standard UDiFF columns: {len(udiff_columns)}")
    print("\nStandard UDiFF column structure:")
    for i, col in enumerate(udiff_columns, 1):
        print(f"{i:2d}. {col}")
    
    return udiff_columns

def compare_with_current_table():
    """Compare current table with standard UDiFF format"""
    
    current_columns = [
        'id', 'trade_date', 'symbol', 'instrument', 'expiry_date', 
        'strike_price', 'option_type', 'open_price', 'high_price', 
        'low_price', 'close_price', 'settle_price', 'contracts_traded', 
        'value_in_lakh', 'open_interest', 'change_in_oi', 'underlying', 
        'source_file', 'created_at'
    ]
    
    # Get standard format
    udiff_columns = show_standard_udiff_format()
    
    print(f"\n" + "="*60)
    print("COMPARISON: CURRENT TABLE vs STANDARD UDiFF")
    print("="*60)
    
    print(f"\nCurrent table columns: {len(current_columns)}")
    print(f"Standard UDiFF columns: {len(udiff_columns)}")
    
    # Column mapping analysis
    print(f"\n" + "-"*50)
    print("COLUMN MAPPING ANALYSIS:")
    print("-"*50)
    
    mapped_columns = {
        'TradDt': 'trade_date',
        'TckrSymb': 'symbol', 
        'FinInstrmTp': 'instrument',
        'XpryDt': 'expiry_date',
        'StrkPric': 'strike_price',
        'OptnTp': 'option_type',
        'OpnPric': 'open_price',
        'HghPric': 'high_price',
        'LwPric': 'low_price',
        'ClsPric': 'close_price',
        'SttlmPric': 'settle_price',
        'TtlTrdgVol': 'contracts_traded',
        'TtlTrfVal': 'value_in_lakh',
        'OpnIntrst': 'open_interest',
        'ChngInOpnIntrst': 'change_in_oi'
    }
    
    print("Mapped columns (UDiFF -> Current):")
    for udiff_col, current_col in mapped_columns.items():
        print(f"  {udiff_col} -> {current_col}")
    
    # Missing columns
    print(f"\n" + "-"*50)
    print("MISSING COLUMNS FROM UDiFF SOURCE:")
    print("-"*50)
    
    missing_columns = []
    for col in udiff_columns:
        if col not in mapped_columns:
            missing_columns.append(col)
            print(f"  - {col}")
    
    print(f"\nTotal missing columns: {len(missing_columns)}")
    
    return missing_columns, udiff_columns

if __name__ == "__main__":
    # Try to find actual files first
    actual_columns = find_available_udiff_files()
    
    # Show comparison
    print(f"\n" + "="*60)
    compare_with_current_table()
