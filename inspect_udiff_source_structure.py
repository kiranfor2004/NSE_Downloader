#!/usr/bin/env python3

import requests
import zipfile
import pandas as pd
import os
from datetime import datetime
import tempfile

def download_and_inspect_udiff_structure():
    """Download a sample UDiFF file and inspect its structure"""
    
    # Try to download Feb 3rd UDiFF file to see actual structure
    date_str = "20250203"  # Feb 3rd 2025
    filename = f"udiff_{date_str}.zip"
    url = f"https://archives.nseindia.com/products/content/derivatives/equities/{filename}"
    
    print(f"Downloading sample UDiFF file: {filename}")
    print(f"URL: {url}")
    
    try:
        # Create headers to mimic browser request
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        
        # Download with session for better success rate
        session = requests.Session()
        session.headers.update(headers)
        
        response = session.get(url, timeout=30)
        response.raise_for_status()
        
        # Save to temporary file
        with tempfile.NamedTemporaryFile(delete=False, suffix='.zip') as temp_file:
            temp_file.write(response.content)
            temp_zip_path = temp_file.name
        
        print(f"Downloaded successfully. File size: {len(response.content)} bytes")
        
        # Extract and examine
        with zipfile.ZipFile(temp_zip_path, 'r') as zip_ref:
            file_list = zip_ref.namelist()
            print(f"\nFiles in ZIP: {file_list}")
            
            # Extract the CSV file
            for file_name in file_list:
                if file_name.endswith('.csv'):
                    print(f"\nExtracting: {file_name}")
                    
                    with zip_ref.open(file_name) as csv_file:
                        # Read just the header to see column structure
                        content = csv_file.read().decode('utf-8')
                        lines = content.split('\n')
                        
                        if lines:
                            header = lines[0]
                            print(f"\nHeader line: {header}")
                            
                            columns = header.split(',')
                            print(f"\nTotal columns in source: {len(columns)}")
                            print("\nColumn structure:")
                            for i, col in enumerate(columns, 1):
                                print(f"{i:2d}. {col.strip()}")
                            
                            # Also show a few sample data rows
                            if len(lines) > 1:
                                print(f"\nSample data rows (first 3):")
                                for i in range(1, min(4, len(lines))):
                                    if lines[i].strip():
                                        print(f"Row {i}: {lines[i]}")
                            
                            return columns
        
        # Clean up
        os.unlink(temp_zip_path)
        
    except requests.exceptions.RequestException as e:
        print(f"Error downloading file: {e}")
        return None
    except Exception as e:
        print(f"Error processing file: {e}")
        return None

def show_current_vs_source_comparison():
    """Compare current table structure with source"""
    
    current_columns = [
        'id', 'trade_date', 'symbol', 'instrument', 'expiry_date', 
        'strike_price', 'option_type', 'open_price', 'high_price', 
        'low_price', 'close_price', 'settle_price', 'contracts_traded', 
        'value_in_lakh', 'open_interest', 'change_in_oi', 'underlying', 
        'source_file', 'created_at'
    ]
    
    print("Current table columns:")
    for i, col in enumerate(current_columns, 1):
        print(f"{i:2d}. {col}")
    
    print(f"\nTotal current columns: {len(current_columns)}")
    
    # Download and inspect source
    source_columns = download_and_inspect_udiff_structure()
    
    if source_columns:
        print(f"\n{'='*60}")
        print("COMPARISON:")
        print(f"Current table columns: {len(current_columns)}")
        print(f"Source file columns: {len(source_columns)}")
        
        # Clean source column names
        source_clean = [col.strip() for col in source_columns]
        
        print("\nMissing columns in table:")
        missing_count = 0
        for col in source_clean:
            if col and col not in current_columns:
                missing_count += 1
                print(f"  - {col}")
        
        if missing_count == 0:
            print("  None - all source columns are covered")
        else:
            print(f"\nTotal missing columns: {missing_count}")

if __name__ == "__main__":
    show_current_vs_source_comparison()
