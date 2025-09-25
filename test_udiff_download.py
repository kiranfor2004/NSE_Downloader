#!/usr/bin/env python3
"""
Test Corrected F&O Download
===========================

Purpose: Test the corrected UDiFF F&O download for one date
"""

import requests
import pandas as pd
import io
import zipfile

def test_udiff_download():
    """Test downloading UDiFF F&O data for one date"""
    date_str = "03-02-2025"
    date_formatted = date_str.replace('-', '')
    
    url = f"https://archives.nseindia.com/products/content/derivatives/equities/udiff_{date_formatted}.zip"
    print(f"Testing UDiFF URL: {url}")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Referer': 'https://www.nseindia.com/'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        print(f"Response status: {response.status_code}")
        
        if response.status_code == 200:
            print(f"Response content length: {len(response.content)}")
            
            # Process ZIP file
            try:
                with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
                    print(f"ZIP contents: {zip_file.namelist()}")
                    
                    # Get CSV file
                    csv_files = [f for f in zip_file.namelist() if f.endswith('.csv')]
                    if csv_files:
                        csv_filename = csv_files[0]
                        print(f"Processing CSV: {csv_filename}")
                        
                        with zip_file.open(csv_filename) as csv_file:
                            df = pd.read_csv(csv_file)
                            print(f"DataFrame shape: {df.shape}")
                            print(f"Columns: {list(df.columns)}")
                            
                            # Check for F&O data
                            if 'SYMBOL' in df.columns and 'INSTRUMENT' in df.columns:
                                print("\n✓ Found correct F&O columns!")
                                
                                # Show sample F&O data
                                fo_df = df[df['INSTRUMENT'].isin(['FUTSTK', 'OPTSTK', 'FUTIDX', 'OPTIDX'])]
                                print(f"F&O records: {len(fo_df)}")
                                
                                if not fo_df.empty:
                                    print(f"Sample F&O symbols: {fo_df['SYMBOL'].unique()[:10]}")
                                    print(f"Sample record:")
                                    print(fo_df.head(1).to_dict('records')[0])
                                    return True
                            else:
                                print("✗ Missing expected F&O columns")
                                print(f"Available columns: {list(df.columns)}")
                    else:
                        print("✗ No CSV files found in ZIP")
            except Exception as e:
                print(f"ZIP processing error: {e}")
        else:
            print(f"HTTP error: {response.status_code}")
            
    except Exception as e:
        print(f"Request error: {e}")
    
    return False

if __name__ == "__main__":
    success = test_udiff_download()
    print(f"\nTest result: {'SUCCESS' if success else 'FAILED'}")