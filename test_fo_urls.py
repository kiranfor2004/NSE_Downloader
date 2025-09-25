#!/usr/bin/env python3
"""
Test Correct F&O Data URLs
==========================

Purpose: Find the correct NSE URL for daily F&O contract data
"""

import requests
import pandas as pd
import io

def test_fo_urls():
    """Test different F&O data URLs"""
    date_str = "03-02-2025"
    date_formatted = date_str.replace('-', '')
    
    # Different possible URL formats for F&O data
    urls_to_test = [
        # Option 1: Historical F&O data
        f"https://nsearchives.nseindia.com/content/historical/DERIVATIVES/{date_formatted}/fo{date_formatted}bhav.csv.zip",
        
        # Option 2: Daily F&O bhav copy
        f"https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{date_formatted}.csv",
        
        # Option 3: Direct F&O bhav
        f"https://nsearchives.nseindia.com/content/fo/BhavCopy_NSE_FO_0_0_0_{date_formatted}_F_0000.csv.zip",
        
        # Option 4: Derivatives data
        f"https://nsearchives.nseindia.com/content/historical/DERIVATIVES/{date_formatted}/DeriData_{date_formatted}.csv",
    ]
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    for i, url in enumerate(urls_to_test, 1):
        print(f"\nTesting URL {i}: {url}")
        try:
            response = requests.get(url, headers=headers, timeout=30)
            print(f"Status: {response.status_code}")
            
            if response.status_code == 200:
                print(f"Content length: {len(response.content)}")
                print(f"Content type: {response.headers.get('content-type', 'unknown')}")
                
                # Check if it's a zip file
                if 'zip' in url.lower():
                    print("ZIP file detected - would need extraction")
                else:
                    # Try to parse as CSV
                    try:
                        df = pd.read_csv(io.StringIO(response.text))
                        print(f"DataFrame shape: {df.shape}")
                        print(f"Columns: {list(df.columns)[:10]}...")  # First 10 columns
                        
                        if 'SYMBOL' in df.columns or 'Symbol' in df.columns:
                            print("✓ Found Symbol column - this looks like F&O data!")
                            print(f"Sample symbols: {df.get('SYMBOL', df.get('Symbol', 'N/A')).head().tolist()}")
                            return url, df
                    except Exception as e:
                        print(f"CSV parsing failed: {e}")
            else:
                print(f"HTTP error: {response.status_code}")
                
        except Exception as e:
            print(f"Request failed: {e}")
    
    return None, None

if __name__ == "__main__":
    working_url, sample_data = test_fo_urls()
    
    if working_url:
        print(f"\n✓ WORKING URL FOUND: {working_url}")
        if sample_data is not None:
            print(f"Sample data preview:")
            print(sample_data.head())
    else:
        print("\n✗ No working URL found")