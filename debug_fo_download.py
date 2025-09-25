#!/usr/bin/env python3
"""
Debug F&O Data Download
======================

Purpose: Check what data is actually being downloaded to fix the loader
"""

import requests
import pandas as pd
import io

def test_download():
    """Test downloading F&O data for one date"""
    date_str = "03-02-2025"
    
    # Use the correct URL format for F&O data
    url = f"https://nsearchives.nseindia.com/content/nsccl/fao_participant_vol_{date_str.replace('-', '')}.csv"
    
    print(f"Testing URL: {url}")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        print(f"Response status: {response.status_code}")
        
        if response.status_code == 200:
            print(f"Response content length: {len(response.text)}")
            print(f"First 500 chars of response:")
            print(response.text[:500])
            print("\n" + "="*50)
            
            # Try to parse as CSV
            try:
                df = pd.read_csv(io.StringIO(response.text))
                print(f"DataFrame shape: {df.shape}")
                print(f"Columns: {list(df.columns)}")
                print(f"First few rows:")
                print(df.head())
                
                return df
            except Exception as e:
                print(f"CSV parsing error: {e}")
                return None
        else:
            print(f"HTTP error: {response.status_code}")
            return None
            
    except Exception as e:
        print(f"Request error: {e}")
        return None

if __name__ == "__main__":
    test_download()