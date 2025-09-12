#!/usr/bin/env python3
"""
🔍 URL Comparison Test
Test why Step 1 (Equity) URLs work but Step 4 (F&O) URLs fail
"""

import requests
import urllib3

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def test_url(url, description):
    """Test a specific URL and report results"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=10, verify=False)
        status = response.status_code
        size = len(response.content)
        
        if status == 200 and size > 1000:
            result = f"✅ SUCCESS"
        elif status == 200 and size <= 1000:
            result = f"🟡 SMALL FILE (likely error page)"
        else:
            result = f"❌ FAILED (Status: {status})"
        
        print(f"{result}")
        print(f"   URL: {url}")
        print(f"   Status: {status}, Size: {size} bytes")
        print(f"   Description: {description}")
        print()
        
        return status == 200 and size > 1000
        
    except Exception as e:
        print(f"❌ ERROR")
        print(f"   URL: {url}")
        print(f"   Error: {str(e)}")
        print(f"   Description: {description}")
        print()
        return False

def main():
    """Test both Step 1 and Step 4 URL patterns"""
    print("🔍 NSE URL Comparison Test")
    print("=" * 80)
    print("Testing why Step 1 (Equity) works but Step 4 (F&O) fails")
    print("=" * 80)
    
    print("\n📊 STEP 1 - EQUITY URLs (KNOWN TO WORK):")
    print("-" * 50)
    
    # Test Step 1 URLs (these work)
    equity_urls = [
        ("https://archives.nseindia.com/products/content/sec_bhavdata_full_03022025.csv", 
         "Step 1: Equity data for Feb 3, 2025"),
        ("https://archives.nseindia.com/products/content/sec_bhavdata_full_01082025.csv", 
         "Step 1: Equity data for Aug 1, 2025"),
    ]
    
    equity_success = 0
    for url, desc in equity_urls:
        if test_url(url, desc):
            equity_success += 1
    
    print("\n🎯 STEP 4 - F&O URLs (KNOWN TO FAIL):")
    print("-" * 50)
    
    # Test Step 4 URLs (these fail)
    fo_urls = [
        ("https://archives.nseindia.com/content/historical/DERIVATIVES/2025/FEB/fo03FEB2025bhav.csv.zip", 
         "Step 4: F&O data for Feb 3, 2025 (Pattern 1)"),
        ("https://archives.nseindia.com/products/content/derivatives/equities/udiff_03022025.zip", 
         "Step 4: F&O UDiFF data for Feb 3, 2025 (Pattern 2)"),
        ("https://archives.nseindia.com/content/historical/DERIVATIVES/2025/AUG/fo01AUG2025bhav.csv.zip", 
         "Step 4: F&O data for Aug 1, 2025 (Pattern 1)"),
    ]
    
    fo_success = 0
    for url, desc in fo_urls:
        if test_url(url, desc):
            fo_success += 1
    
    print("📊 ANALYSIS RESULTS:")
    print("=" * 50)
    print(f"✅ Step 1 (Equity) successful URLs: {equity_success}/{len(equity_urls)}")
    print(f"❌ Step 4 (F&O) successful URLs: {fo_success}/{len(fo_urls)}")
    
    print("\n🔍 KEY DIFFERENCES IDENTIFIED:")
    print("-" * 40)
    print("1. 📁 PATH STRUCTURE:")
    print("   ✅ Equity: /products/content/sec_bhavdata_full_DDMMYYYY.csv")
    print("   ❌ F&O:    /content/historical/DERIVATIVES/YYYY/MON/...")
    print()
    print("2. 📄 FILE FORMAT:")
    print("   ✅ Equity: Direct CSV files")
    print("   ❌ F&O:    ZIP archives")
    print()
    print("3. 🌐 URL PATTERN:")
    print("   ✅ Equity: Uses /products/content/ (current structure)")
    print("   ❌ F&O:    Uses /content/historical/ (legacy structure)")
    print()
    print("💡 CONCLUSION:")
    print("   NSE moved Equity data to /products/content/ but F&O data")
    print("   location has changed or requires different access method.")

if __name__ == "__main__":
    main()
