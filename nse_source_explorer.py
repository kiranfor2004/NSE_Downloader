#!/usr/bin/env python3
"""
NSE Data Source Explorer
Tests different NSE data sources and endpoints
"""

import requests
import time
import json

def test_nse_endpoints():
    """Test various NSE endpoints to find working ones"""
    
    print("🔍 Testing NSE Data Sources")
    print("=" * 50)
    
    # Test different endpoints
    endpoints_to_test = [
        {
            "name": "NSE Historical Archives (Method 1)",
            "url": "https://archives.nseindia.com/content/historical/EQUITIES/2025/JUL/cm01072025bhav.csv.zip",
            "description": "Direct archive access"
        },
        {
            "name": "NSE Historical Archives (Method 2)", 
            "url": "https://www1.nseindia.com/content/historical/EQUITIES/2025/JUL/cm01072025bhav.csv.zip",
            "description": "www1 subdomain"
        },
        {
            "name": "NSE Main API",
            "url": "https://www.nseindia.com/api/historical/cm/equity?symbol=SBIN&series=[%22EQ%22]&from=01-07-2025&to=31-07-2025",
            "description": "New NSE API endpoint"
        },
        {
            "name": "NSE Reports Section",
            "url": "https://www.nseindia.com/products-services/equity-market-data/historical-data",
            "description": "Reports page check"
        }
    ]
    
    working_endpoints = []
    
    for endpoint in endpoints_to_test:
        print(f"\n🔎 Testing {endpoint['name']}...")
        print(f"   📝 {endpoint['description']}")
        print(f"   🔗 {endpoint['url'][:50]}...")
        
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive'
            }
            
            response = requests.get(endpoint['url'], headers=headers, timeout=15, allow_redirects=True)
            
            print(f"   📊 Status: {response.status_code}")
            print(f"   📏 Size: {len(response.content)} bytes")
            
            if response.status_code == 200:
                if len(response.content) > 1000:  # Reasonable size
                    print(f"   ✅ Working! Content detected")
                    working_endpoints.append(endpoint)
                    
                    # Try to identify content type
                    content_type = response.headers.get('content-type', 'unknown')
                    print(f"   📄 Content-Type: {content_type}")
                    
                    # Show first few characters
                    try:
                        preview = response.text[:100].replace('\n', ' ').replace('\r', ' ')
                        print(f"   👀 Preview: {preview}...")
                    except:
                        print(f"   👀 Binary content detected")
                        
                else:
                    print(f"   ⚠️  Response too small, likely error page")
            else:
                print(f"   ❌ Failed with status {response.status_code}")
                
        except Exception as e:
            print(f"   ❌ Error: {str(e)[:50]}")
        
        time.sleep(1)  # Be respectful
    
    print(f"\n📊 SUMMARY:")
    if working_endpoints:
        print(f"✅ Found {len(working_endpoints)} working endpoints:")
        for ep in working_endpoints:
            print(f"   • {ep['name']}")
    else:
        print("❌ No working endpoints found")
        print("💡 Possible reasons:")
        print("   • NSE has changed their URL structure") 
        print("   • Historical data for 2025 isn't published yet")
        print("   • Authentication/session required")
        print("   • Different date format needed")
    
    return working_endpoints

def test_alternative_sources():
    """Test alternative data sources"""
    
    print(f"\n🔍 Testing Alternative Data Sources")
    print("=" * 50)
    
    # Test if we can access any historical data at all
    test_cases = [
        {
            "name": "2024 July Data (Previous Year)",
            "url": "https://archives.nseindia.com/content/historical/EQUITIES/2024/JUL/cm01072024bhav.csv.zip",
            "description": "Test with previous year to verify URL structure"
        },
        {
            "name": "2025 August Data (Known Working)",
            "url": "https://archives.nseindia.com/content/historical/EQUITIES/2025/AUG/cm01082025bhav.csv.zip", 
            "description": "Test August 2025 which we know works"
        }
    ]
    
    for test in test_cases:
        print(f"\n🧪 Testing {test['name']}...")
        
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
            response = requests.get(test['url'], headers=headers, timeout=10)
            
            if response.status_code == 200 and len(response.content) > 1000:
                print(f"   ✅ Success! {len(response.content)} bytes")
                
                # Try to extract and show sample
                try:
                    import zipfile
                    import io
                    zip_file = zipfile.ZipFile(io.BytesIO(response.content))
                    csv_files = zip_file.namelist()
                    if csv_files:
                        sample = zip_file.read(csv_files[0]).decode('utf-8')[:200]
                        print(f"   📋 Sample: {sample[:100]}...")
                        return True  # Found at least one working source
                except:
                    pass
            else:
                print(f"   ❌ Failed: {response.status_code}")
                
        except Exception as e:
            print(f"   ❌ Error: {e}")
    
    return False

def suggest_alternatives():
    """Suggest alternative approaches"""
    
    print(f"\n💡 ALTERNATIVE APPROACHES:")
    print("=" * 50)
    
    print("1. 📅 Wait for NSE to publish historical data")
    print("   • NSE typically publishes historical data with 1-2 month delay")
    print("   • January-June 2025 may not be available yet")
    
    print("\n2. 🔗 Manual Download from NSE Website")
    print("   • Visit: https://www.nseindia.com/market-data/historical-data")
    print("   • Download monthly zip files manually") 
    print("   • Extract to month-wise folders")
    
    print("\n3. 📊 Use Available Data (July-August 2025)")
    print("   • We already have 42 trading days of data")
    print("   • Can perform analysis with existing data")
    print("   • Add more months as they become available")
    
    print("\n4. 🔄 Try Different Date Ranges")
    print("   • Some months might be available while others aren't")
    print("   • Could download partial year data")
    
    print("\n5. 📈 Alternative Data Sources")
    print("   • Yahoo Finance API")
    print("   • Alpha Vantage")
    print("   • Quandl/Nasdaq Data Link")

def main():
    print("🔍 NSE Data Source Explorer")
    print("=" * 60)
    print("🎯 Goal: Find working sources for January-June 2025 NSE data")
    print()
    
    # Test main endpoints
    working_endpoints = test_nse_endpoints()
    
    # Test alternatives if main endpoints don't work
    if not working_endpoints:
        has_alternative = test_alternative_sources()
        
        if not has_alternative:
            suggest_alternatives()
    
    print(f"\n📊 CURRENT STATUS:")
    print(f"✅ July 2025: 23 files downloaded")
    print(f"✅ August 2025: 19 files downloaded") 
    print(f"❓ January-June 2025: Investigating availability")
    
    print(f"\n🎯 RECOMMENDATION:")
    if working_endpoints:
        print("✅ Found working endpoints - can proceed with download")
    else:
        print("⏸️  Wait for NSE to publish historical data")
        print("💡 Continue analysis with available July-August data")
        print("🔄 Check back in a few weeks for earlier months")

if __name__ == "__main__":
    main()
