"""
Test UDiFF file availability for different dates
"""
import requests

def test_udiff_availability():
    base_url = "https://archives.nseindia.com/products/content/derivatives/equities/"
    
    # Test various historical dates
    test_dates = [
        # Recent 2025 dates
        "01082025", "02082025", "05082025",
        # 2024 dates  
        "29122024", "30122024", "31122024",
        "02122024", "03122024", "04122024",
        # 2023 dates
        "29122023", "28122023", "27122023",
        # 2022 dates
        "30122022", "29122022", "28122022",
    ]
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    print("🔍 Testing UDiFF File Availability")
    print("=" * 50)
    
    found_files = []
    
    for date in test_dates:
        filename = f"udiff_{date}.zip"
        url = base_url + filename
        
        try:
            response = requests.head(url, headers=headers, timeout=10)
            status = response.status_code
            
            if status == 200:
                print(f"✅ {date}: FOUND - {filename}")
                found_files.append((date, filename, url))
            elif status == 404:
                print(f"❌ {date}: NOT FOUND")
            else:
                print(f"⚠️ {date}: HTTP {status}")
                
        except Exception as e:
            print(f"❌ {date}: ERROR - {e}")
    
    print(f"\n📊 SUMMARY:")
    print(f"Found {len(found_files)} available UDiFF files")
    
    if found_files:
        print(f"\n✅ Available files:")
        for date, filename, url in found_files[:3]:  # Show first 3
            print(f"  {date}: {url}")
        
        if len(found_files) > 3:
            print(f"  ... and {len(found_files)-3} more")
            
        return found_files[0]  # Return first available file
    else:
        print(f"❌ No UDiFF files found")
        return None

if __name__ == "__main__":
    test_udiff_availability()
