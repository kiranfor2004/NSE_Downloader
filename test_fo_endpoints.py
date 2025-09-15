"""
Download single day F&O data from NSE using alternative methods
"""
import requests
import time

def test_different_fo_endpoints():
    """Test different NSE F&O endpoints"""
    
    test_date = "01082025"  # Aug 1, 2025
    
    # Different possible endpoints for F&O data
    endpoints = [
        # UDiFF endpoints
        {
            "name": "UDiFF - Products/Content",
            "url": f"https://archives.nseindia.com/products/content/derivatives/equities/udiff_{test_date}.zip",
            "description": "F&O UDiFF Common Bhavcopy Final"
        },
        {
            "name": "UDiFF - Archives Direct", 
            "url": f"https://www1.nseindia.com/archives/products/content/derivatives/equities/udiff_{test_date}.zip",
            "description": "Alternative UDiFF endpoint"
        },
        # Standard F&O endpoints
        {
            "name": "F&O Historical",
            "url": f"https://archives.nseindia.com/content/historical/DERIVATIVES/2025/AUG/fo01AUG2025bhav.csv.zip",
            "description": "Historical F&O Bhavcopy"
        },
        {
            "name": "F&O NSE Archives",
            "url": f"https://nsearchives.nseindia.com/content/fo/BhavCopy_NSE_FO_0_0_0_{test_date}_F_0000.csv.zip",
            "description": "NSE F&O Archives"
        },
        # Alternative derivatives endpoints
        {
            "name": "Derivatives Content",
            "url": f"https://archives.nseindia.com/content/derivatives/fo_{test_date}.zip",
            "description": "Derivatives content folder"
        }
    ]
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive',
    }
    
    print(f"🔍 Testing F&O Endpoints for {test_date}")
    print(f"=" * 60)
    
    for endpoint in endpoints:
        print(f"\n📡 Testing: {endpoint['name']}")
        print(f"📝 Description: {endpoint['description']}")
        print(f"🌐 URL: {endpoint['url']}")
        
        try:
            response = requests.head(endpoint['url'], headers=headers, timeout=15)
            status = response.status_code
            
            if status == 200:
                content_length = response.headers.get('content-length', 'Unknown')
                print(f"✅ SUCCESS - Status: {status}, Size: {content_length} bytes")
                
                # Try to download the file
                print(f"📥 Attempting download...")
                download_response = requests.get(endpoint['url'], headers=headers, timeout=30)
                
                if download_response.status_code == 200:
                    file_size = len(download_response.content)
                    print(f"✅ Downloaded: {file_size:,} bytes")
                    
                    # Save file for inspection
                    import os
                    os.makedirs("Test_Downloads", exist_ok=True)
                    filename = endpoint['url'].split('/')[-1]
                    filepath = os.path.join("Test_Downloads", filename)
                    
                    with open(filepath, 'wb') as f:
                        f.write(download_response.content)
                    print(f"💾 Saved to: {filepath}")
                    
                    return True, endpoint, filepath
                else:
                    print(f"❌ Download failed: {download_response.status_code}")
                    
            elif status == 404:
                print(f"❌ NOT FOUND (404)")
            elif status == 503:
                print(f"⚠️ SERVICE UNAVAILABLE (503)")
            else:
                print(f"⚠️ HTTP {status}")
                
        except requests.exceptions.Timeout:
            print(f"⏰ TIMEOUT")
        except Exception as e:
            print(f"❌ ERROR: {e}")
        
        # Small delay between requests
        time.sleep(2)
    
    print(f"\n📊 CONCLUSION:")
    print(f"❌ No working F&O endpoints found for {test_date}")
    print(f"💡 This suggests either:")
    print(f"   • Files don't exist for this date yet")
    print(f"   • NSE has changed their URL structure")
    print(f"   • Access restrictions are in place")
    
    return False, None, None

if __name__ == "__main__":
    test_different_fo_endpoints()
