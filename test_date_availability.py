"""
Test Date Availability
"""
import requests
import datetime

# Test with some dates from different months in 2025
test_dates = ['03092025', '15082025', '30072025', '28062025', '31052025', '30042025', '31032025']

for date in test_dates:
    url = f'https://archives.nseindia.com/products/content/derivatives/equities/udiff_{date}.zip'
    print(f'Testing {date}: {url}')
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    }
    
    try:
        response = requests.head(url, headers=headers, timeout=10)
        print(f'  Status: {response.status_code}')
        if response.status_code == 200:
            print(f'  ✅ Available! Size: {response.headers.get("content-length", "unknown")}')
            # If we find one working, try to download it
            try:
                download_response = requests.get(url, headers=headers, timeout=30)
                if download_response.status_code == 200:
                    print(f'  📥 Download successful: {len(download_response.content)} bytes')
                    filename = f'test_download_{date}.zip'
                    with open(filename, 'wb') as f:
                        f.write(download_response.content)
                    print(f'  💾 Saved as: {filename}')
                break
            except Exception as e:
                print(f'  ❌ Download error: {e}')
        else:
            print(f'  ❌ Not available')
    except Exception as e:
        print(f'  ❌ Error: {e}')