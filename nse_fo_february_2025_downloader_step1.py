#!/usr/bin/env python3
"""
NSE F&O February 2025 Data Downloader (Step 1 Format)
Downloads F&O UDiFF Common Bhavcopy Final (ZIP) files for February 2025 trading days.
Based on Step 1 equity downloader structure.
"""

import os
import requests
from datetime import datetime, timedelta
import time

def get_february_2025_trading_days():
    """Get all trading days for February 2025 (excluding weekends and holidays)"""
    
    # February 2025 trading days (excluding weekends and major holidays)
    february_dates = [
        "03022025",  # Feb 3 (Monday)
        "04022025",  # Feb 4
        "05022025",  # Feb 5
        "06022025",  # Feb 6
        "07022025",  # Feb 7
        "10022025",  # Feb 10
        "11022025",  # Feb 11
        "12022025",  # Feb 12
        "13022025",  # Feb 13
        "14022025",  # Feb 14
        "17022025",  # Feb 17
        "18022025",  # Feb 18
        "19022025",  # Feb 19
        "20022025",  # Feb 20
        "21022025",  # Feb 21
        "24022025",  # Feb 24
        "25022025",  # Feb 25
        "27022025",  # Feb 27
        "28022025",  # Feb 28
    ]
    
    return february_dates

def download_nse_fo_data(date_str):
    """Download NSE F&O data for a specific date with multiple URL attempts"""
    
    # Convert date format for different URL patterns
    # date_str is in format "03022025" (DDMMYYYY)
    day = date_str[:2]
    month = date_str[2:4]
    year = date_str[4:]
    
    # Format variations for F&O URLs
    date_formats = {
        'ddmmyyyy': date_str,  # 03022025
        'ddmmmyyyy': f"{day}{'FEB'}{year}",  # 03FEB2025
        'dd_mm_yyyy': f"{day}{month}{year}",  # 03022025
        'yyyymmdd': f"{year}{month}{day}"  # 20250203
    }
    
    # Multiple NSE F&O URL patterns to try
    urls = [
        # F&O UDiFF Common Bhavcopy patterns
        f"https://archives.nseindia.com/content/historical/DERIVATIVES/2025/FEB/fo{date_formats['ddmmyyyy']}bhav.csv.zip",
        f"https://www1.nseindia.com/content/historical/DERIVATIVES/2025/FEB/fo{date_formats['ddmmyyyy']}bhav.csv.zip",
        f"https://nsearchives.nseindia.com/content/historical/DERIVATIVES/2025/FEB/fo{date_formats['ddmmyyyy']}bhav.csv.zip",
        
        # Alternative F&O patterns
        f"https://archives.nseindia.com/content/historical/DERIVATIVES/2025/02/fo{date_formats['ddmmyyyy']}bhav.csv.zip",
        f"https://www1.nseindia.com/content/historical/DERIVATIVES/2025/02/fo{date_formats['ddmmyyyy']}bhav.csv.zip",
        f"https://nsearchives.nseindia.com/content/historical/DERIVATIVES/2025/02/fo{date_formats['ddmmyyyy']}bhav.csv.zip",
        
        # UDiFF specific patterns
        f"https://archives.nseindia.com/content/historical/DERIVATIVES/2025/FEB/udiff{date_formats['ddmmmyyyy']}.zip",
        f"https://www1.nseindia.com/content/historical/DERIVATIVES/2025/FEB/udiff{date_formats['ddmmmyyyy']}.zip",
        f"https://nsearchives.nseindia.com/content/historical/DERIVATIVES/2025/FEB/udiff{date_formats['ddmmmyyyy']}.zip",
        
        # Additional F&O patterns
        f"https://archives.nseindia.com/products/content/derivatives/equities/udiff_{date_formats['ddmmyyyy']}.zip",
        f"https://www1.nseindia.com/products/content/derivatives/equities/udiff_{date_formats['ddmmyyyy']}.zip",
        f"https://nsearchives.nseindia.com/products/content/derivatives/equities/udiff_{date_formats['ddmmyyyy']}.zip",
        
        # Backup patterns
        f"https://archives.nseindia.com/content/historical/DERIVATIVES/2025/FEB/FO_{date_formats['ddmmmyyyy']}.zip",
        f"https://www1.nseindia.com/content/historical/DERIVATIVES/2025/FEB/FO_{date_formats['ddmmmyyyy']}.zip",
        f"https://nsearchives.nseindia.com/content/historical/DERIVATIVES/2025/FEB/FO_{date_formats['ddmmmyyyy']}.zip"
    ]
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Referer': 'https://www.nseindia.com/',
        'Cache-Control': 'no-cache'
    }
    
    for i, url in enumerate(urls, 1):
        try:
            print(f"  Attempt {i}: {url}")
            
            # Add session to maintain cookies
            session = requests.Session()
            session.headers.update(headers)
            
            response = session.get(url, timeout=30)
            
            if response.status_code == 200 and len(response.content) > 100:
                print(f"    ✅ Success! File size: {len(response.content)} bytes")
                return response.content, url.split('/')[-1]
            else:
                print(f"    ❌ Status: {response.status_code}, Size: {len(response.content) if response.content else 0}")
                
        except Exception as e:
            print(f"    ❌ Error: {str(e)}")
            continue
        
        # Add delay between attempts
        time.sleep(1)
    
    return None, None

def save_data(content, filename, folder):
    """Save downloaded content to file"""
    try:
        filepath = os.path.join(folder, filename)
        
        # Save as file (usually ZIP for F&O data)
        with open(filepath, 'wb') as f:
            f.write(content)
        
        file_size = len(content)
        print(f"    ✅ Saved: {filename} ({file_size} bytes)")
        
        return True
    except Exception as e:
        print(f"    ❌ Save error: {str(e)}")
        return False

def main():
    """Main download function"""
    print("🚀 NSE F&O February 2025 Data Downloader")
    print("📊 F&O UDiFF Common Bhavcopy Final (ZIP) Files")
    print("=" * 60)
    
    # Create download folder
    folder_name = "NSE_FO_February_2025_Data"
    os.makedirs(folder_name, exist_ok=True)
    print(f"📁 Download folder: {folder_name}")
    
    # Get trading days
    trading_days = get_february_2025_trading_days()
    print(f"📅 Total trading days to download: {len(trading_days)}")
    
    successful_downloads = 0
    failed_downloads = 0
    
    print("\n🔄 Starting downloads...")
    print("-" * 60)
    
    for i, date_str in enumerate(trading_days, 1):
        # Format date for display
        day = date_str[:2]
        month = date_str[2:4]
        year = date_str[4:]
        display_date = f"{day}-{month}-{year}"
        
        print(f"\n📊 [{i:2d}/{len(trading_days)}] {display_date}")
        
        # Download data
        content, filename = download_nse_fo_data(date_str)
        
        if content and filename:
            if save_data(content, filename, folder_name):
                successful_downloads += 1
                print(f"    🎉 Success!")
            else:
                failed_downloads += 1
                print(f"    ❌ Save failed!")
        else:
            failed_downloads += 1
            print(f"    ❌ Download failed - No data found")
        
        # Add delay between downloads to be respectful to NSE servers
        if i < len(trading_days):
            print("    ⏳ Waiting 3 seconds...")
            time.sleep(3)
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 DOWNLOAD SUMMARY")
    print("=" * 60)
    print(f"✅ Successful downloads: {successful_downloads}")
    print(f"❌ Failed downloads: {failed_downloads}")
    print(f"📁 Files saved to: {folder_name}/")
    
    if successful_downloads > 0:
        print(f"\n🎉 Downloaded {successful_downloads} F&O files successfully!")
        print("💡 You can now proceed to import this data into your database.")
    else:
        print(f"\n⚠️  No files were downloaded successfully.")
        print("💭 Possible reasons:")
        print("   • February 2025 data might not be available yet (future date)")
        print("   • NSE URL structure might have changed")
        print("   • Network connectivity issues")
        print("   • Server temporary unavailability")
        print("\n🔗 Manual download alternative:")
        print("   Visit: https://www.nseindia.com/all-reports-derivatives")
        print("   Navigate to Historical Reports > Derivatives > February 2025")

if __name__ == "__main__":
    main()
