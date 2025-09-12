#!/usr/bin/env python3
"""
🚀 NSE F&O UDiFF Common Bhavcopy Final Downloader
Download F&O - UDiFF Common Bhavcopy Final (zip) files for February 2025
Based on: https://www.nseindia.com/all-reports-derivatives#cr_deriv_equity_archives
"""

import os
import requests
import time
import zipfile
from datetime import datetime, date
from calendar import monthrange
import urllib3

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def create_download_folder():
    """Create download folder for F&O data"""
    folder_name = "NSE_FO_UDiFF_February_2025_Data"
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
        print(f"📁 Created folder: {folder_name}")
    else:
        print(f"📁 Using existing folder: {folder_name}")
    return folder_name

def get_trading_days(year, month):
    """Get list of trading days for the month (excluding weekends)"""
    trading_days = []
    days_in_month = monthrange(year, month)[1]
    
    for day in range(1, days_in_month + 1):
        date_obj = date(year, month, day)
        # Skip weekends (Saturday=5, Sunday=6)
        if date_obj.weekday() < 5:
            trading_days.append(date_obj)
    
    return trading_days

def get_nse_session():
    """Create NSE session with proper headers"""
    session = requests.Session()
    
    # Headers to mimic browser request
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        'Sec-Ch-Ua-Mobile': '?0',
        'Sec-Ch-Ua-Platform': '"Windows"',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Cache-Control': 'max-age=0'
    }
    
    session.headers.update(headers)
    return session

def try_download_udiff_file(session, trading_date, folder_name):
    """Try to download UDiFF Common Bhavcopy Final file for a specific date"""
    
    day = trading_date.day
    month = trading_date.month
    year = trading_date.year
    
    date_str = trading_date.strftime("%d-%m-%Y")
    month_abbr = trading_date.strftime("%b").upper()
    
    print(f"📊 [{trading_date.strftime('%d-%m-%Y')}] Trying UDiFF Common Bhavcopy Final...")
    
    # Multiple URL patterns for UDiFF Common Bhavcopy Final files
    url_patterns = [
        # Current NSE archives structure
        f"https://archives.nseindia.com/content/historical/DERIVATIVES/{year}/{month_abbr}/udiff{day:02d}{month_abbr}{year}.zip",
        f"https://archives.nseindia.com/products/content/derivatives/equities/udiff{day:02d}{month_abbr}{year}.zip",
        f"https://www1.nseindia.com/content/historical/DERIVATIVES/{year}/{month_abbr}/udiff{day:02d}{month_abbr}{year}.zip",
        
        # Alternative formats
        f"https://archives.nseindia.com/content/historical/DERIVATIVES/{year}/{month_abbr}/udiff_{day:02d}{month:02d}{year}.zip",
        f"https://archives.nseindia.com/content/historical/DERIVATIVES/{year}/{month_abbr}/UDIFF{day:02d}{month_abbr}{year}.zip",
        f"https://archives.nseindia.com/content/historical/DERIVATIVES/{year}/{month_abbr}/udiff{day:02d}{month_abbr.lower()}{year}.zip",
        
        # Date format variations
        f"https://archives.nseindia.com/content/historical/DERIVATIVES/{year}/{month_abbr}/udiff{day:02d}{month:02d}{year}.zip",
        f"https://archives.nseindia.com/products/content/derivatives/equities/udiff_{day:02d}{month:02d}{year}.zip",
        
        # Legacy formats
        f"https://nseindia.com/content/historical/DERIVATIVES/{year}/{month_abbr}/udiff{day:02d}{month_abbr}{year}.zip",
        f"https://www.nseindia.com/content/historical/DERIVATIVES/{year}/{month_abbr}/udiff{day:02d}{month_abbr}{year}.zip",
    ]
    
    for attempt, url in enumerate(url_patterns, 1):
        try:
            print(f"  Attempt {attempt}: {url}")
            
            response = session.get(url, timeout=30, verify=False, allow_redirects=True)
            
            # Check if we got a valid ZIP file
            if response.status_code == 200:
                content_type = response.headers.get('content-type', '').lower()
                content_length = len(response.content)
                
                # Check if it's a ZIP file and has reasonable size
                if content_length > 1000 and (
                    content_type.startswith('application/zip') or 
                    content_type.startswith('application/x-zip') or
                    url.endswith('.zip')
                ):
                    # Try to verify it's a valid ZIP
                    try:
                        # Test if content is a valid ZIP
                        import io
                        test_zip = zipfile.ZipFile(io.BytesIO(response.content))
                        test_zip.testzip()  # This will raise exception if corrupted
                        test_zip.close()
                        
                        # Save the file
                        filename = f"udiff{day:02d}{month_abbr}{year}.zip"
                        file_path = os.path.join(folder_name, filename)
                        
                        with open(file_path, 'wb') as f:
                            f.write(response.content)
                        
                        print(f"    ✅ Downloaded: {filename} ({content_length:,} bytes)")
                        return True
                        
                    except zipfile.BadZipFile:
                        print(f"    ❌ Invalid ZIP file, Size: {content_length}")
                        continue
                
                else:
                    print(f"    ❌ Status: {response.status_code}, Size: {content_length} (not ZIP)")
            else:
                print(f"    ❌ Status: {response.status_code}")
                
        except requests.exceptions.RequestException as e:
            print(f"    ❌ Error: {str(e)}")
            continue
        except Exception as e:
            print(f"    ❌ Unexpected error: {str(e)}")
            continue
    
    print(f"    ❌ Download failed - No valid UDiFF file found")
    return False

def main():
    """Main download function"""
    print("🚀 NSE F&O UDiFF Common Bhavcopy Final Downloader")
    print("📊 Based on: https://www.nseindia.com/all-reports-derivatives#cr_deriv_equity_archives")
    print("=" * 80)
    
    # Create download folder
    folder_name = create_download_folder()
    
    # Get trading days for February 2025
    trading_days = get_trading_days(2025, 2)
    
    print(f"📅 Month: February 2025")
    print(f"📁 Download folder: {folder_name}")
    print(f"📅 Total trading days to download: {len(trading_days)}")
    
    # Create NSE session
    session = get_nse_session()
    
    print(f"\n🔄 Starting UDiFF Common Bhavcopy Final downloads...")
    print("-" * 80)
    
    successful_downloads = 0
    failed_downloads = 0
    
    for i, trading_date in enumerate(trading_days, 1):
        print(f"\n📊 [{i:2d}/{len(trading_days)}] {trading_date.strftime('%d-%m-%Y')}")
        
        success = try_download_udiff_file(session, trading_date, folder_name)
        
        if success:
            successful_downloads += 1
        else:
            failed_downloads += 1
            print(f"    ⏳ Waiting 3 seconds before next attempt...")
            time.sleep(3)
        
        # Small delay to be respectful to NSE servers
        time.sleep(1)
    
    # Final summary
    print(f"\n📊 DOWNLOAD SUMMARY:")
    print("=" * 50)
    print(f"✅ Successfully downloaded: {successful_downloads} files")
    print(f"❌ Failed downloads: {failed_downloads} files")
    print(f"📁 Files saved in: {folder_name}")
    
    if successful_downloads > 0:
        print(f"\n🎉 Download completed! Check the '{folder_name}' folder for your F&O UDiFF files.")
        
        # List downloaded files
        if os.path.exists(folder_name):
            files = [f for f in os.listdir(folder_name) if f.endswith('.zip')]
            if files:
                print(f"\n📄 Downloaded files:")
                for file in sorted(files):
                    file_path = os.path.join(folder_name, file)
                    size = os.path.getsize(file_path)
                    print(f"   📦 {file} ({size:,} bytes)")
    else:
        print(f"\n❌ No files were downloaded.")
        print(f"💡 This might be because:")
        print(f"   1. February 2025 data is not yet available (future date)")
        print(f"   2. NSE has changed their URL structure")
        print(f"   3. Files might be available under different naming convention")
        print(f"\n💡 Try checking NSE website manually or try downloading historical months")

if __name__ == "__main__":
    main()
