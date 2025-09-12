#!/usr/bin/env python3
"""
🚀 NSE F&O UDiFF Common Bhavcopy Final Downloader
Download F&O UDiFF Common Bhavcopy Final (ZIP) files from NSE
Based on: https://www.nseindia.com/all-reports-derivatives#cr_deriv_equity_archives
"""

import requests
import os
from datetime import datetime, timedelta
import calendar
import time

def download_nse_fo_udiff_bhavcopy(year, month, download_folder="NSE_FO_UDiFF_Data"):
    """
    Download NSE F&O UDiFF Common Bhavcopy Final files for a specific month
    
    Args:
        year (int): Year (e.g., 2024, 2023)
        month (int): Month (1-12)
        download_folder (str): Folder to save downloaded files
    """
    
    # Create download folder if it doesn't exist
    if not os.path.exists(download_folder):
        os.makedirs(download_folder)
        print(f"📁 Created download folder: {download_folder}")
    
    # NSE headers to mimic browser request
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Cache-Control': 'max-age=0'
    }
    
    # Create a session for better connection handling
    session = requests.Session()
    session.headers.update(headers)
    
    # Get number of days in the month
    num_days = calendar.monthrange(year, month)[1]
    month_name = calendar.month_name[month]
    month_abbr = calendar.month_abbr[month].upper()
    
    print(f"🚀 NSE F&O UDiFF Common Bhavcopy Final Downloader")
    print(f"📅 Target Month: {month_name} {year}")
    print(f"📁 Download Folder: {download_folder}")
    print(f"📊 Processing {num_days} days...")
    print("=" * 60)
    
    successful_downloads = []
    failed_downloads = []
    skipped_weekends = []
    
    for day in range(1, num_days + 1):
        try:
            # Create date object
            date_obj = datetime(year, month, day)
            
            # Skip weekends (Saturday=5, Sunday=6)
            if date_obj.weekday() >= 5:
                skipped_weekends.append(date_obj.strftime('%Y-%m-%d'))
                print(f"⏭️  Skipping {date_obj.strftime('%d-%m-%Y')} (Weekend)")
                continue
            
            print(f"\n📊 [{day:2d}/{num_days}] {date_obj.strftime('%d-%m-%Y')}")
            
            # Format date for different filename patterns
            date_ddmmyyyy = date_obj.strftime("%d%m%Y")  # 15022024
            date_ddmmmyyyy = date_obj.strftime("%d%b%Y").upper()  # 15FEB2024
            date_ddmmmyyyy_lower = date_obj.strftime("%d%b%Y").lower()  # 15feb2024
            
            # UDiFF Common Bhavcopy URL patterns based on NSE structure
            url_patterns = [
                # Pattern 1: Current NSE Archives
                f"https://archives.nseindia.com/content/historical/DERIVATIVES/{year}/{month_abbr}/udiff{date_ddmmmyyyy_lower}.zip",
                f"https://archives.nseindia.com/content/historical/DERIVATIVES/{year}/{month_abbr}/UDIFF{date_ddmmmyyyy}.zip",
                f"https://archives.nseindia.com/content/historical/DERIVATIVES/{year}/{month_name.upper()}/udiff{date_ddmmmyyyy_lower}.zip",
                
                # Pattern 2: Alternative NSE URLs
                f"https://www1.nseindia.com/content/historical/DERIVATIVES/{year}/{month_abbr}/udiff{date_ddmmmyyyy_lower}.zip",
                f"https://nsearchives.nseindia.com/content/historical/DERIVATIVES/{year}/{month_abbr}/udiff{date_ddmmmyyyy_lower}.zip",
                
                # Pattern 3: Products section
                f"https://archives.nseindia.com/products/content/derivatives/equities/udiff_{date_ddmmyyyy}.zip",
                f"https://archives.nseindia.com/products/content/derivatives/equities/UDIFF_{date_ddmmyyyy}.zip",
                
                # Pattern 4: Direct derivatives path
                f"https://archives.nseindia.com/content/derivatives/udiff{date_ddmmmyyyy_lower}.zip",
                f"https://archives.nseindia.com/content/derivatives/UDIFF{date_ddmmmyyyy}.zip",
                
                # Pattern 5: Historical path variations
                f"https://archives.nseindia.com/historical/DERIVATIVES/{year}/{month_abbr}/udiff{date_ddmmmyyyy_lower}.zip",
            ]
            
            downloaded = False
            attempt = 0
            
            for url in url_patterns:
                attempt += 1
                try:
                    print(f"  Attempt {attempt}: {url}")
                    
                    # Make request with timeout
                    response = session.get(url, timeout=15, verify=False)
                    
                    if response.status_code == 200 and len(response.content) > 1000:  # Valid file should be larger
                        # Determine filename
                        filename = f"udiff_{date_ddmmmyyyy_lower}.zip"
                        filepath = os.path.join(download_folder, filename)
                        
                        # Save file
                        with open(filepath, 'wb') as f:
                            f.write(response.content)
                        
                        file_size = len(response.content)
                        print(f"    ✅ Success! Downloaded: {filename} ({file_size:,} bytes)")
                        successful_downloads.append({
                            'date': date_obj.strftime('%d-%m-%Y'),
                            'filename': filename,
                            'size': file_size,
                            'url': url
                        })
                        downloaded = True
                        break
                        
                    elif response.status_code == 404:
                        print(f"    ❌ Status: 404, Size: {len(response.content)}")
                        continue  # Try next URL pattern
                    else:
                        print(f"    ⚠️  Status: {response.status_code}, Size: {len(response.content)}")
                        
                except requests.exceptions.SSLError as e:
                    print(f"    ❌ SSL Error: {str(e)[:100]}...")
                    continue
                except requests.exceptions.RequestException as e:
                    print(f"    ❌ Error: {str(e)[:100]}...")
                    continue
                except Exception as e:
                    print(f"    ❌ Unexpected error: {str(e)[:100]}...")
                    continue
            
            if not downloaded:
                failed_downloads.append(date_obj.strftime('%d-%m-%Y'))
                print(f"    ❌ Download failed - No data found")
                print(f"    ⏳ Waiting 3 seconds...")
                time.sleep(3)
            else:
                print(f"    ⏳ Waiting 2 seconds...")
                time.sleep(2)
            
        except Exception as e:
            failed_downloads.append(f"{day:02d}-{month:02d}-{year}")
            print(f"❌ Error processing {day:02d}-{month:02d}-{year}: {str(e)}")
    
    # Final Summary
    print(f"\n{'='*60}")
    print(f"📊 DOWNLOAD SUMMARY - {month_name} {year}")
    print(f"{'='*60}")
    print(f"✅ Successful downloads: {len(successful_downloads)}")
    print(f"❌ Failed downloads: {len(failed_downloads)}")
    print(f"⏭️  Skipped weekends: {len(skipped_weekends)}")
    print(f"📁 Download folder: {os.path.abspath(download_folder)}")
    
    if successful_downloads:
        print(f"\n✅ Successfully downloaded files:")
        total_size = 0
        for item in successful_downloads:
            print(f"  📄 {item['date']}: {item['filename']} ({item['size']:,} bytes)")
            total_size += item['size']
        print(f"\n📊 Total downloaded: {total_size:,} bytes ({total_size/1024/1024:.2f} MB)")
    
    if failed_downloads:
        print(f"\n❌ Failed to download data for dates:")
        for date in failed_downloads:
            print(f"  📅 {date}")
        
        print(f"\n💡 Possible reasons for failures:")
        print(f"   • Data not available for future dates")
        print(f"   • NSE changed URL structure")
        print(f"   • Network/SSL issues")
        print(f"   • Data might be available in different format")
    
    return successful_downloads, failed_downloads

def download_specific_date(year, month, day, download_folder="NSE_FO_UDiFF_Data"):
    """
    Download NSE F&O UDiFF Bhavcopy for a specific date
    
    Args:
        year (int): Year
        month (int): Month (1-12) 
        day (int): Day
        download_folder (str): Folder to save downloaded files
    """
    
    if not os.path.exists(download_folder):
        os.makedirs(download_folder)
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    date_obj = datetime(year, month, day)
    date_str_lower = date_obj.strftime("%d%b%Y").lower()
    month_abbr = date_obj.strftime("%b").upper()
    
    print(f"🎯 Downloading UDiFF data for {date_obj.strftime('%d-%m-%Y')}")
    
    url_patterns = [
        f"https://archives.nseindia.com/content/historical/DERIVATIVES/{year}/{month_abbr}/udiff{date_str_lower}.zip",
        f"https://archives.nseindia.com/products/content/derivatives/equities/udiff_{date_obj.strftime('%d%m%Y')}.zip",
    ]
    
    for i, url in enumerate(url_patterns, 1):
        try:
            print(f"  Attempt {i}: {url}")
            response = requests.get(url, headers=headers, timeout=30, verify=False)
            
            if response.status_code == 200 and len(response.content) > 1000:
                filename = f"udiff_{date_str_lower}.zip"
                filepath = os.path.join(download_folder, filename)
                
                with open(filepath, 'wb') as f:
                    f.write(response.content)
                
                print(f"✅ Downloaded: {filename} ({len(response.content):,} bytes)")
                return filepath
                
        except requests.exceptions.RequestException as e:
            print(f"    ❌ Error: {str(e)[:100]}...")
    
    print(f"❌ Failed to download data for {date_obj.strftime('%d-%m-%Y')}")
    return None

def suggest_available_months():
    """Suggest months that might have available data"""
    current_date = datetime.now()
    print(f"\n💡 SUGGESTIONS:")
    print(f"{'='*40}")
    print(f"Since February 2025 is a future date, try downloading:")
    
    # Suggest last few months
    for i in range(1, 7):
        suggested_date = current_date - timedelta(days=30*i)
        if suggested_date.year >= 2020:  # NSE typically has data from 2020+
            print(f"  📅 {suggested_date.strftime('%B %Y')} (Month: {suggested_date.month}, Year: {suggested_date.year})")

# Example usage and main execution:
if __name__ == "__main__":
    print("🚀 NSE F&O UDiFF Common Bhavcopy Final Downloader")
    print("📋 Based on: https://www.nseindia.com/all-reports-derivatives#cr_deriv_equity_archives")
    print("=" * 70)
    
    # Check if trying to download future data
    target_year = 2025
    target_month = 2  # February
    current_date = datetime.now()
    target_date = datetime(target_year, target_month, 1)
    
    if target_date > current_date:
        print(f"⚠️  WARNING: February 2025 is a future date!")
        print(f"📅 Current date: {current_date.strftime('%B %d, %Y')}")
        print(f"🎯 Target date: {target_date.strftime('%B %Y')}")
        print(f"❌ NSE doesn't publish future data")
        
        suggest_available_months()
        
        print(f"\n❓ Do you want to try anyway? (NSE will return 404 errors)")
        response = input("Enter 'yes' to continue or 'no' to exit: ").strip().lower()
        
        if response != 'yes':
            print("👋 Exiting. Please try with a historical month.")
            exit()
    
    # Download all F&O UDiFF Bhavcopy files for the specified month
    print(f"\n🚀 Starting download for {calendar.month_name[target_month]} {target_year}...")
    successful, failed = download_nse_fo_udiff_bhavcopy(target_year, target_month)
    
    # Alternative: Download for a specific date
    # download_specific_date(2024, 8, 15)  # Downloads for Aug 15, 2024
