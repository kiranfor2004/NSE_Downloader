#!/usr/bin/env python3
"""
NSE August 2025 Full Bhavcopy & Security Deliverable Data Downloader

This script downloads all trading day data for August 2025 from NSE archives.
Files include complete Bhavcopy data with Security Deliverable information.

Requirements: pip install requests

Author: Generated for NSE data collection
Date: September 2025
"""

import requests
import os
from datetime import datetime
import time
import sys

def download_nse_august_2025():
    """
    Download all NSE Full Bhavcopy and Security Deliverable data for August 2025
    Total: 20 trading days (excluding weekends and Aug 15 holiday)
    """
    
    print("🚀 NSE August 2025 Full Bhavcopy & Security Deliverable Data Downloader")
    print("=" * 80)
    
    # Base URL pattern - NSE archives structure
    base_url = "https://archives.nseindia.com/products/content/sec_bhavdata_full_{date}.csv"
    
    # Alternative URLs to try if main fails
    alternative_urls = [
        "https://www1.nseindia.com/archives/products/content/sec_bhavdata_full_{date}.csv",
        "https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{date}.csv",
        "https://www.nseindia.com/products/content/sec_bhavdata_full_{date}.csv"
    ]
    
    # All trading days in August 2025 (DDMMYYYY format)
    # Excluded: Weekends (Sat/Sun) and Aug 15 (Independence Day)
    trading_days = [
        "01082025",  # Friday, Aug 1
        "04082025", "05082025", "06082025", "07082025", "08082025",  # Week 2: Aug 4-8
        "11082025", "12082025", "13082025", "14082025",              # Week 3: Aug 11-14 (Skip 15)
        "18082025", "19082025", "20082025", "21082025", "22082025",  # Week 4: Aug 18-22
        "25082025", "26082025", "27082025", "28082025", "29082025"   # Week 5: Aug 25-29
    ]
    
    # Create download directory
    download_dir = "NSE_August_2025_Data"
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)
        print(f"📁 Created directory: {download_dir}")
    
    # Headers to mimic browser request (helps avoid blocking)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/csv,application/csv,text/plain,*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }
    
    # Tracking variables
    successful_downloads = 0
    failed_downloads = []
    already_exists = 0
    
    print(f"\n🎯 Target: {len(trading_days)} files for August 2025")
    print(f"📂 Destination: {os.path.abspath(download_dir)}")
    print("-" * 80)
    
    # Download each file
    for i, date in enumerate(trading_days, 1):
        # Format date for display (DD/MM/YYYY)
        display_date = f"{date[:2]}/{date[2:4]}/{date[4:]}"
        filename = f"sec_bhavdata_full_{date}.csv"
        filepath = os.path.join(download_dir, filename)
        
        print(f"[{i:2d}/{len(trading_days)}] {display_date} - ", end="", flush=True)
        
        # Skip if file already exists and has content
        if os.path.exists(filepath):
            file_size = os.path.getsize(filepath)
            if file_size > 1000:  # At least 1KB (valid CSV should be larger)
                size_kb = file_size // 1024
                print(f"✅ Already exists ({size_kb} KB)")
                already_exists += 1
                successful_downloads += 1
                continue
            else:
                print(f"🔄 Re-downloading (existing file too small)...")
        
        # Try downloading from multiple URLs
        downloaded = False
        urls_to_try = [base_url.format(date=date)] + [url.format(date=date) for url in alternative_urls]
        
        for url_index, url in enumerate(urls_to_try):
            try:
                if url_index > 0:
                    print(f"    🔄 Trying alternative URL {url_index}... ", end="", flush=True)
                
                response = requests.get(url, headers=headers, timeout=30, stream=True)
                
                if response.status_code == 200:
                    # Check if we got actual CSV data (not an error page)
                    content = response.content
                    
                    if len(content) > 1000 and (b'SYMBOL' in content or b'Symbol' in content or b'symbol' in content):
                        # Save the file
                        with open(filepath, 'wb') as f:
                            f.write(content)
                        
                        file_size_kb = len(content) // 1024
                        print(f"✅ Downloaded ({file_size_kb} KB)")
                        successful_downloads += 1
                        downloaded = True
                        break
                    else:
                        # Probably an error page or invalid data
                        if url_index == 0:
                            print(f"❌ Invalid data, ", end="", flush=True)
                        else:
                            print(f"❌ Invalid, ", end="", flush=True)
                        continue
                        
                elif response.status_code == 404:
                    if url_index == 0:
                        print(f"❌ Not found, ", end="", flush=True)
                    else:
                        print(f"❌ 404, ", end="", flush=True)
                    continue
                else:
                    if url_index == 0:
                        print(f"❌ HTTP {response.status_code}, ", end="", flush=True)
                    else:
                        print(f"❌ {response.status_code}, ", end="", flush=True)
                    continue
                    
            except requests.exceptions.Timeout:
                print(f"⏰ Timeout, ", end="", flush=True)
                continue
            except requests.exceptions.RequestException as e:
                print(f"🌐 Network error, ", end="", flush=True)
                continue
        
        # If no URL worked
        if not downloaded:
            print("❌ All attempts failed")
            failed_downloads.append((date, display_date))
            
            # Save URLs to try manually
            with open(os.path.join(download_dir, f"failed_urls_{date}.txt"), 'w') as f:
                f.write(f"Failed to download data for {display_date}\n")
                f.write("Try these URLs manually:\n\n")
                for url in urls_to_try:
                    f.write(f"{url}\n")
        
        # Be respectful to the server - small delay between downloads
        if i < len(trading_days):  # Don't delay after last file
            time.sleep(0.5)
    
    # Download Summary
    print("\n" + "=" * 80)
    print("📊 DOWNLOAD SUMMARY")
    print("=" * 80)
    print(f"✅ Successfully downloaded: {successful_downloads - already_exists}/{len(trading_days)} files")
    print(f"♻️  Already existed: {already_exists}/{len(trading_days)} files")
    print(f"❌ Failed downloads: {len(failed_downloads)}/{len(trading_days)} files")
    print(f"🎯 Total coverage: {successful_downloads}/{len(trading_days)} files ({successful_downloads/len(trading_days)*100:.1f}%)")
    
    # Details for failed downloads
    if failed_downloads:
        print(f"\n⚠️  MANUAL DOWNLOAD NEEDED:")
        print("-" * 40)
        for date, display_date in failed_downloads:
            print(f"📅 {display_date}: sec_bhavdata_full_{date}.csv")
            print(f"   🔗 Try: https://archives.nseindia.com/products/content/sec_bhavdata_full_{date}.csv")
        
        print(f"\n💡 Check the failed_urls_*.txt files in the download folder for all alternative URLs")
    
    # Final status
    total_files = len([f for f in os.listdir(download_dir) if f.endswith('.csv')])
    total_size = sum(os.path.getsize(os.path.join(download_dir, f)) 
                    for f in os.listdir(download_dir) if f.endswith('.csv'))
    total_size_mb = total_size / (1024 * 1024)
    
    print(f"\n📁 Final Results:")
    print(f"   📂 Folder: {os.path.abspath(download_dir)}")
    print(f"   📄 CSV Files: {total_files}")
    print(f"   💾 Total Size: {total_size_mb:.1f} MB")
    
    if successful_downloads == len(trading_days):
        print(f"\n🎉 SUCCESS! All August 2025 data downloaded successfully!")
        print(f"   You now have complete Bhavcopy + Security Deliverable data")
        print(f"   for all {len(trading_days)} trading days in August 2025.")
    elif successful_downloads > len(trading_days) * 0.8:  # 80%+ success
        print(f"\n✅ MOSTLY COMPLETE! {successful_downloads}/{len(trading_days)} files downloaded.")
        print(f"   Try downloading the failed files manually using the URLs above.")
    else:
        print(f"\n⚠️  PARTIAL SUCCESS. Only {successful_downloads}/{len(trading_days)} files downloaded.")
        print(f"   Check your internet connection and try running the script again.")

def check_requirements():
    """Check if required libraries are available"""
    try:
        import requests
        return True
    except ImportError:
        print("❌ Error: 'requests' library not found!")
        print("📦 Please install it using: pip install requests")
        print("   Then run this script again.")
        return False

def show_file_info():
    """Show what files we're going to download"""
    print("\n📋 Files to download:")
    print("-" * 40)
    
    # Week 1
    print("Week 1: Aug  1       - 1 file")
    print("Week 2: Aug  4-8     - 5 files") 
    print("Week 3: Aug 11-14    - 4 files (Skip Aug 15 - Holiday)")
    print("Week 4: Aug 18-22    - 5 files")
    print("Week 5: Aug 25-29    - 5 files")
    print("-" * 40)
    print("Total:               - 20 files")
    print("\nEach file contains:")
    print("• Complete Bhavcopy data (OHLC, Volume, Trades)")
    print("• Security Deliverable data (Delivery %)")
    print("• All NSE listed securities for that trading day")

if __name__ == "__main__":
    print("🐍 NSE Data Downloader for August 2025")
    print("=" * 50)
    
    # Check requirements
    if not check_requirements():
        print("\nPress Enter to exit...")
        input()
        sys.exit(1)
    
    # Show what we're downloading
    show_file_info()
    
    # Get user confirmation
    print(f"\n⏳ This will download ~40MB of data from NSE servers.")
    print(f"📂 Files will be saved to: NSE_August_2025_Data folder")
    
    try:
        confirm = input(f"\n🚀 Ready to start download? (Press Enter to continue, Ctrl+C to cancel): ")
        print()
        download_nse_august_2025()
        
        print(f"\n✨ Script completed! Check the NSE_August_2025_Data folder.")
        input(f"\nPress Enter to exit...")
        
    except KeyboardInterrupt:
        print(f"\n\n🛑 Download cancelled by user.")
        print(f"You can run this script again anytime to resume downloading.")
        input(f"\nPress Enter to exit...")
    except Exception as e:
        print(f"\n💥 Unexpected error: {str(e)}")
        print(f"Please try running the script again.")
        input(f"\nPress Enter to exit...")