#!/usr/bin/env python3
"""
🔄 NSE F&O Historical Data Downloader - Step 4 (Revised)
Downloads available F&O UDiFF Bhavcopy data for historical months
"""

import requests
import os
import zipfile
import io
import csv
import pandas as pd
from datetime import datetime, timedelta
import time

class NSE_FO_Historical_Downloader:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive'
        })
        
        # Try different F&O data sources
        self.fo_endpoints = [
            {
                'name': 'NSE Archives F&O',
                'base_url': 'https://archives.nseindia.com/content/historical/DERIVATIVES',
                'pattern': '{year}/{month}/fo{dd}{mm}{yyyy}bhav.csv.zip'
            },
            {
                'name': 'NSE F&O Reports',
                'base_url': 'https://www1.nseindia.com/content/historical/DERIVATIVES', 
                'pattern': '{year}/{month}/fo{dd}{mm}{yyyy}bhav.csv.zip'
            },
            {
                'name': 'Alternative F&O Pattern',
                'base_url': 'https://archives.nseindia.com/products/content/derivatives',
                'pattern': 'bhav/fo{dd}{mm}{yyyy}bhav.csv.zip'
            }
        ]
    
    def get_trading_days(self, year, month):
        """Get trading days for a given month"""
        from calendar import monthrange
        
        # Get all days in the month
        _, last_day = monthrange(year, month)
        trading_days = []
        
        for day in range(1, last_day + 1):
            date = datetime(year, month, day)
            
            # Skip weekends (Saturday=5, Sunday=6)
            if date.weekday() < 5:  # Monday=0 to Friday=4
                trading_days.append(date)
        
        return trading_days
    
    def download_fo_data_for_month(self, year, month):
        """Download F&O data for a specific month"""
        month_name = datetime(year, month, 1).strftime('%B')
        print(f"\n📅 Downloading F&O data for {month_name} {year}")
        print("=" * 50)
        
        # Create download directory
        download_dir = f"NSE_FO_{month_name}_{year}_Data"
        os.makedirs(download_dir, exist_ok=True)
        
        trading_days = self.get_trading_days(year, month)
        successful_downloads = []
        failed_downloads = []
        
        print(f"🗓️ Found {len(trading_days)} potential trading days")
        
        for date in trading_days:
            dd = f"{date.day:02d}"
            mm = f"{date.month:02d}"
            yyyy = str(date.year)
            yy = yyyy[2:]
            
            date_str = f"{dd}{mm}{yyyy}"
            print(f"\n📊 Trying {date.strftime('%d-%b-%Y')}...")
            
            downloaded = False
            
            # Try different endpoints and patterns
            for endpoint in self.fo_endpoints:
                url_patterns = [
                    f"{endpoint['base_url']}/{yyyy}/{month_name.upper()}/fo{date_str}bhav.csv.zip",
                    f"{endpoint['base_url']}/{yyyy}/{mm}/fo{date_str}bhav.csv.zip",
                    f"{endpoint['base_url']}/fo{date_str}bhav.csv.zip",
                    f"{endpoint['base_url']}/bhav/fo{date_str}bhav.csv.zip",
                    # Additional patterns
                    f"{endpoint['base_url']}/{yyyy}/{month_name.upper()}/fo{dd}{mm}{yy}bhav.csv.zip",
                    f"{endpoint['base_url']}/FO_UDiFF_{date_str}.csv.zip",
                    f"{endpoint['base_url']}/BhavCopy_NSE_FO_{date_str}_F_0000.csv.zip"
                ]
                
                for url in url_patterns:
                    try:
                        print(f"   🔗 Trying: {url.split('/')[-1]}")
                        response = self.session.get(url, timeout=15)
                        
                        if response.status_code == 200 and len(response.content) > 1000:
                            # Save the file
                            filename = f"fo{date_str}bhav.csv.zip"
                            filepath = os.path.join(download_dir, filename)
                            
                            with open(filepath, 'wb') as f:
                                f.write(response.content)
                            
                            print(f"   ✅ Downloaded: {filename} ({len(response.content)} bytes)")
                            
                            # Verify the download
                            if self.verify_fo_file(filepath):
                                successful_downloads.append({
                                    'date': date.strftime('%Y-%m-%d'),
                                    'filename': filename,
                                    'size': len(response.content),
                                    'url': url
                                })
                                downloaded = True
                                break
                            
                        else:
                            print(f"   ❌ Failed: {response.status_code}")
                            
                    except Exception as e:
                        print(f"   ❌ Error: {str(e)[:50]}...")
                        continue
                
                if downloaded:
                    break
                
                time.sleep(0.5)  # Be respectful
            
            if not downloaded:
                failed_downloads.append({
                    'date': date.strftime('%Y-%m-%d'),
                    'reason': 'No working URL found'
                })
                print(f"   ❌ Failed to download data for {date.strftime('%d-%b-%Y')}")
        
        # Save download summary
        self.save_download_summary(download_dir, successful_downloads, failed_downloads, year, month)
        
        return successful_downloads, failed_downloads
    
    def verify_fo_file(self, filepath):
        """Verify that the downloaded F&O file is valid"""
        try:
            with zipfile.ZipFile(filepath, 'r') as zip_file:
                csv_files = zip_file.namelist()
                
                if not csv_files:
                    return False
                
                # Read first CSV file
                csv_content = zip_file.read(csv_files[0]).decode('utf-8')
                lines = csv_content.split('\n')
                
                # Check if it looks like F&O data
                if len(lines) > 1:
                    header = lines[0].upper()
                    if any(keyword in header for keyword in ['INSTRUMENT', 'SYMBOL', 'EXPIRY', 'STRIKE', 'OPTION_TYP', 'OPEN', 'HIGH', 'LOW', 'CLOSE']):
                        return True
                
                return False
                
        except Exception:
            return False
    
    def save_download_summary(self, download_dir, successful, failed, year, month):
        """Save download summary"""
        summary_file = os.path.join(download_dir, f"download_summary_{year}_{month:02d}.txt")
        
        with open(summary_file, 'w') as f:
            f.write(f"F&O Data Download Summary - {datetime(year, month, 1).strftime('%B %Y')}\n")
            f.write("=" * 60 + "\n\n")
            
            f.write(f"📊 DOWNLOAD STATISTICS:\n")
            f.write(f"Total Successful: {len(successful)}\n")
            f.write(f"Total Failed: {len(failed)}\n")
            f.write(f"Success Rate: {len(successful)/(len(successful)+len(failed))*100:.1f}%\n\n")
            
            if successful:
                f.write("✅ SUCCESSFUL DOWNLOADS:\n")
                f.write("-" * 30 + "\n")
                for download in successful:
                    f.write(f"{download['date']}: {download['filename']} ({download['size']} bytes)\n")
                f.write("\n")
            
            if failed:
                f.write("❌ FAILED DOWNLOADS:\n")
                f.write("-" * 30 + "\n")
                for failure in failed:
                    f.write(f"{failure['date']}: {failure['reason']}\n")
    
    def analyze_fo_data(self, download_dir):
        """Analyze downloaded F&O data"""
        print(f"\n📊 Analyzing F&O data in {download_dir}...")
        
        zip_files = [f for f in os.listdir(download_dir) if f.endswith('.zip')]
        
        if not zip_files:
            print("❌ No F&O data files found to analyze")
            return
        
        total_records = 0
        unique_symbols = set()
        instruments = set()
        
        for zip_file in zip_files:
            filepath = os.path.join(download_dir, zip_file)
            
            try:
                with zipfile.ZipFile(filepath, 'r') as zf:
                    csv_files = zf.namelist()
                    
                    for csv_file in csv_files:
                        csv_content = zf.read(csv_file).decode('utf-8')
                        lines = csv_content.split('\n')
                        
                        if len(lines) > 1:
                            header = lines[0].split(',')
                            
                            for line in lines[1:]:
                                if line.strip():
                                    data = line.split(',')
                                    if len(data) >= len(header):
                                        total_records += 1
                                        
                                        # Extract symbol and instrument info
                                        for i, col in enumerate(header):
                                            col = col.strip().upper()
                                            if 'SYMBOL' in col and i < len(data):
                                                unique_symbols.add(data[i].strip())
                                            elif 'INSTRUMENT' in col and i < len(data):
                                                instruments.add(data[i].strip())
                        
            except Exception as e:
                print(f"❌ Error analyzing {zip_file}: {e}")
        
        print(f"📈 Analysis Results:")
        print(f"   Total Records: {total_records:,}")
        print(f"   Unique Symbols: {len(unique_symbols)}")
        print(f"   Instruments: {', '.join(sorted(instruments))}")
        
        # Save analysis
        analysis_file = os.path.join(download_dir, "fo_data_analysis.txt")
        with open(analysis_file, 'w') as f:
            f.write(f"F&O Data Analysis Report\n")
            f.write("=" * 30 + "\n\n")
            f.write(f"Total Records: {total_records:,}\n")
            f.write(f"Unique Symbols: {len(unique_symbols)}\n")
            f.write(f"Files Analyzed: {len(zip_files)}\n\n")
            f.write(f"Instruments Found:\n")
            for instrument in sorted(instruments):
                f.write(f"  - {instrument}\n")
            f.write(f"\nTop 20 Symbols:\n")
            for symbol in sorted(list(unique_symbols))[:20]:
                f.write(f"  - {symbol}\n")

def main():
    """Main download function"""
    print("🔄 NSE F&O Historical Data Downloader - Step 4 (Revised)")
    print("=" * 60)
    print("🎯 Goal: Download available F&O UDiFF Bhavcopy data")
    print()
    
    downloader = NSE_FO_Historical_Downloader()
    
    # Try downloading recent historical months
    months_to_try = [
        (2024, 8),   # August 2024
        (2024, 9),   # September 2024  
        (2024, 7),   # July 2024
        (2024, 10),  # October 2024
    ]
    
    total_successful = 0
    
    for year, month in months_to_try:
        try:
            successful, failed = downloader.download_fo_data_for_month(year, month)
            
            if successful:
                print(f"\n✅ Downloaded {len(successful)} files for {datetime(year, month, 1).strftime('%B %Y')}")
                total_successful += len(successful)
                
                # Analyze the data
                month_name = datetime(year, month, 1).strftime('%B')
                download_dir = f"NSE_FO_{month_name}_{year}_Data"
                downloader.analyze_fo_data(download_dir)
                
                # Stop after first successful month (for now)
                break
            else:
                print(f"\n❌ No data available for {datetime(year, month, 1).strftime('%B %Y')}")
                
        except Exception as e:
            print(f"❌ Error downloading {datetime(year, month, 1).strftime('%B %Y')}: {e}")
    
    # Final summary
    print(f"\n📊 FINAL SUMMARY")
    print("=" * 30)
    if total_successful > 0:
        print(f"✅ Successfully downloaded {total_successful} F&O files")
        print(f"🎯 Step 4 (F&O Data Download) - COMPLETED with historical data")
        print(f"📁 Data saved in NSE_FO_*_Data folders")
        print(f"\n🚀 Ready for next step: Combined equity + F&O analysis")
    else:
        print(f"❌ No F&O data could be downloaded")
        print(f"💡 Recommendation: Continue with equity data analysis")
        print(f"🔍 F&O data may require manual download from NSE website")

if __name__ == "__main__":
    main()
