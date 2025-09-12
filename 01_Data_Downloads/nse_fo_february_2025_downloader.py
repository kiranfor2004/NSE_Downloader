#!/usr/bin/env python3
"""
📈 NSE F&O UDiFF Bhavcopy Downloader - February 2025
Download Futures & Options UDiFF Bhavcopy Final data from NSE India
Step 4 of NSE Data Analysis Project
"""

import requests
import zipfile
import os
import pandas as pd
from datetime import datetime, timedelta
import time
import calendar
from io import BytesIO

class NSE_FO_UDiFF_Downloader:
    def __init__(self):
        self.base_url = "https://archives.nseindia.com/content/fo"
        self.download_dir = "NSE_FO_February_2025_Data"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })
        
    def setup_download_directory(self):
        """Create download directory if it doesn't exist"""
        if not os.path.exists(self.download_dir):
            os.makedirs(self.download_dir)
            print(f"📁 Created directory: {self.download_dir}")
        else:
            print(f"📁 Using existing directory: {self.download_dir}")
    
    def get_february_2025_trading_days(self):
        """Get all trading days for February 2025"""
        # February 2025 trading days (excluding weekends and holidays)
        trading_days = []
        
        # February 2025 calendar
        for day in range(1, 29):  # February 2025 has 28 days
            date = datetime(2025, 2, day)
            
            # Skip weekends (Saturday = 5, Sunday = 6)
            if date.weekday() < 5:  # Monday = 0, Friday = 4
                # Skip known holidays in February 2025
                if day not in [26]:  # Maha Shivratri on 26th Feb 2025
                    trading_days.append(date)
        
        return trading_days
    
    def generate_fo_udiff_url(self, date):
        """Generate F&O UDiFF Bhavcopy URL for given date"""
        # Format: https://archives.nseindia.com/content/fo/BhavCopy_NSE_FO_DDMMYYYY_F_0000.csv.zip
        date_str = date.strftime("%d%m%Y")
        filename = f"BhavCopy_NSE_FO_{date_str}_F_0000.csv.zip"
        url = f"{self.base_url}/{filename}"
        return url, filename
    
    def download_file(self, url, filename, date):
        """Download individual F&O UDiFF file"""
        try:
            print(f"⬇️  Downloading: {filename} for {date.strftime('%d-%b-%Y')}")
            
            response = self.session.get(url, timeout=30)
            
            if response.status_code == 200:
                # Save the zip file
                zip_path = os.path.join(self.download_dir, filename)
                with open(zip_path, 'wb') as f:
                    f.write(response.content)
                
                # Extract CSV from zip
                try:
                    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                        # List files in zip
                        zip_files = zip_ref.namelist()
                        csv_file = None
                        
                        # Find the CSV file
                        for file in zip_files:
                            if file.endswith('.csv'):
                                csv_file = file
                                break
                        
                        if csv_file:
                            # Extract CSV
                            zip_ref.extract(csv_file, self.download_dir)
                            
                            # Rename to standard format
                            old_path = os.path.join(self.download_dir, csv_file)
                            new_filename = f"fo_udiff_{date.strftime('%d%m%Y')}.csv"
                            new_path = os.path.join(self.download_dir, new_filename)
                            os.rename(old_path, new_path)
                            
                            print(f"✅ Successfully extracted: {new_filename}")
                            
                            # Remove zip file to save space
                            os.remove(zip_path)
                            
                            return True
                        else:
                            print(f"❌ No CSV found in {filename}")
                            return False
                            
                except zipfile.BadZipFile:
                    print(f"❌ Invalid zip file: {filename}")
                    return False
                    
            elif response.status_code == 404:
                print(f"❌ File not found: {filename} (Non-trading day or data not available)")
                return False
            else:
                print(f"❌ HTTP {response.status_code}: {filename}")
                return False
                
        except requests.exceptions.RequestException as e:
            print(f"❌ Network error for {filename}: {str(e)}")
            return False
        except Exception as e:
            print(f"❌ Unexpected error for {filename}: {str(e)}")
            return False
    
    def download_february_2025_data(self):
        """Download all F&O UDiFF data for February 2025"""
        print("🚀 NSE F&O UDiFF Bhavcopy Downloader - February 2025")
        print("=" * 60)
        
        self.setup_download_directory()
        
        trading_days = self.get_february_2025_trading_days()
        print(f"📅 Found {len(trading_days)} potential trading days in February 2025")
        
        successful_downloads = 0
        failed_downloads = []
        
        for i, date in enumerate(trading_days, 1):
            print(f"\n📊 Progress: {i}/{len(trading_days)}")
            
            url, filename = self.generate_fo_udiff_url(date)
            
            if self.download_file(url, filename, date):
                successful_downloads += 1
            else:
                failed_downloads.append(date.strftime('%d-%b-%Y'))
            
            # Add delay between downloads to be respectful
            time.sleep(1)
        
        # Summary
        print(f"\n🎉 Download Summary:")
        print("=" * 40)
        print(f"✅ Successful downloads: {successful_downloads}")
        print(f"❌ Failed downloads: {len(failed_downloads)}")
        
        if failed_downloads:
            print(f"\n📋 Failed dates:")
            for date in failed_downloads:
                print(f"   • {date}")
            
            # Save failed URLs for retry
            failed_file = os.path.join(self.download_dir, "failed_downloads_feb2025_fo.txt")
            with open(failed_file, 'w') as f:
                f.write("Failed F&O UDiFF downloads for February 2025:\n")
                for date in failed_downloads:
                    f.write(f"{date}\n")
            print(f"💾 Failed downloads saved to: {failed_file}")
        
        return successful_downloads > 0
    
    def analyze_downloaded_data(self):
        """Analyze the downloaded F&O data"""
        csv_files = [f for f in os.listdir(self.download_dir) if f.endswith('.csv')]
        
        if not csv_files:
            print("❌ No CSV files found to analyze")
            return
        
        print(f"\n📊 F&O Data Analysis:")
        print("=" * 40)
        print(f"📁 CSV files found: {len(csv_files)}")
        
        total_records = 0
        instruments = set()
        symbols = set()
        
        for csv_file in sorted(csv_files):
            try:
                file_path = os.path.join(self.download_dir, csv_file)
                df = pd.read_csv(file_path)
                
                records = len(df)
                total_records += records
                
                # Collect instrument types and symbols
                if 'INSTRUMENT' in df.columns:
                    instruments.update(df['INSTRUMENT'].unique())
                if 'SYMBOL' in df.columns:
                    symbols.update(df['SYMBOL'].unique())
                
                print(f"   📄 {csv_file}: {records:,} records")
                
            except Exception as e:
                print(f"   ❌ Error reading {csv_file}: {str(e)}")
        
        print(f"\n📈 Summary Statistics:")
        print(f"   📊 Total records: {total_records:,}")
        print(f"   🔧 Instrument types: {len(instruments)}")
        print(f"   📈 Unique symbols: {len(symbols)}")
        
        if instruments:
            print(f"\n🔧 Instrument Types Found:")
            for instrument in sorted(instruments):
                print(f"   • {instrument}")
        
        # Show sample data structure
        if csv_files:
            sample_file = os.path.join(self.download_dir, csv_files[0])
            try:
                df_sample = pd.read_csv(sample_file)
                print(f"\n📋 Data Structure (sample from {csv_files[0]}):")
                print(f"   Columns: {list(df_sample.columns)}")
                print(f"   Shape: {df_sample.shape}")
                
                if len(df_sample) > 0:
                    print(f"\n📄 Sample Records:")
                    print(df_sample.head(3).to_string(index=False))
                    
            except Exception as e:
                print(f"❌ Error analyzing sample: {str(e)}")
    
    def create_readme(self):
        """Create README file for F&O data"""
        readme_content = f"""# NSE F&O UDiFF Bhavcopy Data - February 2025

## 📊 Data Overview
This folder contains Futures & Options UDiFF Bhavcopy Final data downloaded from NSE India for February 2025.

## 📁 File Structure
- `fo_udiff_DDMMYYYY.csv` - Daily F&O UDiFF data files
- `failed_downloads_feb2025_fo.txt` - List of failed download dates (if any)

## 🔧 Data Format
F&O UDiFF Bhavcopy contains:
- **INSTRUMENT**: Futures (FUTIDX, FUTSTK) or Options (OPTIDX, OPTSTK)
- **SYMBOL**: Underlying symbol
- **EXPIRY_DT**: Contract expiry date
- **STRIKE_PR**: Strike price (for options)
- **OPTION_TYP**: Call/Put (for options)
- **OPEN**, **HIGH**, **LOW**, **CLOSE**: Price data
- **SETTLE_PR**: Settlement price
- **CONTRACTS**: Number of contracts traded
- **VAL_INLAKH**: Value in lakhs
- **OPEN_INT**: Open interest
- **CHG_IN_OI**: Change in open interest

## 📈 Usage
This F&O data complements the equity data from Steps 1-3:
1. **Step 1-3**: Equity cash market data
2. **Step 4**: F&O derivatives data (this folder)

## 🎯 Analysis Possibilities
- F&O trading volume analysis
- Options chain analysis
- Futures vs cash market comparison
- Open interest tracking
- Volatility analysis through options pricing

## 📅 Data Coverage
February 2025 trading days (excluding weekends and holidays)

*Downloaded: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*
*Project: NSE Data Analysis - Step 4*
"""
        
        readme_path = os.path.join(self.download_dir, "README.md")
        with open(readme_path, 'w') as f:
            f.write(readme_content)
        
        print(f"📄 Created README: {readme_path}")

def main():
    """Main download function"""
    print("📈 NSE F&O UDiFF Bhavcopy Downloader")
    print("Step 4: Futures & Options Data Collection")
    print("=" * 60)
    
    downloader = NSE_FO_UDiFF_Downloader()
    
    # Download February 2025 F&O data
    if downloader.download_february_2025_data():
        print("\n🎉 F&O data download completed!")
        
        # Analyze downloaded data
        downloader.analyze_downloaded_data()
        
        # Create documentation
        downloader.create_readme()
        
        print(f"\n📂 Data saved to: {downloader.download_dir}")
        print("\n✨ Step 4 completed successfully!")
        print("\n🚀 Next Steps:")
        print("   • Analyze F&O trading patterns")
        print("   • Compare with equity data from Steps 1-3")
        print("   • Create F&O specific analysis tools")
        
    else:
        print("❌ F&O data download failed!")

if __name__ == "__main__":
    main()
