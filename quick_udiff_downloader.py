"""
Quick NSE F&O UDiFF Single Day Downloader
Fast download and analysis of F&O - UDiFF Common Bhavcopy Final files
"""
import requests
import zipfile
import pandas as pd
import os
from datetime import datetime
import io

def quick_download_udiff(date_str):
    """Quick download of UDiFF file for analysis"""
    
    # Convert date format if needed
    if '-' in date_str:
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        formatted_date = date_obj.strftime('%d%m%Y')
    else:
        formatted_date = date_str
    
    # Correct NSE UDiFF URL
    base_url = "https://archives.nseindia.com/products/content/derivatives/equities/"
    filename = f"udiff_{formatted_date}.zip"
    url = base_url + filename
    
    print(f"🚀 Quick UDiFF Download for {date_str}")
    print(f"=" * 50)
    print(f"📁 File: {filename}")
    print(f"🌐 URL: {url}")
    
    # Headers to mimic browser
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
    }
    
    try:
        print(f"📥 Downloading...")
        response = requests.get(url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            file_size = len(response.content)
            print(f"✅ Downloaded: {file_size:,} bytes")
            
            # Create download directory
            os.makedirs("Quick_UDiFF_Downloads", exist_ok=True)
            local_path = os.path.join("Quick_UDiFF_Downloads", filename)
            
            # Save file
            with open(local_path, 'wb') as f:
                f.write(response.content)
            print(f"💾 Saved to: {local_path}")
            
            # Quick analysis
            print(f"\n📊 Quick Analysis:")
            print(f"-" * 30)
            
            with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
                file_list = zip_file.namelist()
                print(f"📁 Files in ZIP: {file_list}")
                
                # Find CSV files
                csv_files = [f for f in file_list if f.endswith('.csv')]
                
                if csv_files:
                    csv_file = csv_files[0]
                    print(f"📋 Processing: {csv_file}")
                    
                    # Read CSV
                    with zip_file.open(csv_file) as csv_data:
                        try:
                            df = pd.read_csv(csv_data, encoding='utf-8')
                        except:
                            csv_data.seek(0)
                            df = pd.read_csv(csv_data, encoding='latin-1')
                    
                    print(f"📊 Total records: {len(df):,}")
                    print(f"📋 Columns: {list(df.columns)}")
                    
                    # Show sample data
                    print(f"\n📈 Sample data (first 3 rows):")
                    print(df.head(3).to_string())
                    
                    # Check for F&O instruments
                    if 'INSTRUMENT' in df.columns:
                        instruments = df['INSTRUMENT'].value_counts()
                        print(f"\n🔍 Instruments breakdown:")
                        for inst, count in instruments.items():
                            print(f"  {inst}: {count:,} records")
                    
                    # Check symbols
                    symbol_col = 'SYMBOL' if 'SYMBOL' in df.columns else 'TckrSymb'
                    if symbol_col in df.columns:
                        top_symbols = df[symbol_col].value_counts().head(10)
                        print(f"\n🏆 Top 10 symbols:")
                        for symbol, count in top_symbols.items():
                            print(f"  {symbol}: {count} records")
                    
                    return True, local_path, df
                else:
                    print(f"❌ No CSV files found in ZIP")
                    return False, local_path, None
                    
        elif response.status_code == 404:
            print(f"❌ File not found (404) - Date may not exist: {filename}")
            return False, None, None
        else:
            print(f"❌ HTTP Error {response.status_code}")
            return False, None, None
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return False, None, None

def main():
    print("🎯 NSE F&O UDiFF Quick Downloader")
    print("Downloads: F&O - UDiFF Common Bhavcopy Final (zip)")
    print("=" * 60)
    
    # Test with recent dates
    test_dates = [
        "2025-08-01",  # Aug 1, 2025
        "2025-07-01",  # Jul 1, 2025
        "2025-06-03",  # Jun 3, 2025
    ]
    
    print("📅 Testing recent dates:")
    for date in test_dates:
        print(f"\n" + "="*60)
        success, path, data = quick_download_udiff(date)
        if success:
            print(f"✅ SUCCESS for {date}")
            break
        else:
            print(f"❌ FAILED for {date}")
    
    # Manual date input
    print(f"\n" + "="*60)
    print(f"📝 Or enter your own date:")
    user_date = input("Enter date (YYYY-MM-DD or DDMMYYYY, or press Enter to skip): ").strip()
    
    if user_date:
        print(f"\n" + "="*60)
        success, path, data = quick_download_udiff(user_date)
        if success:
            print(f"\n🎯 SUCCESS! UDiFF file downloaded and analyzed")
            print(f"📍 File location: {path}")
        else:
            print(f"\n❌ Failed to download UDiFF file for {user_date}")

if __name__ == "__main__":
    main()
