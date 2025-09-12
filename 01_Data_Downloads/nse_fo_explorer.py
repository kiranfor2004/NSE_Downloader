#!/usr/bin/env python3
"""
🔍 NSE F&O UDiFF Bhavcopy Explorer - Step 4
Explores available F&O UDiFF data and finds correct download patterns
"""

import requests
import os
from datetime import datetime, timedelta
import time
import zipfile
import io

class NSE_FO_UDiFF_Explorer:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive'
        })
        
        # Known F&O endpoints to test
        self.fo_base_urls = [
            "https://archives.nseindia.com/content/fo",
            "https://www1.nseindia.com/content/fo", 
            "https://archives.nseindia.com/products/content/fo",
            "https://www1.nseindia.com/products/content/fo"
        ]
    
    def test_fo_udiff_patterns(self):
        """Test different F&O UDiFF URL patterns"""
        print("🔍 Testing F&O UDiFF Bhavcopy Patterns")
        print("=" * 50)
        
        # Test with known working dates from 2024
        test_dates = [
            ("01", "08", "2024"),  # August 2024
            ("15", "08", "2024"),
            ("30", "08", "2024"),
            ("01", "09", "2024"),  # September 2024
            ("15", "09", "2024")
        ]
        
        # Different file naming patterns for F&O UDiFF
        patterns = [
            # Original pattern we tried
            "BhavCopy_NSE_FO_{dd}{mm}{yyyy}_F_0000.csv.zip",
            
            # Alternative patterns
            "fo{dd}{mm}{yyyy}.zip",
            "fo{dd}{mm}{yyyy}.csv.zip", 
            "BhavCopy_NSE_FO_{dd}{mm}{yyyy}.csv.zip",
            "udiff_fo_{dd}{mm}{yyyy}.csv.zip",
            "UDIFF_FO_{dd}{mm}{yyyy}.csv.zip",
            "nse_fo_udiff_{dd}{mm}{yyyy}.zip",
            
            # Historical patterns
            "bhav{dd}{mm}{yyyy}fo.zip",
            "fo_bhav_{dd}{mm}{yyyy}.zip",
            "FO_UDIFF_{dd}{mm}{yyyy}.csv.zip",
            
            # Without date formatting
            "fo{dd}{mm}{yy}.zip",
            "BhavCopy_NSE_FO_{dd}{mm}{yy}_F_0000.csv.zip"
        ]
        
        working_patterns = []
        
        for dd, mm, yyyy in test_dates:
            yy = yyyy[2:]  # Last 2 digits of year
            print(f"\n📅 Testing date: {dd}-{mm}-{yyyy}")
            
            for base_url in self.fo_base_urls:
                print(f"   🔗 Base URL: {base_url}")
                
                for i, pattern in enumerate(patterns, 1):
                    filename = pattern.format(dd=dd, mm=mm, yyyy=yyyy, yy=yy)
                    url = f"{base_url}/{filename}"
                    
                    try:
                        response = self.session.head(url, timeout=5)
                        status = response.status_code
                        
                        if status == 200:
                            print(f"      ✅ Pattern {i:2d}: {filename}")
                            working_patterns.append({
                                'pattern': pattern,
                                'base_url': base_url,
                                'test_date': f"{dd}-{mm}-{yyyy}",
                                'url': url,
                                'filename': filename
                            })
                        else:
                            print(f"      ❌ Pattern {i:2d}: {status} - {filename}")
                            
                    except Exception as e:
                        print(f"      ❌ Pattern {i:2d}: Error - {filename}")
                    
                    time.sleep(0.3)  # Be respectful
        
        return working_patterns
    
    def verify_working_pattern(self, pattern_info):
        """Download and verify a working pattern"""
        print(f"\n🔍 Verifying pattern: {pattern_info['filename']}")
        
        try:
            response = self.session.get(pattern_info['url'], timeout=15)
            
            if response.status_code == 200:
                print(f"   ✅ Download successful: {len(response.content)} bytes")
                
                # Try to extract and examine the ZIP
                try:
                    zip_file = zipfile.ZipFile(io.BytesIO(response.content))
                    files_in_zip = zip_file.namelist()
                    
                    print(f"   📁 Files in ZIP: {files_in_zip}")
                    
                    if files_in_zip:
                        # Read first CSV file
                        first_file = files_in_zip[0]
                        csv_content = zip_file.read(first_file).decode('utf-8')
                        lines = csv_content.split('\n')
                        
                        print(f"   📊 CSV lines: {len(lines)}")
                        print(f"   📋 Header: {lines[0][:100]}...")
                        
                        if len(lines) > 1:
                            print(f"   📄 Sample: {lines[1][:100]}...")
                        
                        return True
                        
                except Exception as e:
                    print(f"   ❌ ZIP extraction error: {e}")
                    return False
            else:
                print(f"   ❌ Download failed: {response.status_code}")
                return False
                
        except Exception as e:
            print(f"   ❌ Request error: {e}")
            return False
    
    def check_fo_data_availability(self):
        """Check what F&O data months are actually available"""
        print(f"\n📅 Checking F&O Data Availability")
        print("=" * 50)
        
        # Check months in 2024 (should be available)
        months_to_check = [
            ("2024", "01"), ("2024", "02"), ("2024", "03"),
            ("2024", "04"), ("2024", "05"), ("2024", "06"),
            ("2024", "07"), ("2024", "08"), ("2024", "09"),
            ("2024", "10"), ("2024", "11"), ("2024", "12")
        ]
        
        available_months = []
        
        # Use the most likely patterns based on equity data structure
        test_patterns = [
            "fo{dd}{mm}{yyyy}.zip",
            "BhavCopy_NSE_FO_{dd}{mm}{yyyy}_F_0000.csv.zip"
        ]
        
        for year, month in months_to_check:
            print(f"\n🗓️ Checking {year}-{month}...")
            
            # Test with 15th of each month (likely trading day)
            test_date = ("15", month, year)
            dd, mm, yyyy = test_date
            
            month_available = False
            
            for base_url in self.fo_base_urls[:2]:  # Test main URLs only
                for pattern in test_patterns:
                    filename = pattern.format(dd=dd, mm=mm, yyyy=yyyy)
                    url = f"{base_url}/{filename}"
                    
                    try:
                        response = self.session.head(url, timeout=5)
                        if response.status_code == 200:
                            print(f"   ✅ Available: {filename}")
                            available_months.append(f"{year}-{month}")
                            month_available = True
                            break
                    except:
                        continue
                    
                    time.sleep(0.2)
                
                if month_available:
                    break
            
            if not month_available:
                print(f"   ❌ Not available: {year}-{month}")
        
        return available_months
    
    def suggest_download_strategy(self, working_patterns, available_months):
        """Suggest the best download strategy"""
        print(f"\n💡 Download Strategy Recommendations")
        print("=" * 50)
        
        if working_patterns:
            best_pattern = working_patterns[0]
            print(f"✅ Recommended Pattern:")
            print(f"   📋 Pattern: {best_pattern['pattern']}")
            print(f"   🔗 Base URL: {best_pattern['base_url']}")
            print(f"   ✓ Verified with: {best_pattern['test_date']}")
            
            if available_months:
                print(f"\n📅 Available Months for Download:")
                for month in available_months[-6:]:  # Show last 6 months
                    print(f"   • {month}")
                
                # Suggest specific month to download
                latest_month = available_months[-1]
                year, month = latest_month.split('-')
                
                print(f"\n🎯 Recommended Action:")
                print(f"   • Download F&O data for {latest_month}")
                print(f"   • Use pattern: {best_pattern['pattern']}")
                print(f"   • Start with most recent available month")
                
                return {
                    'pattern': best_pattern['pattern'],
                    'base_url': best_pattern['base_url'],
                    'recommended_month': latest_month,
                    'available_months': available_months
                }
        
        print(f"\n❌ No working patterns found")
        print(f"💡 Alternative Approaches:")
        print(f"   • Manual download from NSE website")
        print(f"   • Check NSE's current F&O data section")
        print(f"   • F&O data might be in different location")
        
        return None

def main():
    """Main exploration function"""
    print("🔍 NSE F&O UDiFF Bhavcopy Explorer - Step 4")
    print("=" * 60)
    print("🎯 Goal: Find working F&O UDiFF data sources")
    print()
    
    explorer = NSE_FO_UDiFF_Explorer()
    
    # Step 1: Test URL patterns
    print("Step 1: Testing F&O URL patterns...")
    working_patterns = explorer.test_fo_udiff_patterns()
    
    # Step 2: Verify a working pattern
    if working_patterns:
        print(f"\n✅ Found {len(working_patterns)} working patterns!")
        
        # Verify the first working pattern
        verified = explorer.verify_working_pattern(working_patterns[0])
        
        if verified:
            print("✅ Pattern verification successful!")
        else:
            print("⚠️ Pattern verification failed")
    
    # Step 3: Check data availability
    print("\nStep 2: Checking data availability...")
    available_months = explorer.check_fo_data_availability()
    
    # Step 4: Suggest strategy
    strategy = explorer.suggest_download_strategy(working_patterns, available_months)
    
    # Final summary
    print(f"\n📊 EXPLORATION SUMMARY")
    print("=" * 30)
    print(f"Working patterns: {len(working_patterns)}")
    print(f"Available months: {len(available_months)}")
    
    if strategy:
        print(f"✅ Ready to download F&O data!")
        print(f"📅 Recommended: {strategy['recommended_month']}")
    else:
        print(f"❌ Need alternative approach for F&O data")
    
    return strategy

if __name__ == "__main__":
    strategy = main()
    
    if strategy:
        print(f"\n🚀 Next step: Create F&O downloader with working pattern")
        print(f"📋 Pattern: {strategy['pattern']}")
        print(f"📅 Month: {strategy['recommended_month']}")
    else:
        print(f"\n⏸️ F&O download requires further investigation")
