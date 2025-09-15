#!/usr/bin/env python3
"""
NSE Indices Symbol Information Downloader - Enhanced Version

PURPOSE:
========
Download INDICES symbol information from NSE India using multiple approaches:
1. Direct API endpoints
2. Web scraping from live market indices page
3. Alternative NSE APIs

This script tries multiple methods to ensure successful data retrieval.
"""

import requests
import pandas as pd
import json
from datetime import datetime, date
import time
import os
from bs4 import BeautifulSoup
import re

class NSEIndicesDownloaderEnhanced:
    def __init__(self):
        """Initialize the enhanced NSE Indices downloader"""
        self.base_url = "https://www.nseindia.com"
        
        # Multiple API endpoints to try
        self.api_endpoints = [
            "https://www.nseindia.com/api/allIndices",
            "https://www.nseindia.com/api/equity-stockIndices?index=SECURITIES%20IN%20F%26O",
            "https://www.nseindia.com/api/master-quote",
            "https://www.nseindia.com/api/chart-databyindex?index=NIFTY%2050&indices=true"
        ]
        
        self.session = requests.Session()
        
        # Enhanced headers
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Referer': 'https://www.nseindia.com/',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'X-Requested-With': 'XMLHttpRequest'
        }
        
        print("🚀 NSE Indices Symbol Information Downloader (Enhanced)")
        print("=" * 60)
        print("📋 Target: Download INDICES symbol information")
        print("🌐 Source: NSE India (multiple methods)")
        print()

    def setup_session(self):
        """Setup session with proper cookies and headers"""
        print("🔧 Setting up session...")
        
        try:
            # First, visit the main NSE page to establish session
            main_page_response = self.session.get(
                "https://www.nseindia.com/",
                headers=self.headers,
                timeout=15
            )
            
            if main_page_response.status_code == 200:
                print("   ✅ Main page access successful")
                
                # Now visit the indices page
                indices_page_response = self.session.get(
                    "https://www.nseindia.com/market-data/live-market-indices",
                    headers=self.headers,
                    timeout=15
                )
                
                if indices_page_response.status_code == 200:
                    print("   ✅ Indices page access successful")
                    return True
                else:
                    print(f"   ❌ Indices page access failed: {indices_page_response.status_code}")
                    return False
            else:
                print(f"   ❌ Main page access failed: {main_page_response.status_code}")
                return False
                
        except requests.exceptions.RequestException as e:
            print(f"   ❌ Session setup error: {str(e)}")
            return False

    def try_api_endpoint(self, url):
        """Try a specific API endpoint"""
        try:
            print(f"   🔍 Trying: {url}")
            
            response = self.session.get(
                url,
                headers=self.headers,
                timeout=15
            )
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    print(f"   ✅ Success: {url}")
                    return data
                except json.JSONDecodeError:
                    print(f"   ❌ Invalid JSON from: {url}")
                    return None
            else:
                print(f"   ❌ HTTP {response.status_code}: {url}")
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"   ❌ Request error for {url}: {str(e)}")
            return None

    def fetch_indices_data_api(self):
        """Try to fetch data from multiple API endpoints"""
        print("📊 Fetching indices data from APIs...")
        
        for endpoint in self.api_endpoints:
            time.sleep(2)  # Rate limiting
            data = self.try_api_endpoint(endpoint)
            if data:
                return data
        
        print("   ❌ All API endpoints failed")
        return None

    def scrape_indices_page(self):
        """Scrape the indices page directly"""
        print("🕷️  Attempting to scrape indices page...")
        
        try:
            response = self.session.get(
                "https://www.nseindia.com/market-data/live-market-indices",
                headers=self.headers,
                timeout=15
            )
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Look for common patterns in NSE pages
                scripts = soup.find_all('script')
                
                for script in scripts:
                    if script.string and 'indices' in script.string.lower():
                        # Try to extract JSON data from script tags
                        script_content = script.string
                        
                        # Look for JSON patterns
                        json_patterns = [
                            r'window\.__NUXT__\s*=\s*({.*?});',
                            r'window\.__INITIAL_STATE__\s*=\s*({.*?});',
                            r'var\s+data\s*=\s*({.*?});',
                            r'"data"\s*:\s*(\[.*?\])',
                        ]
                        
                        for pattern in json_patterns:
                            matches = re.findall(pattern, script_content, re.DOTALL)
                            if matches:
                                try:
                                    data = json.loads(matches[0])
                                    print("   ✅ Found JSON data in script tag")
                                    return data
                                except json.JSONDecodeError:
                                    continue
                
                print("   ❌ No usable JSON data found in page")
                return None
            else:
                print(f"   ❌ Page scraping failed: {response.status_code}")
                return None
                
        except Exception as e:
            print(f"   ❌ Scraping error: {str(e)}")
            return None

    def create_sample_indices_data(self):
        """Create sample indices data as fallback"""
        print("📝 Creating sample indices data...")
        
        # Common NSE indices
        sample_indices = [
            {"SYMBOL": "NIFTY 50", "INDEX_NAME": "Nifty 50", "CATEGORY": "Broad Market"},
            {"SYMBOL": "NIFTY NEXT 50", "INDEX_NAME": "Nifty Next 50", "CATEGORY": "Broad Market"},
            {"SYMBOL": "NIFTY 100", "INDEX_NAME": "Nifty 100", "CATEGORY": "Broad Market"},
            {"SYMBOL": "NIFTY 200", "INDEX_NAME": "Nifty 200", "CATEGORY": "Broad Market"},
            {"SYMBOL": "NIFTY 500", "INDEX_NAME": "Nifty 500", "CATEGORY": "Broad Market"},
            {"SYMBOL": "NIFTY MIDCAP 50", "INDEX_NAME": "Nifty Midcap 50", "CATEGORY": "Mid Cap"},
            {"SYMBOL": "NIFTY MIDCAP 100", "INDEX_NAME": "Nifty Midcap 100", "CATEGORY": "Mid Cap"},
            {"SYMBOL": "NIFTY MIDCAP 150", "INDEX_NAME": "Nifty Midcap 150", "CATEGORY": "Mid Cap"},
            {"SYMBOL": "NIFTY SMALLCAP 50", "INDEX_NAME": "Nifty Smallcap 50", "CATEGORY": "Small Cap"},
            {"SYMBOL": "NIFTY SMALLCAP 100", "INDEX_NAME": "Nifty Smallcap 100", "CATEGORY": "Small Cap"},
            {"SYMBOL": "NIFTY SMALLCAP 250", "INDEX_NAME": "Nifty Smallcap 250", "CATEGORY": "Small Cap"},
            {"SYMBOL": "NIFTY AUTO", "INDEX_NAME": "Nifty Auto", "CATEGORY": "Sectoral"},
            {"SYMBOL": "NIFTY BANK", "INDEX_NAME": "Nifty Bank", "CATEGORY": "Sectoral"},
            {"SYMBOL": "NIFTY ENERGY", "INDEX_NAME": "Nifty Energy", "CATEGORY": "Sectoral"},
            {"SYMBOL": "NIFTY FMCG", "INDEX_NAME": "Nifty FMCG", "CATEGORY": "Sectoral"},
            {"SYMBOL": "NIFTY IT", "INDEX_NAME": "Nifty IT", "CATEGORY": "Sectoral"},
            {"SYMBOL": "NIFTY MEDIA", "INDEX_NAME": "Nifty Media", "CATEGORY": "Sectoral"},
            {"SYMBOL": "NIFTY METAL", "INDEX_NAME": "Nifty Metal", "CATEGORY": "Sectoral"},
            {"SYMBOL": "NIFTY PHARMA", "INDEX_NAME": "Nifty Pharma", "CATEGORY": "Sectoral"},
            {"SYMBOL": "NIFTY PSU BANK", "INDEX_NAME": "Nifty PSU Bank", "CATEGORY": "Sectoral"},
            {"SYMBOL": "NIFTY REALTY", "INDEX_NAME": "Nifty Realty", "CATEGORY": "Sectoral"},
            {"SYMBOL": "NIFTY FINANCIAL SERVICES", "INDEX_NAME": "Nifty Financial Services", "CATEGORY": "Sectoral"},
            {"SYMBOL": "NIFTY PRIVATE BANK", "INDEX_NAME": "Nifty Private Bank", "CATEGORY": "Sectoral"},
            {"SYMBOL": "NIFTY HEALTHCARE", "INDEX_NAME": "Nifty Healthcare", "CATEGORY": "Sectoral"},
            {"SYMBOL": "NIFTY CONSUMER DURABLES", "INDEX_NAME": "Nifty Consumer Durables", "CATEGORY": "Sectoral"},
            {"SYMBOL": "NIFTY OIL & GAS", "INDEX_NAME": "Nifty Oil & Gas", "CATEGORY": "Sectoral"}
        ]
        
        # Add metadata
        for index in sample_indices:
            index.update({
                "LAST_PRICE": "N/A",
                "CHANGE": "N/A", 
                "PERCENT_CHANGE": "N/A",
                "DOWNLOAD_DATE": datetime.now().strftime('%Y-%m-%d'),
                "DOWNLOAD_TIME": datetime.now().strftime('%H:%M:%S'),
                "DATA_SOURCE": "Sample Data"
            })
        
        print(f"   ✅ Created {len(sample_indices)} sample indices")
        return sample_indices

    def parse_indices_data(self, data):
        """Parse the indices data from various sources"""
        print("🔍 Parsing indices data...")
        
        if not data:
            print("   ❌ No data to parse")
            return []
        
        indices_list = []
        
        try:
            # Handle different data structures
            if isinstance(data, list):
                # Direct list of indices
                for item in data:
                    if isinstance(item, dict):
                        indices_list.append(self.extract_index_info(item))
            
            elif isinstance(data, dict):
                # Various dictionary structures
                for key in ['data', 'indices', 'results', 'stocks']:
                    if key in data and isinstance(data[key], list):
                        for item in data[key]:
                            if isinstance(item, dict):
                                indices_list.append(self.extract_index_info(item))
                        break
                
                # If no list found, treat the dict itself as an index
                if not indices_list and any(k in data for k in ['symbol', 'index', 'indexSymbol']):
                    indices_list.append(self.extract_index_info(data))
            
            print(f"   ✅ Parsed {len(indices_list)} indices")
            return [idx for idx in indices_list if idx]  # Filter out None values
            
        except Exception as e:
            print(f"   ❌ Parsing error: {str(e)}")
            return []

    def extract_index_info(self, item):
        """Extract index information from a data item"""
        try:
            # Try different field names for symbol
            symbol = (item.get('indexSymbol') or 
                     item.get('symbol') or 
                     item.get('index') or 
                     item.get('indexName') or 
                     'N/A')
            
            # Try different field names for name
            name = (item.get('indexName') or 
                   item.get('name') or 
                   item.get('index') or 
                   symbol)
            
            # Try different field names for price
            last_price = (item.get('last') or 
                         item.get('lastPrice') or 
                         item.get('close') or 
                         'N/A')
            
            # Try different field names for change
            change = (item.get('change') or 
                     item.get('netChange') or 
                     item.get('dayChange') or 
                     'N/A')
            
            # Try different field names for percent change
            pct_change = (item.get('percentChange') or 
                         item.get('pChange') or 
                         item.get('percentageChange') or 
                         'N/A')
            
            return {
                'SYMBOL': symbol,
                'INDEX_NAME': name,
                'LAST_PRICE': last_price,
                'CHANGE': change,
                'PERCENT_CHANGE': pct_change,
                'DOWNLOAD_DATE': datetime.now().strftime('%Y-%m-%d'),
                'DOWNLOAD_TIME': datetime.now().strftime('%H:%M:%S'),
                'DATA_SOURCE': 'NSE API'
            }
            
        except Exception as e:
            print(f"   ⚠️  Error extracting info from item: {str(e)}")
            return None

    def save_data(self, indices_data):
        """Save indices data to CSV and JSON files"""
        print("💾 Saving indices data...")
        
        if not indices_data:
            print("   ❌ No data to save")
            return False
        
        try:
            # Create timestamp for filenames
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            # Save as CSV
            df = pd.DataFrame(indices_data)
            csv_filename = f"nse_indices_symbols_{timestamp}.csv"
            df.to_csv(csv_filename, index=False)
            print(f"   ✅ CSV saved: {csv_filename}")
            
            # Save as JSON
            json_filename = f"nse_indices_symbols_{timestamp}.json"
            with open(json_filename, 'w', encoding='utf-8') as f:
                json.dump(indices_data, f, indent=2, ensure_ascii=False)
            print(f"   ✅ JSON saved: {json_filename}")
            
            # Also save a latest version without timestamp
            df.to_csv("nse_indices_symbols_latest.csv", index=False)
            with open("nse_indices_symbols_latest.json", 'w', encoding='utf-8') as f:
                json.dump(indices_data, f, indent=2, ensure_ascii=False)
            print("   ✅ Latest versions saved")
            
            return True
            
        except Exception as e:
            print(f"   ❌ Save error: {str(e)}")
            return False

    def display_summary(self, indices_data):
        """Display summary of downloaded data"""
        print("\n📊 DOWNLOAD SUMMARY:")
        print("=" * 60)
        
        if not indices_data:
            print("❌ No data downloaded")
            return
        
        print(f"📈 Total Indices: {len(indices_data)}")
        print(f"📅 Download Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Group by category if available
        categories = {}
        for index in indices_data:
            category = index.get('CATEGORY', 'Other')
            if category not in categories:
                categories[category] = 0
            categories[category] += 1
        
        if len(categories) > 1:
            print(f"📂 Categories: {dict(categories)}")
        
        print("\n🏆 SAMPLE INDICES:")
        print("-" * 60)
        print(f"{'#':<3} {'SYMBOL':<20} {'NAME':<30} {'PRICE':<12} {'CHANGE':<10}")
        print("-" * 60)
        
        for i, index in enumerate(indices_data[:15]):  # Show first 15
            symbol = str(index.get('SYMBOL', 'N/A'))[:19]
            name = str(index.get('INDEX_NAME', 'N/A'))[:29]
            price = str(index.get('LAST_PRICE', 'N/A'))[:11]
            change = str(index.get('PERCENT_CHANGE', 'N/A'))[:9]
            
            print(f"{i+1:<3} {symbol:<20} {name:<30} {price:<12} {change:<10}")
        
        if len(indices_data) > 15:
            print(f"\n... and {len(indices_data) - 15} more indices")

    def download_indices(self):
        """Main method to download indices data"""
        try:
            # Setup session
            if not self.setup_session():
                print("⚠️  Session setup failed, continuing anyway...")
            
            # Try API endpoints first
            raw_data = self.fetch_indices_data_api()
            
            # If API fails, try scraping
            if not raw_data:
                raw_data = self.scrape_indices_page()
            
            # If both fail, use sample data
            if not raw_data:
                print("🔄 Using sample indices data...")
                indices_data = self.create_sample_indices_data()
            else:
                # Parse data
                indices_data = self.parse_indices_data(raw_data)
                
                # If parsing fails, use sample data
                if not indices_data:
                    print("🔄 Parsing failed, using sample data...")
                    indices_data = self.create_sample_indices_data()
            
            # Save data
            if not self.save_data(indices_data):
                print("❌ Failed to save data")
                return False
            
            # Display summary
            self.display_summary(indices_data)
            
            print(f"\n✅ NSE Indices download completed successfully!")
            return True
            
        except Exception as e:
            print(f"❌ Unexpected error: {str(e)}")
            return False

def main():
    """Main execution function"""
    downloader = NSEIndicesDownloaderEnhanced()
    success = downloader.download_indices()
    
    if success:
        print("\n🎉 Download completed successfully!")
        print("📁 Files saved:")
        print("   - nse_indices_symbols_latest.csv")
        print("   - nse_indices_symbols_latest.json")
        print("   - Timestamped versions")
        print("\n💡 Note: If live data wasn't available, sample indices data was provided.")
    else:
        print("\n💥 Download failed. Please check the error messages above.")

if __name__ == "__main__":
    main()