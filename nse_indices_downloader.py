#!/usr/bin/env python3
"""
NSE Indices Symbol Information Downloader

PURPOSE:
========
Download INDICES symbol information from NSE India live market indices page
URL: https://www.nseindia.com/market-data/live-market-indices

This script fetches the live market indices data and extracts symbol information.
"""

import requests
import pandas as pd
import json
from datetime import datetime, date
import time
import os

class NSEIndicesDownloader:
    def __init__(self):
        """Initialize the NSE Indices downloader"""
        self.base_url = "https://www.nseindia.com"
        self.indices_url = "https://www.nseindia.com/api/allIndices"
        self.session = requests.Session()
        
        # Set headers to mimic a browser request
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Referer': 'https://www.nseindia.com/market-data/live-market-indices',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache'
        }
        
        print("🚀 NSE Indices Symbol Information Downloader")
        print("=" * 50)
        print("📋 Target: Download INDICES symbol information")
        print("🌐 Source: NSE India live market indices")
        print()

    def setup_session(self):
        """Setup session with proper cookies and headers"""
        print("🔧 Setting up session...")
        
        try:
            # First, visit the main page to get cookies
            response = self.session.get(
                "https://www.nseindia.com/market-data/live-market-indices",
                headers=self.headers,
                timeout=10
            )
            
            if response.status_code == 200:
                print("   ✅ Session setup successful")
                return True
            else:
                print(f"   ❌ Session setup failed: {response.status_code}")
                return False
                
        except requests.exceptions.RequestException as e:
            print(f"   ❌ Session setup error: {str(e)}")
            return False

    def fetch_indices_data(self):
        """Fetch indices data from NSE API"""
        print("📊 Fetching indices data...")
        
        try:
            # Add a small delay
            time.sleep(1)
            
            response = self.session.get(
                self.indices_url,
                headers=self.headers,
                timeout=15
            )
            
            if response.status_code == 200:
                data = response.json()
                print(f"   ✅ Successfully fetched indices data")
                return data
            else:
                print(f"   ❌ Failed to fetch data: HTTP {response.status_code}")
                print(f"   Response: {response.text[:200]}...")
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"   ❌ Request error: {str(e)}")
            return None
        except json.JSONDecodeError as e:
            print(f"   ❌ JSON decode error: {str(e)}")
            return None

    def parse_indices_data(self, data):
        """Parse the indices data to extract symbol information"""
        print("🔍 Parsing indices data...")
        
        if not data:
            print("   ❌ No data to parse")
            return []
        
        indices_list = []
        
        try:
            # The data structure may vary, let's explore it
            print(f"   📋 Data keys: {list(data.keys())}")
            
            # Common NSE API structure
            if 'data' in data:
                indices_data = data['data']
            else:
                indices_data = data
            
            # If it's a list
            if isinstance(indices_data, list):
                for index_info in indices_data:
                    if isinstance(index_info, dict):
                        symbol = index_info.get('index', index_info.get('indexSymbol', index_info.get('symbol', 'N/A')))
                        name = index_info.get('indexName', index_info.get('name', symbol))
                        last_price = index_info.get('last', index_info.get('lastPrice', 'N/A'))
                        change = index_info.get('change', index_info.get('netChange', 'N/A'))
                        pct_change = index_info.get('percentChange', index_info.get('pChange', 'N/A'))
                        
                        indices_list.append({
                            'SYMBOL': symbol,
                            'INDEX_NAME': name,
                            'LAST_PRICE': last_price,
                            'CHANGE': change,
                            'PERCENT_CHANGE': pct_change,
                            'DOWNLOAD_DATE': datetime.now().strftime('%Y-%m-%d'),
                            'DOWNLOAD_TIME': datetime.now().strftime('%H:%M:%S')
                        })
            
            # If it's a dict with indices
            elif isinstance(indices_data, dict):
                for key, value in indices_data.items():
                    if isinstance(value, list):
                        for index_info in value:
                            if isinstance(index_info, dict):
                                symbol = index_info.get('index', index_info.get('indexSymbol', index_info.get('symbol', key)))
                                name = index_info.get('indexName', index_info.get('name', symbol))
                                last_price = index_info.get('last', index_info.get('lastPrice', 'N/A'))
                                change = index_info.get('change', index_info.get('netChange', 'N/A'))
                                pct_change = index_info.get('percentChange', index_info.get('pChange', 'N/A'))
                                
                                indices_list.append({
                                    'SYMBOL': symbol,
                                    'INDEX_NAME': name,
                                    'LAST_PRICE': last_price,
                                    'CHANGE': change,
                                    'PERCENT_CHANGE': pct_change,
                                    'DOWNLOAD_DATE': datetime.now().strftime('%Y-%m-%d'),
                                    'DOWNLOAD_TIME': datetime.now().strftime('%H:%M:%S')
                                })
            
            print(f"   ✅ Parsed {len(indices_list)} indices")
            return indices_list
            
        except Exception as e:
            print(f"   ❌ Parsing error: {str(e)}")
            return []

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
        print("=" * 50)
        
        if not indices_data:
            print("❌ No data downloaded")
            return
        
        print(f"📈 Total Indices: {len(indices_data)}")
        print(f"📅 Download Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        print("\n🏆 SAMPLE INDICES:")
        print("-" * 50)
        
        for i, index in enumerate(indices_data[:10]):  # Show first 10
            symbol = index.get('SYMBOL', 'N/A')
            name = index.get('INDEX_NAME', 'N/A')
            price = index.get('LAST_PRICE', 'N/A')
            change = index.get('PERCENT_CHANGE', 'N/A')
            
            print(f"{i+1:2d}. {symbol:15s} | {name[:30]:30s} | {str(price):>10s} | {str(change):>8s}%")
        
        if len(indices_data) > 10:
            print(f"    ... and {len(indices_data) - 10} more indices")

    def download_indices(self):
        """Main method to download indices data"""
        try:
            # Setup session
            if not self.setup_session():
                print("❌ Failed to setup session")
                return False
            
            # Fetch data
            raw_data = self.fetch_indices_data()
            if not raw_data:
                print("❌ Failed to fetch indices data")
                return False
            
            # Parse data
            indices_data = self.parse_indices_data(raw_data)
            if not indices_data:
                print("❌ Failed to parse indices data")
                return False
            
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
    downloader = NSEIndicesDownloader()
    success = downloader.download_indices()
    
    if success:
        print("\n🎉 Download completed successfully!")
        print("📁 Files saved:")
        print("   - nse_indices_symbols_latest.csv")
        print("   - nse_indices_symbols_latest.json")
        print("   - Timestamped versions")
    else:
        print("\n💥 Download failed. Please check the error messages above.")

if __name__ == "__main__":
    main()