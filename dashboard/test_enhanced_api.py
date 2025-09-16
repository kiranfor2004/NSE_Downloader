"""
Test script for Enhanced NSE Delivery Analysis Dashboard
Tests all API endpoints to ensure functionality
"""

import requests
import json
from datetime import datetime

API_BASE_URL = 'http://localhost:5001/api'

def test_endpoint(endpoint, description):
    """Test a single endpoint and return results"""
    try:
        print(f"Testing: {description}")
        print(f"URL: {API_BASE_URL}{endpoint}")
        
        response = requests.get(f"{API_BASE_URL}{endpoint}", timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            print(f"✅ SUCCESS - Status: {response.status_code}")
            
            # Show some basic info about the response
            if isinstance(data, dict):
                if 'timestamp' in data:
                    print(f"   Timestamp: {data['timestamp']}")
                if 'data' in data:
                    print(f"   Data records: {len(data['data']) if isinstance(data['data'], list) else 'N/A'}")
                elif isinstance(data, dict) and len(data) > 0:
                    print(f"   Response keys: {list(data.keys())[:5]}...")  # Show first 5 keys
            
            print()
            return True
        else:
            print(f"❌ FAILED - Status: {response.status_code}")
            print(f"   Error: {response.text}")
            print()
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"❌ CONNECTION ERROR: {e}")
        print()
        return False
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: {e}")
        print()
        return False

def main():
    print("=" * 60)
    print("NSE Delivery Analysis Dashboard - API Test Suite")
    print("=" * 60)
    print()
    
    # Test endpoints
    endpoints = [
        # Basic endpoints
        ('/health', 'Health Check'),
        ('/available-symbols', 'Available Symbols'),
        ('/available-dates', 'Available Trading Dates'),
        
        # Tab 1 endpoints
        ('/tab1/daily-performance', 'Tab 1: Daily Performance KPIs'),
        ('/tab1/heatmap-data', 'Tab 1: Heatmap Data'),
        
        # Tab 2 endpoints
        ('/tab2/monthly-trends', 'Tab 2: Monthly Trends'),
        ('/tab2/category-comparison', 'Tab 2: Category Comparison'),
    ]
    
    successful_tests = 0
    total_tests = len(endpoints)
    
    for endpoint, description in endpoints:
        if test_endpoint(endpoint, description):
            successful_tests += 1
    
    # Test symbol-specific endpoints (need to get a symbol first)
    print("Testing symbol-specific endpoints...")
    try:
        # Get available symbols first
        symbols_response = requests.get(f"{API_BASE_URL}/available-symbols", timeout=10)
        if symbols_response.status_code == 200:
            symbols_data = symbols_response.json()
            if symbols_data.get('symbols') and len(symbols_data['symbols']) > 0:
                test_symbol = symbols_data['symbols'][0]['symbol']
                print(f"Using test symbol: {test_symbol}")
                print()
                
                symbol_endpoints = [
                    (f'/tab1/candlestick-data/{test_symbol}', f'Tab 1: Candlestick Data for {test_symbol}'),
                    (f'/tab3/symbol-detail/{test_symbol}', f'Tab 3: Symbol Detail for {test_symbol}'),
                    (f'/tab3/symbol-correlation/{test_symbol}', f'Tab 3: Symbol Correlation for {test_symbol}'),
                ]
                
                for endpoint, description in symbol_endpoints:
                    if test_endpoint(endpoint, description):
                        successful_tests += 1
                    total_tests += 1
            else:
                print("⚠️  No symbols available for testing symbol-specific endpoints")
        else:
            print("⚠️  Could not fetch symbols for testing")
    except Exception as e:
        print(f"⚠️  Error testing symbol-specific endpoints: {e}")
    
    # Summary
    print("=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    print(f"Total Tests: {total_tests}")
    print(f"Successful: {successful_tests}")
    print(f"Failed: {total_tests - successful_tests}")
    print(f"Success Rate: {(successful_tests / total_tests * 100):.1f}%")
    
    if successful_tests == total_tests:
        print("\n🎉 ALL TESTS PASSED! Dashboard is ready to use.")
        print(f"\n🌐 Access the dashboard at: http://localhost:5001")
    else:
        print(f"\n⚠️  {total_tests - successful_tests} tests failed. Please check the API server.")
    
    print("\n" + "=" * 60)

if __name__ == "__main__":
    main()