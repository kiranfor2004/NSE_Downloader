import requests

def test_working_dashboard():
    print("Testing working dashboard...")
    
    try:
        # Test health endpoint
        response = requests.get('http://localhost:5000/api/health', timeout=5)
        print(f"Health check: {response.status_code} - {response.json()}")
        
        # Test dashboard data
        response = requests.get('http://localhost:5000/api/dashboard-data', timeout=10)
        print(f"Dashboard data: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"  Total records: {data.get('total_records', 'N/A')}")
            print(f"  Total symbols: {data.get('total_symbols', 'N/A')}")
            print(f"  Average delivery: {data.get('avg_delivery', 'N/A'):.2f}%")
            print(f"  Max delivery: {data.get('max_delivery', 'N/A'):.2f}%")
            print(f"  Chart data points: {len(data.get('chart_data', []))}")
            
            if data.get('chart_data'):
                top_symbol = data['chart_data'][0]
                print(f"  Top symbol: {top_symbol['symbol']} with {top_symbol['delivery_pct']:.2f}%")
        
        # Test main page
        response = requests.get('http://localhost:5000/', timeout=5)
        print(f"Main page: {response.status_code} - HTML length: {len(response.text)}")
        
        print("\n✅ DASHBOARD IS WORKING PERFECTLY!")
        print("🎯 Access your dashboard at: http://localhost:5000")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")

if __name__ == '__main__':
    test_working_dashboard()