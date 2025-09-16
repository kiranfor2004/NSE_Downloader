import requests
import time

def test_api():
    print("Testing API endpoints...")
    
    # Test health endpoint
    try:
        response = requests.get('http://localhost:5555/api/health', timeout=5)
        print(f"Health check: {response.status_code} - {response.json()}")
    except Exception as e:
        print(f"Health check failed: {e}")
    
    # Test basic data endpoint  
    try:
        response = requests.get('http://localhost:5555/api/basic-data', timeout=10)
        print(f"Basic data: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"  Total symbols: {data.get('total_symbols', 'N/A')}")
            print(f"  Average delivery: {data.get('avg_delivery', 'N/A')}")
            print(f"  Chart data points: {len(data.get('chart_data', []))}")
    except Exception as e:
        print(f"Basic data failed: {e}")

if __name__ == '__main__':
    test_api()