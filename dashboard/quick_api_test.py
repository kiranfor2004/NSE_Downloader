import requests
import json

def test_health():
    try:
        response = requests.get('http://localhost:5001/api/health', timeout=5)
        print(f"Health Status: {response.status_code}")
        print(f"Response: {response.json()}")
        return True
    except Exception as e:
        print(f"Health check failed: {e}")
        return False

def test_daily_performance():
    try:
        response = requests.get('http://localhost:5001/api/tab1/daily-performance', timeout=10)
        print(f"Daily Performance Status: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"Keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}")
        else:
            print(f"Error: {response.text}")
        return response.status_code == 200
    except Exception as e:
        print(f"Daily performance test failed: {e}")
        return False

if __name__ == "__main__":
    print("=== API Quick Test ===")
    
    health_ok = test_health()
    print()
    
    if health_ok:
        perf_ok = test_daily_performance()
        print()
        
        if perf_ok:
            print("✅ API appears to be working!")
        else:
            print("❌ API data endpoints have issues")
    else:
        print("❌ API health check failed")