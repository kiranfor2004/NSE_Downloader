import requests
import time

print("Waiting 2 seconds for server to fully start...")
time.sleep(2)

try:
    response = requests.get('http://localhost:5000/api/health', timeout=10)
    print(f"SUCCESS! Health check: {response.status_code} - {response.json()}")
    
    response = requests.get('http://localhost:5000/api/dashboard-data', timeout=10)
    data = response.json()
    print(f"Dashboard data loaded: {data['total_records']} records, {data['total_symbols']} symbols")
    
except Exception as e:
    print(f"Error: {e}")
    print("The server might not be fully started yet. Please try again.")