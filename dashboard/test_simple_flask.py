import requests

def test_simple_flask():
    try:
        response = requests.get('http://localhost:5556/test', timeout=5)
        print(f"Simple Flask test: {response.status_code} - {response.json()}")
        return True
    except Exception as e:
        print(f"Simple Flask test failed: {e}")
        return False

if __name__ == '__main__':
    test_simple_flask()