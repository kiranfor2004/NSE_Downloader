#!/usr/bin/env python3
"""
Diagnostic script to find the exact issue
"""
import socket
import time
import sys

def test_port_availability(port):
    """Test if a port is available"""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1)
        result = sock.connect_ex(('127.0.0.1', port))
        sock.close()
        if result == 0:
            return f"Port {port}: OCCUPIED"
        else:
            return f"Port {port}: AVAILABLE"
    except Exception as e:
        return f"Port {port}: ERROR - {e}"

def test_socket_server(port):
    """Create a simple socket server to test basic connectivity"""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(('127.0.0.1', port))
        sock.listen(1)
        print(f"✅ Socket server listening on port {port}")
        
        # Wait for connection for 10 seconds
        sock.settimeout(10)
        try:
            conn, addr = sock.accept()
            print(f"✅ Connection received from {addr}")
            conn.send(b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n\r\nHello World")
            conn.close()
        except socket.timeout:
            print("⚠️  No connection received within 10 seconds")
        
        sock.close()
        return True
        
    except Exception as e:
        print(f"❌ Socket server failed: {e}")
        return False

def test_flask_minimal():
    """Test minimal Flask app"""
    try:
        from flask import Flask
        app = Flask(__name__)
        
        @app.route('/test')
        def test():
            return "Hello from Flask!"
        
        print("✅ Flask app created successfully")
        
        # Try to run Flask
        print("🚀 Starting Flask on port 8080...")
        app.run(host='127.0.0.1', port=8080, debug=False, use_reloader=False)
        
    except Exception as e:
        print(f"❌ Flask test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    print("🔍 DIAGNOSTIC TEST - Finding the exact issue")
    print("=" * 50)
    
    # Test port availability
    for port in [5000, 5001, 8080, 8888]:
        print(test_port_availability(port))
    
    print("\n🔌 Testing socket server on port 8888...")
    if test_socket_server(8888):
        print("✅ Basic socket connectivity works")
    else:
        print("❌ Basic socket connectivity failed")
        sys.exit(1)
    
    print("\n🌐 Testing Flask...")
    test_flask_minimal()