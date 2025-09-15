#!/usr/bin/env python3
"""
NSE Delivery Analysis Dashboard Startup Script
Checks prerequisites and starts the dashboard server
"""

import sys
import os
import subprocess
import json
import time
import webbrowser
from pathlib import Path

def check_python_version():
    """Check if Python version is 3.7+"""
    if sys.version_info < (3, 7):
        print("❌ Error: Python 3.7 or higher is required")
        print(f"Current version: {sys.version}")
        return False
    print(f"✅ Python version: {sys.version.split()[0]}")
    return True

def check_database_config():
    """Check if database configuration exists"""
    config_path = Path("../database_config.json")
    if not config_path.exists():
        print("❌ Error: database_config.json not found in parent directory")
        print("Please ensure your database configuration file exists")
        return False
    
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        print("✅ Database configuration found")
        return True
    except json.JSONDecodeError:
        print("❌ Error: Invalid JSON in database_config.json")
        return False
    except Exception as e:
        print(f"❌ Error reading database config: {e}")
        return False

def check_dependencies():
    """Check if required Python packages are installed"""
    required_packages = ['flask', 'flask_cors', 'pyodbc']
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package)
            print(f"✅ {package} is installed")
        except ImportError:
            missing_packages.append(package)
            print(f"❌ {package} is not installed")
    
    if missing_packages:
        print(f"\n📦 Installing missing packages: {', '.join(missing_packages)}")
        try:
            subprocess.check_call([
                sys.executable, "-m", "pip", "install", 
                "flask", "flask-cors", "pyodbc"
            ])
            print("✅ Dependencies installed successfully")
            return True
        except subprocess.CalledProcessError as e:
            print(f"❌ Failed to install dependencies: {e}")
            return False
    
    return True

def start_api_server():
    """Start the Flask API server"""
    print("\n🚀 Starting NSE Dashboard API server...")
    try:
        # Start API in background
        api_process = subprocess.Popen([
            sys.executable, "api.py"
        ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        # Give server time to start
        time.sleep(3)
        
        # Check if process is still running
        if api_process.poll() is None:
            print("✅ API server started successfully on http://localhost:5000")
            return api_process
        else:
            stdout, stderr = api_process.communicate()
            print(f"❌ API server failed to start")
            print(f"Error: {stderr.decode()}")
            return None
    except Exception as e:
        print(f"❌ Failed to start API server: {e}")
        return None

def open_dashboard():
    """Open the dashboard in the default web browser"""
    dashboard_path = Path("index.html").absolute()
    dashboard_url = f"file://{dashboard_path}"
    
    print(f"\n🌐 Opening dashboard: {dashboard_url}")
    try:
        webbrowser.open(dashboard_url)
        print("✅ Dashboard opened in browser")
        return True
    except Exception as e:
        print(f"❌ Failed to open browser: {e}")
        print(f"Please manually open: {dashboard_url}")
        return False

def main():
    """Main startup function"""
    print("🎯 NSE Delivery Analysis Dashboard Startup")
    print("=" * 50)
    
    # Check prerequisites
    if not check_python_version():
        sys.exit(1)
    
    if not check_database_config():
        sys.exit(1)
    
    if not check_dependencies():
        sys.exit(1)
    
    # Start API server
    api_process = start_api_server()
    if not api_process:
        sys.exit(1)
    
    # Open dashboard
    open_dashboard()
    
    print("\n" + "=" * 50)
    print("🎉 Dashboard is ready!")
    print("📊 Dashboard URL: file://" + str(Path("index.html").absolute()))
    print("🔌 API URL: http://localhost:5000")
    print("📖 API Health Check: http://localhost:5000/api/health")
    print("\n💡 Tips:")
    print("   - Refresh the browser if data doesn't load immediately")
    print("   - Check the API health endpoint if you experience issues")
    print("   - Press Ctrl+C to stop the API server")
    print("=" * 50)
    
    try:
        # Keep the API server running
        print("\n⏳ API server is running... Press Ctrl+C to stop")
        api_process.wait()
    except KeyboardInterrupt:
        print("\n🛑 Shutting down API server...")
        api_process.terminate()
        try:
            api_process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            api_process.kill()
        print("✅ API server stopped")

if __name__ == "__main__":
    main()