#!/usr/bin/env python3
"""
🔧 Simple PostgreSQL Setup
Direct approach to set up PostgreSQL for NSE data
"""

import subprocess
import os
import json
import getpass

def test_postgres_connection(password=""):
    """Test connection to PostgreSQL"""
    try:
        psql_path = r"C:\Program Files\PostgreSQL\17\bin\psql.exe"
        
        # Set password environment variable
        env = os.environ.copy()
        if password:
            env['PGPASSWORD'] = password
        
        # Try to connect
        result = subprocess.run([
            psql_path, "-h", "localhost", "-U", "postgres", 
            "-d", "postgres", "-c", "SELECT 1;"
        ], capture_output=True, text=True, env=env, timeout=10)
        
        return result.returncode == 0, result.stderr
        
    except Exception as e:
        return False, str(e)

def try_default_passwords():
    """Try common default PostgreSQL passwords"""
    default_passwords = [
        "",           # No password
        "postgres",   # Common default
        "admin",      # Another common default
        "password",   # Generic default
        "123456"      # Numeric default
    ]
    
    print("🔍 Trying common default passwords...")
    
    for pwd in default_passwords:
        print(f"   Testing: {'(no password)' if pwd == '' else pwd}")
        success, error = test_postgres_connection(pwd)
        
        if success:
            print(f"✅ Success! Working password: {'(no password)' if pwd == '' else pwd}")
            return pwd
        else:
            print(f"   ❌ Failed: {error.strip()[:50]}...")
    
    return None

def setup_postgres_password():
    """Interactive password setup"""
    print("\n🔐 PostgreSQL Password Setup")
    print("=" * 40)
    
    # First try default passwords
    working_password = try_default_passwords()
    
    if working_password is not None:
        # Update config with working password
        update_config(working_password)
        return True
    
    print("\n❌ No default passwords worked.")
    print("\n📋 Manual Setup Options:")
    print("1. The PostgreSQL installation might need initial setup")
    print("2. Try setting a password manually")
    print("3. Use Windows Authentication")
    
    choice = input("\nChoose option (1-3): ").strip()
    
    if choice == "1":
        print("\n🛠️ Initial PostgreSQL Setup:")
        print("1. Open Command Prompt as Administrator")
        print("2. Run: net stop postgresql-x64-17")
        print("3. Navigate to PostgreSQL bin folder:")
        print('   cd "C:\\Program Files\\PostgreSQL\\17\\bin"')
        print("4. Initialize with trust authentication:")
        print('   initdb -D "C:\\Program Files\\PostgreSQL\\17\\data" -A trust')
        print("5. Start service: net start postgresql-x64-17")
        print("6. Run this script again")
        
    elif choice == "2":
        password = getpass.getpass("Enter password to try: ")
        success, error = test_postgres_connection(password)
        
        if success:
            print("✅ Password works!")
            update_config(password)
            return True
        else:
            print(f"❌ Failed: {error}")
            
    elif choice == "3":
        print("\n🪟 Windows Authentication Setup:")
        print("This would require modifying pg_hba.conf file.")
        print("For now, let's try a simpler approach.")
    
    return False

def update_config(password):
    """Update database configuration with working password"""
    config = {
        "host": "localhost",
        "port": 5432,
        "database": "nse_data",
        "username": "postgres", 
        "password": password,
        "tables": {
            "raw_data": "nse_raw_data",
            "unique_analysis": "nse_unique_analysis",
            "comparisons": "nse_comparisons", 
            "metadata": "nse_metadata"
        }
    }
    
    try:
        with open("database_config.json", "w") as f:
            json.dump(config, f, indent=4)
        
        print("✅ Updated database_config.json")
        return True
        
    except Exception as e:
        print(f"❌ Error updating config: {str(e)}")
        return False

def check_installation():
    """Check PostgreSQL installation status"""
    print("🔍 Checking PostgreSQL Installation...")
    
    # Check service
    try:
        result = subprocess.run(['sc', 'query', 'postgresql-x64-17'], 
                              capture_output=True, text=True)
        if 'RUNNING' in result.stdout:
            print("✅ PostgreSQL service is running")
        else:
            print("❌ PostgreSQL service is not running")
            return False
    except Exception as e:
        print(f"❌ Service check failed: {str(e)}")
        return False
    
    # Check binaries
    psql_path = r"C:\Program Files\PostgreSQL\17\bin\psql.exe"
    if os.path.exists(psql_path):
        print("✅ PostgreSQL binaries found")
    else:
        print("❌ PostgreSQL binaries not found")
        return False
    
    # Check version
    try:
        result = subprocess.run([psql_path, "--version"], 
                              capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✅ PostgreSQL version: {result.stdout.strip()}")
        else:
            print("❌ Cannot get PostgreSQL version")
            return False
    except Exception as e:
        print(f"❌ Version check failed: {str(e)}")
        return False
    
    return True

def main():
    """Main setup function"""
    print("🐘 Simple PostgreSQL Setup for NSE Data")
    print("=" * 50)
    
    if not check_installation():
        print("\n❌ PostgreSQL installation issues detected!")
        print("Please reinstall PostgreSQL or check the installation.")
        return False
    
    if setup_postgres_password():
        print("\n🎉 PostgreSQL setup successful!")
        print("✅ You can now run: python nse_database_setup.py")
        return True
    else:
        print("\n❌ Setup incomplete.")
        print("💡 Alternative: Try using SQLite instead of PostgreSQL")
        print("   SQLite requires no server setup and works out of the box.")
        return False

if __name__ == "__main__":
    main()
