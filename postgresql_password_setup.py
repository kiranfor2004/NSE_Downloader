#!/usr/bin/env python3
"""
🔧 PostgreSQL Password Setup Utility
Help set up PostgreSQL authentication
"""

import subprocess
import os
import json

def check_postgresql_installation():
    """Check if PostgreSQL is installed and running"""
    try:
        # Check if service is running
        result = subprocess.run(['sc', 'query', 'postgresql-x64-17'], 
                              capture_output=True, text=True)
        if 'RUNNING' in result.stdout:
            print("✅ PostgreSQL service is running")
            return True
        else:
            print("❌ PostgreSQL service is not running")
            return False
    except Exception as e:
        print(f"❌ Error checking PostgreSQL: {str(e)}")
        return False

def find_postgresql_path():
    """Find PostgreSQL installation path"""
    possible_paths = [
        r"C:\Program Files\PostgreSQL\17\bin",
        r"C:\Program Files\PostgreSQL\16\bin",
        r"C:\Program Files\PostgreSQL\15\bin",
        r"C:\Program Files (x86)\PostgreSQL\17\bin",
        r"C:\Program Files (x86)\PostgreSQL\16\bin"
    ]
    
    for path in possible_paths:
        psql_path = os.path.join(path, "psql.exe")
        if os.path.exists(psql_path):
            print(f"✅ Found PostgreSQL at: {path}")
            return path
    
    print("❌ PostgreSQL installation not found")
    return None

def test_connection(username="postgres", password=""):
    """Test PostgreSQL connection"""
    pg_path = find_postgresql_path()
    if not pg_path:
        return False
    
    psql_exe = os.path.join(pg_path, "psql.exe")
    
    try:
        # Set environment variable for password
        env = os.environ.copy()
        env['PGPASSWORD'] = password
        
        # Test connection
        result = subprocess.run([
            psql_exe, "-h", "localhost", "-U", username, 
            "-d", "postgres", "-c", "SELECT version();"
        ], capture_output=True, text=True, env=env, timeout=10)
        
        if result.returncode == 0:
            print("✅ Connection successful!")
            return True
        else:
            print(f"❌ Connection failed: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        print("❌ Connection timeout")
        return False
    except Exception as e:
        print(f"❌ Connection error: {str(e)}")
        return False

def reset_postgres_password():
    """Guide user through password reset"""
    print("\n🔧 PostgreSQL Password Setup Options:")
    print("=" * 50)
    print("1. Test current password")
    print("2. Try common default passwords")
    print("3. Manual password reset instructions")
    print("4. Create new database user")
    
    choice = input("\nChoose option (1-4): ").strip()
    
    if choice == "1":
        password = input("Enter password to test: ")
        return test_connection("postgres", password)
    
    elif choice == "2":
        common_passwords = ["", "postgres", "admin", "123456", "password"]
        print("\n🔍 Testing common passwords...")
        
        for pwd in common_passwords:
            print(f"Testing: {'(empty)' if pwd == '' else pwd}")
            if test_connection("postgres", pwd):
                print(f"✅ Found working password: {'(empty)' if pwd == '' else pwd}")
                update_config_password(pwd)
                return True
        
        print("❌ No common passwords worked")
        return False
    
    elif choice == "3":
        print("\n📋 Manual Password Reset Instructions:")
        print("=" * 50)
        print("1. Open Command Prompt as Administrator")
        print("2. Stop PostgreSQL service:")
        print("   net stop postgresql-x64-17")
        print("3. Start PostgreSQL in single-user mode:")
        print("   cd \"C:\\Program Files\\PostgreSQL\\17\\bin\"")
        print("   postgres --single -D \"C:\\Program Files\\PostgreSQL\\17\\data\" postgres")
        print("4. In the postgres prompt, run:")
        print("   ALTER USER postgres PASSWORD 'your_new_password';")
        print("   \\q")
        print("5. Start service again:")
        print("   net start postgresql-x64-17")
        
        return False
    
    elif choice == "4":
        print("\n👤 Creating New Database User:")
        admin_password = input("Enter postgres user password (if known): ")
        new_username = input("Enter new username: ")
        new_password = input("Enter new user password: ")
        
        # Try to create new user
        return create_database_user(admin_password, new_username, new_password)
    
    return False

def create_database_user(admin_password, new_username, new_password):
    """Create a new database user"""
    pg_path = find_postgresql_path()
    if not pg_path:
        return False
    
    psql_exe = os.path.join(pg_path, "psql.exe")
    
    try:
        env = os.environ.copy()
        env['PGPASSWORD'] = admin_password
        
        sql_commands = f"""
        CREATE USER {new_username} WITH PASSWORD '{new_password}';
        ALTER USER {new_username} CREATEDB;
        GRANT ALL PRIVILEGES ON DATABASE postgres TO {new_username};
        """
        
        result = subprocess.run([
            psql_exe, "-h", "localhost", "-U", "postgres", 
            "-d", "postgres", "-c", sql_commands
        ], capture_output=True, text=True, env=env)
        
        if result.returncode == 0:
            print(f"✅ Created user: {new_username}")
            update_config_user(new_username, new_password)
            return True
        else:
            print(f"❌ Failed to create user: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Error creating user: {str(e)}")
        return False

def update_config_password(password):
    """Update database config with working password"""
    try:
        with open("database_config.json", "r") as f:
            config = json.load(f)
        
        config["password"] = password
        
        with open("database_config.json", "w") as f:
            json.dump(config, f, indent=4)
        
        print("✅ Updated database_config.json with working password")
        return True
        
    except Exception as e:
        print(f"❌ Error updating config: {str(e)}")
        return False

def update_config_user(username, password):
    """Update database config with new user"""
    try:
        with open("database_config.json", "r") as f:
            config = json.load(f)
        
        config["username"] = username
        config["password"] = password
        
        with open("database_config.json", "w") as f:
            json.dump(config, f, indent=4)
        
        print("✅ Updated database_config.json with new user")
        return True
        
    except Exception as e:
        print(f"❌ Error updating config: {str(e)}")
        return False

def main():
    """Main setup function"""
    print("🔧 PostgreSQL Password Setup Utility")
    print("=" * 50)
    
    if not check_postgresql_installation():
        print("❌ PostgreSQL is not running. Please start the service first.")
        return
    
    if reset_postgres_password():
        print("\n🎉 PostgreSQL setup complete!")
        print("✅ You can now run: python nse_database_setup.py")
    else:
        print("\n❌ Setup incomplete. Please check PostgreSQL installation.")

if __name__ == "__main__":
    main()
