#!/usr/bin/env python3
"""
🔧 PostgreSQL Easy Setup for Beginners
Step-by-step PostgreSQL setup with clear instructions
"""

import subprocess
import os
import json
import getpass

def check_postgresql_service():
    """Check if PostgreSQL service is running"""
    print("🔍 Checking PostgreSQL service status...")
    
    try:
        result = subprocess.run(['sc', 'query', 'postgresql-x64-17'], 
                              capture_output=True, text=True)
        if 'RUNNING' in result.stdout:
            print("✅ PostgreSQL service is RUNNING")
            return True
        else:
            print("❌ PostgreSQL service is NOT running")
            print("Let's try to start it...")
            return start_postgresql_service()
    except Exception as e:
        print(f"❌ Error checking service: {e}")
        return False

def start_postgresql_service():
    """Try to start PostgreSQL service"""
    try:
        print("🚀 Starting PostgreSQL service...")
        result = subprocess.run(['sc', 'start', 'postgresql-x64-17'], 
                              capture_output=True, text=True)
        if result.returncode == 0:
            print("✅ PostgreSQL service started successfully!")
            return True
        else:
            print(f"❌ Failed to start service: {result.stderr}")
            print("💡 You may need to start it manually from Windows Services")
            return False
    except Exception as e:
        print(f"❌ Error starting service: {e}")
        return False

def find_postgresql_installation():
    """Find where PostgreSQL is installed"""
    print("🔍 Looking for PostgreSQL installation...")
    
    possible_paths = [
        r"C:\Program Files\PostgreSQL\17",
        r"C:\Program Files\PostgreSQL\16",
        r"C:\Program Files\PostgreSQL\15",
        r"C:\Program Files (x86)\PostgreSQL\17",
        r"C:\Program Files (x86)\PostgreSQL\16"
    ]
    
    for path in possible_paths:
        if os.path.exists(path):
            bin_path = os.path.join(path, "bin")
            data_path = os.path.join(path, "data")
            
            if os.path.exists(bin_path) and os.path.exists(data_path):
                print(f"✅ Found PostgreSQL at: {path}")
                return path, bin_path, data_path
    
    print("❌ PostgreSQL installation not found in standard locations")
    return None, None, None

def try_default_passwords():
    """Try common default passwords"""
    print("\n🔐 Testing common default passwords...")
    print("This is safe - we're just testing if any work")
    
    _, bin_path, _ = find_postgresql_installation()
    if not bin_path:
        return False, None
    
    psql_exe = os.path.join(bin_path, "psql.exe")
    common_passwords = ["", "postgres", "admin", "password", "123456"]
    
    for i, pwd in enumerate(common_passwords, 1):
        pwd_display = "(empty/no password)" if pwd == "" else pwd
        print(f"  {i}. Testing: {pwd_display}")
        
        try:
            env = os.environ.copy()
            env['PGPASSWORD'] = pwd
            
            result = subprocess.run([
                psql_exe, "-h", "localhost", "-U", "postgres", 
                "-d", "postgres", "-c", "SELECT 'Connection successful!' as status;"
            ], capture_output=True, text=True, env=env, timeout=5)
            
            if result.returncode == 0:
                print(f"     ✅ SUCCESS! Password works: {pwd_display}")
                return True, pwd
            else:
                print(f"     ❌ Failed")
                
        except Exception as e:
            print(f"     ❌ Error: {str(e)[:30]}...")
    
    print("\n❌ None of the common passwords worked")
    return False, None

def reset_postgres_password_simple():
    """Simple password reset method"""
    print("\n🔧 PostgreSQL Password Reset (Beginner-Friendly)")
    print("=" * 60)
    print("Since the default passwords didn't work, we need to reset the password.")
    print("This happens when PostgreSQL was installed but no password was set up properly.")
    
    print("\n📝 Here's what we'll do:")
    print("1. Stop PostgreSQL temporarily")
    print("2. Start it in 'safe mode' (no password needed)")
    print("3. Set a new password")
    print("4. Restart PostgreSQL normally")
    
    # Get new password from user
    print("\n🔑 First, let's choose a new password:")
    print("   - Use something you'll remember")
    print("   - Can be simple like 'postgres123' or 'admin123'")
    print("   - We'll use this for our NSE data project")
    
    new_password = input("\nEnter your new PostgreSQL password: ").strip()
    
    if not new_password:
        print("❌ Password cannot be empty!")
        return False
    
    print(f"\n✅ Got it! New password will be: {new_password}")
    
    # Confirm the user wants to proceed
    proceed = input("\nProceed with password reset? (yes/no): ").strip().lower()
    
    if proceed not in ['yes', 'y']:
        print("❌ Setup cancelled by user")
        return False
    
    return perform_password_reset(new_password)

def perform_password_reset(new_password):
    """Perform the actual password reset"""
    print("\n🔧 Starting password reset process...")
    
    postgres_path, bin_path, data_path = find_postgresql_installation()
    if not postgres_path:
        print("❌ Cannot find PostgreSQL installation")
        return False
    
    # Step 1: Stop the service
    print("\n📍 Step 1: Stopping PostgreSQL service...")
    try:
        result = subprocess.run(['net', 'stop', 'postgresql-x64-17'], 
                              capture_output=True, text=True)
        if 'stopped' in result.stdout.lower() or result.returncode == 0:
            print("✅ Service stopped")
        else:
            print("⚠️ Service might already be stopped")
    except Exception as e:
        print(f"⚠️ Error stopping service: {e}")
    
    # Step 2: Create a temporary SQL file
    print("\n📍 Step 2: Creating password reset command...")
    
    temp_sql_file = os.path.join(postgres_path, "reset_password.sql")
    sql_content = f"ALTER USER postgres PASSWORD '{new_password}';\n"
    
    try:
        with open(temp_sql_file, 'w') as f:
            f.write(sql_content)
        print("✅ Password reset command created")
    except Exception as e:
        print(f"❌ Error creating SQL file: {e}")
        return False
    
    # Step 3: Start in single-user mode and reset password
    print("\n📍 Step 3: Resetting password...")
    
    postgres_exe = os.path.join(bin_path, "postgres.exe")
    
    try:
        # Run postgres in single-user mode with the SQL file
        cmd = [
            postgres_exe, 
            "--single", 
            "-D", data_path,
            "postgres"
        ]
        
        process = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        # Send the password reset command
        stdout, stderr = process.communicate(input=sql_content + "\\q\n", timeout=10)
        
        if "ALTER ROLE" in stdout or process.returncode == 0:
            print("✅ Password reset successful!")
        else:
            print(f"⚠️ Reset output: {stdout}")
            
    except Exception as e:
        print(f"❌ Error during password reset: {e}")
    
    # Clean up temporary file
    try:
        if os.path.exists(temp_sql_file):
            os.remove(temp_sql_file)
    except:
        pass
    
    # Step 4: Restart the service
    print("\n📍 Step 4: Starting PostgreSQL service...")
    try:
        result = subprocess.run(['net', 'start', 'postgresql-x64-17'], 
                              capture_output=True, text=True)
        if 'started' in result.stdout.lower() or result.returncode == 0:
            print("✅ Service restarted")
        else:
            print(f"❌ Error starting service: {result.stderr}")
            return False
    except Exception as e:
        print(f"❌ Error starting service: {e}")
        return False
    
    # Step 5: Test the new password
    print("\n📍 Step 5: Testing new password...")
    
    return test_new_password(new_password)

def test_new_password(password):
    """Test if the new password works"""
    _, bin_path, _ = find_postgresql_installation()
    psql_exe = os.path.join(bin_path, "psql.exe")
    
    try:
        env = os.environ.copy()
        env['PGPASSWORD'] = password
        
        result = subprocess.run([
            psql_exe, "-h", "localhost", "-U", "postgres", 
            "-d", "postgres", "-c", "SELECT 'Password works!' as test;"
        ], capture_output=True, text=True, env=env, timeout=10)
        
        if result.returncode == 0:
            print("✅ NEW PASSWORD WORKS! 🎉")
            
            # Save to config file
            save_password_to_config(password)
            return True
        else:
            print(f"❌ Password test failed: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Error testing password: {e}")
        return False

def save_password_to_config(password):
    """Save the working password to config file"""
    try:
        config = {
            "host": "localhost",
            "port": 5432,
            "database": "postgres",
            "username": "postgres",
            "password": password
        }
        
        with open("database_config.json", "w") as f:
            json.dump(config, f, indent=4)
        
        print("✅ Password saved to database_config.json")
        print("🎯 You can now use PostgreSQL for your NSE data!")
        
    except Exception as e:
        print(f"❌ Error saving config: {e}")

def main():
    """Main setup function with clear steps"""
    print("🔧 PostgreSQL Easy Setup for Beginners")
    print("=" * 50)
    print("👋 Hi! I'll help you set up PostgreSQL step by step.")
    print("   This is needed to store and analyze your NSE stock data.")
    print()
    
    # Step 1: Check if service is running
    if not check_postgresql_service():
        print("❌ Cannot proceed without PostgreSQL running")
        return False
    
    # Step 2: Find installation
    postgres_path, bin_path, data_path = find_postgresql_installation()
    if not postgres_path:
        print("❌ Cannot find PostgreSQL installation")
        return False
    
    print(f"✅ PostgreSQL found at: {postgres_path}")
    
    # Step 3: Try default passwords
    success, password = try_default_passwords()
    
    if success:
        print(f"\n🎉 Great! PostgreSQL is already working!")
        save_password_to_config(password)
        return True
    else:
        print(f"\n🔧 No worries! We need to set up a password.")
        return reset_postgres_password_simple()

if __name__ == "__main__":
    result = main()
    
    if result:
        print(f"\n🎉 SUCCESS! PostgreSQL is now ready!")
        print(f"📊 Next step: Run 'python nse_database_setup.py' to create your NSE database")
    else:
        print(f"\n❌ Setup incomplete. You can:")
        print(f"   1. Try running this script again")
        print(f"   2. Use pgAdmin (the GUI tool) to set a password")
        print(f"   3. Ask for help with the specific error you see")
