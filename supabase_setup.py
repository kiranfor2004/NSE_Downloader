#!/usr/bin/env python3
"""
NSE Supabase Setup Guide and Quick Installer

This script helps you set up Supabase integration for your NSE data.
It will guide you through the complete process.

Author: Generated for NSE Supabase setup
Date: September 2025
"""

import subprocess
import sys
import os
from urllib.parse import urlparse
from datetime import datetime

def print_header(title):
    print("\n" + "=" * 60)
    print(f"  {title}")
    print("=" * 60)

def check_python_version():
    """Check if Python version is compatible"""
    if sys.version_info < (3, 8):
        print("❌ Python 3.8 or higher is required")
        print(f"   Current version: {sys.version}")
        return False
    print(f"✅ Python version: {sys.version}")
    return True

def install_requirements():
    """Install required packages"""
    print("\n📦 Installing required packages...")
    
    required_packages = [
        'supabase>=2.0.0',
        'python-dotenv>=1.0.0', 
        'pandas>=2.0.0',
        'requests>=2.31.0'
    ]
    
    for package in required_packages:
        try:
            print(f"   Installing {package}...")
            subprocess.check_call([
                sys.executable, '-m', 'pip', 'install', package, '--quiet'
            ])
            print(f"   ✅ {package} installed")
        except subprocess.CalledProcessError as e:
            print(f"   ❌ Failed to install {package}: {e}")
            return False
    
    print("✅ All packages installed successfully!")
    return True

def create_supabase_account_guide():
    """Show step-by-step Supabase account creation guide"""
    print_header("🚀 SUPABASE SETUP GUIDE")
    
    print("Step 1: Create Supabase Account")
    print("   🌐 Go to: https://supabase.com")
    print("   📝 Click 'Start your project' or 'Sign Up'")
    print("   🔑 Sign up with GitHub/Google or email")
    print()
    
    print("Step 2: Create New Project")
    print("   ➕ Click 'New Project'")
    print("   🏢 Choose organization (or create new)")
    print("   📝 Enter project name: 'NSE-Stock-Data'")
    print("   🔒 Set database password (save it!)")
    print("   🌍 Choose region: closest to you")
    print("   💰 Select 'Free' plan")
    print("   🚀 Click 'Create new project'")
    print("   ⏳ Wait 2-3 minutes for setup")
    print()
    
    print("Step 3: Get API Credentials")
    print("   ⚙️  Go to Settings > API (left sidebar)")
    print("   📋 Copy 'Project URL'")
    print("   🔑 Copy 'anon/public key'")
    print()
    
    print("Step 4: Setup Database Tables")
    print("   📊 Go to SQL Editor (left sidebar)")
    print("   ➕ Click 'New Query'")
    print("   📝 Run the SQL commands provided by the uploader")
    print()
    
    input("📋 Press Enter when you have your Supabase credentials ready...")

def setup_env_file():
    """Interactive setup of .env file"""
    print_header("📝 CONFIGURATION SETUP")
    
    if os.path.exists('.env'):
        print("⚠️  .env file already exists")
        overwrite = input("🔄 Do you want to update it? (y/n): ").strip().lower()
        if overwrite != 'y':
            return True
    
    print("Please enter your Supabase credentials:")
    print("(You can find these in Supabase Dashboard > Settings > API)")
    print()
    
    # Get Supabase URL
    while True:
        supabase_url = input("🔗 Supabase Project URL: ").strip()
        if supabase_url and 'supabase.co' in supabase_url:
            # Validate URL format
            try:
                parsed = urlparse(supabase_url)
                if parsed.scheme and parsed.netloc:
                    break
            except:
                pass
        print("❌ Please enter a valid Supabase URL (e.g., https://abcdef.supabase.co)")
    
    # Get API Key
    while True:
        api_key = input("🔑 Supabase Anon/Public Key: ").strip()
        if api_key and len(api_key) > 50:  # Basic validation
            break
        print("❌ Please enter a valid API key (should be long JWT token)")
    
    # Create .env file
    env_content = f"""# NSE Supabase Configuration
# Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

SUPABASE_URL={supabase_url}
SUPABASE_ANON_KEY={api_key}

# Backup your credentials:
# Project URL: {supabase_url}
# API Key: {api_key[:20]}...
"""
    
    try:
        with open('.env', 'w') as f:
            f.write(env_content)
        print("✅ .env file created successfully!")
        return True
    except Exception as e:
        print(f"❌ Failed to create .env file: {e}")
        return False

def test_connection():
    """Test Supabase connection"""
    print_header("🔗 TESTING CONNECTION")
    
    try:
        from dotenv import load_dotenv
        from supabase import create_client
        
        load_dotenv()
        supabase_url = os.getenv('SUPABASE_URL')
        supabase_key = os.getenv('SUPABASE_ANON_KEY')
        
        if not supabase_url or not supabase_key:
            print("❌ Credentials not found in .env file")
            return False
        
        print("🔌 Connecting to Supabase...")
        supabase = create_client(supabase_url, supabase_key)
        
        # Test connection with a simple query
        result = supabase.table('nse_stock_data').select('*').limit(1).execute()
        print("✅ Connection successful!")
        
        if result.data:
            print(f"📊 Found existing data: {len(result.data)} records")
        else:
            print("📊 Database is ready for data upload")
        
        return True
        
    except ImportError as e:
        print(f"❌ Missing package: {e}")
        print("Please run the package installation again")
        return False
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        print("Please check your credentials and try again")
        return False

def show_sql_commands():
    """Show SQL commands to run in Supabase"""
    print_header("📊 DATABASE SETUP SQL")
    
    sql_commands = """
-- Run these commands in Supabase SQL Editor:
-- Dashboard > SQL Editor > New Query

-- 1. Create main data table
CREATE TABLE IF NOT EXISTS nse_stock_data (
    id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(50) NOT NULL,
    series VARCHAR(10),
    date DATE NOT NULL,
    prev_close DECIMAL(12,2),
    open_price DECIMAL(12,2),
    high_price DECIMAL(12,2),
    low_price DECIMAL(12,2),
    last_price DECIMAL(12,2),
    close_price DECIMAL(12,2),
    avg_price DECIMAL(12,2),
    total_traded_qty BIGINT,
    turnover DECIMAL(15,2),
    no_of_trades INTEGER,
    deliverable_qty BIGINT,
    deliverable_percentage DECIMAL(8,4),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    UNIQUE(symbol, date, series)
);

-- 2. Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_nse_symbol ON nse_stock_data(symbol);
CREATE INDEX IF NOT EXISTS idx_nse_date ON nse_stock_data(date);
CREATE INDEX IF NOT EXISTS idx_nse_symbol_date ON nse_stock_data(symbol, date);
CREATE INDEX IF NOT EXISTS idx_nse_deliverable_pct ON nse_stock_data(deliverable_percentage);
CREATE INDEX IF NOT EXISTS idx_nse_turnover ON nse_stock_data(turnover);

-- 3. Create upload tracking table
CREATE TABLE IF NOT EXISTS upload_status (
    id BIGSERIAL PRIMARY KEY,
    filename VARCHAR(100) NOT NULL UNIQUE,
    upload_date TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    records_count INTEGER,
    file_size_kb INTEGER,
    status VARCHAR(20) DEFAULT 'completed',
    error_message TEXT
);

-- 4. Enable Row Level Security (optional)
ALTER TABLE nse_stock_data ENABLE ROW LEVEL SECURITY;
ALTER TABLE upload_status ENABLE ROW LEVEL SECURITY;

-- 5. Create policies for public access (adjust as needed)
CREATE POLICY "Allow public read access" ON nse_stock_data FOR SELECT USING (true);
CREATE POLICY "Allow public read access" ON upload_status FOR SELECT USING (true);
"""
    
    print(sql_commands)
    
    # Save to file
    with open('supabase_setup.sql', 'w') as f:
        f.write(sql_commands)
    print("💾 SQL commands saved to: supabase_setup.sql")
    print()
    print("📋 Next steps:")
    print("1. Copy the SQL commands above")
    print("2. Go to Supabase Dashboard > SQL Editor")
    print("3. Click 'New Query' and paste the commands")
    print("4. Click 'Run' to create the tables")
    
    input("\n⏳ Press Enter when you've run the SQL commands...")

def show_next_steps():
    """Show what to do next"""
    print_header("🎯 WHAT'S NEXT?")
    
    print("Your NSE Supabase integration is ready! Here's what you can do:")
    print()
    print("📤 1. Upload Your Data")
    print("   🐍 Run: python supabase_nse_uploader.py")
    print("   📂 This will upload all CSV files from NSE_August_2025_Data/")
    print()
    print("📊 2. Analyze Your Data") 
    print("   🐍 Run: python supabase_analysis_tool.py")
    print("   📈 Get top gainers, losers, high delivery stocks, etc.")
    print()
    print("🌐 3. Access from Anywhere")
    print("   📱 Your data is now in the cloud")
    print("   🔗 Access from any device with your credentials")
    print()
    print("📋 4. Share Analysis")
    print("   📊 Export analysis results to CSV")
    print("   🤝 Share insights with others")

def main():
    """Main setup process"""
    from datetime import datetime
    
    print("☁️  NSE Supabase Setup & Installation")
    print("=" * 60)
    print("This script will help you set up cloud database for NSE data")
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Step 1: Check Python version
    if not check_python_version():
        input("Press Enter to exit...")
        return
    
    # Step 2: Install packages
    print_header("📦 PACKAGE INSTALLATION")
    if not install_requirements():
        input("Press Enter to exit...")
        return
    
    # Step 3: Supabase account setup guide
    create_supabase_account_guide()
    
    # Step 4: Show SQL setup
    show_sql_commands()
    
    # Step 5: Configure .env file
    if not setup_env_file():
        input("Press Enter to exit...")
        return
    
    # Step 6: Test connection
    if not test_connection():
        print("⚠️  Setup completed but connection test failed")
        print("You can still proceed with manual configuration")
    
    # Step 7: Show next steps
    show_next_steps()
    
    print("\n🎉 Setup completed successfully!")
    print("Your NSE data can now be stored and analyzed in the cloud!")
    
    # Ask if user wants to start upload
    start_upload = input("\n🚀 Start uploading your CSV data now? (y/n): ").strip().lower()
    if start_upload == 'y':
        try:
            import subprocess
            subprocess.run([sys.executable, 'supabase_nse_uploader.py'])
        except Exception as e:
            print(f"❌ Failed to start uploader: {e}")
            print("Please run 'python supabase_nse_uploader.py' manually")
    
    input("\nPress Enter to exit...")

if __name__ == "__main__":
    main()
