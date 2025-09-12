#!/usr/bin/env python3
"""
📋 NSE F&O Data Manual Download Guide
Step-by-step instructions for downloading F&O data from NSE website
"""

def show_manual_download_guide():
    """Show detailed manual download instructions"""
    print("📋 NSE F&O Data Manual Download Guide")
    print("=" * 60)
    
    print("\n🌐 Option 1: NSE Website Direct Download")
    print("-" * 40)
    print("1. Visit: https://www.nseindia.com/all-reports-derivatives")
    print("2. Click on 'Historical Reports' tab")
    print("3. Select 'Equity Derivatives'")
    print("4. Choose your desired date range")
    print("5. Download files individually")
    
    print("\n🗂️ Option 2: NSE Archives Direct Access")
    print("-" * 40)
    print("1. Visit: https://nsearchives.nseindia.com/")
    print("2. Navigate to DERIVATIVES section")
    print("3. Browse by year/month folders")
    print("4. Download .zip files directly")
    
    print("\n📊 Option 3: Available Data Analysis")
    print("-" * 40)
    print("Since February 2025 data isn't available, you can:")
    print("• Analyze existing equity data (Jan-Aug 2025)")
    print("• Download F&O data from available months")
    print("• Use current equity data for derivatives analysis")
    
    print("\n🔍 Option 4: Check Current Data Availability")
    print("-" * 40)
    print("Let's check what F&O data is actually available...")

def check_fo_data_status():
    """Check current F&O data availability status"""
    import requests
    from datetime import date, timedelta
    
    print("\n🔍 Checking F&O Data Availability...")
    print("=" * 50)
    
    # Test some recent dates to see what's available
    base_url = "https://nsearchives.nseindia.com/content/historical/DERIVATIVES"
    today = date.today()
    
    print("📅 Testing recent dates for F&O data availability:")
    
    # Check last 30 days
    available_dates = []
    for i in range(30):
        test_date = today - timedelta(days=i)
        if test_date.weekday() < 5:  # Weekday only
            
            # Format date for NSE URL
            months = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN',
                     'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']
            
            day = f"{test_date.day:02d}"
            month = months[test_date.month - 1] 
            year = str(test_date.year)
            date_str = f"{day}{month}{year}"
            
            file_pattern = f"fo{date_str}bhav.csv.zip"
            year_month = f"{test_date.year}/{test_date.month:02d}"
            url = f"{base_url}/{year_month}/{file_pattern}"
            
            try:
                response = requests.head(url, timeout=5)
                if response.status_code == 200:
                    available_dates.append(test_date)
                    print(f"   ✅ {test_date.strftime('%d-%b-%Y')} - Data available")
                else:
                    print(f"   ❌ {test_date.strftime('%d-%b-%Y')} - No data")
            except:
                print(f"   ⚠️ {test_date.strftime('%d-%b-%Y')} - Connection failed")
            
            if len(available_dates) >= 5:  # Stop after finding 5 available dates
                break
    
    if available_dates:
        print(f"\n✅ Found {len(available_dates)} recent dates with F&O data")
        print("💡 You can download these dates instead of February 2025")
    else:
        print("\n❌ No recent F&O data found through automated checking")
        print("💡 Data might be available but requires different access method")

def show_current_data_analysis():
    """Show what can be done with current equity data"""
    print("\n📊 What You Can Do With Current Equity Data")
    print("=" * 50)
    print("Your database contains 467,448 equity records (Jan-Aug 2025)")
    print("You can perform F&O-related analysis using this data:")
    print("")
    print("🎯 Derivatives Analysis Options:")
    print("• Stock price volatility analysis")
    print("• Identify high-volume stocks (potential F&O candidates)")
    print("• Price movement patterns")
    print("• Support/resistance levels")
    print("• Momentum indicators")
    print("")
    print("📈 Create F&O Strategy Insights:")
    print("• Track underlying asset performance")
    print("• Volume-based stock screening")
    print("• Price trend analysis")
    print("• Delivery percentage patterns")

def main():
    """Main guide function"""
    show_manual_download_guide()
    
    print("\n" + "="*60)
    response = input("Would you like me to check current F&O data availability? (y/n): ")
    
    if response.lower() == 'y':
        check_fo_data_status()
    
    print("\n" + "="*60)
    response2 = input("Show analysis options for your current equity data? (y/n): ")
    
    if response2.lower() == 'y':
        show_current_data_analysis()
    
    print("\n💡 Recommendations:")
    print("1. Use your existing equity data for derivatives research")
    print("2. Check NSE website manually for latest F&O data")
    print("3. Consider downloading historical F&O data from available months")
    print("4. Set up alerts for when February 2025 data becomes available")

if __name__ == "__main__":
    main()
