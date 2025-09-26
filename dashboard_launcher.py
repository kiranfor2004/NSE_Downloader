"""
🎯 F&O RISK DASHBOARD LAUNCHER - COMPREHENSIVE
=============================================
Launch multiple dashboard options for F&O Options Risk Analytics
"""

import os
import subprocess
import sys
import webbrowser
from pathlib import Path

def find_latest_html_dashboard():
    """Find the most recent HTML dashboard file"""
    current_dir = Path(".")
    html_files = list(current_dir.glob("FO_Risk_Dashboard_*.html"))
    
    if not html_files:
        return None
    
    # Sort by modification time and return the latest
    latest_file = max(html_files, key=lambda x: x.stat().st_mtime)
    return latest_file

def launch_html_dashboard():
    """Launch HTML dashboard in browser"""
    html_file = find_latest_html_dashboard()
    
    if not html_file:
        print("❌ No HTML dashboard found. Generating new one...")
        try:
            subprocess.run([sys.executable, "generate_html_dashboard.py"], check=True)
            html_file = find_latest_html_dashboard()
        except Exception as e:
            print(f"❌ Error generating HTML dashboard: {e}")
            return False
    
    if html_file:
        print(f"🌐 Opening HTML dashboard: {html_file.name}")
        webbrowser.open(f"file://{html_file.absolute()}")
        return True
    
    return False

def launch_streamlit_dashboard():
    """Launch Streamlit dashboard"""
    try:
        print("🔄 Checking Streamlit installation...")
        subprocess.run([sys.executable, "-c", "import streamlit"], check=True)
        
        print("🚀 Launching Streamlit dashboard...")
        subprocess.run([
            sys.executable, "-m", "streamlit", "run", 
            "fo_risk_dashboard.py",
            "--server.port", "8501",
            "--server.headless", "false",
            "--browser.gatherUsageStats", "false"
        ])
        return True
        
    except subprocess.CalledProcessError:
        print("❌ Streamlit not installed.")
        install = input("📦 Would you like to install Streamlit? (y/n): ").lower().strip()
        
        if install == 'y':
            try:
                print("📦 Installing Streamlit...")
                subprocess.run([sys.executable, "-m", "pip", "install", "streamlit"], check=True)
                print("✅ Streamlit installed successfully!")
                return launch_streamlit_dashboard()  # Retry
            except Exception as e:
                print(f"❌ Error installing Streamlit: {e}")
                return False
        else:
            return False
    
    except Exception as e:
        print(f"❌ Error launching Streamlit: {e}")
        return False

def launch_dash_dashboard():
    """Launch Plotly Dash dashboard"""
    try:
        print("🔄 Checking Dash installation...")
        subprocess.run([sys.executable, "-c", "import dash"], check=True)
        
        print("🚀 Launching Plotly Dash dashboard...")
        subprocess.run([sys.executable, "fo_risk_dashboard_dash.py"])
        return True
        
    except subprocess.CalledProcessError:
        print("❌ Dash not installed.")
        install = input("📦 Would you like to install Dash? (y/n): ").lower().strip()
        
        if install == 'y':
            try:
                print("📦 Installing Dash...")
                subprocess.run([sys.executable, "-m", "pip", "install", "dash", "dash-bootstrap-components"], check=True)
                print("✅ Dash installed successfully!")
                return launch_dash_dashboard()  # Retry
            except Exception as e:
                print(f"❌ Error installing Dash: {e}")
                return False
        else:
            return False
    
    except Exception as e:
        print(f"❌ Error launching Dash: {e}")
        return False

def main():
    """Main launcher function"""
    print("🎯 F&O OPTIONS RISK ANALYTICS DASHBOARD LAUNCHER")
    print("=" * 55)
    print("📊 Multiple dashboard options available:")
    print("1. 🌐 HTML Dashboard (Static, works offline)")
    print("2. 🚀 Streamlit Dashboard (Interactive, web-based)")
    print("3. 📈 Plotly Dash Dashboard (Interactive, web-based)")
    print("4. 🔄 Regenerate HTML Dashboard")
    print("5. ❌ Exit")
    print("=" * 55)
    
    while True:
        try:
            choice = input("\n🎯 Select dashboard option (1-5): ").strip()
            
            if choice == '1':
                print("\n🌐 Launching HTML Dashboard...")
                if launch_html_dashboard():
                    print("✅ HTML dashboard opened in your browser!")
                    print("📱 This dashboard works offline and is mobile-responsive")
                else:
                    print("❌ Failed to launch HTML dashboard")
                break
                
            elif choice == '2':
                print("\n🚀 Launching Streamlit Dashboard...")
                if launch_streamlit_dashboard():
                    print("✅ Streamlit dashboard should open at http://localhost:8501")
                else:
                    print("❌ Failed to launch Streamlit dashboard")
                    print("💡 Try option 1 (HTML Dashboard) instead")
                break
                
            elif choice == '3':
                print("\n📈 Launching Plotly Dash Dashboard...")
                if launch_dash_dashboard():
                    print("✅ Dash dashboard should open at http://localhost:8050")
                else:
                    print("❌ Failed to launch Dash dashboard")
                    print("💡 Try option 1 (HTML Dashboard) instead")
                break
                
            elif choice == '4':
                print("\n🔄 Regenerating HTML Dashboard...")
                try:
                    subprocess.run([sys.executable, "generate_html_dashboard.py"], check=True)
                    print("✅ HTML dashboard regenerated successfully!")
                    
                    launch_html = input("🌐 Open the new dashboard? (y/n): ").lower().strip()
                    if launch_html == 'y':
                        launch_html_dashboard()
                except Exception as e:
                    print(f"❌ Error regenerating dashboard: {e}")
                break
                
            elif choice == '5':
                print("\n👋 Goodbye!")
                break
                
            else:
                print("❌ Invalid choice. Please select 1-5.")
                
        except KeyboardInterrupt:
            print("\n\n👋 Dashboard launcher stopped by user")
            break
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            break

if __name__ == "__main__":
    main()