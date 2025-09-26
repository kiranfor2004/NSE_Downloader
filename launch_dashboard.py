"""
🚀 F&O RISK DASHBOARD LAUNCHER
=============================
Launch the Tableau-style F&O Options Risk Analytics Dashboard
"""

import subprocess
import sys
import os

def launch_dashboard():
    """Launch the Streamlit dashboard"""
    
    print("🚀 Starting F&O Risk Analytics Dashboard...")
    print("📊 Tableau-style interface loading...")
    print("🌐 Dashboard will open in your default browser")
    print("\n" + "="*50)
    print("🎯 F&O OPTIONS RISK ANALYTICS DASHBOARD")
    print("📈 Interactive Analysis of 50% Price Reductions")
    print("📊 Data: Step05_monthly_50percent_reduction_analysis")
    print("="*50 + "\n")
    
    # Get the current directory
    current_dir = os.path.dirname(os.path.abspath(__file__))
    dashboard_file = os.path.join(current_dir, "fo_risk_dashboard.py")
    
    try:
        # Launch Streamlit dashboard
        subprocess.run([
            sys.executable, "-m", "streamlit", "run", 
            dashboard_file,
            "--server.port", "8501",
            "--server.headless", "false",
            "--browser.gatherUsageStats", "false"
        ])
    except KeyboardInterrupt:
        print("\n🛑 Dashboard stopped by user")
    except Exception as e:
        print(f"❌ Error launching dashboard: {e}")
        print("💡 Try running manually: streamlit run fo_risk_dashboard.py")

if __name__ == "__main__":
    launch_dashboard()