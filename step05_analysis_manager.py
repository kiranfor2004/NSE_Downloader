#!/usr/bin/env python3
"""
Step 5 Management Utility: Monitor and Manage All-Symbols Analysis
================================================================

Utility script to monitor, manage, and optimize the all-symbols 50% reduction analysis.
Provides monitoring, performance tuning, and data validation capabilities.

Features:
- 📊 Real-time monitoring of analysis progress
- 🔧 Performance optimization recommendations  
- 📈 Live statistics and progress tracking
- 🛠️ Data validation and integrity checks
- 🎯 Analysis resumption and error recovery
- 📋 Quick status reports and summaries

Author: NSE Data Analysis Team
Date: September 2025
"""

import pyodbc
import pandas as pd
import json
import os
import time
from datetime import datetime

def get_database_connection():
    """Create database connection."""
    connection_string = (
        'Driver={ODBC Driver 17 for SQL Server};'
        'Server=SRIKIRANREDDY\\SQLEXPRESS;'
        'Database=master;'
        'Trusted_Connection=yes;'
    )
    return pyodbc.connect(connection_string)

def check_analysis_status():
    """Check current status of all-symbols analysis."""
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        
        print("📊 STEP 5 ALL-SYMBOLS ANALYSIS STATUS")
        print("="*50)
        
        # Check if table exists
        cursor.execute("""
        SELECT COUNT(*) 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_NAME = 'Step05_50percent_reduction_analysis_all_symbols'
        """)
        
        if cursor.fetchone()[0] == 0:
            print("❌ Analysis table not found. Run the all-symbols analyzer first.")
            return
        
        # Get overall progress
        cursor.execute("SELECT COUNT(*) FROM Step05_50percent_reduction_analysis_all_symbols")
        analyzed_records = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM Step05_strikepriceAnalysisderived")
        total_records = cursor.fetchone()[0]
        
        progress_pct = (analyzed_records / total_records * 100) if total_records > 0 else 0
        
        print(f"📈 Progress: {analyzed_records:,}/{total_records:,} records ({progress_pct:.1f}%)")
        
        # Get processing stats
        cursor.execute("""
        SELECT 
            COUNT(DISTINCT symbol) as symbols_processed,
            MAX(batch_number) as last_batch,
            COUNT(CASE WHEN reduction_found = 1 THEN 1 END) as reductions_found,
            MIN(processing_timestamp) as start_time,
            MAX(processing_timestamp) as last_update
        FROM Step05_50percent_reduction_analysis_all_symbols
        """)
        
        stats = cursor.fetchone()
        if stats[0]:
            print(f"🏢 Symbols processed: {stats[0]}")
            print(f"📦 Last batch: {stats[1]}")
            print(f"✅ 50% reductions found: {stats[2]:,}")
            print(f"⏰ Started: {stats[3]}")
            print(f"🔄 Last update: {stats[4]}")
            
            # Calculate success rate
            success_rate = (stats[2] / analyzed_records * 100) if analyzed_records > 0 else 0
            print(f"🎯 Current success rate: {success_rate:.2f}%")
        
        # Get top performing symbols so far
        print(f"\n🏆 TOP 5 SYMBOLS BY SUCCESS RATE (Processed So Far):")
        cursor.execute("""
        SELECT TOP 5
            symbol,
            COUNT(*) as total_strikes,
            COUNT(CASE WHEN reduction_found = 1 THEN 1 END) as successful_reductions,
            CAST(COUNT(CASE WHEN reduction_found = 1 THEN 1 END) * 100.0 / COUNT(*) AS DECIMAL(5,2)) as success_rate
        FROM Step05_50percent_reduction_analysis_all_symbols
        GROUP BY symbol
        HAVING COUNT(*) >= 5
        ORDER BY success_rate DESC
        """)
        
        for row in cursor.fetchall():
            symbol, total, success, rate = row
            print(f"   {symbol}: {success}/{total} strikes ({rate}% success)")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error checking status: {e}")

def validate_analysis_integrity():
    """Validate the integrity of analysis results."""
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        
        print("🔍 DATA INTEGRITY VALIDATION")
        print("="*40)
        
        # Check for missing symbols
        cursor.execute("""
        SELECT COUNT(DISTINCT symbol) as base_symbols
        FROM Step05_strikepriceAnalysisderived
        """)
        base_symbols = cursor.fetchone()[0]
        
        cursor.execute("""
        SELECT COUNT(DISTINCT symbol) as analyzed_symbols
        FROM Step05_50percent_reduction_analysis_all_symbols
        """)
        analyzed_symbols = cursor.fetchone()[0] if cursor.fetchone() else 0
        
        print(f"📊 Symbols in base table: {base_symbols}")
        print(f"📊 Symbols analyzed: {analyzed_symbols}")
        
        if analyzed_symbols < base_symbols:
            print(f"⚠️  Missing symbols: {base_symbols - analyzed_symbols}")
            
            # Show missing symbols
            cursor.execute("""
            SELECT DISTINCT symbol 
            FROM Step05_strikepriceAnalysisderived 
            WHERE symbol NOT IN (
                SELECT DISTINCT symbol 
                FROM Step05_50percent_reduction_analysis_all_symbols
            )
            """)
            
            missing = cursor.fetchall()
            if missing:
                print(f"🔍 Missing symbols: {', '.join([row[0] for row in missing[:10]])}")
                if len(missing) > 10:
                    print(f"    ... and {len(missing) - 10} more")
        
        # Check for data quality issues
        cursor.execute("""
        SELECT 
            COUNT(CASE WHEN reduction_percentage < 0 THEN 1 END) as negative_reductions,
            COUNT(CASE WHEN days_to_reduction < 0 THEN 1 END) as negative_days,
            COUNT(CASE WHEN price_volatility < 0 THEN 1 END) as negative_volatility
        FROM Step05_50percent_reduction_analysis_all_symbols
        """)
        
        quality = cursor.fetchone()
        print(f"\n🔍 Data Quality Checks:")
        print(f"   Negative reductions: {quality[0]}")
        print(f"   Negative days: {quality[1]}")
        print(f"   Negative volatility: {quality[2]}")
        
        # Check processing uniformity
        cursor.execute("""
        SELECT 
            batch_number,
            COUNT(*) as records_in_batch
        FROM Step05_50percent_reduction_analysis_all_symbols
        GROUP BY batch_number
        ORDER BY batch_number
        """)
        
        batches = cursor.fetchall()
        if batches:
            batch_sizes = [row[1] for row in batches]
            avg_batch_size = sum(batch_sizes) / len(batch_sizes)
            print(f"\n📦 Batch Processing:")
            print(f"   Total batches: {len(batches)}")
            print(f"   Average batch size: {avg_batch_size:.1f}")
            print(f"   Size range: {min(batch_sizes)} - {max(batch_sizes)}")
        
        conn.close()
        print("\n✅ Validation completed")
        
    except Exception as e:
        print(f"❌ Error during validation: {e}")

def generate_quick_report():
    """Generate a quick summary report."""
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        
        print("📋 QUICK ANALYSIS REPORT")
        print("="*30)
        
        # Market overview
        cursor.execute("""
        SELECT 
            COUNT(*) as total_analyzed,
            COUNT(DISTINCT symbol) as unique_symbols,
            COUNT(CASE WHEN reduction_found = 1 THEN 1 END) as with_reduction,
            AVG(CASE WHEN reduction_found = 1 THEN reduction_percentage END) as avg_reduction,
            AVG(CASE WHEN reduction_found = 1 THEN days_to_reduction END) as avg_days
        FROM Step05_50percent_reduction_analysis_all_symbols
        """)
        
        overview = cursor.fetchone()
        
        if overview[0] > 0:
            success_rate = (overview[2] / overview[0] * 100)
            print(f"🎯 Market Success Rate: {success_rate:.1f}%")
            print(f"📊 Records Analyzed: {overview[0]:,}")
            print(f"🏢 Symbols Covered: {overview[1]:,}")
            print(f"✅ Successful Reductions: {overview[2]:,}")
            
            if overview[3]:
                print(f"📈 Average Reduction: {overview[3]:.1f}%")
                print(f"⏰ Average Days: {overview[4]:.1f}")
        
        # Top and bottom performers
        print(f"\n🔝 Best Performing Symbol:")
        cursor.execute("""
        SELECT TOP 1
            symbol,
            COUNT(CASE WHEN reduction_found = 1 THEN 1 END) as successes,
            COUNT(*) as total,
            CAST(COUNT(CASE WHEN reduction_found = 1 THEN 1 END) * 100.0 / COUNT(*) AS DECIMAL(5,2)) as rate
        FROM Step05_50percent_reduction_analysis_all_symbols
        GROUP BY symbol
        HAVING COUNT(*) >= 10
        ORDER BY rate DESC
        """)
        
        best = cursor.fetchone()
        if best:
            print(f"   {best[0]}: {best[1]}/{best[2]} strikes ({best[3]}% success)")
        
        print(f"\n🔻 Most Challenging Symbol:")
        cursor.execute("""
        SELECT TOP 1
            symbol,
            COUNT(CASE WHEN reduction_found = 1 THEN 1 END) as successes,
            COUNT(*) as total,
            CAST(COUNT(CASE WHEN reduction_found = 1 THEN 1 END) * 100.0 / COUNT(*) AS DECIMAL(5,2)) as rate
        FROM Step05_50percent_reduction_analysis_all_symbols
        GROUP BY symbol
        HAVING COUNT(*) >= 10
        ORDER BY rate ASC
        """)
        
        worst = cursor.fetchone()
        if worst:
            print(f"   {worst[0]}: {worst[1]}/{worst[2]} strikes ({worst[3]}% success)")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error generating report: {e}")

def cleanup_progress_file():
    """Clean up progress file if analysis is complete."""
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM Step05_50percent_reduction_analysis_all_symbols")
        analyzed = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM Step05_strikepriceAnalysisderived")
        total = cursor.fetchone()[0]
        
        if analyzed >= total and os.path.exists('step05_analysis_progress.json'):
            os.remove('step05_analysis_progress.json')
            print("✅ Progress file cleaned up (analysis complete)")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error cleaning up: {e}")

def performance_recommendations():
    """Provide performance optimization recommendations."""
    print("🔧 PERFORMANCE OPTIMIZATION RECOMMENDATIONS")
    print("="*50)
    
    print("💡 For faster processing:")
    print("   • Increase BATCH_SIZE to 200-500 for more records per batch")
    print("   • Ensure SQL Server has adequate memory allocation")
    print("   • Run during off-peak hours for better database performance")
    print("   • Consider parallel processing for multiple symbol ranges")
    
    print("\n💾 Memory optimization:")
    print("   • Batch processing prevents memory overflow")
    print("   • Progress saving allows safe interruption and resumption")
    print("   • Indexed queries optimize data retrieval speed")
    
    print("\n📊 Monitoring tips:")
    print("   • Check progress regularly with this utility")
    print("   • Monitor SQL Server performance counters")
    print("   • Validate data integrity periodically")

def main():
    """Main utility menu."""
    while True:
        print("\n" + "="*60)
        print("STEP 5 ALL-SYMBOLS ANALYSIS MANAGEMENT UTILITY")
        print("="*60)
        print("1. 📊 Check Analysis Status")
        print("2. 🔍 Validate Data Integrity") 
        print("3. 📋 Generate Quick Report")
        print("4. 🧹 Cleanup Progress File")
        print("5. 🔧 Performance Recommendations")
        print("6. 🚪 Exit")
        print("="*60)
        
        try:
            choice = input("Select option (1-6): ").strip()
            
            if choice == '1':
                check_analysis_status()
            elif choice == '2':
                validate_analysis_integrity()
            elif choice == '3':
                generate_quick_report()
            elif choice == '4':
                cleanup_progress_file()
            elif choice == '5':
                performance_recommendations()
            elif choice == '6':
                print("👋 Goodbye!")
                break
            else:
                print("❌ Invalid option. Please select 1-6.")
                
        except KeyboardInterrupt:
            print("\n👋 Goodbye!")
            break
        except Exception as e:
            print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()