#!/usr/bin/env python3
"""
Step 03: Month-to-Month Comparison Analysis

Purpose:
  Comprehensive month-to-month comparison analysis using database data from Step 2.
  Identifies stocks with increasing trends across consecutive months.

Features:
  1. Database-driven comparisons (no file dependencies)
  2. Multi-month trend analysis (Jan-Aug 2025)
  3. Volume and delivery increase detection
  4. Percentage and absolute increase calculations
  5. Top performers identification
  6. Excel output with multiple analysis sheets

Input: Step 02 database tables (step02_monthly_analysis)
Output: Comprehensive Excel reports with monthly comparison insights
"""

import pandas as pd
import os
import sys
from datetime import datetime
from typing import Dict, List, Tuple

# Add parent directory to path to import nse_database_integration
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from nse_database_integration import NSEDatabaseManager

class Step03MonthlyComparison:
    def __init__(self):
        self.db = NSEDatabaseManager()
        self.available_months = []
        self.load_available_months()
        
    def load_available_months(self):
        """Load available months from database"""
        print("📅 Loading available months from database...")
        cursor = self.db.connection.cursor()
        cursor.execute("""
            SELECT DISTINCT analysis_month
            FROM step02_monthly_analysis
            ORDER BY analysis_month
        """)
        
        self.available_months = [row[0] for row in cursor.fetchall()]
        print(f"   ✅ Found {len(self.available_months)} months: {', '.join(self.available_months)}")
        
    def get_month_data(self, month: str, analysis_type: str = 'VOLUME') -> pd.DataFrame:
        """Get monthly analysis data for specific month and type"""
        cursor = self.db.connection.cursor()
        cursor.execute("""
            SELECT symbol, peak_value, peak_date, ttl_trd_qnty, deliv_qty, close_price
            FROM step02_monthly_analysis
            WHERE analysis_month = ? AND analysis_type = ?
        """, (month, analysis_type))
        
        columns = ['SYMBOL', 'PEAK_VALUE', 'PEAK_DATE', 'TTL_TRD_QNTY', 'DELIV_QTY', 'CLOSE_PRICE']
        data = cursor.fetchall()
        return pd.DataFrame(data, columns=columns)
        
    def compare_consecutive_months(self, baseline_month: str, compare_month: str, 
                                 analysis_type: str = 'VOLUME') -> pd.DataFrame:
        """Compare two consecutive months and find increases"""
        print(f"🔍 Comparing {compare_month} vs {baseline_month} ({analysis_type})...")
        
        # Get data for both months
        baseline_data = self.get_month_data(baseline_month, analysis_type)
        compare_data = self.get_month_data(compare_month, analysis_type)
        
        # Merge on symbol
        merged = pd.merge(
            baseline_data, compare_data, 
            on='SYMBOL', 
            how='inner', 
            suffixes=('_BASELINE', '_COMPARE')
        )
        
        # Calculate increases
        merged['INCREASE_ABSOLUTE'] = merged['PEAK_VALUE_COMPARE'] - merged['PEAK_VALUE_BASELINE']
        merged['INCREASE_PERCENTAGE'] = (
            (merged['PEAK_VALUE_COMPARE'] - merged['PEAK_VALUE_BASELINE']) / 
            merged['PEAK_VALUE_BASELINE'] * 100
        ).round(2)
        
        # Filter only increases
        increases = merged[merged['INCREASE_ABSOLUTE'] > 0].copy()
        
        # Add comparison info
        increases['BASELINE_MONTH'] = baseline_month
        increases['COMPARE_MONTH'] = compare_month
        increases['ANALYSIS_TYPE'] = analysis_type
        increases['COMPARISON_NAME'] = f"{compare_month}_vs_{baseline_month}"
        
        # Sort by percentage increase
        increases = increases.sort_values('INCREASE_PERCENTAGE', ascending=False)
        
        print(f"   ✅ Found {len(increases)} symbols with {analysis_type.lower()} increases")
        return increases
        
    def generate_all_consecutive_comparisons(self) -> Dict[str, pd.DataFrame]:
        """Generate all consecutive month comparisons"""
        print("🚀 Generating all consecutive month comparisons...")
        print("=" * 60)
        
        all_comparisons = {}
        
        # Process volume comparisons
        for i in range(len(self.available_months) - 1):
            baseline_month = self.available_months[i]
            compare_month = self.available_months[i + 1]
            
            # Volume comparison
            volume_comp = self.compare_consecutive_months(
                baseline_month, compare_month, 'VOLUME'
            )
            comparison_key = f"{compare_month}_vs_{baseline_month}_VOLUME"
            all_comparisons[comparison_key] = volume_comp
            
            # Delivery comparison
            delivery_comp = self.compare_consecutive_months(
                baseline_month, compare_month, 'DELIVERY'
            )
            comparison_key = f"{compare_month}_vs_{baseline_month}_DELIVERY"
            all_comparisons[comparison_key] = delivery_comp
            
        return all_comparisons
        
    def generate_trend_analysis(self) -> pd.DataFrame:
        """Generate multi-month trend analysis for top performers"""
        print("📈 Generating multi-month trend analysis...")
        
        trend_data = []
        
        # Get all symbols that appear in multiple months
        cursor = self.db.connection.cursor()
        cursor.execute("""
            SELECT symbol, 
                   COUNT(DISTINCT analysis_month) as month_count,
                   analysis_type
            FROM step02_monthly_analysis
            GROUP BY symbol, analysis_type
            HAVING COUNT(DISTINCT analysis_month) >= 3
            ORDER BY month_count DESC, symbol
        """)
        
        multi_month_symbols = cursor.fetchall()
        print(f"   📊 Found {len(multi_month_symbols)} symbol-type combinations with 3+ months")
        
        for symbol, month_count, analysis_type in multi_month_symbols[:50]:  # Top 50
            # Get monthly progression for this symbol
            cursor.execute("""
                SELECT analysis_month, peak_value, peak_date
                FROM step02_monthly_analysis
                WHERE symbol = ? AND analysis_type = ?
                ORDER BY analysis_month
            """, (symbol, analysis_type))
            
            monthly_data = cursor.fetchall()
            
            if len(monthly_data) >= 3:
                # Calculate trend metrics
                values = [row[1] for row in monthly_data]
                months = [row[0] for row in monthly_data]
                
                # Calculate overall growth
                start_value = values[0]
                end_value = values[-1]
                overall_growth = ((end_value - start_value) / start_value * 100) if start_value > 0 else 0
                
                # Count consecutive increases
                consecutive_increases = 0
                max_consecutive = 0
                for i in range(1, len(values)):
                    if values[i] > values[i-1]:
                        consecutive_increases += 1
                        max_consecutive = max(max_consecutive, consecutive_increases)
                    else:
                        consecutive_increases = 0
                        
                trend_data.append({
                    'SYMBOL': symbol,
                    'ANALYSIS_TYPE': analysis_type,
                    'MONTH_COUNT': month_count,
                    'START_MONTH': months[0],
                    'END_MONTH': months[-1],
                    'START_VALUE': start_value,
                    'END_VALUE': end_value,
                    'OVERALL_GROWTH_PCT': round(overall_growth, 2),
                    'MAX_CONSECUTIVE_INCREASES': max_consecutive,
                    'MONTHLY_PROGRESSION': ' → '.join([f"{m}: {v:,.0f}" for m, v in zip(months, values)])
                })
                
        trend_df = pd.DataFrame(trend_data)
        trend_df = trend_df.sort_values('OVERALL_GROWTH_PCT', ascending=False)
        
        print(f"   ✅ Generated trend analysis for {len(trend_df)} symbols")
        return trend_df
        
    def create_summary_statistics(self, all_comparisons: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Create summary statistics for all comparisons"""
        print("📊 Creating summary statistics...")
        
        summary_data = []
        
        for comparison_name, df in all_comparisons.items():
            if not df.empty:
                parts = comparison_name.split('_')
                compare_month = parts[0]
                baseline_month = parts[2]
                analysis_type = parts[3]
                
                # Calculate statistics
                total_increases = len(df)
                avg_increase_pct = df['INCREASE_PERCENTAGE'].mean()
                median_increase_pct = df['INCREASE_PERCENTAGE'].median()
                max_increase_pct = df['INCREASE_PERCENTAGE'].max()
                top_performer = df.iloc[0]['SYMBOL'] if not df.empty else 'N/A'
                
                summary_data.append({
                    'COMPARISON': f"{compare_month} vs {baseline_month}",
                    'ANALYSIS_TYPE': analysis_type,
                    'TOTAL_INCREASES': total_increases,
                    'AVG_INCREASE_PCT': round(avg_increase_pct, 2),
                    'MEDIAN_INCREASE_PCT': round(median_increase_pct, 2),
                    'MAX_INCREASE_PCT': round(max_increase_pct, 2),
                    'TOP_PERFORMER': top_performer
                })
                
        summary_df = pd.DataFrame(summary_data)
        summary_df = summary_df.sort_values(['COMPARISON', 'ANALYSIS_TYPE'])
        
        print(f"   ✅ Created summary for {len(summary_df)} comparisons")
        return summary_df
        
    def export_to_excel(self, all_comparisons: Dict[str, pd.DataFrame], 
                       trend_analysis: pd.DataFrame, summary_stats: pd.DataFrame):
        """Export all analysis to comprehensive Excel file"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"step03_monthly_comparisons/Step03_Monthly_Comparison_Analysis_{timestamp}.xlsx"
        
        # Ensure directory exists
        os.makedirs("step03_monthly_comparisons", exist_ok=True)
        
        print(f"💾 Creating comprehensive Excel report: {output_file}")
        
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # Summary sheets first
            summary_stats.to_excel(writer, sheet_name='Summary_Statistics', index=False)
            trend_analysis.to_excel(writer, sheet_name='Multi_Month_Trends', index=False)
            
            # Individual comparison sheets
            sheet_count = 0
            for comparison_name, df in all_comparisons.items():
                if not df.empty and sheet_count < 28:  # Excel sheet limit
                    # Clean sheet name
                    sheet_name = comparison_name.replace('2025-', '').replace('_vs_', 'v')
                    sheet_name = sheet_name[:31]  # Excel sheet name limit
                    
                    # Select key columns for output
                    output_cols = [
                        'SYMBOL', 'PEAK_VALUE_BASELINE', 'PEAK_VALUE_COMPARE', 
                        'INCREASE_ABSOLUTE', 'INCREASE_PERCENTAGE',
                        'PEAK_DATE_BASELINE', 'PEAK_DATE_COMPARE'
                    ]
                    
                    output_df = df[output_cols].copy()
                    output_df.to_excel(writer, sheet_name=sheet_name, index=False)
                    sheet_count += 1
            
            # Create overview sheet
            overview_data = {
                'Metric': [
                    'Total Months Analyzed',
                    'Total Comparisons Generated',
                    'Analysis Period',
                    'Database Records Used',
                    'Generated On'
                ],
                'Value': [
                    len(self.available_months),
                    len(all_comparisons),
                    f"{self.available_months[0]} to {self.available_months[-1]}",
                    f"Step02 Monthly Analysis",
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                ]
            }
            overview_df = pd.DataFrame(overview_data)
            overview_df.to_excel(writer, sheet_name='Overview', index=False)
        
        print(f"   ✅ Excel report created with {sheet_count + 3} sheets")
        return output_file
        
    def run_complete_step03_analysis(self):
        """Run complete Step 3 analysis"""
        print("🚀 STEP 03: Month-to-Month Comparison Analysis")
        print("=" * 70)
        print(f"📅 Analysis Period: {self.available_months[0]} to {self.available_months[-1]}")
        print(f"📊 Total Months: {len(self.available_months)}")
        print()
        
        # Generate all comparisons
        all_comparisons = self.generate_all_consecutive_comparisons()
        
        print()
        # Generate trend analysis
        trend_analysis = self.generate_trend_analysis()
        
        print()
        # Create summary statistics
        summary_stats = self.create_summary_statistics(all_comparisons)
        
        print()
        # Export to Excel
        output_file = self.export_to_excel(all_comparisons, trend_analysis, summary_stats)
        
        # Show results summary
        self.show_results_summary(all_comparisons, trend_analysis)
        
        return output_file
        
    def show_results_summary(self, all_comparisons: Dict[str, pd.DataFrame], 
                           trend_analysis: pd.DataFrame):
        """Show summary of analysis results"""
        print("\n📊 STEP 03 ANALYSIS RESULTS SUMMARY")
        print("=" * 50)
        
        # Comparison summary
        volume_comps = {k: v for k, v in all_comparisons.items() if 'VOLUME' in k}
        delivery_comps = {k: v for k, v in all_comparisons.items() if 'DELIVERY' in k}
        
        print(f"📈 Volume Comparisons: {len(volume_comps)}")
        print(f"📦 Delivery Comparisons: {len(delivery_comps)}")
        
        # Show top performers by category
        print(f"\n🏆 TOP VOLUME PERFORMERS BY MONTH:")
        for comp_name, df in list(volume_comps.items())[:3]:
            if not df.empty:
                top_symbol = df.iloc[0]
                month_pair = comp_name.replace('2025-', '').replace('_VOLUME', '').replace('_vs_', ' vs ')
                print(f"  {month_pair}: {top_symbol['SYMBOL']} (+{top_symbol['INCREASE_PERCENTAGE']:.1f}%)")
        
        print(f"\n📦 TOP DELIVERY PERFORMERS BY MONTH:")
        for comp_name, df in list(delivery_comps.items())[:3]:
            if not df.empty:
                top_symbol = df.iloc[0]
                month_pair = comp_name.replace('2025-', '').replace('_DELIVERY', '').replace('_vs_', ' vs ')
                print(f"  {month_pair}: {top_symbol['SYMBOL']} (+{top_symbol['INCREASE_PERCENTAGE']:.1f}%)")
        
        # Trend analysis summary
        if not trend_analysis.empty:
            print(f"\n📈 MULTI-MONTH TREND LEADERS:")
            for i, row in trend_analysis.head(5).iterrows():
                print(f"  {row['SYMBOL']} ({row['ANALYSIS_TYPE']}): {row['OVERALL_GROWTH_PCT']:.1f}% growth over {row['MONTH_COUNT']} months")
        
    def close(self):
        """Close database connection"""
        self.db.close()

def main():
    analyzer = Step03MonthlyComparison()
    try:
        output_file = analyzer.run_complete_step03_analysis()
        print(f"\n✅ STEP 03 ANALYSIS COMPLETE!")
        print(f"📄 Output File: {output_file}")
        print(f"🎯 Ready for Step 04: Derivatives Analysis")
    finally:
        analyzer.close()

if __name__ == '__main__':
    main()
