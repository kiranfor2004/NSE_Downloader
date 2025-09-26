"""
🎯 F&O OPTIONS RISK DASHBOARD - SIMPLE HTML VERSION
==================================================
Simple standalone dashboard that generates HTML report
Data Sources: Step05_monthly_50percent_reduction_analysis & Step05_strikepriceAnalysisderived
"""

import pandas as pd
import pyodbc
import json
from datetime import datetime
import os

def get_database_connection():
    """Establish database connection"""
    try:
        conn = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};'
            'SERVER=SRIKIRANREDDY\\SQLEXPRESS;'
            'DATABASE=master;'
            'Trusted_Connection=yes;'
        )
        return conn
    except Exception as e:
        print(f"Database connection failed: {e}")
        return None

def load_reduction_analysis_data():
    """Load main reduction analysis data"""
    conn = get_database_connection()
    if not conn:
        return pd.DataFrame()
    
    query = """
    SELECT 
        symbol,
        strike_price,
        option_type,
        reduction_50_found as achieved_50_percent_reduction,
        reduction_50_percentage as reduction_percentage,
        days_to_50_reduction as days_to_50_percent_reduction,
        max_reduction_percentage,
        base_close_price as base_price,
        final_month_price as final_price,
        reduction_50_date,
        analysis_timestamp,
        volatility_score,
        avg_daily_reduction,
        best_single_day_gain,
        worst_single_day_loss
    FROM Step05_monthly_50percent_reduction_analysis
    ORDER BY reduction_50_found DESC, reduction_50_percentage DESC
    """
    
    try:
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        print(f"Error loading reduction analysis data: {e}")
        conn.close()
        return pd.DataFrame()

def generate_html_dashboard():
    """Generate comprehensive HTML dashboard"""
    
    print("📊 Loading data from database...")
    df = load_reduction_analysis_data()
    
    if df.empty:
        print("❌ No data available. Please check database connection.")
        return
    
    print(f"✅ Loaded {len(df):,} records successfully")
    
    # Calculate KPIs
    total_strikes = len(df)
    successful_reductions = len(df[df['achieved_50_percent_reduction'] == 1])
    success_rate = (successful_reductions / total_strikes * 100) if total_strikes > 0 else 0
    avg_days_to_reduction = df[df['achieved_50_percent_reduction'] == 1]['days_to_50_percent_reduction'].mean()
    avg_reduction = df[df['achieved_50_percent_reduction'] == 1]['reduction_percentage'].mean()
    
    # Symbol statistics
    symbol_stats = df.groupby('symbol').agg({
        'achieved_50_percent_reduction': ['count', 'sum'],
        'reduction_percentage': 'mean',
        'days_to_50_percent_reduction': 'mean'
    }).round(2)
    
    symbol_stats.columns = ['total_strikes', 'successful_reductions', 'avg_reduction', 'avg_days']
    symbol_stats['success_rate'] = (symbol_stats['successful_reductions'] / symbol_stats['total_strikes'] * 100).round(1)
    symbol_stats = symbol_stats.reset_index().sort_values('success_rate', ascending=False)
    
    # Option type statistics
    option_stats = df.groupby('option_type').agg({
        'achieved_50_percent_reduction': ['count', 'sum'],
        'reduction_percentage': 'mean',
        'days_to_50_percent_reduction': 'mean'
    }).round(2)
    
    option_stats.columns = ['total_strikes', 'successful_reductions', 'avg_reduction', 'avg_days']
    option_stats['success_rate'] = (option_stats['successful_reductions'] / option_stats['total_strikes'] * 100).round(1)
    option_stats = option_stats.reset_index()
    
    # Top performers
    top_performers = df[df['achieved_50_percent_reduction'] == 1].nlargest(20, 'reduction_percentage')
    
    # Generate HTML
    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>F&O Options Risk Analytics Dashboard</title>
        <style>
            * {{
                margin: 0;
                padding: 0;
                box-sizing: border-box;
            }}
            
            body {{
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                min-height: 100vh;
                padding: 20px;
            }}
            
            .container {{
                max-width: 1400px;
                margin: 0 auto;
                background: white;
                border-radius: 15px;
                box-shadow: 0 20px 40px rgba(0,0,0,0.1);
                overflow: hidden;
            }}
            
            .header {{
                background: linear-gradient(135deg, #1f77b4 0%, #1565c0 100%);
                color: white;
                padding: 40px;
                text-align: center;
            }}
            
            .header h1 {{
                font-size: 2.5rem;
                margin-bottom: 10px;
                font-weight: 700;
            }}
            
            .header p {{
                font-size: 1.2rem;
                opacity: 0.9;
            }}
            
            .content {{
                padding: 40px;
            }}
            
            .kpi-section {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
                gap: 20px;
                margin-bottom: 40px;
            }}
            
            .kpi-card {{
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
                padding: 30px;
                border-radius: 15px;
                text-align: center;
                box-shadow: 0 10px 20px rgba(0,0,0,0.1);
                transition: transform 0.3s ease;
            }}
            
            .kpi-card:hover {{
                transform: translateY(-5px);
            }}
            
            .kpi-value {{
                font-size: 2.5rem;
                font-weight: bold;
                margin-bottom: 10px;
            }}
            
            .kpi-label {{
                font-size: 1rem;
                opacity: 0.9;
            }}
            
            .section-title {{
                font-size: 1.8rem;
                color: #1f77b4;
                margin: 40px 0 20px 0;
                border-bottom: 3px solid #1f77b4;
                padding-bottom: 10px;
            }}
            
            .chart-container {{
                background: #f8f9fa;
                border-radius: 10px;
                padding: 20px;
                margin: 20px 0;
                box-shadow: 0 5px 15px rgba(0,0,0,0.1);
            }}
            
            .stats-grid {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
                gap: 20px;
                margin: 20px 0;
            }}
            
            .stats-card {{
                background: white;
                border: 1px solid #e0e0e0;
                border-radius: 10px;
                padding: 20px;
                box-shadow: 0 5px 15px rgba(0,0,0,0.1);
            }}
            
            .stats-card h3 {{
                color: #1f77b4;
                margin-bottom: 15px;
            }}
            
            table {{
                width: 100%;
                border-collapse: collapse;
                margin: 20px 0;
                font-size: 0.9rem;
            }}
            
            th, td {{
                padding: 12px;
                text-align: left;
                border-bottom: 1px solid #e0e0e0;
            }}
            
            th {{
                background: #1f77b4;
                color: white;
                font-weight: bold;
            }}
            
            tr:nth-child(even) {{
                background: #f8f9fa;
            }}
            
            tr:hover {{
                background: #e3f2fd;
            }}
            
            .success {{
                background: #d4edda !important;
                color: #155724;
                font-weight: bold;
            }}
            
            .failure {{
                background: #f8d7da !important;
                color: #721c24;
            }}
            
            .metric-highlight {{
                color: #1f77b4;
                font-weight: bold;
            }}
            
            .footer {{
                background: #1f77b4;
                color: white;
                text-align: center;
                padding: 20px;
                font-style: italic;
            }}
            
            @media (max-width: 768px) {{
                .kpi-section {{
                    grid-template-columns: 1fr;
                }}
                
                .stats-grid {{
                    grid-template-columns: 1fr;
                }}
                
                .header h1 {{
                    font-size: 2rem;
                }}
                
                .content {{
                    padding: 20px;
                }}
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>🎯 F&O OPTIONS RISK ANALYTICS DASHBOARD</h1>
                <p>Interactive analysis of 50% price reduction patterns in F&O options trading</p>
                <p>Data as of February 2025 | Generated on {datetime.now().strftime("%B %d, %Y at %I:%M %p")}</p>
            </div>
            
            <div class="content">
                <!-- KPI Section -->
                <h2 class="section-title">📊 Key Performance Indicators</h2>
                <div class="kpi-section">
                    <div class="kpi-card">
                        <div class="kpi-value">{total_strikes:,}</div>
                        <div class="kpi-label">Total Strikes Analyzed</div>
                    </div>
                    <div class="kpi-card">
                        <div class="kpi-value">{successful_reductions:,}</div>
                        <div class="kpi-label">Achieved 50%+ Reduction</div>
                    </div>
                    <div class="kpi-card">
                        <div class="kpi-value">{success_rate:.1f}%</div>
                        <div class="kpi-label">Overall Success Rate</div>
                    </div>
                    <div class="kpi-card">
                        <div class="kpi-value">{avg_days_to_reduction:.1f}</div>
                        <div class="kpi-label">Avg Days to 50% Reduction</div>
                    </div>
                    <div class="kpi-card">
                        <div class="kpi-value">{avg_reduction:.1f}%</div>
                        <div class="kpi-label">Avg Reduction Percentage</div>
                    </div>
                </div>
                
                <!-- Top Performers Section -->
                <h2 class="section-title">🏆 Top 20 Best Performers (100% Losses)</h2>
                <div class="chart-container">
                    <table>
                        <thead>
                            <tr>
                                <th>Rank</th>
                                <th>Symbol</th>
                                <th>Strike Price</th>
                                <th>Option Type</th>
                                <th>Reduction %</th>
                                <th>Days to 50%</th>
                                <th>Base Price</th>
                                <th>Final Price</th>
                            </tr>
                        </thead>
                        <tbody>
    """
    
    for idx, row in top_performers.head(20).iterrows():
        html_content += f"""
                            <tr class="success">
                                <td>{top_performers.index.get_loc(idx) + 1}</td>
                                <td><strong>{row['symbol']}</strong></td>
                                <td>₹{row['strike_price']:,.0f}</td>
                                <td>{row['option_type']}</td>
                                <td class="metric-highlight">{row['reduction_percentage']:.1f}%</td>
                                <td>{row['days_to_50_percent_reduction']:.0f} days</td>
                                <td>₹{row['base_price']:.2f}</td>
                                <td>₹{row['final_price']:.2f}</td>
                            </tr>
        """
    
    html_content += """
                        </tbody>
                    </table>
                </div>
                
                <!-- Symbol Analysis Section -->
                <h2 class="section-title">📈 Top 20 Symbols by Success Rate</h2>
                <div class="chart-container">
                    <table>
                        <thead>
                            <tr>
                                <th>Rank</th>
                                <th>Symbol</th>
                                <th>Total Strikes</th>
                                <th>Successful</th>
                                <th>Success Rate</th>
                                <th>Avg Reduction %</th>
                                <th>Avg Days</th>
                            </tr>
                        </thead>
                        <tbody>
    """
    
    for idx, row in symbol_stats.head(20).iterrows():
        html_content += f"""
                            <tr>
                                <td>{idx + 1}</td>
                                <td><strong>{row['symbol']}</strong></td>
                                <td>{row['total_strikes']:,.0f}</td>
                                <td class="metric-highlight">{row['successful_reductions']:,.0f}</td>
                                <td class="metric-highlight">{row['success_rate']:.1f}%</td>
                                <td>{row['avg_reduction']:.1f}%</td>
                                <td>{row['avg_days']:.1f} days</td>
                            </tr>
        """
    
    html_content += """
                        </tbody>
                    </table>
                </div>
                
                <!-- Option Type Analysis -->
                <h2 class="section-title">📊 Call vs Put Options Analysis</h2>
                <div class="stats-grid">
    """
    
    for idx, row in option_stats.iterrows():
        option_name = "Call Options (CE)" if row['option_type'] == 'CE' else "Put Options (PE)"
        html_content += f"""
                    <div class="stats-card">
                        <h3>{option_name}</h3>
                        <p><strong>Total Strikes:</strong> {row['total_strikes']:,}</p>
                        <p><strong>Successful Reductions:</strong> <span class="metric-highlight">{row['successful_reductions']:,}</span></p>
                        <p><strong>Success Rate:</strong> <span class="metric-highlight">{row['success_rate']:.1f}%</span></p>
                        <p><strong>Average Reduction:</strong> {row['avg_reduction']:.1f}%</p>
                        <p><strong>Average Days:</strong> {row['avg_days']:.1f} days</p>
                    </div>
        """
    
    # Sample data table
    sample_data = df.head(100)
    
    html_content += f"""
                </div>
                
                <!-- Sample Data Table -->
                <h2 class="section-title">📋 Sample Data (First 100 Records)</h2>
                <div class="chart-container">
                    <table>
                        <thead>
                            <tr>
                                <th>Symbol</th>
                                <th>Strike</th>
                                <th>Type</th>
                                <th>50%+ Achieved</th>
                                <th>Reduction %</th>
                                <th>Days</th>
                                <th>Base Price</th>
                                <th>Final Price</th>
                            </tr>
                        </thead>
                        <tbody>
    """
    
    for idx, row in sample_data.iterrows():
        css_class = "success" if row['achieved_50_percent_reduction'] == 1 else "failure"
        achieved_text = "✅ Yes" if row['achieved_50_percent_reduction'] == 1 else "❌ No"
        
        html_content += f"""
                            <tr class="{css_class}">
                                <td><strong>{row['symbol']}</strong></td>
                                <td>₹{row['strike_price']:,.0f}</td>
                                <td>{row['option_type']}</td>
                                <td>{achieved_text}</td>
                                <td>{row['reduction_percentage']:.1f}%</td>
                                <td>{row['days_to_50_percent_reduction']:.0f} days</td>
                                <td>₹{row['base_price']:.2f}</td>
                                <td>₹{row['final_price']:.2f}</td>
                            </tr>
        """
    
    html_content += f"""
                        </tbody>
                    </table>
                    <p><em>Showing first 100 records out of {len(df):,} total records. Green rows achieved 50%+ reduction, red rows did not.</em></p>
                </div>
                
                <!-- Summary Statistics -->
                <h2 class="section-title">📊 Summary Statistics</h2>
                <div class="stats-grid">
                    <div class="stats-card">
                        <h3>📈 Reduction Distribution</h3>
                        <p><strong>Minimum Reduction:</strong> {df[df['achieved_50_percent_reduction'] == 1]['reduction_percentage'].min():.1f}%</p>
                        <p><strong>Maximum Reduction:</strong> {df[df['achieved_50_percent_reduction'] == 1]['reduction_percentage'].max():.1f}%</p>
                        <p><strong>Median Reduction:</strong> {df[df['achieved_50_percent_reduction'] == 1]['reduction_percentage'].median():.1f}%</p>
                        <p><strong>Standard Deviation:</strong> {df[df['achieved_50_percent_reduction'] == 1]['reduction_percentage'].std():.1f}%</p>
                    </div>
                    
                    <div class="stats-card">
                        <h3>⏱️ Time Analysis</h3>
                        <p><strong>Fastest 50% Reduction:</strong> {df[df['achieved_50_percent_reduction'] == 1]['days_to_50_percent_reduction'].min():.0f} days</p>
                        <p><strong>Slowest 50% Reduction:</strong> {df[df['achieved_50_percent_reduction'] == 1]['days_to_50_percent_reduction'].max():.0f} days</p>
                        <p><strong>Median Days:</strong> {df[df['achieved_50_percent_reduction'] == 1]['days_to_50_percent_reduction'].median():.1f} days</p>
                        <p><strong>Most Common Days:</strong> {df[df['achieved_50_percent_reduction'] == 1]['days_to_50_percent_reduction'].mode().iloc[0]:.0f} days</p>
                    </div>
                    
                    <div class="stats-card">
                        <h3>🎯 Coverage Analysis</h3>
                        <p><strong>Total Symbols:</strong> {df['symbol'].nunique():,}</p>
                        <p><strong>Total Strike Prices:</strong> {df['strike_price'].nunique():,}</p>
                        <p><strong>Call Options:</strong> {len(df[df['option_type'] == 'CE']):,}</p>
                        <p><strong>Put Options:</strong> {len(df[df['option_type'] == 'PE']):,}</p>
                    </div>
                    
                    <div class="stats-card">
                        <h3>💰 Price Analysis</h3>
                        <p><strong>Avg Base Price:</strong> ₹{df['base_price'].mean():.2f}</p>
                        <p><strong>Avg Final Price:</strong> ₹{df['final_price'].mean():.2f}</p>
                        <p><strong>Highest Base Price:</strong> ₹{df['base_price'].max():.2f}</p>
                        <p><strong>Lowest Final Price:</strong> ₹{df['final_price'].min():.2f}</p>
                    </div>
                </div>
            </div>
            
            <div class="footer">
                <p>F&O Options Risk Analytics Dashboard | Data as of February 2025</p>
                <p>Generated from Step05_monthly_50percent_reduction_analysis table</p>
                <p>Dashboard created on {datetime.now().strftime("%B %d, %Y at %I:%M %p")}</p>
            </div>
        </div>
    </body>
    </html>
    """
    
    # Save HTML file
    filename = f"FO_Risk_Dashboard_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"✅ Dashboard generated successfully!")
    print(f"📁 File saved as: {filename}")
    print(f"🌐 Open the file in your browser to view the dashboard")
    print(f"📊 Dashboard contains analysis of {len(df):,} F&O options records")
    
    return filename

if __name__ == "__main__":
    print("🚀 Generating F&O Risk Analytics Dashboard...")
    print("📊 Creating Tableau-style HTML report...")
    print("="*60)
    
    filename = generate_html_dashboard()
    
    if filename:
        print("\n" + "="*60)
        print("🎉 DASHBOARD GENERATION COMPLETE!")
        print("="*60)
        print(f"📁 File: {filename}")
        print("🌐 Double-click the HTML file to open in your browser")
        print("📱 Responsive design works on desktop, tablet, and mobile")
        print("🎨 Tableau-style visual design with interactive elements")
        print("📊 Complete analysis of 50% F&O options price reductions")
    else:
        print("❌ Dashboard generation failed!")