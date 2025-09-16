#!/usr/bin/env python3
"""
Windows-Compatible NSE Dashboard
Uses a different approach that works with Windows security restrictions
"""
import webbrowser
import tempfile
import os
import json
import pyodbc
from datetime import datetime

def get_data():
    """Get dashboard data from database"""
    try:
        conn = pyodbc.connect(
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=SRIKIRANREDDY\\SQLEXPRESS;"
            "DATABASE=master;"
            "Trusted_Connection=yes;"
        )
        cursor = conn.cursor()
        
        # Get basic stats
        cursor.execute("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT symbol) as total_symbols,
                AVG(CAST(current_deliv_per AS FLOAT)) as avg_delivery,
                MAX(CAST(current_deliv_per AS FLOAT)) as max_delivery
            FROM step03_compare_monthvspreviousmonth
            WHERE current_deliv_per IS NOT NULL
        """)
        stats = cursor.fetchone()
        
        # Get top symbols
        cursor.execute("""
            SELECT TOP 20
                symbol,
                CAST(current_deliv_per AS FLOAT) as delivery_pct,
                current_deliv_qty,
                current_trade_date
            FROM step03_compare_monthvspreviousmonth
            WHERE current_deliv_per IS NOT NULL
            ORDER BY CAST(current_deliv_per AS FLOAT) DESC
        """)
        
        top_symbols = []
        for row in cursor.fetchall():
            top_symbols.append({
                'symbol': row.symbol,
                'delivery_pct': float(row.delivery_pct),
                'delivery_qty': int(row.current_deliv_qty or 0),
                'trade_date': str(row.current_trade_date) if row.current_trade_date else 'N/A'
            })
        
        cursor.close()
        conn.close()
        
        return {
            'total_records': stats.total_records,
            'total_symbols': stats.total_symbols,
            'avg_delivery': float(stats.avg_delivery or 0),
            'max_delivery': float(stats.max_delivery or 0),
            'top_symbols': top_symbols,
            'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
    except Exception as e:
        return {'error': str(e)}

def create_html_dashboard(data):
    """Create a complete HTML dashboard"""
    html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>NSE Delivery Analysis Dashboard</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ 
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; 
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }}
        
        .container {{ 
            max-width: 1200px; 
            margin: 0 auto; 
            background: rgba(255, 255, 255, 0.95);
            border-radius: 15px;
            box-shadow: 0 20px 40px rgba(0, 0, 0, 0.1);
            overflow: hidden;
        }}
        
        .header {{ 
            background: linear-gradient(135deg, #2c3e50 0%, #3498db 100%);
            color: white; 
            padding: 30px; 
            text-align: center; 
        }}
        
        .header h1 {{ 
            font-size: 2.5em; 
            margin-bottom: 10px; 
            text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
        }}
        
        .header p {{ 
            font-size: 1.2em; 
            opacity: 0.9; 
        }}
        
        .stats-grid {{ 
            display: grid; 
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); 
            gap: 20px; 
            padding: 30px; 
            background: #f8f9fa;
        }}
        
        .stat-card {{ 
            background: white;
            padding: 25px;
            border-radius: 10px;
            text-align: center;
            box-shadow: 0 5px 15px rgba(0, 0, 0, 0.1);
            transition: transform 0.3s ease;
        }}
        
        .stat-card:hover {{ 
            transform: translateY(-5px); 
        }}
        
        .stat-value {{ 
            font-size: 2.5em; 
            font-weight: bold; 
            color: #3498db; 
            margin-bottom: 10px;
        }}
        
        .stat-label {{ 
            color: #666; 
            font-size: 1.1em; 
            font-weight: 500;
        }}
        
        .chart-section {{ 
            padding: 30px; 
        }}
        
        .chart-container {{ 
            background: white; 
            border-radius: 10px; 
            padding: 25px; 
            box-shadow: 0 5px 15px rgba(0, 0, 0, 0.1);
            margin-bottom: 30px;
        }}
        
        .chart-title {{ 
            font-size: 1.5em; 
            color: #2c3e50; 
            margin-bottom: 20px; 
            text-align: center;
        }}
        
        .table-container {{ 
            background: white; 
            border-radius: 10px; 
            padding: 25px; 
            box-shadow: 0 5px 15px rgba(0, 0, 0, 0.1);
        }}
        
        table {{ 
            width: 100%; 
            border-collapse: collapse; 
        }}
        
        th, td {{ 
            padding: 12px; 
            text-align: left; 
            border-bottom: 1px solid #ddd; 
        }}
        
        th {{ 
            background: #f1f3f4; 
            font-weight: 600; 
            color: #2c3e50;
        }}
        
        tr:hover {{ 
            background: #f8f9fa; 
        }}
        
        .footer {{ 
            background: #2c3e50; 
            color: white; 
            text-align: center; 
            padding: 20px; 
            font-size: 0.9em;
        }}
        
        .error {{ 
            color: #e74c3c; 
            text-align: center; 
            padding: 50px; 
            font-size: 1.2em;
        }}
        
        .success-badge {{ 
            background: #27ae60; 
            color: white; 
            padding: 5px 10px; 
            border-radius: 15px; 
            font-size: 0.8em;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 NSE Delivery Analysis Dashboard</h1>
            <p>Professional Market Intelligence Platform</p>
            <span class="success-badge">✅ Live Data Connected</span>
        </div>
"""

    if 'error' in data:
        html += f"""
        <div class="error">
            ❌ Database Connection Error: {data['error']}
        </div>
        """
    else:
        html += f"""
        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-value">{data['total_records']:,}</div>
                <div class="stat-label">Total Records</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{data['total_symbols']:,}</div>
                <div class="stat-label">Total Symbols</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{data['avg_delivery']:.2f}%</div>
                <div class="stat-label">Average Delivery %</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{data['max_delivery']:.2f}%</div>
                <div class="stat-label">Maximum Delivery %</div>
            </div>
        </div>
        
        <div class="chart-section">
            <div class="chart-container">
                <div class="chart-title">🔥 Top 20 Symbols by Delivery Percentage</div>
                <div id="chart" style="height: 500px;"></div>
            </div>
            
            <div class="table-container">
                <div class="chart-title">📈 Detailed Symbol Analysis</div>
                <table>
                    <thead>
                        <tr>
                            <th>Rank</th>
                            <th>Symbol</th>
                            <th>Delivery %</th>
                            <th>Delivery Quantity</th>
                            <th>Trade Date</th>
                        </tr>
                    </thead>
                    <tbody>
        """
        
        for i, symbol in enumerate(data['top_symbols'], 1):
            html += f"""
                        <tr>
                            <td>{i}</td>
                            <td><strong>{symbol['symbol']}</strong></td>
                            <td>{symbol['delivery_pct']:.2f}%</td>
                            <td>{symbol['delivery_qty']:,}</td>
                            <td>{symbol['trade_date']}</td>
                        </tr>
            """
        
        html += """
                    </tbody>
                </table>
            </div>
        </div>
        """

    html += f"""
        <div class="footer">
            📅 Last Updated: {data.get('last_updated', 'Unknown')} | 
            🏢 Data Source: step03_compare_monthvspreviousmonth | 
            💻 NSE Delivery Analysis Platform
        </div>
    </div>
    
    <script>
        // Create interactive chart
        const chartData = {{
            x: {json.dumps([s['symbol'] for s in data.get('top_symbols', [])])},
            y: {json.dumps([s['delivery_pct'] for s in data.get('top_symbols', [])])},
            type: 'bar',
            marker: {{
                color: {json.dumps([f'rgb({int(255-s["delivery_pct"]*2.55)}, {int(s["delivery_pct"]*2.55)}, 100)' for s in data.get('top_symbols', [])])},
                line: {{ color: 'rgb(8,48,107)', width: 1 }}
            }},
            text: {json.dumps([f'{s["symbol"]}: {s["delivery_pct"]:.2f}%' for s in data.get('top_symbols', [])])},
            textposition: 'auto',
            hovertemplate: '<b>%{{x}}</b><br>Delivery: %{{y:.2f}}%<extra></extra>'
        }};
        
        const layout = {{
            title: {{
                text: 'Delivery Percentage by Symbol',
                font: {{ size: 18, color: '#2c3e50' }}
            }},
            xaxis: {{ 
                title: 'Symbol',
                tickangle: -45,
                color: '#2c3e50'
            }},
            yaxis: {{ 
                title: 'Delivery Percentage (%)',
                color: '#2c3e50'
            }},
            plot_bgcolor: '#f8f9fa',
            paper_bgcolor: 'white',
            font: {{ family: 'Segoe UI, sans-serif' }}
        }};
        
        Plotly.newPlot('chart', [chartData], layout, {{responsive: true}});
    </script>
</body>
</html>
    """
    
    return html

def main():
    print("🚀 Generating NSE Dashboard (Windows Compatible Version)")
    print("=" * 60)
    
    # Get data from database
    print("📊 Fetching data from database...")
    data = get_data()
    
    if 'error' in data:
        print(f"❌ Database Error: {data['error']}")
    else:
        print(f"✅ Data loaded successfully:")
        print(f"   📈 {data['total_records']:,} total records")
        print(f"   🔖 {data['total_symbols']:,} unique symbols")
        print(f"   📊 Average delivery: {data['avg_delivery']:.2f}%")
        print(f"   🔥 Maximum delivery: {data['max_delivery']:.2f}%")
    
    # Generate HTML
    print("🌐 Generating HTML dashboard...")
    html_content = create_html_dashboard(data)
    
    # Save to temporary file
    temp_file = os.path.join(tempfile.gettempdir(), 'nse_dashboard.html')
    with open(temp_file, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"💾 Dashboard saved to: {temp_file}")
    print("🌐 Opening dashboard in your default browser...")
    
    # Open in browser
    webbrowser.open(f'file://{temp_file}')
    
    print("=" * 60)
    print("✅ SUCCESS! Your NSE Dashboard is now open in your browser!")
    print("📊 Features:")
    print("   • Real-time data from your SQL Server database")
    print("   • Interactive charts with Plotly.js")
    print("   • Top 20 symbols by delivery percentage")
    print("   • Detailed symbol analysis table")
    print("   • Professional responsive design")
    print("=" * 60)

if __name__ == '__main__':
    main()