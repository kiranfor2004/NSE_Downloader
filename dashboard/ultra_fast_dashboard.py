import pyodbc
import pandas as pd
import json
from datetime import datetime
import numpy as np

def create_ultra_fast_dashboard():
    """Create an ultra-fast loading dashboard with pre-calculated data"""
    
    # Database configuration
    server = 'SRIKIRANREDDY\\SQLEXPRESS'
    database = 'master'
    
    # Connect to database
    conn = pyodbc.connect(f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes')
    
    # Super optimized query - only essential data
    query = '''
    SELECT TOP 1000
        symbol,
        ISNULL(current_deliv_per, 0) as delivery_percentage,
        ISNULL(((current_close_price - current_prev_close) / NULLIF(current_prev_close, 0)) * 100, 0) as price_change_pct,
        ISNULL(current_ttl_trd_qnty, 0) as volume,
        ISNULL(current_turnover_lacs, 0) as turnover,
        ISNULL(current_close_price, 0) as close_price,
        ISNULL(current_high_price, 0) as high_price,
        ISNULL(current_low_price, 0) as low_price,
        ISNULL(current_open_price, 0) as open_price,
        ISNULL(current_deliv_qty, 0) as delivery_qty,
        ISNULL(delivery_increase_pct, 0) as delivery_increase_pct,
        ISNULL(delivery_increase_abs, 0) as delivery_increase_abs,
        ISNULL(current_no_of_trades, 0) as no_of_trades,
        ISNULL(index_name, 'Others') as index_name,
        ISNULL(category, 'Others') as category,
        CASE 
            WHEN symbol LIKE '%BANK%' OR symbol IN ('HDFCBANK', 'ICICIBANK', 'SBIN', 'AXISBANK', 'KOTAKBANK') THEN 'Banking'
            WHEN symbol IN ('TCS', 'INFY', 'WIPRO', 'HCLTECH', 'TECHM') THEN 'IT'
            WHEN symbol IN ('RELIANCE', 'ONGC', 'BPCL', 'IOC') THEN 'Energy'
            WHEN symbol IN ('ITC', 'HINDUNILVR', 'NESTLEIND', 'BRITANNIA') THEN 'FMCG'
            WHEN symbol IN ('MARUTI', 'M&M', 'TATAMOTORS', 'BAJAJ-AUTO') THEN 'Auto'
            WHEN symbol IN ('SUNPHARMA', 'DRREDDY', 'CIPLA', 'DIVISLAB') THEN 'Pharma'
            ELSE 'Others'
        END as sector
    FROM step03_compare_monthvspreviousmonth 
    WHERE current_turnover_lacs > 100
        AND symbol IS NOT NULL
    ORDER BY current_turnover_lacs DESC
    '''
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    print(f"Loaded {len(df)} records for Ultra-Fast Dashboard")
    
    # Quick data processing with minimal calculations
    total_delivery_increase = round(df['delivery_increase_abs'].sum() / 100000, 2)
    positive_delivery_growth = len(df[df['delivery_increase_pct'] > 0])
    delivery_turnover_ratio = round((df['delivery_qty'].sum() / df['turnover'].sum()) * 100, 2)
    avg_daily_turnover = round(df['turnover'].mean(), 2)
    
    # Pre-calculate only what we need for charts
    top_10_symbols = df.head(10)
    top_20_symbols = df.head(20)
    
    # Minimal data structure for fast loading
    dashboard_data = {
        'kpis': {
            'delivery_increase': total_delivery_increase,
            'positive_growth': positive_delivery_growth,
            'delivery_ratio': delivery_turnover_ratio,
            'avg_turnover': avg_daily_turnover
        },
        'market_charts': {
            'categories': df.groupby('category')['turnover'].sum().head(6).to_dict(),
            'sectors': df.groupby('sector')['delivery_qty'].sum().head(6).to_dict(),
            'top_symbols': {
                'names': top_10_symbols['symbol'].tolist(),
                'delivery': top_10_symbols['delivery_percentage'].tolist(),
                'turnover': top_10_symbols['turnover'].tolist(),
                'price_change': top_10_symbols['price_change_pct'].tolist()
            }
        },
        'symbols': {
            symbol: {
                'delivery': float(row['delivery_percentage']),
                'price_change': float(row['price_change_pct']),
                'volume': int(row['volume']),
                'turnover': float(row['turnover']),
                'close': float(row['close_price']),
                'high': float(row['high_price']),
                'low': float(row['low_price']),
                'open': float(row['open_price']),
                'sector': row['sector']
            } for symbol, row in top_20_symbols.set_index('symbol').iterrows()
        }
    }
    
    # Ultra-fast loading HTML with minimal JavaScript
    html_content = f'''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>NSE Ultra-Fast Dashboard</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <link href="https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;500;600;700&display=swap" rel="stylesheet">
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        
        body {{
            font-family: 'Poppins', sans-serif;
            background: #1A1A1A;
            color: #E0E0E0;
            line-height: 1.6;
        }}
        
        .container {{
            max-width: 1600px;
            margin: 0 auto;
            padding: 20px;
        }}
        
        .header {{
            text-align: center;
            margin-bottom: 25px;
            background: linear-gradient(135deg, #1A1A1A, #2C2C2C);
            padding: 25px;
            border-radius: 15px;
            border: 2px solid #00B0FF;
            box-shadow: 0 8px 25px rgba(0, 176, 255, 0.15);
        }}
        
        .header h1 {{
            color: #00B0FF;
            font-size: 2.5rem;
            font-weight: 700;
            margin-bottom: 8px;
        }}
        
        .header p {{
            color: #E0E0E0;
            font-size: 1.1rem;
        }}
        
        .tab-navigation {{
            display: flex;
            background: #2C2C2C;
            border-radius: 12px;
            margin-bottom: 25px;
            border: 1px solid #404040;
            overflow: hidden;
        }}
        
        .tab-button {{
            flex: 1;
            padding: 15px 30px;
            background: transparent;
            color: #E0E0E0;
            border: none;
            cursor: pointer;
            font-size: 1.1rem;
            font-weight: 600;
            font-family: 'Poppins', sans-serif;
            transition: all 0.3s ease;
        }}
        
        .tab-button.active {{
            background: linear-gradient(135deg, #00B0FF, #0288D1);
            color: #FFFFFF;
        }}
        
        .tab-button:hover:not(.active) {{
            background: #404040;
            color: #00B0FF;
        }}
        
        .tab-content {{
            display: none;
            animation: fadeIn 0.3s ease-in-out;
        }}
        
        .tab-content.active {{
            display: block;
        }}
        
        @keyframes fadeIn {{
            from {{ opacity: 0; }}
            to {{ opacity: 1; }}
        }}
        
        .kpi-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        
        .kpi-card {{
            background: linear-gradient(135deg, #2C2C2C, #3C3C3C);
            padding: 25px;
            border-radius: 15px;
            text-align: center;
            border: 1px solid #404040;
            transition: all 0.3s ease;
            position: relative;
        }}
        
        .kpi-card::before {{
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            width: 100%;
            height: 3px;
            background: linear-gradient(90deg, #00C853, #00B0FF, #D50000);
        }}
        
        .kpi-card:hover {{
            transform: translateY(-5px);
            box-shadow: 0 12px 25px rgba(0, 176, 255, 0.12);
        }}
        
        .kpi-value {{
            font-size: 2.2rem;
            font-weight: 700;
            margin-bottom: 8px;
            background: linear-gradient(45deg, #00B0FF, #00C853);
            background-clip: text;
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }}
        
        .kpi-value.positive {{ 
            background: linear-gradient(45deg, #00C853, #4CAF50);
            background-clip: text;
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }}
        
        .kpi-label {{
            font-size: 0.9rem;
            color: #B0B0B0;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            font-weight: 500;
        }}
        
        .charts-grid {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 25px;
            margin-bottom: 30px;
        }}
        
        .chart-card {{
            background: #2C2C2C;
            border-radius: 15px;
            padding: 25px;
            border: 1px solid #404040;
            transition: all 0.3s ease;
        }}
        
        .chart-card:hover {{
            transform: translateY(-3px);
            box-shadow: 0 15px 30px rgba(0, 0, 0, 0.25);
        }}
        
        .chart-title {{
            font-size: 1.3rem;
            font-weight: 600;
            color: #00B0FF;
            margin-bottom: 20px;
            text-align: center;
            border-bottom: 1px solid #404040;
            padding-bottom: 12px;
        }}
        
        .chart-container {{
            height: 350px;
            position: relative;
        }}
        
        .symbol-search {{
            background: #2C2C2C;
            padding: 20px;
            border-radius: 12px;
            margin-bottom: 25px;
            border: 1px solid #404040;
        }}
        
        .search-input {{
            width: 100%;
            padding: 12px 18px;
            background: #3C3C3C;
            border: 2px solid #404040;
            border-radius: 8px;
            color: #E0E0E0;
            font-size: 1rem;
            font-family: 'Poppins', sans-serif;
            transition: all 0.3s ease;
        }}
        
        .search-input:focus {{
            outline: none;
            border-color: #00B0FF;
            box-shadow: 0 0 10px rgba(0, 176, 255, 0.3);
        }}
        
        .suggestions {{
            background: #3C3C3C;
            border: 1px solid #404040;
            border-radius: 8px;
            margin-top: 8px;
            max-height: 150px;
            overflow-y: auto;
            display: none;
        }}
        
        .suggestion-item {{
            padding: 10px 15px;
            cursor: pointer;
            transition: background 0.3s ease;
        }}
        
        .suggestion-item:hover {{
            background: #4C4C4C;
            color: #00B0FF;
        }}
        
        .symbol-info {{
            background: #3C3C3C;
            padding: 18px;
            border-radius: 8px;
            margin-top: 12px;
            border: 1px solid #404040;
            display: none;
        }}
        
        .highlight {{
            background: linear-gradient(45deg, #00B0FF, #00C853);
            background-clip: text;
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            font-weight: 700;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>⚡ NSE Ultra-Fast Dashboard</h1>
            <p>Lightning Speed Market Intelligence • <span class="highlight">{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</span></p>
        </div>
        
        <div class="tab-navigation">
            <button class="tab-button active" onclick="showTab('market-overview', this)">
                📊 Market Overview
            </button>
            <button class="tab-button" onclick="showTab('symbol-analysis', this)">
                🔍 Symbol Analysis
            </button>
        </div>
        
        <!-- Market Overview Tab -->
        <div id="market-overview" class="tab-content active">
            <div class="kpi-grid">
                <div class="kpi-card">
                    <div class="kpi-value positive">{dashboard_data['kpis']['delivery_increase']:,.2f}</div>
                    <div class="kpi-label">Total Delivery Increase (Lakhs)</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-value">{dashboard_data['kpis']['positive_growth']:,}</div>
                    <div class="kpi-label">Positive Growth Stocks</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-value">{dashboard_data['kpis']['delivery_ratio']:.2f}%</div>
                    <div class="kpi-label">Delivery-to-Turnover Ratio</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-value">₹{dashboard_data['kpis']['avg_turnover']:,.0f}</div>
                    <div class="kpi-label">Average Turnover (Lacs)</div>
                </div>
            </div>
            
            <div class="charts-grid">
                <div class="chart-card">
                    <div class="chart-title">📊 Category Performance</div>
                    <div class="chart-container" id="category-chart"></div>
                </div>
                
                <div class="chart-card">
                    <div class="chart-title">🥧 Sector Distribution</div>
                    <div class="chart-container" id="sector-chart"></div>
                </div>
                
                <div class="chart-card">
                    <div class="chart-title">📈 Top Performers</div>
                    <div class="chart-container" id="performers-chart"></div>
                </div>
                
                <div class="chart-card">
                    <div class="chart-title">🎯 Delivery vs Turnover</div>
                    <div class="chart-container" id="scatter-chart"></div>
                </div>
            </div>
        </div>
        
        <!-- Symbol Analysis Tab -->
        <div id="symbol-analysis" class="tab-content">
            <div class="symbol-search">
                <input type="text" class="search-input" placeholder="🔍 Search symbol (e.g., RELIANCE, TCS...)" id="symbol-search-input">
                <div class="suggestions" id="suggestions"></div>
                <div class="symbol-info" id="symbol-info">
                    <h3 id="selected-symbol" style="color: #00B0FF; margin-bottom: 15px; text-align: center;">-</h3>
                    <div class="kpi-grid">
                        <div class="kpi-card">
                            <div class="kpi-value" id="symbol-delivery">-</div>
                            <div class="kpi-label">Delivery %</div>
                        </div>
                        <div class="kpi-card">
                            <div class="kpi-value" id="symbol-change">-</div>
                            <div class="kpi-label">Price Change %</div>
                        </div>
                        <div class="kpi-card">
                            <div class="kpi-value" id="symbol-volume">-</div>
                            <div class="kpi-label">Volume</div>
                        </div>
                        <div class="kpi-card">
                            <div class="kpi-value" id="symbol-price">-</div>
                            <div class="kpi-label">Price (₹)</div>
                        </div>
                    </div>
                </div>
            </div>
            
            <div class="charts-grid">
                <div class="chart-card">
                    <div class="chart-title">📈 Price Analysis</div>
                    <div class="chart-container" id="price-chart">
                        <div style="text-align: center; padding-top: 100px; color: #B0B0B0;">
                            Select a symbol to view price analysis
                        </div>
                    </div>
                </div>
                
                <div class="chart-card">
                    <div class="chart-title">📊 Performance Gauge</div>
                    <div class="chart-container" id="gauge-chart">
                        <div style="text-align: center; padding-top: 100px; color: #B0B0B0;">
                            Select a symbol to view performance gauge
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <script>
        // Pre-loaded data for instant rendering
        const data = {json.dumps(dashboard_data)};
        
        console.log('Ultra-fast dashboard loaded with', Object.keys(data.symbols).length, 'symbols');
        
        // Instant initialization
        document.addEventListener('DOMContentLoaded', function() {{
            createMarketCharts();
            setupSymbolSearch();
            console.log('Dashboard ready in under 1 second!');
        }});
        
        function showTab(tabId, button) {{
            document.querySelectorAll('.tab-content').forEach(tab => tab.classList.remove('active'));
            document.querySelectorAll('.tab-button').forEach(btn => btn.classList.remove('active'));
            
            document.getElementById(tabId).classList.add('active');
            button.classList.add('active');
        }}
        
        function createMarketCharts() {{
            createCategoryChart();
            createSectorChart();
            createPerformersChart();
            createScatterChart();
        }}
        
        function createCategoryChart() {{
            const categories = Object.keys(data.market_charts.categories);
            const values = Object.values(data.market_charts.categories);
            
            const plotlyData = [{{
                x: categories,
                y: values,
                type: 'bar',
                marker: {{ color: '#00B0FF' }}
            }}];
            
            const layout = {{
                paper_bgcolor: 'transparent',
                plot_bgcolor: 'transparent',
                font: {{ color: '#E0E0E0' }},
                margin: {{ t: 10, b: 40, l: 50, r: 10 }},
                xaxis: {{ gridcolor: '#404040' }},
                yaxis: {{ gridcolor: '#404040' }}
            }};
            
            Plotly.newPlot('category-chart', plotlyData, layout, {{ responsive: true, displayModeBar: false }});
        }}
        
        function createSectorChart() {{
            const sectors = Object.keys(data.market_charts.sectors);
            const values = Object.values(data.market_charts.sectors);
            
            const plotlyData = [{{
                values: values,
                labels: sectors,
                type: 'pie',
                marker: {{ colors: ['#00C853', '#00B0FF', '#D50000', '#FF9800', '#9C27B0', '#4CAF50'] }}
            }}];
            
            const layout = {{
                paper_bgcolor: 'transparent',
                plot_bgcolor: 'transparent',
                font: {{ color: '#E0E0E0' }},
                margin: {{ t: 10, b: 10, l: 10, r: 10 }}
            }};
            
            Plotly.newPlot('sector-chart', plotlyData, layout, {{ responsive: true, displayModeBar: false }});
        }}
        
        function createPerformersChart() {{
            const symbols = data.market_charts.top_symbols.names;
            const delivery = data.market_charts.top_symbols.delivery;
            
            const plotlyData = [{{
                x: symbols,
                y: delivery,
                type: 'bar',
                marker: {{ color: '#00C853' }}
            }}];
            
            const layout = {{
                paper_bgcolor: 'transparent',
                plot_bgcolor: 'transparent',
                font: {{ color: '#E0E0E0' }},
                margin: {{ t: 10, b: 60, l: 50, r: 10 }},
                xaxis: {{ gridcolor: '#404040' }},
                yaxis: {{ gridcolor: '#404040' }}
            }};
            
            Plotly.newPlot('performers-chart', plotlyData, layout, {{ responsive: true, displayModeBar: false }});
        }}
        
        function createScatterChart() {{
            const delivery = data.market_charts.top_symbols.delivery;
            const turnover = data.market_charts.top_symbols.turnover;
            const symbols = data.market_charts.top_symbols.names;
            
            const plotlyData = [{{
                x: delivery,
                y: turnover,
                mode: 'markers',
                type: 'scatter',
                text: symbols,
                marker: {{ 
                    size: 12, 
                    color: '#00B0FF',
                    line: {{ color: '#FFFFFF', width: 1 }}
                }}
            }}];
            
            const layout = {{
                paper_bgcolor: 'transparent',
                plot_bgcolor: 'transparent',
                font: {{ color: '#E0E0E0' }},
                margin: {{ t: 10, b: 40, l: 60, r: 10 }},
                xaxis: {{ title: 'Delivery %', gridcolor: '#404040' }},
                yaxis: {{ title: 'Turnover', gridcolor: '#404040' }}
            }};
            
            Plotly.newPlot('scatter-chart', plotlyData, layout, {{ responsive: true, displayModeBar: false }});
        }}
        
        function setupSymbolSearch() {{
            const searchInput = document.getElementById('symbol-search-input');
            const suggestions = document.getElementById('suggestions');
            const symbolInfo = document.getElementById('symbol-info');
            const symbols = Object.keys(data.symbols);
            
            searchInput.addEventListener('input', function() {{
                const term = this.value.toUpperCase();
                
                if (term.length >= 1) {{
                    const matches = symbols.filter(s => s.includes(term)).slice(0, 5);
                    
                    if (matches.length > 0) {{
                        suggestions.innerHTML = matches.map(s => 
                            `<div class="suggestion-item" onclick="selectSymbol('${{s}}')">${{s}}</div>`
                        ).join('');
                        suggestions.style.display = 'block';
                    }} else {{
                        suggestions.style.display = 'none';
                    }}
                }} else {{
                    suggestions.style.display = 'none';
                    symbolInfo.style.display = 'none';
                }}
            }});
        }}
        
        function selectSymbol(symbol) {{
            const searchInput = document.getElementById('symbol-search-input');
            const suggestions = document.getElementById('suggestions');
            const symbolInfo = document.getElementById('symbol-info');
            
            searchInput.value = symbol;
            suggestions.style.display = 'none';
            
            const symbolData = data.symbols[symbol];
            
            if (symbolData) {{
                symbolInfo.style.display = 'block';
                document.getElementById('selected-symbol').textContent = symbol;
                document.getElementById('symbol-delivery').textContent = symbolData.delivery.toFixed(2) + '%';
                document.getElementById('symbol-change').textContent = symbolData.price_change.toFixed(2) + '%';
                document.getElementById('symbol-volume').textContent = symbolData.volume.toLocaleString();
                document.getElementById('symbol-price').textContent = '₹' + symbolData.close.toFixed(2);
                
                createPriceChart(symbol, symbolData);
                createGaugeChart(symbol, symbolData);
            }}
        }}
        
        function createPriceChart(symbol, data) {{
            const plotlyData = [{{
                x: ['Open', 'High', 'Low', 'Close'],
                y: [data.open, data.high, data.low, data.close],
                type: 'bar',
                marker: {{ color: ['#00B0FF', '#00C853', '#D50000', '#FF9800'] }}
            }}];
            
            const layout = {{
                title: symbol + ' OHLC',
                paper_bgcolor: 'transparent',
                plot_bgcolor: 'transparent',
                font: {{ color: '#E0E0E0' }},
                margin: {{ t: 40, b: 40, l: 60, r: 10 }}
            }};
            
            Plotly.newPlot('price-chart', plotlyData, layout, {{ responsive: true, displayModeBar: false }});
        }}
        
        function createGaugeChart(symbol, data) {{
            const plotlyData = [{{
                domain: {{ x: [0, 1], y: [0, 1] }},
                value: data.delivery,
                title: {{ text: "Delivery %" }},
                type: "indicator",
                mode: "gauge+number",
                gauge: {{
                    axis: {{ range: [null, 100] }},
                    bar: {{ color: "#00C853" }},
                    steps: [
                        {{ range: [0, 50], color: "#D50000" }},
                        {{ range: [50, 100], color: "#00C853" }}
                    ]
                }}
            }}];
            
            const layout = {{
                paper_bgcolor: 'transparent',
                plot_bgcolor: 'transparent',
                font: {{ color: '#E0E0E0' }},
                margin: {{ t: 20, b: 20, l: 20, r: 20 }}
            }};
            
            Plotly.newPlot('gauge-chart', plotlyData, layout, {{ responsive: true, displayModeBar: false }});
        }}
        
    </script>
</body>
</html>
'''
    
    # Save the ultra-fast dashboard
    dashboard_path = 'ultra_fast_dashboard.html'
    with open(dashboard_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"Ultra-Fast Dashboard created: {dashboard_path}")
    print(f"\nSpeed Optimizations:")
    print(f"⚡ Pre-calculated KPIs: No loading delays")
    print(f"⚡ Reduced dataset: 1,000 records (vs 2,500)")
    print(f"⚡ Inline data: No async loading")
    print(f"⚡ Simplified charts: Instant rendering")
    print(f"⚡ Minimal JavaScript: Lightning fast")
    print(f"⚡ Pre-rendered HTML: Immediate display")
    
    return dashboard_path

if __name__ == "__main__":
    create_ultra_fast_dashboard()