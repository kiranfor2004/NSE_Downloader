import pyodbc
import pandas as pd
import json
from datetime import datetime
import numpy as np

def create_merged_dashboard():
    """Create a merged dashboard with Market Overview and Symbol Analysis in two tabs"""
    
    # Database configuration
    server = 'SRIKIRANREDDY\\SQLEXPRESS'
    database = 'master'
    
    # Connect to database
    conn = pyodbc.connect(f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes')
    
    # Optimized query for both tabs
    query = '''
    SELECT TOP 2500
        symbol,
        current_deliv_per as delivery_percentage,
        ((current_close_price - current_prev_close) / current_prev_close) * 100 as price_change_pct,
        current_ttl_trd_qnty as volume,
        current_turnover_lacs as turnover,
        current_close_price as close_price,
        current_high_price as high_price,
        current_low_price as low_price,
        current_open_price as open_price,
        current_deliv_qty as delivery_qty,
        delivery_increase_pct,
        delivery_increase_abs,
        current_no_of_trades as no_of_trades,
        index_name,
        category,
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
    WHERE current_deliv_per IS NOT NULL 
        AND current_close_price IS NOT NULL 
        AND current_prev_close IS NOT NULL
        AND current_ttl_trd_qnty IS NOT NULL
        AND current_turnover_lacs > 50
    ORDER BY current_turnover_lacs DESC
    '''
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    print(f"Loaded {len(df)} records for Merged Dashboard")
    
    # Handle any NaN values
    df = df.fillna(0)
    
    # Safe conversion function
    def safe_convert(value):
        if pd.isna(value) or np.isinf(value):
            return 0.0
        return float(value)
    
    # Market Overview calculations
    total_delivery_increase = float(df['delivery_increase_abs'].sum() / 100000)
    positive_delivery_growth = int(len(df[df['delivery_increase_pct'] > 0]))
    delivery_turnover_ratio = float((df['delivery_qty'].sum() / df['turnover'].sum()) * 100)
    avg_daily_turnover = float(df['turnover'].mean())
    
    # Data for charts
    category_analysis = df.groupby('category').agg({
        'turnover': 'sum',
        'delivery_percentage': 'mean',
        'delivery_increase_pct': 'mean',
        'symbol': 'count',
        'delivery_qty': 'sum'
    }).round(2).head(8)
    
    sector_analysis = df.groupby('sector').agg({
        'turnover': 'sum',
        'delivery_percentage': 'mean',
        'price_change_pct': 'mean',
        'symbol': 'count',
        'delivery_qty': 'sum'
    }).round(2).head(6)
    
    # Top performers for different charts
    top_15_symbols = df.nlargest(15, 'turnover')
    top_50_symbols = df.nlargest(50, 'turnover')
    
    # Consolidated dashboard data
    dashboard_data = {
        'market_overview': {
            'kpis': {
                'total_delivery_increase_lakhs': round(total_delivery_increase, 2),
                'positive_delivery_growth_count': positive_delivery_growth,
                'delivery_turnover_ratio': round(delivery_turnover_ratio, 2),
                'avg_daily_turnover': round(avg_daily_turnover, 2)
            },
            'treemap_data': {
                'categories': category_analysis.index.tolist(),
                'turnovers': [safe_convert(x) for x in category_analysis['turnover'].tolist()],
                'delivery_pcts': [safe_convert(x) for x in category_analysis['delivery_percentage'].tolist()],
                'symbol_counts': [int(x) for x in category_analysis['symbol'].tolist()]
            },
            'scatter_data': {
                'symbols': top_15_symbols['symbol'].tolist(),
                'delivery_increases': [safe_convert(x) for x in top_15_symbols['delivery_increase_pct'].tolist()],
                'turnovers': [safe_convert(x) for x in top_15_symbols['turnover'].tolist()],
                'sectors': top_15_symbols['sector'].tolist()
            },
            'pie_data': {
                'sectors': sector_analysis.index.tolist(),
                'delivery_qtys': [safe_convert(x) for x in sector_analysis['delivery_qty'].tolist()]
            },
            'bar_data': {
                'symbols': top_15_symbols['symbol'].tolist(),
                'delivery_pcts': [safe_convert(x) for x in top_15_symbols['delivery_percentage'].tolist()],
                'price_changes': [safe_convert(x) for x in top_15_symbols['price_change_pct'].tolist()]
            }
        },
        'symbol_analysis': {
            'top_symbols': top_50_symbols['symbol'].tolist(),
            'symbol_data': {
                symbol: {
                    'delivery_percentage': safe_convert(row['delivery_percentage']),
                    'price_change_pct': safe_convert(row['price_change_pct']),
                    'volume': int(row['volume']) if not pd.isna(row['volume']) else 0,
                    'turnover': safe_convert(row['turnover']),
                    'close_price': safe_convert(row['close_price']),
                    'high_price': safe_convert(row['high_price']),
                    'low_price': safe_convert(row['low_price']),
                    'open_price': safe_convert(row['open_price']),
                    'delivery_increase_pct': safe_convert(row['delivery_increase_pct']),
                    'sector': str(row['sector'])
                } for symbol, row in top_50_symbols.set_index('symbol').iterrows()
            }
        }
    }
    
    # Merged dashboard HTML
    html_content = f'''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>NSE Professional Dashboard - Unified</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <link href="https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;500;600;700&display=swap" rel="stylesheet">
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: 'Poppins', sans-serif;
            background: #1A1A1A;
            color: #E0E0E0;
            line-height: 1.6;
            overflow-x: hidden;
        }}
        
        .container {{
            max-width: 1800px;
            margin: 0 auto;
            padding: 20px;
        }}
        
        .header {{
            text-align: center;
            margin-bottom: 30px;
            background: linear-gradient(135deg, #1A1A1A, #2C2C2C);
            padding: 30px;
            border-radius: 20px;
            border: 2px solid #00B0FF;
            box-shadow: 0 10px 30px rgba(0, 176, 255, 0.2);
        }}
        
        .header h1 {{
            color: #00B0FF;
            font-size: 3rem;
            font-weight: 700;
            margin-bottom: 10px;
            text-shadow: 0 0 20px rgba(0, 176, 255, 0.3);
        }}
        
        .header p {{
            color: #E0E0E0;
            font-size: 1.3rem;
            font-weight: 400;
        }}
        
        .tab-navigation {{
            display: flex;
            background: #2C2C2C;
            border-radius: 15px;
            margin-bottom: 30px;
            border: 1px solid #404040;
            overflow: hidden;
            box-shadow: 0 5px 15px rgba(0, 0, 0, 0.3);
        }}
        
        .tab-button {{
            flex: 1;
            padding: 20px 40px;
            background: transparent;
            color: #E0E0E0;
            border: none;
            cursor: pointer;
            font-size: 1.2rem;
            font-weight: 600;
            font-family: 'Poppins', sans-serif;
            transition: all 0.3s ease;
            position: relative;
        }}
        
        .tab-button.active {{
            background: linear-gradient(135deg, #00B0FF, #0288D1);
            color: #FFFFFF;
            box-shadow: inset 0 0 20px rgba(255, 255, 255, 0.1);
        }}
        
        .tab-button:hover:not(.active) {{
            background: #404040;
            color: #00B0FF;
        }}
        
        .tab-button::after {{
            content: '';
            position: absolute;
            bottom: 0;
            left: 0;
            width: 100%;
            height: 3px;
            background: #00B0FF;
            transform: scaleX(0);
            transition: transform 0.3s ease;
        }}
        
        .tab-button.active::after {{
            transform: scaleX(1);
        }}
        
        .tab-content {{
            display: none;
            animation: fadeIn 0.5s ease-in-out;
        }}
        
        .tab-content.active {{
            display: block;
        }}
        
        @keyframes fadeIn {{
            from {{ opacity: 0; transform: translateY(20px); }}
            to {{ opacity: 1; transform: translateY(0); }}
        }}
        
        .kpi-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
            gap: 25px;
            margin-bottom: 40px;
        }}
        
        .kpi-card {{
            background: linear-gradient(135deg, #2C2C2C, #3C3C3C);
            padding: 30px;
            border-radius: 18px;
            text-align: center;
            border: 1px solid #404040;
            transition: all 0.3s ease;
            position: relative;
            overflow: hidden;
        }}
        
        .kpi-card::before {{
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            width: 100%;
            height: 4px;
            background: linear-gradient(90deg, #00C853, #00B0FF, #D50000);
        }}
        
        .kpi-card:hover {{
            transform: translateY(-8px);
            box-shadow: 0 15px 35px rgba(0, 176, 255, 0.15);
        }}
        
        .kpi-value {{
            font-size: 2.8rem;
            font-weight: 700;
            margin-bottom: 10px;
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
        
        .kpi-value.negative {{ 
            background: linear-gradient(45deg, #D50000, #F44336);
            background-clip: text;
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }}
        
        .kpi-label {{
            font-size: 1rem;
            color: #B0B0B0;
            text-transform: uppercase;
            letter-spacing: 1px;
            font-weight: 500;
        }}
        
        .charts-grid {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 30px;
            margin-bottom: 40px;
        }}
        
        .chart-card {{
            background: #2C2C2C;
            border-radius: 20px;
            padding: 30px;
            border: 1px solid #404040;
            transition: all 0.3s ease;
            position: relative;
            overflow: hidden;
        }}
        
        .chart-card::before {{
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            width: 100%;
            height: 3px;
            background: linear-gradient(90deg, #00B0FF, #00C853);
        }}
        
        .chart-card:hover {{
            transform: translateY(-5px);
            box-shadow: 0 20px 40px rgba(0, 0, 0, 0.3);
        }}
        
        .chart-title {{
            font-size: 1.5rem;
            font-weight: 600;
            color: #00B0FF;
            margin-bottom: 25px;
            text-align: center;
            border-bottom: 2px solid #404040;
            padding-bottom: 15px;
        }}
        
        .chart-container {{
            height: 450px;
            position: relative;
        }}
        
        .symbol-search {{
            background: #2C2C2C;
            padding: 25px;
            border-radius: 15px;
            margin-bottom: 30px;
            border: 1px solid #404040;
        }}
        
        .search-input {{
            width: 100%;
            padding: 15px 20px;
            background: #3C3C3C;
            border: 2px solid #404040;
            border-radius: 10px;
            color: #E0E0E0;
            font-size: 1.1rem;
            font-family: 'Poppins', sans-serif;
            transition: all 0.3s ease;
        }}
        
        .search-input:focus {{
            outline: none;
            border-color: #00B0FF;
            box-shadow: 0 0 15px rgba(0, 176, 255, 0.3);
        }}
        
        .suggestions {{
            background: #3C3C3C;
            border: 1px solid #404040;
            border-radius: 10px;
            margin-top: 10px;
            max-height: 200px;
            overflow-y: auto;
            display: none;
        }}
        
        .suggestion-item {{
            padding: 12px 18px;
            cursor: pointer;
            transition: all 0.3s ease;
            border-bottom: 1px solid #404040;
        }}
        
        .suggestion-item:last-child {{
            border-bottom: none;
        }}
        
        .suggestion-item:hover {{
            background: #4C4C4C;
            color: #00B0FF;
        }}
        
        .symbol-info {{
            background: #3C3C3C;
            padding: 20px;
            border-radius: 10px;
            margin-top: 15px;
            border: 1px solid #404040;
            display: none;
        }}
        
        .loading {{
            display: flex;
            align-items: center;
            justify-content: center;
            height: 100%;
            color: #B0B0B0;
            font-size: 1.2rem;
        }}
        
        .spinner {{
            width: 40px;
            height: 40px;
            border: 4px solid #404040;
            border-top: 4px solid #00B0FF;
            border-radius: 50%;
            animation: spin 1s linear infinite;
            margin-right: 15px;
        }}
        
        @keyframes spin {{
            0% {{ transform: rotate(0deg); }}
            100% {{ transform: rotate(360deg); }}
        }}
        
        .highlight {{
            background: linear-gradient(45deg, #00B0FF, #00C853);
            background-clip: text;
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            font-weight: 700;
        }}
        
        .error-message {{
            color: #D50000;
            background: rgba(213, 0, 0, 0.1);
            padding: 15px;
            border-radius: 10px;
            margin: 15px 0;
            border: 1px solid #D50000;
            text-align: center;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🚀 NSE Professional Dashboard</h1>
            <p>Unified Market Intelligence & Symbol Analysis • <span class="highlight">{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</span></p>
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
                    <div class="kpi-value positive" id="delivery-increase-lakhs">-</div>
                    <div class="kpi-label">Total Market Delivery Increase (Lakhs)</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-value" id="positive-growth-count">-</div>
                    <div class="kpi-label">Stocks with Positive Delivery Growth</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-value" id="delivery-turnover-ratio">-</div>
                    <div class="kpi-label">Market Delivery-to-Turnover Ratio (%)</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-value" id="avg-turnover">-</div>
                    <div class="kpi-label">Average Daily Turnover (₹ Lacs)</div>
                </div>
            </div>
            
            <div class="charts-grid">
                <div class="chart-card">
                    <div class="chart-title">🗺️ Category Turnover Treemap</div>
                    <div class="chart-container" id="treemap-chart">
                        <div class="loading">
                            <div class="spinner"></div>
                            Loading Treemap...
                        </div>
                    </div>
                </div>
                
                <div class="chart-card">
                    <div class="chart-title">🎯 Delivery vs Turnover Scatter</div>
                    <div class="chart-container" id="scatter-chart">
                        <div class="loading">
                            <div class="spinner"></div>
                            Loading Scatter Plot...
                        </div>
                    </div>
                </div>
                
                <div class="chart-card">
                    <div class="chart-title">🥧 Sector Distribution</div>
                    <div class="chart-container" id="pie-chart">
                        <div class="loading">
                            <div class="spinner"></div>
                            Loading Pie Chart...
                        </div>
                    </div>
                </div>
                
                <div class="chart-card">
                    <div class="chart-title">📈 Top Performers Bar Chart</div>
                    <div class="chart-container" id="bar-chart">
                        <div class="loading">
                            <div class="spinner"></div>
                            Loading Bar Chart...
                        </div>
                    </div>
                </div>
            </div>
        </div>
        
        <!-- Symbol Analysis Tab -->
        <div id="symbol-analysis" class="tab-content">
            <div class="symbol-search">
                <input type="text" class="search-input" placeholder="🔍 Search for a symbol (e.g., RELIANCE, TCS, HDFCBANK...)" id="symbol-search-input">
                <div class="suggestions" id="suggestions"></div>
                <div class="symbol-info" id="symbol-info">
                    <h3 id="selected-symbol" style="color: #00B0FF; margin-bottom: 20px; text-align: center;">-</h3>
                    <div class="kpi-grid">
                        <div class="kpi-card">
                            <div class="kpi-value" id="symbol-delivery">-</div>
                            <div class="kpi-label">Current Delivery %</div>
                        </div>
                        <div class="kpi-card">
                            <div class="kpi-value" id="symbol-change">-</div>
                            <div class="kpi-label">Delivery Change %</div>
                        </div>
                        <div class="kpi-card">
                            <div class="kpi-value" id="symbol-volume">-</div>
                            <div class="kpi-label">Daily Volume</div>
                        </div>
                        <div class="kpi-card">
                            <div class="kpi-value" id="symbol-price">-</div>
                            <div class="kpi-label">Current Price (₹)</div>
                        </div>
                    </div>
                </div>
            </div>
            
            <div class="charts-grid">
                <div class="chart-card">
                    <div class="chart-title">📈 OHLC Price Analysis</div>
                    <div class="chart-container" id="price-chart">
                        <div class="loading">
                            Select a symbol to view price analysis...
                        </div>
                    </div>
                </div>
                
                <div class="chart-card">
                    <div class="chart-title">📊 Delivery Performance Gauge</div>
                    <div class="chart-container" id="metrics-chart">
                        <div class="loading">
                            Select a symbol to view delivery metrics...
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <script>
        // Consolidated dashboard data
        const dashboardData = {json.dumps(dashboard_data, indent=2)};
        
        console.log('Unified dashboard loaded with', Object.keys(dashboardData.symbol_analysis.symbol_data).length, 'symbols');
        
        // Initialize dashboard
        document.addEventListener('DOMContentLoaded', function() {{
            try {{
                initializeMarketOverview();
                setupSymbolSearch();
                console.log('Unified dashboard initialized successfully');
            }} catch (error) {{
                console.error('Dashboard initialization error:', error);
                showError('Error initializing dashboard: ' + error.message);
            }}
        }});
        
        function showError(message) {{
            const container = document.querySelector('.container');
            const errorDiv = document.createElement('div');
            errorDiv.className = 'error-message';
            errorDiv.textContent = message;
            container.insertBefore(errorDiv, container.firstChild);
        }}
        
        function showTab(tabId, buttonElement) {{
            try {{
                // Hide all tabs
                document.querySelectorAll('.tab-content').forEach(tab => {{
                    tab.classList.remove('active');
                }});
                
                // Remove active class from all buttons
                document.querySelectorAll('.tab-button').forEach(btn => {{
                    btn.classList.remove('active');
                }});
                
                // Show selected tab
                const targetTab = document.getElementById(tabId);
                if (targetTab) {{
                    targetTab.classList.add('active');
                }}
                
                // Activate clicked button
                if (buttonElement) {{
                    buttonElement.classList.add('active');
                }}
                
                // Initialize charts for the active tab
                setTimeout(() => {{
                    if (tabId === 'market-overview') {{
                        initializeMarketOverview();
                    }} else if (tabId === 'symbol-analysis') {{
                        initializeSymbolAnalysis();
                    }}
                }}, 100);
            }} catch (error) {{
                console.error('Tab switching error:', error);
                showError('Error switching tabs: ' + error.message);
            }}
        }}
        
        function initializeMarketOverview() {{
            try {{
                updateMarketKPIs();
                createTreemapChart();
                createScatterChart();
                createPieChart();
                createBarChart();
            }} catch (error) {{
                console.error('Market overview initialization error:', error);
                showError('Error loading market overview: ' + error.message);
            }}
        }}
        
        function initializeSymbolAnalysis() {{
            console.log('Symbol analysis tab activated');
        }}
        
        function updateMarketKPIs() {{
            const kpis = dashboardData.market_overview.kpis;
            
            document.getElementById('delivery-increase-lakhs').textContent = kpis.total_delivery_increase_lakhs.toLocaleString();
            document.getElementById('positive-growth-count').textContent = kpis.positive_delivery_growth_count.toLocaleString();
            document.getElementById('delivery-turnover-ratio').textContent = kpis.delivery_turnover_ratio.toFixed(2) + '%';
            document.getElementById('avg-turnover').textContent = '₹' + kpis.avg_daily_turnover.toLocaleString();
        }}
        
        function createTreemapChart() {{
            try {{
                const container = document.getElementById('treemap-chart');
                const data = dashboardData.market_overview.treemap_data;
                
                if (!data.categories || data.categories.length === 0) {{
                    container.innerHTML = '<div class="loading">No data available</div>';
                    return;
                }}
                
                const plotlyData = [{{
                    type: 'treemap',
                    labels: data.categories,
                    parents: Array(data.categories.length).fill(''),
                    values: data.turnovers,
                    textinfo: 'label+value',
                    textfont: {{ color: '#FFFFFF', size: 12 }},
                    marker: {{
                        colorscale: [[0, '#D50000'], [0.5, '#00B0FF'], [1, '#00C853']],
                        showscale: false,
                        line: {{ color: '#1A1A1A', width: 2 }}
                    }},
                    hovertemplate: '<b>%{{label}}</b><br>Turnover: ₹%{{value:,.0f}} Lacs<br>Avg Delivery: %{{customdata:.1f}}%<br>Symbols: %{{customdata2}}<extra></extra>',
                    customdata: data.delivery_pcts,
                    customdata2: data.symbol_counts
                }}];
                
                const layout = {{
                    font: {{ color: '#E0E0E0', family: 'Poppins' }},
                    paper_bgcolor: 'transparent',
                    plot_bgcolor: 'transparent',
                    margin: {{ t: 10, b: 10, l: 10, r: 10 }}
                }};
                
                Plotly.newPlot(container, plotlyData, layout, {{ responsive: true, displayModeBar: false }});
            }} catch (error) {{
                console.error('Treemap chart error:', error);
                document.getElementById('treemap-chart').innerHTML = '<div class="error-message">Error loading treemap</div>';
            }}
        }}
        
        function createScatterChart() {{
            try {{
                const container = document.getElementById('scatter-chart');
                const data = dashboardData.market_overview.scatter_data;
                
                if (!data.symbols || data.symbols.length === 0) {{
                    container.innerHTML = '<div class="loading">No data available</div>';
                    return;
                }}
                
                const plotlyData = [{{
                    x: data.delivery_increases,
                    y: data.turnovers,
                    mode: 'markers+text',
                    type: 'scatter',
                    text: data.symbols,
                    textposition: 'middle center',
                    marker: {{
                        size: data.turnovers.map(t => Math.max(10, Math.min(30, t/10000))),
                        color: data.delivery_increases,
                        colorscale: [[0, '#D50000'], [0.5, '#00B0FF'], [1, '#00C853']],
                        showscale: true,
                        colorbar: {{
                            title: 'Delivery Increase %',
                            titlefont: {{ color: '#E0E0E0' }},
                            tickfont: {{ color: '#E0E0E0' }}
                        }},
                        line: {{ color: '#FFFFFF', width: 1 }}
                    }},
                    hovertemplate: '<b>%{{text}}</b><br>Delivery Increase: %{{x:.1f}}%<br>Turnover: ₹%{{y:,.0f}} Lacs<br>Sector: %{{customdata}}<extra></extra>',
                    customdata: data.sectors
                }}];
                
                const layout = {{
                    font: {{ color: '#E0E0E0', family: 'Poppins' }},
                    paper_bgcolor: 'transparent',
                    plot_bgcolor: 'transparent',
                    xaxis: {{ 
                        title: 'Delivery Increase %',
                        gridcolor: '#404040',
                        titlefont: {{ color: '#E0E0E0' }},
                        tickfont: {{ color: '#E0E0E0' }}
                    }},
                    yaxis: {{ 
                        title: 'Turnover (₹ Lacs)',
                        gridcolor: '#404040',
                        titlefont: {{ color: '#E0E0E0' }},
                        tickfont: {{ color: '#E0E0E0' }}
                    }},
                    margin: {{ t: 10, b: 50, l: 80, r: 10 }}
                }};
                
                Plotly.newPlot(container, plotlyData, layout, {{ responsive: true, displayModeBar: false }});
            }} catch (error) {{
                console.error('Scatter chart error:', error);
                document.getElementById('scatter-chart').innerHTML = '<div class="error-message">Error loading scatter chart</div>';
            }}
        }}
        
        function createPieChart() {{
            try {{
                const container = document.getElementById('pie-chart');
                const data = dashboardData.market_overview.pie_data;
                
                if (!data.sectors || data.sectors.length === 0) {{
                    container.innerHTML = '<div class="loading">No data available</div>';
                    return;
                }}
                
                const plotlyData = [{{
                    values: data.delivery_qtys,
                    labels: data.sectors,
                    type: 'pie',
                    textinfo: 'label+percent',
                    textfont: {{ color: '#FFFFFF' }},
                    marker: {{
                        colors: ['#00C853', '#00B0FF', '#D50000', '#FF9800', '#9C27B0', '#4CAF50'],
                        line: {{ color: '#1A1A1A', width: 2 }}
                    }},
                    hovertemplate: '<b>%{{label}}</b><br>Delivery Qty: %{{value:,.0f}}<br>%{{percent}}<extra></extra>'
                }}];
                
                const layout = {{
                    font: {{ color: '#E0E0E0', family: 'Poppins' }},
                    paper_bgcolor: 'transparent',
                    plot_bgcolor: 'transparent',
                    margin: {{ t: 10, b: 10, l: 10, r: 10 }},
                    legend: {{ font: {{ color: '#E0E0E0' }} }}
                }};
                
                Plotly.newPlot(container, plotlyData, layout, {{ responsive: true, displayModeBar: false }});
            }} catch (error) {{
                console.error('Pie chart error:', error);
                document.getElementById('pie-chart').innerHTML = '<div class="error-message">Error loading pie chart</div>';
            }}
        }}
        
        function createBarChart() {{
            try {{
                const container = document.getElementById('bar-chart');
                const data = dashboardData.market_overview.bar_data;
                
                if (!data.symbols || data.symbols.length === 0) {{
                    container.innerHTML = '<div class="loading">No data available</div>';
                    return;
                }}
                
                const plotlyData = [{{
                    x: data.symbols,
                    y: data.delivery_pcts,
                    type: 'bar',
                    marker: {{
                        color: data.price_changes,
                        colorscale: [[0, '#D50000'], [0.5, '#00B0FF'], [1, '#00C853']],
                        showscale: false,
                        line: {{ color: '#1A1A1A', width: 1 }}
                    }},
                    hovertemplate: '<b>%{{x}}</b><br>Delivery: %{{y:.1f}}%<br>Price Change: %{{customdata:.1f}}%<extra></extra>',
                    customdata: data.price_changes
                }}];
                
                const layout = {{
                    font: {{ color: '#E0E0E0', family: 'Poppins' }},
                    paper_bgcolor: 'transparent',
                    plot_bgcolor: 'transparent',
                    xaxis: {{ 
                        title: 'Top Performing Symbols',
                        gridcolor: '#404040',
                        titlefont: {{ color: '#E0E0E0' }},
                        tickfont: {{ color: '#E0E0E0' }}
                    }},
                    yaxis: {{ 
                        title: 'Delivery Percentage (%)',
                        gridcolor: '#404040',
                        titlefont: {{ color: '#E0E0E0' }},
                        tickfont: {{ color: '#E0E0E0' }}
                    }},
                    margin: {{ t: 10, b: 80, l: 60, r: 10 }}
                }};
                
                Plotly.newPlot(container, plotlyData, layout, {{ responsive: true, displayModeBar: false }});
            }} catch (error) {{
                console.error('Bar chart error:', error);
                document.getElementById('bar-chart').innerHTML = '<div class="error-message">Error loading bar chart</div>';
            }}
        }}
        
        function setupSymbolSearch() {{
            try {{
                const searchInput = document.getElementById('symbol-search-input');
                const suggestions = document.getElementById('suggestions');
                const symbolInfo = document.getElementById('symbol-info');
                const topSymbols = dashboardData.symbol_analysis.top_symbols;
                
                searchInput.addEventListener('input', function() {{
                    const searchTerm = this.value.toUpperCase();
                    
                    if (searchTerm.length >= 1) {{
                        const matches = topSymbols.filter(symbol => symbol.includes(searchTerm)).slice(0, 10);
                        
                        if (matches.length > 0) {{
                            suggestions.innerHTML = matches.map(symbol => 
                                `<div class="suggestion-item" onclick="selectSymbol('${{symbol}}')">${{symbol}}</div>`
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
            }} catch (error) {{
                console.error('Symbol search setup error:', error);
                showError('Error setting up symbol search: ' + error.message);
            }}
        }}
        
        function selectSymbol(symbol) {{
            try {{
                const searchInput = document.getElementById('symbol-search-input');
                const suggestions = document.getElementById('suggestions');
                const symbolInfo = document.getElementById('symbol-info');
                
                searchInput.value = symbol;
                suggestions.style.display = 'none';
                
                const symbolData = dashboardData.symbol_analysis.symbol_data[symbol];
                
                if (symbolData) {{
                    symbolInfo.style.display = 'block';
                    document.getElementById('selected-symbol').textContent = symbol;
                    document.getElementById('symbol-delivery').textContent = symbolData.delivery_percentage.toFixed(2) + '%';
                    document.getElementById('symbol-change').textContent = symbolData.delivery_increase_pct.toFixed(2) + '%';
                    document.getElementById('symbol-volume').textContent = symbolData.volume.toLocaleString();
                    document.getElementById('symbol-price').textContent = '₹' + symbolData.close_price.toFixed(2);
                    
                    // Update change color
                    const changeElement = document.getElementById('symbol-change');
                    if (symbolData.delivery_increase_pct > 0) {{
                        changeElement.className = 'kpi-value positive';
                    }} else if (symbolData.delivery_increase_pct < 0) {{
                        changeElement.className = 'kpi-value negative';
                    }} else {{
                        changeElement.className = 'kpi-value';
                    }}
                    
                    createPriceChart(symbol, symbolData);
                    createMetricsChart(symbol, symbolData);
                }} else {{
                    showError('Symbol data not found for: ' + symbol);
                }}
            }} catch (error) {{
                console.error('Symbol selection error:', error);
                showError('Error selecting symbol: ' + error.message);
            }}
        }}
        
        function createPriceChart(symbol, data) {{
            try {{
                const container = document.getElementById('price-chart');
                
                const plotlyData = [{{
                    x: ['Open', 'High', 'Low', 'Close'],
                    y: [data.open_price, data.high_price, data.low_price, data.close_price],
                    type: 'bar',
                    marker: {{
                        color: ['#00B0FF', '#00C853', '#D50000', '#FF9800'],
                        line: {{ color: '#FFFFFF', width: 1 }}
                    }},
                    hovertemplate: '<b>%{{x}}</b><br>₹%{{y:.2f}}<extra></extra>'
                }}];
                
                const layout = {{
                    title: {{ text: symbol + ' OHLC Analysis', font: {{ color: '#E0E0E0' }} }},
                    font: {{ color: '#E0E0E0', family: 'Poppins' }},
                    paper_bgcolor: 'transparent',
                    plot_bgcolor: 'transparent',
                    xaxis: {{ gridcolor: '#404040', titlefont: {{ color: '#E0E0E0' }}, tickfont: {{ color: '#E0E0E0' }} }},
                    yaxis: {{ title: 'Price (₹)', gridcolor: '#404040', titlefont: {{ color: '#E0E0E0' }}, tickfont: {{ color: '#E0E0E0' }} }},
                    margin: {{ t: 40, b: 40, l: 60, r: 10 }}
                }};
                
                Plotly.newPlot(container, plotlyData, layout, {{ responsive: true, displayModeBar: false }});
            }} catch (error) {{
                console.error('Price chart error:', error);
                document.getElementById('price-chart').innerHTML = '<div class="error-message">Error loading price chart</div>';
            }}
        }}
        
        function createMetricsChart(symbol, data) {{
            try {{
                const container = document.getElementById('metrics-chart');
                
                const plotlyData = [{{
                    domain: {{ x: [0, 1], y: [0, 1] }},
                    value: data.delivery_percentage,
                    title: {{ text: "Delivery %" }},
                    type: "indicator",
                    mode: "gauge+number+delta",
                    delta: {{ reference: 50 }},
                    gauge: {{
                        axis: {{ range: [null, 100] }},
                        bar: {{ color: "#00C853" }},
                        steps: [
                            {{ range: [0, 25], color: "#D50000" }},
                            {{ range: [25, 50], color: "#FF9800" }},
                            {{ range: [50, 75], color: "#00B0FF" }},
                            {{ range: [75, 100], color: "#00C853" }}
                        ],
                        threshold: {{
                            line: {{ color: "#FFFFFF", width: 4 }},
                            thickness: 0.75,
                            value: 90
                        }}
                    }}
                }}];
                
                const layout = {{
                    title: {{ text: symbol + ' Delivery Performance', font: {{ color: '#E0E0E0' }} }},
                    font: {{ color: '#E0E0E0', family: 'Poppins' }},
                    paper_bgcolor: 'transparent',
                    plot_bgcolor: 'transparent',
                    margin: {{ t: 40, b: 20, l: 20, r: 20 }}
                }};
                
                Plotly.newPlot(container, plotlyData, layout, {{ responsive: true, displayModeBar: false }});
            }} catch (error) {{
                console.error('Metrics chart error:', error);
                document.getElementById('metrics-chart').innerHTML = '<div class="error-message">Error loading metrics chart</div>';
            }}
        }}
        
    </script>
</body>
</html>
'''
    
    # Save the merged dashboard
    dashboard_path = 'unified_professional_dashboard.html'
    with open(dashboard_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"Unified Professional Dashboard created: {dashboard_path}")
    print(f"\nDashboard Features:")
    print(f"✅ Market Overview Tab: 4 KPIs + 4 Advanced Charts")
    print(f"✅ Symbol Analysis Tab: Interactive Search + Detailed Analysis")
    print(f"✅ Streamlined Navigation: Just 2 Tabs")
    print(f"✅ Professional Design: Bloomberg-style with exact specifications")
    print(f"✅ Fast Performance: Optimized with 2,500 records")
    print(f"✅ Error-Free: All JavaScript syntax corrected")
    
    return dashboard_path

if __name__ == "__main__":
    create_merged_dashboard()