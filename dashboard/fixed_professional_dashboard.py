import pyodbc
import pandas as pd
import json
from datetime import datetime
import numpy as np

def create_fixed_optimized_dashboard():
    """Create an optimized professional dashboard with fixed JavaScript syntax"""
    
    # Database configuration
    server = 'SRIKIRANREDDY\\SQLEXPRESS'
    database = 'master'
    
    # Connect to database
    conn = pyodbc.connect(f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes')
    
    # Optimized query - only fetch essential columns and limit data for performance
    query = '''
    SELECT TOP 3000
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
        AND current_turnover_lacs > 50  -- Filter for meaningful data
    ORDER BY current_turnover_lacs DESC
    '''
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    print(f"Loaded {len(df)} optimized records for Fixed Dashboard")
    
    # Handle any NaN values
    df = df.fillna(0)
    
    # Pre-calculate all analytics efficiently
    total_delivery_increase = float(df['delivery_increase_abs'].sum() / 100000)
    positive_delivery_growth = int(len(df[df['delivery_increase_pct'] > 0]))
    delivery_turnover_ratio = float((df['delivery_qty'].sum() / df['turnover'].sum()) * 100)
    avg_daily_turnover = float(df['turnover'].mean())
    
    # Optimized groupings with limited data
    category_analysis = df.groupby('category').agg({
        'turnover': 'sum',
        'delivery_percentage': 'mean',
        'delivery_increase_pct': 'mean',
        'symbol': 'count',
        'delivery_qty': 'sum'
    }).round(2).head(8)  # Limit to top 8 categories
    
    sector_analysis = df.groupby('sector').agg({
        'turnover': 'sum',
        'delivery_percentage': 'mean',
        'price_change_pct': 'mean',
        'symbol': 'count',
        'delivery_qty': 'sum'
    }).round(2).head(6)  # Limit to top 6 sectors
    
    # Top performers only for charts
    top_15_symbols = df.nlargest(15, 'turnover')
    top_30_symbols = df.nlargest(30, 'turnover')
    
    # Clean and convert data types
    def safe_convert(value):
        if pd.isna(value) or np.isinf(value):
            return 0.0
        return float(value)
    
    # Streamlined dashboard data with proper data types
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
            'top_symbols': top_30_symbols['symbol'].tolist(),
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
                } for symbol, row in top_30_symbols.set_index('symbol').iterrows()
            }
        },
        'category_performance': {
            'kpis': {
                'best_performing_category': str(category_analysis.loc[category_analysis['delivery_increase_pct'].idxmax()].name) if len(category_analysis) > 0 else 'N/A',
                'total_symbols': len(df)
            },
            'category_data': {
                'names': category_analysis.index.tolist(),
                'delivery_increases': [safe_convert(x) for x in category_analysis['delivery_increase_pct'].tolist()],
                'avg_delivery': [safe_convert(x) for x in category_analysis['delivery_percentage'].tolist()],
                'turnovers': [safe_convert(x) for x in category_analysis['turnover'].tolist()]
            }
        }
    }
    
    # Fixed HTML with corrected JavaScript syntax
    html_content = f'''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>NSE Professional Dashboard - Fixed</title>
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
            text-shadow: 0 0 15px rgba(0, 176, 255, 0.3);
        }}
        
        .header p {{
            color: #E0E0E0;
            font-size: 1.1rem;
            font-weight: 400;
        }}
        
        .tab-navigation {{
            display: flex;
            background: #2C2C2C;
            border-radius: 12px;
            margin-bottom: 25px;
            border: 1px solid #404040;
            overflow: hidden;
            box-shadow: 0 4px 12px rgba(0, 0, 0, 0.3);
        }}
        
        .tab-button {{
            flex: 1;
            padding: 15px 25px;
            background: transparent;
            color: #E0E0E0;
            border: none;
            cursor: pointer;
            font-size: 1rem;
            font-weight: 600;
            font-family: 'Poppins', sans-serif;
            transition: all 0.3s ease;
            position: relative;
        }}
        
        .tab-button.active {{
            background: linear-gradient(135deg, #00B0FF, #0288D1);
            color: #FFFFFF;
            box-shadow: inset 0 0 15px rgba(255, 255, 255, 0.1);
        }}
        
        .tab-button:hover:not(.active) {{
            background: #404040;
            color: #00B0FF;
        }}
        
        .tab-content {{
            display: none;
            animation: fadeIn 0.4s ease-in-out;
        }}
        
        .tab-content.active {{
            display: block;
        }}
        
        @keyframes fadeIn {{
            from {{ opacity: 0; transform: translateY(15px); }}
            to {{ opacity: 1; transform: translateY(0); }}
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
            overflow: hidden;
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
        
        .kpi-value.negative {{ 
            background: linear-gradient(45deg, #D50000, #F44336);
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
            position: relative;
            overflow: hidden;
        }}
        
        .chart-card::before {{
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            width: 100%;
            height: 2px;
            background: linear-gradient(90deg, #00B0FF, #00C853);
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
            height: 400px;
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
            max-height: 200px;
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
        
        .loading {{
            display: flex;
            align-items: center;
            justify-content: center;
            height: 100%;
            color: #B0B0B0;
            font-size: 1.1rem;
        }}
        
        .spinner {{
            width: 30px;
            height: 30px;
            border: 3px solid #404040;
            border-top: 3px solid #00B0FF;
            border-radius: 50%;
            animation: spin 1s linear infinite;
            margin-right: 12px;
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
        
        .full-width {{
            grid-column: 1 / -1;
        }}
        
        .error-message {{
            color: #D50000;
            background: rgba(213, 0, 0, 0.1);
            padding: 10px;
            border-radius: 8px;
            margin: 10px 0;
            border: 1px solid #D50000;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>⚡ NSE Professional Dashboard</h1>
            <p>Fixed & Optimized Market Intelligence • <span class="highlight">{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</span></p>
        </div>
        
        <div class="tab-navigation">
            <button class="tab-button active" onclick="showTab('market-overview', this)">
                📊 Market Overview
            </button>
            <button class="tab-button" onclick="showTab('symbol-analysis', this)">
                🔍 Symbol Analysis
            </button>
            <button class="tab-button" onclick="showTab('category-performance', this)">
                📈 Category Performance
            </button>
        </div>
        
        <!-- Market Overview Tab -->
        <div id="market-overview" class="tab-content active">
            <div class="kpi-grid">
                <div class="kpi-card">
                    <div class="kpi-value positive" id="delivery-increase-lakhs">-</div>
                    <div class="kpi-label">Total Delivery Increase (Lakhs)</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-value" id="positive-growth-count">-</div>
                    <div class="kpi-label">Positive Growth Stocks</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-value" id="delivery-turnover-ratio">-</div>
                    <div class="kpi-label">Delivery-to-Turnover Ratio (%)</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-value" id="avg-turnover">-</div>
                    <div class="kpi-label">Average Turnover (₹ Lacs)</div>
                </div>
            </div>
            
            <div class="charts-grid">
                <div class="chart-card">
                    <div class="chart-title">📊 Category Treemap</div>
                    <div class="chart-container" id="treemap-chart">
                        <div class="loading"><div class="spinner"></div>Loading...</div>
                    </div>
                </div>
                
                <div class="chart-card">
                    <div class="chart-title">🎯 Market Scatter</div>
                    <div class="chart-container" id="scatter-chart">
                        <div class="loading"><div class="spinner"></div>Loading...</div>
                    </div>
                </div>
                
                <div class="chart-card">
                    <div class="chart-title">🥧 Sector Distribution</div>
                    <div class="chart-container" id="pie-chart">
                        <div class="loading"><div class="spinner"></div>Loading...</div>
                    </div>
                </div>
                
                <div class="chart-card">
                    <div class="chart-title">📈 Top Performers</div>
                    <div class="chart-container" id="bar-chart">
                        <div class="loading"><div class="spinner"></div>Loading...</div>
                    </div>
                </div>
            </div>
        </div>
        
        <!-- Symbol Analysis Tab -->
        <div id="symbol-analysis" class="tab-content">
            <div class="symbol-search">
                <input type="text" class="search-input" placeholder="Start typing symbol name (e.g., RELIANCE, TCS...)" id="symbol-search-input">
                <div class="suggestions" id="suggestions"></div>
                <div class="symbol-info" id="symbol-info">
                    <h3 id="selected-symbol">-</h3>
                    <div class="kpi-grid">
                        <div class="kpi-card">
                            <div class="kpi-value" id="symbol-delivery">-</div>
                            <div class="kpi-label">Delivery %</div>
                        </div>
                        <div class="kpi-card">
                            <div class="kpi-value" id="symbol-change">-</div>
                            <div class="kpi-label">Change %</div>
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
                    <div class="chart-title">📈 Price Chart</div>
                    <div class="chart-container" id="price-chart">
                        <div class="loading">Select a symbol to view chart...</div>
                    </div>
                </div>
                
                <div class="chart-card">
                    <div class="chart-title">📊 Performance Metrics</div>
                    <div class="chart-container" id="metrics-chart">
                        <div class="loading">Select a symbol to view metrics...</div>
                    </div>
                </div>
            </div>
        </div>
        
        <!-- Category Performance Tab -->
        <div id="category-performance" class="tab-content">
            <div class="kpi-grid">
                <div class="kpi-card">
                    <div class="kpi-value positive" id="best-category">-</div>
                    <div class="kpi-label">Best Performing Category</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-value" id="total-symbols">-</div>
                    <div class="kpi-label">Total Symbols</div>
                </div>
            </div>
            
            <div class="charts-grid">
                <div class="chart-card full-width">
                    <div class="chart-title">📊 Category Performance</div>
                    <div class="chart-container" id="category-chart">
                        <div class="loading"><div class="spinner"></div>Loading...</div>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <script>
        // Fixed dashboard data with proper JavaScript syntax
        const dashboardData = {json.dumps(dashboard_data, indent=2)};
        
        console.log('Fixed dashboard loaded with', Object.keys(dashboardData.symbol_analysis.symbol_data).length, 'symbols');
        
        // Initialize on DOM ready
        document.addEventListener('DOMContentLoaded', function() {{
            try {{
                initializeMarketOverview();
                setupSymbolSearch();
                console.log('Dashboard initialized successfully');
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
                document.querySelectorAll('.tab-content').forEach(tab => tab.classList.remove('active'));
                document.querySelectorAll('.tab-button').forEach(btn => btn.classList.remove('active'));
                
                document.getElementById(tabId).classList.add('active');
                buttonElement.classList.add('active');
                
                setTimeout(() => {{
                    if (tabId === 'market-overview') initializeMarketOverview();
                    else if (tabId === 'symbol-analysis') initializeSymbolAnalysis();
                    else if (tabId === 'category-performance') initializeCategoryPerformance();
                }}, 50);
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
                    textfont: {{ color: '#FFFFFF', size: 11 }},
                    marker: {{
                        colorscale: [[0, '#D50000'], [0.5, '#00B0FF'], [1, '#00C853']],
                        showscale: false,
                        line: {{ color: '#1A1A1A', width: 1 }}
                    }},
                    hovertemplate: '<b>%{{label}}</b><br>Turnover: ₹%{{value:,.0f}}<br>Symbols: %{{customdata}}<extra></extra>',
                    customdata: data.symbol_counts
                }}];
                
                const layout = {{
                    font: {{ color: '#E0E0E0', family: 'Poppins' }},
                    paper_bgcolor: 'transparent',
                    plot_bgcolor: 'transparent',
                    margin: {{ t: 5, b: 5, l: 5, r: 5 }}
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
                    mode: 'markers',
                    type: 'scatter',
                    text: data.symbols,
                    marker: {{
                        size: 12,
                        color: data.delivery_increases,
                        colorscale: [[0, '#D50000'], [0.5, '#00B0FF'], [1, '#00C853']],
                        showscale: true,
                        colorbar: {{ title: 'Delivery %', titlefont: {{ color: '#E0E0E0' }}, tickfont: {{ color: '#E0E0E0' }} }},
                        line: {{ color: '#FFFFFF', width: 1 }}
                    }},
                    hovertemplate: '<b>%{{text}}</b><br>Delivery: %{{x:.1f}}%<br>Turnover: ₹%{{y:,.0f}}<extra></extra>'
                }}];
                
                const layout = {{
                    font: {{ color: '#E0E0E0', family: 'Poppins' }},
                    paper_bgcolor: 'transparent',
                    plot_bgcolor: 'transparent',
                    xaxis: {{ title: 'Delivery %', gridcolor: '#404040', titlefont: {{ color: '#E0E0E0' }}, tickfont: {{ color: '#E0E0E0' }} }},
                    yaxis: {{ title: 'Turnover', gridcolor: '#404040', titlefont: {{ color: '#E0E0E0' }}, tickfont: {{ color: '#E0E0E0' }} }},
                    margin: {{ t: 5, b: 40, l: 60, r: 5 }}
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
                    margin: {{ t: 5, b: 5, l: 5, r: 5 }},
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
                    xaxis: {{ title: 'Symbols', gridcolor: '#404040', titlefont: {{ color: '#E0E0E0' }}, tickfont: {{ color: '#E0E0E0' }} }},
                    yaxis: {{ title: 'Delivery %', gridcolor: '#404040', titlefont: {{ color: '#E0E0E0' }}, tickfont: {{ color: '#E0E0E0' }} }},
                    margin: {{ t: 5, b: 80, l: 60, r: 5 }}
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
                        const matches = topSymbols.filter(symbol => symbol.includes(searchTerm)).slice(0, 8);
                        
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
                    title: {{ text: symbol + ' Price Data', font: {{ color: '#E0E0E0' }} }},
                    font: {{ color: '#E0E0E0', family: 'Poppins' }},
                    paper_bgcolor: 'transparent',
                    plot_bgcolor: 'transparent',
                    xaxis: {{ gridcolor: '#404040', titlefont: {{ color: '#E0E0E0' }}, tickfont: {{ color: '#E0E0E0' }} }},
                    yaxis: {{ title: 'Price (₹)', gridcolor: '#404040', titlefont: {{ color: '#E0E0E0' }}, tickfont: {{ color: '#E0E0E0' }} }},
                    margin: {{ t: 40, b: 40, l: 60, r: 5 }}
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
                    values: [data.delivery_percentage, 100-data.delivery_percentage],
                    labels: ['Delivery %', 'Others'],
                    type: 'pie',
                    hole: 0.4,
                    textinfo: 'label+percent',
                    textfont: {{ color: '#FFFFFF' }},
                    marker: {{
                        colors: ['#00C853', '#404040'],
                        line: {{ color: '#1A1A1A', width: 2 }}
                    }},
                    hovertemplate: '<b>%{{label}}</b><br>%{{percent}}<extra></extra>'
                }}];
                
                const layout = {{
                    title: {{ text: symbol + ' Delivery Analysis', font: {{ color: '#E0E0E0' }} }},
                    font: {{ color: '#E0E0E0', family: 'Poppins' }},
                    paper_bgcolor: 'transparent',
                    plot_bgcolor: 'transparent',
                    margin: {{ t: 40, b: 5, l: 5, r: 5 }},
                    annotations: [{{
                        text: data.delivery_percentage.toFixed(1) + '%',
                        showarrow: false,
                        font: {{ size: 20, color: '#00C853', family: 'Poppins' }}
                    }}]
                }};
                
                Plotly.newPlot(container, plotlyData, layout, {{ responsive: true, displayModeBar: false }});
            }} catch (error) {{
                console.error('Metrics chart error:', error);
                document.getElementById('metrics-chart').innerHTML = '<div class="error-message">Error loading metrics chart</div>';
            }}
        }}
        
        function initializeCategoryPerformance() {{
            try {{
                updateCategoryKPIs();
                createCategoryChart();
            }} catch (error) {{
                console.error('Category performance initialization error:', error);
                showError('Error loading category performance: ' + error.message);
            }}
        }}
        
        function updateCategoryKPIs() {{
            const kpis = dashboardData.category_performance.kpis;
            document.getElementById('best-category').textContent = kpis.best_performing_category;
            document.getElementById('total-symbols').textContent = kpis.total_symbols.toLocaleString();
        }}
        
        function createCategoryChart() {{
            try {{
                const container = document.getElementById('category-chart');
                const data = dashboardData.category_performance.category_data;
                
                if (!data.names || data.names.length === 0) {{
                    container.innerHTML = '<div class="loading">No data available</div>';
                    return;
                }}
                
                const plotlyData = [{{
                    x: data.names,
                    y: data.delivery_increases,
                    type: 'bar',
                    marker: {{
                        color: data.delivery_increases,
                        colorscale: [[0, '#D50000'], [0.5, '#00B0FF'], [1, '#00C853']],
                        showscale: true,
                        colorbar: {{ title: 'Delivery Increase %', titlefont: {{ color: '#E0E0E0' }}, tickfont: {{ color: '#E0E0E0' }} }},
                        line: {{ color: '#FFFFFF', width: 1 }}
                    }},
                    hovertemplate: '<b>%{{x}}</b><br>Delivery Increase: %{{y:.1f}}%<br>Avg Delivery: %{{customdata:.1f}}%<extra></extra>',
                    customdata: data.avg_delivery
                }}];
                
                const layout = {{
                    font: {{ color: '#E0E0E0', family: 'Poppins' }},
                    paper_bgcolor: 'transparent',
                    plot_bgcolor: 'transparent',
                    xaxis: {{ title: 'Categories', gridcolor: '#404040', titlefont: {{ color: '#E0E0E0' }}, tickfont: {{ color: '#E0E0E0' }} }},
                    yaxis: {{ title: 'Delivery Increase %', gridcolor: '#404040', titlefont: {{ color: '#E0E0E0' }}, tickfont: {{ color: '#E0E0E0' }} }},
                    margin: {{ t: 20, b: 80, l: 60, r: 10 }}
                }};
                
                Plotly.newPlot(container, plotlyData, layout, {{ responsive: true, displayModeBar: false }});
            }} catch (error) {{
                console.error('Category chart error:', error);
                document.getElementById('category-chart').innerHTML = '<div class="error-message">Error loading category chart</div>';
            }}
        }}
        
    </script>
</body>
</html>
'''
    
    # Save the fixed dashboard
    dashboard_path = 'fixed_professional_dashboard.html'
    with open(dashboard_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"Fixed Professional Dashboard created: {dashboard_path}")
    print(f"\nError Fixes Applied:")
    print(f"✅ Fixed JavaScript array syntax: Array().fill() instead of Python *")
    print(f"✅ Added comprehensive error handling and try-catch blocks")
    print(f"✅ Fixed CSS background-clip property compatibility")
    print(f"✅ Added data validation and null checks")
    print(f"✅ Reduced dataset to 3000 records for faster loading")
    print(f"✅ Added error display functionality")
    print(f"✅ Console logging for debugging")
    
    return dashboard_path

if __name__ == "__main__":
    create_fixed_optimized_dashboard()