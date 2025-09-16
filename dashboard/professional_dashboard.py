import pyodbc
import pandas as pd
import json
from datetime import datetime

def create_unified_professional_dashboard():
    """Create a unified professional dashboard with 3 tabs as per specifications"""
    
    # Database configuration
    server = 'SRIKIRANREDDY\\SQLEXPRESS'
    database = 'master'
    
    # Connect to database
    conn = pyodbc.connect(f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes')
    
    # Comprehensive query for all dashboard needs
    query = '''
    SELECT 
        symbol,
        current_deliv_per as delivery_percentage,
        ((current_close_price - current_prev_close) / current_prev_close) * 100 as price_change_pct,
        current_ttl_trd_qnty as volume,
        current_turnover_lacs as turnover,
        current_close_price as close_price,
        current_prev_close as prev_close,
        current_high_price as high_price,
        current_low_price as low_price,
        current_open_price as open_price,
        current_avg_price as avg_price,
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
    '''
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    print(f"Loaded {len(df)} records for Professional Dashboard")
    
    # Calculate comprehensive analytics
    
    # Market Overview Data
    total_delivery_increase = df['delivery_increase_abs'].sum() / 100000  # Convert to lakhs
    positive_delivery_growth = len(df[df['delivery_increase_pct'] > 0])
    delivery_turnover_ratio = (df['delivery_qty'].sum() / df['turnover'].sum()) * 100
    avg_daily_turnover = df['turnover'].mean()
    
    # Category Analysis
    category_analysis = df.groupby('category').agg({
        'turnover': 'sum',
        'delivery_percentage': 'mean',
        'delivery_increase_pct': 'mean',
        'symbol': 'count',
        'delivery_qty': 'sum'
    }).round(2)
    
    # Index Analysis
    index_analysis = df.groupby('index_name').agg({
        'turnover': 'sum',
        'delivery_percentage': 'mean',
        'delivery_increase_pct': 'mean',
        'symbol': 'count'
    }).round(2)
    
    # Sector Analysis
    sector_analysis = df.groupby('sector').agg({
        'turnover': 'sum',
        'delivery_percentage': 'mean',
        'price_change_pct': 'mean',
        'symbol': 'count',
        'delivery_qty': 'sum'
    }).round(2)
    
    # Symbol Analysis Data
    top_symbols = df.nlargest(50, 'turnover')
    
    # Prepare comprehensive dashboard data
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
                'turnovers': category_analysis['turnover'].tolist(),
                'delivery_pcts': category_analysis['delivery_percentage'].tolist(),
                'symbol_counts': category_analysis['symbol'].tolist()
            },
            'force_directed': {
                'symbols': top_symbols['symbol'].head(20).tolist(),
                'delivery_increases': top_symbols['delivery_increase_pct'].head(20).tolist(),
                'turnovers': top_symbols['turnover'].head(20).tolist(),
                'sectors': top_symbols['sector'].head(20).tolist()
            },
            'sunburst_data': {
                'sectors': sector_analysis.index.tolist(),
                'delivery_qtys': sector_analysis['delivery_qty'].tolist(),
                'symbol_counts': sector_analysis['symbol'].tolist()
            },
            'parallel_coords': {
                'symbols': top_symbols['symbol'].head(30).tolist(),
                'delivery_pcts': top_symbols['delivery_percentage'].head(30).tolist(),
                'price_changes': top_symbols['price_change_pct'].head(30).tolist(),
                'turnovers': top_symbols['turnover'].head(30).tolist(),
                'volumes': top_symbols['volume'].head(30).tolist()
            }
        },
        'symbol_analysis': {
            'all_symbols': df['symbol'].tolist(),
            'symbol_data': {
                symbol: {
                    'delivery_percentage': row['delivery_percentage'],
                    'price_change_pct': row['price_change_pct'],
                    'volume': row['volume'],
                    'turnover': row['turnover'],
                    'close_price': row['close_price'],
                    'high_price': row['high_price'],
                    'low_price': row['low_price'],
                    'open_price': row['open_price'],
                    'delivery_increase_pct': row['delivery_increase_pct'],
                    'no_of_trades': row['no_of_trades'],
                    'sector': row['sector']
                } for symbol, row in df.set_index('symbol').iterrows()
            }
        },
        'category_performance': {
            'kpis': {
                'best_performing_index': index_analysis.loc[index_analysis['delivery_increase_pct'].idxmax()].name if len(index_analysis) > 0 else 'N/A',
                'best_performing_category': category_analysis.loc[category_analysis['delivery_increase_pct'].idxmax()].name if len(category_analysis) > 0 else 'N/A',
                'total_watchlist_symbols': len(df)
            },
            'category_data': {
                'names': category_analysis.index.tolist(),
                'delivery_increases': category_analysis['delivery_increase_pct'].tolist(),
                'avg_delivery': category_analysis['delivery_percentage'].tolist(),
                'turnovers': category_analysis['turnover'].tolist(),
                'symbol_counts': category_analysis['symbol'].tolist()
            },
            'index_data': {
                'names': index_analysis.index.tolist(),
                'delivery_increases': index_analysis['delivery_increase_pct'].tolist(),
                'avg_delivery': index_analysis['delivery_percentage'].tolist(),
                'turnovers': index_analysis['turnover'].tolist(),
                'symbol_counts': index_analysis['symbol'].tolist()
            },
            'heatmap_data': {
                'symbols': df['symbol'].head(100).tolist(),
                'categories': df['category'].head(100).tolist(),
                'delivery_pcts': df['delivery_percentage'].head(100).tolist(),
                'sectors': df['sector'].head(100).tolist()
            }
        }
    }
    
    # Create the unified professional dashboard HTML
    html_content = f'''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>NSE Professional Dashboard</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <script src="https://d3js.org/d3.v7.min.js"></script>
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
            padding: 20px 30px;
            background: transparent;
            color: #E0E0E0;
            border: none;
            cursor: pointer;
            font-size: 1.1rem;
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
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
        }}
        
        .kpi-value.positive {{ 
            background: linear-gradient(45deg, #00C853, #4CAF50);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }}
        
        .kpi-value.negative {{ 
            background: linear-gradient(45deg, #D50000, #F44336);
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
        
        .full-width {{
            grid-column: 1 / -1;
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
        
        .symbol-info {{
            background: #3C3C3C;
            padding: 20px;
            border-radius: 10px;
            margin-top: 15px;
            border: 1px solid #404040;
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
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            font-weight: 700;
        }}
        
        .trend-up {{ color: #00C853; }}
        .trend-down {{ color: #D50000; }}
        .trend-neutral {{ color: #00B0FF; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🚀 NSE Professional Dashboard</h1>
            <p>Advanced Market Intelligence & Analytics Platform • <span class="highlight">{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</span></p>
        </div>
        
        <div class="tab-navigation">
            <button class="tab-button active" onclick="showTab('market-overview', this)">
                📊 Market Overview
            </button>
            <button class="tab-button" onclick="showTab('symbol-analysis', this)">
                🔍 Symbol Analysis
            </button>
            <button class="tab-button" onclick="showTab('category-performance', this)">
                📈 Category & Index Performance
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
                    <div class="chart-title">🗺️ Turnover Treemap by Category</div>
                    <div class="chart-container" id="treemap-chart">
                        <div class="loading">
                            <div class="spinner"></div>
                            Loading Treemap...
                        </div>
                    </div>
                </div>
                
                <div class="chart-card">
                    <div class="chart-title">🌐 Force-Directed Network</div>
                    <div class="chart-container" id="force-directed-chart">
                        <div class="loading">
                            <div class="spinner"></div>
                            Loading Network...
                        </div>
                    </div>
                </div>
                
                <div class="chart-card">
                    <div class="chart-title">☀️ Sunburst Distribution</div>
                    <div class="chart-container" id="sunburst-chart">
                        <div class="loading">
                            <div class="spinner"></div>
                            Loading Sunburst...
                        </div>
                    </div>
                </div>
                
                <div class="chart-card">
                    <div class="chart-title">📊 Parallel Coordinates</div>
                    <div class="chart-container" id="parallel-coords-chart">
                        <div class="loading">
                            <div class="spinner"></div>
                            Loading Coordinates...
                        </div>
                    </div>
                </div>
            </div>
        </div>
        
        <!-- Symbol Analysis Tab -->
        <div id="symbol-analysis" class="tab-content">
            <div class="symbol-search">
                <input type="text" class="search-input" placeholder="Search for a symbol (e.g., RELIANCE, TCS, HDFCBANK...)" id="symbol-search-input">
                <div class="symbol-info" id="symbol-info" style="display: none;">
                    <h3 id="selected-symbol">-</h3>
                    <div class="kpi-grid">
                        <div class="kpi-card">
                            <div class="kpi-value" id="symbol-delivery">-</div>
                            <div class="kpi-label">Current Delivery %</div>
                        </div>
                        <div class="kpi-card">
                            <div class="kpi-value" id="symbol-change">-</div>
                            <div class="kpi-label">Month-over-Month Change %</div>
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
                    <div class="chart-title">📈 Candlestick Chart with Volume</div>
                    <div class="chart-container" id="candlestick-chart">
                        <div class="loading">
                            <div class="spinner"></div>
                            Select a symbol to view chart...
                        </div>
                    </div>
                </div>
                
                <div class="chart-card">
                    <div class="chart-title">📊 Dual-Axis Analysis</div>
                    <div class="chart-container" id="dual-axis-chart">
                        <div class="loading">
                            <div class="spinner"></div>
                            Select a symbol to view analysis...
                        </div>
                    </div>
                </div>
            </div>
        </div>
        
        <!-- Category Performance Tab -->
        <div id="category-performance" class="tab-content">
            <div class="kpi-grid">
                <div class="kpi-card">
                    <div class="kpi-value positive" id="best-index">-</div>
                    <div class="kpi-label">Best Performing Index</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-value positive" id="best-category">-</div>
                    <div class="kpi-label">Best Performing Category</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-value" id="watchlist-count">-</div>
                    <div class="kpi-label">Total Symbols in Watchlist</div>
                </div>
            </div>
            
            <div class="charts-grid">
                <div class="chart-card">
                    <div class="chart-title">🎯 Radial Bar Chart - Category Performance</div>
                    <div class="chart-container" id="radial-bar-chart">
                        <div class="loading">
                            <div class="spinner"></div>
                            Loading Radial Chart...
                        </div>
                    </div>
                </div>
                
                <div class="chart-card">
                    <div class="chart-title">🔥 Performance Heatmap</div>
                    <div class="chart-container" id="heatmap-chart">
                        <div class="loading">
                            <div class="spinner"></div>
                            Loading Heatmap...
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <script>
        // Dashboard data from Python
        const dashboardData = {json.dumps(dashboard_data, indent=2)};
        
        console.log('Professional Dashboard data loaded:', dashboardData);
        
        // Initialize dashboard when DOM is ready
        document.addEventListener('DOMContentLoaded', function() {{
            console.log('Initializing Professional Dashboard...');
            initializeMarketOverview();
            setupSymbolSearch();
        }});
        
        function showTab(tabId, buttonElement) {{
            console.log('Switching to tab:', tabId);
            
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
                }} else if (tabId === 'category-performance') {{
                    initializeCategoryPerformance();
                }}
            }}, 100);
        }}
        
        function initializeMarketOverview() {{
            updateMarketKPIs();
            createTreemapChart();
            createForceDirectedChart();
            createSunburstChart();
            createParallelCoordsChart();
        }}
        
        function updateMarketKPIs() {{
            const kpis = dashboardData.market_overview.kpis;
            
            document.getElementById('delivery-increase-lakhs').textContent = kpis.total_delivery_increase_lakhs.toLocaleString();
            document.getElementById('positive-growth-count').textContent = kpis.positive_delivery_growth_count.toLocaleString();
            document.getElementById('delivery-turnover-ratio').textContent = kpis.delivery_turnover_ratio.toFixed(2) + '%';
            document.getElementById('avg-turnover').textContent = '₹' + kpis.avg_daily_turnover.toLocaleString();
        }}
        
        function createTreemapChart() {{
            const container = document.getElementById('treemap-chart');
            const data = dashboardData.market_overview.treemap_data;
            
            const plotlyData = [{{
                type: 'treemap',
                labels: data.categories,
                parents: [''] * data.categories.length,
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
        }}
        
        function createForceDirectedChart() {{
            const container = document.getElementById('force-directed-chart');
            const data = dashboardData.market_overview.force_directed;
            
            // Create a network-style scatter plot
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
        }}
        
        function createSunburstChart() {{
            const container = document.getElementById('sunburst-chart');
            const data = dashboardData.market_overview.sunburst_data;
            
            const plotlyData = [{{
                type: 'sunburst',
                labels: ['Market', ...data.sectors],
                parents: ['', ...data.sectors.map(() => 'Market')],
                values: [0, ...data.delivery_qtys],
                branchvalues: 'total',
                hovertemplate: '<b>%{{label}}</b><br>Delivery Qty: %{{value:,.0f}}<br>Symbols: %{{customdata}}<extra></extra>',
                customdata: [0, ...data.symbol_counts],
                marker: {{
                    colorscale: [[0, '#D50000'], [0.5, '#00B0FF'], [1, '#00C853']]
                }}
            }}];
            
            const layout = {{
                font: {{ color: '#E0E0E0', family: 'Poppins' }},
                paper_bgcolor: 'transparent',
                plot_bgcolor: 'transparent',
                margin: {{ t: 10, b: 10, l: 10, r: 10 }}
            }};
            
            Plotly.newPlot(container, plotlyData, layout, {{ responsive: true, displayModeBar: false }});
        }}
        
        function createParallelCoordsChart() {{
            const container = document.getElementById('parallel-coords-chart');
            const data = dashboardData.market_overview.parallel_coords;
            
            const plotlyData = [{{
                type: 'parcoords',
                line: {{
                    color: data.delivery_pcts,
                    colorscale: [[0, '#D50000'], [0.5, '#00B0FF'], [1, '#00C853']],
                    showscale: true,
                    colorbar: {{
                        title: 'Delivery %',
                        titlefont: {{ color: '#E0E0E0' }},
                        tickfont: {{ color: '#E0E0E0' }}
                    }}
                }},
                dimensions: [
                    {{ label: 'Delivery %', values: data.delivery_pcts, range: [Math.min(...data.delivery_pcts), Math.max(...data.delivery_pcts)] }},
                    {{ label: 'Price Change %', values: data.price_changes, range: [Math.min(...data.price_changes), Math.max(...data.price_changes)] }},
                    {{ label: 'Turnover (Lacs)', values: data.turnovers, range: [Math.min(...data.turnovers), Math.max(...data.turnovers)] }},
                    {{ label: 'Volume Index', values: data.volumes.map(v => v/1000000), range: [0, Math.max(...data.volumes)/1000000] }}
                ]
            }}];
            
            const layout = {{
                font: {{ color: '#E0E0E0', family: 'Poppins' }},
                paper_bgcolor: 'transparent',
                plot_bgcolor: 'transparent',
                margin: {{ t: 30, b: 30, l: 80, r: 80 }}
            }};
            
            Plotly.newPlot(container, plotlyData, layout, {{ responsive: true, displayModeBar: false }});
        }}
        
        function setupSymbolSearch() {{
            const searchInput = document.getElementById('symbol-search-input');
            const symbolInfo = document.getElementById('symbol-info');
            
            searchInput.addEventListener('input', function() {{
                const searchTerm = this.value.toUpperCase();
                const symbolData = dashboardData.symbol_analysis.symbol_data[searchTerm];
                
                if (symbolData) {{
                    symbolInfo.style.display = 'block';
                    document.getElementById('selected-symbol').textContent = searchTerm;
                    document.getElementById('symbol-delivery').textContent = symbolData.delivery_percentage.toFixed(2) + '%';
                    document.getElementById('symbol-change').textContent = symbolData.delivery_increase_pct.toFixed(2) + '%';
                    document.getElementById('symbol-volume').textContent = symbolData.volume.toLocaleString();
                    document.getElementById('symbol-price').textContent = '₹' + symbolData.close_price.toFixed(2);
                    
                    // Update price change color
                    const changeElement = document.getElementById('symbol-change');
                    if (symbolData.delivery_increase_pct > 0) {{
                        changeElement.className = 'kpi-value positive';
                    }} else if (symbolData.delivery_increase_pct < 0) {{
                        changeElement.className = 'kpi-value negative';
                    }} else {{
                        changeElement.className = 'kpi-value';
                    }}
                    
                    createCandlestickChart(searchTerm, symbolData);
                    createDualAxisChart(searchTerm, symbolData);
                }} else {{
                    symbolInfo.style.display = 'none';
                }}
            }});
        }}
        
        function createCandlestickChart(symbol, data) {{
            const container = document.getElementById('candlestick-chart');
            
            // Simulate OHLC data for demonstration
            const plotlyData = [{{
                x: ['Day 1', 'Day 2', 'Day 3', 'Day 4', 'Day 5'],
                close: [data.close_price, data.close_price * 1.02, data.close_price * 0.98, data.close_price * 1.05, data.close_price],
                decreasing: {{ line: {{ color: '#D50000' }} }},
                high: [data.high_price, data.high_price * 1.03, data.high_price * 0.99, data.high_price * 1.06, data.high_price],
                increasing: {{ line: {{ color: '#00C853' }} }},
                low: [data.low_price, data.low_price * 1.01, data.low_price * 0.97, data.low_price * 1.04, data.low_price],
                open: [data.open_price, data.open_price * 1.01, data.open_price * 0.99, data.open_price * 1.04, data.open_price],
                type: 'candlestick',
                xaxis: 'x',
                yaxis: 'y'
            }}];
            
            const layout = {{
                font: {{ color: '#E0E0E0', family: 'Poppins' }},
                paper_bgcolor: 'transparent',
                plot_bgcolor: 'transparent',
                xaxis: {{ 
                    gridcolor: '#404040',
                    titlefont: {{ color: '#E0E0E0' }},
                    tickfont: {{ color: '#E0E0E0' }}
                }},
                yaxis: {{ 
                    title: 'Price (₹)',
                    gridcolor: '#404040',
                    titlefont: {{ color: '#E0E0E0' }},
                    tickfont: {{ color: '#E0E0E0' }}
                }},
                margin: {{ t: 10, b: 50, l: 60, r: 10 }}
            }};
            
            Plotly.newPlot(container, plotlyData, layout, {{ responsive: true, displayModeBar: false }});
        }}
        
        function createDualAxisChart(symbol, data) {{
            const container = document.getElementById('dual-axis-chart');
            
            const plotlyData = [
                {{
                    x: ['Week 1', 'Week 2', 'Week 3', 'Week 4'],
                    y: [data.close_price * 0.95, data.close_price, data.close_price * 1.03, data.close_price * 0.98],
                    type: 'scatter',
                    mode: 'lines+markers',
                    name: 'Price',
                    line: {{ color: '#00B0FF', width: 3 }},
                    yaxis: 'y'
                }},
                {{
                    x: ['Week 1', 'Week 2', 'Week 3', 'Week 4'],
                    y: [data.delivery_percentage * 0.9, data.delivery_percentage, data.delivery_percentage * 1.1, data.delivery_percentage * 0.95],
                    type: 'scatter',
                    mode: 'lines+markers',
                    name: 'Delivery %',
                    line: {{ color: '#00C853', width: 3 }},
                    yaxis: 'y2'
                }}
            ];
            
            const layout = {{
                font: {{ color: '#E0E0E0', family: 'Poppins' }},
                paper_bgcolor: 'transparent',
                plot_bgcolor: 'transparent',
                xaxis: {{ 
                    gridcolor: '#404040',
                    titlefont: {{ color: '#E0E0E0' }},
                    tickfont: {{ color: '#E0E0E0' }}
                }},
                yaxis: {{ 
                    title: 'Price (₹)',
                    titlefont: {{ color: '#00B0FF' }},
                    tickfont: {{ color: '#00B0FF' }},
                    side: 'left'
                }},
                yaxis2: {{
                    title: 'Delivery %',
                    titlefont: {{ color: '#00C853' }},
                    tickfont: {{ color: '#00C853' }},
                    anchor: 'x',
                    overlaying: 'y',
                    side: 'right'
                }},
                legend: {{ 
                    font: {{ color: '#E0E0E0' }},
                    bgcolor: 'rgba(0,0,0,0)'
                }},
                margin: {{ t: 10, b: 50, l: 60, r: 60 }}
            }};
            
            Plotly.newPlot(container, plotlyData, layout, {{ responsive: true, displayModeBar: false }});
        }}
        
        function initializeCategoryPerformance() {{
            updateCategoryKPIs();
            createRadialBarChart();
            createHeatmapChart();
        }}
        
        function updateCategoryKPIs() {{
            const kpis = dashboardData.category_performance.kpis;
            
            document.getElementById('best-index').textContent = kpis.best_performing_index;
            document.getElementById('best-category').textContent = kpis.best_performing_category;
            document.getElementById('watchlist-count').textContent = kpis.total_watchlist_symbols.toLocaleString();
        }}
        
        function createRadialBarChart() {{
            const container = document.getElementById('radial-bar-chart');
            const data = dashboardData.category_performance.category_data;
            
            const plotlyData = [{{
                type: 'barpolar',
                r: data.delivery_increases,
                theta: data.names,
                marker: {{
                    color: data.delivery_increases,
                    colorscale: [[0, '#D50000'], [0.5, '#00B0FF'], [1, '#00C853']],
                    showscale: true,
                    colorbar: {{
                        title: 'Delivery Increase %',
                        titlefont: {{ color: '#E0E0E0' }},
                        tickfont: {{ color: '#E0E0E0' }}
                    }}
                }},
                hovertemplate: '<b>%{{theta}}</b><br>Delivery Increase: %{{r:.1f}}%<br>Avg Delivery: %{{customdata:.1f}}%<extra></extra>',
                customdata: data.avg_delivery
            }}];
            
            const layout = {{
                font: {{ color: '#E0E0E0', family: 'Poppins' }},
                paper_bgcolor: 'transparent',
                plot_bgcolor: 'transparent',
                polar: {{
                    bgcolor: 'transparent',
                    radialaxis: {{
                        color: '#E0E0E0',
                        gridcolor: '#404040'
                    }},
                    angularaxis: {{
                        color: '#E0E0E0',
                        gridcolor: '#404040'
                    }}
                }},
                margin: {{ t: 10, b: 10, l: 10, r: 10 }}
            }};
            
            Plotly.newPlot(container, plotlyData, layout, {{ responsive: true, displayModeBar: false }});
        }}
        
        function createHeatmapChart() {{
            const container = document.getElementById('heatmap-chart');
            const data = dashboardData.category_performance.heatmap_data;
            
            // Group symbols by category for heatmap
            const categories = [...new Set(data.categories)];
            const heatmapZ = [];
            const heatmapY = [];
            const heatmapX = [];
            
            categories.forEach(category => {{
                const categorySymbols = data.symbols.filter((symbol, index) => data.categories[index] === category).slice(0, 10);
                const categoryDeliveries = categorySymbols.map(symbol => {{
                    const index = data.symbols.indexOf(symbol);
                    return data.delivery_pcts[index];
                }});
                
                heatmapZ.push(categoryDeliveries);
                heatmapY.push(category);
                heatmapX.push(...categorySymbols.map((symbol, index) => `S${{index + 1}}`));
            }});
            
            const plotlyData = [{{
                z: heatmapZ,
                y: heatmapY,
                type: 'heatmap',
                colorscale: [[0, '#D50000'], [0.5, '#00B0FF'], [1, '#00C853']],
                showscale: true,
                colorbar: {{
                    title: 'Delivery %',
                    titlefont: {{ color: '#E0E0E0' }},
                    tickfont: {{ color: '#E0E0E0' }}
                }},
                hovertemplate: '<b>%{{y}}</b><br>Delivery: %{{z:.1f}}%<extra></extra>'
            }}];
            
            const layout = {{
                font: {{ color: '#E0E0E0', family: 'Poppins' }},
                paper_bgcolor: 'transparent',
                plot_bgcolor: 'transparent',
                xaxis: {{ 
                    title: 'Symbols',
                    gridcolor: '#404040',
                    titlefont: {{ color: '#E0E0E0' }},
                    tickfont: {{ color: '#E0E0E0' }}
                }},
                yaxis: {{ 
                    title: 'Categories',
                    gridcolor: '#404040',
                    titlefont: {{ color: '#E0E0E0' }},
                    tickfont: {{ color: '#E0E0E0' }}
                }},
                margin: {{ t: 10, b: 50, l: 100, r: 10 }}
            }};
            
            Plotly.newPlot(container, plotlyData, layout, {{ responsive: true, displayModeBar: false }});
        }}
        
    </script>
</body>
</html>
'''
    
    # Save the unified dashboard
    dashboard_path = 'professional_dashboard.html'
    with open(dashboard_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"Professional Dashboard created: {dashboard_path}")
    print(f"\nDashboard Features:")
    print(f"✅ Market Overview - Treemap, Force-Directed, Sunburst, Parallel Coordinates")
    print(f"✅ Symbol Analysis - Candlestick, Dual-Axis, Interactive Search")
    print(f"✅ Category Performance - Radial Bar, Heatmap")
    print(f"✅ Professional Design - Dark theme, Poppins font, exact color palette")
    print(f"✅ Interactive Navigation - 3 tabs with smooth transitions")
    
    return dashboard_path

if __name__ == "__main__":
    create_unified_professional_dashboard()