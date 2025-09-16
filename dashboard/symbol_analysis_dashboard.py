import pyodbc
import pandas as pd
import json
from datetime import datetime

def create_symbol_analysis_dashboard():
    """Create a Symbol Analysis tab with detailed stock analysis"""
    
    # Database configuration
    server = 'SRIKIRANREDDY\\SQLEXPRESS'
    database = 'master'
    
    # Connect to database
    conn = pyodbc.connect(f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes')
    
    # Fetch detailed data for symbol analysis
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
        delivery_increase_pct,
        delivery_increase_abs,
        current_no_of_trades as no_of_trades,
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
    ORDER BY current_turnover_lacs DESC
    '''
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    print(f"Loaded {len(df)} records for Symbol Analysis")
    
    # Calculate technical indicators
    df['price_volatility'] = ((df['high_price'] - df['low_price']) / df['close_price']) * 100
    df['volume_price_trend'] = df['volume'] * df['price_change_pct']
    df['avg_trade_size'] = df['turnover'] / df['no_of_trades']
    df['delivery_strength'] = pd.cut(df['delivery_percentage'], 
                                   bins=[0, 40, 60, 80, 100], 
                                   labels=['Weak', 'Moderate', 'Strong', 'Very Strong'])
    
    # Top performers analysis
    top_volume = df.nlargest(30, 'volume')
    top_turnover = df.nlargest(30, 'turnover')
    top_delivery = df.nlargest(30, 'delivery_percentage')
    high_volatility = df.nlargest(20, 'price_volatility')
    
    # Create comprehensive data for Symbol Analysis
    symbol_data = {
        'top_volume_stocks': {
            'symbols': top_volume['symbol'].tolist(),
            'volumes': top_volume['volume'].tolist(),
            'price_changes': top_volume['price_change_pct'].tolist(),
            'delivery_pcts': top_volume['delivery_percentage'].tolist(),
            'sectors': top_volume['sector'].tolist(),
            'close_prices': top_volume['close_price'].tolist()
        },
        'top_turnover_stocks': {
            'symbols': top_turnover['symbol'].tolist(),
            'turnovers': top_turnover['turnover'].tolist(),
            'price_changes': top_turnover['price_change_pct'].tolist(),
            'delivery_pcts': top_turnover['delivery_percentage'].tolist(),
            'avg_trade_sizes': top_turnover['avg_trade_size'].tolist()
        },
        'delivery_leaders': {
            'symbols': top_delivery['symbol'].tolist(),
            'delivery_pcts': top_delivery['delivery_percentage'].tolist(),
            'price_changes': top_delivery['price_change_pct'].tolist(),
            'volumes': top_delivery['volume'].tolist(),
            'sectors': top_delivery['sector'].tolist()
        },
        'volatile_stocks': {
            'symbols': high_volatility['symbol'].tolist(),
            'volatility': high_volatility['price_volatility'].tolist(),
            'price_changes': high_volatility['price_change_pct'].tolist(),
            'delivery_pcts': high_volatility['delivery_percentage'].tolist(),
            'volumes': high_volatility['volume'].tolist()
        },
        'technical_analysis': {
            'all_symbols': df['symbol'].head(100).tolist(),
            'close_prices': df['close_price'].head(100).tolist(),
            'volumes': df['volume'].head(100).tolist(),
            'delivery_pcts': df['delivery_percentage'].head(100).tolist(),
            'volatility': df['price_volatility'].head(100).tolist(),
            'price_changes': df['price_change_pct'].head(100).tolist()
        },
        'market_stats': {
            'total_symbols': len(df),
            'avg_volatility': df['price_volatility'].mean(),
            'total_market_turnover': df['turnover'].sum(),
            'high_vol_count': len(df[df['volume'] > df['volume'].quantile(0.9)]),
            'strong_delivery_count': len(df[df['delivery_percentage'] > 70])
        }
    }
    
    # Create HTML for Symbol Analysis tab
    html_content = f'''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Symbol Analysis - NSE Dashboard</title>
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
            background: #0A0A0A;
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
            margin-bottom: 30px;
            background: linear-gradient(135deg, #7C3AED, #A855F7);
            padding: 25px;
            border-radius: 15px;
            border: 1px solid #8B5CF6;
        }}
        
        .header h1 {{
            color: #FFFFFF;
            font-size: 2.8rem;
            font-weight: 700;
            margin-bottom: 10px;
        }}
        
        .header p {{
            color: #E5E7EB;
            font-size: 1.2rem;
        }}
        
        .tabs {{
            display: flex;
            background: #1F2937;
            border-radius: 10px;
            margin-bottom: 30px;
            border: 1px solid #374151;
        }}
        
        .tab-button {{
            flex: 1;
            padding: 15px 20px;
            background: transparent;
            color: #9CA3AF;
            border: none;
            border-radius: 10px;
            cursor: pointer;
            font-size: 1rem;
            font-weight: 500;
            transition: all 0.3s ease;
        }}
        
        .tab-button.active {{
            background: #7C3AED;
            color: #FFFFFF;
        }}
        
        .tab-button:hover {{
            color: #FFFFFF;
            background: #6D28D9;
        }}
        
        .tab-content {{
            display: none;
        }}
        
        .tab-content.active {{
            display: block;
        }}
        
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        
        .stat-card {{
            background: #1F2937;
            padding: 20px;
            border-radius: 10px;
            text-align: center;
            border: 1px solid #374151;
        }}
        
        .stat-value {{
            font-size: 2rem;
            font-weight: 700;
            color: #A855F7;
            margin-bottom: 5px;
        }}
        
        .stat-label {{
            font-size: 0.9rem;
            color: #9CA3AF;
            text-transform: uppercase;
        }}
        
        .charts-grid {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 25px;
            margin-bottom: 30px;
        }}
        
        .chart-card {{
            background: #1F2937;
            border-radius: 15px;
            padding: 25px;
            border: 1px solid #374151;
        }}
        
        .chart-title {{
            font-size: 1.4rem;
            font-weight: 600;
            color: #A855F7;
            margin-bottom: 20px;
            text-align: center;
            border-bottom: 2px solid #374151;
            padding-bottom: 10px;
        }}
        
        .chart-container {{
            height: 400px;
            position: relative;
        }}
        
        .full-width {{
            grid-column: 1 / -1;
        }}
        
        .search-section {{
            background: #1F2937;
            padding: 20px;
            border-radius: 10px;
            margin-bottom: 20px;
            border: 1px solid #374151;
        }}
        
        .search-input {{
            width: 100%;
            padding: 12px;
            background: #374151;
            border: 1px solid #4B5563;
            border-radius: 8px;
            color: #E0E0E0;
            font-size: 1rem;
        }}
        
        .search-input:focus {{
            outline: none;
            border-color: #A855F7;
        }}
        
        .symbol-list {{
            max-height: 300px;
            overflow-y: auto;
            background: #374151;
            border-radius: 8px;
            margin-top: 10px;
        }}
        
        .symbol-item {{
            padding: 10px 15px;
            border-bottom: 1px solid #4B5563;
            cursor: pointer;
            transition: background 0.2s;
        }}
        
        .symbol-item:hover {{
            background: #4B5563;
        }}
        
        .symbol-name {{
            font-weight: 600;
            color: #E0E0E0;
        }}
        
        .symbol-details {{
            font-size: 0.9rem;
            color: #9CA3AF;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🔍 Symbol Analysis</h1>
            <p>Deep Dive into Individual Stock Performance • {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        </div>
        
        <div class="tabs">
            <button class="tab-button active" onclick="showTab('volume-leaders', this)">Volume Leaders</button>
            <button class="tab-button" onclick="showTab('turnover-leaders', this)">Turnover Leaders</button>
            <button class="tab-button" onclick="showTab('delivery-champions', this)">Delivery Champions</button>
            <button class="tab-button" onclick="showTab('technical-view', this)">Technical View</button>
        </div>
        
        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-value" id="total-symbols">-</div>
                <div class="stat-label">Total Symbols</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" id="avg-volatility">-</div>
                <div class="stat-label">Avg Volatility %</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" id="market-turnover">-</div>
                <div class="stat-label">Market Turnover (Cr)</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" id="high-volume-count">-</div>
                <div class="stat-label">High Volume Stocks</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" id="strong-delivery-count">-</div>
                <div class="stat-label">Strong Delivery (>70%)</div>
            </div>
        </div>
        
        <!-- Volume Leaders Tab -->
        <div id="volume-leaders" class="tab-content active">
            <div class="charts-grid">
                <div class="chart-card">
                    <div class="chart-title">📊 Top Volume Leaders</div>
                    <div class="chart-container" id="volume-chart"></div>
                </div>
                <div class="chart-card">
                    <div class="chart-title">🎯 Volume vs Delivery Analysis</div>
                    <div class="chart-container" id="volume-delivery-chart"></div>
                </div>
            </div>
        </div>
        
        <!-- Turnover Leaders Tab -->
        <div id="turnover-leaders" class="tab-content">
            <div class="charts-grid">
                <div class="chart-card">
                    <div class="chart-title">💰 Turnover Champions</div>
                    <div class="chart-container" id="turnover-chart"></div>
                </div>
                <div class="chart-card">
                    <div class="chart-title">📈 Trade Size Analysis</div>
                    <div class="chart-container" id="trade-size-chart"></div>
                </div>
            </div>
        </div>
        
        <!-- Delivery Champions Tab -->
        <div id="delivery-champions" class="tab-content">
            <div class="charts-grid">
                <div class="chart-card">
                    <div class="chart-title">🏆 Delivery Leaders</div>
                    <div class="chart-container" id="delivery-chart"></div>
                </div>
                <div class="chart-card">
                    <div class="chart-title">💎 Quality vs Performance</div>
                    <div class="chart-container" id="quality-performance-chart"></div>
                </div>
            </div>
        </div>
        
        <!-- Technical View Tab -->
        <div id="technical-view" class="tab-content">
            <div class="search-section">
                <input type="text" class="search-input" placeholder="Search for a symbol..." id="symbol-search">
                <div class="symbol-list" id="symbol-list"></div>
            </div>
            <div class="charts-grid">
                <div class="chart-card">
                    <div class="chart-title">📊 Volatility Heatmap</div>
                    <div class="chart-container" id="volatility-chart"></div>
                </div>
                <div class="chart-card">
                    <div class="chart-title">🎯 Risk-Return Matrix</div>
                    <div class="chart-container" id="risk-return-chart"></div>
                </div>
            </div>
        </div>
    </div>

    <script>
        // Symbol analysis data from Python
        const symbolData = {json.dumps(symbol_data, indent=2)};
        
        console.log('Symbol analysis data loaded:', symbolData);
        
        // Initialize dashboard when DOM is ready
        document.addEventListener('DOMContentLoaded', function() {{
            console.log('Initializing Symbol Analysis dashboard...');
            
            updateStats();
            initializeVolumeTab();
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
                if (tabId === 'volume-leaders') {{
                    initializeVolumeTab();
                }} else if (tabId === 'turnover-leaders') {{
                    initializeTurnoverTab();
                }} else if (tabId === 'delivery-champions') {{
                    initializeDeliveryTab();
                }} else if (tabId === 'technical-view') {{
                    initializeTechnicalTab();
                }}
            }}, 100);
        }}
        
        function updateStats() {{
            const stats = symbolData.market_stats;
            
            document.getElementById('total-symbols').textContent = stats.total_symbols.toLocaleString();
            document.getElementById('avg-volatility').textContent = stats.avg_volatility.toFixed(1) + '%';
            document.getElementById('market-turnover').textContent = (stats.total_market_turnover / 100).toFixed(0);
            document.getElementById('high-volume-count').textContent = stats.high_vol_count;
            document.getElementById('strong-delivery-count').textContent = stats.strong_delivery_count;
        }}
        
        function initializeVolumeTab() {{
            createVolumeChart();
            createVolumeDeliveryChart();
        }}
        
        function initializeTurnoverTab() {{
            createTurnoverChart();
            createTradeSizeChart();
        }}
        
        function initializeDeliveryTab() {{
            createDeliveryChart();
            createQualityPerformanceChart();
        }}
        
        function initializeTechnicalTab() {{
            createVolatilityChart();
            createRiskReturnChart();
        }}
        
        function createVolumeChart() {{
            const container = document.getElementById('volume-chart');
            const data = symbolData.top_volume_stocks;
            
            const plotlyData = [{{
                x: data.symbols.slice(0, 15),
                y: data.volumes.slice(0, 15),
                type: 'bar',
                marker: {{
                    color: data.price_changes.slice(0, 15),
                    colorscale: [[0, '#EF4444'], [0.5, '#F59E0B'], [1, '#10B981']],
                    showscale: false
                }},
                hovertemplate: '<b>%{{x}}</b><br>Volume: %{{y:,.0f}}<br>Price Change: %{{customdata:.1f}}%<br>Delivery: %{{customdata2:.1f}}%<extra></extra>',
                customdata: data.price_changes.slice(0, 15),
                customdata2: data.delivery_pcts.slice(0, 15)
            }}];
            
            const layout = {{
                font: {{ color: '#E0E0E0', family: 'Poppins' }},
                paper_bgcolor: 'transparent',
                plot_bgcolor: 'transparent',
                xaxis: {{ 
                    gridcolor: '#374151',
                    titlefont: {{ color: '#E0E0E0' }},
                    tickfont: {{ color: '#E0E0E0', size: 10 }},
                    tickangle: -45
                }},
                yaxis: {{ 
                    title: 'Volume',
                    gridcolor: '#374151',
                    titlefont: {{ color: '#E0E0E0' }},
                    tickfont: {{ color: '#E0E0E0' }}
                }},
                margin: {{ t: 10, b: 100, l: 60, r: 10 }}
            }};
            
            Plotly.newPlot(container, plotlyData, layout, {{ responsive: true, displayModeBar: false }});
        }}
        
        function createVolumeDeliveryChart() {{
            const container = document.getElementById('volume-delivery-chart');
            const data = symbolData.top_volume_stocks;
            
            const plotlyData = [{{
                x: data.volumes,
                y: data.delivery_pcts,
                mode: 'markers',
                type: 'scatter',
                text: data.symbols,
                marker: {{
                    size: 10,
                    color: data.price_changes,
                    colorscale: [[0, '#EF4444'], [0.5, '#F59E0B'], [1, '#10B981']],
                    showscale: true,
                    colorbar: {{
                        title: 'Price Change %',
                        titlefont: {{ color: '#E0E0E0' }},
                        tickfont: {{ color: '#E0E0E0' }}
                    }}
                }},
                hovertemplate: '<b>%{{text}}</b><br>Volume: %{{x:,.0f}}<br>Delivery: %{{y:.1f}}%<br>Price Change: %{{customdata:.1f}}%<extra></extra>',
                customdata: data.price_changes
            }}];
            
            const layout = {{
                font: {{ color: '#E0E0E0', family: 'Poppins' }},
                paper_bgcolor: 'transparent',
                plot_bgcolor: 'transparent',
                xaxis: {{ 
                    title: 'Volume',
                    gridcolor: '#374151',
                    titlefont: {{ color: '#E0E0E0' }},
                    tickfont: {{ color: '#E0E0E0' }}
                }},
                yaxis: {{ 
                    title: 'Delivery %',
                    gridcolor: '#374151',
                    titlefont: {{ color: '#E0E0E0' }},
                    tickfont: {{ color: '#E0E0E0' }}
                }},
                margin: {{ t: 10, b: 50, l: 60, r: 10 }}
            }};
            
            Plotly.newPlot(container, plotlyData, layout, {{ responsive: true, displayModeBar: false }});
        }}
        
        function createTurnoverChart() {{
            const container = document.getElementById('turnover-chart');
            const data = symbolData.top_turnover_stocks;
            
            const plotlyData = [{{
                x: data.symbols.slice(0, 15),
                y: data.turnovers.slice(0, 15),
                type: 'bar',
                marker: {{ color: '#A855F7' }},
                hovertemplate: '<b>%{{x}}</b><br>Turnover: ₹%{{y:,.0f}} Lacs<br>Price Change: %{{customdata:.1f}}%<extra></extra>',
                customdata: data.price_changes.slice(0, 15)
            }}];
            
            const layout = {{
                font: {{ color: '#E0E0E0', family: 'Poppins' }},
                paper_bgcolor: 'transparent',
                plot_bgcolor: 'transparent',
                xaxis: {{ 
                    gridcolor: '#374151',
                    titlefont: {{ color: '#E0E0E0' }},
                    tickfont: {{ color: '#E0E0E0', size: 10 }},
                    tickangle: -45
                }},
                yaxis: {{ 
                    title: 'Turnover (₹ Lacs)',
                    gridcolor: '#374151',
                    titlefont: {{ color: '#E0E0E0' }},
                    tickfont: {{ color: '#E0E0E0' }}
                }},
                margin: {{ t: 10, b: 100, l: 80, r: 10 }}
            }};
            
            Plotly.newPlot(container, plotlyData, layout, {{ responsive: true, displayModeBar: false }});
        }}
        
        function createTradeSizeChart() {{
            const container = document.getElementById('trade-size-chart');
            const data = symbolData.top_turnover_stocks;
            
            const plotlyData = [{{
                x: data.symbols.slice(0, 15),
                y: data.avg_trade_sizes.slice(0, 15),
                type: 'bar',
                marker: {{ color: '#10B981' }},
                hovertemplate: '<b>%{{x}}</b><br>Avg Trade Size: ₹%{{y:,.0f}}<br>Delivery: %{{customdata:.1f}}%<extra></extra>',
                customdata: data.delivery_pcts.slice(0, 15)
            }}];
            
            const layout = {{
                font: {{ color: '#E0E0E0', family: 'Poppins' }},
                paper_bgcolor: 'transparent',
                plot_bgcolor: 'transparent',
                xaxis: {{ 
                    gridcolor: '#374151',
                    titlefont: {{ color: '#E0E0E0' }},
                    tickfont: {{ color: '#E0E0E0', size: 10 }},
                    tickangle: -45
                }},
                yaxis: {{ 
                    title: 'Average Trade Size (₹)',
                    gridcolor: '#374151',
                    titlefont: {{ color: '#E0E0E0' }},
                    tickfont: {{ color: '#E0E0E0' }}
                }},
                margin: {{ t: 10, b: 100, l: 80, r: 10 }}
            }};
            
            Plotly.newPlot(container, plotlyData, layout, {{ responsive: true, displayModeBar: false }});
        }}
        
        function createDeliveryChart() {{
            const container = document.getElementById('delivery-chart');
            const data = symbolData.delivery_leaders;
            
            const plotlyData = [{{
                x: data.symbols.slice(0, 20),
                y: data.delivery_pcts.slice(0, 20),
                type: 'bar',
                marker: {{
                    color: data.delivery_pcts.slice(0, 20),
                    colorscale: [[0, '#F59E0B'], [1, '#10B981']],
                    showscale: false
                }},
                hovertemplate: '<b>%{{x}}</b><br>Delivery: %{{y:.1f}}%<br>Price Change: %{{customdata:.1f}}%<extra></extra>',
                customdata: data.price_changes.slice(0, 20)
            }}];
            
            const layout = {{
                font: {{ color: '#E0E0E0', family: 'Poppins' }},
                paper_bgcolor: 'transparent',
                plot_bgcolor: 'transparent',
                xaxis: {{ 
                    gridcolor: '#374151',
                    titlefont: {{ color: '#E0E0E0' }},
                    tickfont: {{ color: '#E0E0E0', size: 10 }},
                    tickangle: -45
                }},
                yaxis: {{ 
                    title: 'Delivery %',
                    gridcolor: '#374151',
                    titlefont: {{ color: '#E0E0E0' }},
                    tickfont: {{ color: '#E0E0E0' }}
                }},
                margin: {{ t: 10, b: 100, l: 60, r: 10 }}
            }};
            
            Plotly.newPlot(container, plotlyData, layout, {{ responsive: true, displayModeBar: false }});
        }}
        
        function createQualityPerformanceChart() {{
            const container = document.getElementById('quality-performance-chart');
            const data = symbolData.delivery_leaders;
            
            const plotlyData = [{{
                x: data.delivery_pcts,
                y: data.price_changes,
                mode: 'markers',
                type: 'scatter',
                text: data.symbols,
                marker: {{
                    size: data.volumes.map(v => Math.max(5, Math.min(20, v/10000000))),
                    color: '#A855F7',
                    line: {{ color: '#FFFFFF', width: 1 }}
                }},
                hovertemplate: '<b>%{{text}}</b><br>Delivery: %{{x:.1f}}%<br>Price Change: %{{y:.1f}}%<br>Volume: %{{customdata:,.0f}}<extra></extra>',
                customdata: data.volumes
            }}];
            
            const layout = {{
                font: {{ color: '#E0E0E0', family: 'Poppins' }},
                paper_bgcolor: 'transparent',
                plot_bgcolor: 'transparent',
                xaxis: {{ 
                    title: 'Delivery %',
                    gridcolor: '#374151',
                    titlefont: {{ color: '#E0E0E0' }},
                    tickfont: {{ color: '#E0E0E0' }}
                }},
                yaxis: {{ 
                    title: 'Price Change %',
                    gridcolor: '#374151',
                    titlefont: {{ color: '#E0E0E0' }},
                    tickfont: {{ color: '#E0E0E0' }}
                }},
                margin: {{ t: 10, b: 50, l: 60, r: 10 }}
            }};
            
            Plotly.newPlot(container, plotlyData, layout, {{ responsive: true, displayModeBar: false }});
        }}
        
        function createVolatilityChart() {{
            const container = document.getElementById('volatility-chart');
            const data = symbolData.volatile_stocks;
            
            const plotlyData = [{{
                x: data.symbols,
                y: data.volatility,
                type: 'bar',
                marker: {{
                    color: data.volatility,
                    colorscale: [[0, '#10B981'], [1, '#EF4444']],
                    showscale: false
                }},
                hovertemplate: '<b>%{{x}}</b><br>Volatility: %{{y:.1f}}%<br>Price Change: %{{customdata:.1f}}%<extra></extra>',
                customdata: data.price_changes
            }}];
            
            const layout = {{
                font: {{ color: '#E0E0E0', family: 'Poppins' }},
                paper_bgcolor: 'transparent',
                plot_bgcolor: 'transparent',
                xaxis: {{ 
                    gridcolor: '#374151',
                    titlefont: {{ color: '#E0E0E0' }},
                    tickfont: {{ color: '#E0E0E0', size: 10 }},
                    tickangle: -45
                }},
                yaxis: {{ 
                    title: 'Volatility %',
                    gridcolor: '#374151',
                    titlefont: {{ color: '#E0E0E0' }},
                    tickfont: {{ color: '#E0E0E0' }}
                }},
                margin: {{ t: 10, b: 100, l: 60, r: 10 }}
            }};
            
            Plotly.newPlot(container, plotlyData, layout, {{ responsive: true, displayModeBar: false }});
        }}
        
        function createRiskReturnChart() {{
            const container = document.getElementById('risk-return-chart');
            const data = symbolData.technical_analysis;
            
            const plotlyData = [{{
                x: data.volatility.slice(0, 50),
                y: data.price_changes.slice(0, 50),
                mode: 'markers',
                type: 'scatter',
                text: data.all_symbols.slice(0, 50),
                marker: {{
                    size: 8,
                    color: data.delivery_pcts.slice(0, 50),
                    colorscale: [[0, '#EF4444'], [0.5, '#F59E0B'], [1, '#10B981']],
                    showscale: true,
                    colorbar: {{
                        title: 'Delivery %',
                        titlefont: {{ color: '#E0E0E0' }},
                        tickfont: {{ color: '#E0E0E0' }}
                    }}
                }},
                hovertemplate: '<b>%{{text}}</b><br>Volatility: %{{x:.1f}}%<br>Return: %{{y:.1f}}%<br>Delivery: %{{customdata:.1f}}%<extra></extra>',
                customdata: data.delivery_pcts.slice(0, 50)
            }}];
            
            const layout = {{
                font: {{ color: '#E0E0E0', family: 'Poppins' }},
                paper_bgcolor: 'transparent',
                plot_bgcolor: 'transparent',
                xaxis: {{ 
                    title: 'Risk (Volatility %)',
                    gridcolor: '#374151',
                    titlefont: {{ color: '#E0E0E0' }},
                    tickfont: {{ color: '#E0E0E0' }}
                }},
                yaxis: {{ 
                    title: 'Return (Price Change %)',
                    gridcolor: '#374151',
                    titlefont: {{ color: '#E0E0E0' }},
                    tickfont: {{ color: '#E0E0E0' }}
                }},
                margin: {{ t: 10, b: 50, l: 70, r: 10 }}
            }};
            
            Plotly.newPlot(container, plotlyData, layout, {{ responsive: true, displayModeBar: false }});
        }}
        
        function setupSymbolSearch() {{
            const searchInput = document.getElementById('symbol-search');
            const symbolList = document.getElementById('symbol-list');
            const allSymbols = symbolData.technical_analysis.all_symbols;
            
            searchInput.addEventListener('input', function() {{
                const searchTerm = this.value.toLowerCase();
                const filteredSymbols = allSymbols.filter(symbol => 
                    symbol.toLowerCase().includes(searchTerm)
                ).slice(0, 10);
                
                symbolList.innerHTML = filteredSymbols.map(symbol => {{
                    const index = allSymbols.indexOf(symbol);
                    const price = symbolData.technical_analysis.close_prices[index];
                    const delivery = symbolData.technical_analysis.delivery_pcts[index];
                    const change = symbolData.technical_analysis.price_changes[index];
                    
                    return `
                        <div class="symbol-item" onclick="selectSymbol('${{symbol}}')">
                            <div class="symbol-name">${{symbol}}</div>
                            <div class="symbol-details">₹${{price?.toFixed(2) || 'N/A'}} | ${{change?.toFixed(1) || 'N/A'}}% | Del: ${{delivery?.toFixed(1) || 'N/A'}}%</div>
                        </div>
                    `;
                }}).join('');
            }});
        }}
        
        function selectSymbol(symbol) {{
            console.log('Selected symbol:', symbol);
            // Add symbol-specific analysis here
            alert(`Selected: ${{symbol}} - Detailed analysis coming soon!`);
        }}
        
    </script>
</body>
</html>
'''
    
    # Save dashboard
    dashboard_path = 'dashboard/symbol_analysis.html'
    with open(dashboard_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"Symbol Analysis Dashboard created: {dashboard_path}")
    print(f"\nKey Features:")
    print(f"- Volume Leaders: Top {len(symbol_data['top_volume_stocks']['symbols'])} high-volume stocks")
    print(f"- Turnover Champions: Top {len(symbol_data['top_turnover_stocks']['symbols'])} turnover leaders")
    print(f"- Delivery Champions: Top {len(symbol_data['delivery_leaders']['symbols'])} delivery leaders")
    print(f"- Technical Analysis: Risk-return and volatility analysis")
    print(f"- Symbol Search: Interactive search functionality")
    
    return dashboard_path

if __name__ == "__main__":
    create_symbol_analysis_dashboard()