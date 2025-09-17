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
    
    # Optimized query for fast loading
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
    WHERE current_turnover_lacs > 50
        AND symbol IS NOT NULL
    ORDER BY current_turnover_lacs DESC
    '''
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    print(f"Loaded {len(df)} records for Merged Dashboard")
    
    # Calculate market overview data
    high_delivery_stocks = len(df[df['delivery_percentage'] > 80])
    momentum_stocks = len(df[(df['price_change_pct'] > 5) & (df['delivery_percentage'] > 70)])
    value_opportunities = len(df[(df['price_change_pct'] < -2) & (df['delivery_percentage'] > 70)])
    avg_delivery = df['delivery_percentage'].mean()
    positive_stocks = len(df[df['price_change_pct'] > 0])
    total_turnover = df['turnover'].sum()
    
    # Sector analysis
    sector_data = df.groupby('sector').agg({
        'delivery_percentage': 'mean',
        'price_change_pct': 'mean',
        'turnover': 'sum',
        'symbol': 'count'
    }).round(2)
    
    # Top performers
    top_gainers = df.nlargest(10, 'price_change_pct')
    top_volume = df.nlargest(15, 'volume')
    top_turnover = df.nlargest(15, 'turnover')
    top_delivery = df.nlargest(20, 'delivery_percentage')
    
    # Create comprehensive dashboard data
    dashboard_data = {
        'market_overview': {
            'kpis': {
                'high_delivery_stocks': high_delivery_stocks,
                'momentum_stocks': momentum_stocks,
                'value_opportunities': value_opportunities,
                'avg_delivery': round(avg_delivery, 1),
                'positive_stocks': positive_stocks,
                'total_turnover': round(total_turnover / 100000, 2)
            },
            'sector_performance': {
                'sectors': sector_data.index.tolist(),
                'avg_delivery': sector_data['delivery_percentage'].tolist(),
                'avg_price_change': sector_data['price_change_pct'].tolist(),
                'total_turnover': sector_data['turnover'].tolist()
            },
            'top_gainers': {
                'symbols': top_gainers['symbol'].tolist(),
                'price_changes': top_gainers['price_change_pct'].tolist(),
                'delivery_pcts': top_gainers['delivery_percentage'].tolist()
            }
        },
        'symbol_analysis': {
            'top_volume': {
                'symbols': top_volume['symbol'].tolist(),
                'volumes': top_volume['volume'].tolist(),
                'price_changes': top_volume['price_change_pct'].tolist(),
                'delivery_pcts': top_volume['delivery_percentage'].tolist()
            },
            'top_turnover': {
                'symbols': top_turnover['symbol'].tolist(),
                'turnovers': top_turnover['turnover'].tolist(),
                'price_changes': top_turnover['price_change_pct'].tolist(),
                'delivery_pcts': top_turnover['delivery_percentage'].tolist()
            },
            'top_delivery': {
                'symbols': top_delivery['symbol'].tolist(),
                'delivery_pcts': top_delivery['delivery_percentage'].tolist(),
                'price_changes': top_delivery['price_change_pct'].tolist(),
                'volumes': top_delivery['volume'].tolist()
            }
        },
        'search_symbols': {
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
            } for symbol, row in df.set_index('symbol').iterrows()
        }
    }
    
    # Create unified HTML dashboard
    html_content = f'''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>NSE Professional Dashboard</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <link href="https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;500;600;700&display=swap" rel="stylesheet">
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        
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
            margin-bottom: 25px;
            background: linear-gradient(135deg, #1E3A8A, #3B82F6);
            padding: 25px;
            border-radius: 15px;
            border: 2px solid #1E40AF;
            box-shadow: 0 8px 25px rgba(59, 130, 246, 0.2);
        }}
        
        .header h1 {{
            color: #FFFFFF;
            font-size: 2.5rem;
            font-weight: 700;
            margin-bottom: 8px;
        }}
        
        .header p {{
            color: #E5E7EB;
            font-size: 1.1rem;
        }}
        
        .tab-navigation {{
            display: flex;
            background: #1F2937;
            border-radius: 12px;
            margin-bottom: 25px;
            border: 1px solid #374151;
            overflow: hidden;
        }}
        
        .tab-button {{
            flex: 1;
            padding: 15px 30px;
            background: transparent;
            color: #9CA3AF;
            border: none;
            cursor: pointer;
            font-size: 1.1rem;
            font-weight: 600;
            font-family: 'Poppins', sans-serif;
            transition: all 0.3s ease;
        }}
        
        .tab-button.active {{
            background: linear-gradient(135deg, #3B82F6, #1E40AF);
            color: #FFFFFF;
        }}
        
        .tab-button:hover:not(.active) {{
            background: #374151;
            color: #3B82F6;
        }}
        
        .tab-content {{
            display: none;
            animation: fadeIn 0.3s ease-in-out;
        }}
        
        .tab-content.active {{
            display: block;
        }}
        
        @keyframes fadeIn {{
            from {{ opacity: 0; transform: translateY(10px); }}
            to {{ opacity: 1; transform: translateY(0); }}
        }}
        
        .kpi-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        
        .kpi-card {{
            background: linear-gradient(135deg, #1F2937, #374151);
            padding: 25px;
            border-radius: 15px;
            text-align: center;
            border: 1px solid #4B5563;
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
            background: linear-gradient(90deg, #10B981, #3B82F6, #EF4444);
        }}
        
        .kpi-card:hover {{
            transform: translateY(-5px);
            box-shadow: 0 12px 25px rgba(59, 130, 246, 0.15);
        }}
        
        .kpi-value {{
            font-size: 2.2rem;
            font-weight: 700;
            margin-bottom: 8px;
            background: linear-gradient(45deg, #3B82F6, #10B981);
            background-clip: text;
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }}
        
        .kpi-value.positive {{ 
            background: linear-gradient(45deg, #10B981, #34D399);
            background-clip: text;
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }}
        
        .kpi-value.negative {{ 
            background: linear-gradient(45deg, #EF4444, #F87171);
            background-clip: text;
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }}
        
        .kpi-label {{
            font-size: 0.9rem;
            color: #9CA3AF;
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
            background: #1F2937;
            border-radius: 15px;
            padding: 25px;
            border: 1px solid #374151;
            transition: all 0.3s ease;
        }}
        
        .chart-card:hover {{
            transform: translateY(-3px);
            box-shadow: 0 15px 30px rgba(0, 0, 0, 0.2);
        }}
        
        .chart-title {{
            font-size: 1.3rem;
            font-weight: 600;
            color: #3B82F6;
            margin-bottom: 20px;
            text-align: center;
            border-bottom: 2px solid #374151;
            padding-bottom: 12px;
        }}
        
        .chart-container {{
            height: 350px;
            position: relative;
        }}
        
        .full-width {{
            grid-column: 1 / -1;
        }}
        
        .search-section {{
            background: #1F2937;
            padding: 20px;
            border-radius: 12px;
            margin-bottom: 25px;
            border: 1px solid #374151;
        }}
        
        .search-input {{
            width: 100%;
            padding: 12px 18px;
            background: #374151;
            border: 2px solid #4B5563;
            border-radius: 8px;
            color: #E0E0E0;
            font-size: 1rem;
            font-family: 'Poppins', sans-serif;
            transition: all 0.3s ease;
        }}
        
        .search-input:focus {{
            outline: none;
            border-color: #3B82F6;
            box-shadow: 0 0 10px rgba(59, 130, 246, 0.3);
        }}
        
        .suggestions {{
            background: #374151;
            border: 1px solid #4B5563;
            border-radius: 8px;
            margin-top: 8px;
            max-height: 200px;
            overflow-y: auto;
            display: none;
        }}
        
        .suggestion-item {{
            padding: 12px 15px;
            cursor: pointer;
            transition: background 0.3s ease;
            border-bottom: 1px solid #4B5563;
        }}
        
        .suggestion-item:hover {{
            background: #4B5563;
            color: #3B82F6;
        }}
        
        .symbol-info {{
            background: #374151;
            padding: 20px;
            border-radius: 8px;
            margin-top: 15px;
            border: 1px solid #4B5563;
            display: none;
        }}
        
        .symbol-info h3 {{
            color: #3B82F6;
            margin-bottom: 15px;
            text-align: center;
            font-size: 1.5rem;
        }}
        
        .stats-row {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 15px;
            margin-bottom: 15px;
        }}
        
        .stat-item {{
            background: #4B5563;
            padding: 12px;
            border-radius: 6px;
            text-align: center;
        }}
        
        .stat-value {{
            font-size: 1.2rem;
            font-weight: 600;
            color: #10B981;
        }}
        
        .stat-label {{
            font-size: 0.8rem;
            color: #9CA3AF;
            text-transform: uppercase;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 NSE Professional Dashboard</h1>
            <p>Market Overview & Symbol Analysis • <span style="color: #60A5FA;">{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</span></p>
        </div>
        
        <div class="tab-navigation">
            <button class="tab-button active" onclick="showTab('market-overview', this)">
                📈 Market Overview
            </button>
            <button class="tab-button" onclick="showTab('symbol-analysis', this)">
                🔍 Symbol Analysis
            </button>
        </div>
        
        <!-- Market Overview Tab -->
        <div id="market-overview" class="tab-content active">
            <div class="kpi-grid">
                <div class="kpi-card">
                    <div class="kpi-value positive">{dashboard_data['market_overview']['kpis']['high_delivery_stocks']}</div>
                    <div class="kpi-label">High Delivery Stocks (>80%)</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-value">{dashboard_data['market_overview']['kpis']['momentum_stocks']}</div>
                    <div class="kpi-label">Momentum Stocks</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-value negative">{dashboard_data['market_overview']['kpis']['value_opportunities']}</div>
                    <div class="kpi-label">Value Opportunities</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-value">{dashboard_data['market_overview']['kpis']['avg_delivery']:.1f}%</div>
                    <div class="kpi-label">Average Delivery</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-value positive">{dashboard_data['market_overview']['kpis']['positive_stocks']}</div>
                    <div class="kpi-label">Positive Stocks</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-value">₹{dashboard_data['market_overview']['kpis']['total_turnover']:.0f}Cr</div>
                    <div class="kpi-label">Total Turnover</div>
                </div>
            </div>
            
            <div class="charts-grid">
                <div class="chart-card">
                    <div class="chart-title">🏆 Sector Performance</div>
                    <div class="chart-container" id="sector-performance"></div>
                </div>
                
                <div class="chart-card">
                    <div class="chart-title">📊 Sector Turnover</div>
                    <div class="chart-container" id="sector-turnover"></div>
                </div>
                
                <div class="chart-card full-width">
                    <div class="chart-title">🚀 Top Gainers</div>
                    <div class="chart-container" id="top-gainers"></div>
                </div>
            </div>
        </div>
        
        <!-- Symbol Analysis Tab -->
        <div id="symbol-analysis" class="tab-content">
            <div class="search-section">
                <input type="text" class="search-input" placeholder="🔍 Search symbol (e.g., RELIANCE, TCS, INFY...)" id="symbol-search">
                <div class="suggestions" id="suggestions"></div>
                <div class="symbol-info" id="symbol-info">
                    <h3 id="selected-symbol">-</h3>
                    <div class="stats-row">
                        <div class="stat-item">
                            <div class="stat-value" id="symbol-delivery">-</div>
                            <div class="stat-label">Delivery %</div>
                        </div>
                        <div class="stat-item">
                            <div class="stat-value" id="symbol-change">-</div>
                            <div class="stat-label">Price Change %</div>
                        </div>
                        <div class="stat-item">
                            <div class="stat-value" id="symbol-volume">-</div>
                            <div class="stat-label">Volume</div>
                        </div>
                        <div class="stat-item">
                            <div class="stat-value" id="symbol-price">-</div>
                            <div class="stat-label">Close Price</div>
                        </div>
                    </div>
                </div>
            </div>
            
            <div class="charts-grid">
                <div class="chart-card">
                    <div class="chart-title">📊 Volume Leaders</div>
                    <div class="chart-container" id="volume-leaders"></div>
                </div>
                
                <div class="chart-card">
                    <div class="chart-title">💰 Turnover Leaders</div>
                    <div class="chart-container" id="turnover-leaders"></div>
                </div>
                
                <div class="chart-card full-width">
                    <div class="chart-title">🏆 Delivery Champions</div>
                    <div class="chart-container" id="delivery-champions"></div>
                </div>
            </div>
        </div>
    </div>

    <script>
        // Dashboard data embedded for instant loading
        const data = {json.dumps(dashboard_data)};
        
        console.log('Merged dashboard loaded with', Object.keys(data.search_symbols).length, 'symbols');
        
        // Initialize dashboard
        document.addEventListener('DOMContentLoaded', function() {{
            initializeMarketOverview();
            initializeSymbolAnalysis();
            setupSymbolSearch();
        }});
        
        function showTab(tabId, button) {{
            // Hide all tabs
            document.querySelectorAll('.tab-content').forEach(tab => tab.classList.remove('active'));
            document.querySelectorAll('.tab-button').forEach(btn => btn.classList.remove('active'));
            
            // Show selected tab
            document.getElementById(tabId).classList.add('active');
            button.classList.add('active');
        }}
        
        function initializeMarketOverview() {{
            createSectorPerformanceChart();
            createSectorTurnoverChart();
            createTopGainersChart();
        }}
        
        function initializeSymbolAnalysis() {{
            createVolumeLeadersChart();
            createTurnoverLeadersChart();
            createDeliveryChampionsChart();
        }}
        
        function createSectorPerformanceChart() {{
            const sectorData = data.market_overview.sector_performance;
            
            const plotlyData = [{{
                x: sectorData.avg_delivery,
                y: sectorData.avg_price_change,
                mode: 'markers+text',
                type: 'scatter',
                text: sectorData.sectors,
                textposition: 'middle center',
                marker: {{
                    size: 15,
                    color: sectorData.avg_price_change,
                    colorscale: [[0, '#EF4444'], [0.5, '#F59E0B'], [1, '#10B981']],
                    showscale: false
                }},
                hovertemplate: '<b>%{{text}}</b><br>Avg Delivery: %{{x:.1f}}%<br>Avg Change: %{{y:.1f}}%<extra></extra>'
            }}];
            
            const layout = {{
                paper_bgcolor: 'transparent',
                plot_bgcolor: 'transparent',
                font: {{ color: '#E0E0E0' }},
                margin: {{ t: 10, b: 50, l: 60, r: 10 }},
                xaxis: {{ title: 'Average Delivery %', gridcolor: '#374151' }},
                yaxis: {{ title: 'Average Price Change %', gridcolor: '#374151' }}
            }};
            
            Plotly.newPlot('sector-performance', plotlyData, layout, {{ responsive: true, displayModeBar: false }});
        }}
        
        function createSectorTurnoverChart() {{
            const sectorData = data.market_overview.sector_performance;
            
            const plotlyData = [{{
                values: sectorData.total_turnover,
                labels: sectorData.sectors,
                type: 'pie',
                marker: {{ colors: ['#3B82F6', '#10B981', '#F59E0B', '#EF4444', '#8B5CF6', '#EC4899', '#06B6D4'] }}
            }}];
            
            const layout = {{
                paper_bgcolor: 'transparent',
                font: {{ color: '#E0E0E0' }},
                margin: {{ t: 10, b: 10, l: 10, r: 10 }}
            }};
            
            Plotly.newPlot('sector-turnover', plotlyData, layout, {{ responsive: true, displayModeBar: false }});
        }}
        
        function createTopGainersChart() {{
            const gainersData = data.market_overview.top_gainers;
            
            const plotlyData = [{{
                x: gainersData.symbols,
                y: gainersData.price_changes,
                type: 'bar',
                marker: {{ color: '#10B981' }},
                hovertemplate: '<b>%{{x}}</b><br>Change: %{{y:.1f}}%<br>Delivery: %{{customdata:.1f}}%<extra></extra>',
                customdata: gainersData.delivery_pcts
            }}];
            
            const layout = {{
                paper_bgcolor: 'transparent',
                plot_bgcolor: 'transparent',
                font: {{ color: '#E0E0E0' }},
                margin: {{ t: 10, b: 60, l: 50, r: 10 }},
                xaxis: {{ gridcolor: '#374151', tickangle: -45 }},
                yaxis: {{ title: 'Price Change %', gridcolor: '#374151' }}
            }};
            
            Plotly.newPlot('top-gainers', plotlyData, layout, {{ responsive: true, displayModeBar: false }});
        }}
        
        function createVolumeLeadersChart() {{
            const volumeData = data.symbol_analysis.top_volume;
            
            const plotlyData = [{{
                x: volumeData.symbols.slice(0, 10),
                y: volumeData.volumes.slice(0, 10),
                type: 'bar',
                marker: {{ color: '#3B82F6' }},
                hovertemplate: '<b>%{{x}}</b><br>Volume: %{{y:,.0f}}<br>Change: %{{customdata:.1f}}%<extra></extra>',
                customdata: volumeData.price_changes.slice(0, 10)
            }}];
            
            const layout = {{
                paper_bgcolor: 'transparent',
                plot_bgcolor: 'transparent',
                font: {{ color: '#E0E0E0' }},
                margin: {{ t: 10, b: 60, l: 80, r: 10 }},
                xaxis: {{ gridcolor: '#374151', tickangle: -45 }},
                yaxis: {{ title: 'Volume', gridcolor: '#374151' }}
            }};
            
            Plotly.newPlot('volume-leaders', plotlyData, layout, {{ responsive: true, displayModeBar: false }});
        }}
        
        function createTurnoverLeadersChart() {{
            const turnoverData = data.symbol_analysis.top_turnover;
            
            const plotlyData = [{{
                x: turnoverData.symbols.slice(0, 10),
                y: turnoverData.turnovers.slice(0, 10),
                type: 'bar',
                marker: {{ color: '#F59E0B' }},
                hovertemplate: '<b>%{{x}}</b><br>Turnover: ₹%{{y:,.0f}} Lacs<br>Delivery: %{{customdata:.1f}}%<extra></extra>',
                customdata: turnoverData.delivery_pcts.slice(0, 10)
            }}];
            
            const layout = {{
                paper_bgcolor: 'transparent',
                plot_bgcolor: 'transparent',
                font: {{ color: '#E0E0E0' }},
                margin: {{ t: 10, b: 60, l: 80, r: 10 }},
                xaxis: {{ gridcolor: '#374151', tickangle: -45 }},
                yaxis: {{ title: 'Turnover (₹ Lacs)', gridcolor: '#374151' }}
            }};
            
            Plotly.newPlot('turnover-leaders', plotlyData, layout, {{ responsive: true, displayModeBar: false }});
        }}
        
        function createDeliveryChampionsChart() {{
            const deliveryData = data.symbol_analysis.top_delivery;
            
            const plotlyData = [{{
                x: deliveryData.symbols.slice(0, 15),
                y: deliveryData.delivery_pcts.slice(0, 15),
                type: 'bar',
                marker: {{ color: '#10B981' }},
                hovertemplate: '<b>%{{x}}</b><br>Delivery: %{{y:.1f}}%<br>Change: %{{customdata:.1f}}%<extra></extra>',
                customdata: deliveryData.price_changes.slice(0, 15)
            }}];
            
            const layout = {{
                paper_bgcolor: 'transparent',
                plot_bgcolor: 'transparent',
                font: {{ color: '#E0E0E0' }},
                margin: {{ t: 10, b: 60, l: 50, r: 10 }},
                xaxis: {{ gridcolor: '#374151', tickangle: -45 }},
                yaxis: {{ title: 'Delivery %', gridcolor: '#374151' }}
            }};
            
            Plotly.newPlot('delivery-champions', plotlyData, layout, {{ responsive: true, displayModeBar: false }});
        }}
        
        function setupSymbolSearch() {{
            const searchInput = document.getElementById('symbol-search');
            const suggestions = document.getElementById('suggestions');
            const symbolInfo = document.getElementById('symbol-info');
            const symbols = Object.keys(data.search_symbols);
            
            searchInput.addEventListener('input', function() {{
                const term = this.value.toUpperCase();
                
                if (term.length >= 1) {{
                    const matches = symbols.filter(s => s.includes(term)).slice(0, 8);
                    
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
            const searchInput = document.getElementById('symbol-search');
            const suggestions = document.getElementById('suggestions');
            const symbolInfo = document.getElementById('symbol-info');
            
            searchInput.value = symbol;
            suggestions.style.display = 'none';
            
            const symbolData = data.search_symbols[symbol];
            
            if (symbolData) {{
                symbolInfo.style.display = 'block';
                document.getElementById('selected-symbol').textContent = symbol;
                document.getElementById('symbol-delivery').textContent = symbolData.delivery.toFixed(1) + '%';
                document.getElementById('symbol-change').textContent = symbolData.price_change.toFixed(2) + '%';
                document.getElementById('symbol-volume').textContent = symbolData.volume.toLocaleString();
                document.getElementById('symbol-price').textContent = '₹' + symbolData.close.toFixed(2);
                
                // Update color based on price change
                const changeElement = document.getElementById('symbol-change');
                if (symbolData.price_change > 0) {{
                    changeElement.style.color = '#10B981';
                }} else if (symbolData.price_change < 0) {{
                    changeElement.style.color = '#EF4444';
                }} else {{
                    changeElement.style.color = '#9CA3AF';
                }}
            }}
        }}
        
    </script>
</body>
</html>
'''
    
    # Save the merged dashboard
    dashboard_path = 'merged_professional_dashboard.html'
    with open(dashboard_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"Merged Professional Dashboard created: {dashboard_path}")
    print(f"\nFeatures:")
    print(f"📈 Market Overview: 6 KPIs + 3 interactive charts")
    print(f"🔍 Symbol Analysis: Search + 3 detailed charts")
    print(f"⚡ Fast Loading: Optimized for 1,000 top stocks")
    print(f"🎨 Professional Design: Bloomberg-style dark theme")
    print(f"📱 Responsive: Works on all devices")
    
    return dashboard_path

if __name__ == "__main__":
    create_merged_dashboard()