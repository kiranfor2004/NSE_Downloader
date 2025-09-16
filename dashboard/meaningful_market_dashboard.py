import pyodbc
import pandas as pd
import json
from datetime import datetime

def create_meaningful_dashboard():
    """Create a dashboard with meaningful KPIs and actionable insights"""
    
    # Database configuration
    server = 'SRIKIRANREDDY\\SQLEXPRESS'
    database = 'master'
    
    # Connect to database
    conn = pyodbc.connect(f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes')
    
    # Fetch comprehensive data for meaningful analysis
    query = '''
    SELECT 
        symbol,
        current_deliv_per as delivery_percentage,
        ((current_close_price - current_prev_close) / current_prev_close) * 100 as price_change_pct,
        current_ttl_trd_qnty as volume,
        current_turnover_lacs as turnover,
        current_close_price as close_price,
        current_prev_close as prev_close,
        delivery_increase_pct,
        delivery_increase_abs,
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
    ORDER BY current_deliv_per DESC
    '''
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    print(f"Loaded {len(df)} records")
    
    # Calculate meaningful KPIs
    high_delivery_stocks = df[df['delivery_percentage'] > 80]
    momentum_stocks = df[(df['price_change_pct'] > 5) & (df['delivery_percentage'] > 70)]
    value_opportunities = df[(df['price_change_pct'] < -2) & (df['delivery_percentage'] > 60)]
    high_volume_movers = df.nlargest(50, 'volume')
    
    # Sector performance analysis
    sector_performance = df.groupby('sector').agg({
        'delivery_percentage': 'mean',
        'price_change_pct': 'mean',
        'turnover': 'sum',
        'symbol': 'count',
        'volume': 'sum'
    }).round(2)
    
    # Price bands analysis
    df['price_band'] = pd.cut(df['close_price'], 
                             bins=[0, 100, 500, 1000, 5000, float('inf')], 
                             labels=['<₹100', '₹100-500', '₹500-1K', '₹1K-5K', '>₹5K'])
    
    price_band_analysis = df.groupby('price_band').agg({
        'delivery_percentage': 'mean',
        'price_change_pct': 'mean',
        'symbol': 'count'
    }).round(2)
    
    # Create meaningful data for charts
    dashboard_data = {
        'high_delivery_stocks': {
            'symbols': high_delivery_stocks['symbol'].head(20).tolist(),
            'delivery_pcts': high_delivery_stocks['delivery_percentage'].head(20).tolist(),
            'price_changes': high_delivery_stocks['price_change_pct'].head(20).tolist(),
            'sectors': high_delivery_stocks['sector'].head(20).tolist()
        },
        'momentum_stocks': {
            'symbols': momentum_stocks['symbol'].head(15).tolist(),
            'delivery_pcts': momentum_stocks['delivery_percentage'].head(15).tolist(),
            'price_changes': momentum_stocks['price_change_pct'].head(15).tolist(),
            'turnover': momentum_stocks['turnover'].head(15).tolist()
        },
        'sector_performance': {
            'sectors': sector_performance.index.tolist(),
            'avg_delivery': sector_performance['delivery_percentage'].tolist(),
            'avg_price_change': sector_performance['price_change_pct'].tolist(),
            'total_turnover': sector_performance['turnover'].tolist(),
            'stock_count': sector_performance['symbol'].tolist()
        },
        'price_band_analysis': {
            'bands': price_band_analysis.index.tolist(),
            'avg_delivery': price_band_analysis['delivery_percentage'].tolist(),
            'avg_price_change': price_band_analysis['price_change_pct'].tolist(),
            'stock_count': price_band_analysis['symbol'].tolist()
        },
        'value_opportunities': {
            'symbols': value_opportunities['symbol'].head(10).tolist(),
            'delivery_pcts': value_opportunities['delivery_percentage'].head(10).tolist(),
            'price_changes': value_opportunities['price_change_pct'].head(10).tolist(),
            'close_prices': value_opportunities['close_price'].head(10).tolist()
        },
        'market_summary': {
            'total_stocks': len(df),
            'high_delivery_count': len(high_delivery_stocks),
            'momentum_stocks_count': len(momentum_stocks),
            'value_opportunities_count': len(value_opportunities),
            'avg_delivery': df['delivery_percentage'].mean(),
            'market_trend': 'Bullish' if df['price_change_pct'].mean() > 0 else 'Bearish',
            'top_sector': sector_performance.loc[sector_performance['delivery_percentage'].idxmax()].name if len(sector_performance) > 0 else 'N/A'
        }
    }
    
    # Create HTML dashboard with meaningful insights
    html_content = f'''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>NSE Market Intelligence Dashboard</title>
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
            background: linear-gradient(135deg, #1E3A8A, #3B82F6);
            padding: 25px;
            border-radius: 15px;
            border: 1px solid #1E40AF;
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
        
        .kpi-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        
        .kpi-card {{
            background: linear-gradient(135deg, #1F2937, #374151);
            padding: 25px;
            border-radius: 12px;
            text-align: center;
            border: 1px solid #4B5563;
            transition: transform 0.3s ease, box-shadow 0.3s ease;
        }}
        
        .kpi-card:hover {{
            transform: translateY(-5px);
            box-shadow: 0 10px 25px rgba(59, 130, 246, 0.15);
        }}
        
        .kpi-value {{
            font-size: 2.5rem;
            font-weight: 700;
            margin-bottom: 8px;
        }}
        
        .kpi-value.positive {{ color: #10B981; }}
        .kpi-value.negative {{ color: #EF4444; }}
        .kpi-value.neutral {{ color: #3B82F6; }}
        
        .kpi-label {{
            font-size: 0.95rem;
            color: #9CA3AF;
            text-transform: uppercase;
            letter-spacing: 0.5px;
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
            transition: transform 0.3s ease;
        }}
        
        .chart-card:hover {{
            transform: translateY(-3px);
        }}
        
        .chart-title {{
            font-size: 1.4rem;
            font-weight: 600;
            color: #3B82F6;
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
        
        .insights-section {{
            background: #1F2937;
            border-radius: 15px;
            padding: 30px;
            border: 1px solid #374151;
            margin-bottom: 20px;
        }}
        
        .insights-title {{
            font-size: 1.6rem;
            font-weight: 600;
            color: #3B82F6;
            margin-bottom: 20px;
            text-align: center;
        }}
        
        .insight-item {{
            background: #374151;
            padding: 15px;
            border-radius: 8px;
            margin-bottom: 10px;
            border-left: 4px solid #3B82F6;
        }}
        
        .insight-header {{
            font-weight: 600;
            color: #F3F4F6;
            margin-bottom: 5px;
        }}
        
        .insight-text {{
            color: #D1D5DB;
            font-size: 0.9rem;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🚀 NSE Market Intelligence Dashboard</h1>
            <p>Advanced Analytics for Smart Investment Decisions • {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        </div>
        
        <div class="kpi-grid">
            <div class="kpi-card">
                <div class="kpi-value positive" id="high-delivery-count">-</div>
                <div class="kpi-label">High Delivery Stocks (>80%)</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-value neutral" id="momentum-count">-</div>
                <div class="kpi-label">Momentum Stocks (+5% & >70% Del)</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-value negative" id="value-opportunities">-</div>
                <div class="kpi-label">Value Opportunities (Dip + High Del)</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-value neutral" id="market-trend">-</div>
                <div class="kpi-label">Market Trend</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-value positive" id="avg-delivery">-</div>
                <div class="kpi-label">Average Delivery %</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-value neutral" id="top-sector">-</div>
                <div class="kpi-label">Best Performing Sector</div>
            </div>
        </div>
        
        <div class="charts-grid">
            <div class="chart-card">
                <div class="chart-title">💎 Sector Performance Matrix</div>
                <div class="chart-container" id="sector-performance-chart"></div>
            </div>
            
            <div class="chart-card">
                <div class="chart-title">📊 Price Band Analysis</div>
                <div class="chart-container" id="price-band-chart"></div>
            </div>
            
            <div class="chart-card full-width">
                <div class="chart-title">🚀 Top Momentum Stocks (High Price Movement + Strong Delivery)</div>
                <div class="chart-container" id="momentum-stocks-chart"></div>
            </div>
            
            <div class="chart-card full-width">
                <div class="chart-title">💰 Value Opportunities (Price Dip + Strong Fundamentals)</div>
                <div class="chart-container" id="value-opportunities-chart"></div>
            </div>
        </div>
        
        <div class="insights-section">
            <div class="insights-title">🎯 Key Market Insights</div>
            <div id="insights-container"></div>
        </div>
    </div>

    <script>
        // Dashboard data from Python
        const dashboardData = {json.dumps(dashboard_data, indent=2)};
        
        console.log('Dashboard data loaded:', dashboardData);
        
        // Initialize dashboard when DOM is ready
        document.addEventListener('DOMContentLoaded', function() {{
            console.log('Initializing meaningful dashboard...');
            
            updateKPIs();
            createSectorPerformanceChart();
            createPriceBandChart();
            createMomentumStocksChart();
            createValueOpportunitiesChart();
            generateInsights();
        }});
        
        function updateKPIs() {{
            const summary = dashboardData.market_summary;
            
            document.getElementById('high-delivery-count').textContent = summary.high_delivery_count;
            document.getElementById('momentum-count').textContent = summary.momentum_stocks_count;
            document.getElementById('value-opportunities').textContent = summary.value_opportunities_count;
            document.getElementById('market-trend').textContent = summary.market_trend;
            document.getElementById('avg-delivery').textContent = summary.avg_delivery.toFixed(1) + '%';
            document.getElementById('top-sector').textContent = summary.top_sector;
        }}
        
        function createSectorPerformanceChart() {{
            const container = document.getElementById('sector-performance-chart');
            const data = dashboardData.sector_performance;
            
            const plotlyData = [{{
                x: data.avg_delivery,
                y: data.avg_price_change,
                mode: 'markers+text',
                type: 'scatter',
                text: data.sectors,
                textposition: 'middle center',
                marker: {{
                    size: data.stock_count.map(count => Math.max(10, Math.min(50, count/10))),
                    color: data.avg_price_change,
                    colorscale: [[0, '#EF4444'], [0.5, '#F59E0B'], [1, '#10B981']],
                    showscale: true,
                    colorbar: {{
                        title: 'Price Change %',
                        titlefont: {{ color: '#E0E0E0' }},
                        tickfont: {{ color: '#E0E0E0' }}
                    }}
                }},
                hovertemplate: '<b>%{{text}}</b><br>Avg Delivery: %{{x:.1f}}%<br>Avg Price Change: %{{y:.1f}}%<br>Stocks: %{{customdata}}<extra></extra>',
                customdata: data.stock_count
            }}];
            
            const layout = {{
                font: {{ color: '#E0E0E0', family: 'Poppins' }},
                paper_bgcolor: 'transparent',
                plot_bgcolor: 'transparent',
                xaxis: {{ 
                    title: 'Average Delivery %',
                    gridcolor: '#374151',
                    titlefont: {{ color: '#E0E0E0' }},
                    tickfont: {{ color: '#E0E0E0' }}
                }},
                yaxis: {{ 
                    title: 'Average Price Change %',
                    gridcolor: '#374151',
                    titlefont: {{ color: '#E0E0E0' }},
                    tickfont: {{ color: '#E0E0E0' }}
                }},
                margin: {{ t: 10, b: 50, l: 60, r: 10 }}
            }};
            
            Plotly.newPlot(container, plotlyData, layout, {{ responsive: true, displayModeBar: false }});
        }}
        
        function createPriceBandChart() {{
            const container = document.getElementById('price-band-chart');
            const data = dashboardData.price_band_analysis;
            
            const plotlyData = [{{
                x: data.bands,
                y: data.avg_delivery,
                type: 'bar',
                name: 'Avg Delivery %',
                marker: {{ color: '#3B82F6' }},
                yaxis: 'y',
                hovertemplate: '<b>%{{x}}</b><br>Avg Delivery: %{{y:.1f}}%<br>Stocks: %{{customdata}}<extra></extra>',
                customdata: data.stock_count
            }}, {{
                x: data.bands,
                y: data.avg_price_change,
                type: 'scatter',
                mode: 'lines+markers',
                name: 'Avg Price Change %',
                line: {{ color: '#10B981', width: 3 }},
                marker: {{ color: '#10B981', size: 8 }},
                yaxis: 'y2',
                hovertemplate: '<b>%{{x}}</b><br>Avg Price Change: %{{y:.1f}}%<extra></extra>'
            }}];
            
            const layout = {{
                font: {{ color: '#E0E0E0', family: 'Poppins' }},
                paper_bgcolor: 'transparent',
                plot_bgcolor: 'transparent',
                xaxis: {{ 
                    title: 'Price Bands',
                    gridcolor: '#374151',
                    titlefont: {{ color: '#E0E0E0' }},
                    tickfont: {{ color: '#E0E0E0' }}
                }},
                yaxis: {{ 
                    title: 'Average Delivery %',
                    gridcolor: '#374151',
                    titlefont: {{ color: '#E0E0E0' }},
                    tickfont: {{ color: '#E0E0E0' }},
                    side: 'left'
                }},
                yaxis2: {{
                    title: 'Average Price Change %',
                    titlefont: {{ color: '#10B981' }},
                    tickfont: {{ color: '#10B981' }},
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
        
        function createMomentumStocksChart() {{
            const container = document.getElementById('momentum-stocks-chart');
            const data = dashboardData.momentum_stocks;
            
            if (data.symbols.length === 0) {{
                container.innerHTML = '<div style="text-align: center; padding: 50px; color: #9CA3AF;">No momentum stocks found with current criteria</div>';
                return;
            }}
            
            const plotlyData = [{{
                x: data.symbols,
                y: data.price_changes,
                type: 'bar',
                marker: {{
                    color: data.price_changes,
                    colorscale: [[0, '#F59E0B'], [1, '#10B981']],
                    showscale: false
                }},
                hovertemplate: '<b>%{{x}}</b><br>Price Change: %{{y:.1f}}%<br>Delivery: %{{customdata:.1f}}%<extra></extra>',
                customdata: data.delivery_pcts
            }}];
            
            const layout = {{
                font: {{ color: '#E0E0E0', family: 'Poppins' }},
                paper_bgcolor: 'transparent',
                plot_bgcolor: 'transparent',
                xaxis: {{ 
                    title: 'Stocks',
                    gridcolor: '#374151',
                    titlefont: {{ color: '#E0E0E0' }},
                    tickfont: {{ color: '#E0E0E0', size: 10 }},
                    tickangle: -45
                }},
                yaxis: {{ 
                    title: 'Price Change %',
                    gridcolor: '#374151',
                    titlefont: {{ color: '#E0E0E0' }},
                    tickfont: {{ color: '#E0E0E0' }}
                }},
                margin: {{ t: 10, b: 100, l: 60, r: 10 }}
            }};
            
            Plotly.newPlot(container, plotlyData, layout, {{ responsive: true, displayModeBar: false }});
        }}
        
        function createValueOpportunitiesChart() {{
            const container = document.getElementById('value-opportunities-chart');
            const data = dashboardData.value_opportunities;
            
            if (data.symbols.length === 0) {{
                container.innerHTML = '<div style="text-align: center; padding: 50px; color: #9CA3AF;">No value opportunities found with current criteria</div>';
                return;
            }}
            
            const plotlyData = [{{
                x: data.delivery_pcts,
                y: data.price_changes,
                mode: 'markers+text',
                type: 'scatter',
                text: data.symbols,
                textposition: 'top center',
                marker: {{
                    size: data.close_prices.map(price => Math.max(8, Math.min(20, price/100))),
                    color: '#EF4444',
                    line: {{ color: '#FFFFFF', width: 1 }}
                }},
                hovertemplate: '<b>%{{text}}</b><br>Delivery: %{{x:.1f}}%<br>Price Change: %{{y:.1f}}%<br>Price: ₹%{{customdata:.0f}}<extra></extra>',
                customdata: data.close_prices
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
                    title: 'Price Change % (Negative = Opportunity)',
                    gridcolor: '#374151',
                    titlefont: {{ color: '#E0E0E0' }},
                    tickfont: {{ color: '#E0E0E0' }}
                }},
                margin: {{ t: 30, b: 50, l: 80, r: 10 }}
            }};
            
            Plotly.newPlot(container, plotlyData, layout, {{ responsive: true, displayModeBar: false }});
        }}
        
        function generateInsights() {{
            const container = document.getElementById('insights-container');
            const summary = dashboardData.market_summary;
            const sectorData = dashboardData.sector_performance;
            
            let insights = [];
            
            // Market trend insight
            if (summary.market_trend === 'Bullish') {{
                insights.push({{
                    header: '📈 Bullish Market Sentiment',
                    text: `Market is showing positive momentum. Consider momentum stocks and avoid contrarian plays.`
                }});
            }} else {{
                insights.push({{
                    header: '📉 Bearish Market Sentiment',
                    text: `Market is in correction mode. Focus on value opportunities and high-delivery stocks.`
                }});
            }}
            
            // High delivery insight
            if (summary.high_delivery_count > 0) {{
                insights.push({{
                    header: '💎 Strong Fundamental Stocks',
                    text: `${{summary.high_delivery_count}} stocks show >80% delivery, indicating strong institutional interest and reduced speculation.`
                }});
            }}
            
            // Momentum insight
            if (summary.momentum_stocks_count > 0) {{
                insights.push({{
                    header: '🚀 Momentum Opportunities',
                    text: `${{summary.momentum_stocks_count}} stocks combine price momentum (+5%) with strong delivery (>70%). Good for trend following strategies.`
                }});
            }}
            
            // Value opportunities
            if (summary.value_opportunities_count > 0) {{
                insights.push({{
                    header: '💰 Value Opportunities',
                    text: `${{summary.value_opportunities_count}} quality stocks are available at discount (price dip + high delivery). Consider for contrarian investing.`
                }});
            }}
            
            // Sector insight
            insights.push({{
                header: '🏆 Sector Leadership',
                text: `${{summary.top_sector}} sector is leading with highest delivery percentage, indicating institutional preference.`
            }});
            
            // Render insights
            container.innerHTML = insights.map(insight => `
                <div class="insight-item">
                    <div class="insight-header">${{insight.header}}</div>
                    <div class="insight-text">${{insight.text}}</div>
                </div>
            `).join('');
        }}
        
    </script>
</body>
</html>
'''
    
    # Save dashboard
    dashboard_path = 'dashboard/meaningful_market_dashboard.html'
    with open(dashboard_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"Meaningful Market Dashboard created: {dashboard_path}")
    print(f"\nKey Insights:")
    print(f"- High Delivery Stocks (>80%): {dashboard_data['market_summary']['high_delivery_count']}")
    print(f"- Momentum Stocks: {dashboard_data['market_summary']['momentum_stocks_count']}")
    print(f"- Value Opportunities: {dashboard_data['market_summary']['value_opportunities_count']}")
    print(f"- Market Trend: {dashboard_data['market_summary']['market_trend']}")
    print(f"- Best Sector: {dashboard_data['market_summary']['top_sector']}")
    
    return dashboard_path

if __name__ == "__main__":
    create_meaningful_dashboard()