import pyodbc
import pandas as pd
import json
from datetime import datetime

def create_market_overview_dashboard():
    """Create a focused Market Overview dashboard"""
    
    # Database configuration
    server = 'SRIKIRANREDDY\\SQLEXPRESS'
    database = 'master'  # The table is actually in master database
    
    # Connect to database
    conn = pyodbc.connect(f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes')
    
    # Fetch data
    query = '''
    SELECT 
        symbol,
        current_deliv_per as delivery_percentage,
        ((current_close_price - current_prev_close) / current_prev_close) * 100 as price_change_pct,
        current_ttl_trd_qnty as total_traded_volume,
        current_turnover_lacs as total_turnover,
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
    
    # Group by sector for treemap
    sector_summary = df.groupby('sector').agg({
        'total_turnover': 'sum',
        'delivery_percentage': 'mean',
        'price_change_pct': 'mean',
        'symbol': 'count'
    }).round(2)
    
    print(f"Sector breakdown:")
    print(sector_summary)
    
    # Prepare data for JavaScript
    market_data = {
        'symbols': df['symbol'].tolist()[:50],  # Top 50 for performance
        'delivery_pcts': df['delivery_percentage'].tolist()[:50],
        'price_changes': df['price_change_pct'].tolist()[:50],
        'volumes': df['total_traded_volume'].tolist()[:50],
        'sectors': df['sector'].tolist()[:50],
        'sector_summary': {
            'names': sector_summary.index.tolist(),
            'turnovers': sector_summary['total_turnover'].tolist(),
            'avg_delivery': sector_summary['delivery_percentage'].tolist(),
            'avg_price_change': sector_summary['price_change_pct'].tolist(),
            'count': sector_summary['symbol'].tolist()
        }
    }
    
    # Create HTML dashboard
    html_content = f'''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Market Overview Dashboard</title>
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
        }}
        
        .container {{
            max-width: 1400px;
            margin: 0 auto;
            padding: 20px;
        }}
        
        .header {{
            text-align: center;
            margin-bottom: 30px;
            background: linear-gradient(135deg, #2C2C2C, #3C3C3C);
            padding: 20px;
            border-radius: 15px;
            border: 1px solid #404040;
        }}
        
        .header h1 {{
            color: #00B0FF;
            font-size: 2.5rem;
            font-weight: 700;
            margin-bottom: 10px;
        }}
        
        .header p {{
            color: #B0B0B0;
            font-size: 1.1rem;
        }}
        
        .charts-grid {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 20px;
            margin-bottom: 30px;
        }}
        
        .chart-card {{
            background: #2C2C2C;
            border-radius: 15px;
            padding: 20px;
            border: 1px solid #404040;
            transition: transform 0.3s ease, box-shadow 0.3s ease;
        }}
        
        .chart-card:hover {{
            transform: translateY(-5px);
            box-shadow: 0 10px 25px rgba(0, 176, 255, 0.1);
        }}
        
        .chart-title {{
            font-size: 1.3rem;
            font-weight: 600;
            color: #00B0FF;
            margin-bottom: 15px;
            text-align: center;
        }}
        
        .chart-container {{
            height: 400px;
            position: relative;
        }}
        
        .full-width {{
            grid-column: 1 / -1;
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
            margin-right: 10px;
        }}
        
        @keyframes spin {{
            0% {{ transform: rotate(0deg); }}
            100% {{ transform: rotate(360deg); }}
        }}
        
        .stats-row {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            margin-bottom: 20px;
        }}
        
        .stat-card {{
            background: #2C2C2C;
            padding: 20px;
            border-radius: 10px;
            text-align: center;
            border: 1px solid #404040;
        }}
        
        .stat-value {{
            font-size: 2rem;
            font-weight: 700;
            color: #00C853;
            margin-bottom: 5px;
        }}
        
        .stat-label {{
            font-size: 0.9rem;
            color: #B0B0B0;
            text-transform: uppercase;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Market Overview Dashboard</h1>
            <p>Professional Market Analytics • Updated {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        </div>
        
        <div class="stats-row">
            <div class="stat-card">
                <div class="stat-value" id="total-symbols">-</div>
                <div class="stat-label">Active Symbols</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" id="avg-delivery">-</div>
                <div class="stat-label">Avg Delivery %</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" id="positive-moves">-</div>
                <div class="stat-label">Positive Moves</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" id="total-volume">-</div>
                <div class="stat-label">Total Volume (Cr)</div>
            </div>
        </div>
        
        <div class="charts-grid">
            <div class="chart-card">
                <div class="chart-title">Sector Distribution (Treemap)</div>
                <div class="chart-container" id="sector-treemap">
                    <div class="loading">
                        <div class="spinner"></div>
                        Loading Treemap...
                    </div>
                </div>
            </div>
            
            <div class="chart-card">
                <div class="chart-title">Delivery % vs Price Change</div>
                <div class="chart-container" id="scatter-chart">
                    <div class="loading">
                        <div class="spinner"></div>
                        Loading Scatter Plot...
                    </div>
                </div>
            </div>
            
            <div class="chart-card full-width">
                <div class="chart-title">Top 20 Symbols by Delivery %</div>
                <div class="chart-container" id="bar-chart">
                    <div class="loading">
                        <div class="spinner"></div>
                        Loading Bar Chart...
                    </div>
                </div>
            </div>
        </div>
    </div>

    <script>
        // Market data from Python
        const marketData = {json.dumps(market_data, indent=2)};
        
        console.log('Market data loaded:', marketData);
        
        // Initialize dashboard when DOM is ready
        document.addEventListener('DOMContentLoaded', function() {{
            console.log('DOM loaded, initializing charts...');
            
            updateStats();
            
            // Debug market data
            console.log('Full market data:', marketData);
            console.log('Sector summary:', marketData.sector_summary);
            
            setTimeout(() => {{
                console.log('Attempting to create sector treemap...');
                createSectorTreemap();
            }}, 100);
            
            setTimeout(() => {{
                // Check if treemap loaded, if not create a simple bar chart
                const treemapContainer = document.getElementById('sector-treemap');
                if (treemapContainer && treemapContainer.innerHTML.includes('Error')) {{
                    console.log('Treemap failed, creating fallback bar chart...');
                    createSectorBarChart();
                }}
            }}, 2000);
            
            setTimeout(() => {{
                createScatterPlot();
            }}, 500);
            
            setTimeout(() => {{
                createBarChart();
            }}, 1000);
        }});
        
        function updateStats() {{
            try {{
                const data = marketData;
                
                document.getElementById('total-symbols').textContent = data.symbols.length;
                
                const avgDelivery = data.delivery_pcts.reduce((a, b) => a + b, 0) / data.delivery_pcts.length;
                document.getElementById('avg-delivery').textContent = avgDelivery.toFixed(1) + '%';
                
                const positiveMoves = data.price_changes.filter(p => p > 0).length;
                document.getElementById('positive-moves').textContent = positiveMoves;
                
                const totalVolume = data.volumes.reduce((a, b) => a + b, 0) / 10000000; // Convert to crores
                document.getElementById('total-volume').textContent = totalVolume.toFixed(0);
                
            }} catch (error) {{
                console.error('Error updating stats:', error);
            }}
        }}
        
        function createSectorTreemap() {{
            try {{
                const container = document.getElementById('sector-treemap');
                
                // Clear any existing content
                container.innerHTML = '';
                
                // Use simple hardcoded data first to test
                const testData = [{{
                    type: 'treemap',
                    labels: ['Others', 'Banking', 'IT', 'FMCG', 'Pharma', 'Energy', 'Auto'],
                    parents: ['', '', '', '', '', '', ''],
                    values: [31212, 388, 47, 46, 54, 43, 40],
                    textinfo: 'label+value',
                    textfont: {{ color: '#FFFFFF', size: 12 }},
                    marker: {{
                        colors: ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FECA57', '#FF9FF3', '#54A0FF'],
                        line: {{ color: '#2C2C2C', width: 2 }}
                    }},
                    hovertemplate: '<b>%{{label}}</b><br>Symbols: %{{value}}<extra></extra>'
                }}];
                
                const layout = {{
                    font: {{ color: '#E0E0E0', family: 'Poppins' }},
                    paper_bgcolor: 'transparent',
                    plot_bgcolor: 'transparent',
                    margin: {{ t: 10, b: 10, l: 10, r: 10 }}
                }};
                
                console.log('Creating treemap with test data...');
                
                Plotly.newPlot(container, testData, layout, {{
                    responsive: true,
                    displayModeBar: false
                }}).then(() => {{
                    console.log('Treemap created successfully with test data');
                }}).catch(error => {{
                    console.error('Treemap error:', error);
                    container.innerHTML = '<div style="color: #D50000; text-align: center; padding: 50px; font-size: 14px;">Treemap Error: ' + error.message + '</div>';
                }});
                
            }} catch (error) {{
                console.error('Error in createSectorTreemap:', error);
                const container = document.getElementById('sector-treemap');
                if (container) {{
                    container.innerHTML = '<div style="color: #D50000; text-align: center; padding: 50px; font-size: 14px;">JavaScript Error: ' + error.message + '</div>';
                }}
            }}
        }}
        
        function createSectorBarChart() {{
            try {{
                const container = document.getElementById('sector-treemap');
                container.innerHTML = '<div style="color: #00B0FF; text-align: center; margin-bottom: 10px; font-size: 14px;">Sector Distribution (Fallback Chart)</div>';
                
                const plotlyData = [{{
                    x: ['Others', 'Banking', 'IT', 'FMCG', 'Pharma', 'Energy', 'Auto'],
                    y: [31212, 388, 47, 46, 54, 43, 40],
                    type: 'bar',
                    marker: {{
                        color: ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FECA57', '#FF9FF3', '#54A0FF']
                    }},
                    hovertemplate: '<b>%{{x}}</b><br>Symbols: %{{y}}<extra></extra>'
                }}];
                
                const layout = {{
                    font: {{ color: '#E0E0E0', family: 'Poppins' }},
                    paper_bgcolor: 'transparent',
                    plot_bgcolor: 'transparent',
                    xaxis: {{ 
                        gridcolor: '#404040',
                        titlefont: {{ color: '#E0E0E0' }},
                        tickfont: {{ color: '#E0E0E0', size: 10 }}
                    }},
                    yaxis: {{ 
                        title: 'Symbol Count',
                        gridcolor: '#404040',
                        titlefont: {{ color: '#E0E0E0' }},
                        tickfont: {{ color: '#E0E0E0' }}
                    }},
                    margin: {{ t: 20, b: 40, l: 60, r: 10 }}
                }};
                
                Plotly.newPlot(container, plotlyData, layout, {{
                    responsive: true,
                    displayModeBar: false
                }}).then(() => {{
                    console.log('Sector bar chart created successfully');
                }});
                
            }} catch (error) {{
                console.error('Error in createSectorBarChart:', error);
            }}
        }}
        
        function createScatterPlot() {{
            try {{
                const container = document.getElementById('scatter-chart');
                
                const plotlyData = [{{
                    x: marketData.delivery_pcts,
                    y: marketData.price_changes,
                    mode: 'markers',
                    type: 'scatter',
                    text: marketData.symbols,
                    marker: {{
                        size: 8,
                        color: marketData.price_changes,
                        colorscale: [[0, '#D50000'], [0.5, '#FFC107'], [1, '#00C853']],
                        showscale: true,
                        colorbar: {{
                            title: 'Price Change %',
                            titlefont: {{ color: '#E0E0E0' }},
                            tickfont: {{ color: '#E0E0E0' }}
                        }}
                    }},
                    hovertemplate: '<b>%{{text}}</b><br>Delivery: %{{x:.1f}}%<br>Price Change: %{{y:.1f}}%<extra></extra>'
                }}];
                
                const layout = {{
                    font: {{ color: '#E0E0E0', family: 'Poppins' }},
                    paper_bgcolor: 'transparent',
                    plot_bgcolor: 'transparent',
                    xaxis: {{ 
                        title: 'Delivery %',
                        gridcolor: '#404040',
                        titlefont: {{ color: '#E0E0E0' }},
                        tickfont: {{ color: '#E0E0E0' }}
                    }},
                    yaxis: {{ 
                        title: 'Price Change %',
                        gridcolor: '#404040',
                        titlefont: {{ color: '#E0E0E0' }},
                        tickfont: {{ color: '#E0E0E0' }}
                    }},
                    margin: {{ t: 10, b: 40, l: 50, r: 10 }}
                }};
                
                Plotly.newPlot(container, plotlyData, layout, {{
                    responsive: true,
                    displayModeBar: false
                }}).then(() => {{
                    console.log('Scatter plot created successfully');
                    // Remove loading indicator
                    const loadingDiv = container.querySelector('.loading');
                    if (loadingDiv) loadingDiv.remove();
                }}).catch(error => {{
                    console.error('Scatter plot error:', error);
                    container.innerHTML = '<div style="color: #D50000; text-align: center; padding: 50px;">Error loading scatter plot</div>';
                }});
                
            }} catch (error) {{
                console.error('Error in createScatterPlot:', error);
            }}
        }}
        
        function createBarChart() {{
            try {{
                const container = document.getElementById('bar-chart');
                
                // Get top 20 symbols by delivery percentage
                const top20Indices = marketData.delivery_pcts
                    .map((val, idx) => [val, idx])
                    .sort((a, b) => b[0] - a[0])
                    .slice(0, 20)
                    .map(item => item[1]);
                
                const top20Symbols = top20Indices.map(i => marketData.symbols[i]);
                const top20Delivery = top20Indices.map(i => marketData.delivery_pcts[i]);
                const top20Changes = top20Indices.map(i => marketData.price_changes[i]);
                
                const plotlyData = [{{
                    x: top20Symbols,
                    y: top20Delivery,
                    type: 'bar',
                    marker: {{
                        color: top20Changes,
                        colorscale: [[0, '#D50000'], [0.5, '#FFC107'], [1, '#00C853']],
                        showscale: false
                    }},
                    hovertemplate: '<b>%{{x}}</b><br>Delivery: %{{y:.1f}}%<br>Price Change: %{{customdata:.1f}}%<extra></extra>',
                    customdata: top20Changes
                }}];
                
                const layout = {{
                    font: {{ color: '#E0E0E0', family: 'Poppins' }},
                    paper_bgcolor: 'transparent',
                    plot_bgcolor: 'transparent',
                    xaxis: {{ 
                        title: 'Symbol',
                        gridcolor: '#404040',
                        titlefont: {{ color: '#E0E0E0' }},
                        tickfont: {{ color: '#E0E0E0', size: 10 }},
                        tickangle: -45
                    }},
                    yaxis: {{ 
                        title: 'Delivery %',
                        gridcolor: '#404040',
                        titlefont: {{ color: '#E0E0E0' }},
                        tickfont: {{ color: '#E0E0E0' }}
                    }},
                    margin: {{ t: 10, b: 100, l: 50, r: 10 }}
                }};
                
                Plotly.newPlot(container, plotlyData, layout, {{
                    responsive: true,
                    displayModeBar: false
                }}).then(() => {{
                    console.log('Bar chart created successfully');
                    // Remove loading indicator
                    const loadingDiv = container.querySelector('.loading');
                    if (loadingDiv) loadingDiv.remove();
                }}).catch(error => {{
                    console.error('Bar chart error:', error);
                    container.innerHTML = '<div style="color: #D50000; text-align: center; padding: 50px;">Error loading bar chart</div>';
                }});
                
            }} catch (error) {{
                console.error('Error in createBarChart:', error);
            }}
        }}
        
    </script>
</body>
</html>
'''
    
    # Save dashboard
    dashboard_path = 'dashboard/market_overview_dashboard.html'
    with open(dashboard_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"Market Overview Dashboard created: {dashboard_path}")
    return dashboard_path

if __name__ == "__main__":
    create_market_overview_dashboard()