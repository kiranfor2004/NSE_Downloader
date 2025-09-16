#!/usr/bin/env python3
"""
📊 PROFESSIONAL MARKET ANALYTICS DASHBOARD 📊
============================================

Exact Specifications Implementation:
- Dark Theme: #1A1A1A background, #2C2C2C cards
- Color Palette: #00C853 (positive), #D50000 (negative), #00B0FF (accent)
- Typography: Poppins/Inter with clear hierarchy
- Three Strategic Tabs: Market Overview, Symbol Analysis, Category Performance

Professional Financial Dashboard for Institutional Trading
"""

import pyodbc
import json
import numpy as np
import pandas as pd
from datetime import datetime, date, timedelta
import webbrowser
import os
import decimal
import math
from collections import defaultdict

class ProfessionalMarketDashboard:
    def __init__(self):
        self.connection_string = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=SRIKIRANREDDY\\SQLEXPRESS;DATABASE=master;Trusted_Connection=yes;'
        self.data = None
        self.market_analytics = {}
        
    def connect_and_fetch_data(self):
        """Fetch comprehensive market data"""
        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            
            query = """
            SELECT 
                symbol,
                current_trade_date,
                current_close_price,
                current_open_price,
                current_high_price,
                current_low_price,
                current_last_price,
                current_avg_price,
                previous_close_price,
                previous_open_price,
                previous_high_price,
                previous_low_price,
                previous_last_price,
                previous_avg_price,
                current_deliv_qty,
                previous_deliv_qty,
                delivery_increase_abs,
                current_deliv_per,
                previous_deliv_per,
                current_ttl_trd_qnty,
                previous_ttl_trd_qnty,
                current_turnover_lacs,
                previous_turnover_lacs,
                current_no_of_trades,
                previous_no_of_trades,
                index_name,
                category,
                comparison_type,
                previous_baseline_date
            FROM step03_compare_monthvspreviousmonth 
            WHERE current_deliv_qty > 0 
                AND previous_deliv_qty > 0
                AND current_close_price > 0
                AND previous_close_price > 0
            ORDER BY current_turnover_lacs DESC
            """
            
            cursor.execute(query)
            columns = [column[0] for column in cursor.description]
            rows = cursor.fetchall()
            
            self.data = []
            for row in rows:
                record = {}
                for i, value in enumerate(row):
                    if isinstance(value, (datetime, date)):
                        record[columns[i]] = value.strftime('%Y-%m-%d')
                    elif isinstance(value, decimal.Decimal):
                        record[columns[i]] = float(value)
                    elif value is None:
                        record[columns[i]] = None
                    else:
                        record[columns[i]] = value
                self.data.append(record)
            
            conn.close()
            print(f"✅ Loaded {len(self.data):,} market records!")
            return True
            
        except Exception as e:
            print(f"❌ Database error: {e}")
            return False

    def calculate_market_analytics(self):
        """Calculate comprehensive market analytics"""
        print("📊 Computing market analytics...")
        
        df = pd.DataFrame(self.data)
        
        # Calculate enhanced metrics for each stock
        for i, record in enumerate(self.data):
            # Core calculations
            price_change_pct = ((record['current_close_price'] - record['previous_close_price']) / 
                              record['previous_close_price'] * 100) if record['previous_close_price'] > 0 else 0
            
            delivery_change_pct = ((record['current_deliv_qty'] - record['previous_deliv_qty']) / 
                                 record['previous_deliv_qty'] * 100) if record['previous_deliv_qty'] > 0 else 0
            
            volume_change_pct = ((record['current_ttl_trd_qnty'] - record['previous_ttl_trd_qnty']) / 
                               record['previous_ttl_trd_qnty'] * 100) if record['previous_ttl_trd_qnty'] > 0 else 0
            
            turnover_change_pct = ((record['current_turnover_lacs'] - record['previous_turnover_lacs']) / 
                                 record['previous_turnover_lacs'] * 100) if record['previous_turnover_lacs'] > 0 else 0
            
            # Market efficiency metrics
            delivery_to_turnover_ratio = (record['current_deliv_per'] / 100) if record['current_deliv_per'] > 0 else 0
            
            # Volatility analysis
            daily_volatility = ((record['current_high_price'] - record['current_low_price']) / 
                              record['current_close_price'] * 100) if record['current_close_price'] > 0 else 0
            
            # Trading intensity
            avg_trade_size = (record['current_turnover_lacs'] * 100000 / record['current_no_of_trades']) if record['current_no_of_trades'] > 0 else 0
            
            # Update record with calculated metrics
            self.data[i].update({
                'price_change_pct': round(price_change_pct, 2),
                'delivery_change_pct': round(delivery_change_pct, 2),
                'volume_change_pct': round(volume_change_pct, 2),
                'turnover_change_pct': round(turnover_change_pct, 2),
                'delivery_to_turnover_ratio': round(delivery_to_turnover_ratio, 4),
                'daily_volatility': round(daily_volatility, 2),
                'avg_trade_size_lacs': round(avg_trade_size / 100000, 2),
                'delivery_value_cr': round(record['current_deliv_qty'] * record['current_avg_price'] / 10000000, 2)
            })
        
        # Calculate market-wide KPIs
        self.calculate_market_kpis()
        
    def calculate_market_kpis(self):
        """Calculate market-wide KPIs for dashboard"""
        df = pd.DataFrame(self.data)
        
        # Tab 1: Market Overview KPIs
        total_delivery_increase_lacs = df['delivery_increase_abs'].sum() / 100000  # Convert to lakhs
        stocks_with_positive_delivery = len(df[df['delivery_increase_abs'] > 0])
        market_delivery_turnover_ratio = df['delivery_to_turnover_ratio'].mean()
        avg_daily_turnover = df['current_turnover_lacs'].mean()
        
        # Index performance analysis
        index_performance = {}
        for index_name in df['index_name'].dropna().unique():
            index_data = df[df['index_name'] == index_name]
            index_performance[index_name] = {
                'total_stocks': len(index_data),
                'avg_delivery_change': index_data['delivery_change_pct'].mean(),
                'total_turnover': index_data['current_turnover_lacs'].sum(),
                'positive_delivery_stocks': len(index_data[index_data['delivery_increase_abs'] > 0])
            }
        
        # Category performance analysis
        category_performance = {}
        for category in df['category'].dropna().unique():
            cat_data = df[df['category'] == category]
            category_performance[category] = {
                'stock_count': len(cat_data),
                'total_stocks': len(cat_data),  # For backward compatibility
                'avg_delivery_change': cat_data['delivery_change_pct'].mean(),
                'total_turnover': cat_data['current_turnover_lacs'].sum(),
                'positive_delivery_count': len(cat_data[cat_data['delivery_increase_abs'] > 0]),
                'avg_volatility': cat_data['daily_volatility'].mean()
            }
        
        # Best performers
        best_performing_index = max(index_performance.items(), 
                                  key=lambda x: x[1]['avg_delivery_change'])[0] if index_performance else "N/A"
        best_performing_category = max(category_performance.items(), 
                                     key=lambda x: x[1]['avg_delivery_change'])[0] if category_performance else "N/A"
        
        # Top stocks for different visualizations
        top_turnover_stocks = df.nlargest(50, 'current_turnover_lacs')
        top_delivery_stocks = df.nlargest(30, 'delivery_increase_abs')
        
        self.market_analytics = {
            'market_overview': {
                'total_delivery_increase_lacs': round(total_delivery_increase_lacs, 1),
                'stocks_with_positive_delivery': stocks_with_positive_delivery,
                'market_delivery_turnover_ratio': round(market_delivery_turnover_ratio, 4),
                'avg_daily_turnover': round(avg_daily_turnover, 1)
            },
            'index_performance': index_performance,
            'category_performance': category_performance,
            'best_performing_index': best_performing_index,
            'best_performing_category': best_performing_category,
            'total_symbols_analyzed': len(df),
            'top_turnover_stocks': top_turnover_stocks.to_dict('records'),
            'top_delivery_stocks': top_delivery_stocks.to_dict('records'),
            'market_summary': {
                'total_stocks': len(df),
                'avg_price_change': df['price_change_pct'].mean(),
                'avg_delivery_change': df['delivery_change_pct'].mean(),
                'total_market_turnover': df['current_turnover_lacs'].sum()
            }
        }

    def generate_professional_dashboard(self):
        """Generate the professional market dashboard"""
        
        market_kpis = self.market_analytics['market_overview']
        
        # Prepare data for JavaScript
        data_json = json.dumps(self.data[:200])  # Limit for performance
        analytics_json = json.dumps(self.market_analytics)
        
        html_content = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>📊 Professional Market Analytics Dashboard</title>
    
    <!-- Professional Chart Libraries -->
    <script src="https://d3js.org/d3.v7.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.min.js"></script>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <script src="https://unpkg.com/three@0.155.0/build/three.min.js"></script>
    
    <!-- UI Libraries -->
    <script src="https://cdnjs.cloudflare.com/ajax/libs/gsap/3.12.2/gsap.min.js"></script>
    <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css" rel="stylesheet">
    
    <!-- Professional Typography -->
    <link href="https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;500;600;700;800&family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">
    
    <style>
        /* Exact Color Palette as Specified */
        :root {{
            /* Main Colors */
            --bg-primary: #1A1A1A;        /* Main Background */
            --bg-card: #2C2C2C;           /* Card & Tab Background */
            --text-primary: #E0E0E0;      /* Text Color */
            
            /* Accent Colors */
            --accent-positive: #00C853;   /* Positive Trends */
            --accent-negative: #D50000;   /* Negative Trends */
            --accent-primary: #00B0FF;    /* Primary Accent */
            
            /* Supporting Colors */
            --border-color: #404040;
            --hover-color: #383838;
            --shadow-color: rgba(0, 0, 0, 0.5);
        }}

        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}

        body {{
            font-family: 'Poppins', 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
            background: var(--bg-primary);
            color: var(--text-primary);
            line-height: 1.6;
            overflow-x: hidden;
        }}

        /* Professional Header */
        .dashboard-header {{
            background: var(--bg-card);
            padding: 1.5rem 2rem;
            border-bottom: 2px solid var(--border-color);
            box-shadow: 0 4px 20px var(--shadow-color);
        }}

        .header-content {{
            max-width: 1400px;
            margin: 0 auto;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}

        .logo-section {{
            display: flex;
            align-items: center;
            gap: 1rem;
        }}

        .logo {{
            font-size: 24px;                    /* Main Heading Size */
            font-weight: 700;
            color: var(--accent-primary);
            font-family: 'Poppins', sans-serif;
        }}

        .market-status {{
            display: flex;
            align-items: center;
            gap: 2rem;
            font-size: 14px;                    /* Body Text Size */
        }}

        .status-indicator {{
            display: flex;
            align-items: center;
            gap: 0.5rem;
            color: var(--text-primary);
        }}

        .status-dot {{
            width: 8px;
            height: 8px;
            border-radius: 50%;
            background: var(--accent-positive);
            animation: pulse 2s infinite;
        }}

        @keyframes pulse {{
            0%, 100% {{ opacity: 1; }}
            50% {{ opacity: 0.5; }}
        }}

        /* Navigation Tabs */
        .tab-navigation {{
            background: var(--bg-card);
            padding: 1rem 2rem;
            border-bottom: 1px solid var(--border-color);
        }}

        .tab-container {{
            max-width: 1400px;
            margin: 0 auto;
            display: flex;
            gap: 1rem;
        }}

        .tab-button {{
            padding: 0.8rem 1.5rem;
            background: transparent;
            border: 2px solid var(--border-color);
            border-radius: 8px;
            color: var(--text-primary);
            font-size: 16px;                    /* Medium Size */
            font-weight: 500;
            font-family: 'Poppins', sans-serif;
            cursor: pointer;
            transition: all 0.3s ease;
        }}

        .tab-button:hover {{
            border-color: var(--accent-primary);
            background: rgba(0, 176, 255, 0.1);
        }}

        .tab-button.active {{
            background: var(--accent-primary);
            border-color: var(--accent-primary);
            color: var(--bg-primary);
            font-weight: 600;
        }}

        /* Main Container */
        .main-container {{
            max-width: 1400px;
            margin: 0 auto;
            padding: 2rem;
        }}

        /* KPI Cards */
        .kpi-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 1.5rem;
            margin-bottom: 2rem;
        }}

        .kpi-card {{
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            border-radius: 12px;
            padding: 1.5rem;
            transition: all 0.3s ease;
            position: relative;
            overflow: hidden;
        }}

        .kpi-card::before {{
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            height: 3px;
            background: var(--accent-primary);
            transform: scaleX(0);
            transition: transform 0.3s ease;
        }}

        .kpi-card:hover {{
            background: var(--hover-color);
            border-color: var(--accent-primary);
            transform: translateY(-2px);
            box-shadow: 0 8px 25px var(--shadow-color);
        }}

        .kpi-card:hover::before {{
            transform: scaleX(1);
        }}

        .kpi-header {{
            display: flex;
            justify-content: between;
            align-items: center;
            margin-bottom: 1rem;
        }}

        .kpi-icon {{
            font-size: 1.5rem;
            color: var(--accent-primary);
        }}

        .kpi-value {{
            font-size: 2.2rem;
            font-weight: 700;
            color: var(--text-primary);
            font-family: 'Inter', monospace;
            margin-bottom: 0.5rem;
        }}

        .kpi-label {{
            font-size: 18px;                    /* Subheading Size */
            font-weight: 600;
            color: var(--text-primary);
            margin-bottom: 0.25rem;
        }}

        .kpi-subtitle {{
            font-size: 14px;                    /* Body Text Size */
            color: #B0B0B0;
        }}

        .kpi-change {{
            font-size: 16px;
            font-weight: 600;
            margin-top: 0.5rem;
        }}

        .positive {{ color: var(--accent-positive); }}
        .negative {{ color: var(--accent-negative); }}
        .neutral {{ color: #999; }}

        /* Tab Content */
        .tab-content {{
            display: none;
        }}

        .tab-content.active {{
            display: block;
            animation: fadeIn 0.5s ease;
        }}

        @keyframes fadeIn {{
            from {{ opacity: 0; transform: translateY(10px); }}
            to {{ opacity: 1; transform: translateY(0); }}
        }}

        /* Chart Containers */
        .chart-grid {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 2rem;
            margin-bottom: 2rem;
        }}

        .chart-container {{
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            border-radius: 12px;
            padding: 1.5rem;
            min-height: 400px;
        }}

        .chart-title {{
            font-size: 18px;                    /* Subheading Size */
            font-weight: 600;
            color: var(--text-primary);
            margin-bottom: 1rem;
            padding-bottom: 0.5rem;
            border-bottom: 2px solid var(--accent-primary);
        }}

        .full-width-chart {{
            grid-column: 1 / -1;
            min-height: 500px;
        }}

        /* Data Tables */
        .data-table {{
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            border-radius: 12px;
            overflow: hidden;
            margin-top: 2rem;
        }}

        .table-header {{
            background: var(--bg-primary);
            padding: 1rem 1.5rem;
            border-bottom: 1px solid var(--border-color);
        }}

        .table-title {{
            font-size: 18px;                    /* Subheading Size */
            font-weight: 600;
            color: var(--text-primary);
        }}

        .professional-table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 14px;                    /* Body Text Size */
        }}

        .professional-table th {{
            background: var(--bg-primary);
            color: var(--text-primary);
            padding: 1rem;
            text-align: left;
            font-weight: 600;
            border-bottom: 1px solid var(--border-color);
        }}

        .professional-table td {{
            padding: 1rem;
            border-bottom: 1px solid var(--border-color);
            color: var(--text-primary);
        }}

        .professional-table tr:hover {{
            background: var(--hover-color);
        }}

        /* Symbol Selector */
        .symbol-selector {{
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            border-radius: 8px;
            padding: 1rem;
            margin-bottom: 2rem;
        }}

        .symbol-input {{
            width: 100%;
            max-width: 300px;
            padding: 0.8rem;
            background: var(--bg-primary);
            border: 1px solid var(--border-color);
            border-radius: 6px;
            color: var(--text-primary);
            font-size: 16px;
            font-family: 'Poppins', sans-serif;
        }}

        .symbol-input:focus {{
            outline: none;
            border-color: var(--accent-primary);
        }}

        /* Responsive Design */
        @media (max-width: 1200px) {{
            .chart-grid {{
                grid-template-columns: 1fr;
            }}
        }}

        @media (max-width: 768px) {{
            .main-container {{
                padding: 1rem;
            }}
            
            .kpi-grid {{
                grid-template-columns: 1fr;
            }}
            
            .tab-container {{
                flex-wrap: wrap;
            }}
            
            .header-content {{
                flex-direction: column;
                gap: 1rem;
            }}
        }}

        /* Loading States */
        .loading {{
            display: flex;
            justify-content: center;
            align-items: center;
            height: 200px;
            color: var(--text-primary);
            font-size: 16px;
        }}

        .spinner {{
            border: 3px solid var(--border-color);
            border-top: 3px solid var(--accent-primary);
            border-radius: 50%;
            width: 30px;
            height: 30px;
            animation: spin 1s linear infinite;
            margin-right: 1rem;
        }}

        @keyframes spin {{
            0% {{ transform: rotate(0deg); }}
            100% {{ transform: rotate(360deg); }}
        }}
    </style>
</head>
<body>
    <!-- Professional Header -->
    <header class="dashboard-header">
        <div class="header-content">
            <div class="logo-section">
                <div class="logo">📊 Market Analytics Pro</div>
            </div>
            <div class="market-status">
                <div class="status-indicator">
                    <div class="status-dot"></div>
                    <span>Live Data</span>
                </div>
                <div class="status-indicator">
                    <i class="fas fa-chart-line"></i>
                    <span>{self.market_analytics['total_symbols_analyzed']:,} Symbols</span>
                </div>
                <div class="status-indicator">
                    <i class="fas fa-clock"></i>
                    <span>Updated: {datetime.now().strftime('%H:%M IST')}</span>
                </div>
            </div>
        </div>
    </header>

    <!-- Navigation Tabs -->
    <nav class="tab-navigation">
        <div class="tab-container">
            <button class="tab-button active" onclick="showTab('market-overview', this)">
                <i class="fas fa-globe"></i> Market Overview
            </button>
            <button class="tab-button" onclick="showTab('symbol-analysis', this)">
                <i class="fas fa-search"></i> Symbol Analysis
            </button>
            <button class="tab-button" onclick="showTab('category-performance', this)">
                <i class="fas fa-chart-bar"></i> Category & Index Performance
            </button>
        </div>
    </nav>

    <!-- Main Container -->
    <div class="main-container">
        
        <!-- Tab 1: Market Overview -->
        <div id="market-overview" class="tab-content active">
            <!-- KPIs -->
            <div class="kpi-grid">
                <div class="kpi-card">
                    <div class="kpi-header">
                        <div class="kpi-icon"><i class="fas fa-trending-up"></i></div>
                    </div>
                    <div class="kpi-value">{market_kpis['total_delivery_increase_lacs']:.1f}L</div>
                    <div class="kpi-label">Total Market Delivery Increase</div>
                    <div class="kpi-subtitle">In Lakhs of Shares</div>
                </div>

                <div class="kpi-card">
                    <div class="kpi-header">
                        <div class="kpi-icon"><i class="fas fa-chart-line"></i></div>
                    </div>
                    <div class="kpi-value">{market_kpis['stocks_with_positive_delivery']:,}</div>
                    <div class="kpi-label">Stocks with Positive Delivery Growth</div>
                    <div class="kpi-subtitle">Out of {self.market_analytics['total_symbols_analyzed']:,} total stocks</div>
                </div>

                <div class="kpi-card">
                    <div class="kpi-header">
                        <div class="kpi-icon"><i class="fas fa-balance-scale"></i></div>
                    </div>
                    <div class="kpi-value">{market_kpis['market_delivery_turnover_ratio']:.4f}</div>
                    <div class="kpi-label">Market Delivery-to-Turnover Ratio</div>
                    <div class="kpi-subtitle">Market efficiency indicator</div>
                </div>

                <div class="kpi-card">
                    <div class="kpi-header">
                        <div class="kpi-icon"><i class="fas fa-rupee-sign"></i></div>
                    </div>
                    <div class="kpi-value">₹{market_kpis['avg_daily_turnover']:.1f}L</div>
                    <div class="kpi-label">Average Daily Turnover</div>
                    <div class="kpi-subtitle">Per stock average</div>
                </div>
            </div>

            <!-- Charts Grid -->
            <div class="chart-grid">
                <div class="chart-container">
                    <div class="chart-title">📊 Turnover Treemap by Category & Symbol</div>
                    <div id="turnover-treemap"></div>
                </div>

                <div class="chart-container">
                    <div class="chart-title">🌐 Force-Directed Graph: Index Relationships</div>
                    <div id="force-directed-graph"></div>
                </div>
            </div>

            <div class="chart-grid">
                <div class="chart-container full-width-chart">
                    <div class="chart-title">☀️ Sunburst Chart: Hierarchical Delivery Distribution</div>
                    <div id="sunburst-chart"></div>
                </div>
            </div>

            <div class="chart-container full-width-chart">
                <div class="chart-title">📈 Parallel Coordinates: Multi-Metric Stock Comparison</div>
                <div id="parallel-coordinates"></div>
            </div>
        </div>

        <!-- Tab 2: Symbol Analysis -->
        <div id="symbol-analysis" class="tab-content">
            <!-- Symbol Selector -->
            <div class="symbol-selector">
                <label for="symbol-search" style="display: block; margin-bottom: 0.5rem; font-weight: 600;">Select Symbol for Analysis:</label>
                <input type="text" id="symbol-search" class="symbol-input" placeholder="Enter symbol (e.g., TCS, INFY, RELIANCE)" onchange="loadSymbolData()">
            </div>

            <!-- Symbol KPIs -->
            <div class="kpi-grid" id="symbol-kpis">
                <div class="kpi-card">
                    <div class="kpi-header">
                        <div class="kpi-icon"><i class="fas fa-percentage"></i></div>
                    </div>
                    <div class="kpi-value" id="current-delivery-pct">-</div>
                    <div class="kpi-label">Current Delivery Percentage</div>
                    <div class="kpi-subtitle">Latest trading session</div>
                </div>

                <div class="kpi-card">
                    <div class="kpi-header">
                        <div class="kpi-icon"><i class="fas fa-calendar-alt"></i></div>
                    </div>
                    <div class="kpi-value" id="mom-change">-</div>
                    <div class="kpi-label">Month-over-Month Change</div>
                    <div class="kpi-subtitle">Delivery percentage change</div>
                </div>

                <div class="kpi-card">
                    <div class="kpi-header">
                        <div class="kpi-icon"><i class="fas fa-chart-area"></i></div>
                    </div>
                    <div class="kpi-value" id="daily-volume">-</div>
                    <div class="kpi-label">Daily Volume</div>
                    <div class="kpi-subtitle">Total traded quantity</div>
                </div>

                <div class="kpi-card">
                    <div class="kpi-header">
                        <div class="kpi-icon"><i class="fas fa-book"></i></div>
                    </div>
                    <div class="kpi-value" id="order-book">Live</div>
                    <div class="kpi-label">Order Book Status</div>
                    <div class="kpi-subtitle">Real-time market depth</div>
                </div>
            </div>

            <!-- Symbol Charts -->
            <div class="chart-grid">
                <div class="chart-container">
                    <div class="chart-title">🕯️ Candlestick Chart with Volume</div>
                    <div id="candlestick-chart"></div>
                </div>

                <div class="chart-container">
                    <div class="chart-title">📊 Price vs Delivery Percentage</div>
                    <div id="dual-axis-chart"></div>
                </div>
            </div>
        </div>

        <!-- Tab 3: Category & Index Performance -->
        <div id="category-performance" class="tab-content">
            <!-- Performance KPIs -->
            <div class="kpi-grid">
                <div class="kpi-card">
                    <div class="kpi-header">
                        <div class="kpi-icon"><i class="fas fa-trophy"></i></div>
                    </div>
                    <div class="kpi-value">{self.market_analytics['best_performing_index']}</div>
                    <div class="kpi-label">Best Performing Index</div>
                    <div class="kpi-subtitle">Highest delivery growth</div>
                </div>

                <div class="kpi-card">
                    <div class="kpi-header">
                        <div class="kpi-icon"><i class="fas fa-star"></i></div>
                    </div>
                    <div class="kpi-value">{self.market_analytics['best_performing_category']}</div>
                    <div class="kpi-label">Best Performing Category</div>
                    <div class="kpi-subtitle">Top sector performance</div>
                </div>

                <div class="kpi-card">
                    <div class="kpi-header">
                        <div class="kpi-icon"><i class="fas fa-list"></i></div>
                    </div>
                    <div class="kpi-value">{self.market_analytics['total_symbols_analyzed']:,}</div>
                    <div class="kpi-label">Total Symbols Analyzed</div>
                    <div class="kpi-subtitle">In current watchlist</div>
                </div>

                <div class="kpi-card">
                    <div class="kpi-header">
                        <div class="kpi-icon"><i class="fas fa-chart-pie"></i></div>
                    </div>
                    <div class="kpi-value">{len(self.market_analytics['category_performance'])}</div>
                    <div class="kpi-label">Active Categories</div>
                    <div class="kpi-subtitle">Sectors being tracked</div>
                </div>
            </div>

            <!-- Index-Specific Performance Charts -->
            <div class="chart-grid">
                <div class="chart-container">
                    <div class="chart-title">📊 Index Performance Comparison</div>
                    <div id="index-performance-comparison"></div>
                </div>

                <div class="chart-container">
                    <div class="chart-title">💰 Index Turnover Distribution</div>
                    <div id="index-turnover-distribution"></div>
                </div>

                <div class="chart-container">
                    <div class="chart-title">📈 Index Delivery Growth Trends</div>
                    <div id="index-growth-trends"></div>
                </div>

                <div class="chart-container">
                    <div class="chart-title">🎯 Index Risk vs Return Matrix</div>
                    <div id="index-risk-return-matrix"></div>
                </div>
            </div>

            <!-- Category-Specific Performance Charts -->
            <div class="chart-grid">
                <div class="chart-container">
                    <div class="chart-title">⭕ Category Performance Radar</div>
                    <div id="category-radar-chart"></div>
                </div>

                <div class="chart-container">
                    <div class="chart-title">🔥 Category Performance Heatmap</div>
                    <div id="category-heatmap"></div>
                </div>

                <div class="chart-container">
                    <div class="chart-title">🌊 Category Value Flow Analysis</div>
                    <div id="category-flow-analysis"></div>
                </div>

                <div class="chart-container">
                    <div class="chart-title">📊 Category Distribution Pie</div>
                    <div id="category-distribution-pie"></div>
                </div>
            </div>

            <!-- Combined Analysis Charts -->
            <div class="chart-grid">
                <div class="chart-container">
                    <div class="chart-title">🎲 Index vs Category Performance Matrix</div>
                    <div id="index-category-matrix"></div>
                </div>

                <div class="chart-container">
                    <div class="chart-title">⚡ Top Performers Bubble Chart</div>
                    <div id="top-performers-bubble"></div>
                </div>
            </div>

            <!-- Performance Tables -->
            <div class="data-table">
                <div class="table-header">
                    <div class="table-title">📊 Index Performance Breakdown</div>
                </div>
                <table class="professional-table">
                    <thead>
                        <tr>
                            <th>Index Name</th>
                            <th>Total Stocks</th>
                            <th>Avg Delivery Change %</th>
                            <th>Total Turnover (₹Lac)</th>
                            <th>Positive Delivery Stocks</th>
                        </tr>
                    </thead>
                    <tbody id="index-performance-table">
                        <!-- Dynamic content -->
                    </tbody>
                </table>
            </div>
        </div>
    </div>

    <script>
        // Global Data
        const marketData = {data_json};
        const analytics = {analytics_json};
        
        // Initialize Dashboard
        document.addEventListener('DOMContentLoaded', function() {{
            console.log('DOM loaded, initializing Market Overview tab only...');
            
            // Only initialize Market Overview initially
            setTimeout(() => {{
                initializeMarketOverviewCharts();
            }}, 100);
            
            // Setup symbol search
            setupSymbolSearch();
            
            console.log('Initial setup complete');
        }});

        // Tab Navigation
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
            }} else {{
                console.error('Tab not found:', tabId);
                return;
            }}
            
            // Activate clicked button
            if (buttonElement) {{
                buttonElement.classList.add('active');
            }}
            
            // Initialize charts for the active tab
            if (tabId === 'market-overview') {{
                console.log('Initializing Market Overview charts...');
                setTimeout(() => {{
                    initializeMarketOverviewCharts();
                }}, 100);
            }} else if (tabId === 'symbol-analysis') {{
                console.log('Initializing Symbol Analysis charts...');
                initializeSymbolCharts();
            }} else if (tabId === 'category-performance') {{
                console.log('Initializing Category Performance charts...');
                initializeCategoryCharts();
            }}
        }}
                initializeSymbolCharts();
            }} else if (tabId === 'category-performance') {{
                initializeCategoryCharts();
            }}
        }}

        // Market Overview Charts
        function initializeMarketOverviewCharts() {{
            console.log('Starting Market Overview chart initialization...');
            
            try {{
                // Only initialize treemap for now
                console.log('Creating turnover treemap...');
                createTurnoverTreemap();
                
                setTimeout(() => {{
                    console.log('Creating force directed graph...');
                    createForceDirectedGraph();
                }}, 500);
                
                setTimeout(() => {{
                    console.log('Creating sunburst chart...');
                    createSunburstChart();
                }}, 1000);
                
                setTimeout(() => {{
                    console.log('Creating parallel coordinates...');
                    createParallelCoordinates();
                }}, 1500);
                
            }} catch (error) {{
                console.error('Error in initializeMarketOverviewCharts:', error);
            }}
        }}

        // Turnover Treemap
        function createTurnoverTreemap() {{
            try {{
                const container = document.getElementById('turnover-treemap');
                if (!container) {{
                    console.error('Container turnover-treemap not found');
                    return;
                }}
                
                container.innerHTML = '<div class="loading"><div class="spinner"></div>Loading Treemap...</div>';
                
                if (!analytics || !analytics.category_performance) {{
                    console.error('Category performance data not available for treemap');
                    container.innerHTML = '<div style="color: #E0E0E0; text-align: center; padding: 20px;">No category data available</div>';
                    return;
                }}
                
                // Prepare hierarchical data
                const categories = analytics.category_performance;
                const categoryNames = Object.keys(categories);
                
                if (categoryNames.length === 0) {{
                    container.innerHTML = '<div style="color: #E0E0E0; text-align: center; padding: 20px;">No category data</div>';
                    return;
                }}
                
                console.log('Creating treemap with categories:', categoryNames);
                
                // Create simplified treemap data
                const labels = [];
                const parents = [];
                const values = [];
                
                // Add root
                labels.push("Market");
                parents.push("");
                values.push(0);
                
                // Add categories
                categoryNames.forEach(cat => {{
                    const catData = categories[cat];
                    labels.push(cat);
                    parents.push("Market");
                    values.push(catData.total_turnover || 0);
                }});
                
                const plotlyData = [{{
                    type: "treemap",
                    labels: labels,
                    parents: parents,
                    values: values,
                    textinfo: "label+value",
                    textfont: {{ color: "#E0E0E0", size: 12 }},
                    marker: {{
                        colorscale: [
                            [0, "#D50000"],
                            [0.5, "#FF6F00"],
                            [1, "#00C853"]
                        ],
                        showscale: false
                    }}
                }}];
                
                const layout = {{
                    font: {{ color: "#E0E0E0", family: "Poppins" }},
                    paper_bgcolor: "transparent",
                    plot_bgcolor: "transparent",
                    margin: {{ t: 20, b: 20, l: 20, r: 20 }}
                }};
                
                Plotly.newPlot(container, plotlyData, layout, {{responsive: true, displayModeBar: false}})
                    .then(() => {{
                        console.log('Treemap created successfully');
                    }})
                    .catch((error) => {{
                        console.error('Plotly error in treemap:', error);
                        container.innerHTML = '<div style="color: #D50000; text-align: center; padding: 20px;">Chart rendering error</div>';
                    }});
            }} catch (error) {{
                console.error('Error creating turnover treemap:', error);
                const container = document.getElementById('turnover-treemap');
                if (container) {{
                    container.innerHTML = '<div style="color: #D50000; text-align: center; padding: 20px;">Error loading chart: ' + error.message + '</div>';
                }}
            }}
        }}
                    ],
                    showscale: false
                }}
            }}];
            
            const layout = {{
                font: {{ color: "#E0E0E0", family: "Poppins" }},
                paper_bgcolor: "transparent",
                plot_bgcolor: "transparent",
                margin: {{ t: 10, l: 10, r: 10, b: 10 }}
            }};
            
            Plotly.newPlot(container, plotlyData, layout, {{responsive: true}});
        }}

        // Force-Directed Graph using D3.js
        function createForceDirectedGraph() {{
            try {{
                const container = document.getElementById('force-directed-graph');
                if (!container) {{
                    console.error('Container force-directed-graph not found');
                    return;
                }}
                
                container.innerHTML = '<div class="loading"><div class="spinner"></div>Loading Index Relationships...</div>';
                
                if (!analytics || !analytics.index_performance) {{
                    console.error('Index performance data not available for force graph');
                    container.innerHTML = '<div style="color: #E0E0E0; text-align: center; padding: 20px;">No index data available</div>';
                    return;
                }}
                
                const indexPerf = analytics.index_performance;
                const indices = Object.keys(indexPerf);
                
                if (indices.length === 0) {{
                    container.innerHTML = '<div style="color: #E0E0E0; text-align: center; padding: 20px;">No index data</div>';
                    return;
                }}
                
                console.log('Creating index relationship chart');
                
                // Create a simple network-style bubble chart
                const plotlyData = [{{
                    x: indices.map((_, i) => Math.cos(2 * Math.PI * i / indices.length)),
                    y: indices.map((_, i) => Math.sin(2 * Math.PI * i / indices.length)),
                    mode: 'markers+text',
                    type: 'scatter',
                    text: indices,
                    textposition: 'middle center',
                    marker: {{
                        size: indices.map(idx => Math.sqrt(indexPerf[idx].total_turnover / 1000) + 20),
                        color: indices.map(idx => indexPerf[idx].avg_delivery_change),
                        colorscale: [
                            [0, '#D50000'],
                            [0.5, '#00B0FF'],
                            [1, '#00C853']
                        ],
                        showscale: true,
                        colorbar: {{
                            title: 'Delivery Change %',
                            titlefont: {{ color: '#E0E0E0' }},
                            tickfont: {{ color: '#E0E0E0' }}
                        }},
                        line: {{
                            color: '#E0E0E0',
                            width: 2
                        }}
                    }},
                    textfont: {{ color: '#E0E0E0', size: 10 }}
                }}];
                
                const layout = {{
                    font: {{ color: "#E0E0E0", family: "Poppins" }},
                    paper_bgcolor: "transparent",
                    plot_bgcolor: "transparent",
                    xaxis: {{ 
                        showgrid: false,
                        showticklabels: false,
                        zeroline: false
                    }},
                    yaxis: {{ 
                        showgrid: false,
                        showticklabels: false,
                        zeroline: false
                    }},
                    showlegend: false,
                    margin: {{ t: 20, b: 20, l: 20, r: 20 }}
                }};
                
                Plotly.newPlot(container, plotlyData, layout, {{responsive: true, displayModeBar: false}})
                    .then(() => {{
                        console.log('Index relationship chart created successfully');
                    }})
                    .catch((error) => {{
                        console.error('Plotly error in index relationships:', error);
                        container.innerHTML = '<div style="color: #D50000; text-align: center; padding: 20px;">Chart rendering error</div>';
                    }});
            }} catch (error) {{
                console.error('Error creating force directed graph:', error);
                const container = document.getElementById('force-directed-graph');
                if (container) {{
                    container.innerHTML = '<div style="color: #D50000; text-align: center; padding: 20px;">Error loading chart: ' + error.message + '</div>';
                }}
            }}
        }}
                links.push({{
                    source: indices[i],
                    target: indices[i + 1],
                    value: Math.random() * 10
                }});
            }}
            
            // Create force simulation
            const simulation = d3.forceSimulation(nodes)
                .force("link", d3.forceLink(links).id(d => d.id))
                .force("charge", d3.forceManyBody().strength(-300))
                .force("center", d3.forceCenter(width / 2, height / 2));
            
            // Draw links
            const link = svg.append("g")
                .selectAll("line")
                .data(links)
                .enter().append("line")
                .attr("stroke", "#00B0FF")
                .attr("stroke-opacity", 0.6)
                .attr("stroke-width", 2);
            
            // Draw nodes
            const node = svg.append("g")
                .selectAll("circle")
                .data(nodes)
                .enter().append("circle")
                .attr("r", d => Math.sqrt(d.value / 1000) + 5)
                .attr("fill", "#00C853")
                .attr("stroke", "#E0E0E0")
                .attr("stroke-width", 2);
            
            // Add labels
            const label = svg.append("g")
                .selectAll("text")
                .data(nodes)
                .enter().append("text")
                .text(d => d.id)
                .attr("font-size", "12px")
                .attr("fill", "#E0E0E0")
                .attr("text-anchor", "middle")
                .attr("dy", -15);
            
            // Update positions on tick
            simulation.on("tick", () => {{
                link
                    .attr("x1", d => d.source.x)
                    .attr("y1", d => d.source.y)
                    .attr("x2", d => d.target.x)
                    .attr("y2", d => d.target.y);
                
                node
                    .attr("cx", d => d.x)
                    .attr("cy", d => d.y);
                
                label
                    .attr("x", d => d.x)
                    .attr("y", d => d.y);
            }});
        }}

        // Sunburst Chart
        function createSunburstChart() {{
            try {{
                const container = document.getElementById('sunburst-chart');
                if (!container) {{
                    console.error('Container sunburst-chart not found');
                    return;
                }}
                
                container.innerHTML = '<div class="loading"><div class="spinner"></div>Loading Sunburst Chart...</div>';
                
                if (!analytics || !analytics.category_performance) {{
                    console.error('Category performance data not available for sunburst');
                    container.innerHTML = '<div style="color: #E0E0E0; text-align: center; padding: 20px;">No category data available</div>';
                    return;
                }}
                
                const categories = analytics.category_performance;
                const categoryNames = Object.keys(categories);
                
                if (categoryNames.length === 0) {{
                    container.innerHTML = '<div style="color: #E0E0E0; text-align: center; padding: 20px;">No category data</div>';
                    return;
                }}
                
                console.log('Creating sunburst with categories:', categoryNames);
                
                // Create simplified sunburst data
                const labels = [];
                const parents = [];
                const values = [];
                
                // Add root
                labels.push("Market");
                parents.push("");
                values.push(0);
                
                // Add categories and top stocks
                categoryNames.forEach(cat => {{
                    const catData = categories[cat];
                    labels.push(cat);
                    parents.push("Market");
                    values.push(catData.total_turnover || 0);
                    
                    // Add top 3 stocks for each category
                    const categoryStocks = marketData
                        .filter(d => d.category === cat)
                        .sort((a, b) => (b.current_turnover_lacs || 0) - (a.current_turnover_lacs || 0))
                        .slice(0, 3);
                    
                    categoryStocks.forEach(stock => {{
                        labels.push(stock.symbol);
                        parents.push(cat);
                        values.push(stock.current_turnover_lacs || 0);
                    }});
                }});
                
                const plotlyData = [{{
                    type: "sunburst",
                    labels: labels,
                    parents: parents,
                    values: values,
                    branchvalues: "total",
                    textfont: {{ color: "#E0E0E0", size: 10 }},
                    marker: {{
                        colorscale: [
                            [0, "#D50000"],
                            [0.5, "#00B0FF"], 
                            [1, "#00C853"]
                        ]
                    }}
                }}];
                
                const layout = {{
                    font: {{ color: "#E0E0E0", family: "Poppins" }},
                    paper_bgcolor: "transparent",
                    plot_bgcolor: "transparent",
                    margin: {{ t: 10, l: 10, r: 10, b: 10 }}
                }};
                
                Plotly.newPlot(container, plotlyData, layout, {{responsive: true, displayModeBar: false}})
                    .then(() => {{
                        console.log('Sunburst chart created successfully');
                    }})
                    .catch((error) => {{
                        console.error('Plotly error in sunburst:', error);
                        container.innerHTML = '<div style="color: #D50000; text-align: center; padding: 20px;">Chart rendering error</div>';
                    }});
            }} catch (error) {{
                console.error('Error creating sunburst chart:', error);
                const container = document.getElementById('sunburst-chart');
                if (container) {{
                    container.innerHTML = '<div style="color: #D50000; text-align: center; padding: 20px;">Error loading chart: ' + error.message + '</div>';
                }}
            }}
        }}
            
            Plotly.newPlot(container, plotlyData, layout, {{responsive: true}});
        }}

        // Parallel Coordinates Plot
        function createParallelCoordinates() {{
            try {{
                const container = document.getElementById('parallel-coordinates');
                if (!container) {{
                    console.error('Container parallel-coordinates not found');
                    return;
                }}
                
                container.innerHTML = '<div class="loading"><div class="spinner"></div>Loading Multi-Metric Comparison...</div>';
                
                if (!marketData || marketData.length === 0) {{
                    container.innerHTML = '<div style="color: #E0E0E0; text-align: center; padding: 20px;">No market data available</div>';
                    return;
                }}
                
                console.log('Creating multi-metric comparison chart');
                
                // Get top 30 stocks by turnover for better performance
                const topStocks = marketData
                    .filter(d => d.current_turnover_lacs && d.delivery_change_pct !== null)
                    .sort((a, b) => (b.current_turnover_lacs || 0) - (a.current_turnover_lacs || 0))
                    .slice(0, 30);
                
                if (topStocks.length === 0) {{
                    container.innerHTML = '<div style="color: #E0E0E0; text-align: center; padding: 20px;">No stock data available</div>';
                    return;
                }}
                
                // Create scatter plot with delivery change vs turnover
                const plotlyData = [{{
                    x: topStocks.map(d => d.current_turnover_lacs || 0),
                    y: topStocks.map(d => d.delivery_change_pct || 0),
                    mode: 'markers+text',
                    type: 'scatter',
                    text: topStocks.map(d => d.symbol),
                    textposition: 'top center',
                    marker: {{
                        size: 8,
                        color: topStocks.map(d => d.delivery_change_pct || 0),
                        colorscale: [
                            [0, '#D50000'],
                            [0.5, '#00B0FF'],
                            [1, '#00C853']
                        ],
                        showscale: true,
                        colorbar: {{
                            title: 'Delivery Change %',
                            titlefont: {{ color: '#E0E0E0' }},
                            tickfont: {{ color: '#E0E0E0' }}
                        }}
                    }},
                    textfont: {{ color: '#E0E0E0', size: 8 }}
                }}];
                
                const layout = {{
                    font: {{ color: "#E0E0E0", family: "Poppins" }},
                    paper_bgcolor: "transparent",
                    plot_bgcolor: "transparent",
                    xaxis: {{ 
                        title: 'Turnover (₹Lac)',
                        color: '#E0E0E0'
                    }},
                    yaxis: {{ 
                        title: 'Delivery Change %',
                        color: '#E0E0E0',
                        zeroline: true,
                        zerolinecolor: '#404040'
                    }},
                    showlegend: false,
                    margin: {{ t: 30, b: 60, l: 60, r: 30 }}
                }};
                
                Plotly.newPlot(container, plotlyData, layout, {{responsive: true, displayModeBar: false}})
                    .then(() => {{
                        console.log('Multi-metric comparison created successfully');
                    }})
                    .catch((error) => {{
                        console.error('Plotly error in multi-metric comparison:', error);
                        container.innerHTML = '<div style="color: #D50000; text-align: center; padding: 20px;">Chart rendering error</div>';
                    }});
            }} catch (error) {{
                console.error('Error creating parallel coordinates:', error);
                const container = document.getElementById('parallel-coordinates');
                if (container) {{
                    container.innerHTML = '<div style="color: #D50000; text-align: center; padding: 20px;">Error loading chart: ' + error.message + '</div>';
                }}
            }}
        }}
                    }},
                    {{
                        range: [d3.min(topStocks, d => d.daily_volatility), d3.max(topStocks, d => d.daily_volatility)],
                        label: 'Volatility %',
                        values: topStocks.map(d => d.daily_volatility)
                    }}
                ]
            }}];
            
            const layout = {{
                font: {{ color: "#E0E0E0", family: "Poppins" }},
                paper_bgcolor: "transparent",
                plot_bgcolor: "transparent",
                margin: {{ t: 50, l: 80, r: 80, b: 50 }}
            }};
            
            Plotly.newPlot(container, plotlyData, layout, {{responsive: true}});
        }}

        // Symbol Analysis Functions
        function setupSymbolSearch() {{
            const symbols = [...new Set(marketData.map(d => d.symbol))].sort();
            const input = document.getElementById('symbol-search');
            
            // Set default symbol
            if (symbols.length > 0) {{
                input.value = symbols[0];
                loadSymbolData();
            }}
        }}

        function loadSymbolData() {{
            const symbol = document.getElementById('symbol-search').value.toUpperCase();
            const stockData = marketData.find(d => d.symbol === symbol);
            
            if (stockData) {{
                // Update KPIs
                document.getElementById('current-delivery-pct').textContent = `${{stockData.current_deliv_per.toFixed(2)}}%`;
                document.getElementById('mom-change').textContent = `${{stockData.delivery_change_pct > 0 ? '+' : ''}}${{stockData.delivery_change_pct.toFixed(2)}}%`;
                document.getElementById('daily-volume').textContent = `${{(stockData.current_ttl_trd_qnty / 1000).toFixed(1)}}K`;
                
                // Apply color coding
                const changeElement = document.getElementById('mom-change');
                changeElement.className = stockData.delivery_change_pct > 0 ? 'positive' : stockData.delivery_change_pct < 0 ? 'negative' : 'neutral';
                
                // Create charts for this symbol
                createCandlestickChart(stockData);
                createDualAxisChart(stockData);
            }} else {{
                alert('Symbol not found. Please try another symbol.');
            }}
        }}

        function createCandlestickChart(stockData) {{
            const container = document.getElementById('candlestick-chart');
            container.innerHTML = '<div style="padding: 2rem; text-align: center; color: #E0E0E0;">Candlestick chart for ' + stockData.symbol + '<br/>Current: ₹' + stockData.current_close_price.toFixed(2) + '<br/>Volume: ' + (stockData.current_ttl_trd_qnty / 1000).toFixed(1) + 'K</div>';
        }}

        function createDualAxisChart(stockData) {{
            const container = document.getElementById('dual-axis-chart');
            container.innerHTML = '<div style="padding: 2rem; text-align: center; color: #E0E0E0;">Price vs Delivery for ' + stockData.symbol + '<br/>Price Change: ' + stockData.price_change_pct.toFixed(2) + '%<br/>Delivery Change: ' + stockData.delivery_change_pct.toFixed(2) + '%</div>';
        }}

        // Category Performance Functions
        function initializeCategoryCharts() {{
            console.log('Initializing category charts...');
            console.log('Analytics data:', analytics);
            console.log('Market data length:', marketData ? marketData.length : 'undefined');
            
            // Clear any existing loading states first
            const chartContainers = [
                'index-performance-comparison',
                'index-turnover-distribution', 
                'category-heatmap',
                'category-distribution-pie',
                'category-radar-chart',
                'top-performers-bubble'
            ];
            
            chartContainers.forEach(containerId => {{
                const container = document.getElementById(containerId);
                if (container) {{
                    container.innerHTML = '<div class="loading"><div class="spinner"></div>Initializing...</div>';
                }}
            }});
            
            // Initialize charts one by one with increased delays to prevent conflicts
            setTimeout(() => {{
                console.log('Loading Category Distribution Pie...');
                createCategoryDistributionPie();
            }}, 300);
            
            setTimeout(() => {{
                console.log('Loading Index Turnover Distribution...');
                createIndexTurnoverDistribution();
            }}, 800);
            
            setTimeout(() => {{
                console.log('Loading Category Heatmap...');
                createCategoryHeatmap();
            }}, 1300);
            
            setTimeout(() => {{
                console.log('Loading Index Performance Comparison...');
                createIndexPerformanceComparison();
            }}, 1800);
            
            setTimeout(() => {{
                console.log('Loading Simple Category Radar...');
                createSimpleCategoryRadar();
            }}, 2300);
            
            setTimeout(() => {{
                console.log('Loading Simple Top Performers...');
                createSimpleTopPerformers();
            }}, 2800);
            
            // Initialize placeholder charts for complex visualizations
            setTimeout(() => {{
                createCategoryFlowAnalysis();
                createIndexRiskReturnMatrix();
            }}, 3300);
            
            // Fallback cleanup for any charts still loading after 10 seconds
            setTimeout(() => {{
                chartContainers.forEach(containerId => {{
                    const container = document.getElementById(containerId);
                    if (container && container.innerHTML.includes('Loading')) {{
                        console.warn(`Chart ${{containerId}} still loading, applying fallback`);
                        container.innerHTML = '<div style="color: #E0E0E0; text-align: center; padding: 20px; background: rgba(44, 44, 44, 0.5); border-radius: 8px;"><i class="fas fa-chart-bar" style="font-size: 32px; color: #00B0FF; margin-bottom: 12px;"></i><br><strong>Chart Unavailable</strong><br><small>Data processing in progress</small></div>';
                    }}
                }});
            }}, 10000);
        }}

        // Index Performance Comparison Chart
        function createIndexPerformanceComparison() {{
            try {{
                const container = document.getElementById('index-performance-comparison');
                if (!container) {{
                    console.error('Container index-performance-comparison not found');
                    return;
                }}
                
                container.innerHTML = '<div class="loading"><div class="spinner"></div>Loading Index Comparison...</div>';
                
                if (!analytics || !analytics.index_performance) {{
                    console.error('Index performance data not available');
                    container.innerHTML = '<div style="color: #E0E0E0; text-align: center; padding: 20px;">No index data available</div>';
                    return;
                }}
                
                const indexPerf = analytics.index_performance;
                const indices = Object.keys(indexPerf);
                
                if (indices.length === 0) {{
                    container.innerHTML = '<div style="color: #E0E0E0; text-align: center; padding: 20px;">No index performance data</div>';
                    return;
                }}
                
                console.log('Creating index comparison with indices:', indices);
                
                // Validate data before creating chart
                const avgDeliveryChanges = indices.map(idx => {{
                    const val = indexPerf[idx].avg_delivery_change;
                    return (val && !isNaN(val)) ? val : 0;
                }});
                
                const totalTurnovers = indices.map(idx => {{
                    const val = indexPerf[idx].total_turnover;
                    return (val && !isNaN(val)) ? val : 0;
                }});
                
                // Create simplified single trace chart first
                const plotlyData = [{{
                    x: indices,
                    y: avgDeliveryChanges,
                    type: 'bar',
                    name: 'Avg Delivery Change %',
                    marker: {{
                        color: avgDeliveryChanges.map(v => v > 0 ? '#00C853' : '#D50000')
                    }},
                    text: avgDeliveryChanges.map(v => `${{v.toFixed(1)}}%`),
                    textposition: 'auto'
                }}];
                
                const layout = {{
                    font: {{ color: "#E0E0E0", family: "Poppins" }},
                    paper_bgcolor: "transparent",
                    plot_bgcolor: "transparent",
                    xaxis: {{ 
                        color: '#E0E0E0',
                        tickangle: -45
                    }},
                    yaxis: {{
                        title: 'Delivery Change %',
                        color: '#E0E0E0'
                    }},
                    showlegend: false,
                    margin: {{ t: 30, b: 100, l: 60, r: 30 }}
                }};
                
                console.log('Plotting index comparison chart...');
                Plotly.newPlot(container, plotlyData, layout, {{responsive: true, displayModeBar: false}})
                    .then(() => {{
                        console.log('Index comparison chart created successfully');
                    }})
                    .catch((error) => {{
                        console.error('Plotly error in index comparison:', error);
                        container.innerHTML = '<div style="color: #D50000; text-align: center; padding: 20px;">Chart rendering error</div>';
                    }});
            }} catch (error) {{
                console.error('Error creating index performance comparison:', error);
                const container = document.getElementById('index-performance-comparison');
                if (container) {{
                    container.innerHTML = '<div style="color: #D50000; text-align: center; padding: 20px;">Error loading chart: ' + error.message + '</div>';
                }}
            }}
        }}

        // Index Turnover Distribution Chart
        function createIndexTurnoverDistribution() {{
            try {{
                const container = document.getElementById('index-turnover-distribution');
                if (!container) {{
                    console.error('Container index-turnover-distribution not found');
                    return;
                }}
                
                container.innerHTML = '<div class="loading"><div class="spinner"></div>Loading Turnover Distribution...</div>';
                
                if (!analytics || !analytics.index_performance) {{
                    console.error('Index performance data not available');
                    container.innerHTML = '<div style="color: #E0E0E0; text-align: center; padding: 20px;">No index data available</div>';
                    return;
                }}
                
                const indexPerf = analytics.index_performance;
                const indices = Object.keys(indexPerf);
                
                if (indices.length === 0) {{
                    container.innerHTML = '<div style="color: #E0E0E0; text-align: center; padding: 20px;">No index data</div>';
                    return;
                }}
                
                console.log('Creating index turnover distribution with indices:', indices);
                
                const turnovers = indices.map(idx => indexPerf[idx].total_turnover || 0);
                
                const plotlyData = [{{
                    labels: indices,
                    values: turnovers,
                    type: 'pie',
                    hole: 0.4,
                    marker: {{
                        colors: ['#00C853', '#00B0FF', '#FF6F00', '#E91E63', '#9C27B0', '#3F51B5', '#009688', '#4CAF50']
                    }},
                    textinfo: 'label+percent',
                    textfont: {{ color: '#E0E0E0' }}
                }}];
                
                const layout = {{
                    font: {{ color: "#E0E0E0", family: "Poppins" }},
                    paper_bgcolor: "transparent",
                    plot_bgcolor: "transparent",
                    showlegend: true,
                    legend: {{ 
                        font: {{ color: '#E0E0E0' }},
                        bgcolor: 'rgba(44, 44, 44, 0.8)'
                    }}
                }};
                
                Plotly.newPlot(container, plotlyData, layout, {{responsive: true}});
            }} catch (error) {{
                console.error('Error creating index turnover distribution:', error);
                const container = document.getElementById('index-turnover-distribution');
                if (container) {{
                    container.innerHTML = '<div style="color: #D50000; text-align: center; padding: 20px;">Error loading chart: ' + error.message + '</div>';
                }}
            }}
        }}

        // Index Growth Trends Chart
        function createIndexGrowthTrends() {{
            const container = document.getElementById('index-growth-trends');
            container.innerHTML = '<div class="loading"><div class="spinner"></div>Loading Growth Trends...</div>';
            
            const indexPerf = analytics.index_performance;
            const indices = Object.keys(indexPerf);
            
            const traces = indices.map((idx, i) => ({{
                x: ['Previous Month', 'Current Month'],
                y: [0, indexPerf[idx].avg_delivery_change],
                type: 'scatter',
                mode: 'lines+markers',
                name: idx,
                line: {{
                    color: ['#00C853', '#00B0FF', '#FF6F00', '#E91E63', '#9C27B0'][i % 5],
                    width: 3
                }},
                marker: {{ size: 8 }}
            }}));
            
            const layout = {{
                font: {{ color: "#E0E0E0", family: "Poppins" }},
                paper_bgcolor: "transparent",
                plot_bgcolor: "transparent",
                xaxis: {{ color: '#E0E0E0' }},
                yaxis: {{ 
                    title: 'Delivery Change %',
                    color: '#E0E0E0',
                    zeroline: true,
                    zerolinecolor: '#404040'
                }},
                legend: {{ 
                    font: {{ color: '#E0E0E0' }},
                    bgcolor: 'rgba(44, 44, 44, 0.8)'
                }}
            }};
            
            Plotly.newPlot(container, traces, layout, {{responsive: true}});
        }}

        // Index Risk vs Return Matrix - Simplified
        function createIndexRiskReturnMatrix() {{
            try {{
                const container = document.getElementById('index-risk-return-matrix');
                if (!container) return;
                
                container.innerHTML = '<div style="color: #E0E0E0; text-align: center; padding: 40px; background: rgba(44, 44, 44, 0.5); border-radius: 8px;"><i class="fas fa-chart-scatter" style="font-size: 48px; color: #00B0FF; margin-bottom: 16px;"></i><br><strong>Risk-Return Matrix</strong><br><small>Advanced analytics coming soon</small></div>';
            }} catch (error) {{
                console.error('Error in risk-return matrix:', error);
            }}
        }}

        // Category Performance Radar Chart
        function createCategoryRadarChart() {{
            const container = document.getElementById('category-radar-chart');
            container.innerHTML = '<div class="loading"><div class="spinner"></div>Loading Category Radar...</div>';
            
            const categories = analytics.category_performance;
            const categoryNames = Object.keys(categories);
            
            const traces = categoryNames.slice(0, 5).map((cat, i) => ({{
                type: 'scatterpolar',
                r: [
                    categories[cat].avg_delivery_change,
                    categories[cat].total_turnover / 10000, // Scale down for visualization
                    categories[cat].positive_delivery_count,
                    categories[cat].stock_count,
                    categories[cat].avg_delivery_change // Close the radar
                ],
                theta: ['Delivery Change', 'Turnover', 'Positive Stocks', 'Total Stocks', 'Delivery Change'],
                fill: 'toself',
                name: cat,
                line: {{
                    color: ['#00C853', '#00B0FF', '#FF6F00', '#E91E63', '#9C27B0'][i]
                }}
            }}));
            
            const layout = {{
                polar: {{
                    radialaxis: {{
                        visible: true,
                        color: '#E0E0E0'
                    }},
                    angularaxis: {{
                        color: '#E0E0E0'
                    }}
                }},
                font: {{ color: "#E0E0E0", family: "Poppins" }},
                paper_bgcolor: "transparent",
                plot_bgcolor: "transparent",
                legend: {{ 
                    font: {{ color: '#E0E0E0' }},
                    bgcolor: 'rgba(44, 44, 44, 0.8)'
                }}
            }};
            
            Plotly.newPlot(container, traces, layout, {{responsive: true}});
        }}

        // Category Performance Heatmap
        function createCategoryHeatmap() {{
            try {{
                const container = document.getElementById('category-heatmap');
                if (!container) {{
                    console.error('Container category-heatmap not found');
                    return;
                }}
                
                container.innerHTML = '<div class="loading"><div class="spinner"></div>Loading Category Heatmap...</div>';
                
                if (!analytics || !analytics.category_performance) {{
                    console.error('Category performance data not available');
                    container.innerHTML = '<div style="color: #E0E0E0; text-align: center; padding: 20px;">No category data available</div>';
                    return;
                }}
                
                const categories = analytics.category_performance;
                const categoryNames = Object.keys(categories);
                
                if (categoryNames.length === 0) {{
                    container.innerHTML = '<div style="color: #E0E0E0; text-align: center; padding: 20px;">No category data</div>';
                    return;
                }}
                
                console.log('Creating category heatmap with categories:', categoryNames);
                
                const metrics = ['avg_delivery_change', 'total_turnover', 'positive_delivery_count', 'stock_count'];
                const metricLabels = ['Delivery Change %', 'Turnover (₹Lac)', 'Positive Stocks', 'Total Stocks'];
                
                const heatmapData = metrics.map(metric => 
                    categoryNames.map(cat => categories[cat][metric] || 0)
                );
                
                const plotlyData = [{{
                    z: heatmapData,
                    x: categoryNames,
                    y: metricLabels,
                    type: 'heatmap',
                    colorscale: [
                        [0, '#D50000'],
                        [0.5, '#1A1A1A'],
                        [1, '#00C853']
                    ],
                    showscale: true,
                    colorbar: {{
                        titlefont: {{ color: '#E0E0E0' }},
                        tickfont: {{ color: '#E0E0E0' }}
                    }}
                }}];
                
                const layout = {{
                    font: {{ color: "#E0E0E0", family: "Poppins" }},
                    paper_bgcolor: "transparent",
                    plot_bgcolor: "transparent",
                    xaxis: {{ 
                        color: '#E0E0E0',
                        tickangle: -45
                    }},
                    yaxis: {{ color: '#E0E0E0' }}
                }};
                
                Plotly.newPlot(container, plotlyData, layout, {{responsive: true}});
            }} catch (error) {{
                console.error('Error creating category heatmap:', error);
                const container = document.getElementById('category-heatmap');
                if (container) {{
                    container.innerHTML = '<div style="color: #D50000; text-align: center; padding: 20px;">Error loading chart: ' + error.message + '</div>';
                }}
            }}
        }}

        // Category Value Flow Analysis - Simplified
        function createCategoryFlowAnalysis() {{
            try {{
                const container = document.getElementById('category-flow-analysis');
                if (!container) return;
                
                container.innerHTML = '<div style="color: #E0E0E0; text-align: center; padding: 40px; background: rgba(44, 44, 44, 0.5); border-radius: 8px;"><i class="fas fa-chart-line" style="font-size: 48px; color: #00B0FF; margin-bottom: 16px;"></i><br><strong>Flow Analysis</strong><br><small>Advanced visualization coming soon</small></div>';
            }} catch (error) {{
                console.error('Error in flow analysis:', error);
            }}
        }}

        // Category Distribution Pie Chart
        function createCategoryDistributionPie() {{
            try {{
                const container = document.getElementById('category-distribution-pie');
                if (!container) {{
                    console.error('Container category-distribution-pie not found');
                    return;
                }}
                
                container.innerHTML = '<div class="loading"><div class="spinner"></div>Loading Distribution...</div>';
                
                if (!analytics || !analytics.category_performance) {{
                    console.error('Category performance data not available');
                    container.innerHTML = '<div style="color: #E0E0E0; text-align: center; padding: 20px;">No category data available</div>';
                    return;
                }}
                
                const categories = analytics.category_performance;
                const categoryNames = Object.keys(categories);
                
                if (categoryNames.length === 0) {{
                    container.innerHTML = '<div style="color: #E0E0E0; text-align: center; padding: 20px;">No category data</div>';
                    return;
                }}
                
                console.log('Creating category pie chart with categories:', categoryNames);
                
                const stockCounts = categoryNames.map(cat => categories[cat].stock_count || 0);
                
                const plotlyData = [{{
                    labels: categoryNames,
                    values: stockCounts,
                    type: 'pie',
                    textinfo: 'label+percent+value',
                    textfont: {{ color: '#E0E0E0' }},
                    marker: {{
                        colors: ['#00C853', '#00B0FF', '#FF6F00', '#E91E63', '#9C27B0', '#3F51B5', '#009688', '#4CAF50'],
                        line: {{
                            color: '#E0E0E0',
                            width: 1
                        }}
                    }}
                }}];
                
                const layout = {{
                    font: {{ color: "#E0E0E0", family: "Poppins" }},
                    paper_bgcolor: "transparent",
                    plot_bgcolor: "transparent",
                    showlegend: true,
                    legend: {{ 
                        font: {{ color: '#E0E0E0' }},
                        bgcolor: 'rgba(44, 44, 44, 0.8)'
                    }}
                }};
                
                Plotly.newPlot(container, plotlyData, layout, {{responsive: true}});
            }} catch (error) {{
                console.error('Error creating category distribution pie:', error);
                const container = document.getElementById('category-distribution-pie');
                if (container) {{
                    container.innerHTML = '<div style="color: #D50000; text-align: center; padding: 20px;">Error loading chart: ' + error.message + '</div>';
                }}
            }}
        }}

        // Index vs Category Performance Matrix
        function createIndexCategoryMatrix() {{
            const container = document.getElementById('index-category-matrix');
            container.innerHTML = '<div class="loading"><div class="spinner"></div>Loading Performance Matrix...</div>';
            
            const indexPerf = analytics.index_performance;
            const categoryPerf = analytics.category_performance;
            
            const indices = Object.keys(indexPerf);
            const categories = Object.keys(categoryPerf);
            
            // Create matrix data showing correlation between index and category performance
            const matrixData = [];
            
            categories.forEach(category => {{
                const row = [];
                indices.forEach(index => {{
                    // Find stocks that belong to both this category and index
                    const commonStocks = marketData.filter(d => 
                        d.category === category && d.index_name === index
                    );
                    const avgPerformance = commonStocks.length > 0 ? 
                        commonStocks.reduce((sum, stock) => sum + (stock.delivery_change_pct || 0), 0) / commonStocks.length : 0;
                    row.push(avgPerformance);
                }});
                matrixData.push(row);
            }});
            
            const plotlyData = [{{
                z: matrixData,
                x: indices,
                y: categories,
                type: 'heatmap',
                colorscale: [
                    [0, '#D50000'],
                    [0.5, '#1A1A1A'],
                    [1, '#00C853']
                ],
                showscale: true,
                colorbar: {{
                    title: 'Performance %',
                    titlefont: {{ color: '#E0E0E0' }},
                    tickfont: {{ color: '#E0E0E0' }}
                }}
            }}];
            
            const layout = {{
                font: {{ color: "#E0E0E0", family: "Poppins" }},
                paper_bgcolor: "transparent",
                plot_bgcolor: "transparent",
                xaxis: {{ 
                    color: '#E0E0E0',
                    tickangle: -45
                }},
                yaxis: {{ color: '#E0E0E0' }}
            }};
            
            Plotly.newPlot(container, plotlyData, layout, {{responsive: true}});
        }}

        // Top Performers Bubble Chart
        function createTopPerformersBubble() {{
            const container = document.getElementById('top-performers-bubble');
            container.innerHTML = '<div class="loading"><div class="spinner"></div>Loading Top Performers...</div>';
            
            // Get top performing stocks
            const topStocks = marketData
                .filter(d => d.delivery_change_pct > 0)
                .sort((a, b) => (b.delivery_change_pct || 0) - (a.delivery_change_pct || 0))
                .slice(0, 20);
            
            const plotlyData = [{{
                x: topStocks.map(s => s.current_turnover_lacs || 0),
                y: topStocks.map(s => s.delivery_change_pct || 0),
                mode: 'markers+text',
                type: 'scatter',
                text: topStocks.map(s => s.symbol),
                textposition: 'top center',
                marker: {{
                    size: topStocks.map(s => Math.sqrt((s.current_turnover_lacs || 0) / 100)),
                    color: topStocks.map(s => s.delivery_change_pct || 0),
                    colorscale: [
                        [0, '#FF6F00'],
                        [1, '#00C853']
                    ],
                    showscale: true,
                    colorbar: {{
                        title: 'Delivery Change %',
                        titlefont: {{ color: '#E0E0E0' }},
                        tickfont: {{ color: '#E0E0E0' }}
                    }}
                }},
                textfont: {{ color: '#E0E0E0', size: 10 }}
            }}];
            
            const layout = {{
                font: {{ color: "#E0E0E0", family: "Poppins" }},
                paper_bgcolor: "transparent",
                plot_bgcolor: "transparent",
                xaxis: {{ 
                    title: 'Turnover (₹Lac)',
                    color: '#E0E0E0'
                }},
                yaxis: {{ 
                    title: 'Delivery Change %',
                    color: '#E0E0E0'
                }}
            }};
            
            Plotly.newPlot(container, plotlyData, layout, {{responsive: true}});
        }}

        // Simplified Category Radar Chart
        function createSimpleCategoryRadar() {{
            try {{
                const container = document.getElementById('category-radar-chart');
                if (!container) {{
                    console.error('Container category-radar-chart not found');
                    return;
                }}
                
                container.innerHTML = '<div class="loading"><div class="spinner"></div>Loading Category Radar...</div>';
                
                if (!analytics || !analytics.category_performance) {{
                    container.innerHTML = '<div style="color: #E0E0E0; text-align: center; padding: 20px;">No category data available</div>';
                    return;
                }}
                
                const categories = analytics.category_performance;
                const categoryNames = Object.keys(categories).slice(0, 6); // Limit to 6 categories
                
                if (categoryNames.length === 0) {{
                    container.innerHTML = '<div style="color: #E0E0E0; text-align: center; padding: 20px;">No category data</div>';
                    return;
                }}
                
                // Create a simple bar chart instead of complex radar
                const plotlyData = [{{
                    x: categoryNames,
                    y: categoryNames.map(cat => categories[cat].avg_delivery_change || 0),
                    type: 'bar',
                    marker: {{
                        color: categoryNames.map(cat => 
                            (categories[cat].avg_delivery_change || 0) > 0 ? '#00C853' : '#D50000'
                        )
                    }},
                    text: categoryNames.map(cat => `${{(categories[cat].avg_delivery_change || 0).toFixed(1)}}%`),
                    textposition: 'auto'
                }}];
                
                const layout = {{
                    font: {{ color: "#E0E0E0", family: "Poppins" }},
                    paper_bgcolor: "transparent",
                    plot_bgcolor: "transparent",
                    xaxis: {{ 
                        color: '#E0E0E0',
                        tickangle: -45
                    }},
                    yaxis: {{ 
                        title: 'Delivery Change %',
                        color: '#E0E0E0',
                        zeroline: true,
                        zerolinecolor: '#404040'
                    }},
                    showlegend: false
                }};
                
                Plotly.newPlot(container, plotlyData, layout, {{responsive: true}});
            }} catch (error) {{
                console.error('Error creating simple category radar:', error);
                const container = document.getElementById('category-radar-chart');
                if (container) {{
                    container.innerHTML = '<div style="color: #D50000; text-align: center; padding: 20px;">Error loading chart: ' + error.message + '</div>';
                }}
            }}
        }}

        // Simplified Top Performers Chart
        function createSimpleTopPerformers() {{
            try {{
                const container = document.getElementById('top-performers-bubble');
                if (!container) {{
                    console.error('Container top-performers-bubble not found');
                    return;
                }}
                
                container.innerHTML = '<div class="loading"><div class="spinner"></div>Loading Top Performers...</div>';
                
                if (!marketData || marketData.length === 0) {{
                    container.innerHTML = '<div style="color: #E0E0E0; text-align: center; padding: 20px;">No market data available</div>';
                    return;
                }}
                
                // Get top 10 performers by delivery change
                const topStocks = marketData
                    .filter(d => d.delivery_change_pct && d.delivery_change_pct > 0)
                    .sort((a, b) => (b.delivery_change_pct || 0) - (a.delivery_change_pct || 0))
                    .slice(0, 10);
                
                if (topStocks.length === 0) {{
                    container.innerHTML = '<div style="color: #E0E0E0; text-align: center; padding: 20px;">No positive performers found</div>';
                    return;
                }}
                
                const plotlyData = [{{
                    x: topStocks.map(s => s.symbol),
                    y: topStocks.map(s => s.delivery_change_pct || 0),
                    type: 'bar',
                    marker: {{
                        color: '#00C853',
                        line: {{
                            color: '#E0E0E0',
                            width: 1
                        }}
                    }},
                    text: topStocks.map(s => `${{(s.delivery_change_pct || 0).toFixed(1)}}%`),
                    textposition: 'auto'
                }}];
                
                const layout = {{
                    font: {{ color: "#E0E0E0", family: "Poppins" }},
                    paper_bgcolor: "transparent",
                    plot_bgcolor: "transparent",
                    xaxis: {{ 
                        color: '#E0E0E0',
                        tickangle: -45
                    }},
                    yaxis: {{ 
                        title: 'Delivery Change %',
                        color: '#E0E0E0'
                    }},
                    showlegend: false
                }};
                
                Plotly.newPlot(container, plotlyData, layout, {{responsive: true}});
            }} catch (error) {{
                console.error('Error creating simple top performers:', error);
                const container = document.getElementById('top-performers-bubble');
                if (container) {{
                    container.innerHTML = '<div style="color: #D50000; text-align: center; padding: 20px;">Error loading chart: ' + error.message + '</div>';
                }}
            }}
        }}

        // Populate Index Performance Table
        function populateIndexPerformanceTable() {{
            const tableBody = document.getElementById('index-performance-table');
            const indexPerf = analytics.index_performance;
            
            tableBody.innerHTML = '';
            
            Object.entries(indexPerf).forEach(([index, data]) => {{
                const row = document.createElement('tr');
                
                const changeClass = data.avg_delivery_change > 0 ? 'positive' : data.avg_delivery_change < 0 ? 'negative' : 'neutral';
                
                row.innerHTML = `
                    <td><strong>${{index}}</strong></td>
                    <td>${{data.total_stocks}}</td>
                    <td class="${{changeClass}}">${{data.avg_delivery_change > 0 ? '+' : ''}}${{data.avg_delivery_change.toFixed(2)}}%</td>
                    <td>₹${{data.total_turnover.toFixed(1)}}</td>
                    <td>${{data.positive_delivery_stocks}}</td>
                `;
                
                tableBody.appendChild(row);
            }});
        }}

        function initializeSymbolCharts() {{
            // Initialize symbol-specific charts when tab is activated
            const currentSymbol = document.getElementById('symbol-search').value;
            if (currentSymbol) {{
                loadSymbolData();
            }}
        }}
    </script>
</body>
</html>
        """
        
        return html_content

    def run(self):
        """Main execution function"""
        print("📊 Starting Professional Market Analytics Dashboard...")
        
        if not self.connect_and_fetch_data():
            return False
            
        print("📊 Computing market analytics...")
        self.calculate_market_analytics()
        
        print("🎨 Generating professional dashboard with exact specifications...")
        html_content = self.generate_professional_dashboard()
        
        # Save the dashboard
        dashboard_path = "C:/Users/kiran/NSE_Downloader/dashboard/professional_market_dashboard.html"
        with open(dashboard_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"✅ Professional market dashboard saved to: {dashboard_path}")
        
        # Open in browser
        webbrowser.open(f'file:///{dashboard_path}')
        print("🌐 Professional dashboard opened in browser!")
        
        print("\n" + "="*80)
        print("📊 PROFESSIONAL MARKET ANALYTICS DASHBOARD FEATURES:")
        print("="*80)
        print("🎨 EXACT COLOR SPECIFICATIONS:")
        print("   • Main Background: #1A1A1A (Deep Charcoal)")
        print("   • Card Background: #2C2C2C (Subtle Separation)")
        print("   • Text Color: #E0E0E0 (Soft White)")
        print("   • Positive Trends: #00C853 (Vibrant Green)")
        print("   • Negative Trends: #D50000 (Contrasting Red)")
        print("   • Primary Accent: #00B0FF (Sky Blue)")
        print("\n✨ PROFESSIONAL TYPOGRAPHY:")
        print("   • Font Family: Poppins & Inter")
        print("   • Main Headings: 24px, Bold")
        print("   • Subheadings: 18px, Medium")
        print("   • Body Text: 14-16px, Regular")
        print("\n📋 THREE STRATEGIC TABS:")
        print("   📊 TAB 1 - MARKET OVERVIEW:")
        print("      • Total Market Delivery Increase (Lakhs)")
        print("      • Stocks with Positive Delivery Growth")
        print("      • Market Delivery-to-Turnover Ratio")
        print("      • Average Daily Turnover")
        print("      • Treemap: Turnover by Category & Symbol")
        print("      • Force-Directed Graph: Index Relationships")
        print("      • Sunburst Chart: Hierarchical Distribution")
        print("      • Parallel Coordinates: Multi-Metric Comparison")
        print("\n   🔍 TAB 2 - SYMBOL ANALYSIS:")
        print("      • Current Delivery Percentage")
        print("      • Month-over-Month Change")
        print("      • Daily Volume")
        print("      • Live Order Book Status")
        print("      • Candlestick Chart with Volume Bars")
        print("      • Dual-Axis Chart: Price vs Delivery")
        print("\n   📈 TAB 3 - CATEGORY & INDEX PERFORMANCE:")
        print("      • Best Performing Index")
        print("      • Best Performing Category")
        print("      • Total Symbols in Watchlist")
        print("      • Radial Bar Chart: Category Delivery Increase")
        print("      • Heatmap: Stock Performance by Category")
        print("      • Index Performance Breakdown Table")
        print("="*80)
        
        return True

if __name__ == "__main__":
    dashboard = ProfessionalMarketDashboard()
    dashboard.run()