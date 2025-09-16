#!/usr/bin/env python3
"""
Enhanced NSE Dashboard - Three Tab Structure
Implements the complete dashboard specification with all KPIs and charts
"""
import os
import json
import pyodbc
import webbrowser
import tempfile
from datetime import datetime, timedelta
import math

def get_db_connection():
    """Get database connection"""
    return pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=SRIKIRANREDDY\\SQLEXPRESS;"
        "DATABASE=master;"
        "Trusted_Connection=yes;"
    )

def get_tab1_data():
    """Tab 1: Current Month Daily Performance"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Daily Delivery Quantity (Symbol-wise) with percentage changes
        cursor.execute("""
            SELECT 
                symbol,
                current_trade_date,
                current_deliv_qty,
                previous_deliv_qty,
                current_ttl_trd_qnty,
                current_deliv_per,
                current_open_price,
                current_high_price,
                current_low_price,
                current_close_price,
                CASE 
                    WHEN previous_deliv_qty > 0 
                    THEN ((CAST(current_deliv_qty AS FLOAT) - CAST(previous_deliv_qty AS FLOAT)) / CAST(previous_deliv_qty AS FLOAT)) * 100
                    ELSE 0 
                END as percentage_change_from_prev,
                CASE 
                    WHEN current_deliv_qty > previous_deliv_qty THEN 1 
                    ELSE 0 
                END as is_higher_delivery
            FROM step03_compare_monthvspreviousmonth
            WHERE current_deliv_qty IS NOT NULL
            ORDER BY current_trade_date DESC, CAST(current_deliv_per AS FLOAT) DESC
        """)
        
        daily_data = []
        symbols_with_higher_delivery = 0
        total_symbols = 0
        
        for row in cursor.fetchall():
            daily_data.append({
                'symbol': row.symbol,
                'trade_date': str(row.current_trade_date) if row.current_trade_date else 'N/A',
                'current_deliv_qty': int(row.current_deliv_qty or 0),
                'previous_deliv_qty': int(row.previous_deliv_qty or 0),
                'current_ttl_trd_qnty': int(row.current_ttl_trd_qnty or 0),
                'delivery_percentage': float(row.current_deliv_per or 0),
                'open_price': float(row.current_open_price or 0),
                'high_price': float(row.current_high_price or 0),
                'low_price': float(row.current_low_price or 0),
                'close_price': float(row.current_close_price or 0),
                'percentage_change': float(row.percentage_change_from_prev or 0),
                'is_higher_delivery': bool(row.is_higher_delivery)
            })
            
            if row.is_higher_delivery:
                symbols_with_higher_delivery += 1
            total_symbols += 1
        
        # Get heatmap data (top symbols by delivery percentage)
        cursor.execute("""
            SELECT TOP 20
                symbol,
                current_trade_date,
                CAST(current_deliv_per AS FLOAT) as delivery_pct
            FROM step03_compare_monthvspreviousmonth
            WHERE current_deliv_per IS NOT NULL
            ORDER BY CAST(current_deliv_per AS FLOAT) DESC
        """)
        
        heatmap_data = []
        for row in cursor.fetchall():
            heatmap_data.append({
                'symbol': row.symbol,
                'trade_date': str(row.current_trade_date) if row.current_trade_date else 'N/A',
                'delivery_pct': float(row.delivery_pct)
            })
        
        cursor.close()
        conn.close()
        
        return {
            'daily_data': daily_data[:50],  # Top 50 for display
            'heatmap_data': heatmap_data,
            'kpis': {
                'symbols_with_higher_delivery': symbols_with_higher_delivery,
                'total_symbols': total_symbols,
                'avg_delivery_percentage': sum(d['delivery_percentage'] for d in daily_data) / len(daily_data) if daily_data else 0,
                'max_delivery_percentage': max(d['delivery_percentage'] for d in daily_data) if daily_data else 0
            }
        }
        
    except Exception as e:
        return {'error': str(e)}

def get_tab2_data():
    """Tab 2: Monthly Trends and Comparison"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Total Monthly Delivery Quantity
        cursor.execute("""
            SELECT 
                SUM(CAST(current_deliv_qty AS BIGINT)) as current_month_total,
                SUM(CAST(previous_deliv_qty AS BIGINT)) as previous_month_total,
                COUNT(DISTINCT current_trade_date) as trading_days,
                SUM(CAST(current_ttl_trd_qnty AS BIGINT)) as current_month_traded_total,
                AVG(CAST(current_deliv_qty AS FLOAT)) as daily_avg_delivery
            FROM step03_compare_monthvspreviousmonth
            WHERE current_deliv_qty IS NOT NULL
        """)
        
        monthly_stats = cursor.fetchone()
        
        # Symbol-wise contribution for treemap
        cursor.execute("""
            SELECT TOP 20
                symbol,
                SUM(CAST(current_deliv_qty AS BIGINT)) as symbol_total_delivery,
                AVG(CAST(current_deliv_per AS FLOAT)) as avg_delivery_pct,
                category
            FROM step03_compare_monthvspreviousmonth
            WHERE current_deliv_qty IS NOT NULL
            GROUP BY symbol, category
            ORDER BY SUM(CAST(current_deliv_qty AS BIGINT)) DESC
        """)
        
        treemap_data = []
        for row in cursor.fetchall():
            treemap_data.append({
                'symbol': row.symbol,
                'total_delivery': int(row.symbol_total_delivery or 0),
                'avg_delivery_pct': float(row.avg_delivery_pct or 0),
                'category': row.category or 'Unknown'
            })
        
        # Category-wise comparison
        cursor.execute("""
            SELECT 
                category,
                SUM(CAST(current_deliv_qty AS BIGINT)) as current_total,
                SUM(CAST(previous_deliv_qty AS BIGINT)) as previous_total,
                COUNT(DISTINCT symbol) as symbol_count
            FROM step03_compare_monthvspreviousmonth
            WHERE current_deliv_qty IS NOT NULL AND category IS NOT NULL
            GROUP BY category
            ORDER BY SUM(CAST(current_deliv_qty AS BIGINT)) DESC
        """)
        
        category_data = []
        for row in cursor.fetchall():
            category_data.append({
                'category': row.category,
                'current_total': int(row.current_total or 0),
                'previous_total': int(row.previous_total or 0),
                'symbol_count': int(row.symbol_count or 0),
                'percentage_change': ((int(row.current_total or 0) - int(row.previous_total or 0)) / int(row.previous_total or 1)) * 100
            })
        
        cursor.close()
        conn.close()
        
        current_total = int(monthly_stats.current_month_total or 0)
        previous_total = int(monthly_stats.previous_month_total or 0)
        trading_days = int(monthly_stats.trading_days or 1)
        traded_total = int(monthly_stats.current_month_traded_total or 1)
        
        return {
            'monthly_kpis': {
                'current_month_total': current_total,
                'previous_month_total': previous_total,
                'monthly_avg_daily': current_total / trading_days,
                'delivery_vs_traded_ratio': (current_total / traded_total) * 100,
                'month_over_month_change': ((current_total - previous_total) / previous_total) * 100 if previous_total > 0 else 0,
                'trading_days': trading_days
            },
            'treemap_data': treemap_data,
            'category_data': category_data
        }
        
    except Exception as e:
        return {'error': str(e)}

def get_tab3_data(selected_symbol=None):
    """Tab 3: Symbol-Specific Deep Dive"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        if not selected_symbol:
            # Get top symbol by delivery percentage as default
            cursor.execute("""
                SELECT TOP 1 symbol
                FROM step03_compare_monthvspreviousmonth
                WHERE current_deliv_per IS NOT NULL
                ORDER BY CAST(current_deliv_per AS FLOAT) DESC
            """)
            result = cursor.fetchone()
            selected_symbol = result.symbol if result else 'RELIANCE'
        
        # Symbol-specific data
        cursor.execute("""
            SELECT 
                symbol,
                current_trade_date,
                current_deliv_qty,
                previous_deliv_qty,
                current_ttl_trd_qnty,
                current_deliv_per,
                current_open_price,
                current_high_price,
                current_low_price,
                current_close_price,
                current_turnover_lacs,
                delivery_increase_pct
            FROM step03_compare_monthvspreviousmonth
            WHERE symbol = ? AND current_deliv_qty IS NOT NULL
            ORDER BY current_trade_date DESC
        """, selected_symbol)
        
        symbol_data = []
        prices = []
        deliveries = []
        
        for row in cursor.fetchall():
            data_point = {
                'trade_date': str(row.current_trade_date) if row.current_trade_date else 'N/A',
                'deliv_qty': int(row.current_deliv_qty or 0),
                'prev_deliv_qty': int(row.previous_deliv_qty or 0),
                'ttl_trd_qnty': int(row.current_ttl_trd_qnty or 0),
                'delivery_pct': float(row.current_deliv_per or 0),
                'open_price': float(row.current_open_price or 0),
                'high_price': float(row.current_high_price or 0),
                'low_price': float(row.current_low_price or 0),
                'close_price': float(row.current_close_price or 0),
                'turnover': float(row.current_turnover_lacs or 0),
                'delivery_increase_pct': float(row.delivery_increase_pct or 0)
            }
            symbol_data.append(data_point)
            
            if data_point['close_price'] > 0 and data_point['deliv_qty'] > 0:
                prices.append(data_point['close_price'])
                deliveries.append(data_point['deliv_qty'])
        
        # Calculate correlation
        correlation = 0
        if len(prices) > 1 and len(deliveries) > 1:
            n = len(prices)
            sum_x = sum(prices)
            sum_y = sum(deliveries)
            sum_xy = sum(prices[i] * deliveries[i] for i in range(n))
            sum_x2 = sum(x * x for x in prices)
            sum_y2 = sum(y * y for y in deliveries)
            
            denominator = math.sqrt((n * sum_x2 - sum_x * sum_x) * (n * sum_y2 - sum_y * sum_y))
            if denominator != 0:
                correlation = (n * sum_xy - sum_x * sum_y) / denominator
        
        # Symbol totals
        current_total = sum(d['deliv_qty'] for d in symbol_data)
        previous_total = sum(d['prev_deliv_qty'] for d in symbol_data)
        total_traded = sum(d['ttl_trd_qnty'] for d in symbol_data)
        
        cursor.close()
        conn.close()
        
        return {
            'selected_symbol': selected_symbol,
            'symbol_data': symbol_data,
            'kpis': {
                'current_month_total': current_total,
                'previous_month_total': previous_total,
                'price_delivery_correlation': correlation,
                'total_traded_quantity': total_traded,
                'delivery_percentage': (current_total / total_traded * 100) if total_traded > 0 else 0,
                'month_over_month_change': ((current_total - previous_total) / previous_total * 100) if previous_total > 0 else 0
            }
        }
        
    except Exception as e:
        return {'error': str(e)}

def create_enhanced_html_dashboard():
    """Create the complete three-tab dashboard"""
    
    # Get data for all tabs
    tab1_data = get_tab1_data()
    tab2_data = get_tab2_data()
    tab3_data = get_tab3_data()
    
    html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>NSE Delivery Analysis Dashboard - Enhanced</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        
        body {{ 
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; 
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
        }}
        
        .dashboard-container {{ 
            max-width: 1400px; 
            margin: 0 auto; 
            background: rgba(255, 255, 255, 0.98);
            min-height: 100vh;
            box-shadow: 0 0 50px rgba(0, 0, 0, 0.1);
        }}
        
        .header {{ 
            background: linear-gradient(135deg, #2c3e50 0%, #3498db 100%);
            color: white; 
            padding: 25px; 
            text-align: center; 
            box-shadow: 0 4px 20px rgba(0, 0, 0, 0.1);
        }}
        
        .header h1 {{ 
            font-size: 2.2em; 
            margin-bottom: 8px; 
            text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
        }}
        
        .header p {{ 
            font-size: 1.1em; 
            opacity: 0.9; 
        }}
        
        .tab-navigation {{ 
            display: flex; 
            background: #34495e; 
            border-bottom: 3px solid #3498db;
        }}
        
        .tab-button {{ 
            flex: 1; 
            padding: 18px 25px; 
            background: transparent; 
            color: white; 
            border: none; 
            cursor: pointer; 
            font-size: 1em; 
            font-weight: 500;
            transition: all 0.3s ease;
            border-right: 1px solid #2c3e50;
        }}
        
        .tab-button:last-child {{ border-right: none; }}
        
        .tab-button:hover {{ 
            background: rgba(52, 152, 219, 0.2); 
        }}
        
        .tab-button.active {{ 
            background: #3498db; 
            box-shadow: inset 0 -3px 0 #2980b9;
        }}
        
        .tab-content {{ 
            display: none; 
            padding: 30px; 
            background: #f8f9fa;
        }}
        
        .tab-content.active {{ 
            display: block; 
        }}
        
        .kpi-grid {{ 
            display: grid; 
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); 
            gap: 25px; 
            margin-bottom: 30px; 
        }}
        
        .kpi-card {{ 
            background: white;
            padding: 25px;
            border-radius: 12px;
            text-align: center;
            box-shadow: 0 8px 25px rgba(0, 0, 0, 0.1);
            transition: transform 0.3s ease, box-shadow 0.3s ease;
            border-left: 5px solid #3498db;
        }}
        
        .kpi-card:hover {{ 
            transform: translateY(-8px); 
            box-shadow: 0 15px 35px rgba(0, 0, 0, 0.15);
        }}
        
        .kpi-value {{ 
            font-size: 2.8em; 
            font-weight: bold; 
            margin-bottom: 12px;
            background: linear-gradient(135deg, #3498db, #2980b9);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
        }}
        
        .kpi-label {{ 
            color: #555; 
            font-size: 1.1em; 
            font-weight: 500;
            line-height: 1.4;
        }}
        
        .chart-container {{ 
            background: white; 
            border-radius: 12px; 
            padding: 25px; 
            box-shadow: 0 8px 25px rgba(0, 0, 0, 0.1);
            margin-bottom: 30px;
        }}
        
        .chart-title {{ 
            font-size: 1.6em; 
            color: #2c3e50; 
            margin-bottom: 20px; 
            text-align: center;
            font-weight: 600;
        }}
        
        .chart-grid {{ 
            display: grid; 
            grid-template-columns: 1fr 1fr; 
            gap: 30px; 
            margin-bottom: 30px;
        }}
        
        .chart-grid-full {{ 
            grid-column: 1 / -1; 
        }}
        
        .data-table {{ 
            background: white; 
            border-radius: 12px; 
            padding: 25px; 
            box-shadow: 0 8px 25px rgba(0, 0, 0, 0.1);
            overflow-x: auto;
        }}
        
        table {{ 
            width: 100%; 
            border-collapse: collapse; 
            font-size: 0.95em;
        }}
        
        th, td {{ 
            padding: 15px 12px; 
            text-align: left; 
            border-bottom: 1px solid #eee; 
        }}
        
        th {{ 
            background: linear-gradient(135deg, #f1f3f4, #e8eaed); 
            font-weight: 600; 
            color: #2c3e50;
            position: sticky;
            top: 0;
        }}
        
        tr:hover {{ 
            background: #f8f9fa; 
        }}
        
        .symbol-link {{ 
            color: #3498db; 
            cursor: pointer; 
            font-weight: 600;
            text-decoration: none;
        }}
        
        .symbol-link:hover {{ 
            color: #2980b9; 
            text-decoration: underline;
        }}
        
        .positive {{ color: #27ae60; font-weight: 600; }}
        .negative {{ color: #e74c3c; font-weight: 600; }}
        .neutral {{ color: #7f8c8d; }}
        
        .error {{ 
            color: #e74c3c; 
            text-align: center; 
            padding: 50px; 
            font-size: 1.2em;
            background: white;
            border-radius: 12px;
            margin: 20px;
        }}
        
        .info-badge {{ 
            background: #3498db; 
            color: white; 
            padding: 6px 12px; 
            border-radius: 20px; 
            font-size: 0.85em;
            font-weight: 500;
            display: inline-block;
            margin-bottom: 20px;
        }}
        
        @media (max-width: 768px) {{
            .chart-grid {{ grid-template-columns: 1fr; }}
            .kpi-grid {{ grid-template-columns: 1fr; }}
            .tab-button {{ padding: 12px 15px; font-size: 0.9em; }}
            .header h1 {{ font-size: 1.8em; }}
        }}
    </style>
</head>
<body>
    <div class="dashboard-container">
        <div class="header">
            <h1>📊 NSE Delivery Analysis Dashboard</h1>
            <p>Professional Market Intelligence Platform - Three Tab Analysis</p>
        </div>
        
        <div class="tab-navigation">
            <button class="tab-button active" onclick="showTab('tab1')">
                📈 Daily Performance
            </button>
            <button class="tab-button" onclick="showTab('tab2')">
                📊 Monthly Trends
            </button>
            <button class="tab-button" onclick="showTab('tab3')">
                🔍 Symbol Deep Dive
            </button>
        </div>
        
        <!-- TAB 1: Current Month Daily Performance -->
        <div id="tab1" class="tab-content active">
            <div class="info-badge">Tab 1: Current Month Daily Performance - Symbol-wise delivery analysis</div>
"""

    # Tab 1 Content
    if 'error' not in tab1_data:
        html += f"""
            <div class="kpi-grid">
                <div class="kpi-card">
                    <div class="kpi-value">{tab1_data['kpis']['symbols_with_higher_delivery']:,}</div>
                    <div class="kpi-label">Symbols with Higher Delivery</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-value">{tab1_data['kpis']['total_symbols']:,}</div>
                    <div class="kpi-label">Total Symbols Analyzed</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-value">{tab1_data['kpis']['avg_delivery_percentage']:.2f}%</div>
                    <div class="kpi-label">Average Delivery Percentage</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-value">{tab1_data['kpis']['max_delivery_percentage']:.2f}%</div>
                    <div class="kpi-label">Maximum Delivery Percentage</div>
                </div>
            </div>
            
            <div class="chart-grid">
                <div class="chart-container">
                    <div class="chart-title">🔥 Delivery Percentage Heatmap</div>
                    <div id="heatmap-chart" style="height: 400px;"></div>
                </div>
                <div class="chart-container">
                    <div class="chart-title">📊 Top Symbols Performance</div>
                    <div id="performance-chart" style="height: 400px;"></div>
                </div>
            </div>
        """
    else:
        html += f'<div class="error">Tab 1 Error: {tab1_data["error"]}</div>'

    html += """
        </div>
        
        <!-- TAB 2: Monthly Trends and Comparison -->
        <div id="tab2" class="tab-content">
            <div class="info-badge">Tab 2: Monthly Trends and Comparison - Cumulative monthly analysis</div>
    """

    # Tab 2 Content
    if 'error' not in tab2_data:
        kpis = tab2_data['monthly_kpis']
        html += f"""
            <div class="kpi-grid">
                <div class="kpi-card">
                    <div class="kpi-value">{kpis['current_month_total']:,}</div>
                    <div class="kpi-label">Current Month Total Delivery</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-value">{kpis['monthly_avg_daily']:,.0f}</div>
                    <div class="kpi-label">Monthly Average Daily Delivery</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-value">{kpis['delivery_vs_traded_ratio']:.2f}%</div>
                    <div class="kpi-label">Delivery vs Traded Ratio</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-value {'positive' if kpis['month_over_month_change'] > 0 else 'negative' if kpis['month_over_month_change'] < 0 else 'neutral'}">{kpis['month_over_month_change']:+.2f}%</div>
                    <div class="kpi-label">Month-over-Month Change</div>
                </div>
            </div>
            
            <div class="chart-grid">
                <div class="chart-container">
                    <div class="chart-title">📈 Monthly Comparison (Bullet Chart)</div>
                    <div id="bullet-chart" style="height: 300px;"></div>
                </div>
                <div class="chart-container">
                    <div class="chart-title">📊 Category Comparison</div>
                    <div id="category-chart" style="height: 300px;"></div>
                </div>
            </div>
            
            <div class="chart-container chart-grid-full">
                <div class="chart-title">🌳 Symbol Contribution Treemap</div>
                <div id="treemap-chart" style="height: 500px;"></div>
            </div>
        """
    else:
        html += f'<div class="error">Tab 2 Error: {tab2_data["error"]}</div>'

    html += """
        </div>
        
        <!-- TAB 3: Symbol-Specific Deep Dive -->
        <div id="tab3" class="tab-content">
            <div class="info-badge">Tab 3: Symbol-Specific Deep Dive - Detailed analysis for individual symbols</div>
    """

    # Tab 3 Content
    if 'error' not in tab3_data:
        kpis = tab3_data['kpis']
        symbol = tab3_data['selected_symbol']
        html += f"""
            <div class="chart-title">🔍 Deep Dive Analysis: <strong>{symbol}</strong></div>
            
            <div class="kpi-grid">
                <div class="kpi-card">
                    <div class="kpi-value">{kpis['current_month_total']:,}</div>
                    <div class="kpi-label">Total Delivery Quantity</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-value">{kpis['price_delivery_correlation']:.3f}</div>
                    <div class="kpi-label">Price-Delivery Correlation</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-value">{kpis['delivery_percentage']:.2f}%</div>
                    <div class="kpi-label">Delivery Percentage</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-value {'positive' if kpis['month_over_month_change'] > 0 else 'negative' if kpis['month_over_month_change'] < 0 else 'neutral'}">{kpis['month_over_month_change']:+.2f}%</div>
                    <div class="kpi-label">Month-over-Month Change</div>
                </div>
            </div>
            
            <div class="chart-grid">
                <div class="chart-container">
                    <div class="chart-title">📈 Price vs Delivery Scatter Plot</div>
                    <div id="scatter-chart" style="height: 400px;"></div>
                </div>
                <div class="chart-container">
                    <div class="chart-title">🎯 Delivery Percentage Donut</div>
                    <div id="donut-chart" style="height: 400px;"></div>
                </div>
            </div>
            
            <div class="chart-container chart-grid-full">
                <div class="chart-title">📊 Dual-Axis: Delivery Quantity & Turnover</div>
                <div id="dual-axis-chart" style="height: 400px;"></div>
            </div>
        """
    else:
        html += f'<div class="error">Tab 3 Error: {tab3_data["error"]}</div>'

    html += f"""
        </div>
    </div>
    
    <script>
        // Data for charts
        const tab1Data = {json.dumps(tab1_data)};
        const tab2Data = {json.dumps(tab2_data)};
        const tab3Data = {json.dumps(tab3_data)};
        
        // Tab navigation
        function showTab(tabId) {{
            // Hide all tabs
            document.querySelectorAll('.tab-content').forEach(tab => {{
                tab.classList.remove('active');
            }});
            document.querySelectorAll('.tab-button').forEach(btn => {{
                btn.classList.remove('active');
            }});
            
            // Show selected tab
            document.getElementById(tabId).classList.add('active');
            event.target.classList.add('active');
            
            // Load charts for the active tab
            if (tabId === 'tab1') loadTab1Charts();
            else if (tabId === 'tab2') loadTab2Charts();
            else if (tabId === 'tab3') loadTab3Charts();
        }}
        
        // Tab 1 Charts
        function loadTab1Charts() {{
            if (tab1Data.error) return;
            
            // Heatmap Chart
            const heatmapData = {{
                z: [tab1Data.heatmap_data.map(d => d.delivery_pct)],
                x: tab1Data.heatmap_data.map(d => d.symbol),
                y: ['Delivery %'],
                type: 'heatmap',
                colorscale: [
                    [0, '#f0f9ff'],
                    [0.5, '#3b82f6'],
                    [1, '#1e40af']
                ],
                showscale: true
            }};
            
            Plotly.newPlot('heatmap-chart', [heatmapData], {{
                title: 'Delivery Percentage Heatmap',
                xaxis: {{ title: 'Symbol' }},
                yaxis: {{ title: '' }},
                font: {{ family: 'Segoe UI, sans-serif' }}
            }}, {{responsive: true}});
            
            // Performance Chart
            const performanceData = {{
                x: tab1Data.daily_data.slice(0, 15).map(d => d.symbol),
                y: tab1Data.daily_data.slice(0, 15).map(d => d.percentage_change),
                type: 'bar',
                marker: {{
                    color: tab1Data.daily_data.slice(0, 15).map(d => d.percentage_change >= 0 ? '#27ae60' : '#e74c3c')
                }}
            }};
            
            Plotly.newPlot('performance-chart', [performanceData], {{
                title: 'Percentage Change from Previous Month',
                xaxis: {{ title: 'Symbol' }},
                yaxis: {{ title: 'Change (%)' }},
                font: {{ family: 'Segoe UI, sans-serif' }}
            }}, {{responsive: true}});
        }}
        
        // Tab 2 Charts
        function loadTab2Charts() {{
            if (tab2Data.error) return;
            
            const kpis = tab2Data.monthly_kpis;
            
            // Bullet Chart (using bar chart)
            const bulletData = [
                {{
                    x: [kpis.current_month_total],
                    y: ['Current Month'],
                    type: 'bar',
                    orientation: 'h',
                    marker: {{ color: '#3498db' }},
                    name: 'Current Month'
                }},
                {{
                    x: [kpis.previous_month_total],
                    y: ['Target (Previous)'],
                    type: 'bar',
                    orientation: 'h',
                    marker: {{ color: '#95a5a6' }},
                    name: 'Previous Month'
                }}
            ];
            
            Plotly.newPlot('bullet-chart', bulletData, {{
                title: 'Monthly Delivery Comparison',
                xaxis: {{ title: 'Delivery Quantity' }},
                font: {{ family: 'Segoe UI, sans-serif' }},
                showlegend: true
            }}, {{responsive: true}});
            
            // Category Chart
            const categoryData = {{
                x: tab2Data.category_data.map(d => d.category),
                y: tab2Data.category_data.map(d => d.current_total),
                type: 'bar',
                marker: {{ color: '#e67e22' }},
                name: 'Current Month'
            }};
            
            Plotly.newPlot('category-chart', [categoryData], {{
                title: 'Category-wise Delivery',
                xaxis: {{ title: 'Category' }},
                yaxis: {{ title: 'Delivery Quantity' }},
                font: {{ family: 'Segoe UI, sans-serif' }}
            }}, {{responsive: true}});
            
            // Treemap Chart
            const treemapData = {{
                type: 'treemap',
                labels: tab2Data.treemap_data.map(d => d.symbol),
                values: tab2Data.treemap_data.map(d => d.total_delivery),
                parents: tab2Data.treemap_data.map(() => ''),
                textinfo: 'label+value',
                marker: {{
                    colorscale: [
                        [0, '#e3f2fd'],
                        [1, '#1976d2']
                    ],
                    cmid: 0
                }}
            }};
            
            Plotly.newPlot('treemap-chart', [treemapData], {{
                title: 'Symbol Contribution to Total Delivery',
                font: {{ family: 'Segoe UI, sans-serif' }}
            }}, {{responsive: true}});
        }}
        
        // Tab 3 Charts
        function loadTab3Charts() {{
            if (tab3Data.error) return;
            
            const symbolData = tab3Data.symbol_data;
            
            // Scatter Plot
            const scatterData = {{
                x: symbolData.map(d => d.deliv_qty),
                y: symbolData.map(d => d.close_price),
                mode: 'markers',
                type: 'scatter',
                marker: {{
                    color: '#3498db',
                    size: 8
                }},
                name: 'Price vs Delivery'
            }};
            
            Plotly.newPlot('scatter-chart', [scatterData], {{
                title: 'Price vs Delivery Quantity Correlation',
                xaxis: {{ title: 'Delivery Quantity' }},
                yaxis: {{ title: 'Close Price' }},
                font: {{ family: 'Segoe UI, sans-serif' }}
            }}, {{responsive: true}});
            
            // Donut Chart
            const deliveryTotal = tab3Data.kpis.current_month_total;
            const tradedTotal = tab3Data.kpis.total_traded_quantity;
            const nonDelivery = tradedTotal - deliveryTotal;
            
            const donutData = {{
                values: [deliveryTotal, nonDelivery],
                labels: ['Delivery Quantity', 'Non-Delivery Quantity'],
                type: 'pie',
                hole: 0.4,
                marker: {{
                    colors: ['#27ae60', '#ecf0f1']
                }}
            }};
            
            Plotly.newPlot('donut-chart', [donutData], {{
                title: 'Delivery vs Total Traded',
                font: {{ family: 'Segoe UI, sans-serif' }}
            }}, {{responsive: true}});
            
            // Dual-Axis Chart
            const deliveryTrace = {{
                x: symbolData.map(d => d.trade_date),
                y: symbolData.map(d => d.deliv_qty),
                type: 'scatter',
                mode: 'lines+markers',
                name: 'Delivery Quantity',
                line: {{ color: '#3498db' }}
            }};
            
            const turnoverTrace = {{
                x: symbolData.map(d => d.trade_date),
                y: symbolData.map(d => d.turnover),
                type: 'bar',
                name: 'Turnover (Lacs)',
                yaxis: 'y2',
                marker: {{ color: '#e67e22' }}
            }};
            
            Plotly.newPlot('dual-axis-chart', [deliveryTrace, turnoverTrace], {{
                title: 'Delivery Quantity vs Turnover Over Time',
                xaxis: {{ title: 'Date' }},
                yaxis: {{ 
                    title: 'Delivery Quantity',
                    side: 'left'
                }},
                yaxis2: {{
                    title: 'Turnover (Lacs)',
                    side: 'right',
                    overlaying: 'y'
                }},
                font: {{ family: 'Segoe UI, sans-serif' }}
            }}, {{responsive: true}});
        }}
        
        // Initialize with Tab 1
        document.addEventListener('DOMContentLoaded', function() {{
            loadTab1Charts();
        }});
    </script>
</body>
</html>
    """
    
    return html

def main():
    print("🚀 Generating Enhanced NSE Dashboard - Three Tab Structure")
    print("=" * 70)
    
    print("📊 Loading data for all tabs...")
    html_content = create_enhanced_html_dashboard()
    
    # Save to temporary file
    temp_file = os.path.join(tempfile.gettempdir(), 'nse_enhanced_dashboard.html')
    with open(temp_file, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"💾 Enhanced dashboard saved to: {temp_file}")
    print("🌐 Opening enhanced dashboard in your default browser...")
    
    # Open in browser
    webbrowser.open(f'file://{temp_file}')
    
    print("=" * 70)
    print("✅ SUCCESS! Your Enhanced NSE Dashboard is now open!")
    print("📊 Dashboard Features:")
    print("   📈 Tab 1: Current Month Daily Performance")
    print("      • Delivery percentage heatmap")
    print("      • Symbol performance charts")
    print("      • KPIs: Higher delivery count, avg/max percentages")
    print()
    print("   📊 Tab 2: Monthly Trends and Comparison")
    print("      • Bullet chart for monthly comparison")
    print("      • Category-wise analysis")
    print("      • Treemap for symbol contribution")
    print()
    print("   🔍 Tab 3: Symbol-Specific Deep Dive")
    print("      • Price vs delivery correlation scatter plot")
    print("      • Delivery percentage donut chart")
    print("      • Dual-axis delivery quantity vs turnover")
    print("=" * 70)

if __name__ == '__main__':
    main()