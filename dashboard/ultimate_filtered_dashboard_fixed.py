#!/usr/bin/env python3
"""
🚀 ULTIMATE NSE ANALYTICS DASHBOARD WITH COMPREHENSIVE FILTERS 🚀
===============================================================

Features:
- 10+ Filter Categories with 50+ Filter Options
- Real-time Chart Updates
- Filter Presets & Quick Actions
- Advanced Search & Pattern Matching
- Performance Optimized for 31K+ Records
- Beautiful Glassmorphism UI
"""

import pyodbc
import json
from datetime import datetime, timedelta, date
import webbrowser
import os
import decimal

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.strftime('%Y-%m-%d')
        elif isinstance(obj, decimal.Decimal):
            return float(obj)
        return super().default(obj)

class UltimateNSEFilteredDashboard:
    def __init__(self):
        self.connection_string = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=SRIKIRANREDDY\\SQLEXPRESS;DATABASE=master;Trusted_Connection=yes;'
        self.data = None
        self.filtered_data = None
        
    def connect_and_fetch_data(self):
        """Fetch all NSE data with optimized query"""
        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            
            query = """
            SELECT 
                current_trade_date,
                symbol,
                series,
                current_close_price,
                current_open_price,
                current_high_price,
                current_low_price,
                current_ttl_trd_qnty,
                current_turnover_lacs,
                current_no_of_trades,
                current_deliv_qty,
                current_deliv_per,
                previous_close_price,
                previous_ttl_trd_qnty,
                previous_deliv_per,
                delivery_increase_abs,
                delivery_increase_pct,
                index_name,
                category,
                -- Calculated Fields
                CAST(CASE 
                    WHEN previous_close_price IS NULL OR previous_close_price = 0 THEN 0
                    ELSE ((current_close_price - previous_close_price) / previous_close_price) * 100 
                END AS DECIMAL(10,2)) as price_change_pct,
                CAST(CASE 
                    WHEN current_close_price IS NULL OR current_close_price = 0 THEN 0
                    ELSE ((current_high_price - current_low_price) / current_close_price) * 100 
                END AS DECIMAL(10,2)) as volatility_pct,
                CAST(CASE 
                    WHEN previous_ttl_trd_qnty IS NULL OR previous_ttl_trd_qnty = 0 THEN 0
                    ELSE ((current_ttl_trd_qnty - previous_ttl_trd_qnty) / previous_ttl_trd_qnty) * 100 
                END AS DECIMAL(10,2)) as volume_change_pct,
                CASE 
                    WHEN current_close_price < 50 THEN 'Penny'
                    WHEN current_close_price BETWEEN 50 AND 200 THEN 'Low'
                    WHEN current_close_price BETWEEN 200 AND 500 THEN 'Mid'
                    WHEN current_close_price BETWEEN 500 AND 1000 THEN 'High'
                    ELSE 'Premium'
                END as price_category,
                CASE 
                    WHEN current_ttl_trd_qnty < 10000 THEN 'Low'
                    WHEN current_ttl_trd_qnty BETWEEN 10000 AND 100000 THEN 'Medium'
                    WHEN current_ttl_trd_qnty BETWEEN 100000 AND 1000000 THEN 'High'
                    ELSE 'Very High'
                END as volume_category,
                CASE 
                    WHEN current_deliv_per < 20 THEN 'Low'
                    WHEN current_deliv_per BETWEEN 20 AND 40 THEN 'Medium'
                    WHEN current_deliv_per BETWEEN 40 AND 60 THEN 'High'
                    WHEN current_deliv_per BETWEEN 60 AND 80 THEN 'Very High'
                    ELSE 'Extreme'
                END as delivery_category,
                CASE 
                    WHEN delivery_increase_pct < 0 THEN 'Decreased'
                    WHEN delivery_increase_pct = 0 THEN 'No Change'
                    WHEN delivery_increase_pct BETWEEN 0 AND 25 THEN 'Low Increase'
                    WHEN delivery_increase_pct BETWEEN 25 AND 50 THEN 'Moderate Increase'
                    WHEN delivery_increase_pct BETWEEN 50 AND 100 THEN 'High Increase'
                    ELSE 'Very High Increase'
                END as delivery_trend
            FROM step03_compare_monthvspreviousmonth 
            WHERE current_trade_date IS NOT NULL 
                AND symbol IS NOT NULL
                AND current_close_price > 0
            ORDER BY current_trade_date DESC, current_turnover_lacs DESC
            """
            
            cursor.execute(query)
            columns = [column[0] for column in cursor.description]
            rows = cursor.fetchall()
            
            self.data = []
            for row in rows:
                record = {}
                for i, value in enumerate(row):
                    if isinstance(value, datetime):
                        record[columns[i]] = value.strftime('%Y-%m-%d')
                    elif hasattr(value, 'date'):  # Handle date objects
                        record[columns[i]] = value.strftime('%Y-%m-%d') if value else None
                    elif value is None:
                        record[columns[i]] = None
                    else:
                        record[columns[i]] = value
                self.data.append(record)
            
            conn.close()
            print(f"✅ Loaded {len(self.data):,} records successfully!")
            return True
            
        except Exception as e:
            print(f"❌ Database error: {e}")
            return False

    def get_filter_options(self):
        """Generate all available filter options from data"""
        if not self.data:
            return {}
            
        # Extract unique values for dropdowns
        index_names = sorted(list(set([r['index_name'] for r in self.data if r['index_name']])))
        categories = sorted(list(set([r['category'] for r in self.data if r['category']])))
        price_categories = sorted(list(set([r['price_category'] for r in self.data if r['price_category']])))
        volume_categories = sorted(list(set([r['volume_category'] for r in self.data if r['volume_category']])))
        delivery_categories = sorted(list(set([r['delivery_category'] for r in self.data if r['delivery_category']])))
        delivery_trends = sorted(list(set([r['delivery_trend'] for r in self.data if r['delivery_trend']])))
        
        # Calculate ranges
        prices = [r['current_close_price'] for r in self.data if r['current_close_price']]
        volumes = [r['current_ttl_trd_qnty'] for r in self.data if r['current_ttl_trd_qnty']]
        delivery_pcts = [r['current_deliv_per'] for r in self.data if r['current_deliv_per']]
        turnovers = [r['current_turnover_lacs'] for r in self.data if r['current_turnover_lacs']]
        
        dates = sorted(list(set([r['current_trade_date'] for r in self.data if r['current_trade_date']])))
        
        return {
            'index_names': index_names,
            'categories': categories,
            'price_categories': price_categories,
            'volume_categories': volume_categories,
            'delivery_categories': delivery_categories,
            'delivery_trends': delivery_trends,
            'price_range': {'min': min(prices), 'max': max(prices)},
            'volume_range': {'min': min(volumes), 'max': max(volumes)},
            'delivery_range': {'min': min(delivery_pcts), 'max': max(delivery_pcts)},
            'turnover_range': {'min': min(turnovers), 'max': max(turnovers)},
            'date_range': {'min': dates[0], 'max': dates[-1]},
            'total_records': len(self.data)
        }

    def generate_dashboard_html(self):
        """Generate the ultimate filtered dashboard"""
        filter_options = self.get_filter_options()
        # Create JSON data with custom encoder
        data_json = json.dumps(self.data, cls=CustomJSONEncoder)
        filter_options_json = json.dumps(filter_options, cls=CustomJSONEncoder)
        
        # Create index options HTML
        index_options = ""
        for idx in filter_options['index_names']:
            index_options += f'<option value="{idx}">{idx}</option>'
        
        # Create category options HTML
        category_options = ""
        for cat in filter_options['categories']:
            category_options += f'<option value="{cat}">{cat}</option>'
            
        # Create price category options HTML
        price_category_options = ""
        for cat in filter_options['price_categories']:
            price_category_options += f'<option value="{cat}">{cat}</option>'
            
        # Create volume category options HTML
        volume_category_options = ""
        for cat in filter_options['volume_categories']:
            volume_category_options += f'<option value="{cat}">{cat}</option>'
            
        # Create delivery category options HTML
        delivery_category_options = ""
        for cat in filter_options['delivery_categories']:
            delivery_category_options += f'<option value="{cat}">{cat}</option>'
            
        # Create delivery trend options HTML
        delivery_trend_options = ""
        for trend in filter_options['delivery_trends']:
            delivery_trend_options += f'<option value="{trend}">{trend}</option>'
        
        html_content = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>🚀 Ultimate NSE Analytics Dashboard</title>
    
    <!-- Chart Libraries -->
    <script src="https://cdn.jsdelivr.net/npm/apexcharts@3.44.0/dist/apexcharts.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.min.js"></script>
    <script src="https://d3js.org/d3.v7.min.js"></script>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    
    <!-- UI Libraries -->
    <script src="https://cdnjs.cloudflare.com/ajax/libs/gsap/3.12.2/gsap.min.js"></script>
    <script src="https://unpkg.com/aos@2.3.1/dist/aos.js"></script>
    <link href="https://unpkg.com/aos@2.3.1/dist/aos.css" rel="stylesheet">
    <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css" rel="stylesheet">
    
    <style>
        :root {{
            --primary-gradient: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            --secondary-gradient: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
            --success-gradient: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);
            --glass-bg: rgba(255, 255, 255, 0.1);
            --glass-border: rgba(255, 255, 255, 0.2);
            --text-primary: #2d3748;
            --text-secondary: #4a5568;
            --shadow-xl: 0 25px 50px -12px rgba(0, 0, 0, 0.25);
        }}

        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}

        body {{
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 50%, #f093fb 100%);
            min-height: 100vh;
            color: var(--text-primary);
        }}

        /* Header */
        .header {{
            background: var(--glass-bg);
            backdrop-filter: blur(20px);
            border-bottom: 1px solid var(--glass-border);
            padding: 1rem 2rem;
            position: sticky;
            top: 0;
            z-index: 1000;
            box-shadow: var(--shadow-xl);
        }}

        .header-content {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            max-width: 1400px;
            margin: 0 auto;
        }}

        .logo {{
            display: flex;
            align-items: center;
            gap: 1rem;
            font-size: 1.5rem;
            font-weight: 700;
            color: white;
        }}

        .stats-summary {{
            display: flex;
            gap: 2rem;
            color: white;
            font-size: 0.9rem;
        }}

        .stat-item {{
            text-align: center;
        }}

        .stat-value {{
            font-size: 1.2rem;
            font-weight: 700;
            display: block;
        }}

        /* Filter Panel */
        .filter-panel {{
            background: var(--glass-bg);
            backdrop-filter: blur(20px);
            border: 1px solid var(--glass-border);
            margin: 2rem;
            padding: 2rem;
            border-radius: 20px;
            box-shadow: var(--shadow-xl);
        }}

        .filter-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 2rem;
            color: white;
        }}

        .filter-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 1.5rem;
            margin-bottom: 2rem;
        }}

        .filter-group {{
            background: rgba(255, 255, 255, 0.05);
            padding: 1.5rem;
            border-radius: 15px;
            border: 1px solid rgba(255, 255, 255, 0.1);
        }}

        .filter-label {{
            display: block;
            color: white;
            font-weight: 600;
            margin-bottom: 0.5rem;
            font-size: 0.9rem;
        }}

        .filter-input {{
            width: 100%;
            padding: 0.75rem;
            border: 1px solid rgba(255, 255, 255, 0.2);
            border-radius: 10px;
            background: rgba(255, 255, 255, 0.1);
            color: white;
            font-size: 0.9rem;
            backdrop-filter: blur(10px);
        }}

        .filter-input::placeholder {{
            color: rgba(255, 255, 255, 0.6);
        }}

        .filter-input:focus {{
            outline: none;
            border-color: #4facfe;
            box-shadow: 0 0 20px rgba(79, 172, 254, 0.3);
        }}

        .range-input {{
            -webkit-appearance: none;
            appearance: none;
            width: 100%;
            height: 6px;
            border-radius: 3px;
            background: rgba(255, 255, 255, 0.2);
            outline: none;
            margin-bottom: 0.5rem;
        }}

        .range-input::-webkit-slider-thumb {{
            -webkit-appearance: none;
            appearance: none;
            width: 20px;
            height: 20px;
            border-radius: 50%;
            background: var(--success-gradient);
            cursor: pointer;
            border: 2px solid white;
            box-shadow: 0 2px 10px rgba(0, 0, 0, 0.2);
        }}

        .range-values {{
            display: flex;
            justify-content: space-between;
            color: rgba(255, 255, 255, 0.8);
            font-size: 0.8rem;
        }}

        /* Quick Filter Buttons */
        .quick-filters {{
            display: flex;
            flex-wrap: wrap;
            gap: 1rem;
            margin-bottom: 2rem;
        }}

        .quick-filter-btn {{
            background: var(--primary-gradient);
            color: white;
            border: none;
            padding: 0.75rem 1.5rem;
            border-radius: 25px;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.3s ease;
            font-size: 0.9rem;
            position: relative;
            overflow: hidden;
        }}

        .quick-filter-btn::before {{
            content: '';
            position: absolute;
            top: 0;
            left: -100%;
            width: 100%;
            height: 100%;
            background: linear-gradient(90deg, transparent, rgba(255,255,255,0.2), transparent);
            transition: left 0.5s;
        }}

        .quick-filter-btn:hover {{
            transform: translateY(-2px);
            box-shadow: 0 10px 25px rgba(0, 0, 0, 0.2);
        }}

        .quick-filter-btn:hover::before {{
            left: 100%;
        }}

        .quick-filter-btn.active {{
            background: var(--success-gradient);
            box-shadow: 0 0 20px rgba(79, 172, 254, 0.5);
        }}

        /* Results Panel */
        .results-panel {{
            background: var(--glass-bg);
            backdrop-filter: blur(20px);
            border: 1px solid var(--glass-border);
            margin: 0 2rem 2rem;
            padding: 2rem;
            border-radius: 20px;
            box-shadow: var(--shadow-xl);
        }}

        .results-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 2rem;
            color: white;
        }}

        .results-count {{
            background: var(--success-gradient);
            color: white;
            padding: 0.5rem 1rem;
            border-radius: 15px;
            font-weight: 600;
        }}

        /* Charts Grid */
        .charts-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(400px, 1fr));
            gap: 2rem;
            margin-bottom: 2rem;
        }}

        .chart-container {{
            background: rgba(255, 255, 255, 0.95);
            border-radius: 15px;
            padding: 1.5rem;
            box-shadow: 0 10px 30px rgba(0, 0, 0, 0.1);
            position: relative;
            overflow: hidden;
        }}

        .chart-title {{
            color: var(--text-primary);
            font-weight: 700;
            margin-bottom: 1rem;
            font-size: 1.1rem;
        }}

        /* Data Table */
        .data-table {{
            background: rgba(255, 255, 255, 0.95);
            border-radius: 15px;
            overflow: hidden;
            box-shadow: 0 10px 30px rgba(0, 0, 0, 0.1);
        }}

        .table-header {{
            background: var(--primary-gradient);
            color: white;
            padding: 1rem;
            font-weight: 600;
        }}

        .table-content {{
            max-height: 400px;
            overflow-y: auto;
        }}

        .table-row {{
            display: grid;
            grid-template-columns: 1fr 1fr 1fr 1fr 1fr 1fr;
            padding: 1rem;
            border-bottom: 1px solid #e2e8f0;
            transition: background-color 0.2s;
        }}

        .table-row:hover {{
            background-color: rgba(79, 172, 254, 0.1);
        }}

        .table-cell {{
            color: var(--text-secondary);
            font-size: 0.9rem;
        }}

        .table-cell.symbol {{
            font-weight: 600;
            color: var(--text-primary);
        }}

        .table-cell.positive {{
            color: #48bb78;
            font-weight: 600;
        }}

        .table-cell.negative {{
            color: #f56565;
            font-weight: 600;
        }}

        /* Loading States */
        .loading {{
            display: flex;
            align-items: center;
            justify-content: center;
            padding: 3rem;
            color: white;
        }}

        .spinner {{
            width: 40px;
            height: 40px;
            border: 4px solid rgba(255, 255, 255, 0.3);
            border-left: 4px solid white;
            border-radius: 50%;
            animation: spin 1s ease-in-out infinite;
            margin-right: 1rem;
        }}

        @keyframes spin {{
            to {{ transform: rotate(360deg); }}
        }}

        /* Responsive Design */
        @media (max-width: 768px) {{
            .filter-grid {{
                grid-template-columns: 1fr;
            }}
            
            .charts-grid {{
                grid-template-columns: 1fr;
            }}
            
            .header-content {{
                flex-direction: column;
                gap: 1rem;
            }}
            
            .stats-summary {{
                flex-wrap: wrap;
                justify-content: center;
            }}
        }}
    </style>
</head>
<body>
    <!-- Header -->
    <div class="header">
        <div class="header-content">
            <div class="logo">
                <i class="fas fa-chart-line"></i>
                <span>Ultimate NSE Analytics</span>
            </div>
            <div class="stats-summary">
                <div class="stat-item">
                    <span class="stat-value" id="total-records">{filter_options['total_records']:,}</span>
                    <span>Total Records</span>
                </div>
                <div class="stat-item">
                    <span class="stat-value" id="filtered-count">{filter_options['total_records']:,}</span>
                    <span>Filtered Results</span>
                </div>
                <div class="stat-item">
                    <span class="stat-value">{len(filter_options['index_names'])}</span>
                    <span>Indices</span>
                </div>
                <div class="stat-item">
                    <span class="stat-value">{filter_options['date_range']['min']} - {filter_options['date_range']['max']}</span>
                    <span>Date Range</span>
                </div>
            </div>
        </div>
    </div>

    <!-- Filter Panel -->
    <div class="filter-panel" data-aos="fade-up">
        <div class="filter-header">
            <h2><i class="fas fa-filter"></i> Advanced Filters</h2>
            <button class="quick-filter-btn" onclick="clearAllFilters()">
                <i class="fas fa-refresh"></i> Clear All
            </button>
        </div>

        <!-- Quick Filter Buttons -->
        <div class="quick-filters">
            <button class="quick-filter-btn" onclick="applyQuickFilter('topGainers')">
                <i class="fas fa-arrow-up"></i> Top Gainers
            </button>
            <button class="quick-filter-btn" onclick="applyQuickFilter('topVolume')">
                <i class="fas fa-volume-high"></i> High Volume
            </button>
            <button class="quick-filter-btn" onclick="applyQuickFilter('topDelivery')">
                <i class="fas fa-truck"></i> High Delivery
            </button>
            <button class="quick-filter-btn" onclick="applyQuickFilter('momentum')">
                <i class="fas fa-rocket"></i> Momentum Stocks
            </button>
            <button class="quick-filter-btn" onclick="applyQuickFilter('institutional')">
                <i class="fas fa-building"></i> Institutional Interest
            </button>
            <button class="quick-filter-btn" onclick="applyQuickFilter('value')">
                <i class="fas fa-gem"></i> Value Picks
            </button>
        </div>

        <!-- Filter Grid -->
        <div class="filter-grid">
            <!-- Symbol Search -->
            <div class="filter-group">
                <label class="filter-label">
                    <i class="fas fa-search"></i> Symbol Search
                </label>
                <input type="text" class="filter-input" id="symbol-search" 
                       placeholder="Search symbols (e.g., RELIANCE, TCS*)" 
                       oninput="debounceFilter()">
            </div>

            <!-- Index Filter -->
            <div class="filter-group">
                <label class="filter-label">
                    <i class="fas fa-list"></i> Index
                </label>
                <select class="filter-input" id="index-filter" onchange="applyFilters()">
                    <option value="">All Indices</option>
                    {index_options}
                </select>
            </div>

            <!-- Category Filter -->
            <div class="filter-group">
                <label class="filter-label">
                    <i class="fas fa-tags"></i> Category
                </label>
                <select class="filter-input" id="category-filter" onchange="applyFilters()">
                    <option value="">All Categories</option>
                    {category_options}
                </select>
            </div>

            <!-- Price Range -->
            <div class="filter-group">
                <label class="filter-label">
                    <i class="fas fa-rupee-sign"></i> Price Range
                </label>
                <input type="range" class="range-input" id="price-min" 
                       min="{filter_options['price_range']['min']}" 
                       max="{filter_options['price_range']['max']}" 
                       value="{filter_options['price_range']['min']}"
                       oninput="updateRangeValues('price')" onchange="applyFilters()">
                <input type="range" class="range-input" id="price-max" 
                       min="{filter_options['price_range']['min']}" 
                       max="{filter_options['price_range']['max']}" 
                       value="{filter_options['price_range']['max']}"
                       oninput="updateRangeValues('price')" onchange="applyFilters()">
                <div class="range-values">
                    <span id="price-min-val">₹{filter_options['price_range']['min']:.0f}</span>
                    <span id="price-max-val">₹{filter_options['price_range']['max']:.0f}</span>
                </div>
            </div>

            <!-- Volume Range -->
            <div class="filter-group">
                <label class="filter-label">
                    <i class="fas fa-chart-bar"></i> Volume Range
                </label>
                <input type="range" class="range-input" id="volume-min" 
                       min="{filter_options['volume_range']['min']}" 
                       max="{filter_options['volume_range']['max']}" 
                       value="{filter_options['volume_range']['min']}"
                       oninput="updateRangeValues('volume')" onchange="applyFilters()">
                <input type="range" class="range-input" id="volume-max" 
                       min="{filter_options['volume_range']['min']}" 
                       max="{filter_options['volume_range']['max']}" 
                       value="{filter_options['volume_range']['max']}"
                       oninput="updateRangeValues('volume')" onchange="applyFilters()">
                <div class="range-values">
                    <span id="volume-min-val">{filter_options['volume_range']['min']:,.0f}</span>
                    <span id="volume-max-val">{filter_options['volume_range']['max']:,.0f}</span>
                </div>
            </div>

            <!-- Delivery Percentage -->
            <div class="filter-group">
                <label class="filter-label">
                    <i class="fas fa-truck"></i> Delivery %
                </label>
                <input type="range" class="range-input" id="delivery-min" 
                       min="{filter_options['delivery_range']['min']}" 
                       max="{filter_options['delivery_range']['max']}" 
                       value="{filter_options['delivery_range']['min']}"
                       oninput="updateRangeValues('delivery')" onchange="applyFilters()">
                <input type="range" class="range-input" id="delivery-max" 
                       min="{filter_options['delivery_range']['min']}" 
                       max="{filter_options['delivery_range']['max']}" 
                       value="{filter_options['delivery_range']['max']}"
                       oninput="updateRangeValues('delivery')" onchange="applyFilters()">
                <div class="range-values">
                    <span id="delivery-min-val">{filter_options['delivery_range']['min']:.1f}%</span>
                    <span id="delivery-max-val">{filter_options['delivery_range']['max']:.1f}%</span>
                </div>
            </div>

            <!-- Price Category -->
            <div class="filter-group">
                <label class="filter-label">
                    <i class="fas fa-layer-group"></i> Price Category
                </label>
                <select class="filter-input" id="price-category-filter" onchange="applyFilters()">
                    <option value="">All Price Categories</option>
                    {price_category_options}
                </select>
            </div>

            <!-- Volume Category -->
            <div class="filter-group">
                <label class="filter-label">
                    <i class="fas fa-signal"></i> Volume Category
                </label>
                <select class="filter-input" id="volume-category-filter" onchange="applyFilters()">
                    <option value="">All Volume Categories</option>
                    {volume_category_options}
                </select>
            </div>

            <!-- Delivery Category -->
            <div class="filter-group">
                <label class="filter-label">
                    <i class="fas fa-layer-group"></i> Delivery Category
                </label>
                <select class="filter-input" id="delivery-category-filter" onchange="applyFilters()">
                    <option value="">All Delivery Categories</option>
                    {delivery_category_options}
                </select>
            </div>

            <!-- Delivery Trend -->
            <div class="filter-group">
                <label class="filter-label">
                    <i class="fas fa-trending-up"></i> Delivery Trend
                </label>
                <select class="filter-input" id="delivery-trend-filter" onchange="applyFilters()">
                    <option value="">All Trends</option>
                    {delivery_trend_options}
                </select>
            </div>

            <!-- Date Range -->
            <div class="filter-group">
                <label class="filter-label">
                    <i class="fas fa-calendar"></i> Date Range
                </label>
                <input type="date" class="filter-input" id="date-from" 
                       value="{filter_options['date_range']['min']}" 
                       min="{filter_options['date_range']['min']}" 
                       max="{filter_options['date_range']['max']}"
                       onchange="applyFilters()">
                <input type="date" class="filter-input" id="date-to" 
                       value="{filter_options['date_range']['max']}" 
                       min="{filter_options['date_range']['min']}" 
                       max="{filter_options['date_range']['max']}"
                       onchange="applyFilters()" style="margin-top: 0.5rem;">
            </div>
        </div>
    </div>

    <!-- Results Panel -->
    <div class="results-panel" data-aos="fade-up" data-aos-delay="200">
        <div class="results-header">
            <h2><i class="fas fa-chart-area"></i> Analytics Dashboard</h2>
            <div class="results-count" id="results-count">
                {filter_options['total_records']:,} Results
            </div>
        </div>

        <!-- Charts Grid -->
        <div class="charts-grid">
            <div class="chart-container">
                <div class="chart-title">📊 Price Distribution</div>
                <div id="price-chart" style="height: 300px;"></div>
            </div>
            <div class="chart-container">
                <div class="chart-title">📈 Volume Analysis</div>
                <div id="volume-chart" style="height: 300px;"></div>
            </div>
            <div class="chart-container">
                <div class="chart-title">🚚 Delivery Trends</div>
                <div id="delivery-chart" style="height: 300px;"></div>
            </div>
            <div class="chart-container">
                <div class="chart-title">🏆 Index Performance</div>
                <div id="index-chart" style="height: 300px;"></div>
            </div>
        </div>

        <!-- Data Table -->
        <div class="data-table">
            <div class="table-header">
                <i class="fas fa-table"></i> Filtered Results (Top 100)
            </div>
            <div class="table-content" id="data-table-content">
                <div class="loading">
                    <div class="spinner"></div>
                    Loading data...
                </div>
            </div>
        </div>
    </div>

    <script>
        // Global Variables
        const originalData = {data_json};
        const filterOptions = {filter_options_json};
        let filteredData = [...originalData];
        let debounceTimer;
        let charts = {{}};

        // Initialize Dashboard
        document.addEventListener('DOMContentLoaded', function() {{
            AOS.init({{
                duration: 800,
                once: true
            }});
            
            applyFilters();
        }});

        // Debounced Filter Function
        function debounceFilter() {{
            clearTimeout(debounceTimer);
            debounceTimer = setTimeout(applyFilters, 300);
        }}

        // Update Range Values Display
        function updateRangeValues(type) {{
            const minVal = document.getElementById(type + '-min').value;
            const maxVal = document.getElementById(type + '-max').value;
            
            if (type === 'price') {{
                document.getElementById('price-min-val').textContent = '₹' + parseInt(minVal).toLocaleString();
                document.getElementById('price-max-val').textContent = '₹' + parseInt(maxVal).toLocaleString();
            }} else if (type === 'volume') {{
                document.getElementById('volume-min-val').textContent = parseInt(minVal).toLocaleString();
                document.getElementById('volume-max-val').textContent = parseInt(maxVal).toLocaleString();
            }} else if (type === 'delivery') {{
                document.getElementById('delivery-min-val').textContent = parseFloat(minVal).toFixed(1) + '%';
                document.getElementById('delivery-max-val').textContent = parseFloat(maxVal).toFixed(1) + '%';
            }}
        }}

        // Apply Quick Filters
        function applyQuickFilter(type) {{
            // Clear current filters first
            clearAllFilters();
            
            // Remove active class from all buttons
            document.querySelectorAll('.quick-filter-btn').forEach(btn => btn.classList.remove('active'));
            
            // Add active class to clicked button
            event.target.classList.add('active');
            
            switch(type) {{
                case 'topGainers':
                    filteredData = originalData.filter(item => 
                        item.price_change_pct && item.price_change_pct > 5
                    );
                    break;
                    
                case 'topVolume':
                    const volumeThreshold = originalData.map(d => d.current_ttl_trd_qnty || 0).sort((a,b) => b-a)[Math.floor(originalData.length * 0.1)];
                    filteredData = originalData.filter(item => 
                        (item.current_ttl_trd_qnty || 0) >= volumeThreshold
                    );
                    break;
                    
                case 'topDelivery':
                    filteredData = originalData.filter(item => 
                        (item.current_deliv_per || 0) > 70
                    );
                    break;
                    
                case 'momentum':
                    filteredData = originalData.filter(item => 
                        (item.delivery_increase_pct || 0) > 50 && 
                        (item.volume_change_pct || 0) > 25
                    );
                    break;
                    
                case 'institutional':
                    filteredData = originalData.filter(item => 
                        (item.current_deliv_per || 0) > 60 && 
                        (item.current_turnover_lacs || 0) > 500 &&
                        ['NIFTY 50', 'NIFTY BANK', 'NIFTY IT'].includes(item.index_name)
                    );
                    break;
                    
                case 'value':
                    filteredData = originalData.filter(item => 
                        (item.current_close_price || 0) < 200 && 
                        (item.delivery_increase_pct || 0) > 25 && 
                        (item.current_deliv_per || 0) > 40
                    );
                    break;
            }}
            
            updateDashboard();
        }}

        // Apply All Filters
        function applyFilters() {{
            let data = [...originalData];
            
            // Symbol Search
            const symbolSearch = document.getElementById('symbol-search').value.toLowerCase();
            if (symbolSearch) {{
                data = data.filter(item => {{
                    const symbol = (item.symbol || '').toLowerCase();
                    if (symbolSearch.includes('*')) {{
                        const pattern = symbolSearch.replace(/\\*/g, '.*');
                        const regex = new RegExp(pattern);
                        return regex.test(symbol);
                    }}
                    return symbol.includes(symbolSearch);
                }});
            }}
            
            // Index Filter
            const indexFilter = document.getElementById('index-filter').value;
            if (indexFilter) {{
                data = data.filter(item => item.index_name === indexFilter);
            }}
            
            // Category Filter
            const categoryFilter = document.getElementById('category-filter').value;
            if (categoryFilter) {{
                data = data.filter(item => item.category === categoryFilter);
            }}
            
            // Price Range
            const priceMin = parseFloat(document.getElementById('price-min').value);
            const priceMax = parseFloat(document.getElementById('price-max').value);
            data = data.filter(item => 
                (item.current_close_price || 0) >= priceMin && 
                (item.current_close_price || 0) <= priceMax
            );
            
            // Volume Range
            const volumeMin = parseInt(document.getElementById('volume-min').value);
            const volumeMax = parseInt(document.getElementById('volume-max').value);
            data = data.filter(item => 
                (item.current_ttl_trd_qnty || 0) >= volumeMin && 
                (item.current_ttl_trd_qnty || 0) <= volumeMax
            );
            
            // Delivery Range
            const deliveryMin = parseFloat(document.getElementById('delivery-min').value);
            const deliveryMax = parseFloat(document.getElementById('delivery-max').value);
            data = data.filter(item => 
                (item.current_deliv_per || 0) >= deliveryMin && 
                (item.current_deliv_per || 0) <= deliveryMax
            );
            
            // Price Category
            const priceCategoryFilter = document.getElementById('price-category-filter').value;
            if (priceCategoryFilter) {{
                data = data.filter(item => item.price_category === priceCategoryFilter);
            }}
            
            // Volume Category
            const volumeCategoryFilter = document.getElementById('volume-category-filter').value;
            if (volumeCategoryFilter) {{
                data = data.filter(item => item.volume_category === volumeCategoryFilter);
            }}
            
            // Delivery Category
            const deliveryCategoryFilter = document.getElementById('delivery-category-filter').value;
            if (deliveryCategoryFilter) {{
                data = data.filter(item => item.delivery_category === deliveryCategoryFilter);
            }}
            
            // Delivery Trend
            const deliveryTrendFilter = document.getElementById('delivery-trend-filter').value;
            if (deliveryTrendFilter) {{
                data = data.filter(item => item.delivery_trend === deliveryTrendFilter);
            }}
            
            // Date Range
            const dateFrom = document.getElementById('date-from').value;
            const dateTo = document.getElementById('date-to').value;
            if (dateFrom && dateTo) {{
                data = data.filter(item => 
                    (item.current_trade_date || '') >= dateFrom && 
                    (item.current_trade_date || '') <= dateTo
                );
            }}
            
            filteredData = data;
            updateDashboard();
        }}

        // Clear All Filters
        function clearAllFilters() {{
            document.getElementById('symbol-search').value = '';
            document.getElementById('index-filter').value = '';
            document.getElementById('category-filter').value = '';
            document.getElementById('price-category-filter').value = '';
            document.getElementById('volume-category-filter').value = '';
            document.getElementById('delivery-category-filter').value = '';
            document.getElementById('delivery-trend-filter').value = '';
            document.getElementById('date-from').value = filterOptions.date_range.min;
            document.getElementById('date-to').value = filterOptions.date_range.max;
            
            // Reset ranges
            document.getElementById('price-min').value = filterOptions.price_range.min;
            document.getElementById('price-max').value = filterOptions.price_range.max;
            document.getElementById('volume-min').value = filterOptions.volume_range.min;
            document.getElementById('volume-max').value = filterOptions.volume_range.max;
            document.getElementById('delivery-min').value = filterOptions.delivery_range.min;
            document.getElementById('delivery-max').value = filterOptions.delivery_range.max;
            
            // Update range displays
            updateRangeValues('price');
            updateRangeValues('volume');
            updateRangeValues('delivery');
            
            // Remove active class from quick filter buttons
            document.querySelectorAll('.quick-filter-btn').forEach(btn => btn.classList.remove('active'));
            
            filteredData = [...originalData];
            updateDashboard();
        }}

        // Update Dashboard with Filtered Data
        function updateDashboard() {{
            // Update counters
            document.getElementById('filtered-count').textContent = filteredData.length.toLocaleString();
            document.getElementById('results-count').textContent = filteredData.length.toLocaleString() + ' Results';
            
            // Update charts
            updateCharts();
            updateDataTable();
        }}

        // Update All Charts
        function updateCharts() {{
            updatePriceChart();
            updateVolumeChart();
            updateDeliveryChart();
            updateIndexChart();
        }}

        // Price Distribution Chart
        function updatePriceChart() {{
            const priceRanges = [
                {{ name: 'Penny (<₹50)', min: 0, max: 50 }},
                {{ name: 'Low (₹50-200)', min: 50, max: 200 }},
                {{ name: 'Mid (₹200-500)', min: 200, max: 500 }},
                {{ name: 'High (₹500-1K)', min: 500, max: 1000 }},
                {{ name: 'Premium (>₹1K)', min: 1000, max: Infinity }}
            ];
            
            const priceData = priceRanges.map(range => {{
                const count = filteredData.filter(item => 
                    (item.current_close_price || 0) >= range.min && 
                    (item.current_close_price || 0) < range.max
                ).length;
                return {{ category: range.name, count }};
            }});
            
            if (charts.price) {{
                charts.price.destroy();
            }}
            
            const options = {{
                chart: {{ type: 'donut', height: 300 }},
                series: priceData.map(d => d.count),
                labels: priceData.map(d => d.category),
                colors: ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FECA57'],
                responsive: [{{
                    breakpoint: 480,
                    options: {{ legend: {{ position: 'bottom' }} }}
                }}],
                legend: {{ position: 'bottom' }}
            }};
            
            charts.price = new ApexCharts(document.querySelector("#price-chart"), options);
            charts.price.render();
        }}

        // Volume Analysis Chart
        function updateVolumeChart() {{
            const volumeData = filteredData
                .map(item => ({{
                    x: item.symbol || 'N/A',
                    y: item.current_ttl_trd_qnty || 0
                }}))
                .sort((a, b) => b.y - a.y)
                .slice(0, 20);
            
            if (charts.volume) {{
                charts.volume.destroy();
            }}
            
            const options = {{
                chart: {{ type: 'bar', height: 300 }},
                series: [{{ name: 'Volume', data: volumeData }}],
                xaxis: {{ 
                    type: 'category',
                    labels: {{ rotate: -45 }}
                }},
                colors: ['#667eea']
            }};
            
            charts.volume = new ApexCharts(document.querySelector("#volume-chart"), options);
            charts.volume.render();
        }}

        // Delivery Trends Chart
        function updateDeliveryChart() {{
            const deliveryTrends = {{}};
            filteredData.forEach(item => {{
                const trend = item.delivery_trend || 'Unknown';
                deliveryTrends[trend] = (deliveryTrends[trend] || 0) + 1;
            }});
            
            const trendData = Object.entries(deliveryTrends).map(([trend, count]) => ({{
                category: trend,
                count
            }}));
            
            if (charts.delivery) {{
                charts.delivery.destroy();
            }}
            
            const options = {{
                chart: {{ type: 'bar', height: 300 }},
                series: [{{ 
                    name: 'Count', 
                    data: trendData.map(d => d.count)
                }}],
                xaxis: {{ 
                    categories: trendData.map(d => d.category),
                    labels: {{ rotate: -45 }}
                }},
                colors: ['#f093fb']
            }};
            
            charts.delivery = new ApexCharts(document.querySelector("#delivery-chart"), options);
            charts.delivery.render();
        }}

        // Index Performance Chart
        function updateIndexChart() {{
            const indexData = {{}};
            filteredData.forEach(item => {{
                const index = item.index_name || 'Other';
                if (!indexData[index]) {{
                    indexData[index] = {{ count: 0, avgDelivery: 0, totalDelivery: 0 }};
                }}
                indexData[index].count += 1;
                indexData[index].totalDelivery += parseFloat(item.current_deliv_per || 0);
            }});
            
            Object.keys(indexData).forEach(index => {{
                indexData[index].avgDelivery = indexData[index].totalDelivery / indexData[index].count;
            }});
            
            const chartData = Object.entries(indexData)
                .map(([index, data]) => ({{
                    category: index,
                    count: data.count,
                    avgDelivery: data.avgDelivery.toFixed(2)
                }}))
                .sort((a, b) => b.count - a.count)
                .slice(0, 10);
            
            if (charts.index) {{
                charts.index.destroy();
            }}
            
            const options = {{
                chart: {{ type: 'column', height: 300 }},
                series: [
                    {{ name: 'Count', data: chartData.map(d => d.count) }},
                    {{ name: 'Avg Delivery %', data: chartData.map(d => parseFloat(d.avgDelivery)) }}
                ],
                xaxis: {{ 
                    categories: chartData.map(d => d.category),
                    labels: {{ rotate: -45 }}
                }},
                colors: ['#4facfe', '#00f2fe']
            }};
            
            charts.index = new ApexCharts(document.querySelector("#index-chart"), options);
            charts.index.render();
        }}

        // Update Data Table
        function updateDataTable() {{
            const tableContent = document.getElementById('data-table-content');
            
            if (filteredData.length === 0) {{
                tableContent.innerHTML = '<div class="loading">No data matches the current filters</div>';
                return;
            }}
            
            const displayData = filteredData.slice(0, 100); // Show first 100 rows
            
            let tableHTML = '';
            displayData.forEach(item => {{
                const priceChange = parseFloat(item.price_change_pct || 0);
                const priceClass = priceChange > 0 ? 'positive' : priceChange < 0 ? 'negative' : '';
                
                tableHTML += `
                    <div class="table-row">
                        <div class="table-cell symbol">${{item.symbol || 'N/A'}}</div>
                        <div class="table-cell">₹${{parseFloat(item.current_close_price || 0).toFixed(2)}}</div>
                        <div class="table-cell ${{priceClass}}">${{priceChange.toFixed(2)}}%</div>
                        <div class="table-cell">${{parseInt(item.current_ttl_trd_qnty || 0).toLocaleString()}}</div>
                        <div class="table-cell">${{parseFloat(item.current_deliv_per || 0).toFixed(1)}}%</div>
                        <div class="table-cell">${{item.index_name || 'N/A'}}</div>
                    </div>
                `;
            }});
            
            tableContent.innerHTML = `
                <div class="table-row" style="background: #f7fafc; font-weight: 600;">
                    <div class="table-cell">Symbol</div>
                    <div class="table-cell">Price</div>
                    <div class="table-cell">Change %</div>
                    <div class="table-cell">Volume</div>
                    <div class="table-cell">Delivery %</div>
                    <div class="table-cell">Index</div>
                </div>
                ${{tableHTML}}
            `;
        }}
    </script>
</body>
</html>
        """
        
        return html_content

    def run(self):
        """Main execution function"""
        print("🚀 Starting Ultimate NSE Filtered Dashboard...")
        
        if not self.connect_and_fetch_data():
            return False
            
        print("📊 Generating comprehensive filtered dashboard...")
        html_content = self.generate_dashboard_html()
        
        # Save the dashboard
        dashboard_path = "C:/Users/kiran/NSE_Downloader/dashboard/ultimate_filtered_dashboard.html"
        with open(dashboard_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"✅ Dashboard saved to: {dashboard_path}")
        
        # Open in browser
        webbrowser.open(f'file:///{dashboard_path}')
        print("🌐 Dashboard opened in browser!")
        
        print("\n" + "="*80)
        print("🎯 ULTIMATE NSE FILTERED DASHBOARD FEATURES:")
        print("="*80)
        print("📊 FILTER CATEGORIES:")
        print("   • Symbol Search (exact/pattern matching)")
        print("   • Index Classification (NIFTY 50, BANK, IT, etc.)")
        print("   • Price Ranges (Penny to Premium stocks)")
        print("   • Volume Categories (Low to Very High)")
        print("   • Delivery Analysis (percentage & trends)")
        print("   • Date Range Selection")
        print("   • Category Filters (Broad Market, Sectoral)")
        print("\n🚀 QUICK FILTERS:")
        print("   • Top Gainers (Price increase >5%)")
        print("   • High Volume Stocks")
        print("   • High Delivery Stocks")
        print("   • Momentum Stocks (High delivery + volume surge)")
        print("   • Institutional Interest")
        print("   • Value Picks")
        print("\n📈 REAL-TIME ANALYTICS:")
        print("   • Price Distribution Charts")
        print("   • Volume Analysis")
        print("   • Delivery Trends")
        print("   • Index Performance")
        print("\n💎 ADVANCED FEATURES:")
        print("   • Debounced real-time filtering")
        print("   • Interactive range sliders")
        print("   • Pattern-based symbol search")
        print("   • Filter combinations")
        print("   • Responsive glassmorphism UI")
        print("="*80)
        
        return True

if __name__ == "__main__":
    dashboard = UltimateNSEFilteredDashboard()
    dashboard.run()