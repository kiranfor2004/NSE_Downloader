# NSE Delivery Analysis Dashboard - Enhanced Three-Tab Version

## Overview

This enhanced dashboard provides comprehensive analysis of NSE delivery data based on the `step03_compare_monthvspreviousmonth` SQL table. The dashboard features three specialized tabs for progressive analysis of stock delivery patterns.

## Dashboard Structure

### Tab 1: Current Month Daily Performance
A high-level overview of daily stock activity with the following features:

#### KPIs and Formulas
- **Daily Delivery Quantity (Symbol-wise)**: `current_deliv_qty`
- **Percentage Change from Previous Month**: `((current_deliv_qty - previous_deliv_qty) / previous_deliv_qty) * 100`
- **Number of Symbols with Higher Delivery**: `COUNT(symbol) WHERE current_deliv_qty > previous_deliv_qty`
- **Delivery Percentage (Symbol-wise)**: `(current_deliv_qty / current_ttl_trd_qnty) * 100`

#### Visualizations
1. **Candlestick/OHLC Chart with Volume**: Primary technical analysis chart showing daily price action with volume bars
2. **Heatmap**: Delivery percentage visualization across symbols (Y-axis) and trading dates (X-axis)
3. **KPI Cards**: Display key metrics with immediate understanding
4. **Top Performers Chart**: Horizontal bar chart showing symbols with highest delivery increases

#### Features
- Date selection for historical analysis
- Symbol-specific candlestick charts
- Click-through functionality to Symbol Deep Dive
- Real-time data refresh

### Tab 2: Monthly Trends and Comparison
Focuses on overall trends and high-level comparison between current and previous months.

#### KPIs and Formulas
- **Total Monthly Delivery Quantity**: `SUM(current_deliv_qty)`
- **Monthly Average Daily Delivery**: `Total Monthly Delivery / Number of Trading Days`
- **Delivery Volume vs. Total Traded Volume Ratio**: `Total Monthly Delivery / Total Monthly Traded Quantity`

#### Visualizations
1. **Bullet Chart**: Current month total vs. previous month target
2. **Grouped Column Chart**: Side-by-side comparison of monthly averages
3. **Treemap**: Symbol contribution to total monthly delivery
4. **Category Performance**: Category-wise comparison charts

#### Features
- Month-over-month comparison
- Category analysis
- Symbol contribution analysis
- Interactive treemap with drill-down

### Tab 3: Symbol-Specific Deep Dive
Interactive analysis for individual stock symbols with drill-down functionality.

#### KPIs and Formulas
- **Symbol's Total Delivery (Current vs. Previous Month)**: `SUM(current_deliv_qty) for selected symbol`
- **Correlation between Price and Delivery**: `CORREL(current_close_price, current_deliv_qty)`

#### Visualizations
1. **Dual-Axis Line/Column Chart**: Daily delivery (line) vs. daily turnover (column)
2. **Scatter Plot**: Price vs. delivery quantity correlation with trend line
3. **Donut Chart**: Delivery percentage vs. total traded quantity

#### Features
- Symbol search functionality
- Comprehensive correlation analysis
- Historical performance tracking
- Detailed statistical metrics

## Technical Implementation

### Backend API (api_dashboard.py)
- **Framework**: Flask with CORS support
- **Database**: SQL Server via pyodbc
- **Endpoints**: RESTful API with specialized endpoints for each tab
- **Port**: 5001 (to avoid conflicts with existing API)

#### Key Endpoints
```
=== TAB 1: Current Month Daily Performance ===
GET /api/tab1/daily-performance - Daily performance KPIs
GET /api/tab1/heatmap-data - Heatmap data for delivery percentage
GET /api/tab1/candlestick-data/<symbol> - OHLC data for specific symbol

=== TAB 2: Monthly Trends and Comparison ===
GET /api/tab2/monthly-trends - Monthly trends and comparison
GET /api/tab2/category-comparison - Category-wise comparison

=== TAB 3: Symbol-Specific Deep Dive ===
GET /api/tab3/symbol-detail/<symbol> - Detailed symbol analysis
GET /api/tab3/symbol-correlation/<symbol> - Correlation data for scatter plot

=== UTILITY ENDPOINTS ===
GET /api/available-symbols - Get all available symbols
GET /api/available-dates - Get all available trading dates
GET /api/health - Health check
```

### Frontend (index_enhanced.html + dashboard_enhanced.js)
- **Charting Library**: Plotly.js for interactive visualizations
- **Styling**: Custom CSS with modern design
- **JavaScript**: Vanilla JS with jQuery for DOM manipulation
- **Responsive Design**: Mobile-friendly layout

#### Key Features
- Tab-based navigation
- Interactive charts with click-through functionality
- Real-time data loading with loading indicators
- Symbol search and autocomplete
- Error handling and connection status monitoring
- Auto-refresh functionality (5-minute intervals)

## Color Scheme and Design
- **Primary Colors**: Purple gradient (#667eea to #764ba2)
- **Chart Colors**: 
  - Bullish: #10b981 (Green)
  - Bearish: #ef4444 (Red)
  - Sequential: Light to dark green palette for heatmaps
  - Qualitative: Distinct colors for categories

## Setup and Installation

### Prerequisites
- Python 3.7+
- SQL Server with ODBC Driver 17
- Access to `step03_compare_monthvspreviousmonth` table

### Installation Steps
1. **Install Dependencies**:
   ```bash
   pip install -r requirements_enhanced.txt
   ```

2. **Configure Database**:
   - Ensure `database_config.json` is properly configured
   - Test database connection

3. **Start the Dashboard**:
   ```bash
   # Windows
   start_enhanced_dashboard.bat
   
   # Or manually
   python api_dashboard.py
   ```

4. **Access Dashboard**:
   - Open browser to `http://localhost:5001`

## Database Schema Requirements

The dashboard expects the following columns in `step03_compare_monthvspreviousmonth`:

### Core Columns
- `id` - Primary key
- `current_trade_date` - Trading date
- `symbol` - Stock symbol
- `series` - Stock series (EQ, etc.)

### Current Month Data
- `current_prev_close` - Previous close price
- `current_open_price` - Opening price
- `current_high_price` - High price
- `current_low_price` - Low price
- `current_last_price` - Last traded price
- `current_close_price` - Closing price
- `current_avg_price` - Average price
- `current_ttl_trd_qnty` - Total traded quantity
- `current_turnover_lacs` - Turnover in lacs
- `current_no_of_trades` - Number of trades
- `current_deliv_qty` - Delivery quantity
- `current_deliv_per` - Delivery percentage

### Previous Month Data
- `previous_baseline_date` - Previous month baseline date
- `previous_deliv_qty` - Previous month delivery quantity
- (Similar structure for other previous month fields)

### Analysis Columns
- `delivery_increase_abs` - Absolute delivery increase
- `delivery_increase_pct` - Percentage delivery increase
- `comparison_type` - Type of comparison
- `index_name` - Index classification
- `category` - Stock category

## Performance Optimizations

### Backend Optimizations
1. **Efficient Queries**: Optimized SQL queries with proper indexing
2. **Data Pagination**: Limit large result sets with OFFSET/FETCH
3. **Caching Strategy**: Connection pooling and query result caching
4. **Error Handling**: Comprehensive error handling and logging

### Frontend Optimizations
1. **Lazy Loading**: Charts load only when tabs are active
2. **Data Filtering**: Client-side filtering for better performance
3. **Responsive Charts**: Charts resize automatically
4. **Progressive Loading**: KPIs load first, then charts

## Interactivity Features

### Drill-Down Functionality
1. **Heatmap Click**: Click on any cell to drill down to symbol details
2. **Treemap Click**: Click on symbol rectangles to view detailed analysis
3. **Top Performers Click**: Click on bars to navigate to symbol deep dive
4. **Symbol Search**: Type-ahead search with autocomplete

### Real-Time Features
1. **Auto-Refresh**: Data refreshes every 5 minutes
2. **Connection Monitoring**: Real-time database connection status
3. **Error Recovery**: Automatic retry on connection failures
4. **Loading Indicators**: Visual feedback during data loading

## Customization Options

### Chart Customization
- Color schemes can be modified in `COLORS` object in JavaScript
- Chart layouts and styling in Plotly configurations
- KPI card styling in CSS

### Data Filtering
- Date range selection
- Category filtering
- Symbol filtering
- Custom comparison periods

## Troubleshooting

### Common Issues
1. **Database Connection Fails**: Check connection string in `database_config.json`
2. **Charts Not Loading**: Verify Plotly.js library is loaded
3. **API Errors**: Check Flask server logs for detailed error messages
4. **Performance Issues**: Consider adding database indexes

### Debug Mode
- Set `debug=True` in Flask app for detailed error messages
- Use browser developer tools for frontend debugging
- Check network tab for API request/response details

## Future Enhancements

### Planned Features
1. **Export Functionality**: PDF/Excel export of charts and data
2. **Alert System**: Email/SMS alerts for significant changes
3. **Historical Analysis**: Extended historical data analysis
4. **Portfolio Tracking**: Multi-symbol portfolio analysis
5. **Advanced Filters**: More sophisticated filtering options

### Technical Improvements
1. **WebSocket Integration**: Real-time data streaming
2. **Database Optimization**: Advanced caching and indexing
3. **Mobile App**: Native mobile application
4. **API Rate Limiting**: Enhanced API security and rate limiting

## Support and Maintenance

### Regular Maintenance
1. **Data Validation**: Regular checks for data consistency
2. **Performance Monitoring**: Monitor API response times
3. **Security Updates**: Keep dependencies updated
4. **Backup Procedures**: Regular database backups

### Monitoring
- API health checks every minute
- Database connection monitoring
- Error rate tracking
- User activity analytics

This dashboard provides a comprehensive solution for NSE delivery data analysis with professional-grade visualizations and interactive features suitable for financial analysis and decision-making.