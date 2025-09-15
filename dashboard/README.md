# NSE Delivery Analysis Dashboard

A comprehensive web dashboard for analyzing NSE (National Stock Exchange) delivery data with professional NSE India-inspired design. The dashboard provides interactive visualizations and analytics for the `step03_compare_monthvspreviousmonth` table data.

## 🎯 Features

### Dashboard Capabilities
- **Multi-tab Interface**: Overview, Performance, Indices, Sectors, and Comparison views
- **Interactive Charts**: Built with Chart.js for dynamic data visualization
- **Real-time Filtering**: Category, search, and sorting capabilities
- **Responsive Design**: Mobile-first approach with NSE-inspired styling
- **Data Tables**: Paginated tables with comprehensive data display
- **Performance Analytics**: Top performers, sector analysis, and trend visualization

### Data Insights
- **Delivery Increase Analysis**: Track delivery quantity percentage increases
- **Index Classification**: NIFTY 50, Sectoral indices, and Other indices
- **Category Analysis**: Broad Market, Sectoral, and Other category performance
- **Symbol Performance**: Individual stock performance tracking
- **Monthly Comparisons**: Month-over-month delivery trend analysis

## 🚀 Quick Start

### Prerequisites
- Python 3.7+
- SQL Server with NSE data
- Modern web browser
- Required Python packages (see requirements.txt)

### Installation

1. **Clone or navigate to the dashboard directory**:
   ```bash
   cd c:\Users\kiran\NSE_Downloader\dashboard
   ```

2. **Install Python dependencies**:
   ```bash
   pip install flask flask-cors pyodbc
   ```

3. **Ensure database configuration**:
   - Verify `database_config.json` exists in the parent directory
   - Ensure `step03_compare_monthvspreviousmonth` table is populated

4. **Start the API server**:
   ```bash
   python api.py
   ```

5. **Open the dashboard**:
   - Open `index.html` in your web browser
   - Or serve via local HTTP server for better performance:
     ```bash
     python -m http.server 8080
     ```
   - Navigate to `http://localhost:8080`

## 📊 Dashboard Sections

### 1. Overview Tab
- **Key Metrics**: Total records, active symbols, average increase
- **Category Distribution**: Pie chart of Broad Market vs Sectoral vs Other
- **Trend Analysis**: Monthly delivery increase trends
- **Data Table**: Paginated view of all records with filtering

### 2. Performance Tab
- **Top Performers**: Top 10 stocks by delivery increase percentage
- **Performance Distribution**: Histogram of performance ranges (0-50%, 50-100%, etc.)
- **Performance Metrics**: Statistical analysis of delivery increases

### 3. Indices Tab
- **Index Cards**: Summary cards for each index with symbol counts
- **Index Distribution**: Horizontal bar chart of index performance
- **Index Categories**: Breakdown by Broad Market, Sectoral, and Other

### 4. Sectors Tab
- **Sector Heatmap**: Visual heatmap of sector performance
- **Sector Details**: Detailed analysis of sectoral indices
- **Performance Comparison**: Cross-sector performance metrics

### 5. Comparison Tab
- **Trend Charts**: Time-series analysis of delivery trends
- **Comparative Analysis**: Month-over-month performance comparison
- **Filter Options**: Date range and comparison type filters

## 🎨 Design System

### NSE-Inspired Styling
- **Primary Colors**: NSE Blue (#1c3f7c), Secondary Blue (#2c5aa0)
- **Accent Colors**: Orange (#ff6b35), Success Green (#28a745)
- **Typography**: Professional font stack with proper hierarchy
- **Layout**: Clean, modern design with consistent spacing

### Responsive Design
- **Desktop**: Full-featured layout with multi-column views
- **Tablet**: Optimized layout with responsive charts
- **Mobile**: Single-column layout with touch-friendly navigation

## 🔧 API Endpoints

The dashboard connects to a Flask API with the following endpoints:

### Core Data Endpoints
- `GET /api/health` - Health check and database connectivity
- `GET /api/delivery-data` - Main delivery data with filtering options
- `GET /api/summary-stats` - Dashboard summary statistics
- `GET /api/performance-analysis` - Performance distribution analysis

### Data Management Endpoints
- `GET /api/symbol/<symbol>` - Individual symbol details
- `GET /api/categories` - Available data categories
- `GET /api/indices` - Available indices with metadata

### Query Parameters
- `category`: Filter by category (Broad Market, Sectoral, Other)
- `limit`: Pagination limit
- `offset`: Pagination offset
- `sort_by`: Sort field (delivery_increase_pct, current_trade_date, symbol)
- `sort_order`: Sort direction (ASC, DESC)
- `search`: Symbol name search

## 🗃️ Database Schema

The dashboard works with the `step03_compare_monthvspreviousmonth` table containing:

```sql
-- Key columns used by the dashboard
symbol                    VARCHAR(50)    -- Stock symbol
index_name               VARCHAR(100)   -- Associated index (NIFTY 50, etc.)
category                 VARCHAR(50)    -- Category (Broad Market, Sectoral, Other)
current_deliv_qty        BIGINT         -- Current delivery quantity
delivery_increase_pct    DECIMAL(10,2)  -- Percentage increase
comparison_type          VARCHAR(50)    -- Month comparison (AUG_VS_JUL_2025, etc.)
current_trade_date       DATE           -- Current trading date
current_close_price      DECIMAL(10,2)  -- Current closing price
previous_deliv_qty       BIGINT         -- Previous delivery quantity
```

## 🔄 Data Flow

1. **Data Source**: NSE delivery data in SQL Server
2. **API Layer**: Flask API serves data with filtering and aggregation
3. **Frontend**: JavaScript dashboard consumes API data
4. **Visualization**: Chart.js renders interactive charts
5. **User Interaction**: Real-time filtering and navigation

## 📈 Performance Features

### Optimization
- **Lazy Loading**: Charts and data loaded on-demand
- **Pagination**: Efficient handling of large datasets
- **Caching**: Client-side data caching for improved performance
- **Responsive**: Optimized for various screen sizes

### Analytics
- **Real-time Metrics**: Live calculation of performance statistics
- **Interactive Filtering**: Dynamic data filtering without page refresh
- **Export Ready**: Data structured for easy export capabilities
- **Drill-down**: Detailed views for individual symbols and sectors

## 🛠️ Customization

### Styling
- Modify `dashboard.css` for custom color schemes
- Update NSE color variables in CSS `:root` section
- Customize chart colors in `dashboard.js`

### Functionality
- Add new chart types in the JavaScript dashboard class
- Extend API endpoints in `api.py` for additional data views
- Modify table columns in the HTML structure

### Data Sources
- Update database queries in `api.py` for different data sources
- Modify the `DatabaseConnection` class for different database types
- Extend data processing methods for additional metrics

## 🔍 Troubleshooting

### Common Issues

1. **API Connection Failed**:
   - Verify Flask server is running on port 5000
   - Check database connectivity in `database_config.json`
   - Ensure SQL Server is accessible

2. **Charts Not Loading**:
   - Verify Chart.js library is loaded
   - Check browser console for JavaScript errors
   - Ensure data format matches chart expectations

3. **Database Connection Issues**:
   - Verify ODBC driver installation
   - Check SQL Server authentication settings
   - Ensure database and table permissions

4. **Performance Issues**:
   - Consider pagination for large datasets
   - Optimize database queries with proper indexing
   - Monitor browser memory usage with large data sets

### Development Tips
- Use browser developer tools for debugging
- Monitor API response times in Network tab
- Check console logs for JavaScript errors
- Validate SQL queries in SQL Server Management Studio

## 📝 License

This dashboard is part of the NSE Downloader project and follows the same licensing terms.

## 🤝 Contributing

When contributing to the dashboard:
1. Maintain NSE design consistency
2. Ensure responsive design compatibility
3. Add appropriate error handling
4. Update documentation for new features
5. Test across different browsers and devices

## 📞 Support

For issues related to:
- **Database connectivity**: Check SQL Server configuration
- **API endpoints**: Review Flask application logs
- **Frontend issues**: Check browser console and network requests
- **Performance**: Monitor database query performance and browser resources