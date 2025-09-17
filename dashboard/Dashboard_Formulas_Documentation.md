# NSE Professional Dashboard - Formulas & Calculations Documentation

## Overview
This document provides detailed explanations of all formulas, calculations, and metrics used in the NSE Professional Dashboard. The dashboard analyzes stock market data from the `step03_compare_monthvspreviousmonth` table to provide comprehensive market insights.

---

## Table of Contents
1. [Data Source & Query](#data-source--query)
2. [Market Overview Tab - Formulas](#market-overview-tab---formulas)
3. [Symbol Analysis Tab - Formulas](#symbol-analysis-tab---formulas)
4. [Category & Index Performance Tab - Formulas](#category--index-performance-tab---formulas)
5. [Chart Calculations](#chart-calculations)
6. [Data Processing Rules](#data-processing-rules)

---

## Data Source & Query

### Base Query
```sql
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
    -- Sector Classification Logic
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
```

### Key Formula: Price Change Percentage
```sql
price_change_pct = ((current_close_price - current_prev_close) / NULLIF(current_prev_close, 0)) * 100
```
**Explanation**: Calculates the percentage change from previous close to current close price. Uses NULLIF to avoid division by zero.

---

## Market Overview Tab - Formulas

### KPI 1: High Delivery Stocks
```python
high_delivery_stocks = len(df[df['delivery_percentage'] > 80])
```
**Definition**: Count of stocks with delivery percentage greater than 80%
**Purpose**: Identifies stocks with strong institutional interest and reduced speculation

### KPI 2: Momentum Stocks
```python
momentum_stocks = len(df[(df['price_change_pct'] > 5) & (df['delivery_percentage'] > 70)])
```
**Definition**: Count of stocks with both:
- Price change > 5% (positive momentum)
- Delivery percentage > 70% (strong fundamentals)
**Purpose**: Identifies stocks suitable for trend-following strategies

### KPI 3: Value Opportunities
```python
value_opportunities = len(df[(df['price_change_pct'] < -2) & (df['delivery_percentage'] > 70)])
```
**Definition**: Count of stocks with both:
- Price change < -2% (price dip)
- Delivery percentage > 70% (strong fundamentals)
**Purpose**: Identifies quality stocks available at discount for contrarian investing

### KPI 4: Average Delivery Percentage
```python
avg_delivery = df['delivery_percentage'].mean()
```
**Definition**: Simple arithmetic mean of delivery percentages across all stocks
**Purpose**: Market-wide delivery sentiment indicator

### KPI 5: Positive Stocks
```python
positive_stocks = len(df[df['price_change_pct'] > 0])
```
**Definition**: Count of stocks with positive price change
**Purpose**: Market breadth indicator

### KPI 6: Total Turnover
```python
total_turnover = df['turnover'].sum() / 100000  # Convert to Crores
```
**Definition**: Sum of all stock turnovers, converted from Lacs to Crores
**Formula**: `Total Turnover (Cr) = Sum(turnover_lacs) / 100`

---

## Symbol Analysis Tab - Formulas

### Volume Leaders Analysis
```python
top_volume = df.nlargest(15, 'volume')
```
**Definition**: Top 15 stocks by trading volume
**Purpose**: Identifies stocks with highest liquidity and market interest

### Turnover Leaders Analysis
```python
top_turnover = df.nlargest(15, 'turnover')
```
**Definition**: Top 15 stocks by turnover value
**Purpose**: Identifies stocks with highest value trading activity

### Delivery Champions Analysis
```python
top_delivery = df.nlargest(20, 'delivery_percentage')
```
**Definition**: Top 20 stocks by delivery percentage
**Purpose**: Identifies stocks with strongest fundamental backing

### Average Trade Size (Derived Metric)
```python
avg_trade_size = turnover_lacs * 100000 / no_of_trades
```
**Formula**: `Average Trade Size = (Turnover in Lacs × 100,000) / Number of Trades`
**Purpose**: Indicates the typical transaction size and institutional participation

---

## Category & Index Performance Tab - Formulas

### Sector Performance Analysis
```python
sector_data = df.groupby('sector').agg({
    'delivery_percentage': 'mean',     # Average delivery per sector
    'price_change_pct': 'mean',        # Average price change per sector
    'turnover': 'sum',                 # Total turnover per sector
    'symbol': 'count'                  # Number of stocks per sector
}).round(2)
```

### Category Performance Analysis
```python
category_data = df.groupby('category').agg({
    'delivery_percentage': 'mean',     # Average delivery per category
    'price_change_pct': 'mean',        # Average price change per category
    'turnover': 'sum',                 # Total turnover per category
    'symbol': 'count',                 # Number of stocks per category
    'delivery_qty': 'sum'              # Total delivery quantity per category
}).round(2)
```

### Index Performance Analysis
```python
index_data = df.groupby('index_name').agg({
    'delivery_percentage': 'mean',     # Average delivery per index
    'price_change_pct': 'mean',        # Average price change per index
    'turnover': 'sum',                 # Total turnover per index
    'symbol': 'count',                 # Number of stocks per index
    'delivery_qty': 'sum'              # Total delivery quantity per index
}).round(2)
```

### Best Performer Identification
```python
best_index = index_data.loc[index_data['price_change_pct'].idxmax()]
best_category = category_data.loc[category_data['price_change_pct'].idxmax()]
```
**Definition**: Identifies the index and category with highest average price change percentage

---

## Chart Calculations

### Sector Performance Scatter Plot
- **X-axis**: Average delivery percentage per sector
- **Y-axis**: Average price change percentage per sector
- **Bubble Size**: Number of stocks in sector (scaled)
- **Color**: Price change percentage (green for positive, red for negative)

### Volume vs Delivery Analysis
- **X-axis**: Trading volume
- **Y-axis**: Delivery percentage
- **Color**: Price change percentage
- **Purpose**: Correlation analysis between volume and delivery quality

### Radial Bar Chart (Category Performance)
```python
# Normalizes negative values for radial display
normalized_values = category_data['price_change_pct'].map(lambda x: max(0, x + 10))
```
**Formula**: `Normalized Value = max(0, price_change_pct + 10)`
**Purpose**: Ensures all values are positive for radial chart display

### Heatmap Calculation
```python
# Creates matrix of delivery percentages by index
heatmap_data = [index_data['avg_delivery']]
```
**Definition**: Single-row heatmap showing delivery percentage intensity across indices

---

## Data Processing Rules

### Data Quality Filters
1. **Minimum Turnover Filter**: `current_turnover_lacs > 50`
   - Ensures only actively traded stocks are included
   - Eliminates low-liquidity penny stocks

2. **Null Value Handling**: `ISNULL(column, default_value)`
   - Replaces NULL values with appropriate defaults (usually 0)
   - Ensures calculations don't fail due to missing data

3. **Division by Zero Protection**: `NULLIF(denominator, 0)`
   - Prevents division by zero errors in percentage calculations
   - Returns NULL if denominator is zero

### Data Transformation Rules

#### Sector Classification Logic
- **Banking**: Symbols containing 'BANK' or major bank symbols
- **IT**: Major IT service companies
- **Energy**: Oil, gas, and energy companies
- **FMCG**: Fast-moving consumer goods companies
- **Auto**: Automobile and auto component companies
- **Pharma**: Pharmaceutical companies
- **Others**: All remaining stocks

#### Performance Classification
- **High Delivery**: delivery_percentage > 80%
- **Strong Delivery**: delivery_percentage > 70%
- **Momentum**: price_change_pct > 5% AND delivery_percentage > 70%
- **Value Opportunity**: price_change_pct < -2% AND delivery_percentage > 70%
- **Positive Trend**: price_change_pct > 0%

---

## Statistical Measures Used

### Central Tendency
- **Mean (Average)**: Used for delivery percentages and price changes
- **Sum**: Used for turnover and volume aggregations
- **Count**: Used for stock counts in categories/sectors

### Ranking Methods
- **nlargest()**: Returns top N records by specified column
- **idxmax()**: Returns index of maximum value in a series

### Grouping and Aggregation
- **groupby()**: Groups data by categorical variables (sector, category, index)
- **agg()**: Applies multiple aggregation functions simultaneously

---

## Color Coding Logic

### Chart Color Schemes
- **Positive Values**: Green (#00C853) - for gains, positive changes
- **Negative Values**: Red (#D50000) - for losses, negative changes
- **Neutral Values**: Blue (#00B0FF) - for neutral or informational data
- **Gradient Scales**: Used in heatmaps and scatter plots for continuous data

### Performance Indicators
- **Green Gradient**: Strong positive performance
- **Red Gradient**: Negative performance or losses
- **Blue/Yellow Gradient**: Neutral to positive transition

---

## Dashboard Refresh Logic

### Data Update Frequency
- **Real-time**: Dashboard reflects latest database state on each run
- **Historical**: Based on month-over-month comparison data
- **Filtering**: Only includes stocks with meaningful trading activity

### Performance Optimization
- **TOP 1000 Limit**: Ensures fast loading while covering major market players
- **Pre-calculation**: All metrics calculated during data load, not on-demand
- **Caching**: Data embedded in HTML for instant chart rendering

---

## Error Handling

### Data Validation
1. **NULL Value Replacement**: All NULL values replaced with appropriate defaults
2. **Zero Division Protection**: Using NULLIF() in SQL calculations
3. **Data Type Consistency**: Explicit type casting for JavaScript compatibility
4. **Boundary Checks**: Ensuring percentage values are within valid ranges

### Missing Data Handling
- **Symbol Names**: Default to 'Others' if NULL
- **Financial Metrics**: Default to 0 if NULL
- **Category/Index**: Default to 'Others' if NULL

---

## Usage Guidelines

### KPI Interpretation
- **High values in delivery-based KPIs**: Indicate strong institutional confidence
- **High momentum stock count**: Suggests bullish market sentiment
- **High value opportunity count**: Indicates potential buying opportunities

### Chart Analysis
- **Scatter plots**: Look for clustering patterns and outliers
- **Bar charts**: Compare relative performance across entities
- **Pie charts**: Understand proportional distributions
- **Heatmaps**: Identify performance hotspots quickly

### Performance Monitoring
- **Track KPI trends**: Monitor changes over time
- **Sector rotation**: Observe shifting sector leadership
- **Volume patterns**: Identify changing market participation

---

## Technical Implementation Notes

### JavaScript Chart Configuration
- **Plotly.js**: Primary charting library
- **Responsive Design**: Charts adapt to screen size
- **Interactive Elements**: Hover tooltips and click events
- **Performance**: Optimized for 1000+ data points

### Data Pipeline
1. **SQL Query**: Extracts and transforms raw data
2. **Python Processing**: Calculates derived metrics
3. **JSON Serialization**: Embeds data in HTML
4. **JavaScript Rendering**: Creates interactive visualizations

---

*Last Updated: September 17, 2025*
*Dashboard Version: 3.0 (3-Tab Enhanced Professional)*