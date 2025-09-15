# NSE Delivery Analysis Dashboard - KPI Formulas Documentation

## Overview
This document provides detailed formulas and calculations used for all Key Performance Indicators (KPIs) in the NSE Delivery Analysis Dashboard.

---

## Market Overview Tab - KPIs

### 1. Total Market Delivery Increase
**Display Format:** `₹{value}L`  
**Purpose:** Shows the total absolute increase in delivery quantities across all stocks in lakhs.

```javascript
// Formula
const totalDeliveryIncrease = this.filteredData.reduce((sum, item) => {
    const deliveryIncreaseAbs = parseFloat(item.delivery_increase_abs) || 0;
    return sum + deliveryIncreaseAbs;
}, 0) / 100000; // Convert to lakhs

// Mathematical Expression
Total Market Delivery Increase = (Σ delivery_increase_abs) ÷ 100,000

// Data Source Field
delivery_increase_abs - Absolute increase in delivery quantity
```

### 2. Positive Growth Stocks Count
**Display Format:** `{count} stocks`  
**Purpose:** Counts how many stocks have positive delivery percentage growth.

```javascript
// Formula
const positiveGrowthCount = this.filteredData.filter(item => {
    const deliveryIncreasePct = parseFloat(item.delivery_increase_pct) || 0;
    return deliveryIncreasePct > 0;
}).length;

// Mathematical Expression
Positive Growth Count = COUNT(stocks WHERE delivery_increase_pct > 0)

// Data Source Field
delivery_increase_pct - Percentage increase in delivery
```

### 3. Positive Growth Percentage
**Display Format:** `{percentage}% of total`  
**Purpose:** Shows what percentage of total stocks have positive delivery growth.

```javascript
// Formula
const positiveGrowthPercentage = (positiveGrowthCount / this.filteredData.length) * 100;

// Mathematical Expression
Positive Growth Percentage = (Positive Growth Count ÷ Total Stocks) × 100

// Dependencies
Requires: Positive Growth Count (from above)
Total Stocks = filteredData.length
```

### 4. Market Delivery-to-Turnover Ratio
**Display Format:** `{ratio}%`  
**Purpose:** Shows the ratio of delivery quantities to turnover values as a percentage.

```javascript
// Formula
const totalCurrentDelivQty = this.filteredData.reduce((sum, item) => {
    const currentDelivQty = parseFloat(item.current_deliv_qty) || 0;
    return sum + currentDelivQty;
}, 0);

const totalCurrentTurnoverLacs = this.filteredData.reduce((sum, item) => {
    const currentTurnoverLacs = parseFloat(item.current_turnover_lacs) || 0;
    return sum + currentTurnoverLacs;
}, 0);

const deliveryTurnoverRatio = totalCurrentTurnoverLacs > 0 ? 
    (totalCurrentDelivQty / (totalCurrentTurnoverLacs * 100000)) * 100 : 0;

// Mathematical Expression
Delivery-to-Turnover Ratio = (Σ current_deliv_qty ÷ (Σ current_turnover_lacs × 100,000)) × 100

// Data Source Fields
current_deliv_qty - Current month delivery quantity
current_turnover_lacs - Current month turnover in lakhs
```

### 5. Average Daily Turnover
**Display Format:** `₹{formatted_currency}`  
**Purpose:** Shows the average turnover per stock record.

```javascript
// Formula
const avgDailyTurnover = this.filteredData.length > 0 ? 
    (totalCurrentTurnoverLacs * 100000) / this.filteredData.length : 0;

// Mathematical Expression
Average Daily Turnover = (Total Turnover Lacs × 100,000) ÷ Number of Records

// Dependencies
Requires: totalCurrentTurnoverLacs (from Delivery-to-Turnover Ratio calculation)
Number of Records = filteredData.length
```

---

## Market Overview Visualizations

### 1. Category Turnover Distribution (TreeMap)

**Purpose:** Shows hierarchical view of turnover by category and symbol
**Container ID:** `treeMap`

```javascript
// Data Preparation Formula
const categoryMap = new Map();

this.filteredData.forEach(item => {
    const category = item.category || 'Other';
    const symbol = item.symbol;
    const turnoverLacs = parseFloat(item.current_turnover_lacs) || 0;
    const deliveryIncPct = parseFloat(item.delivery_increase_pct) || 0;
    const deliveryIncAbs = parseFloat(item.delivery_increase_abs) || 0;

    if (!categoryMap.has(category)) {
        categoryMap.set(category, {
            name: category,
            children: [],
            value: 0
        });
    }

    const categoryData = categoryMap.get(category);
    categoryData.children.push({
        name: symbol,
        value: turnoverLacs,
        deliveryIncPct: deliveryIncPct,
        deliveryIncAbs: deliveryIncAbs,
        category: category
    });
    categoryData.value += turnoverLacs;
});

// Hierarchical Structure
const treeMapData = {
    name: "Market",
    children: Array.from(categoryMap.values()).sort((a, b) => b.value - a.value)
};

// Mathematical Expression
Category Value = Σ(current_turnover_lacs for all symbols in category)
Symbol Size = individual current_turnover_lacs
Display: Top 5 categories with proportional boxes
```

### 2. Index-Stock Relationships (Force-Directed Graph)

**Purpose:** Interactive network showing relationships between indices and symbols
**Container ID:** `forceDirectedGraph`

```javascript
// Node Creation Formula
const nodeMap = new Map();
const links = [];

this.filteredData.forEach(item => {
    const indexName = item.index_name || 'Other';
    const symbol = item.symbol;
    const turnoverLacs = parseFloat(item.current_turnover_lacs) || 0;
    const deliveryIncPct = parseFloat(item.delivery_increase_pct) || 0;

    // Create index node
    if (!nodeMap.has(indexName)) {
        nodeMap.set(indexName, {
            id: indexName,
            type: 'index',
            size: 0,
            count: 0
        });
    }

    // Add symbol as node
    const symbolId = `${indexName}-${symbol}`;
    nodeMap.set(symbolId, {
        id: symbolId,
        symbol: symbol,
        index: indexName,
        type: 'symbol',
        size: turnoverLacs,
        deliveryIncPct: deliveryIncPct
    });

    // Update index node size
    const indexNode = nodeMap.get(indexName);
    indexNode.size += turnoverLacs;
    indexNode.count += 1;

    // Create link between index and symbol
    links.push({
        source: indexName,
        target: symbolId,
        value: Math.abs(deliveryIncPct),
        deliveryIncPct: deliveryIncPct
    });
});

// Mathematical Expressions
Index Size = Σ(current_turnover_lacs for all symbols in index)
Symbol Count = COUNT(symbols per index)
Link Strength = |delivery_increase_pct|
Display: Top 6 indices with market size bars
```

### 3. Hierarchical Distribution (Sunburst)

**Purpose:** Multi-level breakdown of market hierarchy with category rings
**Container ID:** `sunburstChart`

```javascript
// Category Ring Calculation Formula
const totalValue = data.children.reduce((sum, cat) => sum + cat.value, 0);
const categoryRings = data.children.slice(0, 8).map((category, index) => {
    const percentage = ((category.value / totalValue) * 100);
    const topSymbols = category.children.slice(0, 3);
    
    return {
        name: category.name,
        percentage: percentage,
        value: category.value,
        topSymbols: topSymbols,
        totalSymbols: category.children.length,
        colorHue: index * 45  // HSL color rotation
    };
});

// Mathematical Expressions
Category Percentage = (Category Turnover ÷ Total Market Turnover) × 100
Ring Size = Proportional to category percentage
Color Assignment = HSL(index × 45°, 70%, 60%)
Display: Top 8 categories with percentage rings and top 3 symbols each
```

### 4. Multi-Metric Analysis (Parallel Coordinates)

**Purpose:** Comparative analysis across multiple dimensions for top performing stocks
**Container ID:** `parallelCoordinates`

```javascript
// Top Stocks Selection Formula
const topStocks = this.filteredData
    .filter(item => parseFloat(item.current_turnover_lacs) > 0)
    .sort((a, b) => parseFloat(b.current_turnover_lacs) - parseFloat(a.current_turnover_lacs))
    .slice(0, 10);

// Metric Normalization Formula
const maxTurnover = Math.max(...topStocks.map(s => parseFloat(s.current_turnover_lacs)));
const maxDelivPct = Math.max(...topStocks.map(s => parseFloat(s.delivery_increase_pct) || 0));
const maxDelivQty = Math.max(...topStocks.map(s => parseFloat(s.current_deliv_qty) || 0));

// For each stock:
const turnover = parseFloat(stock.current_turnover_lacs) || 0;
const delivPct = parseFloat(stock.delivery_increase_pct) || 0;
const delivQty = parseFloat(stock.current_deliv_qty) || 0;
const delivIncrease = parseFloat(stock.delivery_increase_abs) || 0;

// Normalize values for bar display (0-100%)
const turnoverNorm = (turnover / maxTurnover) * 100;
const delivPctNorm = Math.min(100, Math.abs(delivPct / maxDelivPct) * 100);
const delivQtyNorm = (delivQty / maxDelivQty) * 100;

// Mathematical Expressions
Stock Selection = TOP 10 BY current_turnover_lacs WHERE current_turnover_lacs > 0
Turnover Normalization = (individual_turnover ÷ max_turnover) × 100
Delivery % Normalization = MIN(100, |individual_deliv_pct ÷ max_deliv_pct| × 100)
Delivery Qty Normalization = (individual_deliv_qty ÷ max_deliv_qty) × 100
Display: Tabular format with proportional metric bars
```

---

## Data Source Fields Reference

### Primary Database Table
**Table:** `step03_compare_monthvspreviousmonth`

### Complete Database Schema - All Available Columns

Based on the API implementation and dashboard usage, the following columns are available in the database table:

#### **Core Identification Fields**
| Field Name | Data Type | Description | Used in Dashboard |
|------------|-----------|-------------|-------------------|
| `symbol` | String | Stock symbol identifier (e.g., "RELIANCE", "TCS") | ✅ All visualizations, filters, search |
| `index_name` | String | Index classification (e.g., "NIFTY 50", "NIFTY BANK") | ✅ Force Graph grouping, filters |
| `category` | String | Stock category classification (e.g., "Large Cap", "Mid Cap") | ✅ TreeMap/Sunburst grouping, filters |
| `current_trade_date` | Date | Trade date for the current month record | ✅ API response (as 'date') |

#### **Current Month Delivery Data**
| Field Name | Data Type | Description | Used in Dashboard |
|------------|-----------|-------------|-------------------|
| `current_deliv_qty` | Numeric | Current month delivery quantity | ✅ KPIs, Multi-Metric Analysis |
| `current_deliv_per` | Numeric | Current month delivery percentage | ✅ API mapping |
| `current_turnover_lacs` | Numeric | Current month turnover value in lakhs | ✅ All visualizations (primary size metric) |
| `current_ttl_trd_qnty` | Numeric | Current month total traded quantity | ✅ API mapping |

#### **Previous Month Delivery Data**
| Field Name | Data Type | Description | Used in Dashboard |
|------------|-----------|-------------|-------------------|
| `previous_deliv_qty` | Numeric | Previous month delivery quantity | ✅ API mapping |
| `previous_deliv_per` | Numeric | Previous month delivery percentage | ✅ API mapping |
| `previous_ttl_trd_qnty` | Numeric | Previous month total traded quantity | ✅ API mapping |

#### **Calculated Delivery Changes**
| Field Name | Data Type | Description | Used in Dashboard |
|------------|-----------|-------------|-------------------|
| `delivery_increase_abs` | Numeric | Absolute increase in delivery quantity | ✅ KPIs, TreeMap, Multi-Metric |
| `delivery_increase_pct` | Numeric | Percentage increase in delivery | ✅ All KPIs and visualizations |

### Field Usage Matrix

#### **Dashboard Components vs Database Fields**

| Component | Primary Fields | Secondary Fields | Sorting/Filtering Fields |
|-----------|---------------|------------------|-------------------------|
| **Market Overview KPIs** | `delivery_increase_abs`, `delivery_increase_pct`, `current_deliv_qty`, `current_turnover_lacs` | - | - |
| **TreeMap Visualization** | `current_turnover_lacs`, `category`, `symbol` | `delivery_increase_pct`, `delivery_increase_abs` | `current_turnover_lacs` (DESC) |
| **Force Graph** | `current_turnover_lacs`, `index_name`, `symbol` | `delivery_increase_pct` | `current_turnover_lacs` (DESC) |
| **Sunburst Chart** | `current_turnover_lacs`, `category`, `symbol` | `delivery_increase_pct` | `current_turnover_lacs` (DESC) |
| **Multi-Metric Analysis** | `current_turnover_lacs`, `delivery_increase_pct`, `current_deliv_qty`, `delivery_increase_abs` | `symbol` | `current_turnover_lacs` (DESC) |
| **Symbol Search** | `symbol` | - | `symbol` (LIKE search) |
| **Category Filter** | `category` | - | `category` (equality) |
| **Index Filter** | `index_name` | - | `index_name` (equality) |

### API Field Mapping

The API provides both mapped field names (for compatibility) and raw database field names. The dashboard primarily uses the raw field names for calculations:

#### **API Response Structure**
```json
{
  "delivery_data": [
    {
      // Mapped fields (for compatibility)
      "symbol": "RELIANCE",
      "index_name": "NIFTY 50",
      "category": "Large Cap",
      "date": "2025-02-01",
      "current_month_delivery_percentage": 45.67,
      "current_month_delivery_qty": 1234567,
      "current_month_delivery_value": 890.12,
      "current_month_traded_qty": 2345678,
      "current_month_traded_value": 890.12,
      "previous_month_delivery_percentage": 40.23,
      "previous_month_delivery_qty": 987654,
      "previous_month_delivery_value": 650.45,
      "previous_month_traded_qty": 1876543,
      "previous_month_traded_value": 650.45,
      "delivery_percentage_change": 13.52,
      "delivery_qty_change": 246913,
      "delivery_value_change": 13.52,
      
      // Raw database fields (used by dashboard)
      "delivery_increase_abs": 246913,
      "delivery_increase_pct": 13.52,
      "current_deliv_qty": 1234567,
      "current_turnover_lacs": 890.12
    }
  ]
}
```

### Key Fields Used in All Calculations

| Field Name | Data Type | Description | Usage in Visualizations |
|------------|-----------|-------------|------------------------|
| `delivery_increase_abs` | Numeric | Absolute increase in delivery quantity | KPIs, TreeMap, Multi-Metric |
| `delivery_increase_pct` | Numeric | Percentage increase in delivery | KPIs, Force Graph, TreeMap, Sunburst, Multi-Metric |
| `current_deliv_qty` | Numeric | Current month delivery quantity | KPIs, Multi-Metric |
| `current_turnover_lacs` | Numeric | Current month turnover in lakhs | All visualizations (primary metric) |
| `symbol` | String | Stock symbol identifier | All visualizations (grouping/labeling) |
| `category` | String | Stock category classification | TreeMap, Sunburst (primary grouping) |
| `index_name` | String | Index classification | Force Graph (primary grouping) |

### Database Constraints and Data Quality

#### **Field Validation Rules**
- `symbol`: Always present, uppercase format, unique per record
- `current_turnover_lacs`: Always numeric, >= 0
- `delivery_increase_pct`: Can be negative (indicates decrease)
- `delivery_increase_abs`: Can be negative (indicates decrease)
- `category`: May be NULL (defaulted to 'Other' in dashboard)
- `index_name`: May be NULL (defaulted to 'Other' in dashboard)

#### **Data Quality Measures in Dashboard**
```javascript
// Safe numeric parsing with fallbacks
const value = parseFloat(item.field_name) || 0;

// Null handling for text fields
const category = item.category || 'Other';
const indexName = item.index_name || 'Other';

// Division by zero protection
const ratio = denominator > 0 ? (numerator / denominator) : 0;
```

### Fields Available but NOT Currently Used

The following fields are available in the database but are not currently utilized in the dashboard visualizations. These could be used for future enhancements:

#### **Potential Enhancement Fields**
| Field Name | Data Type | Description | Potential Usage |
|------------|-----------|-------------|-----------------|
| `current_deliv_per` | Numeric | Current month delivery percentage | Alternative to calculated percentages |
| `previous_deliv_per` | Numeric | Previous month delivery percentage | Historical comparison charts |
| `previous_deliv_qty` | Numeric | Previous month delivery quantity | Trend analysis, historical data |
| `current_ttl_trd_qnty` | Numeric | Current month total traded quantity | Trading volume analysis |
| `previous_ttl_trd_qnty` | Numeric | Previous month total traded quantity | Volume trend analysis |
| `current_trade_date` | Date | Trade date for current record | Time-series analysis, date filtering |

#### **Enhancement Opportunities**
1. **Historical Trends**: Use `previous_*` fields for month-over-month trend visualization
2. **Volume Analysis**: Implement trading volume charts using `current_ttl_trd_qnty` and `previous_ttl_trd_qnty`
3. **Time-based Filtering**: Add date range filtering using `current_trade_date`
4. **Delivery Efficiency Metrics**: Calculate delivery-to-trading ratios using available percentage fields
5. **Comparative Analysis**: Build before/after comparison views using current vs previous month data

#### **Implementation Considerations**
- All unused fields are already fetched by the API (`SELECT * FROM table`)
- No additional database queries needed to access these fields
- Dashboard code would need enhancement to utilize these additional data points
- UI components would need to be added for new visualization types

### Derived Calculations

| Calculation | Formula | Usage |
|-------------|---------|--------|
| **Category Total Turnover** | `Σ(current_turnover_lacs WHERE category = X)` | TreeMap, Sunburst |
| **Index Total Size** | `Σ(current_turnover_lacs WHERE index_name = X)` | Force Graph |
| **Market Share %** | `(Category Turnover ÷ Total Market Turnover) × 100` | Sunburst rings |
| **Normalized Metrics** | `(Individual Value ÷ Maximum Value) × 100` | Multi-Metric bars |
| **Symbol Count per Group** | `COUNT(DISTINCT symbol WHERE group = X)` | Force Graph, Sunburst |

---

## Visualization Data Flow

### Initialization Sequence
1. **Dashboard loads** → `init()` called
2. **Data fetched** → `loadData()` from `/api/delivery-data`
3. **Data stored** → `this.filteredData` array populated
4. **Tab rendered** → `renderMarketOverview()` called
5. **KPIs calculated** → `calculateMarketOverviewKPIs()` executed
6. **Visualizations prepared** → Data transformation functions called
7. **DOM updated** → All containers populated with real data

### Data Transformation Pipeline
```javascript
Raw API Data → filteredData Array → Visualization-Specific Processing → DOM Rendering

// Example for TreeMap:
API Response → Group by Category → Aggregate Turnover → Sort by Value → Render Top 5

// Example for Force Graph:
API Response → Group by Index → Create Nodes/Links → Calculate Sizes → Render Top 6

// Example for Sunburst:
API Response → Category Hierarchy → Calculate Percentages → Color Assignment → Render Rings

// Example for Multi-Metric:
API Response → Filter/Sort → Normalize Metrics → Create Comparison Bars → Render Table
```

---

## Performance Metrics

### Data Volume Handling
- **Maximum Records:** 5,000 per API call (configurable via `limit` parameter)
- **Visualization Limits:** 
  - TreeMap: Top 5 categories displayed
  - Force Graph: Top 6 indices displayed
  - Sunburst: Top 8 categories displayed
  - Multi-Metric: Top 10 stocks displayed

### Calculation Complexity
- **KPI Calculations:** O(n) linear complexity for all metrics
- **TreeMap Processing:** O(n log n) due to sorting
- **Force Graph Processing:** O(n) for node creation, O(n) for link creation
- **Sunburst Processing:** O(n log n) due to category sorting
- **Multi-Metric Processing:** O(n log n) due to stock ranking

### Browser Compatibility
- **JavaScript Features:** ES6+ (arrow functions, template literals, Map objects)
- **DOM APIs:** Modern element creation and manipulation
- **CSS Features:** CSS Grid, Flexbox, CSS Custom Properties
- **Performance:** Client-side calculations for real-time filtering

---

## Error Handling & Validation

### Data Validation
```javascript
// All numeric fields use safe parsing with fallbacks
const value = parseFloat(item.field_name) || 0;

// Division by zero protection
const ratio = denominator > 0 ? (numerator / denominator) * 100 : 0;

// Array length validation
const average = array.length > 0 ? sum / array.length : 0;

// Container existence checks
if (!container) return;
```

### Edge Cases Handled
- **Empty Data Sets:** All functions handle empty `filteredData` arrays
- **Missing Fields:** Default values (0, 'Other', etc.) for undefined fields
- **Invalid Numbers:** `parseFloat()` with fallback to 0
- **Missing DOM Elements:** Early return if containers not found
- **Extreme Values:** Normalization capped at 100% maximum

---

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | Sept 2025 | Initial KPI formulas implementation |
| 1.1 | Sept 2025 | Added debugging and error handling |
| 1.2 | Sept 2025 | Enhanced field mapping and visualization data prep |
| 1.3 | Sept 2025 | **Added all 4 Market Overview visualizations with real data** |
| 1.4 | Sept 2025 | **Complete formula documentation for all visualizations** |

---

## Related Files

- `dashboard.js` - Main implementation file (all formulas)
- `api_working.py` - Backend API providing data
- `index.html` - Frontend display elements with container IDs
- `dashboard.css` - Styling for KPI cards and visualizations

---

## Debug Information

### Console Debugging Commands
```javascript
// Check if containers exist
console.log('TreeMap container:', document.getElementById('treeMap'));
console.log('Force Graph container:', document.getElementById('forceDirectedGraph'));
console.log('Sunburst container:', document.getElementById('sunburstChart'));
console.log('Multi-Metric container:', document.getElementById('parallelCoordinates'));

// Check data structure
console.log('Filtered data length:', dashboard.filteredData?.length);
console.log('Sample record:', dashboard.filteredData?.[0]);

// Verify KPI calculations
const kpis = dashboard.calculateMarketOverviewKPIs();
console.log('Calculated KPIs:', kpis);
```

### Container IDs Reference
- **TreeMap:** `#treeMap`
- **Force-Directed Graph:** `#forceDirectedGraph`
- **Hierarchical Distribution:** `#sunburstChart`
- **Multi-Metric Analysis:** `#parallelCoordinates`

---

*This documentation is automatically maintained and should be updated whenever KPI formulas or visualization algorithms are modified.*

---

## Symbol Analysis Tab - KPIs

### 1. Current Delivery Percentage
**Display Format:** `{percentage}%`  
**Purpose:** Shows the current month delivery percentage for the selected stock.

```javascript
// Formula
const currentDeliveryPercentage = parseFloat(symbolData.current_deliv_per) || 0;

// Mathematical Expression
Current Delivery Percentage = current_deliv_per

// Data Source Field
current_deliv_per - Current month delivery percentage for selected symbol
```

### 2. Month-over-Month Change
**Display Format:** `+{percentage}%` or `-{percentage}%`  
**Purpose:** Shows the percentage change in delivery from previous month to current month.

```javascript
// Formula
const monthOverMonthChange = parseFloat(symbolData.delivery_increase_pct) || 0;

// Mathematical Expression
Month-over-Month Change = delivery_increase_pct

// Data Source Field
delivery_increase_pct - Percentage increase/decrease in delivery
```

### 3. Delivery Quantity Change
**Display Format:** `+{quantity}` or `-{quantity}`  
**Purpose:** Shows the absolute change in delivery quantity from previous month.

```javascript
// Formula
const deliveryQuantityChange = parseFloat(symbolData.delivery_increase_abs) || 0;

// Mathematical Expression
Delivery Quantity Change = delivery_increase_abs

// Data Source Field
delivery_increase_abs - Absolute change in delivery quantity
```

### 4. Current Trading Volume
**Display Format:** `{formatted_quantity}`  
**Purpose:** Shows the total traded quantity for the selected stock in current month.

```javascript
// Formula
const currentTradingVolume = parseFloat(symbolData.current_ttl_trd_qnty) || 0;

// Mathematical Expression
Current Trading Volume = current_ttl_trd_qnty

// Data Source Field
current_ttl_trd_qnty - Current month total traded quantity
```

### 5. Delivery-to-Trading Ratio
**Display Format:** `{ratio}%`  
**Purpose:** Shows what percentage of total trading was delivered for the selected stock.

```javascript
// Formula
const deliveryTradingRatio = symbolData.current_ttl_trd_qnty > 0 ? 
    (parseFloat(symbolData.current_deliv_qty) / parseFloat(symbolData.current_ttl_trd_qnty)) * 100 : 0;

// Mathematical Expression
Delivery-to-Trading Ratio = (current_deliv_qty ÷ current_ttl_trd_qnty) × 100

// Data Source Fields
current_deliv_qty - Current month delivery quantity
current_ttl_trd_qnty - Current month total traded quantity
```

---

## Symbol Analysis Visualizations

### 1. Delivery Trend Analysis (Line Chart)

**Purpose:** Shows delivery percentage trends for the selected symbol
**Container ID:** `deliveryTrendChart`

```javascript
// Data Preparation Formula
const deliveryTrendData = {
    current: {
        month: 'Current Month',
        deliveryPercentage: parseFloat(symbolData.current_deliv_per) || 0,
        deliveryQuantity: parseFloat(symbolData.current_deliv_qty) || 0,
        tradingVolume: parseFloat(symbolData.current_ttl_trd_qnty) || 0
    },
    previous: {
        month: 'Previous Month', 
        deliveryPercentage: parseFloat(symbolData.previous_deliv_per) || 0,
        deliveryQuantity: parseFloat(symbolData.previous_deliv_qty) || 0,
        tradingVolume: parseFloat(symbolData.previous_ttl_trd_qnty) || 0
    }
};

// Mathematical Expression
Trend Analysis = Compare current_deliv_per vs previous_deliv_per
Volume Trend = Compare current_ttl_trd_qnty vs previous_ttl_trd_qnty
Display: Line chart with dual metrics (percentage and volume)
```

### 2. Volume Profile Analysis (Bar Chart)

**Purpose:** Comparative analysis of trading volumes and delivery quantities
**Container ID:** `volumeProfileChart`

```javascript
// Data Preparation Formula
const volumeProfileData = [
    {
        metric: 'Current Month Trading Volume',
        value: parseFloat(symbolData.current_ttl_trd_qnty) || 0,
        type: 'trading'
    },
    {
        metric: 'Current Month Delivery Quantity', 
        value: parseFloat(symbolData.current_deliv_qty) || 0,
        type: 'delivery'
    },
    {
        metric: 'Previous Month Trading Volume',
        value: parseFloat(symbolData.previous_ttl_trd_qnty) || 0,
        type: 'trading'
    },
    {
        metric: 'Previous Month Delivery Quantity',
        value: parseFloat(symbolData.previous_deliv_qty) || 0,
        type: 'delivery'
    }
];

// Mathematical Expressions
Current Trading Volume = current_ttl_trd_qnty
Current Delivery Volume = current_deliv_qty
Previous Trading Volume = previous_ttl_trd_qnty
Previous Delivery Volume = previous_deliv_qty
Display: Grouped bar chart with comparative volumes
```

### 3. Delivery Efficiency Gauge

**Purpose:** Visual gauge showing delivery efficiency as percentage
**Container ID:** `deliveryEfficiencyGauge`

```javascript
// Gauge Calculation Formula
const deliveryEfficiency = symbolData.current_ttl_trd_qnty > 0 ? 
    (parseFloat(symbolData.current_deliv_qty) / parseFloat(symbolData.current_ttl_trd_qnty)) * 100 : 0;

// Efficiency Rating Classification
const getEfficiencyRating = (percentage) => {
    if (percentage >= 80) return { rating: 'Excellent', color: '#4CAF50' };
    if (percentage >= 60) return { rating: 'Good', color: '#2196F3' };
    if (percentage >= 40) return { rating: 'Average', color: '#FF9800' };
    if (percentage >= 20) return { rating: 'Below Average', color: '#FF5722' };
    return { rating: 'Poor', color: '#F44336' };
};

// Mathematical Expression
Delivery Efficiency = (current_deliv_qty ÷ current_ttl_trd_qnty) × 100
Rating = Efficiency percentage classification
Display: Circular gauge with color-coded efficiency rating
```

### 4. Comparative Metrics Table

**Purpose:** Detailed comparison of current vs previous month metrics
**Container ID:** `comparativeMetricsTable`

```javascript
// Comparison Data Structure
const comparisonMetrics = [
    {
        metric: 'Delivery Percentage',
        current: parseFloat(symbolData.current_deliv_per) || 0,
        previous: parseFloat(symbolData.previous_deliv_per) || 0,
        change: parseFloat(symbolData.delivery_increase_pct) || 0,
        unit: '%'
    },
    {
        metric: 'Delivery Quantity',
        current: parseFloat(symbolData.current_deliv_qty) || 0,
        previous: parseFloat(symbolData.previous_deliv_qty) || 0,
        change: parseFloat(symbolData.delivery_increase_abs) || 0,
        unit: 'qty'
    },
    {
        metric: 'Trading Volume',
        current: parseFloat(symbolData.current_ttl_trd_qnty) || 0,
        previous: parseFloat(symbolData.previous_ttl_trd_qnty) || 0,
        change: ((parseFloat(symbolData.current_ttl_trd_qnty) || 0) - (parseFloat(symbolData.previous_ttl_trd_qnty) || 0)),
        unit: 'qty'
    },
    {
        metric: 'Turnover (Lakhs)',
        current: parseFloat(symbolData.current_turnover_lacs) || 0,
        previous: parseFloat(symbolData.previous_turnover_lacs) || 0,
        change: ((parseFloat(symbolData.current_turnover_lacs) || 0) - (parseFloat(symbolData.previous_turnover_lacs) || 0)),
        unit: '₹L'
    }
];

// Change Calculation Formula
const calculatePercentageChange = (current, previous) => {
    return previous > 0 ? ((current - previous) / previous) * 100 : 0;
};

// Mathematical Expressions
Delivery % Change = delivery_increase_pct (direct from database)
Delivery Qty Change = delivery_increase_abs (direct from database)
Trading Volume Change = current_ttl_trd_qnty - previous_ttl_trd_qnty
Turnover Change = current_turnover_lacs - previous_turnover_lacs
Display: Tabular format with current, previous, change, and percentage change columns
```

---

## Symbol Analysis Tab - Data Fields Reference

### Primary Fields Used in Symbol Analysis

| Field Name | Data Type | Description | Usage in Symbol Analysis |
|------------|-----------|-------------|-------------------------|
| `current_deliv_per` | Numeric | Current month delivery percentage | Main KPI, Trend Analysis, Comparison Table |
| `previous_deliv_per` | Numeric | Previous month delivery percentage | Trend Analysis, Comparison Table |
| `delivery_increase_pct` | Numeric | Month-over-month delivery change % | Main KPI, Comparison Table |
| `delivery_increase_abs` | Numeric | Absolute delivery quantity change | KPI, Comparison Table |
| `current_ttl_trd_qnty` | Numeric | Current month total traded quantity | KPI, Volume Profile, Efficiency Gauge |
| `previous_ttl_trd_qnty` | Numeric | Previous month total traded quantity | Volume Profile, Comparison Table |
| `current_deliv_qty` | Numeric | Current month delivery quantity | Efficiency Gauge, Volume Profile |
| `previous_deliv_qty` | Numeric | Previous month delivery quantity | Volume Profile, Comparison Table |
| `current_turnover_lacs` | Numeric | Current month turnover in lakhs | Comparison Table |
| `previous_turnover_lacs` | Numeric | Previous month turnover in lakhs | Comparison Table |

### Symbol Analysis Visualization Data Flow

```javascript
// Initialization Sequence for Symbol Analysis
1. User selects symbol → updateSelectedSymbol(symbol)
2. Find symbol data → this.data.find(item => item.symbol === selectedSymbol)
3. Calculate Symbol KPIs → calculateSymbolAnalysisKPIs(symbolData)
4. Render Visualizations → renderDeliveryTrend(), renderVolumeProfile(), renderEfficiencyGauge(), renderComparisonTable()
5. Update UI → Display all charts and metrics for selected symbol

// Data Validation for Symbol Analysis
symbolData = symbolData || {};
const safeValue = (fieldName, defaultValue = 0) => parseFloat(symbolData[fieldName]) || defaultValue;
```

### Symbol Selection and Error Handling

```javascript
// Symbol Selection Validation
if (!this.selectedSymbol) {
    // Show symbol selection prompt
    displaySymbolSelectionMessage();
    return;
}

// Symbol Data Validation
const symbolData = this.data.find(item => item.symbol === this.selectedSymbol);
if (!symbolData) {
    // Show symbol not found error
    displaySymbolNotFoundError(this.selectedSymbol);
    return;
}

// Field Existence Validation
const hasRequiredFields = symbolData.current_deliv_per !== undefined && 
                         symbolData.current_ttl_trd_qnty !== undefined;
if (!hasRequiredFields) {
    // Show incomplete data warning
    displayIncompleteDataWarning(this.selectedSymbol);
}
```