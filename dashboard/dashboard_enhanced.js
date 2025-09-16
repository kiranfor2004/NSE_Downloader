/**
 * NSE Delivery Analysis Dashboard - Enhanced JavaScript
 * Handles all three tabs with comprehensive charting and interactivity
 */

// Global variables
const API_BASE_URL = 'http://localhost:5001/api';
let currentTab = 'tab1';
let availableSymbols = [];
let availableDates = [];
let selectedSymbol = null;

// Color palettes for consistent theming
const COLORS = {
    primary: '#667eea',
    secondary: '#764ba2',
    success: '#10b981',
    danger: '#ef4444',
    warning: '#f59e0b',
    info: '#3b82f6',
    
    // Chart colors
    chart: {
        bullish: '#10b981',
        bearish: '#ef4444',
        neutral: '#64748b',
        background: '#f8fafc',
        
        // Sequential palette for heatmaps
        sequential: ['#f0fdf4', '#bbf7d0', '#86efac', '#4ade80', '#22c55e', '#16a34a', '#15803d', '#166534'],
        
        // Qualitative palette for categories
        qualitative: ['#3b82f6', '#ef4444', '#10b981', '#f59e0b', '#8b5cf6', '#ec4899', '#06b6d4', '#84cc16']
    }
};

// ===================== INITIALIZATION =====================

$(document).ready(function() {
    initializeDashboard();
});

async function initializeDashboard() {
    console.log('Initializing dashboard...');
    
    try {
        // Check API health
        await checkAPIHealth();
        
        // Load initial data
        await loadAvailableSymbols();
        await loadAvailableDates();
        
        // Load Tab 1 data by default
        await loadTab1Data();
        
        // Setup event listeners
        setupEventListeners();
        
        console.log('Dashboard initialized successfully');
    } catch (error) {
        console.error('Failed to initialize dashboard:', error);
        showError('Failed to initialize dashboard. Please check your connection.');
    }
}

async function checkAPIHealth() {
    try {
        const response = await fetch(`${API_BASE_URL}/health`);
        const data = await response.json();
        
        if (data.status === 'healthy') {
            updateConnectionStatus(true);
        } else {
            updateConnectionStatus(false);
        }
    } catch (error) {
        updateConnectionStatus(false);
        throw error;
    }
}

function updateConnectionStatus(isConnected) {
    const statusElement = document.getElementById('connection-status');
    const indicatorElement = document.querySelector('.status-indicator');
    
    if (isConnected) {
        statusElement.textContent = 'Connected to Database';
        indicatorElement.className = 'status-indicator online';
    } else {
        statusElement.textContent = 'Database Connection Failed';
        indicatorElement.className = 'status-indicator offline';
    }
}

// ===================== TAB MANAGEMENT =====================

function showTab(tabId) {
    // Hide all tabs
    document.querySelectorAll('.tab-content').forEach(tab => {
        tab.classList.remove('active');
    });
    
    // Remove active class from all buttons
    document.querySelectorAll('.tab-button').forEach(button => {
        button.classList.remove('active');
    });
    
    // Show selected tab
    document.getElementById(tabId).classList.add('active');
    
    // Add active class to clicked button
    event.target.classList.add('active');
    
    currentTab = tabId;
    
    // Load tab-specific data
    switch(tabId) {
        case 'tab1':
            loadTab1Data();
            break;
        case 'tab2':
            loadTab2Data();
            break;
        case 'tab3':
            // Tab 3 loads data when symbol is selected
            break;
    }
}

// ===================== DATA LOADING FUNCTIONS =====================

async function loadAvailableSymbols() {
    try {
        const response = await fetch(`${API_BASE_URL}/available-symbols`);
        const data = await response.json();
        availableSymbols = data.symbols;
        
        // Populate symbol selector
        const symbolSelector = document.getElementById('symbol-selector');
        symbolSelector.innerHTML = '<option value="">Select Symbol</option>';
        
        availableSymbols.forEach(symbol => {
            const option = document.createElement('option');
            option.value = symbol.symbol;
            option.textContent = `${symbol.symbol} (${symbol.category})`;
            symbolSelector.appendChild(option);
        });
        
        console.log(`Loaded ${availableSymbols.length} symbols`);
    } catch (error) {
        console.error('Failed to load symbols:', error);
    }
}

async function loadAvailableDates() {
    try {
        const response = await fetch(`${API_BASE_URL}/available-dates`);
        const data = await response.json();
        availableDates = data.dates;
        
        // Populate date selector
        const dateSelector = document.getElementById('trading-date');
        dateSelector.innerHTML = '<option value="">Latest Date</option>';
        
        availableDates.forEach(dateInfo => {
            const option = document.createElement('option');
            option.value = dateInfo.trading_date;
            option.textContent = `${dateInfo.trading_date} (${dateInfo.symbol_count} symbols)`;
            dateSelector.appendChild(option);
        });
        
        console.log(`Loaded ${availableDates.length} trading dates`);
    } catch (error) {
        console.error('Failed to load dates:', error);
    }
}

// ===================== TAB 1: CURRENT MONTH DAILY PERFORMANCE =====================

async function loadTab1Data() {
    if (currentTab !== 'tab1') return;
    
    console.log('Loading Tab 1 data...');
    showLoading('tab1-kpis');
    
    try {
        const selectedDate = document.getElementById('trading-date').value;
        const url = selectedDate ? 
            `${API_BASE_URL}/tab1/daily-performance?trading_date=${selectedDate}` :
            `${API_BASE_URL}/tab1/daily-performance`;
            
        const response = await fetch(url);
        const data = await response.json();
        
        // Update KPIs
        renderTab1KPIs(data.kpis);
        
        // Load and render charts
        await renderHeatmapChart();
        await renderTopPerformersChart(data.daily_data);
        
        // Setup candlestick chart with first symbol if available
        if (data.daily_data && data.daily_data.length > 0) {
            await renderCandlestickChart(data.daily_data[0].symbol);
        }
        
        console.log('Tab 1 data loaded successfully');
    } catch (error) {
        console.error('Failed to load Tab 1 data:', error);
        showError('Failed to load daily performance data');
    }
}

function renderTab1KPIs(kpis) {
    const kpiContainer = document.getElementById('tab1-kpis');
    
    kpiContainer.innerHTML = `
        <div class="kpi-card">
            <div class="kpi-value">${kpis.total_symbols.toLocaleString()}</div>
            <div class="kpi-label">Total Symbols</div>
        </div>
        <div class="kpi-card">
            <div class="kpi-value">${kpis.symbols_with_higher_delivery.toLocaleString()}</div>
            <div class="kpi-label">Symbols with Higher Delivery</div>
            <div class="kpi-change positive">
                ${kpis.percentage_symbols_higher.toFixed(1)}% of total
            </div>
        </div>
        <div class="kpi-card">
            <div class="kpi-value">${kpis.avg_delivery_percentage.toFixed(2)}%</div>
            <div class="kpi-label">Average Delivery Percentage</div>
        </div>
        <div class="kpi-card">
            <div class="kpi-value">${(kpis.total_delivery_quantity / 1000000).toFixed(1)}M</div>
            <div class="kpi-label">Total Delivery Quantity</div>
            <div class="kpi-change">
                ${(kpis.overall_delivery_percentage).toFixed(2)}% of total traded
            </div>
        </div>
    `;
}

async function renderHeatmapChart() {
    try {
        const response = await fetch(`${API_BASE_URL}/tab1/heatmap-data`);
        const data = await response.json();
        
        // Prepare data for heatmap
        const symbols = [...new Set(data.heatmap_data.map(d => d.symbol))].slice(0, 20); // Top 20 symbols
        const dates = [...new Set(data.heatmap_data.map(d => d.date))].sort().slice(-10); // Last 10 dates
        
        const z = symbols.map(symbol => 
            dates.map(date => {
                const point = data.heatmap_data.find(d => d.symbol === symbol && d.date === date);
                return point ? point.value : 0;
            })
        );
        
        const heatmapData = [{
            z: z,
            x: dates,
            y: symbols,
            type: 'heatmap',
            colorscale: [
                [0, COLORS.chart.sequential[0]],
                [0.2, COLORS.chart.sequential[2]],
                [0.4, COLORS.chart.sequential[4]],
                [0.6, COLORS.chart.sequential[6]],
                [0.8, COLORS.chart.sequential[7]],
                [1, COLORS.chart.sequential[7]]
            ],
            showscale: true,
            colorbar: {
                title: 'Delivery %',
                titlefont: { size: 12 }
            }
        }];
        
        const layout = {
            title: '',
            xaxis: { title: 'Trading Date' },
            yaxis: { title: 'Symbol' },
            margin: { l: 80, r: 50, t: 50, b: 80 }
        };
        
        Plotly.newPlot('heatmap-chart', heatmapData, layout, {responsive: true});
        
        // Add click handler for drill-down
        document.getElementById('heatmap-chart').on('plotly_click', function(data) {
            const symbolClicked = data.points[0].y;
            drillDownToSymbol(symbolClicked);
        });
        
    } catch (error) {
        console.error('Failed to render heatmap:', error);
    }
}

async function renderTopPerformersChart(dailyData) {
    if (!dailyData || dailyData.length === 0) return;
    
    // Get top 10 performers
    const topPerformers = dailyData
        .sort((a, b) => b.delivery_increase_pct - a.delivery_increase_pct)
        .slice(0, 10);
    
    const chartData = [{
        x: topPerformers.map(d => d.delivery_increase_pct),
        y: topPerformers.map(d => d.symbol),
        type: 'bar',
        orientation: 'h',
        marker: {
            color: topPerformers.map(d => d.delivery_increase_pct > 0 ? COLORS.success : COLORS.danger)
        }
    }];
    
    const layout = {
        title: '',
        xaxis: { title: 'Delivery Increase %' },
        yaxis: { title: '' },
        margin: { l: 80, r: 50, t: 50, b: 50 }
    };
    
    Plotly.newPlot('top-performers-chart', chartData, layout, {responsive: true});
    
    // Add click handler for drill-down
    document.getElementById('top-performers-chart').on('plotly_click', function(data) {
        const symbolClicked = data.points[0].y;
        drillDownToSymbol(symbolClicked);
    });
}

async function renderCandlestickChart(symbol) {
    if (!symbol) return;
    
    try {
        const response = await fetch(`${API_BASE_URL}/tab1/candlestick-data/${symbol}`);
        const data = await response.json();
        
        if (!data.candlestick_data || data.candlestick_data.length === 0) return;
        
        const candlestickData = data.candlestick_data;
        
        // Candlestick trace
        const candlestick = {
            x: candlestickData.map(d => d.date),
            open: candlestickData.map(d => d.open),
            high: candlestickData.map(d => d.high),
            low: candlestickData.map(d => d.low),
            close: candlestickData.map(d => d.close),
            type: 'candlestick',
            name: `${symbol} Price`,
            increasing: { line: { color: COLORS.success } },
            decreasing: { line: { color: COLORS.danger } }
        };
        
        // Volume trace
        const volume = {
            x: candlestickData.map(d => d.date),
            y: candlestickData.map(d => d.volume),
            type: 'bar',
            name: 'Volume',
            yaxis: 'y2',
            marker: { color: COLORS.chart.neutral }
        };
        
        // Delivery quantity trace
        const delivery = {
            x: candlestickData.map(d => d.date),
            y: candlestickData.map(d => d.delivery_qty),
            type: 'bar',
            name: 'Delivery Qty',
            yaxis: 'y2',
            marker: { color: COLORS.primary, opacity: 0.6 }
        };
        
        const layout = {
            title: `${symbol} - OHLC with Volume and Delivery`,
            xaxis: { title: 'Date' },
            yaxis: { title: 'Price (₹)', side: 'left' },
            yaxis2: { 
                title: 'Volume / Delivery Qty', 
                side: 'right', 
                overlaying: 'y',
                showgrid: false
            },
            margin: { l: 50, r: 50, t: 50, b: 50 },
            showlegend: true
        };
        
        Plotly.newPlot('candlestick-chart', [candlestick, volume, delivery], layout, {responsive: true});
        
    } catch (error) {
        console.error('Failed to render candlestick chart:', error);
    }
}

// ===================== TAB 2: MONTHLY TRENDS AND COMPARISON =====================

async function loadTab2Data() {
    if (currentTab !== 'tab2') return;
    
    console.log('Loading Tab 2 data...');
    showLoading('tab2-kpis');
    
    try {
        // Load monthly trends
        const trendsResponse = await fetch(`${API_BASE_URL}/tab2/monthly-trends`);
        const trendsData = await trendsResponse.json();
        
        // Load category comparison
        const categoryResponse = await fetch(`${API_BASE_URL}/tab2/category-comparison`);
        const categoryData = await categoryResponse.json();
        
        // Render KPIs
        renderTab2KPIs(trendsData.monthly_data);
        
        // Render charts
        renderBulletChart(trendsData.monthly_data);
        renderColumnChart(trendsData.monthly_data);
        renderTreemapChart(trendsData.symbol_contributions);
        renderCategoryChart(categoryData.category_data);
        
        console.log('Tab 2 data loaded successfully');
    } catch (error) {
        console.error('Failed to load Tab 2 data:', error);
        showError('Failed to load monthly trends data');
    }
}

function renderTab2KPIs(monthlyData) {
    if (!monthlyData || monthlyData.length === 0) return;
    
    const currentMonth = monthlyData[0];
    const previousMonth = monthlyData[1] || {};
    
    const kpiContainer = document.getElementById('tab2-kpis');
    
    kpiContainer.innerHTML = `
        <div class="kpi-card">
            <div class="kpi-value">${(currentMonth.total_delivery_qty / 1000000).toFixed(1)}M</div>
            <div class="kpi-label">Current Month Total Delivery</div>
            <div class="kpi-change ${currentMonth.total_delivery_qty > (previousMonth.total_delivery_qty || 0) ? 'positive' : 'negative'}">
                vs ${(previousMonth.total_delivery_qty / 1000000 || 0).toFixed(1)}M previous
            </div>
        </div>
        <div class="kpi-card">
            <div class="kpi-value">${(currentMonth.avg_daily_delivery / 1000).toFixed(1)}K</div>
            <div class="kpi-label">Average Daily Delivery</div>
            <div class="kpi-change">
                Over ${currentMonth.trading_days} trading days
            </div>
        </div>
        <div class="kpi-card">
            <div class="kpi-value">${currentMonth.delivery_volume_ratio.toFixed(2)}%</div>
            <div class="kpi-label">Delivery Volume Ratio</div>
        </div>
        <div class="kpi-card">
            <div class="kpi-value">${currentMonth.avg_delivery_increase_pct.toFixed(1)}%</div>
            <div class="kpi-label">Average Delivery Increase</div>
        </div>
    `;
}

function renderBulletChart(monthlyData) {
    if (!monthlyData || monthlyData.length < 2) return;
    
    const current = monthlyData[0];
    const previous = monthlyData[1];
    
    const bulletData = [{
        type: "indicator",
        mode: "gauge",
        value: current.total_delivery_qty,
        domain: { x: [0, 1], y: [0, 1] },
        title: { text: "Monthly Delivery vs Target" },
        gauge: {
            axis: { range: [null, Math.max(current.total_delivery_qty, previous.total_delivery_qty) * 1.2] },
            bar: { color: COLORS.primary },
            steps: [
                { range: [0, previous.total_delivery_qty], color: COLORS.chart.background }
            ],
            threshold: {
                line: { color: COLORS.danger, width: 4 },
                thickness: 0.75,
                value: previous.total_delivery_qty
            }
        }
    }];
    
    const layout = {
        margin: { l: 20, r: 20, t: 40, b: 20 }
    };
    
    Plotly.newPlot('bullet-chart', bulletData, layout, {responsive: true});
}

function renderColumnChart(monthlyData) {
    if (!monthlyData || monthlyData.length < 2) return;
    
    const chartData = [{
        x: monthlyData.map(d => `${d.year}-${String(d.month).padStart(2, '0')}`),
        y: monthlyData.map(d => d.avg_daily_delivery),
        type: 'bar',
        name: 'Average Daily Delivery',
        marker: {
            color: monthlyData.map((d, i) => i === 0 ? COLORS.primary : COLORS.secondary)
        }
    }];
    
    const layout = {
        title: '',
        xaxis: { title: 'Month' },
        yaxis: { title: 'Average Daily Delivery Quantity' },
        margin: { l: 60, r: 50, t: 50, b: 60 }
    };
    
    Plotly.newPlot('column-chart', chartData, layout, {responsive: true});
}

function renderTreemapChart(symbolContributions) {
    if (!symbolContributions || symbolContributions.length === 0) return;
    
    // Take top 20 symbols for better visualization
    const topSymbols = symbolContributions.slice(0, 20);
    
    const treemapData = [{
        type: "treemap",
        labels: topSymbols.map(d => d.symbol),
        values: topSymbols.map(d => d.total_delivery_qty),
        parents: topSymbols.map(d => d.category),
        text: topSymbols.map(d => `${d.symbol}<br>${d.contribution_percentage.toFixed(2)}%`),
        textinfo: "label+text",
        hovertemplate: '<b>%{label}</b><br>Delivery: %{value:,.0f}<br>Contribution: %{text}<extra></extra>',
        marker: {
            colorscale: 'Blues',
            showscale: true
        }
    }];
    
    const layout = {
        title: '',
        margin: { l: 20, r: 20, t: 50, b: 20 }
    };
    
    Plotly.newPlot('treemap-chart', treemapData, layout, {responsive: true});
    
    // Add click handler for drill-down
    document.getElementById('treemap-chart').on('plotly_click', function(data) {
        const symbolClicked = data.points[0].label;
        drillDownToSymbol(symbolClicked);
    });
}

function renderCategoryChart(categoryData) {
    if (!categoryData || categoryData.length === 0) return;
    
    const chartData = [{
        x: categoryData.map(d => d.category),
        y: categoryData.map(d => d.total_delivery),
        type: 'bar',
        marker: {
            color: categoryData.map((d, i) => COLORS.chart.qualitative[i % COLORS.chart.qualitative.length])
        }
    }];
    
    const layout = {
        title: '',
        xaxis: { title: 'Category' },
        yaxis: { title: 'Total Delivery Quantity' },
        margin: { l: 60, r: 50, t: 50, b: 100 }
    };
    
    Plotly.newPlot('category-chart', chartData, layout, {responsive: true});
}

// ===================== TAB 3: SYMBOL-SPECIFIC DEEP DIVE =====================

function setupEventListeners() {
    // Symbol search
    const symbolSearchInput = document.getElementById('symbol-search-input');
    symbolSearchInput.addEventListener('input', handleSymbolSearch);
    symbolSearchInput.addEventListener('keypress', function(e) {
        if (e.key === 'Enter') {
            const firstMatch = document.querySelector('.symbol-suggestion.active');
            if (firstMatch) {
                loadSymbolDetails(firstMatch.textContent);
            }
        }
    });
    
    // Symbol selector change
    document.getElementById('symbol-selector').addEventListener('change', function(e) {
        if (e.target.value) {
            renderCandlestickChart(e.target.value);
        }
    });
}

function handleSymbolSearch() {
    const searchTerm = document.getElementById('symbol-search-input').value.toUpperCase();
    
    if (searchTerm.length >= 2) {
        const matchingSymbols = availableSymbols.filter(s => 
            s.symbol.toUpperCase().includes(searchTerm)
        ).slice(0, 10);
        
        showSymbolSuggestions(matchingSymbols);
    } else {
        hideSymbolSuggestions();
    }
}

function showSymbolSuggestions(symbols) {
    // Implementation for symbol suggestions dropdown
    // This would show a dropdown list of matching symbols
}

function hideSymbolSuggestions() {
    // Implementation to hide symbol suggestions
}

async function loadSymbolDetails(symbol) {
    selectedSymbol = symbol;
    
    try {
        const response = await fetch(`${API_BASE_URL}/tab3/symbol-detail/${symbol}`);
        const data = await response.json();
        
        if (data.error) {
            showError(`Symbol ${symbol} not found`);
            return;
        }
        
        // Show symbol info panel
        document.getElementById('symbol-info-panel').classList.remove('hidden');
        document.getElementById('tab3-kpis').classList.remove('hidden');
        document.getElementById('tab3-charts-row1').classList.remove('hidden');
        document.getElementById('tab3-charts-row2').classList.remove('hidden');
        document.getElementById('no-symbol-message').classList.add('hidden');
        
        // Update symbol name
        document.getElementById('selected-symbol-name').textContent = `${symbol} Analysis`;
        
        // Render KPIs
        renderTab3KPIs(data.summary);
        
        // Render charts
        await renderDualAxisChart(symbol);
        await renderScatterChart(symbol);
        renderDonutChart(data.latest_data);
        
    } catch (error) {
        console.error('Failed to load symbol details:', error);
        showError(`Failed to load details for ${symbol}`);
    }
}

function renderTab3KPIs(summary) {
    const kpiContainer = document.getElementById('tab3-kpis');
    
    kpiContainer.innerHTML = `
        <div class="kpi-card">
            <div class="kpi-value">${(summary.current_month_total_delivery / 1000).toFixed(1)}K</div>
            <div class="kpi-label">Current Month Delivery</div>
            <div class="kpi-change ${summary.month_over_month_change > 0 ? 'positive' : 'negative'}">
                ${summary.month_over_month_change > 0 ? '+' : ''}${summary.month_over_month_change.toFixed(1)}% MoM
            </div>
        </div>
        <div class="kpi-card">
            <div class="kpi-value">${summary.correlation_price_delivery.toFixed(3)}</div>
            <div class="kpi-label">Price-Delivery Correlation</div>
            <div class="kpi-change ${Math.abs(summary.correlation_price_delivery) > 0.5 ? 'positive' : 'neutral'}">
                ${Math.abs(summary.correlation_price_delivery) > 0.5 ? 'Strong' : 'Moderate'} correlation
            </div>
        </div>
        <div class="kpi-card">
            <div class="kpi-value">${summary.latest_delivery_percentage.toFixed(2)}%</div>
            <div class="kpi-label">Latest Delivery %</div>
        </div>
        <div class="kpi-card">
            <div class="kpi-value">${summary.trading_days}</div>
            <div class="kpi-label">Trading Days</div>
            <div class="kpi-change">
                ${summary.category} | ${summary.index_name}
            </div>
        </div>
    `;
}

async function renderDualAxisChart(symbol) {
    try {
        const response = await fetch(`${API_BASE_URL}/tab3/symbol-detail/${symbol}`);
        const data = await response.json();
        
        const symbolData = data.symbol_data;
        
        // Delivery quantity line
        const deliveryTrace = {
            x: symbolData.map(d => d.current_trade_date),
            y: symbolData.map(d => d.current_deliv_qty),
            type: 'scatter',
            mode: 'lines+markers',
            name: 'Daily Delivery Qty',
            yaxis: 'y',
            line: { color: COLORS.primary }
        };
        
        // Turnover column
        const turnoverTrace = {
            x: symbolData.map(d => d.current_trade_date),
            y: symbolData.map(d => d.current_turnover_lacs),
            type: 'bar',
            name: 'Daily Turnover (Lacs)',
            yaxis: 'y2',
            marker: { color: COLORS.secondary, opacity: 0.6 }
        };
        
        const layout = {
            title: '',
            xaxis: { title: 'Date' },
            yaxis: { title: 'Delivery Quantity', side: 'left' },
            yaxis2: { 
                title: 'Turnover (Lacs)', 
                side: 'right', 
                overlaying: 'y',
                showgrid: false
            },
            margin: { l: 60, r: 60, t: 50, b: 60 }
        };
        
        Plotly.newPlot('dual-axis-chart', [deliveryTrace, turnoverTrace], layout, {responsive: true});
        
    } catch (error) {
        console.error('Failed to render dual-axis chart:', error);
    }
}

async function renderScatterChart(symbol) {
    try {
        const response = await fetch(`${API_BASE_URL}/tab3/symbol-correlation/${symbol}`);
        const data = await response.json();
        
        const scatterData = [{
            x: data.scatter_data.map(d => d.delivery_qty),
            y: data.scatter_data.map(d => d.price),
            type: 'scatter',
            mode: 'markers',
            marker: {
                color: COLORS.primary,
                size: 8,
                opacity: 0.7
            },
            text: data.scatter_data.map(d => d.date),
            hovertemplate: 'Delivery: %{x:,.0f}<br>Price: ₹%{y:.2f}<br>Date: %{text}<extra></extra>'
        }];
        
        const layout = {
            title: '',
            xaxis: { title: 'Delivery Quantity' },
            yaxis: { title: 'Close Price (₹)' },
            margin: { l: 60, r: 50, t: 50, b: 60 }
        };
        
        Plotly.newPlot('scatter-chart', scatterData, layout, {responsive: true});
        
    } catch (error) {
        console.error('Failed to render scatter chart:', error);
    }
}

function renderDonutChart(latestData) {
    if (!latestData) return;
    
    const deliveryQty = latestData.current_deliv_qty || 0;
    const totalTraded = latestData.current_ttl_trd_qnty || 0;
    const nonDeliveryQty = totalTraded - deliveryQty;
    
    const donutData = [{
        values: [deliveryQty, nonDeliveryQty],
        labels: ['Delivery Quantity', 'Non-Delivery Quantity'],
        type: 'pie',
        hole: 0.4,
        marker: {
            colors: [COLORS.primary, COLORS.chart.neutral]
        },
        textinfo: 'label+percent',
        textposition: 'outside'
    }];
    
    const layout = {
        title: '',
        showlegend: true,
        margin: { l: 50, r: 50, t: 50, b: 50 }
    };
    
    Plotly.newPlot('donut-chart', donutData, layout, {responsive: true});
}

// ===================== DRILL-DOWN FUNCTIONALITY =====================

function drillDownToSymbol(symbol) {
    // Switch to Tab 3
    showTab('tab3');
    
    // Update search input
    document.getElementById('symbol-search-input').value = symbol;
    
    // Load symbol details
    loadSymbolDetails(symbol);
}

// ===================== UTILITY FUNCTIONS =====================

function showLoading(containerId) {
    const container = document.getElementById(containerId);
    container.innerHTML = `
        <div class="loading">
            <div class="spinner"></div>
        </div>
    `;
}

function showError(message) {
    // You can implement a more sophisticated error display
    console.error(message);
    
    // Show error in a toast or modal
    alert(message);
}

function formatNumber(num) {
    if (num >= 1000000) {
        return (num / 1000000).toFixed(1) + 'M';
    } else if (num >= 1000) {
        return (num / 1000).toFixed(1) + 'K';
    }
    return num.toLocaleString();
}

function formatCurrency(amount) {
    return new Intl.NumberFormat('en-IN', {
        style: 'currency',
        currency: 'INR',
        minimumFractionDigits: 0,
        maximumFractionDigits: 0
    }).format(amount);
}

// ===================== AUTO-REFRESH FUNCTIONALITY =====================

// Auto-refresh every 5 minutes for the current tab
setInterval(() => {
    switch(currentTab) {
        case 'tab1':
            loadTab1Data();
            break;
        case 'tab2':
            loadTab2Data();
            break;
        case 'tab3':
            if (selectedSymbol) {
                loadSymbolDetails(selectedSymbol);
            }
            break;
    }
}, 5 * 60 * 1000); // 5 minutes