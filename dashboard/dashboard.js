// NSE Delivery Analysis Dashboard - Professional Version

class ProfessionalNSEDashboard {
    constructor() {
        this.data = null;
        this.filteredData = null;
        this.selectedSymbol = null;
        this.apiBaseUrl = 'http://localhost:5000/api';
        this.currentTab = 'market-overview';
        
        this.init();
    }

    async init() {
        console.log('Dashboard initializing...');
        this.showLoading();
        this.setupEventListeners();
        console.log('About to load data...');
        await this.loadData();
        console.log('Data loaded, rendering dashboard...');
        this.renderDashboard();
        this.hideLoading();
        console.log('Dashboard initialization complete');
    }

    setupEventListeners() {
        // Tab navigation
        document.querySelectorAll('.tab-btn').forEach(btn => {
            btn.addEventListener('click', (e) => {
                this.switchTab(btn.dataset.tab);
            });
        });

        // Refresh button
        document.getElementById('refreshData').addEventListener('click', () => {
            this.refreshData();
        });

        // Global filter
        document.getElementById('globalFilter').addEventListener('change', (e) => {
            this.filterData(e.target.value);
        });

        // Symbol search
        document.getElementById('symbolSearch').addEventListener('input', (e) => {
            this.searchSymbol(e.target.value);
        });

        // Popular symbol chips
        document.querySelectorAll('.symbol-chip').forEach(chip => {
            chip.addEventListener('click', (e) => {
                this.selectSymbol(e.target.dataset.symbol);
            });
        });

        // Search button
        document.querySelector('.search-btn').addEventListener('click', () => {
            const symbol = document.getElementById('symbolSearch').value.trim().toUpperCase();
            if (symbol) {
                this.selectSymbol(symbol);
            }
        });

        // Enter key for symbol search
        document.getElementById('symbolSearch').addEventListener('keypress', (e) => {
            if (e.key === 'Enter') {
                const symbol = e.target.value.trim().toUpperCase();
                if (symbol) {
                    this.selectSymbol(symbol);
                }
            }
        });
    }

    async loadData() {
        try {
            console.log('Starting to load data...');
            console.log('API URL:', `${this.apiBaseUrl}/delivery-data?limit=5000`);
            
            const response = await fetch(`${this.apiBaseUrl}/delivery-data?limit=5000`);
            console.log('Response received:', response.status, response.statusText);
            
            if (!response.ok) throw new Error(`Failed to fetch data: ${response.status} ${response.statusText}`);
            
            const responseData = await response.json();
            console.log('Response data structure:', Object.keys(responseData));
            console.log('Data length:', responseData.data?.length);
            
            // Extract data array from the API response with better error handling
            if (responseData && responseData.data && Array.isArray(responseData.data)) {
                this.data = responseData.data;
                this.filteredData = [...this.data];
                console.log('Data loaded successfully:', this.data.length, 'records');
            } else {
                console.error('Invalid data structure received:', responseData);
                this.data = [];
                this.filteredData = [];
                throw new Error('Invalid data structure received from API');
            }
            
            // Update header stats
            this.updateHeaderStats();
            
        } catch (error) {
            console.error('Error loading data:', error);
            this.data = [];
            this.filteredData = [];
            this.showError('Failed to load data. Please check your connection.');
        }
    }

    updateHeaderStats() {
        const totalRecordsElement = document.getElementById('totalRecords');
        const lastUpdatedElement = document.getElementById('lastUpdated');
        
        if (totalRecordsElement && this.data && Array.isArray(this.data)) {
            totalRecordsElement.textContent = this.data.length.toLocaleString();
        } else if (totalRecordsElement) {
            totalRecordsElement.textContent = '0';
        }
        
        if (lastUpdatedElement) {
            lastUpdatedElement.textContent = new Date().toLocaleTimeString();
        }
    }

    filterData(category) {
        if (category === 'all') {
            this.filteredData = [...this.data];
        } else {
            this.filteredData = this.data.filter(item => item.category === category);
        }
        this.renderCurrentTab();
    }

    switchTab(tabId) {
        // Update tab buttons
        document.querySelectorAll('.tab-btn').forEach(btn => {
            btn.classList.remove('active');
        });
        document.querySelector(`[data-tab="${tabId}"]`).classList.add('active');

        // Update tab content
        document.querySelectorAll('.tab-content').forEach(content => {
            content.classList.remove('active');
        });
        document.getElementById(tabId).classList.add('active');

        this.currentTab = tabId;
        this.renderCurrentTab();
    }

    renderCurrentTab() {
        console.log('renderCurrentTab called for:', this.currentTab);
        console.log('Data available:', !!this.data, 'Length:', this.data?.length);
        console.log('Filtered data available:', !!this.filteredData, 'Length:', this.filteredData?.length);
        
        switch (this.currentTab) {
            case 'market-overview':
                this.renderMarketOverview();
                break;
            case 'symbol-analysis':
                this.renderSymbolAnalysis();
                break;
            case 'category-performance':
                this.renderCategoryPerformance();
                break;
        }
    }

    renderDashboard() {
        this.renderMarketOverview();
        this.renderSymbolAnalysis();
        this.renderCategoryPerformance();
    }

    // Tab 1: Market Overview
    renderMarketOverview() {
        console.log('renderMarketOverview called');
        console.log('filteredData exists:', !!this.filteredData);
        console.log('filteredData length:', this.filteredData?.length);
        
        if (!this.filteredData || this.filteredData.length === 0) {
            console.log('No filtered data available, returning early');
            return;
        }

        console.log('Calculating KPIs...');
        // Calculate KPIs
        const kpis = this.calculateMarketOverviewKPIs();
        
        console.log('Updating KPI display elements...');
        // Update KPI cards with null checks
        const totalDeliveryElement = document.getElementById('totalDeliveryIncrease');
        const positiveGrowthElement = document.getElementById('positiveGrowthStocks');
        const positiveGrowthPctElement = document.getElementById('positiveGrowthPercentage');
        const deliveryRatioElement = document.getElementById('deliveryTurnoverRatio');
        const avgTurnoverElement = document.getElementById('avgDailyTurnover');

        if (totalDeliveryElement) {
            totalDeliveryElement.textContent = `₹${(kpis.totalDeliveryIncrease || 0).toFixed(2)}L`;
            console.log('Updated totalDeliveryIncrease:', totalDeliveryElement.textContent);
        }
        
        if (positiveGrowthElement) {
            positiveGrowthElement.textContent = (kpis.positiveGrowthCount || 0).toLocaleString();
            console.log('Updated positiveGrowthStocks:', positiveGrowthElement.textContent);
        }
        
        if (positiveGrowthPctElement) {
            positiveGrowthPctElement.textContent = `${(kpis.positiveGrowthPercentage || 0).toFixed(1)}% of total`;
            console.log('Updated positiveGrowthPercentage:', positiveGrowthPctElement.textContent);
        }
        
        if (deliveryRatioElement) {
            deliveryRatioElement.textContent = `${(kpis.deliveryTurnoverRatio || 0).toFixed(2)}%`;
            console.log('Updated deliveryTurnoverRatio:', deliveryRatioElement.textContent);
        }
        
        if (avgTurnoverElement) {
            avgTurnoverElement.textContent = this.formatCurrency(kpis.avgDailyTurnover || 0);
            console.log('Updated avgDailyTurnover:', avgTurnoverElement.textContent);
        }

        console.log('KPI display updated successfully');

        // Prepare visualization data
        const forceGraphData = this.prepareForceDirectedGraphData();
        const treeMapData = this.prepareTreeMapData();

        // Render visualizations
        this.renderForceDirectedGraph(forceGraphData);
        this.renderTreeMap(treeMapData);
        this.renderHierarchicalDistribution(treeMapData);
        this.renderMultiMetricAnalysis();
    }

    calculateMarketOverviewKPIs() {
        console.log('Calculating KPIs with filtered data length:', this.filteredData.length);
        console.log('Sample data record:', this.filteredData[0]);
        
        // 1. Total Market Delivery Increase (in Lakhs): Sum of delivery_increase_abs
        const totalDeliveryIncrease = this.filteredData.reduce((sum, item) => {
            const deliveryIncreaseAbs = parseFloat(item.delivery_increase_abs) || 0;
            return sum + deliveryIncreaseAbs;
        }, 0) / 100000; // Convert to lakhs
        console.log('Total Delivery Increase (Lakhs):', totalDeliveryIncrease);

        // 2. Stocks with Positive Delivery Growth: Count where delivery_increase_pct > 0
        const positiveGrowthCount = this.filteredData.filter(item => {
            const deliveryIncreasePct = parseFloat(item.delivery_increase_pct) || 0;
            return deliveryIncreasePct > 0;
        }).length;
        console.log('Positive Growth Count:', positiveGrowthCount);

        const positiveGrowthPercentage = (positiveGrowthCount / this.filteredData.length) * 100;
        console.log('Positive Growth Percentage:', positiveGrowthPercentage);

        // 3. Market Delivery-to-Turnover Ratio: (Sum of current_deliv_qty / Sum of current_turnover_lacs)
        const totalCurrentDelivQty = this.filteredData.reduce((sum, item) => {
            const currentDelivQty = parseFloat(item.current_deliv_qty) || 0;
            return sum + currentDelivQty;
        }, 0);
        console.log('Total Current Delivery Qty:', totalCurrentDelivQty);

        const totalCurrentTurnoverLacs = this.filteredData.reduce((sum, item) => {
            const currentTurnoverLacs = parseFloat(item.current_turnover_lacs) || 0;
            return sum + currentTurnoverLacs;
        }, 0);
        console.log('Total Current Turnover Lacs:', totalCurrentTurnoverLacs);

        const deliveryTurnoverRatio = totalCurrentTurnoverLacs > 0 ? 
            (totalCurrentDelivQty / (totalCurrentTurnoverLacs * 100000)) * 100 : 0; // Convert lacs to actual value
        console.log('Delivery Turnover Ratio:', deliveryTurnoverRatio);

        // 4. Average Daily Turnover (keeping this for display purposes)
        const avgDailyTurnover = this.filteredData.length > 0 ? 
            (totalCurrentTurnoverLacs * 100000) / this.filteredData.length : 0;
        console.log('Average Daily Turnover:', avgDailyTurnover);

        const kpis = {
            totalDeliveryIncrease,
            positiveGrowthCount,
            positiveGrowthPercentage,
            deliveryTurnoverRatio,
            avgDailyTurnover,
            // Additional data for visualizations
            totalCurrentTurnoverLacs,
            totalCurrentDelivQty
        };
        
        console.log('Final KPIs:', kpis);
        return kpis;
    }

    prepareForceDirectedGraphData() {
        // Prepare nodes and links for Force-Directed Graph visualization
        const nodeMap = new Map();
        const links = [];

        // Group data by index_name for nodes
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

        return {
            nodes: Array.from(nodeMap.values()),
            links: links
        };
    }

    prepareTreeMapData() {
        // Prepare hierarchical data for TreeMap visualization
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

        // Convert to hierarchical structure
        const treeMapData = {
            name: "Market",
            children: Array.from(categoryMap.values()).sort((a, b) => b.value - a.value)
        };

        // Sort children within each category
        treeMapData.children.forEach(category => {
            category.children.sort((a, b) => b.value - a.value);
        });

        return treeMapData;
    }

    renderForceDirectedGraph(data) {
        const container = document.getElementById('forceDirectedGraph');
        console.log('renderForceDirectedGraph called, container found:', !!container);
        if (!container) return;

        // Clear existing content
        container.innerHTML = '';

        // Create visualization info
        const info = document.createElement('div');
        info.className = 'visualization-info';
        info.innerHTML = `
            <h3>Index-Stock Relationships</h3>
            <p><strong>Nodes:</strong> ${data.nodes.length} (${data.nodes.filter(n => n.type === 'index').length} indices, ${data.nodes.filter(n => n.type === 'symbol').length} symbols)</p>
            <p><strong>Links:</strong> ${data.links.length} symbol-index connections</p>
            <p><strong>Total Market Size:</strong> ₹${data.nodes.filter(n => n.type === 'index').reduce((sum, n) => sum + n.size, 0).toFixed(2)}L</p>
        `;
        container.appendChild(info);

        // Create actual data visualization
        const visualization = document.createElement('div');
        visualization.className = 'force-graph-data';
        
        // Show top indices by size
        const topIndices = data.nodes
            .filter(n => n.type === 'index')
            .sort((a, b) => b.size - a.size)
            .slice(0, 6);
            
        const indicesDisplay = topIndices.map(index => {
            const connectedSymbols = data.links.filter(link => link.source === index.id).length;
            return `
                <div class="index-card">
                    <h4>${index.id}</h4>
                    <p>Size: ₹${index.size.toFixed(2)}L</p>
                    <p>Symbols: ${connectedSymbols}</p>
                    <div class="index-bar" style="width: ${Math.min(100, (index.size / topIndices[0].size) * 100)}%"></div>
                </div>
            `;
        }).join('');

        visualization.innerHTML = `
            <div class="network-visualization">
                <h4>Top Indices by Market Size</h4>
                <div class="indices-grid">
                    ${indicesDisplay}
                </div>
            </div>
        `;
        container.appendChild(visualization);
    }

    renderTreeMap(data) {
        const container = document.getElementById('treeMap');
        console.log('renderTreeMap called, container found:', !!container);
        if (!container) return;

        // Clear existing content
        container.innerHTML = '';

        // Create visualization info
        const info = document.createElement('div');
        info.className = 'visualization-info';
        info.innerHTML = `
            <h3>Market Hierarchy</h3>
            <p><strong>Categories:</strong> ${data.children.length}</p>
            <p><strong>Total Symbols:</strong> ${data.children.reduce((sum, cat) => sum + cat.children.length, 0)}</p>
            <p><strong>Total Market Value:</strong> ₹${data.children.reduce((sum, cat) => sum + cat.value, 0).toFixed(2)}L</p>
        `;
        container.appendChild(info);

        // Create sample visualization with top categories
        const visualization = document.createElement('div');
        visualization.className = 'treemap-placeholder';
        
        const topCategories = data.children.slice(0, 5);
        const categoryBoxes = topCategories.map(category => `
            <div class="category-box" style="flex: ${category.value}">
                <h4>${category.name}</h4>
                <p>₹${category.value.toFixed(2)}L</p>
                <p>${category.children.length} symbols</p>
            </div>
        `).join('');

        visualization.innerHTML = `
            <div class="treemap-container">
                <h4>Top Categories by Turnover</h4>
                <div class="category-boxes">
                    ${categoryBoxes}
                </div>
            </div>
        `;
        container.appendChild(visualization);
    }

    renderHierarchicalDistribution(data) {
        const container = document.getElementById('sunburstChart');
        console.log('renderHierarchicalDistribution called, container found:', !!container);
        if (!container) return;

        // Clear existing content
        container.innerHTML = '';

        // Create visualization info
        const info = document.createElement('div');
        info.className = 'visualization-info';
        info.innerHTML = `
            <h3>Hierarchical Market Structure</h3>
            <p><strong>Categories:</strong> ${data.children.length}</p>
            <p><strong>Levels:</strong> Category → Symbol → Delivery</p>
            <p><strong>Total Market:</strong> ₹${data.children.reduce((sum, cat) => sum + cat.value, 0).toFixed(2)}L</p>
        `;
        container.appendChild(info);

        // Create hierarchical rings display
        const visualization = document.createElement('div');
        visualization.className = 'sunburst-data';
        
        // Calculate percentages for each category
        const totalValue = data.children.reduce((sum, cat) => sum + cat.value, 0);
        const categoryRings = data.children.slice(0, 8).map((category, index) => {
            const percentage = ((category.value / totalValue) * 100);
            const topSymbols = category.children.slice(0, 3);
            
            return `
                <div class="category-ring" style="--ring-color: hsl(${index * 45}, 70%, 60%)">
                    <div class="ring-header">
                        <h4>${category.name}</h4>
                        <span class="ring-percentage">${percentage.toFixed(1)}%</span>
                    </div>
                    <div class="ring-value">₹${category.value.toFixed(2)}L</div>
                    <div class="ring-symbols">
                        ${topSymbols.map(symbol => `<span class="symbol-tag">${symbol.name}</span>`).join('')}
                        ${category.children.length > 3 ? `<span class="more-symbols">+${category.children.length - 3} more</span>` : ''}
                    </div>
                </div>
            `;
        }).join('');

        visualization.innerHTML = `
            <div class="sunburst-container">
                <h4>Market Hierarchy Breakdown</h4>
                <div class="category-rings">
                    ${categoryRings}
                </div>
            </div>
        `;
        container.appendChild(visualization);
    }

    renderMultiMetricAnalysis() {
        const container = document.getElementById('parallelCoordinates');
        console.log('renderMultiMetricAnalysis called, container found:', !!container);
        if (!container) return;

        // Clear existing content
        container.innerHTML = '';

        // Prepare multi-metric data
        const topStocks = this.filteredData
            .filter(item => parseFloat(item.current_turnover_lacs) > 0)
            .sort((a, b) => parseFloat(b.current_turnover_lacs) - parseFloat(a.current_turnover_lacs))
            .slice(0, 10);

        // Create visualization info
        const info = document.createElement('div');
        info.className = 'visualization-info';
        info.innerHTML = `
            <h3>Multi-Dimensional Stock Analysis</h3>
            <p><strong>Top Performers:</strong> ${topStocks.length} stocks by turnover</p>
            <p><strong>Metrics:</strong> Turnover, Delivery %, Delivery Increase, Volume</p>
            <p><strong>Analysis:</strong> Correlation patterns across multiple dimensions</p>
        `;
        container.appendChild(info);

        // Create metrics table
        const visualization = document.createElement('div');
        visualization.className = 'parallel-coordinates-data';
        
        // Calculate metric scales
        const maxTurnover = Math.max(...topStocks.map(s => parseFloat(s.current_turnover_lacs)));
        const maxDelivPct = Math.max(...topStocks.map(s => parseFloat(s.delivery_increase_pct) || 0));
        const maxDelivQty = Math.max(...topStocks.map(s => parseFloat(s.current_deliv_qty) || 0));

        const stockRows = topStocks.map((stock, index) => {
            const turnover = parseFloat(stock.current_turnover_lacs) || 0;
            const delivPct = parseFloat(stock.delivery_increase_pct) || 0;
            const delivQty = parseFloat(stock.current_deliv_qty) || 0;
            const delivIncrease = parseFloat(stock.delivery_increase_abs) || 0;
            
            // Normalize values for bar display (0-100%)
            const turnoverNorm = (turnover / maxTurnover) * 100;
            const delivPctNorm = Math.min(100, Math.abs(delivPct / maxDelivPct) * 100);
            const delivQtyNorm = (delivQty / maxDelivQty) * 100;
            
            return `
                <tr class="stock-row">
                    <td class="stock-symbol">${stock.symbol}</td>
                    <td class="metric-cell">
                        <div class="metric-bar turnover-bar" style="width: ${turnoverNorm}%"></div>
                        <span class="metric-value">₹${turnover.toFixed(2)}L</span>
                    </td>
                    <td class="metric-cell">
                        <div class="metric-bar delivery-pct-bar" style="width: ${delivPctNorm}%"></div>
                        <span class="metric-value">${delivPct.toFixed(2)}%</span>
                    </td>
                    <td class="metric-cell">
                        <div class="metric-bar delivery-qty-bar" style="width: ${delivQtyNorm}%"></div>
                        <span class="metric-value">${(delivQty/1000).toFixed(1)}K</span>
                    </td>
                    <td class="metric-cell">
                        <span class="metric-value trend-${delivIncrease > 0 ? 'up' : 'down'}">${(delivIncrease/1000).toFixed(1)}K</span>
                    </td>
                </tr>
            `;
        }).join('');

        visualization.innerHTML = `
            <div class="parallel-coordinates-container">
                <h4>Top Stocks Multi-Metric Comparison</h4>
                <table class="metrics-table">
                    <thead>
                        <tr>
                            <th>Symbol</th>
                            <th>Turnover (₹L)</th>
                            <th>Delivery Change (%)</th>
                            <th>Delivery Qty (K)</th>
                            <th>Qty Increase (K)</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${stockRows}
                    </tbody>
                </table>
            </div>
        `;
        container.appendChild(visualization);
    }

    // Tab 2: Symbol Analysis
    renderSymbolAnalysis() {
        console.log('renderSymbolAnalysis called, selectedSymbol:', this.selectedSymbol);
        
        if (!this.selectedSymbol) {
            this.renderDefaultSymbolView();
            return;
        }

        const symbolData = this.data.find(item => item.symbol === this.selectedSymbol);
        
        if (!symbolData) {
            this.renderSymbolNotFound();
            return;
        }

        console.log('Rendering symbol analysis for:', this.selectedSymbol);
        console.log('Symbol data:', symbolData);
        
        // Calculate and display KPIs
        this.renderSymbolKPIs(symbolData);
        
        // Render visualizations
        this.renderDeliveryTrendChart(symbolData);
        this.renderVolumeProfileChart(symbolData);
        this.renderDeliveryEfficiencyGauge(symbolData);
        this.renderComparativeMetricsTable(symbolData);
    }

    selectSymbol(symbol) {
        console.log('selectSymbol called with:', symbol);
        this.selectedSymbol = symbol;
        document.getElementById('symbolSearch').value = symbol;
        
        // Switch to symbol analysis tab if not already there
        if (this.currentTab !== 'symbol-analysis') {
            this.switchTab('symbol-analysis');
        }
        
        // Always render symbol analysis when a symbol is selected
        this.renderSymbolAnalysis();
    }

    searchSymbol(searchTerm) {
        // Enhanced autocomplete functionality could be added here
        // For now, we'll handle the search when Enter is pressed or search button clicked
        if (searchTerm && searchTerm.length >= 2) {
            // Find matching symbols
            const matches = this.data.filter(item => 
                item.symbol && item.symbol.toLowerCase().includes(searchTerm.toLowerCase())
            );
            
            if (matches.length > 0) {
                // Auto-select first match if exact match found
                const exactMatch = matches.find(item => 
                    item.symbol.toLowerCase() === searchTerm.toLowerCase()
                );
                if (exactMatch) {
                    this.selectSymbol(exactMatch.symbol);
                }
            }
        }
    }

    renderSymbolAnalysis() {
        console.log('renderSymbolAnalysis called');
        console.log('Selected symbol:', this.selectedSymbol);
        console.log('Data length:', this.data.length);
        
        // Debug: Show first few symbols in data
        if (this.data && this.data.length > 0) {
            console.log('Available symbols (first 10):', this.data.slice(0, 10).map(item => item.symbol));
            console.log('Sample data structure:', this.data[0]);
        }
        
        if (!this.selectedSymbol) {
            console.log('No symbol selected, rendering default view');
            this.renderDefaultSymbolView();
            return;
        }

        const symbolData = this.data.find(item => item.symbol === this.selectedSymbol);
        console.log('Found symbol data:', symbolData);
        
        if (!symbolData) {
            console.log('Symbol not found in data:', this.selectedSymbol);
            this.renderSymbolNotFound();
            return;
        }

        console.log('Rendering symbol analysis for:', this.selectedSymbol);
        console.log('Symbol data:', symbolData);
        
        // Calculate and display KPIs
        this.renderSymbolKPIs(symbolData);
        
        // Render visualizations
        this.renderDeliveryTrendChart(symbolData);
        this.renderVolumeProfileChart(symbolData);
        this.renderDeliveryEfficiencyGauge(symbolData);
        this.renderComparativeMetricsTable(symbolData);
    }

    renderSymbolKPIs(symbolData) {
        const kpis = this.calculateSymbolAnalysisKPIs(symbolData);
        
        // Update all KPI cards with enhanced data
        document.getElementById('currentDeliveryPercentage').textContent = `${kpis.currentDeliveryPercentage.toFixed(2)}%`;
        document.getElementById('monthOverMonthChange').textContent = `${kpis.monthOverMonthChange >= 0 ? '+' : ''}${kpis.monthOverMonthChange.toFixed(2)}%`;
        document.getElementById('currentTradingVolume').textContent = this.formatNumber(kpis.currentTradingVolume);
        document.getElementById('deliveryTradingRatio').textContent = `${kpis.deliveryTradingRatio.toFixed(2)}%`;
        document.getElementById('deliveryQuantityChange').textContent = `${kpis.deliveryQuantityChange >= 0 ? '+' : ''}${this.formatNumber(kpis.deliveryQuantityChange)}`;

        // Update trend indicator with enhanced logic
        const trendElement = document.getElementById('deliveryTrend');
        if (kpis.monthOverMonthChange > 5) {
            trendElement.textContent = '↗ Strong Bullish';
            trendElement.className = 'kpi-trend positive';
        } else if (kpis.monthOverMonthChange > 0) {
            trendElement.textContent = '↗ Bullish';
            trendElement.className = 'kpi-trend positive';
        } else if (kpis.monthOverMonthChange < -5) {
            trendElement.textContent = '↘ Strong Bearish';
            trendElement.className = 'kpi-trend negative';
        } else if (kpis.monthOverMonthChange < 0) {
            trendElement.textContent = '↘ Bearish';
            trendElement.className = 'kpi-trend negative';
        } else {
            trendElement.textContent = '→ Neutral';
            trendElement.className = 'kpi-trend';
        }
    }

    calculateSymbolAnalysisKPIs(symbolData) {
        const currentDeliveryPercentage = parseFloat(symbolData.current_deliv_per) || 0;
        const monthOverMonthChange = parseFloat(symbolData.delivery_increase_pct) || 0;
        const deliveryQuantityChange = parseFloat(symbolData.delivery_increase_abs) || 0;
        const currentTradingVolume = parseFloat(symbolData.current_ttl_trd_qnty) || 0;
        const currentDelivQty = parseFloat(symbolData.current_deliv_qty) || 0;
        
        const deliveryTradingRatio = currentTradingVolume > 0 ? 
            (currentDelivQty / currentTradingVolume) * 100 : 0;

        return {
            currentDeliveryPercentage,
            monthOverMonthChange,
            deliveryQuantityChange,
            currentTradingVolume,
            deliveryTradingRatio
        };
    }

    renderDeliveryTrendChart(symbolData) {
        console.log('renderDeliveryTrendChart called for:', symbolData.symbol);
        const container = document.getElementById('deliveryTrendChart');
        if (!container) {
            console.log('Delivery trend chart container not found');
            return;
        }

        const currentDelivPer = parseFloat(symbolData.current_deliv_per) || 0;
        const previousDelivPer = parseFloat(symbolData.previous_deliv_per) || 0;
        const currentVolume = parseFloat(symbolData.current_ttl_trd_qnty) || 0;
        const previousVolume = parseFloat(symbolData.previous_ttl_trd_qnty) || 0;

        container.innerHTML = `
            <div class="trend-chart">
                <div class="chart-title">Delivery Percentage Trend</div>
                <div class="trend-comparison">
                    <div class="trend-period">
                        <div class="period-label">Previous Month</div>
                        <div class="period-value">${previousDelivPer.toFixed(2)}%</div>
                        <div class="period-volume">Volume: ${this.formatNumber(previousVolume)}</div>
                    </div>
                    <div class="trend-arrow">
                        <i class="fas fa-arrow-right"></i>
                    </div>
                    <div class="trend-period current">
                        <div class="period-label">Current Month</div>
                        <div class="period-value">${currentDelivPer.toFixed(2)}%</div>
                        <div class="period-volume">Volume: ${this.formatNumber(currentVolume)}</div>
                    </div>
                </div>
                <div class="trend-change ${currentDelivPer >= previousDelivPer ? 'positive' : 'negative'}">
                    Change: ${(currentDelivPer - previousDelivPer).toFixed(2)}%
                </div>
            </div>
        `;
    }

    renderVolumeProfileChart(symbolData) {
        console.log('renderVolumeProfileChart called for:', symbolData.symbol);
        const container = document.getElementById('volumeProfileChart');
        if (!container) {
            console.log('Volume profile chart container not found');
            return;
        }

        const currentTradingVol = parseFloat(symbolData.current_ttl_trd_qnty) || 0;
        const currentDelivVol = parseFloat(symbolData.current_deliv_qty) || 0;
        const previousTradingVol = parseFloat(symbolData.previous_ttl_trd_qnty) || 0;
        const previousDelivVol = parseFloat(symbolData.previous_deliv_qty) || 0;

        const maxValue = Math.max(currentTradingVol, currentDelivVol, previousTradingVol, previousDelivVol);

        container.innerHTML = `
            <div class="volume-profile">
                <div class="chart-title">Volume Comparison Analysis</div>
                <div class="volume-bars">
                    <div class="volume-group">
                        <div class="group-label">Current Month</div>
                        <div class="bar-item">
                            <div class="bar-label">Trading Volume</div>
                            <div class="bar-container">
                                <div class="bar trading" style="width: ${(currentTradingVol / maxValue) * 100}%"></div>
                                <span class="bar-value">${this.formatNumber(currentTradingVol)}</span>
                            </div>
                        </div>
                        <div class="bar-item">
                            <div class="bar-label">Delivery Volume</div>
                            <div class="bar-container">
                                <div class="bar delivery" style="width: ${(currentDelivVol / maxValue) * 100}%"></div>
                                <span class="bar-value">${this.formatNumber(currentDelivVol)}</span>
                            </div>
                        </div>
                    </div>
                    <div class="volume-group">
                        <div class="group-label">Previous Month</div>
                        <div class="bar-item">
                            <div class="bar-label">Trading Volume</div>
                            <div class="bar-container">
                                <div class="bar trading previous" style="width: ${(previousTradingVol / maxValue) * 100}%"></div>
                                <span class="bar-value">${this.formatNumber(previousTradingVol)}</span>
                            </div>
                        </div>
                        <div class="bar-item">
                            <div class="bar-label">Delivery Volume</div>
                            <div class="bar-container">
                                <div class="bar delivery previous" style="width: ${(previousDelivVol / maxValue) * 100}%"></div>
                                <span class="bar-value">${this.formatNumber(previousDelivVol)}</span>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        `;
    }

    renderDeliveryEfficiencyGauge(symbolData) {
        console.log('renderDeliveryEfficiencyGauge called for:', symbolData.symbol);
        const container = document.getElementById('deliveryEfficiencyGauge');
        if (!container) {
            console.log('Delivery efficiency gauge container not found');
            return;
        }

        const currentDelivQty = parseFloat(symbolData.current_deliv_qty) || 0;
        const currentTradingVol = parseFloat(symbolData.current_ttl_trd_qnty) || 0;
        
        const efficiency = currentTradingVol > 0 ? (currentDelivQty / currentTradingVol) * 100 : 0;
        
        const getEfficiencyRating = (percentage) => {
            if (percentage >= 80) return { rating: 'Excellent', color: '#4CAF50', icon: 'fas fa-star' };
            if (percentage >= 60) return { rating: 'Good', color: '#2196F3', icon: 'fas fa-thumbs-up' };
            if (percentage >= 40) return { rating: 'Average', color: '#FF9800', icon: 'fas fa-minus-circle' };
            if (percentage >= 20) return { rating: 'Below Average', color: '#FF5722', icon: 'fas fa-exclamation-triangle' };
            return { rating: 'Poor', color: '#F44336', icon: 'fas fa-times-circle' };
        };

        const rating = getEfficiencyRating(efficiency);

        container.innerHTML = `
            <div class="efficiency-gauge">
                <div class="gauge-header">
                    <div class="gauge-title">Delivery Efficiency</div>
                    <div class="gauge-subtitle">${symbolData.symbol}</div>
                </div>
                <div class="gauge-circle" style="--efficiency: ${efficiency}; --color: ${rating.color}">
                    <div class="gauge-value">${efficiency.toFixed(1)}%</div>
                </div>
                <div class="gauge-rating" style="color: ${rating.color}">
                    <i class="${rating.icon}"></i>
                    <span>${rating.rating}</span>
                </div>
                <div class="gauge-details">
                    <div class="detail-item">
                        <span class="detail-label">Delivery Qty:</span>
                        <span class="detail-value">${this.formatNumber(currentDelivQty)}</span>
                    </div>
                    <div class="detail-item">
                        <span class="detail-label">Trading Vol:</span>
                        <span class="detail-value">${this.formatNumber(currentTradingVol)}</span>
                    </div>
                </div>
            </div>
        `;
    }

    renderComparativeMetricsTable(symbolData) {
        console.log('renderComparativeMetricsTable called for:', symbolData.symbol);
        const container = document.getElementById('comparativeMetricsTable');
        if (!container) {
            console.log('Comparative metrics table container not found');
            return;
        }

        const metrics = [
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
                change: (parseFloat(symbolData.current_ttl_trd_qnty) || 0) - (parseFloat(symbolData.previous_ttl_trd_qnty) || 0),
                unit: 'qty'
            },
            {
                metric: 'Turnover (Lakhs)',
                current: parseFloat(symbolData.current_turnover_lacs) || 0,
                previous: parseFloat(symbolData.previous_turnover_lacs) || 0,
                change: (parseFloat(symbolData.current_turnover_lacs) || 0) - (parseFloat(symbolData.previous_turnover_lacs) || 0),
                unit: '₹L'
            }
        ];

        const calculatePercentageChange = (current, previous) => {
            return previous > 0 ? ((current - previous) / previous) * 100 : 0;
        };

        const tableRows = metrics.map(metric => {
            const percentChange = calculatePercentageChange(metric.current, metric.previous);
            const changeClass = percentChange > 0 ? 'positive' : percentChange < 0 ? 'negative' : 'neutral';
            
            return `
                <tr class="metric-row">
                    <td class="metric-name">${metric.metric}</td>
                    <td class="metric-current">${metric.unit === 'qty' ? this.formatNumber(metric.current) : metric.current.toFixed(2)}${metric.unit === '%' ? '%' : ''}</td>
                    <td class="metric-previous">${metric.unit === 'qty' ? this.formatNumber(metric.previous) : metric.previous.toFixed(2)}${metric.unit === '%' ? '%' : ''}</td>
                    <td class="metric-change ${changeClass}">
                        ${metric.change >= 0 ? '+' : ''}${metric.unit === 'qty' ? this.formatNumber(metric.change) : metric.change.toFixed(2)}${metric.unit === '%' ? '%' : ''}
                    </td>
                    <td class="metric-percent-change ${changeClass}">
                        ${percentChange >= 0 ? '+' : ''}${percentChange.toFixed(1)}%
                    </td>
                </tr>
            `;
        }).join('');

        container.innerHTML = `
            <div class="metrics-table">
                <div class="table-header">
                    <h4>Detailed Metrics Comparison - ${symbolData.symbol}</h4>
                </div>
                <table class="comparison-table">
                    <thead>
                        <tr>
                            <th>Metric</th>
                            <th>Current Month</th>
                            <th>Previous Month</th>
                            <th>Absolute Change</th>
                            <th>% Change</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${tableRows}
                    </tbody>
                </table>
            </div>
        `;
    }

    renderDefaultSymbolView() {
        // Show default values when no symbol is selected
        document.getElementById('currentDeliveryPercentage').textContent = '--';
        document.getElementById('monthOverMonthChange').textContent = '--';
        document.getElementById('currentTradingVolume').textContent = '--';
        document.getElementById('deliveryTradingRatio').textContent = '--';
        document.getElementById('deliveryQuantityChange').textContent = '--';
        document.getElementById('deliveryTrend').textContent = 'Select a symbol';
        document.getElementById('deliveryTrend').className = 'kpi-trend';
        
        // Clear visualization containers
        const containers = ['deliveryTrendChart', 'volumeProfileChart', 'deliveryEfficiencyGauge', 'comparativeMetricsTable'];
        containers.forEach(containerId => {
            const container = document.getElementById(containerId);
            if (container) {
                container.innerHTML = `
                    <div class="no-symbol-message">
                        <i class="fas fa-search"></i>
                        <h4>No Symbol Selected</h4>
                        <p>Please select a symbol to view detailed analysis</p>
                    </div>
                `;
            }
        });
    }

    renderSymbolNotFound() {
        document.getElementById('currentDeliveryPercentage').textContent = 'N/A';
        document.getElementById('monthOverMonthChange').textContent = 'N/A';
        document.getElementById('currentTradingVolume').textContent = 'N/A';
        document.getElementById('deliveryTradingRatio').textContent = 'N/A';
        document.getElementById('deliveryQuantityChange').textContent = 'N/A';
        document.getElementById('deliveryTrend').textContent = 'Not Found';
        document.getElementById('deliveryTrend').className = 'kpi-trend';
        
        // Show symbol not found message in containers
        const containers = ['deliveryTrendChart', 'volumeProfileChart', 'deliveryEfficiencyGauge', 'comparativeMetricsTable'];
        containers.forEach(containerId => {
            const container = document.getElementById(containerId);
            if (container) {
                container.innerHTML = `
                    <div class="symbol-not-found">
                        <i class="fas fa-exclamation-triangle"></i>
                        <h4>Symbol Not Found</h4>
                        <p>The symbol "${this.selectedSymbol}" was not found in the current dataset</p>
                    </div>
                `;
            }
        });
    }

    renderSelectedSymbolData() {
        // Legacy function - now calls the enhanced renderSymbolAnalysis
        this.renderSymbolAnalysis();
    }

    calculateSymbolKPIs(symbolData) {
        // Legacy function - now calls the enhanced calculateSymbolAnalysisKPIs
        return this.calculateSymbolAnalysisKPIs(symbolData);
    }

    // Tab 3: Category & Index Performance
    renderCategoryPerformance() {
        if (!this.filteredData || this.filteredData.length === 0) return;

        const kpis = this.calculateCategoryKPIs();
        
        // Update KPI cards
        document.getElementById('bestPerformingIndex').textContent = kpis.bestIndex.name;
        document.getElementById('indexPerformance').textContent = `${kpis.bestIndex.performance.toFixed(1)}%`;
        document.getElementById('bestPerformingCategory').textContent = kpis.bestCategory.name;
        document.getElementById('categoryPerformance').textContent = `${kpis.bestCategory.performance.toFixed(1)}%`;
        document.getElementById('totalWatchlistSymbols').textContent = kpis.totalSymbols.toLocaleString();
    }

    calculateCategoryKPIs() {
        // Group by index
        const indexPerformance = {};
        const categoryPerformance = {};

        this.filteredData.forEach(item => {
            const indexName = item.index_name || 'Other';
            const categoryName = item.category || 'Other';
            const performance = parseFloat(item.delivery_percentage_change) || 0;

            // Index performance
            if (!indexPerformance[indexName]) {
                indexPerformance[indexName] = { total: 0, count: 0 };
            }
            indexPerformance[indexName].total += performance;
            indexPerformance[indexName].count += 1;

            // Category performance
            if (!categoryPerformance[categoryName]) {
                categoryPerformance[categoryName] = { total: 0, count: 0 };
            }
            categoryPerformance[categoryName].total += performance;
            categoryPerformance[categoryName].count += 1;
        });

        // Find best performing index
        let bestIndex = { name: '--', performance: 0 };
        Object.entries(indexPerformance).forEach(([name, data]) => {
            const avgPerformance = data.total / data.count;
            if (avgPerformance > bestIndex.performance) {
                bestIndex = { name, performance: avgPerformance };
            }
        });

        // Find best performing category
        let bestCategory = { name: '--', performance: 0 };
        Object.entries(categoryPerformance).forEach(([name, data]) => {
            const avgPerformance = data.total / data.count;
            if (avgPerformance > bestCategory.performance) {
                bestCategory = { name, performance: avgPerformance };
            }
        });

        return {
            bestIndex,
            bestCategory,
            totalSymbols: this.filteredData.length
        };
    }

    // Utility Functions
    formatCurrency(value) {
        if (value >= 10000000) { // 1 crore
            return `₹${(value / 10000000).toFixed(2)}Cr`;
        } else if (value >= 100000) { // 1 lakh
            return `₹${(value / 100000).toFixed(2)}L`;
        } else if (value >= 1000) { // 1 thousand
            return `₹${(value / 1000).toFixed(2)}K`;
        } else {
            return `₹${value.toFixed(2)}`;
        }
    }

    formatNumber(value) {
        if (value >= 10000000) {
            return `${(value / 10000000).toFixed(2)}Cr`;
        } else if (value >= 100000) {
            return `${(value / 100000).toFixed(2)}L`;
        } else if (value >= 1000) {
            return `${(value / 1000).toFixed(2)}K`;
        } else {
            return value.toLocaleString();
        }
    }

    showLoading() {
        document.getElementById('loadingOverlay').classList.add('active');
    }

    hideLoading() {
        document.getElementById('loadingOverlay').classList.remove('active');
    }

    showError(message) {
        // You could implement a toast notification system here
        console.error(message);
        alert(message);
    }

    async refreshData() {
        this.showLoading();
        try {
            await this.loadData();
            this.renderDashboard();
        } catch (error) {
            this.showError('Failed to refresh data');
        } finally {
            this.hideLoading();
        }
    }
}

// Initialize the dashboard when the page loads
document.addEventListener('DOMContentLoaded', () => {
    window.dashboard = new ProfessionalNSEDashboard();
});

// Auto-refresh every 5 minutes
setInterval(() => {
    if (window.dashboard) {
        window.dashboard.refreshData();
    }
}, 300000);