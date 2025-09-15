// NSE Delivery Analysis Dashboard JavaScript

class NSEDashboard {
    constructor() {
        this.data = null;
        this.filteredData = null;
        this.currentPage = 1;
        this.itemsPerPage = 20;
        this.charts = {};
        this.apiBaseUrl = 'http://localhost:5000/api';
        
        // Drill-down state management
        this.drillDownStack = [];
        this.currentDrillLevel = 'overview';
        this.selectedCategory = null;
        this.selectedIndex = null;
        this.selectedSymbol = null;
        this.selectedSector = null;
        this.selectedPerformanceRange = null;
        
        this.init();
    }

    async init() {
        this.showLoading();
        this.setupEventListeners();
        await this.loadData();
        this.renderDashboard();
        this.hideLoading();
    }

    setupEventListeners() {
        // Tab navigation
        document.querySelectorAll('.nav-link').forEach(link => {
            link.addEventListener('click', (e) => {
                e.preventDefault();
                this.switchTab(link.dataset.tab);
            });
        });

        // Refresh button
        document.getElementById('refreshData').addEventListener('click', () => {
            this.refreshData();
        });

        // Category filter
        document.getElementById('categoryFilter').addEventListener('change', (e) => {
            this.filterByCategory(e.target.value);
        });

        // Search functionality
        document.getElementById('searchSymbol').addEventListener('input', (e) => {
            this.searchSymbols(e.target.value);
        });

        // Sort functionality
        document.getElementById('sortBy').addEventListener('change', (e) => {
            this.sortData(e.target.value);
        });

        // Chart controls
        document.querySelectorAll('.chart-toggle').forEach(btn => {
            btn.addEventListener('click', (e) => {
                this.toggleChartType(e.target.dataset.chart);
            });
        });

        // Trend metric selector
        document.getElementById('trendMetric').addEventListener('change', (e) => {
            this.updateTrendChart(e.target.value);
        });

        // Comparison filters
        document.getElementById('dateRange').addEventListener('change', () => {
            this.updateComparisonChart();
        });

        document.getElementById('comparisonType').addEventListener('change', () => {
            this.updateComparisonChart();
        });

        // Drill-down navigation
        this.setupDrillDownListeners();
    }

    setupDrillDownListeners() {
        // Add event listeners for drill-down functionality
        document.addEventListener('click', (e) => {
            // Handle breadcrumb navigation
            if (e.target.classList.contains('breadcrumb-item')) {
                this.navigateToBreadcrumb(e.target.dataset.level);
            }
            
            // Handle back button
            if (e.target.id === 'drillBackBtn') {
                this.drillBack();
            }
        });
    }

    // Drill-down state management
    pushDrillState(level, data) {
        this.drillDownStack.push({
            level: this.currentDrillLevel,
            filteredData: [...this.filteredData],
            selectedCategory: this.selectedCategory,
            selectedIndex: this.selectedIndex,
            selectedSymbol: this.selectedSymbol,
            selectedSector: this.selectedSector,
            selectedPerformanceRange: this.selectedPerformanceRange
        });
        
        this.currentDrillLevel = level;
        this.updateBreadcrumb();
    }

    drillBack() {
        if (this.drillDownStack.length > 0) {
            const previousState = this.drillDownStack.pop();
            this.currentDrillLevel = previousState.level;
            this.filteredData = previousState.filteredData;
            this.selectedCategory = previousState.selectedCategory;
            this.selectedIndex = previousState.selectedIndex;
            this.selectedSymbol = previousState.selectedSymbol;
            this.selectedSector = previousState.selectedSector;
            this.selectedPerformanceRange = previousState.selectedPerformanceRange;
            
            this.updateBreadcrumb();
            this.renderCurrentView();
        }
    }

    navigateToBreadcrumb(level) {
        // Find the target level in the stack and restore to that state
        let targetStateIndex = -1;
        for (let i = this.drillDownStack.length - 1; i >= 0; i--) {
            if (this.drillDownStack[i].level === level) {
                targetStateIndex = i;
                break;
            }
        }
        
        if (targetStateIndex >= 0) {
            const targetState = this.drillDownStack[targetStateIndex];
            this.drillDownStack = this.drillDownStack.slice(0, targetStateIndex);
            
            this.currentDrillLevel = targetState.level;
            this.filteredData = targetState.filteredData;
            this.selectedCategory = targetState.selectedCategory;
            this.selectedIndex = targetState.selectedIndex;
            this.selectedSymbol = targetState.selectedSymbol;
            this.selectedSector = targetState.selectedSector;
            this.selectedPerformanceRange = targetState.selectedPerformanceRange;
            
            this.updateBreadcrumb();
            this.renderCurrentView();
        } else if (level === 'overview') {
            // Reset to overview
            this.resetDrillDown();
        }
    }

    resetDrillDown() {
        this.drillDownStack = [];
        this.currentDrillLevel = 'overview';
        this.filteredData = [...this.data];
        this.selectedCategory = null;
        this.selectedIndex = null;
        this.selectedSymbol = null;
        this.selectedSector = null;
        this.selectedPerformanceRange = null;
        
        this.updateBreadcrumb();
        this.renderCurrentView();
    }

    updateBreadcrumb() {
        const breadcrumbContainer = document.getElementById('breadcrumbNav') || this.createBreadcrumbContainer();
        const breadcrumbs = ['overview'];
        
        // Build breadcrumb path
        this.drillDownStack.forEach(state => {
            breadcrumbs.push(state.level);
        });
        breadcrumbs.push(this.currentDrillLevel);
        
        // Generate breadcrumb HTML
        const breadcrumbHTML = breadcrumbs.map((level, index) => {
            const isLast = index === breadcrumbs.length - 1;
            const levelName = this.formatBreadcrumbName(level);
            
            if (isLast) {
                return `<span class="breadcrumb-current">${levelName}</span>`;
            } else {
                return `<span class="breadcrumb-item" data-level="${level}">${levelName}</span>`;
            }
        }).join(' > ');
        
        breadcrumbContainer.innerHTML = `
            <div class="breadcrumb-container">
                ${breadcrumbHTML}
                ${this.drillDownStack.length > 0 ? '<button id="drillBackBtn" class="drill-back-btn">← Back</button>' : ''}
            </div>
        `;
    }

    createBreadcrumbContainer() {
        const container = document.createElement('div');
        container.id = 'breadcrumbNav';
        container.className = 'breadcrumb-navigation';
        
        // Insert after the header but before the main content
        const header = document.querySelector('.dashboard-header');
        header.insertAdjacentElement('afterend', container);
        
        return container;
    }

    formatBreadcrumbName(level) {
        const levelNames = {
            'overview': 'Overview',
            'category-detail': this.selectedCategory || 'Category Detail',
            'index-detail': this.selectedIndex || 'Index Detail',
            'symbol-detail': this.selectedSymbol || 'Symbol Detail',
            'sector-detail': this.selectedSector || 'Sector Detail',
            'performance-range': this.selectedPerformanceRange || 'Performance Range'
        };
        
        return levelNames[level] || level;
    }

    renderCurrentView() {
        this.currentPage = 1; // Reset pagination
        
        switch (this.currentDrillLevel) {
            case 'overview':
                this.renderOverview();
                break;
            case 'category-detail':
                this.renderCategoryDetail();
                break;
            case 'index-detail':
                this.renderIndexDetail();
                break;
            case 'symbol-detail':
                this.renderSymbolDetail();
                break;
            case 'sector-detail':
                this.renderSectorDetail();
                break;
            case 'performance-range':
                this.renderPerformanceRangeDetail();
                break;
            default:
                this.renderOverview();
        }
    }

    async loadData() {
        try {
            // Try to fetch from API first, if fails use mock data
            const response = await fetch(`${this.apiBaseUrl}/delivery-data`);
            if (response.ok) {
                this.data = await response.json();
            } else {
                // Use mock data if API is not available
                this.data = this.generateMockData();
            }
            this.filteredData = [...this.data];
        } catch (error) {
            console.warn('API not available, using mock data:', error);
            this.data = this.generateMockData();
            this.filteredData = [...this.data];
        }
    }

    generateMockData() {
        const symbols = ['RELIANCE', 'TCS', 'HDFCBANK', 'ICICIBANK', 'INFY', 'ITC', 'BAJFINANCE', 'KOTAKBANK', 
                        'HINDUNILVR', 'ASIANPAINT', 'MARUTI', 'SUNPHARMA', 'BRITANNIA', 'NESTLEIND', 'WIPRO'];
        const indices = ['NIFTY 50', 'NIFTY IT', 'NIFTY BANK', 'NIFTY AUTO', 'NIFTY PHARMA', 'NIFTY FMCG', 'Other Index'];
        const categories = ['Broad Market', 'Sectoral', 'Other'];
        const comparisons = ['AUG_VS_JUL_2025', 'JUL_VS_JUN_2025', 'JUN_VS_MAY_2025', 'MAY_VS_APR_2025'];

        const mockData = [];
        for (let i = 0; i < 100; i++) {
            const symbol = symbols[Math.floor(Math.random() * symbols.length)];
            const index = indices[Math.floor(Math.random() * indices.length)];
            const category = index === 'NIFTY 50' ? 'Broad Market' : 
                           index === 'Other Index' ? 'Other' : 'Sectoral';
            
            mockData.push({
                id: i + 1,
                symbol: symbol + (i > 50 ? Math.floor(Math.random() * 100) : ''),
                index_name: index,
                category: category,
                current_deliv_qty: Math.floor(Math.random() * 10000000) + 100000,
                delivery_increase_pct: Math.floor(Math.random() * 2000) + 10,
                comparison_type: comparisons[Math.floor(Math.random() * comparisons.length)],
                current_trade_date: this.getRandomDate(),
                current_close_price: Math.floor(Math.random() * 5000) + 100,
                previous_deliv_qty: Math.floor(Math.random() * 5000000) + 50000
            });
        }

        return mockData.sort((a, b) => b.delivery_increase_pct - a.delivery_increase_pct);
    }

    getRandomDate() {
        const start = new Date(2025, 0, 1);
        const end = new Date(2025, 8, 14);
        return new Date(start.getTime() + Math.random() * (end.getTime() - start.getTime()))
            .toISOString().split('T')[0];
    }

    async refreshData() {
        this.showLoading();
        await this.loadData();
        this.renderDashboard();
        this.hideLoading();
    }

    switchTab(tabName) {
        // Update nav links
        document.querySelectorAll('.nav-link').forEach(link => {
            link.classList.remove('active');
        });
        document.querySelector(`[data-tab="${tabName}"]`).classList.add('active');

        // Show/hide tab content
        document.querySelectorAll('.tab-content').forEach(tab => {
            tab.classList.remove('active');
        });
        document.getElementById(`${tabName}-tab`).classList.add('active');

        // Render tab-specific content
        switch(tabName) {
            case 'overview':
                this.renderOverview();
                break;
            case 'performance':
                this.renderPerformance();
                break;
            case 'indices':
                this.renderIndices();
                break;
            case 'sectors':
                this.renderSectors();
                break;
            case 'comparison':
                this.renderComparison();
                break;
        }
    }

    renderDashboard() {
        this.updateHeaderStats();
        this.renderOverview();
    }

    updateHeaderStats() {
        const totalRecords = this.data.length;
        const activeSymbols = new Set(this.data.map(d => d.symbol)).size;
        
        document.getElementById('totalRecords').textContent = totalRecords.toLocaleString();
        document.getElementById('activeSymbols').textContent = activeSymbols.toLocaleString();
        document.getElementById('lastUpdated').textContent = new Date().toLocaleTimeString();
    }

    renderOverview() {
        this.updateMetrics();
        this.renderCategoryChart();
        this.renderTrendChart();
        this.renderDataTable();
    }

    updateMetrics() {
        const avgIncrease = this.filteredData.reduce((sum, d) => sum + d.delivery_increase_pct, 0) / this.filteredData.length;
        const maxIncrease = Math.max(...this.filteredData.map(d => d.delivery_increase_pct));
        const topSymbol = this.filteredData.find(d => d.delivery_increase_pct === maxIncrease)?.symbol || '--';
        const nifty50Count = this.filteredData.filter(d => d.index_name === 'NIFTY 50').length;
        const sectoralCount = this.filteredData.filter(d => d.category === 'Sectoral').length;

        document.getElementById('avgIncrease').textContent = `${avgIncrease.toFixed(1)}%`;
        document.getElementById('maxIncrease').textContent = `${maxIncrease.toFixed(1)}%`;
        document.getElementById('topSymbol').textContent = topSymbol;
        document.getElementById('nifty50Count').textContent = nifty50Count.toLocaleString();
        document.getElementById('sectoralCount').textContent = sectoralCount.toLocaleString();
        document.getElementById('nifty50Percentage').textContent = `${((nifty50Count / this.filteredData.length) * 100).toFixed(1)}%`;
        document.getElementById('sectoralPercentage').textContent = `${((sectoralCount / this.filteredData.length) * 100).toFixed(1)}%`;
    }

    renderCategoryChart() {
        const ctx = document.getElementById('categoryChart').getContext('2d');
        
        if (this.charts.category) {
            this.charts.category.destroy();
        }

        const categoryData = this.getCategoryDistribution();
        
        this.charts.category = new Chart(ctx, {
            type: 'doughnut',
            data: {
                labels: categoryData.labels,
                datasets: [{
                    data: categoryData.values,
                    backgroundColor: [
                        '#1c3f7c',
                        '#ff6b35',
                        '#28a745',
                        '#ffc107'
                    ],
                    borderWidth: 2,
                    borderColor: '#fff'
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        position: 'bottom',
                        labels: {
                            padding: 20,
                            usePointStyle: true
                        }
                    }
                },
                onClick: (event, elements) => {
                    if (elements.length > 0) {
                        const elementIndex = elements[0].index;
                        const category = categoryData.labels[elementIndex];
                        this.drillDownToCategory(category);
                    }
                },
                onHover: (event, elements) => {
                    event.native.target.style.cursor = elements.length > 0 ? 'pointer' : 'default';
                }
            }
        });
    }

    renderTrendChart() {
        const ctx = document.getElementById('trendChart').getContext('2d');
        
        if (this.charts.trend) {
            this.charts.trend.destroy();
        }

        const trendData = this.getTrendData();
        
        this.charts.trend = new Chart(ctx, {
            type: 'line',
            data: {
                labels: trendData.labels,
                datasets: [{
                    label: 'Alert Count',
                    data: trendData.values,
                    borderColor: '#1c3f7c',
                    backgroundColor: 'rgba(28, 63, 124, 0.1)',
                    borderWidth: 3,
                    fill: true,
                    tension: 0.4
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    y: {
                        beginAtZero: true
                    }
                },
                plugins: {
                    legend: {
                        display: false
                    }
                }
            }
        });
    }

    renderDataTable() {
        const tbody = document.getElementById('tableBody');
        const startIndex = (this.currentPage - 1) * this.itemsPerPage;
        const endIndex = startIndex + this.itemsPerPage;
        const pageData = this.filteredData.slice(startIndex, endIndex);

        tbody.innerHTML = pageData.map(row => `
            <tr class="clickable-row" onclick="dashboard.drillDownToSymbol('${row.symbol}')">
                <td><strong>${row.symbol}</strong></td>
                <td><span class="index-badge clickable" onclick="event.stopPropagation(); dashboard.drillDownToIndex('${row.index_name}')">${row.index_name}</span></td>
                <td><span class="category-badge ${row.category.toLowerCase().replace(' ', '-')} clickable" onclick="event.stopPropagation(); dashboard.drillDownToCategory('${row.category}')">${row.category}</span></td>
                <td>${row.current_deliv_qty?.toLocaleString() || '--'}</td>
                <td><span class="increase-value">${row.delivery_increase_pct.toFixed(2)}%</span></td>
                <td>${row.comparison_type}</td>
                <td>${new Date(row.current_trade_date).toLocaleDateString()}</td>
            </tr>
        `).join('');

        this.renderPagination();
    }

    renderPagination() {
        const totalPages = Math.ceil(this.filteredData.length / this.itemsPerPage);
        const pagination = document.getElementById('pagination');
        
        let paginationHTML = '';
        
        // Previous button
        paginationHTML += `<button class="page-btn" ${this.currentPage === 1 ? 'disabled' : ''} onclick="dashboard.goToPage(${this.currentPage - 1})">Previous</button>`;
        
        // Page numbers
        for (let i = Math.max(1, this.currentPage - 2); i <= Math.min(totalPages, this.currentPage + 2); i++) {
            paginationHTML += `<button class="page-btn ${i === this.currentPage ? 'active' : ''}" onclick="dashboard.goToPage(${i})">${i}</button>`;
        }
        
        // Next button
        paginationHTML += `<button class="page-btn" ${this.currentPage === totalPages ? 'disabled' : ''} onclick="dashboard.goToPage(${this.currentPage + 1})">Next</button>`;
        
        pagination.innerHTML = paginationHTML;
    }

    goToPage(page) {
        this.currentPage = page;
        this.renderDataTable();
    }

    renderPerformance() {
        this.renderTopPerformers();
        this.renderPerformanceChart();
    }

    renderTopPerformers() {
        const topPerformers = [...this.filteredData]
            .sort((a, b) => b.delivery_increase_pct - a.delivery_increase_pct)
            .slice(0, 10);

        const container = document.getElementById('topPerformersList');
        container.innerHTML = topPerformers.map((performer, index) => `
            <div class="performer-item">
                <div>
                    <div class="performer-symbol">#${index + 1} ${performer.symbol}</div>
                    <div class="performer-index">${performer.index_name}</div>
                </div>
                <div class="performer-increase">${performer.delivery_increase_pct.toFixed(2)}%</div>
            </div>
        `).join('');
    }

    renderPerformanceChart() {
        const ctx = document.getElementById('performanceChart').getContext('2d');
        
        if (this.charts.performance) {
            this.charts.performance.destroy();
        }

        const performanceData = this.getPerformanceDistribution();
        
        this.charts.performance = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: performanceData.labels,
                datasets: [{
                    label: 'Number of Stocks',
                    data: performanceData.values,
                    backgroundColor: '#ff6b35',
                    borderColor: '#e55a2b',
                    borderWidth: 1
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    y: {
                        beginAtZero: true
                    }
                },
                onClick: (event, elements) => {
                    if (elements.length > 0) {
                        const elementIndex = elements[0].index;
                        const range = performanceData.labels[elementIndex];
                        this.drillDownToPerformanceRange(range);
                    }
                },
                onHover: (event, elements) => {
                    event.native.target.style.cursor = elements.length > 0 ? 'pointer' : 'default';
                }
            }
        });
    }

    renderIndices() {
        this.renderIndexCards();
        this.renderIndexChart();
    }

    renderIndexCards() {
        const indexData = this.getIndexData();
        const container = document.getElementById('indexCards');
        
        container.innerHTML = indexData.map(index => `
            <div class="index-card">
                <div class="index-name">${index.name}</div>
                <div class="index-count">${index.count}</div>
                <div class="index-category">${index.category}</div>
            </div>
        `).join('');
    }

    renderIndexChart() {
        const ctx = document.getElementById('indexChart').getContext('2d');
        
        if (this.charts.index) {
            this.charts.index.destroy();
        }

        const indexData = this.getIndexData();
        
        this.charts.index = new Chart(ctx, {
            type: 'horizontalBar',
            data: {
                labels: indexData.map(d => d.name),
                datasets: [{
                    label: 'Alert Count',
                    data: indexData.map(d => d.count),
                    backgroundColor: '#1c3f7c',
                    borderColor: '#2c5aa0',
                    borderWidth: 1
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    x: {
                        beginAtZero: true
                    }
                },
                onClick: (event, elements) => {
                    if (elements.length > 0) {
                        const elementIndex = elements[0].index;
                        const indexName = indexData[elementIndex].name;
                        this.drillDownToIndex(indexName);
                    }
                },
                onHover: (event, elements) => {
                    event.native.target.style.cursor = elements.length > 0 ? 'pointer' : 'default';
                }
            }
        });
    }

    renderSectors() {
        this.renderSectorHeatmap();
        this.renderSectorDetails();
    }

    renderSectorHeatmap() {
        const sectorData = this.getSectorData();
        const container = document.getElementById('sectorHeatmap');
        
        container.innerHTML = sectorData.map(sector => {
            let intensity = 'low';
            if (sector.avgIncrease > 100) intensity = 'high';
            else if (sector.avgIncrease > 50) intensity = 'medium';
            
            return `
                <div class="heatmap-item ${intensity} clickable" onclick="dashboard.drillDownToSector('${sector.name}')">
                    <div class="sector-name">${sector.name}</div>
                    <div class="sector-value">${sector.avgIncrease.toFixed(1)}%</div>
                </div>
            `;
        }).join('');
    }

    renderSectorDetails() {
        const sectorData = this.getSectorData();
        const container = document.getElementById('sectorDetailsList');
        
        container.innerHTML = sectorData.map(sector => `
            <div class="sector-detail-item">
                <h4>${sector.name}</h4>
                <p>Count: ${sector.count} | Avg: ${sector.avgIncrease.toFixed(2)}% | Max: ${sector.maxIncrease.toFixed(2)}%</p>
            </div>
        `).join('');
    }

    renderComparison() {
        this.updateComparisonChart();
    }

    updateComparisonChart() {
        const ctx = document.getElementById('comparisonChart').getContext('2d');
        
        if (this.charts.comparison) {
            this.charts.comparison.destroy();
        }

        const comparisonData = this.getComparisonData();
        
        this.charts.comparison = new Chart(ctx, {
            type: 'line',
            data: {
                labels: comparisonData.labels,
                datasets: comparisonData.datasets
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    y: {
                        beginAtZero: true
                    }
                }
            }
        });
    }

    // Data processing methods
    getCategoryDistribution() {
        const categories = {};
        this.filteredData.forEach(d => {
            categories[d.category] = (categories[d.category] || 0) + 1;
        });
        
        return {
            labels: Object.keys(categories),
            values: Object.values(categories)
        };
    }

    getTrendData() {
        const trends = {};
        this.filteredData.forEach(d => {
            const month = d.comparison_type;
            trends[month] = (trends[month] || 0) + 1;
        });
        
        return {
            labels: Object.keys(trends),
            values: Object.values(trends)
        };
    }

    getPerformanceDistribution() {
        const ranges = {
            '0-50%': 0,
            '50-100%': 0,
            '100-500%': 0,
            '500-1000%': 0,
            '1000%+': 0
        };
        
        this.filteredData.forEach(d => {
            const increase = d.delivery_increase_pct;
            if (increase <= 50) ranges['0-50%']++;
            else if (increase <= 100) ranges['50-100%']++;
            else if (increase <= 500) ranges['100-500%']++;
            else if (increase <= 1000) ranges['500-1000%']++;
            else ranges['1000%+']++;
        });
        
        return {
            labels: Object.keys(ranges),
            values: Object.values(ranges)
        };
    }

    getIndexData() {
        const indices = {};
        this.filteredData.forEach(d => {
            if (!indices[d.index_name]) {
                indices[d.index_name] = {
                    name: d.index_name,
                    count: 0,
                    category: d.category
                };
            }
            indices[d.index_name].count++;
        });
        
        return Object.values(indices).sort((a, b) => b.count - a.count);
    }

    getSectorData() {
        const sectors = {};
        this.filteredData.forEach(d => {
            if (d.category === 'Sectoral') {
                if (!sectors[d.index_name]) {
                    sectors[d.index_name] = {
                        name: d.index_name,
                        count: 0,
                        totalIncrease: 0,
                        maxIncrease: 0
                    };
                }
                sectors[d.index_name].count++;
                sectors[d.index_name].totalIncrease += d.delivery_increase_pct;
                sectors[d.index_name].maxIncrease = Math.max(sectors[d.index_name].maxIncrease, d.delivery_increase_pct);
            }
        });
        
        return Object.values(sectors).map(sector => ({
            ...sector,
            avgIncrease: sector.totalIncrease / sector.count
        }));
    }

    getComparisonData() {
        const comparisons = {};
        this.filteredData.forEach(d => {
            if (!comparisons[d.comparison_type]) {
                comparisons[d.comparison_type] = [];
            }
            comparisons[d.comparison_type].push(d.delivery_increase_pct);
        });
        
        const labels = Object.keys(comparisons);
        const datasets = [{
            label: 'Average Increase',
            data: labels.map(label => {
                const values = comparisons[label];
                return values.reduce((sum, val) => sum + val, 0) / values.length;
            }),
            borderColor: '#1c3f7c',
            backgroundColor: 'rgba(28, 63, 124, 0.1)',
            borderWidth: 2,
            fill: false
        }];
        
        return { labels, datasets };
    }

    // Filter and search methods
    filterByCategory(category) {
        if (category === 'all') {
            this.filteredData = [...this.data];
        } else {
            this.filteredData = this.data.filter(d => d.category === category);
        }
        this.currentPage = 1;
        this.renderDashboard();
    }

    searchSymbols(query) {
        if (!query) {
            this.filteredData = [...this.data];
        } else {
            this.filteredData = this.data.filter(d => 
                d.symbol.toLowerCase().includes(query.toLowerCase())
            );
        }
        this.currentPage = 1;
        this.renderDataTable();
    }

    sortData(field) {
        this.filteredData.sort((a, b) => {
            if (field === 'current_trade_date') {
                return new Date(b[field]) - new Date(a[field]);
            }
            return b[field] - a[field];
        });
        this.currentPage = 1;
        this.renderDataTable();
    }

    toggleChartType(type) {
        document.querySelectorAll('.chart-toggle').forEach(btn => {
            btn.classList.remove('active');
        });
        document.querySelector(`[data-chart="${type}"]`).classList.add('active');
        this.renderCategoryChart();
    }

    updateTrendChart(metric) {
        this.renderTrendChart();
    }

    showLoading() {
        document.getElementById('loadingOverlay').classList.add('active');
    }

    hideLoading() {
        document.getElementById('loadingOverlay').classList.remove('active');
    }

    // Drill-down functionality methods
    drillDownToCategory(category) {
        this.pushDrillState('category-detail', { category });
        this.selectedCategory = category;
        this.filteredData = this.data.filter(d => d.category === category);
        this.renderCurrentView();
    }

    drillDownToIndex(indexName) {
        this.pushDrillState('index-detail', { index: indexName });
        this.selectedIndex = indexName;
        this.filteredData = this.data.filter(d => d.index_name === indexName);
        this.renderCurrentView();
    }

    drillDownToSymbol(symbol) {
        this.pushDrillState('symbol-detail', { symbol });
        this.selectedSymbol = symbol;
        this.filteredData = this.data.filter(d => d.symbol === symbol);
        this.renderCurrentView();
    }

    drillDownToSector(sectorName) {
        this.pushDrillState('sector-detail', { sector: sectorName });
        this.selectedSector = sectorName;
        this.filteredData = this.data.filter(d => d.index_name === sectorName);
        this.renderCurrentView();
    }

    drillDownToPerformanceRange(range) {
        this.pushDrillState('performance-range', { range });
        this.selectedPerformanceRange = range;
        
        // Filter data based on performance range
        this.filteredData = this.data.filter(d => {
            const increase = d.delivery_increase_pct;
            switch (range) {
                case '0-50%': return increase <= 50;
                case '50-100%': return increase > 50 && increase <= 100;
                case '100-500%': return increase > 100 && increase <= 500;
                case '500-1000%': return increase > 500 && increase <= 1000;
                case '1000%+': return increase > 1000;
                default: return true;
            }
        });
        
        this.renderCurrentView();
    }

    // Detailed view renderers
    renderCategoryDetail() {
        const mainContent = document.querySelector('.tab-content.active .card-grid') || 
                           document.querySelector('.tab-content.active');
        
        if (!mainContent) return;
        
        mainContent.innerHTML = `
            <div class="drill-down-header">
                <h2>Category Analysis: ${this.selectedCategory}</h2>
                <p>Detailed breakdown of ${this.filteredData.length} symbols in ${this.selectedCategory} category</p>
            </div>
            
            <div class="category-detail-grid">
                <div class="card">
                    <h3>Index Distribution in ${this.selectedCategory}</h3>
                    <div class="chart-container">
                        <canvas id="categoryIndexChart"></canvas>
                    </div>
                </div>
                
                <div class="card">
                    <h3>Performance Metrics</h3>
                    <div class="metrics-grid">
                        <div class="metric-item">
                            <span class="metric-label">Average Increase</span>
                            <span class="metric-value">${this.getAverageIncrease().toFixed(2)}%</span>
                        </div>
                        <div class="metric-item">
                            <span class="metric-label">Top Performer</span>
                            <span class="metric-value">${this.getTopPerformer()}</span>
                        </div>
                        <div class="metric-item">
                            <span class="metric-label">Total Symbols</span>
                            <span class="metric-value">${this.filteredData.length}</span>
                        </div>
                        <div class="metric-item">
                            <span class="metric-label">Median Increase</span>
                            <span class="metric-value">${this.getMedianIncrease().toFixed(2)}%</span>
                        </div>
                    </div>
                </div>
            </div>
            
            <div class="card">
                <h3>Symbols in ${this.selectedCategory}</h3>
                <div class="table-container">
                    <table class="data-table">
                        <thead>
                            <tr>
                                <th>Symbol</th>
                                <th>Index</th>
                                <th>Delivery Qty</th>
                                <th>Increase %</th>
                                <th>Trade Date</th>
                                <th>Action</th>
                            </tr>
                        </thead>
                        <tbody id="categoryDetailTable">
                            <!-- Dynamic content -->
                        </tbody>
                    </table>
                </div>
                <div id="categoryDetailPagination" class="pagination-container"></div>
            </div>
        `;
        
        this.renderCategoryIndexChart();
        this.renderCategoryDetailTable();
    }

    renderIndexDetail() {
        const mainContent = document.querySelector('.tab-content.active .card-grid') || 
                           document.querySelector('.tab-content.active');
        
        if (!mainContent) return;
        
        mainContent.innerHTML = `
            <div class="drill-down-header">
                <h2>Index Analysis: ${this.selectedIndex}</h2>
                <p>Detailed analysis of ${this.filteredData.length} symbols in ${this.selectedIndex}</p>
            </div>
            
            <div class="index-detail-grid">
                <div class="card">
                    <h3>Performance Distribution</h3>
                    <div class="chart-container">
                        <canvas id="indexPerformanceChart"></canvas>
                    </div>
                </div>
                
                <div class="card">
                    <h3>Top & Bottom Performers</h3>
                    <div class="performers-grid">
                        <div class="top-performers">
                            <h4>Top 5 Performers</h4>
                            <div id="indexTopPerformers"></div>
                        </div>
                        <div class="bottom-performers">
                            <h4>Bottom 5 Performers</h4>
                            <div id="indexBottomPerformers"></div>
                        </div>
                    </div>
                </div>
            </div>
            
            <div class="card">
                <h3>All Symbols in ${this.selectedIndex}</h3>
                <div class="table-container">
                    <table class="data-table">
                        <thead>
                            <tr>
                                <th>Symbol</th>
                                <th>Delivery Qty</th>
                                <th>Increase %</th>
                                <th>Close Price</th>
                                <th>Trade Date</th>
                                <th>Action</th>
                            </tr>
                        </thead>
                        <tbody id="indexDetailTable">
                            <!-- Dynamic content -->
                        </tbody>
                    </table>
                </div>
                <div id="indexDetailPagination" class="pagination-container"></div>
            </div>
        `;
        
        this.renderIndexPerformanceChart();
        this.renderIndexTopBottomPerformers();
        this.renderIndexDetailTable();
    }

    renderSymbolDetail() {
        if (!this.selectedSymbol || this.filteredData.length === 0) return;
        
        const symbolData = this.filteredData[0]; // Should be single symbol
        const mainContent = document.querySelector('.tab-content.active .card-grid') || 
                           document.querySelector('.tab-content.active');
        
        if (!mainContent) return;
        
        mainContent.innerHTML = `
            <div class="drill-down-header">
                <h2>Symbol Analysis: ${this.selectedSymbol}</h2>
                <p>Detailed analysis for ${symbolData.symbol} in ${symbolData.index_name}</p>
            </div>
            
            <div class="symbol-detail-grid">
                <div class="card">
                    <h3>Symbol Overview</h3>
                    <div class="symbol-overview">
                        <div class="symbol-metric">
                            <span class="label">Current Delivery Qty</span>
                            <span class="value">${(symbolData.current_deliv_qty || 0).toLocaleString()}</span>
                        </div>
                        <div class="symbol-metric">
                            <span class="label">Delivery Increase</span>
                            <span class="value increase">${symbolData.delivery_increase_pct.toFixed(2)}%</span>
                        </div>
                        <div class="symbol-metric">
                            <span class="label">Current Price</span>
                            <span class="value">₹${(symbolData.current_close_price || 0).toFixed(2)}</span>
                        </div>
                        <div class="symbol-metric">
                            <span class="label">Previous Delivery Qty</span>
                            <span class="value">${(symbolData.previous_deliv_qty || 0).toLocaleString()}</span>
                        </div>
                        <div class="symbol-metric">
                            <span class="label">Index</span>
                            <span class="value">${symbolData.index_name}</span>
                        </div>
                        <div class="symbol-metric">
                            <span class="label">Category</span>
                            <span class="value">${symbolData.category}</span>
                        </div>
                    </div>
                </div>
                
                <div class="card">
                    <h3>Comparison Analysis</h3>
                    <div class="comparison-details">
                        <div class="comparison-item">
                            <span class="label">Comparison Type</span>
                            <span class="value">${symbolData.comparison_type}</span>
                        </div>
                        <div class="comparison-item">
                            <span class="label">Current Trade Date</span>
                            <span class="value">${new Date(symbolData.current_trade_date).toLocaleDateString()}</span>
                        </div>
                        <div class="comparison-item">
                            <span class="label">Previous Trade Date</span>
                            <span class="value">${symbolData.previous_trade_date ? new Date(symbolData.previous_trade_date).toLocaleDateString() : 'N/A'}</span>
                        </div>
                        <div class="comparison-item">
                            <span class="label">Previous Price</span>
                            <span class="value">₹${(symbolData.previous_close_price || 0).toFixed(2)}</span>
                        </div>
                    </div>
                </div>
            </div>
            
            <div class="card">
                <h3>Performance Context</h3>
                <div class="performance-context">
                    <p><strong>Index Performance:</strong> This symbol belongs to ${symbolData.index_name} index in the ${symbolData.category} category.</p>
                    <p><strong>Delivery Trend:</strong> The delivery quantity increased by ${symbolData.delivery_increase_pct.toFixed(2)}% compared to the previous period (${symbolData.comparison_type}).</p>
                    <p><strong>Market Context:</strong> ${this.getSymbolMarketContext(symbolData)}</p>
                </div>
            </div>
        `;
    }

    getSymbolMarketContext(symbolData) {
        const categoryAvg = this.data
            .filter(d => d.category === symbolData.category)
            .reduce((sum, d) => sum + d.delivery_increase_pct, 0) / 
            this.data.filter(d => d.category === symbolData.category).length;
        
        const indexAvg = this.data
            .filter(d => d.index_name === symbolData.index_name)
            .reduce((sum, d) => sum + d.delivery_increase_pct, 0) / 
            this.data.filter(d => d.index_name === symbolData.index_name).length;
        
        const performance = symbolData.delivery_increase_pct;
        let context = '';
        
        if (performance > categoryAvg * 1.5) {
            context = `This symbol is significantly outperforming the ${symbolData.category} category average of ${categoryAvg.toFixed(2)}%.`;
        } else if (performance > categoryAvg) {
            context = `This symbol is performing above the ${symbolData.category} category average of ${categoryAvg.toFixed(2)}%.`;
        } else {
            context = `This symbol is performing below the ${symbolData.category} category average of ${categoryAvg.toFixed(2)}%.`;
        }
        
        context += ` Within the ${symbolData.index_name} index, the average increase is ${indexAvg.toFixed(2)}%.`;
        
        return context;
    }

    // Helper methods for drill-down views
    getAverageIncrease() {
        return this.filteredData.reduce((sum, d) => sum + d.delivery_increase_pct, 0) / this.filteredData.length;
    }

    getMedianIncrease() {
        const sorted = [...this.filteredData].sort((a, b) => a.delivery_increase_pct - b.delivery_increase_pct);
        const mid = Math.floor(sorted.length / 2);
        return sorted.length % 2 !== 0 ? sorted[mid].delivery_increase_pct : 
               (sorted[mid - 1].delivery_increase_pct + sorted[mid].delivery_increase_pct) / 2;
    }

    getTopPerformer() {
        if (this.filteredData.length === 0) return 'N/A';
        const top = this.filteredData.reduce((max, d) => 
            d.delivery_increase_pct > max.delivery_increase_pct ? d : max
        );
        return `${top.symbol} (${top.delivery_increase_pct.toFixed(2)}%)`;
    }

    renderCategoryIndexChart() {
        const ctx = document.getElementById('categoryIndexChart');
        if (!ctx) return;
        
        const indexData = {};
        this.filteredData.forEach(d => {
            indexData[d.index_name] = (indexData[d.index_name] || 0) + 1;
        });
        
        new Chart(ctx.getContext('2d'), {
            type: 'bar',
            data: {
                labels: Object.keys(indexData),
                datasets: [{
                    label: 'Symbol Count',
                    data: Object.values(indexData),
                    backgroundColor: '#1c3f7c',
                    borderColor: '#2c5aa0',
                    borderWidth: 1
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                onClick: (event, elements) => {
                    if (elements.length > 0) {
                        const elementIndex = elements[0].index;
                        const indexName = Object.keys(indexData)[elementIndex];
                        this.drillDownToIndex(indexName);
                    }
                },
                onHover: (event, elements) => {
                    event.native.target.style.cursor = elements.length > 0 ? 'pointer' : 'default';
                }
            }
        });
    }

    renderCategoryDetailTable() {
        const tbody = document.getElementById('categoryDetailTable');
        if (!tbody) return;
        
        const startIndex = (this.currentPage - 1) * this.itemsPerPage;
        const endIndex = startIndex + this.itemsPerPage;
        const pageData = this.filteredData.slice(startIndex, endIndex);

        tbody.innerHTML = pageData.map(row => `
            <tr>
                <td><strong>${row.symbol}</strong></td>
                <td><span class="index-badge" onclick="dashboard.drillDownToIndex('${row.index_name}')" style="cursor: pointer;">${row.index_name}</span></td>
                <td>${(row.current_deliv_qty || 0).toLocaleString()}</td>
                <td><span class="increase-value">${row.delivery_increase_pct.toFixed(2)}%</span></td>
                <td>${new Date(row.current_trade_date).toLocaleDateString()}</td>
                <td><button class="drill-btn" onclick="dashboard.drillDownToSymbol('${row.symbol}')">Details</button></td>
            </tr>
        `).join('');

        this.renderCategoryDetailPagination();
    }

    renderCategoryDetailPagination() {
        const totalPages = Math.ceil(this.filteredData.length / this.itemsPerPage);
        const pagination = document.getElementById('categoryDetailPagination');
        
        if (!pagination) return;
        
        let paginationHTML = '';
        
        paginationHTML += `<button class="page-btn" ${this.currentPage === 1 ? 'disabled' : ''} onclick="dashboard.goToCategoryDetailPage(${this.currentPage - 1})">Previous</button>`;
        
        for (let i = Math.max(1, this.currentPage - 2); i <= Math.min(totalPages, this.currentPage + 2); i++) {
            paginationHTML += `<button class="page-btn ${i === this.currentPage ? 'active' : ''}" onclick="dashboard.goToCategoryDetailPage(${i})">${i}</button>`;
        }
        
        paginationHTML += `<button class="page-btn" ${this.currentPage === totalPages ? 'disabled' : ''} onclick="dashboard.goToCategoryDetailPage(${this.currentPage + 1})">Next</button>`;
        
        pagination.innerHTML = paginationHTML;
    }

    goToCategoryDetailPage(page) {
        this.currentPage = page;
        this.renderCategoryDetailTable();
    }

    renderIndexPerformanceChart() {
        const ctx = document.getElementById('indexPerformanceChart');
        if (!ctx) return;
        
        const ranges = {
            '0-50%': 0, '50-100%': 0, '100-500%': 0, '500-1000%': 0, '1000%+': 0
        };
        
        this.filteredData.forEach(d => {
            const increase = d.delivery_increase_pct;
            if (increase <= 50) ranges['0-50%']++;
            else if (increase <= 100) ranges['50-100%']++;
            else if (increase <= 500) ranges['100-500%']++;
            else if (increase <= 1000) ranges['500-1000%']++;
            else ranges['1000%+']++;
        });
        
        new Chart(ctx.getContext('2d'), {
            type: 'bar',
            data: {
                labels: Object.keys(ranges),
                datasets: [{
                    label: 'Number of Symbols',
                    data: Object.values(ranges),
                    backgroundColor: '#ff6b35',
                    borderColor: '#e55a2b',
                    borderWidth: 1
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                onClick: (event, elements) => {
                    if (elements.length > 0) {
                        const elementIndex = elements[0].index;
                        const range = Object.keys(ranges)[elementIndex];
                        this.drillDownToPerformanceRange(range);
                    }
                },
                onHover: (event, elements) => {
                    event.native.target.style.cursor = elements.length > 0 ? 'pointer' : 'default';
                }
            }
        });
    }

    renderIndexTopBottomPerformers() {
        const sorted = [...this.filteredData].sort((a, b) => b.delivery_increase_pct - a.delivery_increase_pct);
        const top5 = sorted.slice(0, 5);
        const bottom5 = sorted.slice(-5).reverse();
        
        const topContainer = document.getElementById('indexTopPerformers');
        const bottomContainer = document.getElementById('indexBottomPerformers');
        
        if (topContainer) {
            topContainer.innerHTML = top5.map((item, index) => `
                <div class="performer-item" onclick="dashboard.drillDownToSymbol('${item.symbol}')" style="cursor: pointer;">
                    <span class="rank">#${index + 1}</span>
                    <span class="symbol">${item.symbol}</span>
                    <span class="increase">${item.delivery_increase_pct.toFixed(2)}%</span>
                </div>
            `).join('');
        }
        
        if (bottomContainer) {
            bottomContainer.innerHTML = bottom5.map((item, index) => `
                <div class="performer-item" onclick="dashboard.drillDownToSymbol('${item.symbol}')" style="cursor: pointer;">
                    <span class="rank">#${sorted.length - index}</span>
                    <span class="symbol">${item.symbol}</span>
                    <span class="increase">${item.delivery_increase_pct.toFixed(2)}%</span>
                </div>
            `).join('');
        }
    }

    renderIndexDetailTable() {
        const tbody = document.getElementById('indexDetailTable');
        if (!tbody) return;
        
        const startIndex = (this.currentPage - 1) * this.itemsPerPage;
        const endIndex = startIndex + this.itemsPerPage;
        const pageData = this.filteredData.slice(startIndex, endIndex);

        tbody.innerHTML = pageData.map(row => `
            <tr onclick="dashboard.drillDownToSymbol('${row.symbol}')" style="cursor: pointer;">
                <td><strong>${row.symbol}</strong></td>
                <td>${(row.current_deliv_qty || 0).toLocaleString()}</td>
                <td><span class="increase-value">${row.delivery_increase_pct.toFixed(2)}%</span></td>
                <td>₹${(row.current_close_price || 0).toFixed(2)}</td>
                <td>${new Date(row.current_trade_date).toLocaleDateString()}</td>
                <td><button class="drill-btn" onclick="event.stopPropagation(); dashboard.drillDownToSymbol('${row.symbol}')">Details</button></td>
            </tr>
        `).join('');

        this.renderIndexDetailPagination();
    }

    renderIndexDetailPagination() {
        const totalPages = Math.ceil(this.filteredData.length / this.itemsPerPage);
        const pagination = document.getElementById('indexDetailPagination');
        
        if (!pagination) return;
        
        let paginationHTML = '';
        
        paginationHTML += `<button class="page-btn" ${this.currentPage === 1 ? 'disabled' : ''} onclick="dashboard.goToIndexDetailPage(${this.currentPage - 1})">Previous</button>`;
        
        for (let i = Math.max(1, this.currentPage - 2); i <= Math.min(totalPages, this.currentPage + 2); i++) {
            paginationHTML += `<button class="page-btn ${i === this.currentPage ? 'active' : ''}" onclick="dashboard.goToIndexDetailPage(${i})">${i}</button>`;
        }
        
        paginationHTML += `<button class="page-btn" ${this.currentPage === totalPages ? 'disabled' : ''} onclick="dashboard.goToIndexDetailPage(${this.currentPage + 1})">Next</button>`;
        
        pagination.innerHTML = paginationHTML;
    }

    goToIndexDetailPage(page) {
        this.currentPage = page;
        this.renderIndexDetailTable();
    }

    renderPerformanceRangeDetail() {
        const mainContent = document.querySelector('.tab-content.active .card-grid') || 
                           document.querySelector('.tab-content.active');
        
        if (!mainContent) return;
        
        const rangeStats = this.getPerformanceRangeStats();
        
        mainContent.innerHTML = `
            <div class="drill-down-header">
                <h2>Performance Range Analysis: ${this.selectedPerformanceRange}</h2>
                <p>Detailed analysis of ${this.filteredData.length} symbols in ${this.selectedPerformanceRange} performance range</p>
            </div>
            
            <div class="performance-range-grid">
                <div class="card">
                    <h3>Range Statistics</h3>
                    <div class="range-stats">
                        <div class="stat-item">
                            <span class="label">Total Symbols</span>
                            <span class="value">${this.filteredData.length}</span>
                        </div>
                        <div class="stat-item">
                            <span class="label">Average Increase</span>
                            <span class="value">${rangeStats.average.toFixed(2)}%</span>
                        </div>
                        <div class="stat-item">
                            <span class="label">Median Increase</span>
                            <span class="value">${rangeStats.median.toFixed(2)}%</span>
                        </div>
                        <div class="stat-item">
                            <span class="label">Range Span</span>
                            <span class="value">${rangeStats.min.toFixed(2)}% - ${rangeStats.max.toFixed(2)}%</span>
                        </div>
                    </div>
                </div>
                
                <div class="card">
                    <h3>Category Distribution</h3>
                    <div class="chart-container">
                        <canvas id="rangeCategoyChart"></canvas>
                    </div>
                </div>
            </div>
            
            <div class="card">
                <h3>Symbols in ${this.selectedPerformanceRange} Range</h3>
                <div class="table-container">
                    <table class="data-table">
                        <thead>
                            <tr>
                                <th>Symbol</th>
                                <th>Index</th>
                                <th>Category</th>
                                <th>Increase %</th>
                                <th>Delivery Qty</th>
                                <th>Action</th>
                            </tr>
                        </thead>
                        <tbody id="rangeDetailTable">
                            <!-- Dynamic content -->
                        </tbody>
                    </table>
                </div>
                <div id="rangeDetailPagination" class="pagination-container"></div>
            </div>
        `;
        
        this.renderRangeCategoryChart();
        this.renderRangeDetailTable();
    }

    getPerformanceRangeStats() {
        const increases = this.filteredData.map(d => d.delivery_increase_pct);
        const sorted = [...increases].sort((a, b) => a - b);
        const mid = Math.floor(sorted.length / 2);
        
        return {
            average: increases.reduce((sum, val) => sum + val, 0) / increases.length,
            median: sorted.length % 2 !== 0 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2,
            min: Math.min(...increases),
            max: Math.max(...increases)
        };
    }

    renderRangeCategoryChart() {
        const ctx = document.getElementById('rangeCategoyChart');
        if (!ctx) return;
        
        const categoryData = this.getCategoryDistribution();
        
        new Chart(ctx.getContext('2d'), {
            type: 'pie',
            data: {
                labels: categoryData.labels,
                datasets: [{
                    data: categoryData.values,
                    backgroundColor: ['#1c3f7c', '#ff6b35', '#28a745', '#ffc107'],
                    borderWidth: 2,
                    borderColor: '#fff'
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                onClick: (event, elements) => {
                    if (elements.length > 0) {
                        const elementIndex = elements[0].index;
                        const category = categoryData.labels[elementIndex];
                        this.drillDownToCategory(category);
                    }
                },
                onHover: (event, elements) => {
                    event.native.target.style.cursor = elements.length > 0 ? 'pointer' : 'default';
                }
            }
        });
    }

    renderRangeDetailTable() {
        const tbody = document.getElementById('rangeDetailTable');
        if (!tbody) return;
        
        const startIndex = (this.currentPage - 1) * this.itemsPerPage;
        const endIndex = startIndex + this.itemsPerPage;
        const pageData = this.filteredData.slice(startIndex, endIndex);

        tbody.innerHTML = pageData.map(row => `
            <tr onclick="dashboard.drillDownToSymbol('${row.symbol}')" style="cursor: pointer;">
                <td><strong>${row.symbol}</strong></td>
                <td><span class="index-badge clickable" onclick="event.stopPropagation(); dashboard.drillDownToIndex('${row.index_name}')">${row.index_name}</span></td>
                <td><span class="category-badge ${row.category.toLowerCase().replace(' ', '-')} clickable" onclick="event.stopPropagation(); dashboard.drillDownToCategory('${row.category}')">${row.category}</span></td>
                <td><span class="increase-value">${row.delivery_increase_pct.toFixed(2)}%</span></td>
                <td>${(row.current_deliv_qty || 0).toLocaleString()}</td>
                <td><button class="drill-btn" onclick="event.stopPropagation(); dashboard.drillDownToSymbol('${row.symbol}')">Details</button></td>
            </tr>
        `).join('');

        this.renderRangeDetailPagination();
    }

    renderRangeDetailPagination() {
        const totalPages = Math.ceil(this.filteredData.length / this.itemsPerPage);
        const pagination = document.getElementById('rangeDetailPagination');
        
        if (!pagination) return;
        
        let paginationHTML = '';
        
        paginationHTML += `<button class="page-btn" ${this.currentPage === 1 ? 'disabled' : ''} onclick="dashboard.goToRangeDetailPage(${this.currentPage - 1})">Previous</button>`;
        
        for (let i = Math.max(1, this.currentPage - 2); i <= Math.min(totalPages, this.currentPage + 2); i++) {
            paginationHTML += `<button class="page-btn ${i === this.currentPage ? 'active' : ''}" onclick="dashboard.goToRangeDetailPage(${i})">${i}</button>`;
        }
        
        paginationHTML += `<button class="page-btn" ${this.currentPage === totalPages ? 'disabled' : ''} onclick="dashboard.goToRangeDetailPage(${this.currentPage + 1})">Next</button>`;
        
        pagination.innerHTML = paginationHTML;
    }

    goToRangeDetailPage(page) {
        this.currentPage = page;
        this.renderRangeDetailTable();
    }

    renderSectorDetail() {
        const mainContent = document.querySelector('.tab-content.active .card-grid') || 
                           document.querySelector('.tab-content.active');
        
        if (!mainContent) return;
        
        mainContent.innerHTML = `
            <div class="drill-down-header">
                <h2>Sector Analysis: ${this.selectedSector}</h2>
                <p>Detailed analysis of ${this.filteredData.length} symbols in ${this.selectedSector} sector</p>
            </div>
            
            <div class="sector-detail-analysis">
                <div class="card">
                    <h3>Sector Performance Overview</h3>
                    <div class="sector-overview">
                        <div class="sector-metric">
                            <span class="label">Average Increase</span>
                            <span class="value">${this.getAverageIncrease().toFixed(2)}%</span>
                        </div>
                        <div class="sector-metric">
                            <span class="label">Top Performer</span>
                            <span class="value">${this.getTopPerformer()}</span>
                        </div>
                        <div class="sector-metric">
                            <span class="label">Total Symbols</span>
                            <span class="value">${this.filteredData.length}</span>
                        </div>
                        <div class="sector-metric">
                            <span class="label">Sector Category</span>
                            <span class="value">${this.filteredData[0]?.category || 'N/A'}</span>
                        </div>
                    </div>
                </div>
                
                <div class="card">
                    <h3>Performance Distribution</h3>
                    <div class="chart-container">
                        <canvas id="sectorPerformanceChart"></canvas>
                    </div>
                </div>
            </div>
            
            <div class="card">
                <h3>All Symbols in ${this.selectedSector}</h3>
                <div class="table-container">
                    <table class="data-table">
                        <thead>
                            <tr>
                                <th>Symbol</th>
                                <th>Delivery Qty</th>
                                <th>Increase %</th>
                                <th>Close Price</th>
                                <th>Trade Date</th>
                                <th>Action</th>
                            </tr>
                        </thead>
                        <tbody id="sectorDetailTable">
                            <!-- Dynamic content -->
                        </tbody>
                    </table>
                </div>
                <div id="sectorDetailPagination" class="pagination-container"></div>
            </div>
        `;
        
        this.renderSectorPerformanceChart();
        this.renderSectorDetailTable();
    }

    renderSectorPerformanceChart() {
        const ctx = document.getElementById('sectorPerformanceChart');
        if (!ctx) return;
        
        const sorted = [...this.filteredData].sort((a, b) => b.delivery_increase_pct - a.delivery_increase_pct);
        const top10 = sorted.slice(0, 10);
        
        new Chart(ctx.getContext('2d'), {
            type: 'bar',
            data: {
                labels: top10.map(d => d.symbol),
                datasets: [{
                    label: 'Delivery Increase %',
                    data: top10.map(d => d.delivery_increase_pct),
                    backgroundColor: '#1c3f7c',
                    borderColor: '#2c5aa0',
                    borderWidth: 1
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                onClick: (event, elements) => {
                    if (elements.length > 0) {
                        const elementIndex = elements[0].index;
                        const symbol = top10[elementIndex].symbol;
                        this.drillDownToSymbol(symbol);
                    }
                },
                onHover: (event, elements) => {
                    event.native.target.style.cursor = elements.length > 0 ? 'pointer' : 'default';
                }
            }
        });
    }

    renderSectorDetailTable() {
        const tbody = document.getElementById('sectorDetailTable');
        if (!tbody) return;
        
        const startIndex = (this.currentPage - 1) * this.itemsPerPage;
        const endIndex = startIndex + this.itemsPerPage;
        const pageData = this.filteredData.slice(startIndex, endIndex);

        tbody.innerHTML = pageData.map(row => `
            <tr onclick="dashboard.drillDownToSymbol('${row.symbol}')" style="cursor: pointer;">
                <td><strong>${row.symbol}</strong></td>
                <td>${(row.current_deliv_qty || 0).toLocaleString()}</td>
                <td><span class="increase-value">${row.delivery_increase_pct.toFixed(2)}%</span></td>
                <td>₹${(row.current_close_price || 0).toFixed(2)}</td>
                <td>${new Date(row.current_trade_date).toLocaleDateString()}</td>
                <td><button class="drill-btn" onclick="event.stopPropagation(); dashboard.drillDownToSymbol('${row.symbol}')">Details</button></td>
            </tr>
        `).join('');

        this.renderSectorDetailPagination();
    }

    renderSectorDetailPagination() {
        const totalPages = Math.ceil(this.filteredData.length / this.itemsPerPage);
        const pagination = document.getElementById('sectorDetailPagination');
        
        if (!pagination) return;
        
        let paginationHTML = '';
        
        paginationHTML += `<button class="page-btn" ${this.currentPage === 1 ? 'disabled' : ''} onclick="dashboard.goToSectorDetailPage(${this.currentPage - 1})">Previous</button>`;
        
        for (let i = Math.max(1, this.currentPage - 2); i <= Math.min(totalPages, this.currentPage + 2); i++) {
            paginationHTML += `<button class="page-btn ${i === this.currentPage ? 'active' : ''}" onclick="dashboard.goToSectorDetailPage(${i})">${i}</button>`;
        }
        
        paginationHTML += `<button class="page-btn" ${this.currentPage === totalPages ? 'disabled' : ''} onclick="dashboard.goToSectorDetailPage(${this.currentPage + 1})">Next</button>`;
        
        pagination.innerHTML = paginationHTML;
    }

    goToSectorDetailPage(page) {
        this.currentPage = page;
        this.renderSectorDetailTable();
    }
}

// Initialize dashboard when DOM is loaded
let dashboard;
document.addEventListener('DOMContentLoaded', () => {
    dashboard = new NSEDashboard();
});

// Add some CSS for dynamic elements
const style = document.createElement('style');
style.textContent = `
    .index-badge {
        background: #e3f2fd;
        color: #1976d2;
        padding: 0.25rem 0.5rem;
        border-radius: 4px;
        font-size: 0.8rem;
        font-weight: 500;
    }
    
    .category-badge {
        padding: 0.25rem 0.5rem;
        border-radius: 4px;
        font-size: 0.8rem;
        font-weight: 500;
    }
    
    .category-badge.broad-market {
        background: #e8f5e8;
        color: #2e7d32;
    }
    
    .category-badge.sectoral {
        background: #fff3e0;
        color: #f57c00;
    }
    
    .category-badge.other {
        background: #f3e5f5;
        color: #7b1fa2;
    }
    
    .increase-value {
        color: #c62828;
        font-weight: 600;
    }
    
    .sector-detail-item {
        background: #f8f9fa;
        padding: 1rem;
        margin-bottom: 0.5rem;
        border-radius: 8px;
        border-left: 4px solid #1c3f7c;
    }
    
    .sector-detail-item h4 {
        color: #1c3f7c;
        margin-bottom: 0.5rem;
    }
    
    .sector-detail-item p {
        margin: 0;
        color: #666;
        font-size: 0.9rem;
    }
`;
document.head.appendChild(style);