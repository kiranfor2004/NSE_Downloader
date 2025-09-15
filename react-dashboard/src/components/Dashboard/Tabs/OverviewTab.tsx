import React, { useMemo } from 'react'
import { motion } from 'framer-motion'
import Card from '../../UI/Card'
import { ComparisonData, SummaryStats, formatCurrency, formatPercentage, formatNumber } from '../../../services/api'

interface OverviewTabProps {
  data: ComparisonData[]
  summary?: SummaryStats
}

const OverviewTab: React.FC<OverviewTabProps> = ({ data, summary }) => {
  // Calculate new KPIs based on requirements
  const kpis = useMemo(() => {
    if (!data.length) return null

    // 1. Total Market Delivery Increase (in Lakhs)
    const totalDeliveryIncrease = data.reduce((sum, item) => {
      const increase = Number(item.delivery_qty_change) || 0
      return sum + increase
    }, 0) / 100000 // Convert to lakhs

    // 2. Stocks with Positive Delivery Growth
    const positiveGrowthCount = data.filter(item => {
      const pct = Number(item.delivery_percentage_change) || 0
      return pct > 0
    }).length

    // 3. Market Delivery-to-Turnover Ratio
    const totalDeliveryQty = data.reduce((sum, item) => {
      const qty = Number(item.current_month_delivery_qty) || 0
      return sum + qty
    }, 0)
    
    const totalTurnover = data.reduce((sum, item) => {
      const turnover = Number(item.current_month_traded_value) || 0
      return sum + turnover
    }, 0)
    
    const deliveryToTurnoverRatio = totalTurnover > 0 ? (totalDeliveryQty / totalTurnover) * 100 : 0

    // 4. Average Daily Turnover
    const avgDailyTurnover = totalTurnover / data.length

    return {
      totalDeliveryIncrease,
      positiveGrowthCount,
      deliveryToTurnoverRatio,
      avgDailyTurnover,
      totalSymbols: data.length
    }
  }, [data])

  // Prepare data for visualizations
  const visualizationData = useMemo(() => {
    if (!data.length) return null

    // Group data by category for treemap
    const categoryData = data.reduce((acc, item) => {
      const category = item.category || 'Others'
      if (!acc[category]) {
        acc[category] = {
          name: category,
          value: 0,
          children: []
        }
      }
      acc[category].value += Number(item.current_month_traded_value) || 0
      acc[category].children.push({
        name: item.symbol,
        value: Number(item.current_month_traded_value) || 0,
        deliveryPct: Number(item.current_month_delivery_percentage) || 0
      })
      return acc
    }, {} as any)

    // Group data by index for force-directed graph
    const indexData = data.reduce((acc, item) => {
      const index = item.index_name || 'Others'
      if (!acc[index]) {
        acc[index] = []
      }
      acc[index].push({
        symbol: item.symbol,
        deliveryIncrease: Number(item.delivery_percentage_change) || 0,
        turnover: Number(item.current_month_traded_value) || 0
      })
      return acc
    }, {} as any)

    return {
      treemapData: Object.values(categoryData),
      forceGraphData: indexData,
      parallelData: data.map(item => ({
        symbol: item.symbol,
        turnover: Number(item.current_month_traded_value) || 0,
        closePrice: Number(item.current_month_delivery_percentage) || 0, // Using delivery percentage as proxy
        deliveryPct: Number(item.current_month_delivery_percentage) || 0,
        deliveryChange: Number(item.delivery_percentage_change) || 0
      }))
    }
  }, [data])

  if (!kpis || !visualizationData) {
    return (
      <div className="p-6">
        <div className="text-center text-gray-400">
          <p>Loading market overview...</p>
        </div>
      </div>
    )
  }

  return (
    <div className="p-6 space-y-6">
      {/* Page Header */}
      <div className="mb-8">
        <h1 className="text-3xl font-bold text-white mb-2">Market Overview</h1>
        <p className="text-gray-400">Comprehensive market analysis with advanced visualizations</p>
      </div>

      {/* Key Performance Indicators */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-8">
        {/* Total Market Delivery Increase */}
        <Card className="bg-gradient-to-br from-blue-900/20 to-blue-800/10 border-blue-500/20">
          <div className="p-6">
            <div className="flex items-center justify-between mb-4">
              <div className="w-12 h-12 bg-blue-500/20 rounded-xl flex items-center justify-center">
                <span className="text-2xl">📈</span>
              </div>
              <div className="text-right">
                <p className="text-xs text-gray-400 uppercase tracking-wide">Market Delivery</p>
                <p className="text-sm text-gray-500">Increase (Lakhs)</p>
              </div>
            </div>
            <div className="space-y-1">
              <p className="text-3xl font-bold text-blue-400">
                ₹{kpis.totalDeliveryIncrease.toFixed(2)}L
              </p>
              <p className="text-sm text-gray-400">
                Total delivery value increase
              </p>
            </div>
          </div>
        </Card>

        {/* Positive Growth Stocks */}
        <Card className="bg-gradient-to-br from-green-900/20 to-green-800/10 border-green-500/20">
          <div className="p-6">
            <div className="flex items-center justify-between mb-4">
              <div className="w-12 h-12 bg-green-500/20 rounded-xl flex items-center justify-center">
                <span className="text-2xl">📊</span>
              </div>
              <div className="text-right">
                <p className="text-xs text-gray-400 uppercase tracking-wide">Positive Growth</p>
                <p className="text-sm text-gray-500">Stocks</p>
              </div>
            </div>
            <div className="space-y-1">
              <p className="text-3xl font-bold text-green-400">
                {kpis.positiveGrowthCount}
              </p>
              <p className="text-sm text-gray-400">
                {((kpis.positiveGrowthCount / kpis.totalSymbols) * 100).toFixed(1)}% of total
              </p>
            </div>
          </div>
        </Card>

        {/* Delivery-to-Turnover Ratio */}
        <Card className="bg-gradient-to-br from-purple-900/20 to-purple-800/10 border-purple-500/20">
          <div className="p-6">
            <div className="flex items-center justify-between mb-4">
              <div className="w-12 h-12 bg-purple-500/20 rounded-xl flex items-center justify-center">
                <span className="text-2xl">⚖️</span>
              </div>
              <div className="text-right">
                <p className="text-xs text-gray-400 uppercase tracking-wide">Delivery/Turnover</p>
                <p className="text-sm text-gray-500">Ratio</p>
              </div>
            </div>
            <div className="space-y-1">
              <p className="text-3xl font-bold text-purple-400">
                {kpis.deliveryToTurnoverRatio.toFixed(2)}%
              </p>
              <p className="text-sm text-gray-400">
                Market commitment ratio
              </p>
            </div>
          </div>
        </Card>

        {/* Average Daily Turnover */}
        <Card className="bg-gradient-to-br from-orange-900/20 to-orange-800/10 border-orange-500/20">
          <div className="p-6">
            <div className="flex items-center justify-between mb-4">
              <div className="w-12 h-12 bg-orange-500/20 rounded-xl flex items-center justify-center">
                <span className="text-2xl">💰</span>
              </div>
              <div className="text-right">
                <p className="text-xs text-gray-400 uppercase tracking-wide">Average</p>
                <p className="text-sm text-gray-500">Daily Turnover</p>
              </div>
            </div>
            <div className="space-y-1">
              <p className="text-3xl font-bold text-orange-400">
                {formatCurrency(kpis.avgDailyTurnover)}
              </p>
              <p className="text-sm text-gray-400">
                Market liquidity indicator
              </p>
            </div>
          </div>
        </Card>
      </div>

      {/* Visualizations Grid */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Treemap Placeholder */}
        <Card className="col-span-1">
          <div className="p-6">
            <div className="flex items-center justify-between mb-6">
              <h3 className="text-xl font-semibold text-white">Category Turnover Distribution</h3>
              <div className="flex items-center space-x-2">
                <div className="w-3 h-3 bg-blue-200 rounded-full"></div>
                <span className="text-xs text-gray-400">Low</span>
                <div className="w-3 h-3 bg-blue-500 rounded-full"></div>
                <span className="text-xs text-gray-400">High</span>
              </div>
            </div>
            <div className="h-80 bg-gray-800/30 rounded-lg border-2 border-dashed border-gray-600/50 flex items-center justify-center">
              <div className="text-center">
                <div className="text-4xl mb-2">🗺️</div>
                <p className="text-gray-400 text-sm">Treemap Visualization</p>
                <p className="text-gray-500 text-xs">Hierarchical view of turnover by category</p>
              </div>
            </div>
          </div>
        </Card>

        {/* Force-Directed Graph Placeholder */}
        <Card className="col-span-1">
          <div className="p-6">
            <div className="flex items-center justify-between mb-6">
              <h3 className="text-xl font-semibold text-white">Index-Symbol Relationships</h3>
              <div className="flex items-center space-x-4">
                <div className="flex items-center space-x-1">
                  <div className="w-3 h-3 bg-yellow-500 rounded-full"></div>
                  <span className="text-xs text-gray-400">NIFTY 50</span>
                </div>
                <div className="flex items-center space-x-1">
                  <div className="w-3 h-3 bg-blue-500 rounded-full"></div>
                  <span className="text-xs text-gray-400">BANK NIFTY</span>
                </div>
              </div>
            </div>
            <div className="h-80 bg-gray-800/30 rounded-lg border-2 border-dashed border-gray-600/50 flex items-center justify-center">
              <div className="text-center">
                <div className="text-4xl mb-2">🌐</div>
                <p className="text-gray-400 text-sm">Force-Directed Graph</p>
                <p className="text-gray-500 text-xs">Delivery increase relationships</p>
              </div>
            </div>
          </div>
        </Card>

        {/* Sunburst Chart Placeholder */}
        <Card className="col-span-1">
          <div className="p-6">
            <div className="flex items-center justify-between mb-6">
              <h3 className="text-xl font-semibold text-white">Hierarchical Distribution</h3>
              <div className="text-xs text-gray-400">Click segments to drill down</div>
            </div>
            <div className="h-80 bg-gray-800/30 rounded-lg border-2 border-dashed border-gray-600/50 flex items-center justify-center">
              <div className="text-center">
                <div className="text-4xl mb-2">☀️</div>
                <p className="text-gray-400 text-sm">Sunburst Chart</p>
                <p className="text-gray-500 text-xs">Index to symbol breakdown</p>
              </div>
            </div>
          </div>
        </Card>

        {/* Parallel Coordinates Placeholder */}
        <Card className="col-span-1">
          <div className="p-6">
            <div className="flex items-center justify-between mb-6">
              <h3 className="text-xl font-semibold text-white">Multi-Metric Comparison</h3>
              <div className="flex items-center space-x-2">
                <div className="w-3 h-3 bg-gray-400 rounded-full"></div>
                <span className="text-xs text-gray-400">Default</span>
                <div className="w-3 h-3 bg-cyan-400 rounded-full"></div>
                <span className="text-xs text-gray-400">Selected</span>
              </div>
            </div>
            <div className="h-80 bg-gray-800/30 rounded-lg border-2 border-dashed border-gray-600/50 flex items-center justify-center">
              <div className="text-center">
                <div className="text-4xl mb-2">📊</div>
                <p className="text-gray-400 text-sm">Parallel Coordinates</p>
                <p className="text-gray-500 text-xs">Comparative analysis tool</p>
              </div>
            </div>
          </div>
        </Card>
      </div>

      {/* Summary Section */}
      <Card>
        <div className="p-6">
          <h3 className="text-xl font-semibold text-white mb-4">Market Summary</h3>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
            <div className="text-center">
              <p className="text-2xl font-bold text-blue-400">{kpis.totalSymbols}</p>
              <p className="text-sm text-gray-400">Total Symbols Analyzed</p>
            </div>
            <div className="text-center">
              <p className="text-2xl font-bold text-green-400">
                {((kpis.positiveGrowthCount / kpis.totalSymbols) * 100).toFixed(1)}%
              </p>
              <p className="text-sm text-gray-400">Positive Growth Rate</p>
            </div>
            <div className="text-center">
              <p className="text-2xl font-bold text-purple-400">
                {formatCurrency(kpis.avgDailyTurnover)}
              </p>
              <p className="text-sm text-gray-400">Avg Daily Turnover</p>
            </div>
          </div>
        </div>
      </Card>
    </div>
  )
}

export default OverviewTab