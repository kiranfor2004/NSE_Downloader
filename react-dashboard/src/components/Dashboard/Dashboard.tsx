import React, { useEffect } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import { useQuery } from 'react-query'
import { toast } from 'react-hot-toast'

// Components
import LoadingSpinner from '../UI/LoadingSpinner'
import Card from '../UI/Card'
import OverviewTab from './Tabs/OverviewTab'
import SymbolsTab from './Tabs/SymbolsTab'
import CategoriesTab from './Tabs/CategoriesTab'
import IndicesTab from './Tabs/IndicesTab'
import AnalyticsTab from './Tabs/AnalyticsTab'

// Services
import { dashboardApi, handleApiError } from '../../services/api'

// Store
import { useAppStore, useView } from '../../stores/appStore'

const Dashboard: React.FC = () => {
  const { activeTab } = useView()
  const { setLoading } = useAppStore()

  // Health check query
  const { data: healthData, error: healthError } = useQuery(
    'health-check',
    dashboardApi.healthCheck,
    {
      refetchInterval: 60000, // Check every minute
      onError: (error) => {
        const apiError = handleApiError(error)
        toast.error(`API Connection Error: ${apiError.message}`)
      }
    }
  )

  // Main data query
  const {
    data: dashboardData,
    isLoading: isDashboardLoading,
    error: dashboardError,
    refetch: refetchDashboard
  } = useQuery(
    'dashboard-data',
    dashboardApi.getAllData,
    {
      staleTime: 5 * 60 * 1000, // 5 minutes
      onError: (error) => {
        const apiError = handleApiError(error)
        toast.error(`Failed to load dashboard data: ${apiError.message}`)
      }
    }
  )

  // Summary data query
  const {
    data: summaryData,
    isLoading: isSummaryLoading,
    error: summaryError
  } = useQuery(
    'summary-data',
    dashboardApi.getSummary,
    {
      staleTime: 5 * 60 * 1000,
      onError: (error) => {
        const apiError = handleApiError(error)
        console.warn(`Summary data not available: ${apiError.message}`)
      }
    }
  )

  // Update global loading state
  useEffect(() => {
    setLoading(isDashboardLoading || isSummaryLoading)
  }, [isDashboardLoading, isSummaryLoading, setLoading])

  // Show connection status
  useEffect(() => {
    if (healthData) {
      console.log('API Health:', healthData)
    }
    if (healthError) {
      console.error('API Health Error:', healthError)
    }
  }, [healthData, healthError])

  // Handle errors
  if (dashboardError || summaryError) {
    return (
      <motion.div
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        className="flex items-center justify-center min-h-96"
      >
        <Card className="max-w-md mx-auto text-center" background="darker">
          <div className="mb-4">
            <div className="w-16 h-16 bg-danger-500 bg-opacity-10 rounded-full flex items-center justify-center mx-auto mb-4">
              <span className="text-2xl">⚠️</span>
            </div>
            <h3 className="text-xl font-semibold text-white mb-2">Connection Error</h3>
            <p className="text-gray-400 mb-4">
              Unable to connect to the dashboard API. Please check your connection and try again.
            </p>
            <button
              onClick={() => {
                refetchDashboard()
                toast.loading('Reconnecting...', { duration: 2000 })
              }}
              className="px-4 py-2 bg-primary-600 text-white rounded-lg hover:bg-primary-700 transition-colors"
            >
              Retry Connection
            </button>
          </div>
        </Card>
      </motion.div>
    )
  }

  // Loading state
  if (isDashboardLoading || isSummaryLoading) {
    return (
      <motion.div
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        className="flex items-center justify-center min-h-96"
      >
        <LoadingSpinner size="large" text="Loading dashboard data..." />
      </motion.div>
    )
  }

  // Tab content mapping
  const renderTabContent = () => {
    const baseProps = {
      data: dashboardData || [],
      summary: summaryData
    }

    switch (activeTab) {
      case 'overview':
        return <OverviewTab {...baseProps} />
      case 'symbols':
        return <SymbolsTab {...baseProps} />
      case 'categories':
        return <CategoriesTab {...baseProps} />
      case 'indices':
        return <IndicesTab {...baseProps} />
      case 'analytics':
        return <AnalyticsTab {...baseProps} />
      default:
        return <OverviewTab {...baseProps} />
    }
  }

  return (
    <motion.div
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      transition={{ duration: 0.6 }}
      className="space-y-6"
    >
      {/* Header Stats */}
      {summaryData && (
        <motion.div
          initial={{ opacity: 0, y: -20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.2 }}
          className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6"
        >
          <Card hover className="text-center">
            <div className="space-y-2">
              <p className="text-sm text-gray-400">Total Symbols</p>
              <p className="text-2xl font-bold text-white">
                {summaryData.total_symbols.toLocaleString()}
              </p>
            </div>
          </Card>
          
          <Card hover className="text-center">
            <div className="space-y-2">
              <p className="text-sm text-gray-400">Avg Delivery %</p>
              <p className="text-2xl font-bold text-primary-400">
                {summaryData.avg_delivery_percentage.toFixed(2)}%
              </p>
            </div>
          </Card>
          
          <Card hover className="text-center">
            <div className="space-y-2">
              <p className="text-sm text-gray-400">Total Value</p>
              <p className="text-2xl font-bold text-success-400">
                ₹{(summaryData.total_delivery_value / 1e9).toFixed(2)}B
              </p>
            </div>
          </Card>
          
          <Card hover className="text-center">
            <div className="space-y-2">
              <p className="text-sm text-gray-400">Connection</p>
              <div className="flex items-center justify-center space-x-2">
                <div className={`w-2 h-2 rounded-full ${healthData ? 'bg-success-500 animate-pulse' : 'bg-danger-500'}`} />
                <p className={`text-sm font-medium ${healthData ? 'text-success-400' : 'text-danger-400'}`}>
                  {healthData ? 'Connected' : 'Disconnected'}
                </p>
              </div>
            </div>
          </Card>
        </motion.div>
      )}

      {/* Tab Content */}
      <AnimatePresence mode="wait">
        <motion.div
          key={activeTab}
          initial={{ opacity: 0, x: 20 }}
          animate={{ opacity: 1, x: 0 }}
          exit={{ opacity: 0, x: -20 }}
          transition={{ duration: 0.3 }}
        >
          {renderTabContent()}
        </motion.div>
      </AnimatePresence>
    </motion.div>
  )
}

export default Dashboard