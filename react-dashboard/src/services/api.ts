import axios, { AxiosResponse } from 'axios'

// Base URL for API
const API_BASE_URL = 'http://127.0.0.1:5000/api'

// Create axios instance
const api = axios.create({
  baseURL: API_BASE_URL,
  timeout: 30000,
  headers: {
    'Content-Type': 'application/json',
  },
})

// Request interceptor
api.interceptors.request.use(
  (config) => {
    console.log(`Making API request: ${config.method?.toUpperCase()} ${config.url}`)
    return config
  },
  (error) => {
    console.error('Request error:', error)
    return Promise.reject(error)
  }
)

// Response interceptor
api.interceptors.response.use(
  (response: AxiosResponse) => {
    console.log(`API response: ${response.status} ${response.config.url}`)
    return response
  },
  (error) => {
    console.error('Response error:', error.response?.data || error.message)
    return Promise.reject(error)
  }
)

// Types
export interface ComparisonData {
  symbol: string
  index_name: string
  category: string
  date: string
  current_month_delivery_percentage: number
  current_month_delivery_qty: number
  current_month_delivery_value: number
  current_month_traded_qty: number
  current_month_traded_value: number
  previous_month_delivery_percentage: number
  previous_month_delivery_qty: number
  previous_month_delivery_value: number
  previous_month_traded_qty: number
  previous_month_traded_value: number
  delivery_percentage_change: number
  delivery_qty_change: number
  delivery_value_change: number
  traded_qty_change: number
  traded_value_change: number
}

export interface SummaryStats {
  total_symbols: number
  avg_delivery_percentage: number
  total_delivery_value: number
  total_traded_value: number
  top_gainers: number
  top_losers: number
}

export interface CategoryStats {
  category: string
  symbol_count: number
  avg_delivery_percentage: number
  total_delivery_value: number
  avg_change: number
}

export interface IndexStats {
  index_name: string
  symbol_count: number
  avg_delivery_percentage: number
  total_delivery_value: number
  avg_change: number
}

export interface ApiResponse<T> {
  success: boolean
  data: T
  message?: string
  total_count?: number
}

// API endpoints
export const dashboardApi = {
  // Get all comparison data
  getAllData: async (): Promise<ComparisonData[]> => {
    const response = await api.get('/delivery-data')
    return response.data.delivery_data || []
  },

  // Get summary statistics
  getSummary: async (): Promise<SummaryStats> => {
    const response = await api.get('/summary-stats')
    return response.data.summary_stats || {}
  },

  // Get data by symbol
  getBySymbol: async (symbol: string): Promise<ComparisonData[]> => {
    const response = await api.get(`/symbol/${symbol}`)
    const symbolData = response.data.symbol_data
    return symbolData ? [symbolData] : []
  },

  // Get categories
  getCategories: async (): Promise<string[]> => {
    const response = await api.get('/categories')
    return response.data.categories || []
  },

  // Get indices
  getIndices: async (): Promise<any[]> => {
    const response = await api.get('/indices')
    return response.data.indices || []
  },

  // Get performance analysis
  getPerformanceAnalysis: async (): Promise<any> => {
    const response = await api.get('/performance-analysis')
    return response.data.performance_analysis || {}
  },

  // Health check
  healthCheck: async (): Promise<{ status: string; timestamp: string }> => {
    const response = await api.get('/health')
    return response.data
  },
}

// Utility functions
export const formatCurrency = (value: number | string | null | undefined): string => {
  // Convert to number and handle invalid values
  const numValue = Number(value)
  if (isNaN(numValue) || numValue === null || numValue === undefined) {
    return '₹0.00'
  }
  
  if (numValue >= 1e9) {
    return `₹${(numValue / 1e9).toFixed(2)}B`
  } else if (numValue >= 1e7) {
    return `₹${(numValue / 1e7).toFixed(2)}Cr`
  } else if (numValue >= 1e5) {
    return `₹${(numValue / 1e5).toFixed(2)}L`
  } else if (numValue >= 1e3) {
    return `₹${(numValue / 1e3).toFixed(2)}K`
  }
  return `₹${numValue.toFixed(2)}`
}

export const formatPercentage = (value: number | string | null | undefined): string => {
  // Convert to number and handle invalid values
  const numValue = Number(value)
  if (isNaN(numValue) || numValue === null || numValue === undefined) {
    return '0.00%'
  }
  return `${numValue >= 0 ? '+' : ''}${numValue.toFixed(2)}%`
}

export const formatNumber = (value: number | string | null | undefined): string => {
  // Convert to number and handle invalid values
  const numValue = Number(value)
  if (isNaN(numValue) || numValue === null || numValue === undefined) {
    return '0'
  }
  
  if (numValue >= 1e9) {
    return `${(numValue / 1e9).toFixed(2)}B`
  } else if (numValue >= 1e7) {
    return `${(numValue / 1e7).toFixed(2)}Cr`
  } else if (numValue >= 1e5) {
    return `${(numValue / 1e5).toFixed(2)}L`
  } else if (numValue >= 1e3) {
    return `${(numValue / 1e3).toFixed(2)}K`
  }
  return numValue.toLocaleString()
}

export const getChangeColor = (value: number | string | null | undefined): string => {
  const numValue = Number(value)
  if (isNaN(numValue) || numValue === null || numValue === undefined) {
    return 'text-gray-400'
  }
  if (numValue > 0) return 'text-success-500'
  if (numValue < 0) return 'text-danger-500'
  return 'text-gray-400'
}

export const getChangeIcon = (value: number | string | null | undefined): string => {
  const numValue = Number(value)
  if (isNaN(numValue) || numValue === null || numValue === undefined) {
    return '→'
  }
  if (numValue > 0) return '↗'
  if (numValue < 0) return '↘'
  return '→'
}

// Error handling
export class ApiError extends Error {
  constructor(
    message: string,
    public status?: number,
    public data?: any
  ) {
    super(message)
    this.name = 'ApiError'
  }
}

export const handleApiError = (error: any): ApiError => {
  if (error.response) {
    // Server error
    return new ApiError(
      error.response.data?.message || 'Server error occurred',
      error.response.status,
      error.response.data
    )
  } else if (error.request) {
    // Network error
    return new ApiError('Network error - please check your connection')
  } else {
    // Other error
    return new ApiError(error.message || 'An unexpected error occurred')
  }
}

export default api