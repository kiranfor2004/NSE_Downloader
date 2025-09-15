import React from 'react'
import { QueryClient, QueryClientProvider } from 'react-query'
import { Toaster } from 'react-hot-toast'
import { BrowserRouter as Router } from 'react-router-dom'
import { motion } from 'framer-motion'

// Components
import Header from './components/Layout/Header'
import Dashboard from './components/Dashboard/Dashboard'
import LoadingSpinner from './components/UI/LoadingSpinner'

// Stores
import { useAppStore } from './stores/appStore'

// Styles
import './index.css'

// Create React Query client
const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 3,
      staleTime: 5 * 60 * 1000, // 5 minutes
      cacheTime: 10 * 60 * 1000, // 10 minutes
      refetchOnWindowFocus: false,
    },
  },
})

const App: React.FC = () => {
  const { isLoading } = useAppStore()

  return (
    <QueryClientProvider client={queryClient}>
      <Router>
        <div className="min-h-screen bg-dark-900 text-white">
          {/* Toast Notifications */}
          <Toaster
            position="top-right"
            toastOptions={{
              duration: 4000,
              style: {
                background: '#2C2C2C',
                color: '#FFFFFF',
                border: '1px solid #404040',
                borderRadius: '12px',
                fontSize: '14px',
                fontWeight: '500',
              },
              success: {
                iconTheme: {
                  primary: '#00C853',
                  secondary: '#FFFFFF',
                },
              },
              error: {
                iconTheme: {
                  primary: '#D50000',
                  secondary: '#FFFFFF',
                },
              },
            }}
          />

          {/* Loading Overlay */}
          {isLoading && (
            <motion.div
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              className="fixed inset-0 bg-dark-900 bg-opacity-95 flex items-center justify-center z-50 backdrop-blur-sm"
            >
              <LoadingSpinner size="large" />
            </motion.div>
          )}

          {/* Main App Layout */}
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.6, ease: 'easeOut' }}
            className="flex flex-col min-h-screen"
          >
            {/* Header */}
            <Header />

            {/* Main Content */}
            <main className="flex-1 relative">
              <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
                <Dashboard />
              </div>
            </main>

            {/* Footer */}
            <footer className="bg-dark-800 border-t border-gray-700 py-6">
              <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
                <div className="flex flex-col md:flex-row justify-between items-center">
                  <div className="text-gray-400 text-sm">
                    © 2025 NSE Delivery Analysis Dashboard. All rights reserved.
                  </div>
                  <div className="flex items-center space-x-6 mt-4 md:mt-0">
                    <span className="text-gray-400 text-sm">
                      Last Updated: {new Date().toLocaleString()}
                    </span>
                    <div className="flex items-center space-x-2">
                      <div className="w-2 h-2 bg-success-500 rounded-full animate-pulse"></div>
                      <span className="text-gray-400 text-sm">Live Data</span>
                    </div>
                  </div>
                </div>
              </div>
            </footer>
          </motion.div>
        </div>
      </Router>
    </QueryClientProvider>
  )
}

export default App