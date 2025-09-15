import React, { useState } from 'react'
import { motion } from 'framer-motion'
import Button from '../UI/Button'
import { useAppStore } from '../../stores/appStore'

const Header: React.FC = () => {
  const [isMenuOpen, setIsMenuOpen] = useState(false)
  const { view, toggleDarkMode } = useAppStore()

  const menuItems = [
    { label: 'Overview', value: 'overview', icon: '📊' },
    { label: 'Symbols', value: 'symbols', icon: '🏢' },
    { label: 'Categories', value: 'categories', icon: '📋' },
    { label: 'Indices', value: 'indices', icon: '📈' },
    { label: 'Analytics', value: 'analytics', icon: '🔍' },
  ]

  return (
    <header className="bg-dark-800 border-b border-gray-700 sticky top-0 z-40 backdrop-blur-lg">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex justify-between items-center h-16">
          {/* Logo and Title */}
          <motion.div
            initial={{ opacity: 0, x: -20 }}
            animate={{ opacity: 1, x: 0 }}
            className="flex items-center space-x-3"
          >
            <div className="flex items-center justify-center w-10 h-10 bg-gradient-to-r from-primary-500 to-primary-600 rounded-lg">
              <span className="text-white font-bold text-lg">N</span>
            </div>
            <div className="flex flex-col">
              <h1 className="text-xl font-bold text-white">NSE Delivery Analysis</h1>
              <p className="text-xs text-gray-400 hidden sm:block">
                Month vs Previous Month Comparison Dashboard
              </p>
            </div>
          </motion.div>

          {/* Desktop Navigation */}
          <nav className="hidden md:flex items-center space-x-1">
            {menuItems.map((item) => {
              const isActive = view.activeTab === item.value
              return (
                <motion.button
                  key={item.value}
                  onClick={() => useAppStore.getState().setActiveTab(item.value as any)}
                  className={`
                    flex items-center space-x-2 px-4 py-2 rounded-lg text-sm font-medium
                    transition-all duration-200 relative
                    ${isActive
                      ? 'text-primary-400 bg-primary-500 bg-opacity-10'
                      : 'text-gray-400 hover:text-white hover:bg-dark-700'
                    }
                  `}
                  whileHover={{ scale: 1.05 }}
                  whileTap={{ scale: 0.95 }}
                >
                  <span>{item.icon}</span>
                  <span>{item.label}</span>
                  {isActive && (
                    <motion.div
                      layoutId="activeNavItem"
                      className="absolute inset-0 bg-primary-500 bg-opacity-10 rounded-lg"
                      initial={false}
                      transition={{
                        type: 'spring',
                        stiffness: 500,
                        damping: 30
                      }}
                    />
                  )}
                </motion.button>
              )
            })}
          </nav>

          {/* Right side actions */}
          <div className="flex items-center space-x-3">
            {/* Status Indicator */}
            <motion.div
              initial={{ opacity: 0, scale: 0 }}
              animate={{ opacity: 1, scale: 1 }}
              className="hidden sm:flex items-center space-x-2 px-3 py-1.5 bg-success-500 bg-opacity-10 rounded-full"
            >
              <div className="w-2 h-2 bg-success-500 rounded-full animate-pulse" />
              <span className="text-success-400 text-xs font-medium">Live Data</span>
            </motion.div>

            {/* Dark Mode Toggle */}
            <Button
              variant="ghost"
              size="small"
              onClick={toggleDarkMode}
              icon={view.darkMode ? '🌙' : '☀️'}
              aria-label="Toggle dark mode"
            >
              <span className="sr-only">Toggle dark mode</span>
            </Button>

            {/* Mobile Menu Button */}
            <Button
              variant="ghost"
              size="small"
              onClick={() => setIsMenuOpen(!isMenuOpen)}
              className="md:hidden"
              icon={isMenuOpen ? '✕' : '☰'}
              aria-label="Toggle menu"
            >
              <span className="sr-only">Toggle menu</span>
            </Button>
          </div>
        </div>

        {/* Mobile Navigation */}
        {isMenuOpen && (
          <motion.div
            initial={{ opacity: 0, height: 0 }}
            animate={{ opacity: 1, height: 'auto' }}
            exit={{ opacity: 0, height: 0 }}
            className="md:hidden border-t border-gray-700 py-4"
          >
            <nav className="flex flex-col space-y-2">
              {menuItems.map((item) => {
                const isActive = view.activeTab === item.value
                return (
                  <motion.button
                    key={item.value}
                    onClick={() => {
                      useAppStore.getState().setActiveTab(item.value as any)
                      setIsMenuOpen(false)
                    }}
                    className={`
                      flex items-center space-x-3 px-4 py-3 rounded-lg text-sm font-medium
                      transition-all duration-200 text-left
                      ${isActive
                        ? 'text-primary-400 bg-primary-500 bg-opacity-10'
                        : 'text-gray-400 hover:text-white hover:bg-dark-700'
                      }
                    `}
                    whileHover={{ x: 4 }}
                    whileTap={{ scale: 0.98 }}
                  >
                    <span className="text-lg">{item.icon}</span>
                    <span>{item.label}</span>
                  </motion.button>
                )
              })}
            </nav>
          </motion.div>
        )}
      </div>
    </header>
  )
}

export default Header