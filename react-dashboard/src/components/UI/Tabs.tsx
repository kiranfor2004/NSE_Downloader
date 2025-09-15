import React from 'react'
import { motion } from 'framer-motion'

interface TabsProps {
  tabs: Array<{
    id: string
    label: string
    icon?: React.ReactNode
    badge?: string | number
    disabled?: boolean
  }>
  activeTab: string
  onTabChange: (tabId: string) => void
  className?: string
}

const Tabs: React.FC<TabsProps> = ({
  tabs,
  activeTab,
  onTabChange,
  className = ''
}) => {
  return (
    <div className={`w-full ${className}`}>
      <div className="border-b border-gray-700">
        <nav className="flex space-x-8" aria-label="Tabs">
          {tabs.map((tab) => {
            const isActive = activeTab === tab.id
            const isDisabled = tab.disabled

            return (
              <motion.button
                key={tab.id}
                onClick={() => !isDisabled && onTabChange(tab.id)}
                className={`
                  relative flex items-center space-x-2 py-4 px-1
                  text-sm font-medium border-b-2 transition-all duration-200
                  ${isActive
                    ? 'border-primary-500 text-primary-400'
                    : isDisabled
                    ? 'border-transparent text-gray-600 cursor-not-allowed'
                    : 'border-transparent text-gray-400 hover:text-gray-300 hover:border-gray-600'
                  }
                  ${!isDisabled && 'focus:outline-none focus:text-primary-400'}
                `}
                disabled={isDisabled}
                whileHover={!isDisabled ? { y: -1 } : {}}
                whileTap={!isDisabled ? { y: 0 } : {}}
              >
                {tab.icon && (
                  <span className="flex-shrink-0">{tab.icon}</span>
                )}
                <span>{tab.label}</span>
                {tab.badge && (
                  <motion.span
                    initial={{ scale: 0 }}
                    animate={{ scale: 1 }}
                    className={`
                      inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium
                      ${isActive
                        ? 'bg-primary-500 text-white'
                        : 'bg-gray-700 text-gray-300'
                      }
                    `}
                  >
                    {tab.badge}
                  </motion.span>
                )}
                
                {/* Active indicator */}
                {isActive && (
                  <motion.div
                    layoutId="activeTab"
                    className="absolute bottom-0 left-0 right-0 h-0.5 bg-primary-500 rounded-full"
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
      </div>
    </div>
  )
}

export default Tabs