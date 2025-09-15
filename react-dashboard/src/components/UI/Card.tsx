import React from 'react'
import { motion } from 'framer-motion'

interface CardProps {
  children: React.ReactNode
  className?: string
  padding?: 'none' | 'small' | 'medium' | 'large'
  hover?: boolean
  border?: boolean
  background?: 'default' | 'darker' | 'transparent'
}

const Card: React.FC<CardProps> = ({
  children,
  className = '',
  padding = 'medium',
  hover = false,
  border = true,
  background = 'default'
}) => {
  const paddingClasses = {
    none: '',
    small: 'p-4',
    medium: 'p-6',
    large: 'p-8'
  }

  const backgroundClasses = {
    default: 'bg-dark-800',
    darker: 'bg-dark-900',
    transparent: 'bg-transparent'
  }

  const borderClass = border ? 'border border-gray-700' : ''

  return (
    <motion.div
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.3 }}
      whileHover={hover ? { y: -2, boxShadow: '0 10px 25px rgba(0, 0, 0, 0.3)' } : {}}
      className={`
        ${backgroundClasses[background]}
        ${borderClass}
        ${paddingClasses[padding]}
        rounded-xl shadow-lg backdrop-blur-sm
        transition-all duration-200
        ${className}
      `}
    >
      {children}
    </motion.div>
  )
}

export default Card