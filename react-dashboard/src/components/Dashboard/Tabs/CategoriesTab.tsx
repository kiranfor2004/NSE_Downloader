import React from 'react'
import Card from '../../UI/Card'
import { ComparisonData, SummaryStats } from '../../../services/api'

interface CategoriesTabProps {
  data: ComparisonData[]
  summary?: SummaryStats
}

const CategoriesTab: React.FC<CategoriesTabProps> = ({ data, summary }) => {
  return (
    <div className="space-y-6">
      <Card>
        <div className="text-center py-12">
          <div className="flex items-center justify-center w-16 h-16 bg-success-500 bg-opacity-10 rounded-full mx-auto mb-4">
            <span className="text-3xl">📋</span>
          </div>
          <h3 className="text-xl font-semibold text-white mb-2">Categories Analysis</h3>
          <p className="text-gray-400">
            Category-wise performance analysis coming soon...
          </p>
          <p className="text-sm text-gray-500 mt-2">
            Total Categories: {new Set(data.map(item => item.category)).size}
          </p>
        </div>
      </Card>
    </div>
  )
}

export default CategoriesTab