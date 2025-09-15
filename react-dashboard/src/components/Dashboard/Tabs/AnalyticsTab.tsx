import React from 'react'
import Card from '../../UI/Card'
import { ComparisonData, SummaryStats } from '../../../services/api'

interface AnalyticsTabProps {
  data: ComparisonData[]
  summary?: SummaryStats
}

const AnalyticsTab: React.FC<AnalyticsTabProps> = ({ data, summary }) => {
  return (
    <div className="space-y-6">
      <Card>
        <div className="text-center py-12">
          <div className="flex items-center justify-center w-16 h-16 bg-danger-500 bg-opacity-10 rounded-full mx-auto mb-4">
            <span className="text-3xl">🔍</span>
          </div>
          <h3 className="text-xl font-semibold text-white mb-2">Advanced Analytics</h3>
          <p className="text-gray-400">
            Advanced analytics and insights coming soon...
          </p>
          <p className="text-sm text-gray-500 mt-2">
            Data Points: {data.length}
          </p>
        </div>
      </Card>
    </div>
  )
}

export default AnalyticsTab