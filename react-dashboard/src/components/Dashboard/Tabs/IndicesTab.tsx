import React from 'react'
import Card from '../../UI/Card'
import { ComparisonData, SummaryStats } from '../../../services/api'

interface IndicesTabProps {
  data: ComparisonData[]
  summary?: SummaryStats
}

const IndicesTab: React.FC<IndicesTabProps> = ({ data, summary }) => {
  return (
    <div className="space-y-6">
      <Card>
        <div className="text-center py-12">
          <div className="flex items-center justify-center w-16 h-16 bg-warning-500 bg-opacity-10 rounded-full mx-auto mb-4">
            <span className="text-3xl">📈</span>
          </div>
          <h3 className="text-xl font-semibold text-white mb-2">Indices Analysis</h3>
          <p className="text-gray-400">
            Index-wise performance analysis coming soon...
          </p>
          <p className="text-sm text-gray-500 mt-2">
            Total Indices: {new Set(data.map(item => item.index_name)).size}
          </p>
        </div>
      </Card>
    </div>
  )
}

export default IndicesTab