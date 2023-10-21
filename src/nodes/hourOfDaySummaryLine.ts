import { HourOfDaySummaryLineChart, MongoAggregation } from '../types'
import { timezoneOffset } from '../util'

export const results = ({
  x,
  y,
  group,
  idPath,
  agg,
  offset = 0
}: HourOfDaySummaryLineChart): MongoAggregation => [
  {
    $group: {
      _id: {
        hour: { $hour: { date: `$${x}`, timezone: timezoneOffset(offset) } },
        [`${group || 'results'}`]: group
          ? `$${group}${idPath ? '.' : ''}${idPath || ''}`
          : 'results'
      },
      value: { $sum: agg === 'sum' ? `$${y}` : `$${y}` }
    }
  },
  {
    $group: {
      _id: `$_id.${group || 'results'}`,
      data: {
        $push: {
          x: '$_id.hour',
          y: `$value`
        }
      }
    }
  },
  {
    $project: {
      _id: 0,
      id: '$_id',
      data: 1
    }
  }
]
