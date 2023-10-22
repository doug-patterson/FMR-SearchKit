import { DateLineSingleChart, MongoAggregation } from '../types'

import { dateGroup, dateProject } from '../util'

export const results = ({
  x,
  y,
  period,
  agg = 'sum',
  offset = 0
}: DateLineSingleChart): MongoAggregation => [
  {
    $group: {
      _id: dateGroup(x, period, offset),
      value: { $sum: agg === 'sum' ? `$${y}` : 1 },
      idx: { $min: `$${x}` }
    }
  },
  { $sort: { idx: 1 } },
  {
    $project: {
      _id: 0,
      x: dateProject(period),
      y: `$value`
    }
  },
  { $group: { _id: null, data: { $push: '$$ROOT' } } },
  { $project: { _id: 0, id: 'results', data: 1 } }
]
