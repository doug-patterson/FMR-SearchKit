import {
  DayOfWeekSummaryBarsChart,
  MongoAggregation
} from '../types'

import { timezoneOffset } from '../util'

export const results = ({ x, y, group, idPath, agg, offset = 0 }: DayOfWeekSummaryBarsChart): MongoAggregation => [
  { $group: {
    _id: { day: { $dayOfWeek: { date: `$${x}`, timezone: timezoneOffset(offset) } }, [`${group || 'results'}`]: group ? `$${group}${idPath ? '.' : ''}${idPath || ''}` : 'results' },
    value: { $sum: agg === 'sum' ? `$${y}` : `$${y}` }
  } },
  { $group: {
    _id: `$_id.day`,
    segments: { $push: { k: `$_id.${group || 'results'}`, v: `$value` } },
  } },
  { $set: {
    segments: { $arrayToObject: '$segments'}
  }},
  { $set: {
    'segments.id': '$_id'
  }},
  { $replaceRoot: { newRoot: '$segments' } },
  { $sort: { id: 1 } },
  { $set: {
    id: { $arrayElemAt: [[null, 'Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'], `$id`] }
  } }
]

