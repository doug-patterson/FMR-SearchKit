import {
  QuantityByPeriodCalendarChart,
  MongoAggregation
} from '../types'
import { dateGroup, dateProject2 } from '../util'

export const results = ({ x, y, offset = 0 }: QuantityByPeriodCalendarChart): MongoAggregation => [
  { $group: { _id: dateGroup(x, 'day', offset), value: { $sum: `$${y}` }, idx: { $min: `$${x}`} } },
  { $sort: { idx: 1 } },
  { $project: {
    _id: 0,
    day: dateProject2('day'),
    value: `$value`
  } },
]
