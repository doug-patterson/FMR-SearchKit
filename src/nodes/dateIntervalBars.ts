import {
  DateIntervalBarChart,
  MongoAggregation
} from '../types'

export const results = ({ x, y, group, period }: DateIntervalBarChart): MongoAggregation => [
  { $group: { _id: { [`${period}`]: { [`$${period}`]: `$${x}` }, group: `$${group}` }, value: { $sum: `$${y}` } } },
]

