import {
  DateLineMultipleChart,
  MongoAggregation
} from '../types'
import _ from 'lodash/fp'
import { periods, dateGroup } from '../util'

const dateProjectMultiple = (period: string) => {
  let dateGroupPick = _.slice(_.indexOf(period, periods), Infinity, periods)

  let [first, ...rest] = _.map((field: string) => `$_id.date.${field}`, dateGroupPick)
  let arr: any[] = [{ $toString: first }]
  let field: string
  
  while (field = rest.shift() || '') {
    arr.push('/', { $toString: field })
  }

  return { $concat: arr }
}

export const results = ({ x, y, period, agg = 'sum', offset = 0, group, idPath }: DateLineMultipleChart): MongoAggregation => [
  { $group: { _id: { date: dateGroup(x, period, offset), group: `$${group}${idPath ? '.' : ''}${idPath || ''}` }, value: { $sum: agg === 'sum' ? `$${y}` : 1 }, idx: { $min: `$${x}`} } },
  { $sort: {
    '_id.date.year': 1,
    '_id.date.month': 1,
    '_id.date.day': 1
  } },
  { $project: {
    _id: 0,
    x: dateProjectMultiple(period),
    y: `$value`,
    group: `$_id.group`
  } },
  { $group: { _id: '$group', data: { $push: '$$ROOT' } } },
  { $project: { _id: 0, id: '$_id', data: 1 } }
]
