import {
  SummaryTableChart,
  MongoAggregation
} from '../types'
import _ from 'lodash/fp'

export const results = ({ rows, group, sortField, nameField }: SummaryTableChart): MongoAggregation => [
  ..._.flow(
    _.filter('unwind'),
    (val: any) => _.map(({ key = '', field = '' }) => ({
      $set: { [key]: { $reduce: {
        input: `$${field}`,
        initialValue: 0,
        in: { $sum: [ '$$value', '$$this' ] }
      } } }
    }))(val)
  )(rows),
  { $group: {
    _id: group ? `$${group}` : null,
    name: { $first: `$${nameField}` },
    ..._.flow(
      _.filter('agg'),
      (val: any) => _.map(({ key = '', field = '', agg = '', unwind = false } ) => [key, { [`$${agg}`]: `$${unwind ? key : field}` }])(val),
      _.fromPairs,
    )(rows),
  } },
  ...(sortField ? [{ $sort: { [`${sortField}`]: -1 } }] : [])
]
