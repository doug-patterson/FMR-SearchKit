import {
  FieldStatsChart,
  MongoAggregation
} from '../types'
import _ from 'lodash/fp'

export const results = ({ unwind, field, idPath, statsField, include, page = 1, pageSize, sort, sortDir }: FieldStatsChart): MongoAggregation => [
  ...(unwind ? [{ $unwind: { path: `$${unwind}`, preserveNullAndEmptyArrays: true  } }] : []),
  {
    $group: {
      _id: `$${field}${idPath ? '.' : ''}${idPath || ''}`,
      value: { $first: `$${field}` },
      count: { $sum: 1 },
      ..._.flow(
        _.map((stat: string) => [stat, { [`$${stat}`]: `$${statsField}`}]),
        _.fromPairs
      )(_.without(['count'], include))
    }
  },
  { $group: {
    _id: null,
    records: { $push: '$$ROOT' },
    resultsCount: { $sum: 1}
  } },
  { $unwind: '$records' },
  { $set: { 'records.resultsCount': '$resultsCount' } },
  { $replaceRoot: { newRoot: '$records' } },
  { $sort: { [sort || _.first(include) as string]: sortDir === 'asc' ? 1 : -1 } },
  { $skip: (page - 1) * pageSize },
  { $limit: pageSize }
]
