import { TopNPieChart, SearchRestrictions, MongoAggregation } from '../types'
import _ from 'lodash/fp'

import { arrayToObject } from '../util'

export const results =
  (restrictions: SearchRestrictions): MongoAggregation =>
  ({ field, idPath, size = 10, unwind, lookup, include }: TopNPieChart) => [
    ...(unwind ? [{ $unwind: `$${unwind}` }] : []),
    {
      $group: {
        _id: `$${field}${idPath ? '.' : ''}${idPath || ''}`,
        count: { $sum: 1 },
        labelValue: { $first: `$${field}` }
      }
    },
    { $sort: { count: -1 } },
    { $limit: size },
    ...(lookup
      ? [
          {
            $lookup: {
              from: lookup.from,
              let: { localId: '$_id' },
              pipeline: [
                ...restrictions[lookup.from],
                ...(lookup.unwind ? [{ $unwind: `$${lookup.unwind}` }] : []),
                {
                  $match: {
                    $expr: { $eq: [`$${lookup.foreignField}`, '$$localId'] }
                  }
                }
              ],
              as: 'lookup'
            }
          },
          { $unwind: '$lookup' },
          {
            $project: {
              _id: 1,
              count: 1,
              ...arrayToObject(
                (include: string) => `lookup.${include}`,
                _.constant(1)
              )(lookup.include)
            }
          }
        ]
      : []),
    {
      $project: {
        _id: 0,
        id: `$_id`,
        label: '$_id',
        lookup: 1,
        ...(include
          ? {
              ...arrayToObject((include: string) => `labelValue.${include}`, _.constant(1))(include)
            }
          : { labelValue: 1 }),
        value: `$count`
      }
    }
  ]
