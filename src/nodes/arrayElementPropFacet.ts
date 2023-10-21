import {
  ArrayElementPropFacetFilter,
  Filter,
  SearchRestrictons,
  MongoAggregation
} from '../types'
import { arrayToObject } from '../util'
import { ObjectId } from 'mongodb'
import _ from 'lodash/fp'
import { getTypeFilterStages } from '../filterApplication'

export const filter = ({ field, prop, idPath, values, isMongoId, exclude }: ArrayElementPropFacetFilter): MongoAggregation =>
    _.size(values)
      ? [
          {
            $match: {
              [`${field}.${prop}${idPath ? '.' : ''}${idPath || ''}`]: {
                [`${exclude ? '$nin' : '$in'}`]:
                  _.size(values) && isMongoId
                    ? _.map((val: string) => new ObjectId(val), values)
                    : values,
              },
            },
          },
        ]
      : []

export const results = (restrictions: SearchRestrictons, subqueryValues: { [key: string]: any }): MongoAggregation => (
  { key, field, prop, values = [], isMongoId, lookup, idPath, include, optionSearch, size = 100 }: ArrayElementPropFacetFilter,
  filters: Filter[],
  collection: string
) => [
  ...restrictions[collection],
  ...getTypeFilterStages(_.reject({ key }, filters), subqueryValues),
  { $unwind: { path: `$${field}` } },
  { $group: { _id: `$${field}.${prop}${idPath ? '.' : ''}${idPath || ''}`, count: { $addToSet: '$_id' }, value: { $first: idPath ? `$${field}.${prop}`: `$${field}` } } },
  ...(lookup
    ? [
        {
          $lookup: {
            from: lookup.from,
            let: { localId: '$_id' },
            pipeline: [
              ...restrictions[lookup.from],
              {
                $match: {
                  $expr: { $eq: [`$${lookup.foreignField}`, '$$localId'] },
                },
              },
            ],
            as: 'lookup',
          },
        },
        {
          $unwind: {
            path: '$lookup',
            preserveNullAndEmptyArrays: true,
          },
        },
        {
          $project: {
            _id: 1,
            count: 1,
            ...arrayToObject(
              (include: string) => `lookup.${include}`,
              _.constant(1),
              lookup.include
            ),
          },
        },
      ]
    : []),
  ...(optionSearch ? [{ $match: { [(include || lookup) ? `value.${include ? _.first(include) : _.first(lookup?.include)}`: '_id']: { $regex: optionSearch, $options: 'i' } } }] : []),
  {
    $project: {
      _id: 1,
      count: { $size: '$count' },
      lookup: 1,
      ...(include ? {
        ...arrayToObject(
          (include: string) => `value.${include}`,
          _.constant(1),
          include
        )
      } : { value: 1 }),
      checked: {
        $in: [
          '$_id',
          _.size(values) && isMongoId ? _.map((val: string) => new ObjectId(val), values) : values,
        ],
      },
    },
  },
  {
    $sort: { checked: -1, count: -1, ...(include ? { [`value.${_.first(include)}`]: 1 } : { value: 1 }), ...(lookup ? { [`lookup.${_.first(lookup.include)}`]: 1 } : { value: 1 }), ...((!include && !lookup) ? { value: 1 } : {}) }
  },
  { $limit: size },
]
