import {
  FacetFilter,
  SearchRestrictions,
  Filter,
  MongoAggregation,
  MongoObjectIdConstructor
} from '../types'
import _ from 'lodash/fp'
import { arrayToObject } from '../util'
import { getTypeFilterStages } from '../filterApplication'

export const filter =
  (ObjectId: MongoObjectIdConstructor) =>
  ({ field, idPath, values, isMongoId, exclude }: FacetFilter): MongoAggregation =>
    _.size(values)
      ? [
          {
            $match: {
              [`${field}${idPath ? '.' : ''}${idPath || ''}`]: {
                [`${exclude ? '$nin' : '$in'}`]:
                  _.size(values) && isMongoId
                    ? _.map((val: string) => new ObjectId(val), values)
                    : values
              }
            }
          }
        ]
      : []

export const results =
  (
    restrictions: SearchRestrictions,
    subqueryValues: { [key: string]: any },
    ObjectId: MongoObjectIdConstructor
  ): MongoAggregation =>
  (
    {
      key,
      field,
      idPath,
      include,
      values = [],
      isMongoId,
      lookup,
      optionSearch,
      size = 100
    }: FacetFilter,
    filters: Filter[],
    collection: string
  ) => [
    ...restrictions[collection] || [],
    ...getTypeFilterStages(_.reject({ key }, filters), subqueryValues, ObjectId),
    {
      $unwind: {
        path: `$${field}${idPath ? '.' : ''}${idPath || ''}`,
        preserveNullAndEmptyArrays: true
      }
    },
    {
      $group: {
        _id: `$${field}${idPath ? '.' : ''}${idPath || ''}`,
        count: { $sum: 1 },
        value: { $first: `$${field}` }
      }
    },
    ...(lookup
      ? [
          {
            $lookup: {
              from: lookup.from,
              let: { localId: '$_id' },
              pipeline: [
                ...restrictions[lookup.from] || [],
                {
                  $match: {
                    $expr: { $eq: [`$${lookup.foreignField}`, '$$localId'] }
                  }
                }
              ],
              as: 'lookup'
            }
          },
          {
            $unwind: {
              path: '$lookup',
              preserveNullAndEmptyArrays: true
            }
          },
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
    ...(optionSearch
      ? [
          {
            $match: {
              [include || lookup
                ? `value.${include ? _.first(include) : _.first(lookup?.include)}`
                : '_id']: { $regex: optionSearch, $options: 'i' }
            }
          }
        ]
      : []),
    {
      $project: {
        _id: 1,
        count: 1,
        lookup: 1,
        ...(include
          ? {
              ...arrayToObject((include: string) => `value.${include}`, _.constant(1))(include)
            }
          : { value: 1 }),
        checked: {
          $in: [
            '$_id',
            _.size(values) && isMongoId ? _.map((val: string) => new ObjectId(val), values) : values
          ]
        }
      }
    },
    {
      $sort: {
        checked: -1,
        count: -1,
        ...(include ? { [`value.${_.first(include)}`]: 1 } : { value: 1 }),
        ...(lookup ? { [`lookup.${_.first(lookup.include)}`]: 1 } : { value: 1 }),
        ...(!include && !lookup ? { value: 1 } : {})
      }
    },
    { $limit: size }
  ]
