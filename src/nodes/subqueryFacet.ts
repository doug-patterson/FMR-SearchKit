import {
  SubqueryFacetFilter,
  Filter,
  SearchRestrictons,
  MongoAggregation
} from '../types'
import _ from 'lodash/fp'
import { getTypeFilterStages } from '../filterApplication'
import { arrayToObject } from '../util'
import { ObjectId } from 'mongodb'

export const filter = ({
  field,
  idPath,
  subqueryValues
}: SubqueryFacetFilter): MongoAggregation =>
  _.size(subqueryValues)
    ? [
        {
          $match: {
            [`${field}${idPath ? '.' : ''}${idPath || ''}`]: {
              $in: subqueryValues
            },
          },
        },
      ]
    : []

export const results = (restrictions: SearchRestrictons, subqueryValues: { [key: string]: any }) => (
  { key, field, idPath, subqueryLocalIdPath, subqueryCollection, subqueryField, include, values = [], optionsAreMongoIds, optionSearch, size = 100, lookup }: SubqueryFacetFilter,
  filters: Filter[],
  collection: string
): MongoAggregation => [
  ...restrictions[collection],
  ...getTypeFilterStages(_.reject({ key }, filters), subqueryValues),
  { $group: { _id: `$${field}${idPath ? '.' : ''}${idPath || ''}`, count: { $sum: 1 } } },
  { $lookup: {
    from: subqueryCollection,
    as: 'value',
    localField: '_id',
    foreignField: subqueryField
  } },
  { $unwind: {
    path: '$value',
    preserveNullAndEmptyArrays: true
  } },
  { $group: { _id: `$value.${subqueryLocalIdPath}`, count: { $sum: '$count' }, value: { $first: `$value` } } },
  ...(optionSearch ? [{ $match: { [include ? `value.${_.first(include)}`: '_id']: { $regex: optionSearch, $options: 'i' } } }] : []),
  {
    $project: {
      _id: 1,
      count: 1,
      ...(include ? {
        ...arrayToObject(
          (include: string) => `value.${include}`,
          _.constant(1)
        )(include)
      } : { value: 1 }),
      checked: {
        $in: [
          '$_id',
          _.size(values) && optionsAreMongoIds ? _.map((val: string) => new ObjectId(val), values) : values,
        ],
      },
    },
  },
  {
    $sort: { checked: -1, count: -1, ...(include ? { [`value.${_.first(include)}`]: 1 } : { value: 1 }), ...((!include && !lookup) ? { value: 1 } : {}) }
  },
  { $limit: size },
]