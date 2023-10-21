import _ from'lodash/fp'
import { ObjectId } from 'mongodb'
import {
  Filter,
  SubqueryFacetFilter,
  ArrayElementPropFacetFilter,
  FacetFilter,
  SearchRestrictons,
} from './types'
import { getTypeFilterStages } from './filterApplication'
import { arrayToObject } from './util'

let typeAggs = (restrictions: SearchRestrictons, subqueryValues: { [key: string]: any }) => ({
  arrayElementPropFacet: (
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
                _.constant(1)
              )(lookup.include),
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
            _.constant(1)
          )(include)
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
  ],
  facet: (
    { key, field, idPath, include, values = [], isMongoId, lookup, optionSearch, size = 100 }: FacetFilter,
    filters: Filter[],
    collection: string
  ) => [
    ...restrictions[collection],
    ...getTypeFilterStages(_.reject({ key }, filters), subqueryValues),
    { $unwind: { path: `$${field}${idPath ? '.' : ''}${idPath || ''}`, preserveNullAndEmptyArrays: true } },
    { $group: { _id: `$${field}${idPath ? '.' : ''}${idPath || ''}`, count: { $sum: 1 }, value: { $first: `$${field}` } } },
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
                _.constant(1)
              )(lookup.include),
            },
          },
        ]
      : []),
    ...(optionSearch ? [{ $match: { [(include || lookup) ? `value.${include ? _.first(include) : _.first(lookup?.include)}`: '_id']: { $regex: optionSearch, $options: 'i' } } }] : []),
    {
      $project: {
        _id: 1,
        count: 1,
        lookup: 1,
        ...(include ? {
          ...arrayToObject(
            (include: string) => `value.${include}`,
            _.constant(1)
          )(include)
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
  ],
  subqueryFacet: (
    { key, field, idPath, subqueryLocalIdPath, subqueryCollection, subqueryField, include, values = [], optionsAreMongoIds, optionSearch, size = 100, lookup }: SubqueryFacetFilter,
    filters: Filter[],
    collection: string
  ) => [
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
  ],
})

let noResultsTypes = ['propExists', 'numeric', 'dateTimeInterval', 'boolean', 'fieldHasTruthyValue', 'arraySize']

export let getFacets = (restrictions: SearchRestrictons, subqueryValues: { [k: string]: any[]}, filters: Filter[], collection: string) => {
  let facetFilters = _.reject((f: Filter) => _.includes(f.type, noResultsTypes), filters)
  let result: any = {}

  let restrictedTypeAggs: any = typeAggs(restrictions, subqueryValues)

  for (let filter of _.values(facetFilters)) {
    const resultForKey: any = restrictedTypeAggs[filter.type](filter, filters, collection, subqueryValues) as any
    result[filter.key] = resultForKey
  }

  return result
}