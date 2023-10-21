import _ from'lodash/fp'
import {
  Filter,
  SearchRestrictons,
  MongoAggregation,
  MongoObjectIdConstructor
} from './types'
import { results as arrayElementPropFacet } from './nodes/arrayElementPropFacet'
import { results as facet } from './nodes/facet'
import { results as subqueryFacet } from './nodes/subqueryFacet'

const typeAggs = (restrictions: SearchRestrictons, subqueryValues: { [key: string]: any }, ObjectId: MongoObjectIdConstructor) => ({
  arrayElementPropFacet: arrayElementPropFacet(restrictions, subqueryValues, ObjectId),
  facet: facet(restrictions, subqueryValues, ObjectId),
  subqueryFacet: subqueryFacet (restrictions, subqueryValues, ObjectId),
})

const noResultsTypes = ['propExists', 'numeric', 'dateTimeInterval', 'boolean', 'fieldHasTruthyValue', 'arraySize']

export const getFacets = (restrictions: SearchRestrictons, subqueryValues: { [k: string]: any[]}, filters: Filter[], collection: string, ObjectId: MongoObjectIdConstructor) => {
  const facetFilters = _.reject((f: Filter) => _.includes(f.type, noResultsTypes), filters)
  const result: { [k: string]: MongoAggregation } = {}

  const restrictedTypeAggs: { [k: string]: MongoAggregation } = typeAggs(restrictions, subqueryValues, ObjectId)

  for (const filter of _.values(facetFilters)) {
    const resultForKey: MongoAggregation = restrictedTypeAggs[filter.type](filter, filters, collection, subqueryValues)
    result[filter.key] = resultForKey
  }

  return result
}
