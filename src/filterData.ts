import _ from'lodash/fp'
import {
  Filter,
  SearchRestrictons,
  MongoAggregation
} from './types'
import { results as arrayElementPropFacet } from './nodes/arrayElementPropFacet'
import { results as facet } from './nodes/facet'
import { results as subqueryFacet } from './nodes/subqueryFacet'

let typeAggs = (restrictions: SearchRestrictons, subqueryValues: { [key: string]: any }) => ({
  arrayElementPropFacet: arrayElementPropFacet(restrictions, subqueryValues),
  facet: facet(restrictions, subqueryValues),
  subqueryFacet: subqueryFacet (restrictions, subqueryValues),
})

let noResultsTypes = ['propExists', 'numeric', 'dateTimeInterval', 'boolean', 'fieldHasTruthyValue', 'arraySize']

export let getFacets = (restrictions: SearchRestrictons, subqueryValues: { [k: string]: any[]}, filters: Filter[], collection: string) => {
  let facetFilters = _.reject((f: Filter) => _.includes(f.type, noResultsTypes), filters)
  let result: { [k: string]: MongoAggregation } = {}

  let restrictedTypeAggs: { [k: string]: MongoAggregation } = typeAggs(restrictions, subqueryValues)

  for (let filter of _.values(facetFilters)) {
    const resultForKey: MongoAggregation = restrictedTypeAggs[filter.type](filter, filters, collection, subqueryValues)
    result[filter.key] = resultForKey
  }

  return result
}
