import _ from'lodash/fp'
import { Filter } from './types'
import { filter as dateTimeInterval } from './nodes/dateTimeInterval'
import { filter as arrayElementPropFacet } from './nodes/arrayElementPropFacet'
import { filter as facet } from './nodes/facet'
import { filter as subqueryFacet } from './nodes/subqueryFacet'
import { filter as numeric } from './nodes/numeric'
import { filter as boolean } from './nodes/boolean'
import { filter as fieldHasTruthyValue } from './nodes/fieldHasTruthyValue'
import { filter as propExists } from './nodes/propExists'
import { filter as arraySize } from './nodes/arraySize'

let typeFilters = {
  dateTimeInterval,
  arrayElementPropFacet,
  facet,
  subqueryFacet,
  numeric,
  boolean,
  fieldHasTruthyValue,
  propExists,
  arraySize,
}

let typeFilterStages = (subqueryValues = {}) => (filter: Filter) => typeFilters[filter.type as keyof typeof typeFilters]({ ...filter, subqueryValues: subqueryValues[filter.key as keyof typeof subqueryValues] } as any)

export let getTypeFilterStages = (queryFilters: Filter[], subqueryValues: { [key: string]: any }) =>
  _.flatMap(typeFilterStages(subqueryValues), queryFilters)