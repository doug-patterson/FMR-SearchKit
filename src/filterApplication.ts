import _ from 'lodash/fp'
import { Filter, MongoObjectIdConstructor } from './types'
import { filter as dateTimeInterval } from './nodes/dateTimeInterval'
import { filter as arrayElementPropFacet } from './nodes/arrayElementPropFacet'
import { filter as facet } from './nodes/facet'
import { filter as subqueryFacet } from './nodes/subqueryFacet'
import { filter as numeric } from './nodes/numeric'
import { filter as boolean } from './nodes/boolean'
import { filter as fieldHasTruthyValue } from './nodes/fieldHasTruthyValue'
import { filter as propExists } from './nodes/propExists'
import { filter as arraySize } from './nodes/arraySize'

const typeFilters = (ObjectId: MongoObjectIdConstructor): any => ({
  dateTimeInterval,
  arrayElementPropFacet: arrayElementPropFacet(ObjectId),
  facet: facet(ObjectId),
  subqueryFacet,
  numeric,
  boolean,
  fieldHasTruthyValue,
  propExists,
  arraySize
})

const typeFilterStages =
  (subqueryValues = {}, ObjectId: MongoObjectIdConstructor) =>
  (filter: Filter) =>
    typeFilters(ObjectId)[filter.type]({
      ...filter,
      subqueryValues: subqueryValues[filter.key as keyof typeof subqueryValues]
    } as any)

export const getTypeFilterStages = (
  queryFilters: Filter[],
  subqueryValues: { [key: string]: any },
  ObjectId: MongoObjectIdConstructor
) => _.flatMap(typeFilterStages(subqueryValues, ObjectId), queryFilters)
