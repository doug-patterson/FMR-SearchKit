import _ from'lodash/fp'
import { ObjectId } from 'mongodb'
import {
  FeathersApp,
  FeathersContextParams,
  FeathersServiceHooks,
  MongoAggregation,
  Search,
  SubqueryFacetFilter,
  SearchRestrictons,
} from './types'
import { arrayToObject } from './util'
import {
  applyServiceRestrictions,
  getAfterHookExecutor
} from './hookApplication'
import { getTypeFilterStages } from './filterApplication'
import { getFacets } from './filterData'
import { getCharts } from './charts'
import { lookupStages } from './lookup'

module.exports = ({
  services,
  restrictSchemasForUser = _.constant(_.identity),
  servicesPath = 'services/',
  maxResultSize
}: {
  services: string[]
  restrictSchemasForUser?: Function
  servicesPath?: string
  maxResultSize?: number
}) => async (app: FeathersApp) => {
  let schemas = _.flow(
    _.map((service: string) => [service, require(`${servicesPath}${service}/schema`)]),
    _.fromPairs,
  )(services)
  
  app.use('/schema', {
    get: async (collection: string, { user }: FeathersContextParams) => {
      return restrictSchemasForUser(user)(schemas)[collection]
    },
    find: async ({ user }: FeathersContextParams) => {
      return restrictSchemasForUser(user)(schemas)
    },
  })

  let hooks = _.flow(
    _.map((service: string) => [service, require(`${servicesPath}${service}/hooks`)]),
    _.fromPairs,
    _.mapValues(({ before, after }: FeathersServiceHooks) => ({
      before: [...(before?.all || []), ...(before?.find || [])],
      after: [...(after?.find || []), ...(after?.all || [])]
    }))
  )(services)

  let applyRestrictions = applyServiceRestrictions({ app, hooks })
  let afterHookExecutor = getAfterHookExecutor({ app, hooks })

  app.use('/search', {
    create: async (
      {
        collection,
        sortField,
        sortDir,
        include,
        page = 1,
        pageSize = 100,
        filters,
        charts,
        lookup,
        includeSchema,
      }: Search,
      params: FeathersContextParams
    ) => {
      if (!_.includes(collection, services)) {
        throw new Error('Unauthorized collection request')
      }
      if (maxResultSize && pageSize > maxResultSize) {
        throw new Error('Too many results requested')
      }

      let schema = await app.service('schema').get(collection)
      let project = arrayToObject(_.identity, _.constant(1))(include || _.keys(schema.properties))

      let collections: string[] = _.flow(_.compact, _.uniq)([
        collection, 
        ..._.map('lookup.from', charts),
        ..._.map('lookup.from', filters),
        ..._.map('from', lookup)
      ])

      if (_.size(_.difference(collections, services))) {
        throw new Error('Unauthorized collection request')
      }

      let getRestrictions = async () => {
        let restrictionAggs = await Promise.all(_.map((collectionName: string) => applyRestrictions(collectionName, params), collections))
        return _.zipObject(collections, restrictionAggs)
      }

      let subqueryFilters = _.filter({ type: 'subqueryFacet' }, filters) as SubqueryFacetFilter[]
      let subqueryCollections = _.map('subqueryCollection', subqueryFilters)
      if (_.size(_.difference(subqueryCollections, services))) {
        throw new Error('Unauthorized collection request')
      }

      let runSubqueries = _.size(subqueryFilters) ? async () => {
        let subqueryAggs = await Promise.all(_.map(async ({ values, optionsAreMongoIds, subqueryCollection, subqueryKey, subqueryField, subqueryFieldIdPath, subqueryFieldIsArray }: SubqueryFacetFilter) => [
          subqueryCollection,
          _.size(values) ? [
            ...await applyRestrictions(subqueryCollection, params),
            { $match: { [subqueryKey]: { $in: optionsAreMongoIds ? _.map((val: string) => new ObjectId(val), values) : values } } },
            ...(subqueryFieldIsArray ? [{ $unwind: { path: `$${subqueryField}${subqueryFieldIdPath ? '.' : ''}${subqueryFieldIdPath || ''}`, preserveNullAndEmptyArrays: true }}] : []),
            { $group: { _id: null, value: { $addToSet: `$${subqueryField}${subqueryFieldIdPath ? '.' : ''}${subqueryFieldIdPath || ''}` } } },
            { $unwind: '$value' },
          ]: null
        ], subqueryFilters))

        let subqueryResults = await Promise.all(_.map((agg: any) => _.last(agg) ? app.service(_.first(agg)).Model.aggregate(_.last(agg), { allowDiskUse: true }).toArray() : [], subqueryAggs))

        return _.zipObject(_.map('key', subqueryFilters), _.map(_.map('value'), subqueryResults))
      } : _.noop

      let [restrictions, subqueryValues]: [SearchRestrictons, any] = await Promise.all([
        getRestrictions(),
        runSubqueries()
      ])

      let fullQuery = getTypeFilterStages(filters, subqueryValues)

      let aggs: { [k: string]: MongoAggregation } = {
        resultsFacet: [
          ...restrictions[collection],
          ...fullQuery,
          { $facet: {
            ...(pageSize !== 0 ? { results: [
              ...(sortField
                ? [{ $sort: { [sortField]: sortDir === 'asc' ? 1 : -1 } }]
                : []),
              { $skip: (page - 1) * (pageSize || 100) },
              { $limit: pageSize || 100 },
              ...(lookup ? _.flatten(lookupStages(restrictions, lookup)) : []),
              { $project: project },
            ] } : {}),
            resultsCount: [
              { $group: { _id: null, count: { $sum: 1 } } },
            ],
            ...getCharts(restrictions, charts)
          } }
        ],
        ...getFacets(restrictions, subqueryValues, filters, collection)
      }

      let result: any = _.fromPairs(
        await Promise.all(
          _.map(async (key: string): Promise<[string, any[]]> => {
            let agg = aggs[key]
            let aggResult = await app.mongodb.db
              .collection(collection)
              .aggregate(agg, { allowDiskUse: true })
              .toArray()

            return [key, aggResult]
          }, _.keys(aggs))
        )
      )

      let resultsFacet: any = _.first(result.resultsFacet)

      result = {
        ..._.omit(['resultsFacet'], result),
        results: resultsFacet.results,
        resultsCount: _.first(resultsFacet.resultsCount),
        charts: _.omit(['results', 'resultsCount'], resultsFacet)
      }

      result.results = await Promise.all(
        _.map(afterHookExecutor({ collection, params }), result.results)
      )

      for (let field in lookup) {
        let { from } = lookup[field]

        result.results = await Promise.all(
          _.map(
            async (result: any[]) => ({
              ...result,
              [field]: await afterHookExecutor({
                collection: from,
                field,
                params
              })(result),
            }),
            result.results
          )
        )
      }

      if (includeSchema) {
        result.schema = schema
      }

      return result
    },
  })
}
