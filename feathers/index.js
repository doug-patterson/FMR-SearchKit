let _ = require('lodash/fp')
let { ObjectId } = require('mongodb')

let mapIndexed = _.convert({ cap: false }).map
let arrayToObject = _.curry((key, val, arr) =>
  _.flow(_.keyBy(key), _.mapValues(val))(arr)
)

let makeContextForBeforeHooks = ({ app, params, collection }) => ({
  params: { ...params, query: {} },
  type: 'before',
  method: 'find',
  service: app.service(collection),
  provider: params.provider,
  app,
})

let applyServiceRestrictions = ({ app, hooks }) => async (collection, params) => {
  let beforeHooks = _.get(`${collection}.before`, hooks)
  let beforeContext = makeContextForBeforeHooks({ app, params, collection })
  
  for (let hook of beforeHooks) {
    beforeContext = (await hook(beforeContext)) || beforeContext
  }

  return [{ $match: beforeContext.params.query }]
}

let makeContextForAfterHooks = ({ app, params, field, record }) => ({
  params,
  method: 'find',
  type: 'after',
  result: field ? record[field] : record,
  provider: params.provider,
  app,
})

let getAfterHookExecutor = ({ app, hooks }) => ({ collection, field, params }) => async record => {
  let afterContext = makeContextForAfterHooks({
    app,
    params,
    field,
    record,
  })

  let afterHooks = _.get(`${collection}.after`, hooks)

  for (let hook of afterHooks) {
    afterContext = (await hook(afterContext)) || afterContext
  }
  return _.flow(_.castArray, _.first)(_.get('result', afterContext))
}

let typeFilters = {
  arrayElementPropFacet: ({ field, prop, values, isMongoId }) =>
    _.size(values)
      ? [
          {
            $match: {
              [`${field}.${prop}`]: {
                $in:
                  _.size(values) && isMongoId
                    ? _.map(ObjectId, values)
                    : values,
              },
            },
          },
        ]
      : [],
  facet: ({ field, values, isMongoId, exclude }) =>
    _.size(values)
      ? [
          {
            $match: {
              [field]: {
                [`${exclude ? '$nin' : '$in'}`]:
                  _.size(values) && isMongoId
                    ? _.map(ObjectId, values)
                    : values,
              },
            },
          },
        ]
      : [],
  numeric: ({ field, from, to }) =>
    _.isNumber(from) || _.isNumber(to)
      ? [
          {
            $match: {
              $and: _.compact([
                _.isNumber(from) && { [field]: { $gte: from } },
                _.isNumber(to) && { [field]: { $lte: to } },
              ]),
            },
          },
        ]
      : [],
  boolean: ({ field, checked }) =>
    checked
      ? [
          {
            $match: { [field]: true },
          },
        ]
      : [],
  arraySize: ({ field, from, to }) =>
    _.isNumber(from) || _.isNumber(to)
      ? [
          {
            $match: {
              $and: _.compact([
                _.isNumber(from) && {
                  [`${field}.${from - 1}`]: { $exists: true },
                },
                _.isNumber(to) && { [`${field}.${to}`]: { $exists: false } },
              ]),
            },
          },
        ]
      : [],
}

typeFilters.hidden = typeFilters.facet

let typeFilterStages = filter => typeFilters[filter.type](filter)

let getTypeFilterStages = queryFilters =>
  _.flatMap(typeFilterStages, queryFilters)

let typeAggs = applyRestrictions => ({
  arrayElementPropFacet: async (
    { key, field, prop, values = [], isMongoId },
    filters,
    collection,
    params
  ) => [
    ...(await applyRestrictions(collection, params)),
    ...getTypeFilterStages(_.reject({ key }, filters)),
    { $unwind: { path: `$${field}` } },
    { $group: { _id: `$${field}.${prop}`, count: { $addToSet: '$_id' } } },
    {
      $project: {
        _id: 1,
        count: { $size: '$count' },
        checked: {
          $in: [
            '$_id',
            _.size(values) && isMongoId ? _.map(ObjectId, values) : values,
          ],
        },
      },
    },
    {
      $sort: {
        count: -1,
      },
    },
  ],
  facet: async (
    { key, field, values = [], isMongoId, lookup },
    filters,
    collection,
    params
  ) => [
    // we should actually figure out what collections we'll be doing lookups on
    // beforehand and run all the apply restrictions calls in parallel with 
    // Promise.all and then pass the results into all these functions. Right now
    // we're running these potentially heavy feathers hook pipelines repeatedly and serially
    ...(await applyRestrictions(collection, params)),
    ...getTypeFilterStages(_.reject({ key }, filters)),
    { $unwind: { path: `$${field}`, preserveNullAndEmptyArrays: true } },
    { $group: { _id: `$${field}`, count: { $sum: 1 } } },
    ...(lookup
      ? [
          {
            $lookup: {
              from: lookup.from,
              // need to add support for `localField`
              let: { localId: '$_id' },
              pipeline: [
                ...(await applyRestrictions(lookup.from, params)),
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
                include => `lookup.${include}`,
                _.constant(1)
              )(lookup.include),
            },
          },
        ]
      : []),
    {
      $project: {
        _id: 1,
        count: 1,
        lookup: 1,
        checked: {
          $in: [
            '$_id',
            _.size(values) && isMongoId ? _.map(ObjectId, values) : values,
          ],
        },
      },
    },
    {
      $sort: { count: -1 }
    }
  ],
})

let noResultsTypes = ['hidden', 'numeric', 'boolean', 'arraySize']

let getFacets = applyRestrictions => async (filters, collection, params) => {
  let facetFilters = _.omitBy(f => _.includes(f.type, noResultsTypes), filters)
  let result = {}

  // parallelize this with Promise.all
  for (let filter of _.values(facetFilters)) {
    result[filter.key] = await typeAggs(applyRestrictions)[filter.type](filter, filters, collection, params)
  }

  return result
}

const fullDateGroup = field => ({
  year: { $year: `$${field}` },
  month: { $month: `$${field}` },
  week: { $week: `$${field}` },
  day: { $dayOfMonth: `$${field}` }
})

const periods = ['day', 'month', 'year']

const dateGroup = (field, period) => {
  let dateGroupPick = _.slice(_.indexOf(period, periods), Infinity, periods)

  return _.pick(dateGroupPick, fullDateGroup(field))
}

const dateProject = period => {
  let dateGroupPick = _.slice(_.indexOf(period, periods), Infinity, periods)

  let [first, ...rest] = _.map(field => `$_id.${field}`, dateGroupPick)
  let arr = [{ $toString: first }]
  
  while (field = rest.shift()) {
    arr.push('/', { $toString: field })
  }

  return { $concat: arr }
}

const dateProject2 = period => {
  let dateGroupPick = _.slice(_.indexOf(period, periods), Infinity, periods)

  let [first, ...rest] = _.map(field => `$_id.${field}`, _.reverse(dateGroupPick))
  let arr = [{ $toString: first }]
  
  while (field = rest.shift()) {
    arr.push('-', { $toString: field })
  }

  return { $concat: arr }
}


const getChart = type => ({
  dateIntervalBars: ({ x, y, group, period }) => [
    { $group: { _id: { [`${period}`]: { [`$${period}`]: `$${x}` }, group: `$${group}` }, value: { $sum: `$${y}` } } },
  ],
  dateLineSingle: ({ x, y, period }) => [
    { $group: { _id: dateGroup(x, period), value: { $sum: `$${y}` }, idx: { $min: `$${x}`} } },
    { $sort: { idx: 1 } },
    { $project: {
      _id: 0,
      x: dateProject(period),
      y: '$value'
    } },
    { $group: { _id: null, data: { $push: '$$ROOT' } } },
    { $project: { _id: 0, id: 'results', data: 1 } }
  ],
  // combine this with previous
  quantityByPeriodCalendar: ({ x, y }) => [
    { $group: { _id: dateGroup(x, 'day'), value: { $sum: `$${y}` }, idx: { $min: `$${x}`} } },
    { $sort: { idx: 1 } },
    { $project: {
      _id: 0,
      day: dateProject2('day'),
      value: '$value'
    } },
  ],
  topNPie: ({ field, size = 10, unwind }) => [
    ...[(unwind ? { $unwind: `$${unwind}` } : {})],
    { $group: { _id: `$${field}`, count: { $sum: 1 } } },
    { $sort: { count: -1 } },
    { $limit: size },
    { $project: { _id: 0, id: `$_id`, label: `$_id`, value: `$count` } }
  ]
}[type])

let getCharts = charts => _.zipObject(_.map('key', charts), _.map(chart =>getChart(chart.type)(chart), charts))

let lookupStages = (applyRestrictions, params) => async lookups => {
  let result = []
  for (let lookupName in lookups) {
    let {
      localField,
      foreignField,
      from,
      unwind,
      preserveNullAndEmptyArrays,
      include,
      isArray,
    } = lookups[lookupName]
    let lookupStages = _.compact([
      {
        $lookup: {
          from,
          let: { localVal: `$${localField}` },
          pipeline: [
            ...(await applyRestrictions(from, params)),
            {
              $match: {
                $expr: {
                  [isArray ? '$in' : '$eq']: [`$${foreignField}`, '$$localVal'],
                },
              },
            },
          ],
          as: localField,
        },
      },
      unwind && {
        $unwind: {
          path: `$${localField}`,
          ...(preserveNullAndEmptyArrays
            ? { preserveNullAndEmptyArrays: true }
            : {}),
        },
      },
      include && {
        $project: arrayToObject(_.identity, _.constant(1))(include),
      },
    ])

    result.push(lookupStages)
  }

  return result
}

module.exports = ({
  services,
  restrictSchemasForUser = _.constant(_.identity),
  servicesPath = 'services/'
}) => async app => {
  let schemas = _.flow(
    _.map(service => [service, require(`${servicesPath}${service}/schema`)]),
    _.fromPairs,
  )(services)
  
  app.use('/schema', {
    get: async (collection, { user }) => {
      return restrictSchemasForUser(user)(schemas)[collection]
    },
    find: async ({ user }) => {
      return restrictSchemasForUser(user)(schemas)
    },
  })

  let hooks = _.flow(
    _.map(service => [service, require(`${servicesPath}${service}/hooks`)]),
    _.fromPairs,
    _.mapValues(({ before, after }) => ({
      before: [...(before.all || []), ...(before.find || [])],
      after: [...(after.find || []), ...(after.all || [])]
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
        includeSchema
      },
      params
    ) => {
      let project = arrayToObject(_.identity, _.constant(1))(include)
      project._id = _.includes('_id', include) ? 1 : 0

      let fullQuery = getTypeFilterStages(filters)

      let aggs = {
        resultsFacet: [
          ...(await applyRestrictions(collection, params)),
          ...fullQuery,
          { $facet: {
            results: [
              ...(sortField
                ? [{ $sort: { [sortField]: sortDir === 'asc' ? 1 : -1 } }]
                : []),
              { $skip: (page - 1) * pageSize },
              { $limit: pageSize },
              ...(lookup ? _.flatten(await lookupStages(applyRestrictions, params)(lookup)) : []),
              { $project: project },
            ],
            resultsCount: [
              { $group: { _id: null, count: { $sum: 1 } } },
            ],
            ...getCharts(charts)
          } }
        ],
        ...(await getFacets(applyRestrictions)(filters, collection, params)),
      }

      let result = _.fromPairs(
        await Promise.all(
          mapIndexed(async (agg, key) => {
            let aggResult = await app.mongodb.db
              .collection(collection)
              .aggregate(agg, { allowDiskUse: true })
              .toArray()

            return [key, aggResult]
          }, aggs)
        )
      )

      let resultsFacet = _.first(result.resultsFacet)

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
            async result => ({
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
        result.schema = await app.service('schema').get(collection)
      }

      return result
    },
  })
}
