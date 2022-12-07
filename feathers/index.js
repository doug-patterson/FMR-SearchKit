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
  let beforeHooks = _.get(`${collection}.before`, hooks) || []
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

  let afterHooks = _.get(`${collection}.after`, hooks) || []

  for (let hook of afterHooks) {
    afterContext = (await hook(afterContext)) || afterContext
  }
  return _.flow(_.castArray, _.first)(_.get('result', afterContext))
}

let typeFilters = {
  arrayElementPropFacet: ({ field, prop, idPath, values, isMongoId }) =>
    _.size(values)
      ? [
          {
            $match: {
              [`${field}.${prop}${idPath ? '.' : ''}${idPath || ''}`]: {
                $in:
                  _.size(values) && isMongoId
                    ? _.map(ObjectId, values)
                    : values,
              },
            },
          },
        ]
      : [],
  facet: ({ field, idPath, values, isMongoId, exclude }) =>
    _.size(values)
      ? [
          {
            $match: {
              [`${field}${idPath ? '.' : ''}${idPath || ''}`]: {
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
  hiddenExists: ({ field, negate }) =>
    [
      {
        $match: { [field]: { $exists: !negate } },
      },
    ],
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

let typeAggs = restrictions => ({
  arrayElementPropFacet: (
    { key, field, prop, values = [], isMongoId, lookup, idPath, include },
    filters,
    collection,
    size = 100
  ) => [
    ...restrictions[collection],
    ...getTypeFilterStages(_.reject({ key }, filters)),
    { $unwind: { path: `$${field}` } },
    { $group: { _id: `$${field}.${prop}${idPath ? '.' : ''}${idPath || ''}`, count: { $addToSet: '$_id' }, value: { $first: `$${field}.${prop}` } } },
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
        count: { $size: '$count' },
        lookup: 1,
        ...(include ? {
          ...arrayToObject(
            include => `value.${include}`,
            _.constant(1)
          )(include)
        } : { value: 1 }),
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
    { $limit: size },
  ],
  // both of these need to support an `idPath` property such that if it's present
  // the filtering is by field.idPath and the rest of field is included on `value`
  facet: (
    { key, field, idPath, include, values = [], isMongoId, lookup },
    filters,
    collection
  ) => [
    // we should actually figure out what collections we'll be doing lookups on
    // beforehand and run all the apply restrictions calls in parallel with 
    // Promise.all and then pass the results into all these functions. Right now
    // we're running these potentially heavy feathers hook pipelines repeatedly and serially
    ...restrictions[collection],
    ...getTypeFilterStages(_.reject({ key }, filters)),
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
        ...(include ? {
          ...arrayToObject(
            include => `value.${include}`,
            _.constant(1)
          )(include)
        } : { value: 1 }),
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

let noResultsTypes = ['hidden', 'hiddenExists', 'numeric', 'boolean', 'arraySize']

let getFacets = (restrictions, filters, collection) => {
  let facetFilters = _.omitBy(f => _.includes(f.type, noResultsTypes), filters)
  let result = {}

  for (let filter of _.values(facetFilters)) {
    result[filter.key] = typeAggs(restrictions)[filter.type](filter, filters, collection)
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


const getChart = restrictions => type => ({
  dateIntervalBars: ({ x, y, group, period }) => [
    { $group: { _id: { [`${period}`]: { [`$${period}`]: `$${x}` }, group: `$${group}` }, value: { $sum: `$${y}` } } },
  ],
  dateLineSingle: ({ x, y, period, agg = 'sum' }) => [ // implements sum and count right now
    { $group: { _id: dateGroup(x, period), value: { $sum: agg === 'sum' ? `$${y}` : 1 }, idx: { $min: `$${x}`} } },
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
  topNPie: ({ field, idPath, size = 10, unwind, lookup, include }) => [
    ...(unwind ? [{ $unwind: `$${unwind}` }] : []),
    { $group: { _id: `$${field}${idPath ? '.' : ''}${idPath || ''}`, count: { $sum: 1 }, labelValue: { $first: `$${field}` } } },
    { $sort: { count: -1 } },
    { $limit: size },
    ...(lookup ? [
      { $lookup: {
        from: lookup.from,
        let: { localId: '$_id' },
        pipeline: [
          ...restrictions[lookup.from],
          ...(lookup.unwind ? [{ $unwind: `$${lookup.unwind}` }]: []),
          {
            $match: {
              $expr: { $eq: [`$${lookup.foreignField}`, '$$localId'] },
            },
          },
        ],
        as: 'lookup',
      } },
      { $unwind: '$lookup' },
      { $project: {
        _id: 1,
        count: 1,
        ...arrayToObject(
          include => `lookup.${include}`,
          _.constant(1)
        )(lookup.include),
      } }] : []
    ),
    { $project: {
      _id: 0,
      id: `$_id`,
      label: '$_id',
      lookup: 1,
      ...(include ? {
        ...arrayToObject(
          include => `labelValue.${include}`,
          _.constant(1)
        )(include)
      } : { labelValue: 1 }),
      value: `$count`
    } }
  ]
}[type])

let getCharts = (restrictions, charts) => _.zipObject(_.map('key', charts), _.map(chart =>getChart(restrictions)(chart.type)(chart), charts))

let lookupStages = (restrictions, lookups) => {
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
    let stages = _.compact([
      {
        $lookup: {
          from,
          let: { localVal: `$${localField}` },
          pipeline: [
            ...restrictions[from],
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

    result.push(stages)
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
        includeSchema
      },
      params
    ) => {
      let schema = await app.service('schema').get(collection)
      let project = arrayToObject(_.identity, _.constant(1))(include || _.keys(schema.properties))

      let collections = _.flow(_.compact, _.uniq)([
        collection, 
        ..._.map('lookup.from', charts),
        ..._.map('lookup.from', filters)
      ])

      let restrictionAggs = await Promise.all(_.map(collectionName => applyRestrictions(collectionName, params), collections))
      let restrictions = _.zipObject(collections, restrictionAggs)

      let fullQuery = getTypeFilterStages(filters)

      let aggs = {
        resultsFacet: [
          ...restrictions[collection],
          ...fullQuery,
          { $facet: {
            results: [
              ...(sortField
                ? [{ $sort: { [sortField]: sortDir === 'asc' ? 1 : -1 } }]
                : []),
              { $skip: (page - 1) * pageSize },
              { $limit: pageSize },
              ...(lookup ? _.flatten(lookupStages(restrictions, lookup)) : []),
              { $project: project },
            ],
            resultsCount: [
              { $group: { _id: null, count: { $sum: 1 } } },
            ],
            ...getCharts(restrictions, charts)
          } }
        ],
        ...getFacets(restrictions, filters, collection),
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
        result.schema = schema
      }

      return result
    },
  })
}
