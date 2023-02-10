let _ = require('lodash/fp')
let { ObjectId } = require('mongodb')
let {
  startOfDay,
  startOfWeek,
  startOfMonth,
  startOfQuarter,
  startOfYear,
  addMinutes,
  addHours,
  addDays,
  addWeeks,
  addMonths,
  addQuarters,
  addYears
} = require('date-fns')

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

  let beforeHookQueryProps = _.keys(beforeContext.params.query)
  let props = _.reject(prop => _.includes(prop, ['$skip', '$limit']), beforeHookQueryProps)
  let beforeHookQuery = _.pick(props, beforeContext.params.query)

  return [{ $match: beforeHookQuery }]
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

let applyOffset = (endpoint, offset) => addMinutes(endpoint, 0 - offset)

let intervals = {
  'Today': (date, offset) => ({ from: applyOffset(startOfDay(applyOffset(date, offset)), 0 - offset) }),
  'Current Week': (date, offset) => ({ from: applyOffset(startOfWeek(applyOffset(date, offset)), 0 - offset) }),
  'Current Month': (date, offset) => ({ from: applyOffset(startOfMonth(applyOffset(date, offset)), 0 - offset) }),
  'Current Quarter': (date, offset) => ({ from: applyOffset(startOfQuarter(applyOffset(date, offset)), 0 - offset) }),
  'Current Year': (date, offset) => ({ from: applyOffset(startOfYear(applyOffset(date, offset)), 0 - offset) }),

  // no offest for these
  'Last Hour': date => ({ from: addHours(date, -1) }),
  'Last Two Hours': date => ({ from: addHours(date, -2) }),
  'Last Four Hours': date => ({ from: addHours(date, -4) }),
  'Last Eight Hours': date => ({ from: addHours(date, -8) }),
  'Last Twelve Hours': date => ({ from: addHours(date, -12) }),
  'Last Day': date => ({ from: addDays(date, -1) }),
  'Last Two Days': date => ({ from: addDays(date, -2) }),
  'Last Three Days': date => ({ from: addDays(date, -3) }),
  'Last Week': date => ({ from: addDays(date, -7) }),
  'Last Two Weeks': date => ({ from: addDays(date, -14) }),
  'Last Month': date => ({ from: addMonths(date, -1) }),
  'Last Quarter': date => ({ from: addQuarters(date, -1) }),
  'Last Year': date => ({ from: addYears(date, -1) }),
  'Last Two Years': date => ({ from: addYears(date, -1) }),

  'Previous Full Day': (date, offset) => ({ from: applyOffset(addDays(startOfDay(applyOffset(date, offset)), -1), 0 - offset), to: applyOffset(startOfDay(applyOffset(date, offset)), 0 - offset) }),
  'Previous Full Week': (date, offset) => ({ from: applyOffset(addWeeks(startOfWeek(applyOffset(date, offset)), -1), 0 - offset), to: applyOffset(startOfWeek(applyOffset(date, offset)), 0 - offset) }),
  'Previous Full Month': (date, offset) => ({ from: applyOffset(addMonths(startOfMonth(applyOffset(date, offset)), -1), 0 - offset), to: applyOffset(startOfMonth(applyOffset(date, offset)), 0 - offset) }),
  'Previous Full Quarter': (date, offset) => ({ from: applyOffset(addQuarters(startOfQuarter(applyOffset(date, offset)), -1), 0 - offset), to: applyOffset(startOfQuarter(applyOffset(date, offset)), 0 - offset) }),
  'Previous Full Year': (date, offset) => ({ from: applyOffset(addYears(startOfYear(applyOffset(date, offset)), -1), 0 - offset), to: applyOffset(startOfYear(applyOffset(date, offset)), 0 - offset) }),
}

let intervalEndpoints = (interval, offset) => intervals[interval](new Date(), offset)

let typeFilters = {
  arrayElementPropFacet: ({ field, prop, idPath, values, isMongoId, exclude }) =>
    _.size(values)
      ? [
          {
            $match: {
              [`${field}.${prop}${idPath ? '.' : ''}${idPath || ''}`]: {
                [`${exclude ? '$nin' : '$in'}`]:
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
  subqueryFacet: ({
    field,
    idPath,
    subqueryValues
  }) =>
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
  dateTimeInterval: ({ field, from, to, interval, offset }) => {
    if (interval) {
      let endpoints = intervalEndpoints(interval, offset)
      to = endpoints.to
      from = endpoints.from
    } else {
      from = from && new Date(from)
      to = to && new Date(to)
      if (offset) {
        from = from && addMinutes(from, offset)
        to = to && addMinutes(to, offset)
      }
    }
    return (from || to) ? [
      {
        $match: {
          $and: _.compact([
            from && { [field]: { $gte: from } },
            to && { [field]: { $lt: to } },
          ]),
        },
      }
    ] : []
  },
  boolean: ({ field, checked }) =>
    checked
      ? [
          {
            $match: { [field]: true },
          },
        ]
      : [],
  fieldHasTruthyValue: ({ field, checked, negate }) =>
    checked
      ? [
          {
            $match: { [field]: { $ne: null } },
          },
        ]
      : [],
  propExists: ({ field, negate }) =>
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

let typeFilterStages = (subqueryValues = {}) => filter => typeFilters[filter.type]({ ...filter, subqueryValues: subqueryValues[filter.key] })

let getTypeFilterStages = (queryFilters, subqueryValues) =>
  _.flatMap(typeFilterStages(subqueryValues), queryFilters)

let typeAggs = (restrictions, subqueryValues) => ({
  arrayElementPropFacet: (
    { key, field, prop, values = [], isMongoId, lookup, idPath, include, size = 100 },
    filters,
    collection
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
      $sort: { checked: -1, count: -1, ...(include ? { [`value.${_.first(include)}`]: 1 } : { value: 1 }) }
    },
    { $limit: size },
  ],
  facet: (
    { key, field, idPath, include, values = [], isMongoId, lookup, size = 100 },
    filters,
    collection
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
      $sort: { checked: -1, count: -1, ...(include ? { [`value.${_.first(include)}`]: 1 } : { value: 1 }) }
    },
    { $limit: size },
  ],
  subqueryFacet: (
    { key, field, idPath, subqueryLocalIdPath, subqueryCollection, subqueryField, include, values = [], optionsAreMongoIds, size = 100 },
    filters,
    collection
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
    {
      $project: {
        _id: 1,
        count: 1,
        ...(include ? {
          ...arrayToObject(
            include => `value.${include}`,
            _.constant(1)
          )(include)
        } : { value: 1 }),
        checked: {
          $in: [
            '$_id',
            _.size(values) && optionsAreMongoIds ? _.map(ObjectId, values) : values,
          ],
        },
      },
    },
    {
      $sort: { checked: -1, count: -1, ...(include ? { [`value.${_.first(include)}`]: 1 } : { value: 1 }) }
    },
    { $limit: size },
  ],
})

let noResultsTypes = ['propExists', 'numeric', 'dateTimeInterval', 'boolean', 'fieldHasTruthyValue', 'arraySize']

let getFacets = (restrictions, subqueryValues, filters, collection) => {
  let facetFilters = _.reject(f => _.includes(f.type, noResultsTypes), filters)
  let result = {}

  let restrictedTypeAggs = typeAggs(restrictions, subqueryValues)

  for (let filter of _.values(facetFilters)) {
    result[filter.key] = restrictedTypeAggs[filter.type](filter, filters, collection, subqueryValues)
  }

  return result
}

const fullDateGroup = (field, timezone) => ({
  year: { $year: { date: `$${field}`, timezone } },
  month: { $month: { date: `$${field}`, timezone } },
  week: { $week: { date: `$${field}`, timezone } },
  day: { $dayOfMonth: { date: `$${field}`, timezone } }
})

const timezoneOffset = num => {
  let sign = num < 0 ? '+' : '-' // reverse the offset received from the browser
  let abs = Math.abs(num)
  let hours = Math.floor(abs/60)
  let minutes = abs % 60
  let hoursString = `00${hours}`.substr(-2)
  let minutesString = `00${minutes}`.substr(-2)

  return `${sign}${hoursString}${minutesString}`
}

const periods = ['day', 'month', 'year']

const dateGroup = (field, period, offset) => {
  let dateGroupPick = _.slice(_.indexOf(period, periods), Infinity, periods)

  return _.pick(dateGroupPick, fullDateGroup(field, timezoneOffset(offset)))
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

const dateProjectMultiple = period => {
  let dateGroupPick = _.slice(_.indexOf(period, periods), Infinity, periods)

  let [first, ...rest] = _.map(field => `$_id.date.${field}`, dateGroupPick)
  let arr = [{ $toString: first }]
  
  while (field = rest.shift()) {
    arr.push('/', { $toString: field })
  }

  return { $concat: arr }
}

const getChart = restrictions => type => ({
  dateIntervalBars: ({ x, y, group, period }) => [
    { $group: { _id: { [`${period}`]: { [`$${period}`]: `$${x}` }, group: `$${group}` }, value: { $sum: `$${y}` } } },
  ],
  dateLineSingle: ({ x, y, period, agg = 'sum', offset = 0 }) => [
    { $group: { _id: dateGroup(x, period, offset), value: { $sum: agg === 'sum' ? `$${y}` : 1 }, idx: { $min: `$${x}`} } },
    { $sort: { idx: 1 } },
    { $project: {
      _id: 0,
      x: dateProject(period),
      y: `$value`
    } },
    { $group: { _id: null, data: { $push: '$$ROOT' } } },
    { $project: { _id: 0, id: 'results', data: 1 } }
  ],
  dateLineMultiple: ({ x, y, period, agg = 'sum', offset = 0, group, idPath }) => [
    { $group: { _id: { date: dateGroup(x, period, offset), group: `$${group}${idPath ? '.' : ''}${idPath || ''}` }, value: { $sum: agg === 'sum' ? `$${y}` : 1 }, idx: { $min: `$${x}`} } },
    { $sort: {
      '_id.date.year': 1,
      '_id.date.month': 1,
      '_id.date.day': 1
    } },
    { $project: {
      _id: 0,
      x: dateProjectMultiple(period),
      y: `$value`,
      group: `$_id.group`
    } },
    { $group: { _id: '$group', data: { $push: '$$ROOT' } } },
    { $project: { _id: 0, id: '$_id', data: 1 } }
  ],
  quantityByPeriodCalendar: ({ x, y, offset = 0 }) => [
    { $group: { _id: dateGroup(x, 'day', offset), value: { $sum: `$${y}` }, idx: { $min: `$${x}`} } },
    { $sort: { idx: 1 } },
    { $project: {
      _id: 0,
      day: dateProject2('day'),
      value: `$value`
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
  ],
  dayOfWeekSummaryBars: ({ x, y, group, idPath, agg, offset = 0 }) => [
    { $group: {
      _id: { day: { $dayOfWeek: { date: `$${x}`, timezone: timezoneOffset(offset) } }, [`${group || 'results'}`]: group ? `$${group}${idPath ? '.' : ''}${idPath || ''}` : 'results' },
      value: { $sum: agg === 'sum' ? `$${y}` : `$${y}` }
    } },
    { $group: {
      _id: `$_id.day`,
      segments: { $push: { k: `$_id.${group || 'results'}`, v: `$value` } },
    } },
    { $set: {
      segments: { $arrayToObject: '$segments'}
    }},
    { $set: {
      'segments.id': '$_id'
    }},
    { $replaceRoot: { newRoot: '$segments' } },
    { $sort: { id: 1 } },
    { $set: {
      id: { $arrayElemAt: [[null, 'Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'], `$id`] }
    } }
  ],
  hourOfDaySummaryLine: ({ x, y, group, idPath, agg, offset = 0 }) => [
    { $group: {
      _id: { hour: { $hour: { date: `$${x}`, timezone: timezoneOffset(offset) } }, [`${group || 'results'}`]: group ? `$${group}${idPath ? '.' : ''}${idPath || ''}` : 'results' },
      value: { $sum: agg === 'sum' ? `$${y}` : `$${y}` }
    } },
    { $group: {
      _id: `$_id.${group || 'results'}`,
      data: { $push: {
        x: '$_id.hour',
        y: `$value`
      } },
    } },
    { $project: {
      _id: 0,
      id: '$_id',
      data: 1
    } }
  ],
  summaryTable: ({ rows, group, sortField, nameField }) => [
    ..._.flow(
      _.filter('unwind'),
      _.map(({ key, field }) => ({
        $set: { [key]: { $reduce: {
          input: `$${field}`,
          initialValue: 0,
          in: { $sum: [ '$$value', '$$this' ] }
        } } }
      }))
    )(rows),
    { $group: {
      _id: group ? `$${group}` : null,
      name: { $first: `$${nameField}` },
      ..._.flow(
        _.filter('agg'),
        _.map(({ key, field, agg, unwind }) => [key, { [`$${agg}`]: `$${unwind ? key : field}` }]),
        _.fromPairs,
      )(rows),
    } },
    ...(sortField ? [{ $sort: { [`${sortField}`]: -1 } }] : [])
  ],
  fieldStats: ({ unwind, field, idPath, statsField, include, page = 1, pageSize, sort, sortDir }) => [
    ...(unwind ? [{ $unwind: { path: `$${unwind}`, preserveNullAndEmptyArrays: true  } }] : []),
    {
      $group: {
        _id: `$${field}${idPath ? '.' : ''}${idPath || ''}`,
        value: { $first: `$${field}` }, // need to use `valueInclude`
        count: { $sum: 1 },
        ..._.flow(
          _.map(stat => [stat, { [`$${stat}`]: `$${statsField}`}]),
          _.fromPairs
        )(_.without(['count'], include))
      }
    },
    { $group: {
      _id: null,
      records: { $push: '$$ROOT' },
      resultsCount: { $sum: 1}
    } },
    { $unwind: '$records' },
    { $set: { 'records.resultsCount': '$resultsCount' } },
    { $replaceRoot: { newRoot: '$records' } },
    { $sort: { [sort || _.first(include)]: sortDir === 'asc' ? 1 : -1 } },
    { $skip: (page - 1) * pageSize },
    { $limit: pageSize }
  ],
  groupedTotals: ({ group, include}) => [
    { $group: {
      _id: group ? `$${group}` : null,
      ..._.flow(
        _.map(({ key, field, agg }) => [key || field, { [`$${agg}`]: `$${field}` }]),
        _.fromPairs
      )(include)
    } }
  ]
}[type])

let getCharts = (restrictions, charts) => _.zipObject(_.map('key', charts), _.map(chart => getChart(restrictions)(chart.type)(chart), charts))

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
  servicesPath = 'services/',
  maxResultSize
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
        includeSchema,
      },
      params
    ) => {
      if (!_.includes(collection, services)) {
        throw new Error('Unauthorized collection request')
      }
      if (maxResultSize && pageSize > maxResultSize) {
        throw new Error('Too many results requested')
      }

      let schema = await app.service('schema').get(collection)
      let project = arrayToObject(_.identity, _.constant(1))(include || _.keys(schema.properties))

      let collections = _.flow(_.compact, _.uniq)([
        collection, 
        ..._.map('lookup.from', charts),
        ..._.map('lookup.from', filters)
      ])
      if (_.size(_.difference(collections, services))) {
        throw new Error('Unauthorized collection request')
      }

      let getRestrictions = async () => {
        let restrictionAggs = await Promise.all(_.map(collectionName => applyRestrictions(collectionName, params), collections))
        return _.zipObject(collections, restrictionAggs)
      }

      let subqueryFilters = _.filter({ type: 'subqueryFacet' }, filters)
      let subqueryCollections = _.map('subqueryCollection', subqueryFilters)
      if (_.size(_.difference(subqueryCollections, services))) {
        throw new Error('Unauthorized collection request')
      }

      let runSubqueries = _.size(subqueryFilters) ? async () => {
        let subqueryAggs = await Promise.all(_.map(async ({ values, optionsAreMongoIds, subqueryCollection, subqueryKey, subqueryField, subqueryFieldIdPath, subqueryFieldIsArray }) => [
          subqueryCollection,
          _.size(values) ? [
            ...await applyRestrictions(subqueryCollection, params),
            { $match: { [subqueryKey]: { $in: optionsAreMongoIds ? _.map(ObjectId, values) : values } } },
            ...(subqueryFieldIsArray ? [{ $unwind: { path: `$${subqueryField}${subqueryFieldIdPath ? '.' : ''}${subqueryFieldIdPath || ''}`, preserveNullAndEmptyArrays: true }}] : []),
            { $group: { _id: null, value: { $addToSet: `$${subqueryField}${subqueryFieldIdPath ? '.' : ''}${subqueryFieldIdPath || ''}` } } },
            { $unwind: '$value' },
          ]: null
        ], subqueryFilters))

        let subqueryResults = await Promise.all(_.map(agg => _.last(agg) ? app.service(_.first(agg)).Model.aggregate(_.last(agg), { allowDiskUse: true }).toArray() : [], subqueryAggs))

        return _.zipObject(_.map('key', subqueryFilters), _.map(_.map('value'), subqueryResults))
      } : _.noop

      let [restrictions, subqueryValues] = await Promise.all([
        getRestrictions(),
        runSubqueries()
      ])

      let fullQuery = getTypeFilterStages(filters, subqueryValues)

      let aggs = {
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
