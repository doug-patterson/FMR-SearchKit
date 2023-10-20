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

import {
  FeathersApp,
  FeathersContextParams,
  FeathersServiceHooks,
  MongoAggregation,
  Lookup,
  Search,
  Filter,
  SubqueryFacetFilter,
  ArrayElementPropFacetFilter,
  FacetFilter,
  NumericFilter,
  DateTimeIntervalFilter,
  BooleanFilter,
  FieldHasTruthyValueFilter,
  PropExistsFilter,
  ArraySizeFilter,
  SearchRestrictons,
  DateIntervalBarChart,
  DateLineSingleChart,
  DateLineMultipleChart,
  QuantityByPeriodCalendarChart,
  TopNPieChart,
  DayOfWeekSummaryBarsChart,
  HourOfDaySummaryLineChart,
  SummaryTableChart,
  FieldStatsChart,
  TotalsBarColumn,
  TotalsBarChart,
  Chart
} from './types'

let mapIndexed = _.convert({ cap: false }).map
let arrayToObject = _.curry((key: string, val: any, arr: any[]) =>
  _.flow(_.keyBy(key), _.mapValues(val))(arr)
)

let makeContextForBeforeHooks = ({ app, params, collection }: { app: FeathersApp, params: FeathersContextParams, collection: string}) => ({
  params: { ...params, query: {} },
  type: 'before',
  method: 'find',
  service: app.service(collection),
  provider: params.provider,
  app,
})

let applyServiceRestrictions = ({ app, hooks }: { app: FeathersApp, hooks: FeathersServiceHooks }) => async (collection: string, params: FeathersContextParams) => {
  let beforeHooks = _.get(`${collection}.before`, hooks) || []
  let beforeContext = makeContextForBeforeHooks({ app, params, collection })
  
  for (let hook of beforeHooks) {
    beforeContext = (await hook(beforeContext)) || beforeContext
  }

  let beforeHookQueryProps = _.keys(beforeContext.params.query)
  let props = _.reject((prop: string) => _.includes(prop, ['$skip', '$limit']), beforeHookQueryProps)
  let beforeHookQuery = _.pick(props, beforeContext.params.query)

  return [{ $match: beforeHookQuery }]
}

let makeContextForAfterHooks = ({
    app,
    params,
    field,
    record
  }: {
    app: FeathersApp
    params: FeathersContextParams
    field?: string
    record: any
  }) => ({
  params,
  method: 'find',
  type: 'after',
  result: field ? record[field] : record,
  provider: params.provider,
  app,
})

let getAfterHookExecutor = ({ app, hooks }: { app: FeathersApp, hooks: FeathersServiceHooks}) => ({ collection, field, params }: { collection: string, field?: string, params: any}) => async (record: any) => {
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

let applyOffset = (endpoint: Date, offset: number): Date => addMinutes(endpoint, 0 - offset)

let intervals = {
  'Today': (date: Date, offset: number) => ({ from: applyOffset(startOfDay(applyOffset(date, offset)), 0 - offset) }),
  'Current Week': (date: Date, offset: number) => ({ from: applyOffset(startOfWeek(applyOffset(date, offset)), 0 - offset) }),
  'Current Month': (date: Date, offset: number) => ({ from: applyOffset(startOfMonth(applyOffset(date, offset)), 0 - offset) }),
  'Current Quarter': (date: Date, offset: number) => ({ from: applyOffset(startOfQuarter(applyOffset(date, offset)), 0 - offset) }),
  'Current Year': (date: Date, offset: number) => ({ from: applyOffset(startOfYear(applyOffset(date, offset)), 0 - offset) }),

  // no offest for these
  'Last Hour': (date: Date) => ({ from: addHours(date, -1) }),
  'Last Two Hours': (date: Date) => ({ from: addHours(date, -2) }),
  'Last Four Hours': (date: Date) => ({ from: addHours(date, -4) }),
  'Last Eight Hours': (date: Date) => ({ from: addHours(date, -8) }),
  'Last Twelve Hours': (date: Date) => ({ from: addHours(date, -12) }),
  'Last Day': (date: Date) => ({ from: addDays(date, -1) }),
  'Last Two Days': (date: Date) => ({ from: addDays(date, -2) }),
  'Last Three Days': (date: Date) => ({ from: addDays(date, -3) }),
  'Last Week': (date: Date) => ({ from: addDays(date, -7) }),
  'Last Two Weeks': (date: Date) => ({ from: addDays(date, -14) }),
  'Last Month': (date: Date) => ({ from: addMonths(date, -1) }),
  'Last Quarter': (date: Date) => ({ from: addQuarters(date, -1) }),
  'Last Year': (date: Date) => ({ from: addYears(date, -1) }),
  'Last Two Years': (date: Date) => ({ from: addYears(date, -1) }),

  'Previous Full Day': (date: Date, offset: number) => ({ from: applyOffset(addDays(startOfDay(applyOffset(date, offset)), -1), 0 - offset), to: applyOffset(startOfDay(applyOffset(date, offset)), 0 - offset) }),
  'Previous Full Week': (date: Date, offset: number) => ({ from: applyOffset(addWeeks(startOfWeek(applyOffset(date, offset)), -1), 0 - offset), to: applyOffset(startOfWeek(applyOffset(date, offset)), 0 - offset) }),
  'Previous Full Month': (date: Date, offset: number) => ({ from: applyOffset(addMonths(startOfMonth(applyOffset(date, offset)), -1), 0 - offset), to: applyOffset(startOfMonth(applyOffset(date, offset)), 0 - offset) }),
  'Previous Full Quarter': (date: Date, offset: number) => ({ from: applyOffset(addQuarters(startOfQuarter(applyOffset(date, offset)), -1), 0 - offset), to: applyOffset(startOfQuarter(applyOffset(date, offset)), 0 - offset) }),
  'Previous Full Year': (date: Date, offset: number) => ({ from: applyOffset(addYears(startOfYear(applyOffset(date, offset)), -1), 0 - offset), to: applyOffset(startOfYear(applyOffset(date, offset)), 0 - offset) }),
}

let intervalEndpoints = (interval: keyof typeof intervals, offset: number) => intervals[interval](new Date(), offset)

let typeFilters = {
  arrayElementPropFacet: ({ field, prop, idPath, values, isMongoId, exclude }: ArrayElementPropFacetFilter) =>
    _.size(values)
      ? [
          {
            $match: {
              [`${field}.${prop}${idPath ? '.' : ''}${idPath || ''}`]: {
                [`${exclude ? '$nin' : '$in'}`]:
                  _.size(values) && isMongoId
                    ? _.map((val: string) => new ObjectId(val), values)
                    : values,
              },
            },
          },
        ]
      : [],
  facet: ({ field, idPath, values, isMongoId, exclude }: FacetFilter) =>
    _.size(values)
      ? [
          {
            $match: {
              [`${field}${idPath ? '.' : ''}${idPath || ''}`]: {
                [`${exclude ? '$nin' : '$in'}`]:
                  _.size(values) && isMongoId
                    ? _.map((val: string) => new ObjectId(val), values)
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
  }: SubqueryFacetFilter) =>
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
  numeric: ({ field, from, to }: NumericFilter) =>
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
  dateTimeInterval: ({ field, from, to, interval, offset }: DateTimeIntervalFilter) => {
    if (interval) {
      let endpoints: { from?: Date, to?: Date } = intervalEndpoints(interval as keyof typeof intervals, offset || 0)
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
  boolean: ({ field, checked }: BooleanFilter) =>
    checked
      ? [
          {
            $match: { [field]: true },
          },
        ]
      : [],
  fieldHasTruthyValue: ({ field, checked }: FieldHasTruthyValueFilter) =>
    checked
      ? [
          {
            $match: { [field]: { $ne: null } },
          },
        ]
      : [],
  propExists: ({ field, negate }: PropExistsFilter) =>
    [
      {
        $match: { [field]: { $exists: !negate } },
      },
    ],
  arraySize: ({ field, from, to }: ArraySizeFilter) =>
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

let typeFilterStages = (subqueryValues = {}) => (filter: Filter) => typeFilters[filter.type as keyof typeof typeFilters]({ ...filter, subqueryValues: subqueryValues[filter.key as keyof typeof subqueryValues] } as any)

let getTypeFilterStages = (queryFilters: Filter[], subqueryValues: { [key: string]: any }) =>
  _.flatMap(typeFilterStages(subqueryValues), queryFilters)

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

let getFacets = (restrictions: SearchRestrictons, subqueryValues: { [k: string]: any[]}, filters: Filter[], collection: string) => {
  let facetFilters = _.reject((f: Filter) => _.includes(f.type, noResultsTypes), filters)
  let result: any = {}

  let restrictedTypeAggs: any = typeAggs(restrictions, subqueryValues)

  for (let filter of _.values(facetFilters)) {
    const resultForKey: any = restrictedTypeAggs[filter.type](filter, filters, collection, subqueryValues) as any
    result[filter.key] = resultForKey
  }

  return result
}

const fullDateGroup = (field: string, timezone: string) => ({
  year: { $year: { date: `$${field}`, timezone } },
  month: { $month: { date: `$${field}`, timezone } },
  week: { $week: { date: `$${field}`, timezone } },
  day: { $dayOfMonth: { date: `$${field}`, timezone } }
})

const timezoneOffset = (num: number) => {
  let sign = num < 0 ? '+' : '-' // reverse the offset received from the browser
  let abs = Math.abs(num)
  let hours = Math.floor(abs/60)
  let minutes = abs % 60
  let hoursString = `00${hours}`.substr(-2)
  let minutesString = `00${minutes}`.substr(-2)

  return `${sign}${hoursString}${minutesString}`
}

const periods = ['day', 'month', 'year']

const dateGroup = (field: string, period: string, offset: number) => {
  let dateGroupPick = _.slice(_.indexOf(period, periods), Infinity, periods)

  return _.pick(dateGroupPick, fullDateGroup(field, timezoneOffset(offset)))
}

const dateProject = (period: string) => {
  let dateGroupPick = _.slice(_.indexOf(period, periods), Infinity, periods)

  let [first, ...rest] = _.map((field: string) => `$_id.${field}`, dateGroupPick)
  let arr: any[] = [{ $toString: first }]
  let field: string

  while (field = rest.shift()) {
    arr.push('/', { $toString: field })
  }

  return { $concat: arr }
}

const dateProject2 = (period: string) => {
  let dateGroupPick = _.slice(_.indexOf(period, periods), Infinity, periods)

  let [first, ...rest] = _.map((field: string) => `$_id.${field}`, _.reverse(dateGroupPick))
  let arr: any[] = [{ $toString: first }]
  let field: string
  
  while (field = rest.shift()) {
    arr.push('-', { $toString: field })
  }

  return { $concat: arr }
}

const dateProjectMultiple = (period: string) => {
  let dateGroupPick = _.slice(_.indexOf(period, periods), Infinity, periods)

  let [first, ...rest] = _.map((field: string) => `$_id.date.${field}`, dateGroupPick)
  let arr: any[] = [{ $toString: first }]
  let field: string
  
  while (field = rest.shift()) {
    arr.push('/', { $toString: field })
  }

  return { $concat: arr }
}

const chartAggs = (restrictions: SearchRestrictons) => ({
  dateIntervalBars: ({ x, y, group, period }: DateIntervalBarChart) => [
    { $group: { _id: { [`${period}`]: { [`$${period}`]: `$${x}` }, group: `$${group}` }, value: { $sum: `$${y}` } } },
  ],
  dateLineSingle: ({ x, y, period, agg = 'sum', offset = 0 }: DateLineSingleChart) => [
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
  dateLineMultiple: ({ x, y, period, agg = 'sum', offset = 0, group, idPath }: DateLineMultipleChart) => [
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
  quantityByPeriodCalendar: ({ x, y, offset = 0 }: QuantityByPeriodCalendarChart) => [
    { $group: { _id: dateGroup(x, 'day', offset), value: { $sum: `$${y}` }, idx: { $min: `$${x}`} } },
    { $sort: { idx: 1 } },
    { $project: {
      _id: 0,
      day: dateProject2('day'),
      value: `$value`
    } },
  ],
  topNPie: ({ field, idPath, size = 10, unwind, lookup, include }: TopNPieChart) => [
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
          (include: string) => `lookup.${include}`,
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
          (include: string) => `labelValue.${include}`,
          _.constant(1)
        )(include)
      } : { labelValue: 1 }),
      value: `$count`
    } }
  ],
  dayOfWeekSummaryBars: ({ x, y, group, idPath, agg, offset = 0 }: DayOfWeekSummaryBarsChart) => [
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
  hourOfDaySummaryLine: ({ x, y, group, idPath, agg, offset = 0 }: HourOfDaySummaryLineChart) => [
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
  summaryTable: ({ rows, group, sortField, nameField }: SummaryTableChart) => [
    ..._.flow(
      _.filter('unwind'),
      _.map(({ key = '', field = '' }) => ({
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
        _.map(({ key = '', field = '', agg = '', unwind = false } ) => [key, { [`$${agg}`]: `$${unwind ? key : field}` }]),
        _.fromPairs,
      )(rows),
    } },
    ...(sortField ? [{ $sort: { [`${sortField}`]: -1 } }] : [])
  ],
  fieldStats: ({ unwind, field, idPath, statsField, include, page = 1, pageSize, sort, sortDir }: FieldStatsChart) => [
    ...(unwind ? [{ $unwind: { path: `$${unwind}`, preserveNullAndEmptyArrays: true  } }] : []),
    {
      $group: {
        _id: `$${field}${idPath ? '.' : ''}${idPath || ''}`,
        value: { $first: `$${field}` }, // need to use `valueInclude`
        count: { $sum: 1 },
        ..._.flow(
          _.map((stat: string) => [stat, { [`$${stat}`]: `$${statsField}`}]),
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
  totalsBar: ({ columns }: TotalsBarChart) => [
    { $group: {
      _id: null,
      ..._.flow(
        _.map(({ key, field, agg }: TotalsBarColumn) => [key || field, { [`$${agg === 'count' ? 'sum' : agg}`]: agg === 'count' ? 1 : `$${field}` }]),
        _.fromPairs
      )(columns)
    } }
  ]
})

const getChart = (restrictions: SearchRestrictons) => (type: keyof ReturnType<typeof chartAggs>): Function => (chartAggs(restrictions)[type])

let getCharts = (restrictions: SearchRestrictons, charts: Chart[]) => _.zipObject(_.map('key', charts), _.map((chart: Chart) => getChart(restrictions)(chart.type as keyof ReturnType<typeof chartAggs>)(chart), charts))

let lookupStages = (restrictions: SearchRestrictons, lookups: { [k: string]: Lookup }) => {
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

      let subqueryFilters: SubqueryFacetFilter[] = _.filter({ type: 'subqueryFacet' }, filters)
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

      let [restrictions, subqueryValues] = await Promise.all([
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

      let result = _.fromPairs(
        await Promise.all(
          mapIndexed(async (agg: MongoAggregation, key: string) => {
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
