import _ from'lodash/fp'
import {
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
import { arrayToObject } from './util'



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

  while (field = rest.shift() || '') {
    arr.push('/', { $toString: field })
  }

  return { $concat: arr }
}

const dateProject2 = (period: string) => {
  let dateGroupPick = _.slice(_.indexOf(period, periods), Infinity, periods)

  let [first, ...rest] = _.map((field: string) => `$_id.${field}`, _.reverse(dateGroupPick))
  let arr: any[] = [{ $toString: first }]
  let field: string
  
  while (field = rest.shift() || '') {
    arr.push('-', { $toString: field })
  }

  return { $concat: arr }
}

const dateProjectMultiple = (period: string) => {
  let dateGroupPick = _.slice(_.indexOf(period, periods), Infinity, periods)

  let [first, ...rest] = _.map((field: string) => `$_id.date.${field}`, dateGroupPick)
  let arr: any[] = [{ $toString: first }]
  let field: string
  
  while (field = rest.shift() || '') {
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
      (val: any) => _.map(({ key = '', field = '' }) => ({
        $set: { [key]: { $reduce: {
          input: `$${field}`,
          initialValue: 0,
          in: { $sum: [ '$$value', '$$this' ] }
        } } }
      }))(val)
    )(rows),
    { $group: {
      _id: group ? `$${group}` : null,
      name: { $first: `$${nameField}` },
      ..._.flow(
        _.filter('agg'),
        (val: any) => _.map(({ key = '', field = '', agg = '', unwind = false } ) => [key, { [`$${agg}`]: `$${unwind ? key : field}` }])(val),
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
    { $sort: { [sort || _.first(include) as string]: sortDir === 'asc' ? 1 : -1 } },
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

export let getCharts = (restrictions: SearchRestrictons, charts: Chart[]) => _.zipObject(_.map('key', charts), _.map((chart: Chart) => getChart(restrictions)(chart.type as keyof ReturnType<typeof chartAggs>)(chart), charts))
