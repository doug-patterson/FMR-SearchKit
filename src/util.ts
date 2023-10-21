import _ from 'lodash/fp'
import {
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
} from 'date-fns'
import { LodashIteratee } from './types'

export const arrayToObject = _.curry((key: LodashIteratee, val: LodashIteratee, arr: any[]): { [k: string]: any } =>
  _.flow((v: string): any => _.keyBy(key, v), (obj: any): any => _.mapValues(val, obj))(arr as any)
)

export const periods = ['day', 'month', 'year']

export const timezoneOffset = (num: number) => {
  const sign = num < 0 ? '+' : '-' // reverse the offset received from the browser
  const abs = Math.abs(num)
  const hours = Math.floor(abs/60)
  const minutes = abs % 60
  const hoursString = `00${hours}`.substr(-2)
  const minutesString = `00${minutes}`.substr(-2)

  return `${sign}${hoursString}${minutesString}`
}

export const fullDateGroup = (field: string, timezone: string) => ({
  year: { $year: { date: `$${field}`, timezone } },
  month: { $month: { date: `$${field}`, timezone } },
  week: { $week: { date: `$${field}`, timezone } },
  day: { $dayOfMonth: { date: `$${field}`, timezone } }
})

export const dateGroup = (field: string, period: string, offset: number) => {
  const dateGroupPick = _.slice(_.indexOf(period, periods), Infinity, periods)

  return _.pick(dateGroupPick, fullDateGroup(field, timezoneOffset(offset)))
}

export const dateProject = (period: string) => {
  const dateGroupPick = _.slice(_.indexOf(period, periods), Infinity, periods)

  const [first, ...rest] = _.map((field: string) => `$_id.${field}`, dateGroupPick)
  const arr: any[] = [{ $toString: first }]
  let field: string

  // eslint-disable-next-line
  while (field = rest.shift() || '') {
    arr.push('/', { $toString: field })
  }

  return { $concat: arr }
}

export const dateProject2 = (period: string) => {
  const dateGroupPick = _.slice(_.indexOf(period, periods), Infinity, periods)

  const [first, ...rest] = _.map((field: string) => `$_id.${field}`, _.reverse(dateGroupPick))
  const arr: any[] = [{ $toString: first }]
  let field: string
  
  // eslint-disable-next-line
  while (field = rest.shift() || '') {
    arr.push('-', { $toString: field })
  }

  return { $concat: arr }
}

const applyOffset = (endpoint: Date, offset: number): Date => addMinutes(endpoint, 0 - offset)

const intervals = {
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

export type Interval = keyof typeof intervals

export const intervalEndpoints = (interval: keyof typeof intervals, offset: number) => intervals[interval](new Date(), offset)

