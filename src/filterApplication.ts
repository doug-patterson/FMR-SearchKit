import _ from'lodash/fp'
import { ObjectId } from 'mongodb'
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
import {
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
} from './types'


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

export let getTypeFilterStages = (queryFilters: Filter[], subqueryValues: { [key: string]: any }) =>
  _.flatMap(typeFilterStages(subqueryValues), queryFilters)