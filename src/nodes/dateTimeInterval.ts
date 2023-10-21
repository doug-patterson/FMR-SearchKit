import {
  DateTimeIntervalFilter,
  MongoAggregation
} from '../types'
import _ from 'lodash/fp'
import {
  addMinutes,
} from 'date-fns'
import { Interval, intervalEndpoints } from '../util'

export const filter = ({ field, from, to, interval, offset }: DateTimeIntervalFilter): MongoAggregation => {
  if (interval) {
    const endpoints: { from?: Date, to?: Date } = intervalEndpoints(interval as Interval, offset || 0)
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
}