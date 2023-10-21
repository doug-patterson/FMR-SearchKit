import {
  ArraySizeFilter,
  MongoAggregation
} from '../types'
import _ from 'lodash/fp'

export const filter = ({ field, from, to }: ArraySizeFilter): MongoAggregation =>
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
      : []