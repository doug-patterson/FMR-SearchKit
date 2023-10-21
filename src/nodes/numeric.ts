import {
  NumericFilter,
  MongoAggregation
} from '../types'
import _ from 'lodash/fp'

export const filter = ({ field, from, to }: NumericFilter): MongoAggregation =>
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
      : []