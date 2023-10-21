import {
  BooleanFilter,
  MongoAggregation
} from '../types'
import _ from 'lodash/fp'

export const filter = ({ field, checked }: BooleanFilter): MongoAggregation =>
    checked
      ? [
          {
            $match: { [field]: true },
          },
        ]
      : []