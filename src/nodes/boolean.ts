import {
  BooleanFilter,
  MongoAggregation
} from '../types'

export const filter = ({ field, checked }: BooleanFilter): MongoAggregation =>
    checked
      ? [
          {
            $match: { [field]: true },
          },
        ]
      : []