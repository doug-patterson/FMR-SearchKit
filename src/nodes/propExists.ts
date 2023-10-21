import {
  PropExistsFilter,
  MongoAggregation
} from '../types'

export const filter = ({ field, negate }: PropExistsFilter): MongoAggregation =>
    [
      {
        $match: { [field]: { $exists: !negate } },
      },
    ]