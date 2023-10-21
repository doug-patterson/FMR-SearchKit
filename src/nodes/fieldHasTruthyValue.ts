import { FieldHasTruthyValueFilter, MongoAggregation } from '../types'

export const filter = ({ field, checked }: FieldHasTruthyValueFilter): MongoAggregation =>
  checked
    ? [
        {
          $match: { [field]: { $ne: null } }
        }
      ]
    : []
