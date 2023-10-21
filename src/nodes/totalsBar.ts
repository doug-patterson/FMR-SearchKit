import { TotalsBarColumn, TotalsBarChart, MongoAggregation } from '../types'
import _ from 'lodash/fp'

export const results = ({ columns }: TotalsBarChart): MongoAggregation => [
  {
    $group: {
      _id: null,
      ..._.flow(
        _.map(({ key, field, agg }: TotalsBarColumn) => [
          key || field,
          { [`$${agg === 'count' ? 'sum' : agg}`]: agg === 'count' ? 1 : `$${field}` }
        ]),
        _.fromPairs
      )(columns)
    }
  }
]
