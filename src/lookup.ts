import _ from 'lodash/fp'
import { Lookup, SearchRestrictons } from './types'

import { arrayToObject } from './util'

export const lookupStages = (restrictions: SearchRestrictons, lookups: { [k: string]: Lookup }) => {
  const result = []
  for (const lookupName in lookups) {
    const { localField, foreignField, from, unwind, preserveNullAndEmptyArrays, include, isArray } =
      lookups[lookupName]
    const stages = _.compact([
      {
        $lookup: {
          from,
          let: { localVal: `$${localField}` },
          pipeline: [
            ...restrictions[from],
            {
              $match: {
                $expr: {
                  [isArray ? '$in' : '$eq']: [`$${foreignField}`, '$$localVal']
                }
              }
            }
          ],
          as: localField
        }
      },
      unwind && {
        $unwind: {
          path: `$${localField}`,
          ...(preserveNullAndEmptyArrays ? { preserveNullAndEmptyArrays: true } : {})
        }
      },
      include && {
        $project: arrayToObject(_.identity, _.constant(1))(include)
      }
    ])

    result.push(stages)
  }

  return result
}
