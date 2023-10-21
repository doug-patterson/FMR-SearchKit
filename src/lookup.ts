import _ from'lodash/fp'
import {
  Lookup,
  SearchRestrictons,
} from './types'

import { arrayToObject } from './util'

export let lookupStages = (restrictions: SearchRestrictons, lookups: { [k: string]: Lookup }) => {
  let result = []
  for (let lookupName in lookups) {
    let {
      localField,
      foreignField,
      from,
      unwind,
      preserveNullAndEmptyArrays,
      include,
      isArray,
    } = lookups[lookupName]
    let stages = _.compact([
      {
        $lookup: {
          from,
          let: { localVal: `$${localField}` },
          pipeline: [
            ...restrictions[from],
            {
              $match: {
                $expr: {
                  [isArray ? '$in' : '$eq']: [`$${foreignField}`, '$$localVal'],
                },
              },
            },
          ],
          as: localField,
        },
      },
      unwind && {
        $unwind: {
          path: `$${localField}`,
          ...(preserveNullAndEmptyArrays
            ? { preserveNullAndEmptyArrays: true }
            : {}),
        },
      },
      include && {
        $project: arrayToObject(_.identity, _.constant(1))(include),
      },
    ])

    result.push(stages)
  }

  return result
}
