const _ = require('lodash/fp')

export let mapIndexed = _.convert({ cap: false }).map

export let arrayToObject = _.curry((key: string, val: any, arr: any[]) =>
  _.flow(_.keyBy(key), _.mapValues(val))(arr)
)
