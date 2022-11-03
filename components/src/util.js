let _ = require('lodash/fp')

export let mapIndexed = _.convert({ cap: false }).map
export let mapValuesIndexed = _.convert({ cap: false }).mapValues
export let arrayToObject = _.curry((key, val, arr) =>
  _.flow(_.keyBy(key), _.mapValues(val))(arr)
)
