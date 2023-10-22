import { filter } from './numeric'
import { NumericFilter } from '../types'
import { Aggregator } from 'mingo/aggregator'
import 'mingo/init/system'

const friends = [
  { name: 'Fred', age: 79 },
  { name: 'Sally', age: 44 },
  { name: 'Akiko', age: 55 },
  { name: 'Panu', age: 48 },
  { name: 'Chen', age: 23 },
  { name: 'Mohan', age: 19 },
]

describe('numeric filter', () => {
  it('should select documents `field` values within he selected range', () => {
    const agg = filter({ field: 'age', from: 40, to: 60 } as NumericFilter)

    const result = new Aggregator(agg).run(friends)

    expect(result).toEqual([
      { name: 'Sally', age: 44 },
      { name: 'Akiko', age: 55 },
      { name: 'Panu', age: 48 },
    ])
  })
  it('should select documents with `field` values below `to`', () => {
    const agg = filter({ field: 'age', to: 50 } as NumericFilter)

    const result = new Aggregator(agg).run(friends)

    expect(result).toEqual([
      { name: 'Sally', age: 44 },
      { name: 'Panu', age: 48 },
      { name: 'Chen', age: 23 },
      { name: 'Mohan', age: 19 },
    ])
  })
  it('should select documents with `field` values above `from`', () => {
    const agg = filter({ field: 'age', from: 50 } as NumericFilter)

    const result = new Aggregator(agg).run(friends)

    expect(result).toEqual([
      { name: 'Fred', age: 79 },
      { name: 'Akiko', age: 55 },
    ])
  })
})
