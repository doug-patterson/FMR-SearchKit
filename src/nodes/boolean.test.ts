import { filter } from './boolean'
import { BooleanFilter } from '../types'
import { Aggregator } from 'mingo/aggregator'
import 'mingo/init/system'

const someMembers = [
  { name: 'Fred', isMember: false },
  { name: 'Sally', isMember: true },
  { name: 'Mohan', isMember: true },
]

describe('boolean filter', () => {
  it('should select documets with the passed in boolean at the specified field', () => {
    const agg = filter({ field: 'isMember', checked: true } as BooleanFilter)

    const result = new Aggregator(agg).run(someMembers)

    expect(result).toEqual([
      { name: 'Sally', isMember: true },
      { name: 'Mohanx', isMember: true },
    ])
  })
})
