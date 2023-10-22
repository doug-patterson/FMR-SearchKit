import { filter } from './propExists'
import { PropExistsFilter } from '../types'
import { Aggregator } from 'mingo/aggregator'
import 'mingo/init/system'

const someMembers = [
  { name: 'Fred', score: 27 },
  { name: 'Sally', score: 1 },
  { name: 'Mohan', score: 0 },
  { name: 'Mohan', score: {} },
  { name: 'Japa', score: null },
  { name: 'Lupe', score: '' },
  { name: 'Mohan' },

]

describe('propExists filter', () => {
  it('should select documets where the property in question exists on the record, no matter what the value', () => {
    const agg = filter({ key: 'test', type: 'propExists', field: 'score', checked: true } as PropExistsFilter)

    const result = new Aggregator(agg).run(someMembers)

    expect(result).toEqual([
      { name: 'Fred', score: 27 },
      { name: 'Sally', score: 1 },
      { name: 'Mohan', score: 0 },
      { name: 'Mohan', score: {} },
      { name: 'Japa', score: null },
      { name: 'Lupe', score: '' },
    ])
  })
  it('should apply the `negate` option correctly', () => {
    const agg = filter({ key: 'test', type: 'propExists', field: 'score', checked: true, negate: true } as PropExistsFilter)

    const result = new Aggregator(agg).run(someMembers)

    expect(result).toEqual([
      { name: 'Mohan' }
    ])
  })
})
