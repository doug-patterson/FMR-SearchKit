import { filter } from './fieldHasTruthyValue'
import { FieldHasTruthyValueFilter } from '../types'
import { Aggregator } from 'mingo/aggregator'
import 'mingo/init/system'

const someMembers = [
  { name: 'Jan', score: true },
  { name: 'Fred', score: 27 },
  { name: 'Sally', score: '17' },
  { name: 'Fred', score: { value: 17 } },
  { name: 'Sally', score: [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1] },
  { name: 'Mohan', score: 0 },
  { name: 'Mohan', score: false },
  { name: 'Japa', score: null },
  { name: 'Lupe', score: '' },
  { name: 'Mohan' },

]

describe('field has truthy value filter', () => {
  it('should select documets where the property in question has some value that evaluates to `true` in JS', () => {
    const agg = filter({ key: 'test', type: 'fieldHasTruthyValue', field: 'score', checked: true } as FieldHasTruthyValueFilter)

    const result = new Aggregator(agg).run(someMembers)

    expect(result).toEqual([
      { name: 'Jan', score: true },
      { name: 'Fred', score: 27 },
      { name: 'Sally', score: '17' },
      { name: 'Fred', score: { value: 17 } },
      { name: 'Sally', score: [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1] },
      { name: 'Mohan', score: 0 },
      { name: 'Mohan', score: false }, // this actually selects non-nil values and ought to be renamed
      { name: 'Lupe', score: '' },
    ])
  })
})
