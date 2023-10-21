import { filter } from './arrayElementPropFacet'
import { ArrayElementPropFacetFilter } from '../types'
import { Aggregator } from 'mingo/aggregator'
import 'mingo/init/system'
import _ from 'lodash/fp'

const folks = [
  { name: 'Fred', children: [{ name: 'Joe' }, { name: 'Hiro' }] },
  { name: 'Sally', children: [{ name: 'Panu' }, { name: 'Senyo' }]},
  { name: 'Mohan', children: [{ name: 'Randy' }] },
]

const query1: ArrayElementPropFacetFilter = { key: 'test', type: 'arrayElementPropFacet', field: 'children', prop: 'name', values: ['Randy', 'Hiro'] }

describe('boolean filter', () => {
  it('should select documets with the passed in boolean at the specified field', () => {
    const agg = filter(_.constant(1) as any)(query1)

    const result = new Aggregator(agg).run(folks)

    expect(result).toEqual([
      { name: 'Fred', children: [{ name: 'Joe' }, { name: 'Hiro' }] },
      { name: 'Mohan', children: [{ name: 'Randy' }] },
    ])
  })
})
