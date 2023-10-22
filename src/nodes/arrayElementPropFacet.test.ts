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

const agg1: ArrayElementPropFacetFilter = { key: 'test', type: 'arrayElementPropFacet', field: 'children', prop: 'name', values: ['Randy', 'Hiro'] }
const agg2 = { ...agg1, exclude: true }

describe('Array element prop facet filter', () => {
  it('should select documents with arrays at `field` that have elements where `prop` is one of `values`', () => {
    const agg = filter(_.constant(1) as any)(agg1)
    const result = new Aggregator(agg).run(folks)

    expect(result).toEqual([
      { name: 'Fred', children: [{ name: 'Joe' }, { name: 'Hiro' }] },
      { name: 'Mohan', children: [{ name: 'Randy' }] },
    ])
  })
  it('should select documents with arrays at `field` that have elements where `prop` is one of `values`', () => {
    const agg = filter(_.constant(1) as any)(agg2)
    const result = new Aggregator(agg).run(folks)

    expect(result).toEqual([
      { name: 'Sally', children: [{ name: 'Panu' }, { name: 'Senyo' }]},
    ])
  })
})
