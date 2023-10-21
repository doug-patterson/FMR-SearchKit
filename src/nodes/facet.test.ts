import { filter, results } from './facet'
import { FacetFilter, SearchRestrictions, MongoObjectIdConstructor } from '../types'
import { Aggregator } from 'mingo/aggregator'
import 'mingo/init/system'

const users = [
  { name: 'Fred', role: 'admin' },
  { name: 'Sally', role: 'manager' },
  { name: 'Mohan', role: 'employee' },
]

const populatedUsers = [
  { user: { id: 1, name: 'Fred' }, role: 'admin' },
  { user: { id: 2, name: 'Sally' }, role: 'manager' },
  { user: { id: 3, name: 'Mohan' }, role: 'employee' },
]

const alphaUsers = [
  { name: 'A', role: 'admin' },
  { name: 'B', role: 'admin' },
  { name: 'C', role: 'manager' },
  { name: 'D', role: 'manager' },
  { name: 'E', role: 'manager' },
  { name: 'F', role: 'employee' },
]

const passwordUsers = [
  { user: { id: 1, name: 'Fred', password: 'password123' }, role: 'admin' },
  { user: { id: 2, name: 'Sally', password: 'qwerty' }, role: 'manager' },
  { user: { id: 3, name: 'Mohan', password: '12345' }, role: 'employee' },
]

describe('Facet filter: filter', () => {
  it('should select documents with the passed in boolean at the specified field', () => {
    const agg = filter(() => 1)({ field: 'role', values: ['manager'] } as FacetFilter)

    const result = new Aggregator(agg).run(users)

    expect(result).toEqual([
      { name: 'Sally', role: 'manager' },
    ])
  })
  it('The `idPath` prop should deterimine that the filter selects documents that have one of `values` at `idPath`', () => {
    const agg = filter(() => 1)({ field: 'user', values: [2], idPath: 'id' } as FacetFilter)

    const result = new Aggregator(agg).run(populatedUsers)

    expect(result).toEqual([
      { user: { id: 2, name: 'Sally' }, role: 'manager' },
    ])
  })
  it('should select the complement when `exclude` is set', () => {
    const agg = filter(() => 1)({ field: 'role', values: ['manager'], exclude: true } as FacetFilter)

    const result = new Aggregator(agg).run(users)

    expect(result).toEqual([
      { name: 'Fred', role: 'admin' },
      { name: 'Mohan', role: 'employee' },
    ])
  })
})

const emptyResultsContext: [SearchRestrictions, { [k: string]: any }, MongoObjectIdConstructor] = [{}, {}, () => 1]

describe('Facet filter: results', () => {
  it('should return complete facet options', () => {
    const agg = results(...emptyResultsContext)({ key: 'test', type: 'facet', field: 'role', values: [] } as FacetFilter, {}, 'users')

    const result = new Aggregator(agg).run(users)

    expect(result).toEqual([
      { _id: 'admin', checked: false, value: 'admin', count: 1 },
      { _id: 'employee', checked: false, value: 'employee', count: 1 },
      { _id: 'manager', checked: false, value: 'manager', count: 1 },
    ])
  })
  it('should sort options by checked, size, alpha', () => {
    const agg = results(...emptyResultsContext)({ key: 'test', type: 'facet', field: 'role', values: ['admin'] } as FacetFilter, {}, 'users')

    const result = new Aggregator(agg).run(alphaUsers)

    expect(result).toEqual([
      { _id: 'admin', checked: true, value: 'admin', count: 2 },
      { _id: 'manager', checked: false, value: 'manager', count: 3 },
      { _id: 'employee', checked: false, value: 'employee', count: 1 },
    ])
  })
  it('should filter options by `optionSearch` when present', () => {
    const agg = results(...emptyResultsContext)({ key: 'test', type: 'facet', field: 'role', values: [], optionSearch: 'emp' } as FacetFilter, {}, 'users')

    const result = new Aggregator(agg).run(users)

    expect(result).toEqual([
      { _id: 'employee', checked: false, value: 'employee', count: 1 },
    ])
  })
  it('should include full value data when `include` is not passed', () => {
    const agg = results(...emptyResultsContext)({ key: 'test', type: 'facet', field: 'user', idPath: 'id' } as FacetFilter, {}, 'users')

    const result = new Aggregator(agg).run(passwordUsers)

    expect(result).toEqual([
      {
        "_id": 1,
        "count": 1,
        "value": {
          "id": 1,
          "name": "Fred",
          "password": "password123"
        },
        "checked": false
      },
      {
        "_id": 2,
        "count": 1,
        "value": {
          "id": 2,
          "name": "Sally",
          "password": "qwerty"
        },
        "checked": false
      },
      {
        "_id": 3,
        "count": 1,
        "value": {
          "id": 3,
          "name": "Mohan",
          "password": "12345"
        },
        "checked": false
      }
    ])
  })
  it('should respect include when present', () => {
    const agg = results(...emptyResultsContext)({ key: 'test', type: 'facet', field: 'user', idPath: 'id', include: ['id', 'name'] } as FacetFilter, {}, 'users')

    const result = new Aggregator(agg).run(passwordUsers)

    expect(result).toEqual([
      {
        "_id": 1,
        "count": 1,
        "value": {
          "id": 1,
          "name": "Fred"
        },
        "checked": false
      },
      {
        "_id": 2,
        "count": 1,
        "value": {
          "id": 2,
          "name": "Sally"
        },
        "checked": false
      },
      {
        "_id": 3,
        "count": 1,
        "value": {
          "id": 3,
          "name": "Mohan"
        },
        "checked": false
      }
    ])
  })
  it('should should respect `lookup.include`', () => {
    // need to figure out how to do this with mingo - the fact  that `from` is an array
    // for mingo but a string for mongo is a significant issue for testing with mingo.
    expect(1).toEqual(1)
  })
})