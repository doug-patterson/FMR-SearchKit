import { filter } from './arraySize'
import { ArraySizeFilter } from '../types'
import { Aggregator } from 'mingo/aggregator'
import 'mingo/init/system'

const authors = [
  { name: 'Fred', books: [{ title: 'a' }, { title: 'b' }, { title: 'c' }, { title: 'd' }, { title: 'e' }] },
  { name: 'Sally',  books: [{ title: 'a' }] },
  { name: 'Mohan', books: [{ title: 'a' }, { title: 'b' }, { title: 'c' }] },
]

describe('array size filter', () => {
  it('should select documents with arrays within the selected range', () => {
    const agg = filter({ field: 'books', from: 2, to: 4 } as ArraySizeFilter)

    const result = new Aggregator(agg).run(authors)

    expect(result).toEqual([
      { name: 'Mohan', books: [{ title: 'a' }, { title: 'b' }, { title: 'c' }] },
    ])
  })
  it('should select documents with arrays at `field` of the passed-in max size', () => {
    const agg = filter({ field: 'books', to: 3 } as ArraySizeFilter)

    const result = new Aggregator(agg).run(authors)

    expect(result).toEqual([
      { name: 'Sally',  books: [{ title: 'a' }] },
      { name: 'Mohan', books: [{ title: 'a' }, { title: 'b' }, { title: 'c' }] },
    ])
  })
  it('should select documents with arrays at `field` of the passed-in min size', () => {
    const agg = filter({ field: 'books', from: 4 } as ArraySizeFilter)

    const result = new Aggregator(agg).run(authors)

    expect(result).toEqual([
      { name: 'Fred', books: [{ title: 'a' }, { title: 'b' }, { title: 'c' }, { title: 'd' }, { title: 'e' }] },
    ])
  })
})
