import React from 'react'
import * as DefaultUIComponents from './DefaultUIComponents'

import _ from 'lodash/fp'
import { mapIndexed, mapValuesIndexed, arrayToObject } from './util'

import BooleanFilter from './BooleanFilter'
import Facet from './Facet'
import NumericFilter from './NumericFilter'
import Results from './Results'

let NoComponent = () => 'no filter found'
let Hidden = () => ''
let getFilterComponent = type =>
  ({
    none: NoComponent,
    facet: Facet,
    arrayElementPropFacet: Facet,
    hidden: Hidden,
    numeric: NumericFilter,
    boolean: BooleanFilter,
    arraySize: NumericFilter,
  }[type || 'none'])

let updateFilters = (filters, setFilters) => idx => patch =>
  setFilters([
    ..._.slice(0, idx, filters),
    mapValuesIndexed(
      (v, k) => (_.has(k, patch) ? _.get(k, patch) : v),
      _.clone(filters[idx])
    ),
    ..._.slice(idx + 1, Infinity, filters),
  ])

let getLocalStorageSearch = (storageKey, searchVersion) => {
  let item = localStorage.getItem(storageKey)
  let search
  if (item) {
    let data = JSON.parse(item)
    if (data.searchVersion >= searchVersion) {
      search = data.search
    }
  }

  return search
}

export default ({
  collection,
  initialSearch,
  displays,
  children,
  storageKey,
  searchVersion,
  UIComponents: ThemeComponents,
  schemas,
  execute
}) => {
  let UIComponents = _.defaults(DefaultUIComponents, ThemeComponents)
  let schema = schemas[collection]
  if (!schema) {
    return 'Schema not found'
  }
  storageKey = storageKey || collection

  let localStorageSearch =
    _.isNumber(searchVersion) && getLocalStorageSearch(storageKey, searchVersion)

  // at some point improve performance by keeping a bunch of these
  // settings on a single object so several can be updated in a single render
  let [sortField, setSortField] = React.useState(
    _.getOr('createdAt', 'sortField', localStorageSearch || initialSearch)
  )
  let [sortDir, setSortDir] = React.useState(
    _.getOr('desc', 'sortDir', localStorageSearch || initialSearch)
  )
  let [include, setInclude] = React.useState(
    _.getOr(
      _.keys(schema.properties),
      'include',
      localStorageSearch || initialSearch
    )
  )

  let [page, setPage] = React.useState(_.getOr(1, 'page', initialSearch))
  let [pageSize, setPageSize] = React.useState(
    _.getOr(20, 'pageSize', localStorageSearch || initialSearch)
  )
  let [filters, setFilters] = React.useState(
    (localStorageSearch || initialSearch).filters
  )
  let [filterOptions, setFilterOptions] = React.useState(
    _.map(_.pick('key'), initialSearch.filters)
  )
  let [rows, setRows] = React.useState([])
  let [resultsCount, setResultsCount] = React.useState('')
  let updateFilter = updateFilters(filters, setFilters)

  React.useEffect(async () => {
    let search = {
      collection,
      filters,
      sortField,
      sortDir,
      include,
      page,
      pageSize,
      lookup: initialSearch.lookup,
    }

    localStorage.setItem(
      storageKey,
      JSON.stringify({
        searchVersion,
        search,
      })
    )

    let { results, resultsCount, ...filterResults } = await execute({
        ...search,
        include: _.concat(include, initialSearch.omitFromResults),
      })
    setRows(results)
    let newFilterOptions = _.map(
      ({ key }) => ({
        key,
        options: filterResults[key],
      }),
      filters
    )
    setResultsCount(_.getOr(0, 'count', _.first(resultsCount)))
    setFilterOptions(newFilterOptions)
  }, [sortField, sortDir, include, page, pageSize, filters])

  return (
    <UIComponents.Box>
      <UIComponents.Grid columns="1fr 5fr" gap={10}>
        <UIComponents.Box>
          {children}
          {mapIndexed((filter, idx) => {
            let Component = getFilterComponent(filter.type)
            return (
              <Component
                key={filter.key}
                onChange={patch => {
                  console.log('ONCHANGE')
                  console.log(patch)
                  updateFilter(idx)(patch)
                  setPage(1)
                }}
                title={filter.key}
                {...filter}
                options={_.get(
                  'options',
                  _.find({ key: filter.key }, filterOptions)
                )}
                display={displays[collection][filter.field]}
                UIComponents={UIComponents}
              />
            )
          }, filters)}
          <UIComponents.Button
            onClick={() => {
              setSortField(initialSearch.sortField)
              setSortDir(initialSearch.sortDir)
              setFilters(initialSearch.filters)
              setPageSize(initialSearch.pageSize)
              setInclude(
                _.getOr(_.keys(schema.properties), 'include', initialSearch)
              )
            }}
          >Reset Search</UIComponents.Button>
        </UIComponents.Box>
        <UIComponents.Box style={{ overflowY: 'scroll', paddingBottom: 120 }}>
          <Results
            {...{
              include: _.without(initialSearch.omitFromResults, include),
              setInclude,
              setSortField,
              setSortDir,
              schema: {
                ...schema,
                properties: _.flow(
                  _.keys,
                  keys => _.difference(keys, initialSearch.omitFromResults),
                  arrayToObject(_.identity, k => schema.properties[k])
                )(schema.properties),
              },
              collection,
              displays,
              rows,
              resultsCount,
              pageSize,
              setPageSize,
              page,
              setPage,
              UIComponents
            }}
          />
        </UIComponents.Box>
      </UIComponents.Grid>
    </UIComponents.Box>
  )
}
