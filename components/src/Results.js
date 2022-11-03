import React from 'react'
import Paginator from './Paginator'
import ResultsTable from './ResultsTable'
import _ from 'lodash/fp'

export default ({
  include,
  setInclude,
  setSortField,
  setSortDir,
  schema,
  collection,
  displays,
  rows,
  resultsCount,
  pageSize,
  setPageSize,
  page = 1,
  setPage,
  ResultsComponent = ResultsTable,
  UIComponents
}) => (
  <UIComponents.Box>
    <ResultsComponent
      {...{
        include,
        setInclude,
        setSortField,
        setSortDir,
        collection,
        schema,
        displays,
        rows,
        UIComponents
      }}
    />
    <UIComponents.Grid
      columns="auto 1fr auto"
      style={{ padding: 10, backgroundColor: '#f8f8f8' }}
    >
      <UIComponents.Select
        options={[10, 20, 50, 100]}
        value={pageSize}
        onChange={option => setPageSize(_.toNumber(option))}
        style={{ backgroundColor: 'white' }}
      />
      <div />
      <Paginator {...{ page, setPage, resultsCount, pageSize, UIComponents }} />
    </UIComponents.Grid>
  </UIComponents.Box>
)
