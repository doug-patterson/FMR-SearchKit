import React from 'react'
import _ from 'lodash/fp'
import { mapIndexed } from './util'

// longer-term we need to refactor to not pass in collection
// and also only pass in the schema for the collection we're rendering

let moveColumn = ({ include, setInclude, field, increment }) => {
  let currentIdx = _.findIndex(f => f === field, include)
  let newIdx = currentIdx + increment
  let otherField = _.get([newIdx], include)
  let newInclude = _.flow(
    _.set([currentIdx], otherField),
    _.set([newIdx], field)
  )(include)

  setInclude(newInclude)
}

let schemaFields = _.flow(_.get('properties'), _.keys)

let getAddOpts = ({ include, schema, field, setInclude, setIsAdding }) => {
  let addableFields = _.difference(schemaFields(schema), include)
  return [
    ..._.map(
      f => ({
        label: _.startCase(f),
        onClick: () => {
          let idx = _.findIndex(ff => ff === field, include)
          setInclude([
            ..._.slice(0, idx, include),
            f,
            ..._.slice(idx, Infinity, include),
          ])
        },
      }),
      addableFields
    ),
    {
      label: 'None',
      onClick: () => setIsAdding(false),
    },
  ]
}

// setSortField, setSortDir

let HeaderMenu = ({
  field,
  include,
  setInclude,
  setSortField,
  setSortDir,
  schema,
  UIComponents
}) => {
  let [isAdding, setIsAdding] = React.useState(false)
  return (
    <UIComponents.Menu
      key={isAdding}
      label={isAdding ? 'Add Column' : _.startCase(field)}
      open={isAdding}
      items={_.compact(
        isAdding
          ? getAddOpts({
              include,
              schema,
              setInclude,
              field,
              setIsAdding,
              setSortField,
              setSortDir,
            })
          : [
              {
                label: 'Sort Ascending',
                onClick: () => {
                  setSortField(field)
                  setSortDir('asc')
                },
              },
              {
                label: 'Sort Descending',
                onClick: () => {
                  setSortField(field)
                  setSortDir('desc')
                },
              },
              _.findIndex(f => f === field, include) > 0 && {
                label: 'Move Left',
                onClick: () =>
                  moveColumn({ include, setInclude, field, increment: -1 }),
              },
              _.findIndex(f => f === field, include) < _.size(include) - 1 && {
                label: 'Move Right',
                onClick: () =>
                  moveColumn({ include, setInclude, field, increment: 1 }),
              },
              _.size(_.difference(schemaFields(schema), include)) && {
                label: 'Add Column',
                onClick: () => setIsAdding(true),
              },
              {
                label: 'Remove Column',
                onClick: () => {
                  let idx = _.findIndex(f => f === field, include)
                  setInclude([
                    ..._.slice(0, idx, include),
                    ..._.slice(idx + 1, Infinity, include),
                  ])
                },
              },
            ]
      )}
    />
  )
}

export default ({
  include,
  setInclude,
  setSortField,
  setSortDir,
  schema,
  collection,
  displays,
  rows,
  UIComponents
}) => (
  <UIComponents.Box style={{ width: '100%', overflowX: 'auto' }}>
    <UIComponents.Table className="results-table" key={_.uniqueId()}>
      <UIComponents.TableHeader style={{ borderTop: '1px solid #eee' }}>
        <UIComponents.TableRow elevation="medium">
          {_.map(
            field => (
              <UIComponents.TableCell key={field}>
                <HeaderMenu
                  {...{
                    field,
                    include,
                    setInclude,
                    setSortField,
                    setSortDir,
                    schema,
                    UIComponents
                  }}
                />
              </UIComponents.TableCell>
            ),
            include
          )}
        </UIComponents.TableRow>
      </UIComponents.TableHeader>
      <UIComponents.TableBody>
        {mapIndexed(
          (row, idx) => (
            <UIComponents.TableRow
              key={_.uniqueId()}
              id={row._id}
              style={idx % 2 === 0 ? { backgroundColor: '#eee' } : {}}
            >
              {_.map(
                field => (
                  <UIComponents.TableCell key={field}>
                    {_.getOr(_.identity, [collection, field], displays)(
                      _.get(field, row),
                      row,
                      idx
                    )}
                  </UIComponents.TableCell>
                ),
                include
              )}
            </UIComponents.TableRow>
          ),
          rows
        )}
      </UIComponents.TableBody>
    </UIComponents.Table>
  </UIComponents.Box>
)
