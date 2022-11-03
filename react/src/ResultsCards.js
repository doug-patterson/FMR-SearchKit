import React from 'react'
import _ from 'lodash/fp'
import { mapIndexed } from './util'

// it will be more composable to make the action an override on some field and then
// get it in the right place on the card by ordering the includes. Right now the need
// for the handling of the extra "action" prop is janky

let DefaultCardBody = ({ row, include, displays, collection, UIComponents }) => (
  <>
    <UIComponents.CardHeader pad="small">{_.capitalize(row.type)}</UIComponents.CardHeader>
    <UIComponents.CardBody pad="small">
      <UIComponents.Grid columns="1fr">
        {_.map(
          field => (
            <div key={field}>
              {_.getOr(
                x => `${x}`,
                field === 'content'
                  ? [row.type, 'content']
                  : [collection, field],
                displays
              )(_.get(field === 'content' ? `content.content` : field, row), row)}
            </div>
          ),
          include
        )}
      </UIComponents.Grid>
    </UIComponents.CardBody>
  </>
)

export default ({
  include,
  collection,
  displays,
  rows,
  columns = `1fr 1fr`,
  action = _.noop,
  actionLabel = 'Remove',
  UIComponents
}) => (
  <UIComponents.Grid columns={columns} gap={10}>
    {mapIndexed(
      (row, idx) => (
        <UIComponents.Card key={row._id}>
          <DefaultCardBody
            row={row}
            rowIdx={idx}
            include={include}
            collection={collection}
            displays={displays}
            action={action}
          />
          <UIComponents.CardFooter
            style={{ display: 'flex', justifyContent: 'flex-end' }}
            pad="small"
          >
            <UIComponents.Button onClick={() => action(row)}>{actionLabel}</UIComponents.Button>
          </UIComponents.CardFooter>
        </UIComponents.Card>
      ),
      rows
    )}
  </UIComponents.Grid>
)
