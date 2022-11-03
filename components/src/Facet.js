import React from 'react'
import _ from 'lodash/fp'

export default ({ title, options, values, onChange, display, UIComponents }) => (
  <UIComponents.Card>
    <UIComponents.CardHeader>
      {_.startCase(title)}
    </UIComponents.CardHeader>
    <UIComponents.CardBody>
      <div style={{ display: 'flex', flexDirection: 'column' }}>
        {_.map(
          ({ _id, checked, count, lookup }) => (
            <div
              style={{ display: 'grid', gridTemplateColumns: 'auto 1fr 1fr' }}
              key={`${_id}-${checked ? 'checked' : 'unchecked'}`}
            >
              <UIComponents.CheckBox
                label={lookup ? display({ ...lookup, _id }) : _.startCase(_id)}
                checked={checked}
                onChange={checked => {
                  let newValues = checked
                    ? _.concat(values, _id)
                    : _.without([_id], values)
                  onChange({ values: newValues })
                }}
              />
              <span style={{ textAlign: 'right' }}>{count}</span>
            </div>
          ),
          options
        )}
      </div>
    </UIComponents.CardBody>
  </UIComponents.Card>
)
