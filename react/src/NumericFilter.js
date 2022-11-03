import React from 'react'
import _ from 'lodash/fp'

export default ({ title, from, to, disableFrom, disableTo, onChange, UIComponents }) => (
  <UIComponents.Card>
    <UIComponents.CardHeader>
      {_.startCase(title)}
    </UIComponents.CardHeader>
    <UIComponents.CardBody>
      <UIComponents.Grid
        columns={_.join(' ', _.compact([!disableFrom && '1fr', !disableTo && '1fr']))}
        rows={'28px'}
      >
        {!disableFrom && (
          <UIComponents.Input
            type="number"
            value={from || ''}
            placeholder={'Min'}
            onChange={val => onChange({ from: val && _.toNumber(val) }) }
          />
        )}
        {!disableTo && (
          <UIComponents.Input
            type="number"
            value={to || ''}
            placeholder={'Max'}
            onChange={val => onChange({ to: val && _.toNumber(val) }) }
          />
        )}
      </UIComponents.Grid>
    </UIComponents.CardBody>
  </UIComponents.Card>
)
