import React from 'react'
import _ from 'lodash/fp'

export default ({ title, checked, onChange, UIComponents }) => (
  <UIComponents.Card>
    <UIComponents.CardHeader>
      {_.startCase(title)}
    </UIComponents.CardHeader>
    <UIComponents.CardBody>
      <UIComponents.CheckBox
        checked={checked}
        label={'True'}
        onChange={checked => onChange({ checked })}
      />
    </UIComponents.CardBody>
  </UIComponents.Card>
)
