import React from 'react'
import _ from 'lodash/fp'

export let Box = ({ children, ...props }) => <div className="fmr-box" {...props}>{children}</div>

export let Grid = ({ children, rows, columns, areas, gap, ...props }) => <div
  className="fmr-grid"
  {...props}
  style={{
    gridTemplateAreas: areas,
    gridTemplateRows: rows,
    gridTemplateColumns: columns,
    ...(gap ? { columnGap: gap, rowGap: gap } : {}),
    ...props.style,
    display: 'grid'
  }}
>{children}</div>

export let Button = ({ onClick, children, ...props }) => <button className="fmr-button" onClick={onClick} {...props}>{children}</button>

export let Select = ({ value, options, onChange, ...props}) => <select
  className="fmr-select"
  onChange={e => e && e.target && onChange(e.target.value)}
  value={value}
  {...props}
>
  {_.map(option => <option
    key={option.value || option}
    value={option.value || option}
  >{option.label || option}</option>, options)}
</select>

export let Input = ({ type, value, placeholder, onChange, ...props}) => <input
  className="fmr-input"
  onChange={e => e && e.target && onChange(e.target.value)}
  placeholder={placeholder}
  value={value}
  type={type}
  {...props}
/>

export let CheckBox = ({ checked, label, onChange }) => {
  let id = _.uniqueId('fmr-checkbox-')

  return <>
    <input
      type="checkbox"
      checked={checked}
      id={id}
      className="fmr-checkbox"
      onChange={e => e.target && onChange(e.target.checked)}
    />
    <label htmlFor={id}>
      {label}
    </label>
  </>
} 

export let Menu = ({ label, open, items }) => {
  let [isOpen, setIsOpen] = React.useState(open)

  return <div
    className={`fmr-menu${open ? ' open': ''}`}
    style={{ position: 'relative' }}
  >
    <div onClick={() => setIsOpen(!isOpen)} className="fmr-menu-label">{label}</div>
    {isOpen && <div
      style={{
        position: 'absolute'
      }}
    >{_.map(item => <button onClick={item.onClick} className="fmr-button">{item.label}</button>, items)}</div>}
  </div>
}

export let Nav = ({ children, direction, ...props }) => <nav className="fmr-nav" style={{ display: 'flex', flexDirection: direction === 'row' ? 'row' : 'column' }} {...props}>{children}</nav>
export let NavItem = ({ label, onClick, icon }) => <div onClick={onClick} style={{ padding: '0 0.5rem' }}>{icon}{label}</div>

export let Table = ({ children, ...props }) => <table className="fmr-table" {...props}>{children}</table>
export let TableHeader = ({ children, ...props }) => <thead {...props}>{children}</thead>
export let TableBody = ({ children, ...props }) => <tbody {...props}>{children}</tbody>
export let TableRow = ({ children, ...props }) => <tr {...props}>{children}</tr>
export let TableCell = ({ children, ...props }) => <td {...props}>{children}</td>

export let Card = ({ children, ...props }) => <div className="fmr-card" {...props}>{children}</div>
export let CardHeader = ({ children, ...props }) => <div className="fmr-card-header" {...props}>{children}</div>
export let CardBody = ({ children, ...props }) => <div className="fmr-card-body" {...props}>{children}</div>
export let CardFooter = ({ children, ...props }) => <div className="fmr-card-footer" {...props}>{children}</div>

export let IconBack = () => <span style={{ fontSize: '1.5rem' }}>&#171;</span>
export let IconForward = () => <span style={{ fontSize: '1.5rem' }}>&#187;</span>
export let IconPrevious = () => <span style={{ fontSize: '1.5rem' }}>&#8249;</span>
export let IconNext = () => <span style={{ fontSize: '1.5rem' }}>&#8250;</span>



