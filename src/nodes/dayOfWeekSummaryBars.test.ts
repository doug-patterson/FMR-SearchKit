import { results } from './dayOfWeekSummaryBars'
import { DayOfWeekSummaryBarsChart } from '../types'
import { Aggregator } from 'mingo/aggregator'
import 'mingo/init/system'

const players = [
  { beganProgram: new Date('March 1, 2020'), pointsScored: 11, team: 'Lions' },
  { beganProgram: new Date('March 1, 2020'), pointsScored: 32, team: 'Bears' },
  { beganProgram: new Date('March 1, 2020'), pointsScored: 4, team: 'Lions' },
  { beganProgram: new Date('March 1, 2020'), pointsScored: 113, team: 'Lions' },
  { beganProgram: new Date('March 2, 2020'), pointsScored: 91, team: 'Lions' },
  { beganProgram: new Date('March 2, 2020'), pointsScored: 67, team: 'Bears' },
  { beganProgram: new Date('March 2, 2020'), pointsScored: 14, team: 'Bears' },
  { beganProgram: new Date('March 2, 2020'), pointsScored: 18, team: 'Lions' },
  { beganProgram: new Date('March 2, 2020'), pointsScored: 31, team: 'Lions' },
  { beganProgram: new Date('March 3, 2020'), pointsScored: 57, team: 'Lions' },
  { beganProgram: new Date('March 3, 2020'), pointsScored: 18, team: 'Lions' },
  { beganProgram: new Date('March 3, 2020'), pointsScored: 87, team: 'Bears' },
  { beganProgram: new Date('March 3, 2020'), pointsScored: 95, team: 'Lions' },
  { beganProgram: new Date('March 3, 2020'), pointsScored: 31, team: 'Lions' },
  { beganProgram: new Date('March 3, 2020'), pointsScored: 43, team: 'Lions' },
  { beganProgram: new Date('March 4, 2020'), pointsScored: 118, team: 'Lions' },
  { beganProgram: new Date('March 4, 2020'), pointsScored: 98, team: 'Lions' },
  { beganProgram: new Date('March 4, 2020'), pointsScored: 1, team: 'Bears' },
  { beganProgram: new Date('March 4, 2020'), pointsScored: 78, team: 'Lions' },
  { beganProgram: new Date('March 5, 2020'), pointsScored: 4, team: 'Lions' },
  { beganProgram: new Date('March 5, 2020'), pointsScored: 0, team: 'Lions' },
  { beganProgram: new Date('March 5, 2020'), pointsScored: 8, team: 'Lions' },
  { beganProgram: new Date('March 5, 2020'), pointsScored: 5, team: 'Lions' },
]

describe('day of week summary bar chart', () => {
  it('should return data suitable for a multi-line chart', () => {
    const agg = results({ x: 'beganProgram', y: 'pointsScored', period: 'day', group: 'team', agg: 'sum' } as DayOfWeekSummaryBarsChart)

    const result = new Aggregator(agg).run(players)

    expect(result).toEqual([
      {
        "Lions": 128,
        "Bears": 32,
        "id": "Sunday"
      },
      {
        "Lions": 140,
        "Bears": 81,
        "id": "Monday"
      },
      {
        "Lions": 244,
        "Bears": 87,
        "id": "Tuesday"
      },
      {
        "Lions": 294,
        "Bears": 1,
        "id": "Wednesday"
      },
      {
        "Lions": 17,
        "id": "Thursday"
      }
    ])
  })
})
