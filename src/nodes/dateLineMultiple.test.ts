import { results } from './dateLineMultiple'
import { DateLineMultipleChart } from '../types'
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

describe('date line multiple chart', () => {
  it('should return data suitable for a multi-line chart', () => {
    const agg = results({ x: 'beganProgram', y: 'pointsScored', period: 'day', group: 'team' } as DateLineMultipleChart)

    const result = new Aggregator(agg).run(players)

    expect(result).toEqual([
      {
        "id": "Lions",
        "data": [
          {
            "x": "1/3/2020",
            "y": 128,
            "group": "Lions"
          },
          {
            "x": "2/3/2020",
            "y": 140,
            "group": "Lions"
          },
          {
            "x": "3/3/2020",
            "y": 244,
            "group": "Lions"
          },
          {
            "x": "4/3/2020",
            "y": 294,
            "group": "Lions"
          },
          {
            "x": "5/3/2020",
            "y": 17,
            "group": "Lions"
          }
        ]
      },
      {
        "id": "Bears",
        "data": [
          {
            "x": "1/3/2020",
            "y": 32,
            "group": "Bears"
          },
          {
            "x": "2/3/2020",
            "y": 81,
            "group": "Bears"
          },
          {
            "x": "3/3/2020",
            "y": 87,
            "group": "Bears"
          },
          {
            "x": "4/3/2020",
            "y": 1,
            "group": "Bears"
          }
        ]
      }
    ])
  })
})
