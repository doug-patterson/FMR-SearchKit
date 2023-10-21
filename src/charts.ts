import _ from'lodash/fp'
import {
  SearchRestrictons,
  Chart
} from './types'
import { results as dateIntervalBars } from './nodes/dateIntervalBars'
import { results as dateLineSingle } from './nodes/dateLineSingle'
import { results as dateLineMultiple } from './nodes/dateLineMultiple'
import { results as quantityByPeriodCalendar } from './nodes/dateLineMultiple'
import { results as topNPie } from './nodes/topNPie'
import { results as dayOfWeekSummaryBars } from './nodes/dayOfWeekSummaryBars'
import { results as hourOfDaySummaryLine } from './nodes/hourOfDaySummaryLine'
import { results as summaryTable } from './nodes/summaryTable'
import { results as fieldStats } from './nodes/fieldStats'
import { results as totalsBar } from './nodes/totalsBar'

const chartAggs = (restrictions: SearchRestrictons) => ({
  dateIntervalBars,
  dateLineSingle,
  dateLineMultiple,
  quantityByPeriodCalendar,
  topNPie: topNPie(restrictions),
  dayOfWeekSummaryBars,
  hourOfDaySummaryLine,
  summaryTable,
  fieldStats,
  totalsBar
})

const getChart = (restrictions: SearchRestrictons) => (type: keyof ReturnType<typeof chartAggs>): Function => (chartAggs(restrictions)[type])

export const getCharts = (restrictions: SearchRestrictons, charts: Chart[]) => _.zipObject(_.map('key', charts), _.map((chart: Chart) => getChart(restrictions)(chart.type as keyof ReturnType<typeof chartAggs>)(chart), charts))
