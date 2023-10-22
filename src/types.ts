interface BaseFilter {
  key: string
  type: string
  field: string
}

export interface BooleanFilter extends BaseFilter {
  checked: boolean
}

export interface FieldHasTruthyValueFilter extends BooleanFilter {
  negate: boolean
}

export interface PropExistsFilter extends BaseFilter {
  negate?: boolean
}

export interface ArraySizeFilter extends BaseFilter {
  from: number
  to: number
}

export interface NumericFilter extends BaseFilter {
  from?: number
  to?: number
}

export interface DateTimeIntervalFilter extends BaseFilter {
  from?: Date
  to?: Date
  interval?: string
  offset?: number
}

export interface FacetFilter extends BaseFilter {
  idPath?: string
  values?: any[]
  isMongoId?: boolean
  exclude?: boolean
  optionSearch?: string
  size?: number
  include?: string[]
  lookup?: {
    from: string
    foreignField: string
    include: string[]
  }
}

export interface ArrayElementPropFacetFilter extends FacetFilter {
  prop: string
}

export interface SubqueryFacetFilter extends FacetFilter {
  subqueryCollection: string
  subqueryKey: string
  optionsAreMongoIds: boolean
  subqueryField: string
  subqueryFieldIdPath: string
  subqueryIdPath: string
  subqueryFieldIsArray: boolean
  subqueryLocalField: string
  subqueryLocalIdPath: string
  include: string[]
  optionSearch: string
  subqueryValues: any[]
}

export type Filter =
  | BooleanFilter
  | FieldHasTruthyValueFilter
  | PropExistsFilter
  | ArraySizeFilter
  | NumericFilter
  | DateTimeIntervalFilter
  | FacetFilter
  | ArrayElementPropFacetFilter
  | SubqueryFacetFilter

interface BaseChart {
  key: string
  type: string
}

export interface DateIntervalBarChart extends BaseChart {
  x: string
  y: string
  group: string
  period: string
}

export interface DateLineSingleChart extends BaseChart {
  x: string
  y: string
  period: string
  agg: string
  offset: number
}

export interface DateLineMultipleChart extends BaseChart {
  x: string
  y: string
  period: string
  agg: string
  offset?: number
  group?: string
  idPath?: string
}

export interface QuantityByPeriodCalendarChart extends BaseChart {
  x: string
  y: string
  offset: number
}

export interface TopNPieChart extends BaseChart {
  field: string
  idPath: string
  size: number
  unwind: boolean
  lookup: {
    from: string
    unwind: boolean
    foreignField: string
    include: string[]
  }
  include: string[]
}

export interface DayOfWeekSummaryBarsChart extends BaseChart {
  x: string
  y: string
  period: string
  agg: string
  offset?: number
  group: string
  idPath?: string
}

export interface HourOfDaySummaryLineChart extends BaseChart {
  x: string
  y: string
  period: string
  agg: string
  offset: number
  group: string
  idPath: string
}

export interface SummaryTableChart extends BaseChart {
  rows: string[]
  group: string
  sortField: string
  nameField: string
}

export interface FieldStatsChart extends BaseChart {
  unwind: boolean
  field: string
  idPath: string
  statsField: string
  include: string[]
  page: number
  pageSize: number
  sort: string
  sortDir: string
}

export interface TotalsBarColumn {
  key: string
  field: string
  agg: string
}

export interface TotalsBarChart extends BaseChart {
  columns: TotalsBarColumn[]
}

export type Chart =
  | DateIntervalBarChart
  | DateLineSingleChart
  | DateLineMultipleChart
  | QuantityByPeriodCalendarChart
  | TopNPieChart
  | DayOfWeekSummaryBarsChart
  | HourOfDaySummaryLineChart
  | SummaryTableChart
  | FieldStatsChart
  | TotalsBarChart

export interface Lookup {
  from: string
  localField: string
  foreignField: string
  as: string
  isArray: boolean
  isMongoId: boolean
  inlcude: string[]
  unwind: boolean
  preserveNullAndEmptyArrays: boolean
  include: string[]
}

// we'll import proper Feathers types when we upgrade to Feathers 5
export type FeathersApp = any
export type FeatherService = any
export type FeathersContext = any
export type FeathersContextParams = any
export type FeathersServiceHooks = any

// we can improve this to roughly AggregationNode[] || { [k: string]: AggregationNode }
// but the code will need to be updated to actually test for the cases
export type MongoAggregation = any

// someday. needs to cover all the cases, enums, objects, arrays etc
type BsonSchemaProp = any

// again, nice-to-have
export type MongoObjectIdConstructor = any

export interface BsonSchema {
  bsonType: string
  required?: string[]
  additionalProperties?: boolean
  properties: BsonSchemaProp[]
}

export interface Search {
  id?: string
  collection: string
  sortField?: string
  sortDir?: 'asc' | 'desc'
  include?: string[]
  page: number
  pageSize: number
  filters: Filter[]
  charts: Chart[]
  lookup: { [key: string]: Lookup }
  includeSchema: boolean
}

export interface SearchRestrictions {
  [k: string]: MongoAggregation
}

export type LodashIteratee = any
