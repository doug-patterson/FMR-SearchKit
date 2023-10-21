import _ from'lodash/fp'
import {
  FeathersApp,
  FeathersContextParams,
  FeathersServiceHooks,
} from './types'

let makeContextForBeforeHooks = ({ app, params, collection }: { app: FeathersApp, params: FeathersContextParams, collection: string}) => ({
  params: { ...params, query: {} },
  type: 'before',
  method: 'find',
  service: app.service(collection),
  provider: params.provider,
  app,
})

export let applyServiceRestrictions = ({ app, hooks }: { app: FeathersApp, hooks: FeathersServiceHooks }) => async (collection: string, params: FeathersContextParams) => {
  let beforeHooks = _.get(`${collection}.before`, hooks) || []
  let beforeContext = makeContextForBeforeHooks({ app, params, collection })
  
  for (let hook of beforeHooks) {
    beforeContext = (await hook(beforeContext)) || beforeContext
  }

  let beforeHookQueryProps = _.keys(beforeContext.params.query)
  let props = _.reject((prop: string) => _.includes(prop, ['$skip', '$limit']), beforeHookQueryProps)
  let beforeHookQuery = _.pick(props, beforeContext.params.query)

  return [{ $match: beforeHookQuery }]
}

let makeContextForAfterHooks = ({
    app,
    params,
    field,
    record
  }: {
    app: FeathersApp
    params: FeathersContextParams
    field?: string
    record: any
  }) => ({
  params,
  method: 'find',
  type: 'after',
  result: field ? record[field] : record,
  provider: params.provider,
  app,
})

export let getAfterHookExecutor = ({ app, hooks }: { app: FeathersApp, hooks: FeathersServiceHooks}) => ({ collection, field, params }: { collection: string, field?: string, params: any}) => async (record: any) => {
  let afterContext = makeContextForAfterHooks({
    app,
    params,
    field,
    record,
  })

  let afterHooks = _.get(`${collection}.after`, hooks) || []

  for (let hook of afterHooks) {
    afterContext = (await hook(afterContext)) || afterContext
  }
  return _.flow(_.castArray, _.first)(_.get('result', afterContext))
}