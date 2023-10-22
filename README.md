# FMR-SearchKit
Build rich mongodb search interfaces with cross collection lookups while preserving the security and business logic in your FeathersJS hooks.

## The Problem
[FeathersJS](https://feathersjs.com/) imposes great structure on a web application project with the hooks system. In the before hooks for a service one can transform data, check permissions and perform other tasks appropriate before a DB operation, and in the after hooks one can perform side effects an implement further transformations.

Let's take _query restriction_ and _data redaction_ as examples of each. A service can have a before hook that restricts the user to their own records by adding their _id to the query. And anther service might have an after hook to redact passwords or other credentials.

Now what happens if you want to build a client side web page or app screen that uses the full power of the MongoDB aggregation framework as applied to your collections? This goes beyond simple CRUD operations as supported by [feathers-mongodb](https://feathersjs.com/api/databases/mongodb.html) and so you'll need to implement your own servce. But in this service you'll need to re-implement all of that same restriction and redaction logic, or you'll need to make sure your users have special permission to use the service. All of this leads to additional highly coupled code in your project, leading to maintainability problems and security leaks and eventually higher costs down the road.

## The Solution
This is what FMR-Searchkit is for. It's a drop-in solution for building a highly functional cross-collection search backed by a full MongoDB aggregation that re-uses the before and after hoooks from your feathers services while building the aggregations. Does your user service redact passwords in an after find hook? Searches of your user collection will too, right out of the box - whether user data appears as the main rows of your search as names on a facet filter. Does your personal_info collection restrict people to their own records in a before find hook? So will FMR-Searchkit searches of that collection, right out of the box. And so on.

## Installation
Install `fmr-searchkit` with your favorite package manager.

## Use
The module assumes the following
* In the folder for each service you will expose through the searchkit there is a standard Feathers `hooks` file
* In the each of the same folders there is a BSON schema file called `schema` for the Mongo collection backing the service.
* Make the db connection available at `app.mongodb.db` as below

In `app.ts`:

```
import mongodb, { ObjectId } from 'mongodb'
import searchkit from 'fmr-searchkit'

const client = await MongoClient.connect(/* your connection args */)

app.mongodb = { db: client.db() }

app.configure(
  searchkit({
    ObjectId,
    services: [
      'service1',
      'service2',
      'service3',
      'service4'
    ],
    servicesPath: `${__dirname}/services/`
  })
)
```

There are a couple of options as discussed below. You'll likely want to put some of your own hooks on the new `search` and `schema` services this creates, but other than that this is really it - you now have a highly functional search service that reuses all of the hooks you already have on your services.

## Configuration
This section is in progress

## API
The `search` service takes a `POST` or Feathers `create` with a JSON stringified object specifying the search. The properties of this object: 

| Prop            | Required | Default | Comment                                          |
| --------        | -------  | ------- | ---------                                        |
| `collection`    | true     | none    | base collection from which results will be drawn |
| `sortField`     | false    | none    | `asc` or `desc`                                  |
| `sortDir`       | false    | none    | `asc` or `desc`                                  |
| `page`          | false    | 1       | `asc` or `desc`                                  |
| `pageSize`      | false    | 100     | `asc` or `desc`                                  |
| `include`       | false    | none    | Returns all fields when unset                    |
| `includeSchema` | false    | false   | Return base schema with the response             |
| `filters`       | false    | none    | Array of `Filter`. See `./types`                 |
| `charts`        | false    | none    | Array of `Chart`. See `./types`                  |
| `lookup`        | false    | none    | Keyed object of `Lookup`. See `./types`          |
| `id`            | false    | none    | Not used server-side                             |

## Use
So how do you use it? This is where (FMR-Searchkit-React)[https://github.com/doug-patterson/FMR-SearchKit-React] comes in. It allows your to build a wide variety of client- and server-rendered searches with React 18 and full Next 13 support. As you can see here the searchkit ships now with about 20 filters and charts. Instructions on how to use them are at (FMR-Searchkit-React)[https://github.com/doug-patterson/FMR-SearchKit-React]

## A note on indexing, `$lookup` and denormalization
The sad truth is that Mongo shines on single collections when the queries have been anticipated with good indexing and struggles with lookups to other collections when you get beyond maybe 20,000 records, often in ways that defy indexing. So what's the point?

You should think of the `lookup` functionality here as a tool for prototying and offline layouts. There are plenty of applications in the course of a normal web development career where collections are small or users tolerate longer response times. Launching a new product or feature at all but the biggest companies would be an example, as would basic offline analytics functionality for employees.

When you need real performance you need to _denormalize_. Luckily the plan is simple: after every mutation of the records in your collection you run all necessary lookups and write the populated record to another collection. Just change your layout to search the new collection and your done. The live data stays small and queries of the denormalized data perfom. So the lifecycle is like this

* prototype and explore with free use of `lookup`.
* index properly for the layouts you actually build - all filters will need to be supported by an index and in fact indexes will increase eponentially in number with filters so choose wisely.
* when indexing still isn't getting you by drop in the `denormalizeAndUpsert` hook provided here and adjust the consuming searchkit layout to point at the new collection with appropriate adjustments.