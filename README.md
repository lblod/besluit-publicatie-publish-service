# besluit-publicatie-publish-service

This service polls for [signing:PublishedResource](http://mu.semte.ch/vocabularies/ext/signing/PublishedResource), a derived resource from gelinkt-notuleren notulen.
The service extracts data and maps it to applicatieprofiel [Besluit Publicatie](https://data.vlaanderen.be/doc/applicatieprofiel/besluit-publicatie/)

## Rest API

For debugging:

- `POST /publish-tasks`: starts publishing tasks# besluit-publicatie-publish-service

## Params

```
besluit-publicatie:
    image: lblod/besluit-publicatie-publish-service:z.y.x
    environment: #defaults params are shown here
    PENDING_TIMEOUT_HOURS: "3"
    CACHING_CRON_PATTERN: "0 */5 * * * *"
    links:
      - virtuoso:database
```

## Example delta notifier config

```
export default [
  {
    match: {
      // form of element is {subject,predicate,object}
      object: { type: "uri", value: "http://mu.semte.ch/vocabularies/ext/signing/PublishedResource" }
    },
    callback: {
      url: "http://besluit-publicatie/publish-tasks", method: "POST"
    },
    options: {
      resourceFormat: "v0.0.1",
      gracePeriod: 1000,
      ignoreFromSelf: true
    }
  }
]
```

## Inner workings

Takes a published resource as input, tries to extract data from it. Removes old data.
Assummes all snippets contain a zitting and bestuursorgaan.

## model

### PublishedResource

#### Class

`ext:PublishedResource`

#### Properties

| Name                | Predicate                                                  | Range | Definition                 |
| ------------------- | ---------------------------------------------------------- | ----- | -------------------------- |
| `created`           | `purl:created`                                             |       |                            |
| `number-of-retries` | `ext:besluit-publicatie-publish-service/number-of-retries` |       |                            |
| `status`            | `ext:besluit-publicatie-publish-service/status`            |       |                            |
| `content`           | `sign:text`                                                |       | The published html snippet |

## TODOS

- some general cleanup/documenting
