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

## Inner workings
Takes a published resource as input, tries to extract data from it. Removes old data.
Assummes all snippets contain a zitting and bestuursorgaan.

## TODOS
* ! The service does not work with SEAS. (frequenst socket hang up error.) Needs investigation. Now map on virtuoso !
*  some general cleanup/documenting