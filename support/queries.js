import mu from 'mu';
import {uuid, sparqlEscapeUri, sparqlEscapeString, sparqlEscapeInt, sparqlEscapeDate, sparqlEscapeDateTime } from 'mu';
import { querySudo as query, updateSudo as update } from './auth-sudo';

const PENDING_STATUS = "http://mu.semte.ch/vocabularies/ext/besluit-publicatie-publish-service/status/pending";
const FAILED_STATUS = "http://mu.semte.ch/vocabularies/ext/besluit-publicatie-publish-service/status/failed";
const SUCCESS_STATUS = "http://mu.semte.ch/vocabularies/ext/besluit-publicatie-publish-service/status/success";

const IS_PUBLISHED_AGENDA = "http://mu.semte.ch/vocabularies/ext/publishesAgenda";
const IS_PUBLISHED_BESLUITENLIJST = "http://mu.semte.ch/vocabularies/ext/publishesBesluitenlijst";
const IS_PUBLISHED_NOTULEN = "http://mu.semte.ch/vocabularies/ext/publishesNotulen";

async function getUnprocessedPublishedResources(pendingTimeout, maxAttempts = 10){
  let queryStr = `
    PREFIX sign: <http://mu.semte.ch/vocabularies/ext/signing/>
    PREFIX publicationStatus: <http://mu.semte.ch/vocabularies/ext/signing/publication-status/>

     SELECT DISTINCT ?graph ?resource ?rdfaSnippet ?status ?created ?numberOfRetries {
       GRAPH ?graph {
         ?resource a sign:PublishedResource;
                   <http://purl.org/dc/terms/created> ?created;
                   <http://mu.semte.ch/vocabularies/ext/signing/text> ?rdfaSnippet.

         OPTIONAL{
            ?resource <http://mu.semte.ch/vocabularies/ext/besluit-publicatie-publish-service/status> ?status.
         }

         OPTIONAL{
            ?resource <http://mu.semte.ch/vocabularies/ext/besluit-publicatie-publish-service/number-of-retries> ?numberOfRetries.
         }

        FILTER (
          (!BOUND(?status)

           ||

           (?status IN (<http://mu.semte.ch/vocabularies/ext/besluit-publicatie-publish-service/status/failed>) && ?numberOfRetries < ${sparqlEscapeInt(maxAttempts)})

           ||

           ?status IN (<http://mu.semte.ch/vocabularies/ext/besluit-publicatie-publish-service/status/pending>)
          )
        )
      }
    }
  `;

  let res = await query(queryStr);
  let results = parseResult(res);
  return results.filter(filterPendingTimeout(pendingTimeout));
}

async function belongsToType(resource, type){
  let queryStr = `
  SELECT DISTINCT ?doc {
    GRAPH ${sparqlEscapeUri(resource.graph)}{
      ${sparqlEscapeUri(resource.resource)} ${sparqlEscapeUri(type)} ?doc.
    }
  }`;
  let res =  await query(queryStr);
  res = parseResult(res);
  return res.length > 0;
}

async function updateStatus(resource, status, attempts = 1){
  let queryStr = `
     DELETE {
        GRAPH ${sparqlEscapeUri(resource.graph)} {
          ${sparqlEscapeUri(resource.resource)} <http://mu.semte.ch/vocabularies/ext/besluit-publicatie-publish-service/status> ?status.
          ${sparqlEscapeUri(resource.resource)} <http://mu.semte.ch/vocabularies/ext/besluit-publicatie-publish-service/number-of-retries> ?retries.
       }
     }
     WHERE {
       GRAPH ${sparqlEscapeUri(resource.graph)} {
         OPTIONAL { ${sparqlEscapeUri(resource.resource)} <http://mu.semte.ch/vocabularies/ext/besluit-publicatie-publish-service/status> ?status. }
         OPTIONAL { ${sparqlEscapeUri(resource.resource)} <http://mu.semte.ch/vocabularies/ext/besluit-publicatie-publish-service/number-of-retries> ?retries. }
       }
     }

     ;

     INSERT DATA{
        GRAPH ${sparqlEscapeUri(resource.graph)} {
          ${sparqlEscapeUri(resource.resource)} <http://mu.semte.ch/vocabularies/ext/besluit-publicatie-publish-service/status> ${sparqlEscapeUri(status)}.
          ${sparqlEscapeUri(resource.resource)} <http://mu.semte.ch/vocabularies/ext/besluit-publicatie-publish-service/number-of-retries> ${sparqlEscapeInt(attempts)}.
       }
     }
  `;

  await query(queryStr);
};

async function persistExtractedData(triples, escapeFunctionsData, graph = "http://mu.semte.ch/graphs/public"){
  //we assume triples are expanded
  if(triples.length == 0){
    return;
  }

  graph = sparqlEscapeUri(graph);
  triples = applyEscapeFunctionData(triples, escapeFunctionsData);

  let resources = triples.filter(isAResource);

  for(const r of resources){
    await getUuidForResource(r.subject, graph);
  }

  let triplesStr = triples.map(t => `${t.subject} ${t.predicate} ${t.object}.`).join('\n');
  let insertStr = `
    INSERT DATA{
      GRAPH ${graph}{
        ${triplesStr}
      }
    };
  `;

  await query(insertStr);

}

async function cleanUpResource(uri, exceptionList = [], graph = "http://mu.semte.ch/graphs/public"){
  let queryStr = `
      DELETE {
        GRAPH ${sparqlEscapeUri(graph)}{
          ${sparqlEscapeUri(uri)} ?p ?o.
        }
      } WHERE {
          GRAPH ${sparqlEscapeUri(graph)}{
            ${sparqlEscapeUri(uri)} ?p ?o.
            FILTER(?p NOT IN (${ exceptionList.map(uri => sparqlEscapeUri(uri)).join(', ') }))
          }
      }
  `;
  await query(queryStr);
};

async function getRelationDataForZitting(zittingUri, relationUri, graph = "http://mu.semte.ch/graphs/public"){
  let queryStr = `
       SELECT DISTINCT ?o
       WHERE {
          GRAPH ${sparqlEscapeUri(graph)}{
            ${sparqlEscapeUri(zittingUri)} ${sparqlEscapeUri(relationUri)} ?o.
          }
      }
  `;
  let res = await query(queryStr);
  return parseResult(res);
};


async function getUuidForResource(escapedUri, escapedGraph ){
  let queryStr = `
       PREFIX  mu:  <http://mu.semte.ch/vocabularies/core/>
       SELECT DISTINCT ?uuid{
         GRAPH ${escapedGraph}{
            ${escapedUri} mu:uuid ?uuid.
         }
       }
  `;

  let exUuid = parseResult(await query(queryStr))[0];
  if(exUuid) return exUuid.uuid;

  exUuid = uuid();
  let insertStr = `
       PREFIX  mu:  <http://mu.semte.ch/vocabularies/core/>
       INSERT DATA{
         GRAPH ${escapedGraph} {
            ${escapedUri} mu:uuid ${sparqlEscapeString(exUuid)}.
         }
       }
  `;

  await query(insertStr);

  return exUuid;
};

/*************************************************************
 * HELPERS
 *************************************************************/
function applyEscapeFunctionData(triples, escapeData){
  return triples.map(t => {
    let p =  escapeData.find(p => p.predicate == t.predicate);
    let escapedPredicate = 'a' == p.predicate?'a':sparqlEscapeUri(t.predicate);
    return {subject: p.escapeSubjectF(t.subject), predicate: escapedPredicate, object: p.escapeObjectF(t.object) };
  });
}

function isAResource(triple){
  return triple.predicate == 'a';
};

/**
 * convert results of select query to an array of objects.
 * courtesy: Niels Vandekeybus & Felix
 * @method parseResult
 * @return {Array}
 */
const parseResult = function( result ) {
  const bindingKeys = result.head.vars;
  return result.results.bindings.map((row) => {
    const obj = {};
    bindingKeys.forEach((key) => obj[key] = row[key]?row[key].value:undefined);
    return obj;
  });
};

const filterPendingTimeout = function( timeout ) {
  return (resource) => {

    if(resource.status !== 'http://mu.semte.ch/vocabularies/ext/besluit-publicatie-publish-service/status/pending')
      return true;

    let modifiedDate = new Date(resource.created);
    let currentDate = new Date();
    return ((currentDate - modifiedDate) / (1000 * 60 * 60)) >= parseInt(timeout);
  };
};

export { getUnprocessedPublishedResources,
         persistExtractedData,
         updateStatus,
         belongsToType,
         cleanUpResource,
         getRelationDataForZitting,
         PENDING_STATUS,
         FAILED_STATUS,
         SUCCESS_STATUS,
         IS_PUBLISHED_AGENDA,
         IS_PUBLISHED_BESLUITENLIJST,
         IS_PUBLISHED_NOTULEN
       }
