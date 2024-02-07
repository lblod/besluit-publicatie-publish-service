import mu from 'mu';
import {uuid, sparqlEscapeString, sparqlEscapeUri, sparqlEscapeInt, sparqlEscapeDate, sparqlEscapeDateTime, sparqlEscapeBool } from 'mu';
import { querySudo as query, updateSudo as update } from '@lblod/mu-auth-sudo';
import { readFile } from 'fs';

const PENDING_STATUS = "http://mu.semte.ch/vocabularies/ext/besluit-publicatie-publish-service/status/pending";
const FAILED_STATUS = "http://mu.semte.ch/vocabularies/ext/besluit-publicatie-publish-service/status/failed";
const SUCCESS_STATUS = "http://mu.semte.ch/vocabularies/ext/besluit-publicatie-publish-service/status/success";

const IS_PUBLISHED_AGENDA = "http://mu.semte.ch/vocabularies/ext/publishesAgenda";
const IS_PUBLISHED_BEHANDELING = "http://mu.semte.ch/vocabularies/ext/publishesBehandeling";
const IS_PUBLISHED_BESLUITENLIJST = "http://mu.semte.ch/vocabularies/ext/publishesBesluitenlijst";
const IS_PUBLISHED_NOTULEN = "http://mu.semte.ch/vocabularies/ext/publishesNotulen";

async function getUnprocessedPublishedResources(graph, pendingTimeout, maxAttempts = 10){
  // put resources that already failed before at the end of queue
  let queryStr = `
    PREFIX  xsd:  <http://www.w3.org/2001/XMLSchema#>
    PREFIX sign: <http://mu.semte.ch/vocabularies/ext/signing/>
    PREFIX publicationStatus: <http://mu.semte.ch/vocabularies/ext/signing/publication-status/>
    PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
    PREFIX prov: <http://www.w3.org/ns/prov#>
     SELECT DISTINCT ?graph ?resource ?rdfaSnippet ?filePath ?status ?created ?numberOfRetries {
       VALUES ?graph { ${sparqlEscapeUri(graph)} }

       GRAPH ?graph {
         {
         ?resource a sign:PublishedResource;
                   <http://purl.org/dc/terms/created> ?created.
         }
         UNION {
            ?resource <http://mu.semte.ch/vocabularies/ext/besluit-publicatie-publish-service/number-of-retries> ?numberOfRetries.
         }
         UNION {
            ?resource <http://mu.semte.ch/vocabularies/ext/besluit-publicatie-publish-service/status> ?status.
         }
         UNION {
           ?resource sign:text ?content
           BIND(?content as ?rdfaSnippet).
         }
         UNION {
           ?resource prov:generated ?file.
           ?fileOnDisk nie:dataSource ?file.
           BIND(IRI(REPLACE(STR(?fileUrl), "share://", "/share/")) as ?filePath)
         }
         FILTER (
          (!BOUND(?status))

           ||

           (?status = <http://mu.semte.ch/vocabularies/ext/besluit-publicatie-publish-service/status/failed>) && ?numberOfRetries < ${sparqlEscapeInt(maxAttempts)}

           ||

           (?status = <http://mu.semte.ch/vocabularies/ext/besluit-publicatie-publish-service/status/pending>)
          )
      }
    } ORDER BY ASC(?numberOfRetries) ASC(?created)
  `;

  let res = await query(queryStr);
  let results = parseResult(res);
  for (const result of results) {
    if (!result.rdfaSnippet && result.filePath) {
      result.rdfaSnippet = await readFile(result.filePath);
    }
  }
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

async function persistExtractedData(triples, graph = "http://mu.semte.ch/graphs/public"){
  //we assume triples are expanded
  if(triples.length == 0){
    return;
  }

  graph = sparqlEscapeUri(graph);
  triples = applyEscapeFunctionData(triples);
  let insertedTriples = [];
  try {
    const resources = [...new Set(triples.filter(isAResource).map((t) => t.subject))];
    for (const r of resources) {
      await ensureUuidForResource(r, graph);
    }
    const subjects = [...new Set(triples.map((t) => t.subject))];
    for(const r of subjects){
      const resourceTriples = triples.filter(t => t.subject == r);
    await query(`
    INSERT DATA{
      GRAPH ${graph}{
        ${resourceTriples.map(t => `${t.subject} ${t.predicate} ${t.object}.`).join('\n')}
      }
    };
    `);
    await ensureUuidForResource(r, graph);
    insertedTriples = insertedTriples.concat(resourceTriples);
    }
  }
  catch(e) {
    console.error('error while trying to persist data!', e.message);
    console.info('persisted triples', JSON.stringify(insertedTriples));
    console.info('all triples', JSON.stringify(triples));
    throw e;
  }
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

async function insertZittingPermalinkQuery(zittingUri, graph = "http://mu.semte.ch/graphs/public"){
  let queryStr = `
      PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
      PREFIX mandaat: <http://data.vlaanderen.be/ns/mandaat#>
      PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
      PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
      PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
      PREFIX foaf: <http://xmlns.com/foaf/0.1/>
      PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
      
      INSERT {
        GRAPH ${sparqlEscapeUri(graph)} {
          ${sparqlEscapeUri(zittingUri)} foaf:page ?redirectUrl.
        }
      }
      WHERE {
          ${sparqlEscapeUri(zittingUri)} mu:uuid ?zittingUuid ;
              (besluit:isGehoudenDoor/mandaat:isTijdspecialisatieVan) ?administrativeUnit .
          ?administrativeUnit skos:prefLabel ?administrativeUnitFullName ;
              besluit:bestuurt ?bestuurseenheid .
          ?bestuurseenheid skos:prefLabel ?administrativeUnitName ;
              (besluit:classificatie/skos:prefLabel) ?administrativeUnitTypeName .
          BIND(
              CONCAT("/", ?administrativeUnitName, "/", ?administrativeUnitTypeName, "/zittingen/", ?zittingUuid)
              AS ?redirectUrl
          )
      }

  `;

  await query(queryStr);
}


async function ensureUuidForResource(escapedUri, escapedGraph ){
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
function applyEscapeFunctionData(triples){
  return triples.map(t => {
    let p =  predicateDataTypeEscapeMap( t.predicate );
    let escapedPredicate = 'a' == p.predicate ? 'a' : sparqlEscapeUri(t.predicate);
    try {
      return {subject: p.escapeSubjectF(t.subject), predicate: escapedPredicate, object: p.escapeObjectF(t.object) };
    }
    catch(e) {
      console.warn(`failed to convert triple ${t.subject} ${t.predicate} ${JSON.stringify(t.object)}`);
      throw e;
    }
  }).filter((e) => e);
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

const predicateDataTypeEscapeMap = function ( predicate ){
  let besluit = [
    { escapeSubjectF: sparqlEscapeUri, predicate: 'a', escapeObjectF: sparqlEscapeUri },
    { escapeSubjectF: sparqlEscapeUri, predicate: 'http://data.europa.eu/eli/ontology#description', escapeObjectF: sparqlEscapeString },
    { escapeSubjectF: sparqlEscapeUri, predicate: 'http://data.europa.eu/eli/ontology#title_short', escapeObjectF: sparqlEscapeString },
    { escapeSubjectF: sparqlEscapeUri, predicate: 'http://data.vlaanderen.be/ns/besluit#motivering', escapeObjectF: sparqlEscapeString },
    { escapeSubjectF: sparqlEscapeUri, predicate: 'http://data.europa.eu/eli/ontology#date_publication', escapeObjectF: sparqlEscapeDate },
    { escapeSubjectF: sparqlEscapeUri, predicate: 'http://data.europa.eu/eli/ontology#realizes', escapeObjectF: sparqlEscapeUri },
    { escapeSubjectF: sparqlEscapeUri, predicate: 'http://www.w3.org/ns/prov#wasGeneratedBy', escapeObjectF: sparqlEscapeUri },
    { escapeSubjectF: sparqlEscapeUri, predicate: 'http://data.europa.eu/eli/ontology#title', escapeObjectF: sparqlEscapeString },
    { escapeSubjectF: sparqlEscapeUri, predicate: 'http://data.europa.eu/eli/ontology#language', escapeObjectF: sparqlEscapeString },
    { escapeSubjectF: sparqlEscapeUri, predicate: 'http://data.europa.eu/eli/ontology#description', escapeObjectF: sparqlEscapeString },
    { escapeSubjectF: sparqlEscapeUri, predicate: 'http://data.europa.eu/eli/ontology#has_part', escapeObjectF: sparqlEscapeString },
    { escapeSubjectF: sparqlEscapeUri, predicate: 'http://www.w3.org/ns/prov#value', escapeObjectF: sparqlEscapeString },
    { escapeSubjectF: sparqlEscapeUri, predicate: 'http://www.w3.org/ns/prov#wasDerivedFrom', escapeObjectF: sparqlEscapeUri },
    { escapeSubjectF: sparqlEscapeUri, predicate: 'http://mu.semte.ch/vocabularies/ext/besluitPublicatieLinkedBesluit', escapeObjectF: sparqlEscapeUri },
    { escapeSubjectF: sparqlEscapeUri, predicate: 'http://data.europa.eu/eli/ontology#related_to', escapeObjectF: sparqlEscapeUri }
    ];

  let bvap = [
    { escapeSubjectF: sparqlEscapeUri, predicate: 'a', escapeObjectF: sparqlEscapeUri },
    { escapeSubjectF: sparqlEscapeUri, predicate: 'http://data.vlaanderen.be/ns/besluit#gebeurtNa', escapeObjectF: sparqlEscapeUri },
    { escapeSubjectF: sparqlEscapeUri, predicate: 'http://www.w3.org/ns/prov#generated', escapeObjectF: sparqlEscapeUri },
    { escapeSubjectF: sparqlEscapeUri, predicate: 'http://data.vlaanderen.be/ns/besluit#heeftAanwezige', escapeObjectF: sparqlEscapeUri },
    { escapeSubjectF: sparqlEscapeUri, predicate: 'http://purl.org/dc/terms/subject', escapeObjectF: sparqlEscapeUri },
    { escapeSubjectF: sparqlEscapeUri, predicate: 'http://data.vlaanderen.be/ns/besluit#heeftSecretaris', escapeObjectF: sparqlEscapeUri },
    { escapeSubjectF: sparqlEscapeUri, predicate: 'http://data.vlaanderen.be/ns/besluit#heeftStemming', escapeObjectF: sparqlEscapeUri },
    { escapeSubjectF: sparqlEscapeUri, predicate: 'http://data.vlaanderen.be/ns/besluit#heeftVoorzitter', escapeObjectF: sparqlEscapeUri },
    { escapeSubjectF: sparqlEscapeUri, predicate: 'http://data.vlaanderen.be/ns/besluit#openbaar', escapeObjectF: sparqlEscapeBoolWrapper },
    { escapeSubjectF: sparqlEscapeUri, predicate: 'http://schema.org/position', escapeObjectF: sparqlEscapeInt },
    { escapeSubjectF: sparqlEscapeUri, predicate: 'http://www.w3.org/ns/prov#wasDerivedFrom', escapeObjectF: sparqlEscapeUri },
    { escapeSubjectF: sparqlEscapeUri, predicate: 'http://mu.semte.ch/vocabularies/ext/besluitPublicatieLinkedBvap', escapeObjectF: sparqlEscapeUri }
  ];
  let stemmingen = [
    { escapeSubjectF: sparqlEscapeUri, predicate: 'a', escapeObjectF: sparqlEscapeUri },
    { escapeSubjectF: sparqlEscapeUri, predicate: 'http://data.vlaanderen.be/ns/besluit#onderwerp', escapeObjectF: sparqlEscapeString },
    { escapeSubjectF: sparqlEscapeUri, predicate: 'http://data.vlaanderen.be/ns/besluit#gevolg', escapeObjectF: sparqlEscapeString }
  ]
  let agendapunten = [
    { escapeSubjectF: sparqlEscapeUri, predicate: 'a', escapeObjectF: sparqlEscapeUri },
    { escapeSubjectF: sparqlEscapeUri, predicate: 'http://data.vlaanderen.be/ns/besluit#aangebrachtNa', escapeObjectF: sparqlEscapeUri },
    { escapeSubjectF: sparqlEscapeUri, predicate: 'http://purl.org/dc/terms/description', escapeObjectF: sparqlEscapeString },
    { escapeSubjectF: sparqlEscapeUri, predicate: 'http://data.vlaanderen.be/ns/besluit#geplandOpenbaar', escapeObjectF: sparqlEscapeBoolWrapper },
    { escapeSubjectF: sparqlEscapeUri, predicate: 'http://data.vlaanderen.be/ns/besluit#heeftOntwerpbesluit', escapeObjectF: sparqlEscapeUri },
    { escapeSubjectF: sparqlEscapeUri, predicate: 'http://purl.org/dc/terms/references', escapeObjectF: sparqlEscapeUri },
    { escapeSubjectF: sparqlEscapeUri, predicate: 'http://purl.org/dc/terms/title', escapeObjectF: sparqlEscapeString },
    { escapeSubjectF: sparqlEscapeUri, predicate: 'http://data.vlaanderen.be/ns/besluit#Agendapunt.type', escapeObjectF: sparqlEscapeUri },
    { escapeSubjectF: sparqlEscapeUri, predicate: 'http://schema.org/position', escapeObjectF: sparqlEscapeInt },
    { escapeSubjectF: sparqlEscapeUri, predicate: 'http://www.w3.org/ns/prov#wasDerivedFrom', escapeObjectF: sparqlEscapeUri },
    { escapeSubjectF: sparqlEscapeUri, predicate: 'http://data.vlaanderen.be/ns/besluit#behandelt', escapeObjectF: sparqlEscapeUri }
  ];

  let zitting = [
    { escapeSubjectF: sparqlEscapeUri, predicate: 'a', escapeObjectF: sparqlEscapeUri},
    { escapeSubjectF: sparqlEscapeUri, predicate: 'http://data.vlaanderen.be/ns/besluit#geplandeStart', escapeObjectF: sparqlEscapeDateTime},
    { escapeSubjectF: sparqlEscapeUri, predicate: 'http://www.w3.org/ns/prov#startedAtTime', escapeObjectF: sparqlEscapeDateTime},
    { escapeSubjectF: sparqlEscapeUri, predicate: 'http://data.vlaanderen.be/ns/besluit#isGehoudenDoor', escapeObjectF: sparqlEscapeUri},
    { escapeSubjectF: sparqlEscapeUri, predicate: 'http://data.vlaanderen.be/ns/besluit#behandelt', escapeObjectF: sparqlEscapeUri},
    { escapeSubjectF: sparqlEscapeUri, predicate: 'http://www.w3.org/ns/prov#endedAtTime', escapeObjectF: sparqlEscapeDateTime},
    { escapeSubjectF: sparqlEscapeUri, predicate: 'http://data.vlaanderen.be/ns/besluit#heeftAanwezigeBijStart', escapeObjectF: sparqlEscapeUri},
    { escapeSubjectF: sparqlEscapeUri, predicate: 'http://data.vlaanderen.be/ns/besluit#heeftNotulen', escapeObjectF: sparqlEscapeUri},
    { escapeSubjectF: sparqlEscapeUri, predicate: 'http://data.vlaanderen.be/ns/besluit#heeftSecretaris', escapeObjectF: sparqlEscapeUri},
    { escapeSubjectF: sparqlEscapeUri, predicate: 'http://data.vlaanderen.be/ns/besluit#heeftVoorzitter', escapeObjectF: sparqlEscapeUri},
    { escapeSubjectF: sparqlEscapeUri, predicate: 'http://data.vlaanderen.be/ns/besluit#heeftZittingsverslag', escapeObjectF: sparqlEscapeUri},
    { escapeSubjectF: sparqlEscapeUri, predicate: 'http://www.w3.org/ns/prov#atLocation', escapeObjectF: sparqlEscapeString },
    { escapeSubjectF: sparqlEscapeUri, predicate: 'http://www.w3.org/ns/prov#wasDerivedFrom', escapeObjectF: sparqlEscapeUri },
    { escapeSubjectF: sparqlEscapeUri, predicate: 'http://mu.semte.ch/vocabularies/ext/besluitenlijst', escapeObjectF: sparqlEscapeUri },
    { escapeSubjectF: sparqlEscapeUri, predicate: 'http://mu.semte.ch/vocabularies/ext/uittreksel', escapeObjectF: sparqlEscapeUri },
    { escapeSubjectF: sparqlEscapeUri, predicate: 'http://mu.semte.ch/vocabularies/ext/agenda', escapeObjectF: sparqlEscapeUri },
  ];

  let notulen = [
    { escapeSubjectF: sparqlEscapeUri, predicate: 'a', escapeObjectF: sparqlEscapeUri },
    { escapeSubjectF: sparqlEscapeUri, predicate: 'http://www.w3.org/ns/prov#value', escapeObjectF: sparqlEscapeString },
    { escapeSubjectF: sparqlEscapeUri, predicate: 'http://data.vlaanderen.be/ns/besluit#heeftNotulen', escapeObjectF: sparqlEscapeUri },
    { escapeSubjectF: sparqlEscapeUri, predicate: 'http://www.w3.org/ns/prov#wasDerivedFrom', escapeObjectF: sparqlEscapeUri }
  ];

  let besluitenlijst = [
    { escapeSubjectF: sparqlEscapeUri, predicate: 'a', escapeObjectF: sparqlEscapeUri },
    { escapeSubjectF: sparqlEscapeUri, predicate: 'http://mu.semte.ch/vocabularies/ext/besluitenlijstBesluit', escapeObjectF: sparqlEscapeUri},
    { escapeSubjectF: sparqlEscapeUri, predicate: 'http://www.w3.org/ns/prov#value', escapeObjectF: sparqlEscapeString },
    { escapeSubjectF: sparqlEscapeUri, predicate: 'http://data.europa.eu/eli/ontology#date_publication', escapeObjectF: sparqlEscapeDate },
  ];

  let uittreksel = [
    { escapeSubjectF: sparqlEscapeUri, predicate: 'a', escapeObjectF: sparqlEscapeUri },
    { escapeSubjectF: sparqlEscapeUri, predicate: 'http://mu.semte.ch/vocabularies/ext/uittrekselBvap', escapeObjectF: sparqlEscapeUri },
    { escapeSubjectF: sparqlEscapeUri, predicate: 'http://www.w3.org/ns/prov#value', escapeObjectF: sparqlEscapeString },
  ];

  let agenda =  [
    { escapeSubjectF: sparqlEscapeUri, predicate: 'a', escapeObjectF: sparqlEscapeUri },
    { escapeSubjectF: sparqlEscapeUri, predicate: 'http://mu.semte.ch/vocabularies/ext/agendaAgendapunt', escapeObjectF: sparqlEscapeUri },
    { escapeSubjectF: sparqlEscapeUri, predicate: 'http://www.w3.org/ns/prov#value', escapeObjectF: sparqlEscapeString },
  ];

  let allEscapeFunctions = [...besluit, ...bvap, ...agendapunten, ...zitting, ...notulen, ...besluitenlijst, ...uittreksel, ...agenda, ...stemmingen];

  return allEscapeFunctions.find(f => f.predicate == predicate);
};

function sparqlEscapeBoolWrapper(bool) {
  if (typeof bool === 'string') {
    return sparqlEscapeBool(bool == "true");
  }
  return sparqlEscapeBool(bool);
}

export { getUnprocessedPublishedResources,
         persistExtractedData,
         updateStatus,
         belongsToType,
         cleanUpResource,
         getRelationDataForZitting,
         insertZittingPermalinkQuery,
         PENDING_STATUS,
         FAILED_STATUS,
         SUCCESS_STATUS,
         IS_PUBLISHED_AGENDA,
         IS_PUBLISHED_BEHANDELING,
         IS_PUBLISHED_BESLUITENLIJST,
         IS_PUBLISHED_NOTULEN
       }
