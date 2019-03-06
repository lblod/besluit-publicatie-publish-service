import {findFirstNodeOfType, findAllNodesOfType} from '@lblod/marawa/dist/dom-helpers';
import rdfaDomDocument from './rdfa-dom-document';
import { analyse, resolvePrefixes } from '@lblod/marawa/dist/rdfa-context-scanner';
import { getRelationDataForZitting, persistExtractedData, belongsToType, cleanUpResource, IS_PUBLISHED_AGENDA, IS_PUBLISHED_BESLUITENLIJST, IS_PUBLISHED_NOTULEN } from './queries';
import {uuid, sparqlEscapeUri, sparqlEscapeString, sparqlEscapeInt, sparqlEscapeDate, sparqlEscapeDateTime, sparqlEscapeBool } from 'mu';
import crypto from 'crypto';

/**
 * Main entry point for extraction of data.
 * Assumes
 * ------
 *  - All Rdfa snippets contain a Zitting
 *  - Latest published snippet, is truth. E.g. a spoedeisended agenda will overrule data from aanvullende agenda
*   - All extracted resources, are linked to a Zitting.
      This is an exstension on the AP. Because users can publish besluitenlijst before publising agenda. The same for behandeling van agendapunt
 **/
async function startPipeline(resourceToPublish){
  let doc = new rdfaDomDocument(resourceToPublish.rdfaSnippet);
  let triples = flatTriples(doc.getTopDomNode()); //let's not make an assumption about how the document is structured. But, might explode memory?
  triples = preProcess(triples);

  await insertZitting(triples, resourceToPublish);
  await insertAgendaPunten(triples, resourceToPublish);
  await insertBvap(triples, resourceToPublish);
  await insertBesluiten(triples, resourceToPublish);
  await insertNotulen(triples, resourceToPublish);
};

/**
 * Extracts besluiten from triples and saves them.
 * What is does
 * ------------
 * 1. checks if published resource is a besluitenlijst
 * 2. extracts all ?p ?o (the ones we want) from the besluiten
 * 3. links all besluiten to zitting.
 * 4. links also besluiten to published resource
 * 5. some postprocessing
 * 6. cleaning obsolete deltas. E.g a snippet removes some besluiten. We want it (for now) reflected in DB
 * 7. cleans the triples which need update values
 * 8. saves it
 *
 * Alls the insert* functions have roughly the same structure. Won't document them here.
 */
async function insertBesluiten(triples, resourceToPublish){
  if(!(await belongsToType(resourceToPublish, IS_PUBLISHED_BESLUITENLIJST))){
    return;
  }
  let data = getBesluiten(triples);
  let linkBesP = "http://mu.semte.ch/vocabularies/ext/besluitPublicatieLinkedBesluit";
  linkToZitting(data.trs, triples, linkBesP);
  linkToPublishedResource(data.trs, resourceToPublish.resource);
  data.trs = postProcess(data.trs);

  await persistExtractedData(data.trs, data.poi);
}

async function insertBvap(triples, resourceToPublish){
  if(!(await belongsToType(resourceToPublish, IS_PUBLISHED_BESLUITENLIJST))){
    return;
  }

  //we really need it to be uri here
  let trs = triples.filter(t =>
                           (t.predicate !== 'http://data.vlaanderen.be/ns/besluit#gebeurtNa') ||
                           isURI(t.object));
  let linkBvapP = "http://mu.semte.ch/vocabularies/ext/besluitPublicatieLinkedBvap";
  let data = getBvap(trs);

  linkToZitting(data.trs, triples, linkBvapP);
  linkToPublishedResource(data.trs, resourceToPublish.resource);
  data.trs = postProcess(data.trs);
  data.trs = orderGebeurtNa(data.trs,
                       'http://data.vlaanderen.be/ns/besluit#BehandelingVanAgendapunt',
                       'http://data.vlaanderen.be/ns/besluit#gebeurtNa');

  await persistExtractedData(data.trs, data.poi);
};

async function insertZitting(triples, resourceToPublish){
  let data = getZittingResource(triples);
  linkToPublishedResource(data.trs, resourceToPublish.resource);
  data.trs = postProcess(data.trs);

  await persistExtractedData(data.trs, data.poi);
};

async function insertAgendaPunten(triples, resourceToPublish){
  if(!(await belongsToType(resourceToPublish, IS_PUBLISHED_AGENDA))){
    return;
  }

  //keep on preprocessing. We really need this predicate to point to a uri
  let trs = triples.filter(t =>
                           (t.predicate !== 'http://data.vlaanderen.be/ns/besluit#aangebrachtNa') ||
                           isURI(t.object));

  let data = getAgendaPunten(trs);
  let linkBP = "http://data.vlaanderen.be/ns/besluit#behandelt";
  linkToZitting(data.trs, triples, linkBP);
  linkToPublishedResource(data.trs, resourceToPublish.resource);
  data.trs = postProcess(data.trs);
  data.trs = orderGebeurtNa(data.trs);

  await persistExtractedData(data.trs, data.poi);
};

async function insertNotulen(triples, resourceToPublish){
  if(!(await belongsToType(resourceToPublish, IS_PUBLISHED_NOTULEN))){
    return;
  }
  //Make stable uri.
  let zitting = triples.find(isAZitting);
  let subject = `http://data.lblod.info/vocabularies/lblod/notulen/${await hashStr(zitting.subject)}`;
  let trs = [];
  trs.push({subject, predicate: "a", object: `http://mu.semte.ch/vocabularies/ext/Notulen`});
  trs.push({subject, predicate: 'http://www.w3.org/ns/prov#value', object: resourceToPublish.rdfaSnippet});
  linkToZitting(trs, triples, "http://data.vlaanderen.be/ns/besluit#heeftNotulen");
  linkToPublishedResource(trs, resourceToPublish.resource);
  trs = postProcess(trs);

  let poi = [ { escapeSubjectF: sparqlEscapeUri, predicate: 'a', escapeObjectF: sparqlEscapeUri },
              { escapeSubjectF: sparqlEscapeUri, predicate: 'http://www.w3.org/ns/prov#value', escapeObjectF: sparqlEscapeString },
              { escapeSubjectF: sparqlEscapeUri, predicate: 'http://data.vlaanderen.be/ns/besluit#heeftNotulen', escapeObjectF: sparqlEscapeUri },
              { escapeSubjectF: sparqlEscapeUri, predicate: 'http://www.w3.org/ns/prov#wasDerivedFrom', escapeObjectF: sparqlEscapeUri }
            ];

  await persistExtractedData(trs, poi);
}

/*************************************************************
 * HELPERS
 *************************************************************/

function isAZitting(triple){
  return triple.predicate == 'a' && triple.object == 'http://data.vlaanderen.be/ns/besluit#Zitting';
};

/*
 * If new set of resources contains less resources then the previous version, remove them here.
 */
async function cleanupDeltaRelationsToZitting(zittingTriple, zittingProperty, newTriples){
  let res = await getRelationDataForZitting(zittingTriple.subject, zittingProperty);
  let uris = res.map(d => d.o);
  let obsoleteUris = uris.filter(uri => !newTriples.find(t => t.subject == uri));
  for(const uri of obsoleteUris){
    await cleanUpResource(uri);
  }
};

/*
 * A bunch of triples could need an update, first remove their previous values
 */
async function batchCleanupBeforeUpdate(triples, type, exceptionList){
  let subjectUris = new Set(triples.filter(t => t.predicate == 'a' && t.object == type).map(t => t.subject));
  for(const uri of subjectUris){
    await cleanUpResource(uri, exceptionList);
  }
};

/*
 * Adds order to bvap and agendapunten (AP extension)
 */
function orderGebeurtNa(triples, type = 'http://data.vlaanderen.be/ns/besluit#Agendapunt',
                        gebeurtNa = 'http://data.vlaanderen.be/ns/besluit#aangebrachtNa'){
  //written for Agendapunten, works on behandeling van agendapunten too.
  //assumes AP's are a list.
  //assumes no duplicates
  let orderedPunten = [];
  let orderAps = triples.filter(t => t.predicate == gebeurtNa);

  if(orderAps.length == 0) return triples;

  //find first agendapunt as uri
  let ap1 = triples
        .filter(e => e.predicate == 'a' && e.object == type)
        .map(t => t.subject)
        .find(t => !orderAps.map(t => t.subject).find(uri => uri == t));

  if(!ap1) return triples;

  let currIndex = 0;
  let currAp = ap1;

  triples.push({subject: ap1 , predicate: 'http://schema.org/position' , object: currIndex });

  while(currIndex < orderAps.length){
    let nextAp = orderAps.find(t => t.object == currAp);
    currIndex += 1;
    triples.push({subject: nextAp.subject , predicate: 'http://schema.org/position' , object: currIndex });
    currAp = nextAp.subject;
  }

  return triples;
};

/*
 * links resources to zitting
 */
function linkToZitting(preparedTriples, origTriples, predicate){
  let zitting = origTriples.find(isAZitting);
  let resources = preparedTriples.filter(t => t.predicate == 'a');
  resources.forEach(t => {
    preparedTriples.push({subject: zitting.subject, predicate: predicate, object: t.subject});
  });
  return preparedTriples;
}

/*
 * links resources to publishedResource
 */
function linkToPublishedResource(preparedTriples, resourceUri){
  let predicate = 'http://www.w3.org/ns/prov#wasDerivedFrom';
  let resources = preparedTriples.filter(t => t.predicate == 'a'); //extract the types
    resources.forEach(t => {
      preparedTriples.push({subject: t.subject, predicate, object: resourceUri});
    });
  return preparedTriples;
}

/*
 * encapsulate some preprocessing, e.g. remapping stuff etc
 */
function preProcess(triples){
  //remap triples (for backwards compatibilty, will be deleted one day)
  let remapP = {'http://data.vlaanderen.be/ns/besluit#heeftAgendapunt': 'http://data.vlaanderen.be/ns/besluit#behandelt'};
  triples =  triples.map(t =>  {
    if(remapP[t.predicate]){
      t.predicate = remapP[t.predicate];
    }
    return t;
  });
  return triples;
};

/*
 * encapsulate some post processing, e.g removing duplicates
 */
function postProcess(triples){
  //remove duplicates
  let cleanedT = [];
  for(const ot of triples){
    let existingT = cleanedT.find( t => JSON.stringify(t) === JSON.stringify(ot) ); //fair enough
    if(!existingT){
      cleanedT.push(ot);
    }
  }
  return cleanedT;
}

/*
 * flatten output from contextscanner
 */
function flatTriples(node){
  let contexts = analyse( node ).map((c) => c.context);
  return contexts.reduce((acc, e) => { return [ ...acc, ...e]; }, []);
}

/*
 * checks if URI (stolen from SO)
 */
function isURI(str) {
  //Fuck this: see https://stackoverflow.com/a/45567717/1092608
  var pattern = new RegExp('^((ft|htt)ps?:\\/\\/)?'+ // protocol
  '((([a-z\\d]([a-z\\d-]*[a-z\\d])*)\\.)+[a-z]{2,}|'+ // domain name and extension
  '((\\d{1,3}\\.){3}\\d{1,3}))'+ // OR ip (v4) address
  '(\\:\\d+)?'+ // port
  '(\\/[-a-z\\d%@_.~+&:]*)*'+ // path
  '(\\?[;&a-z\\d%@_.,~+&:=-]*)?'+ // query string
  '(\\#[-a-z\\d_]*)?$','i'); // fragment locator
  return pattern.test(str);
}

/*
 * hash string
 */
async function hashStr(message){
  return crypto.createHmac('sha256', message).digest('hex');
}

/*
 * Weird intermediate function which does probably too much. Anyway it came handy for here right now.
 * Extracts the triples we want, and also provides an escapefunction mapping per S and O for serializing to db
 */
function getBesluiten(triples){
  let trs = triples.filter(e => e.predicate == 'a' && e.object == 'http://data.vlaanderen.be/ns/besluit#Besluit');

  //We are conservative in what to persist; we respect applicatieprofiel
  let poi = [{ escapeSubjectF: sparqlEscapeUri, predicate: 'a', escapeObjectF: sparqlEscapeUri },
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
             { escapeSubjectF: sparqlEscapeUri, predicate: 'http://mu.semte.ch/vocabularies/ext/besluitPublicatieLinkedBesluit', escapeObjectF: sparqlEscapeUri }
            ];

  trs = triples.filter(t => trs.find(a => a.subject == t.subject) && poi.find(p => p.predicate == t.predicate));
  return { trs, poi };
}

function getBvap(triples){
  let trs = triples.filter(e => e.predicate == 'a' && e.object == 'http://data.vlaanderen.be/ns/besluit#BehandelingVanAgendapunt');

  //We are conservative in what to persist; we respect applicatieprofiel
  let poi = [{ escapeSubjectF: sparqlEscapeUri, predicate: 'a', escapeObjectF: sparqlEscapeUri },
             { escapeSubjectF: sparqlEscapeUri, predicate: 'http://data.vlaanderen.be/ns/besluit#gebeurtNa', escapeObjectF: sparqlEscapeUri },
             { escapeSubjectF: sparqlEscapeUri, predicate: 'http://www.w3.org/ns/prov#generated', escapeObjectF: sparqlEscapeUri },
             { escapeSubjectF: sparqlEscapeUri, predicate: 'http://data.vlaanderen.be/ns/besluit#heeftAanwezige', escapeObjectF: sparqlEscapeUri },
             { escapeSubjectF: sparqlEscapeUri, predicate: 'http://purl.org/dc/terms/subject', escapeObjectF: sparqlEscapeUri },
             { escapeSubjectF: sparqlEscapeUri, predicate: 'http://data.vlaanderen.be/ns/besluit#heeftSecretaris', escapeObjectF: sparqlEscapeUri },
             { escapeSubjectF: sparqlEscapeUri, predicate: 'http://data.vlaanderen.be/ns/besluit#heeftStemming', escapeObjectF: sparqlEscapeUri },
             { escapeSubjectF: sparqlEscapeUri, predicate: 'http://data.vlaanderen.be/ns/besluit#heeftVoorzitter', escapeObjectF: sparqlEscapeUri },
             { escapeSubjectF: sparqlEscapeUri, predicate: 'http://data.vlaanderen.be/ns/besluit#openbaar', escapeObjectF: sparqlEscapeBool },
             { escapeSubjectF: sparqlEscapeUri, predicate: 'http://schema.org/position', escapeObjectF: sparqlEscapeInt },
             { escapeSubjectF: sparqlEscapeUri, predicate: 'http://www.w3.org/ns/prov#wasDerivedFrom', escapeObjectF: sparqlEscapeUri },
             { escapeSubjectF: sparqlEscapeUri, predicate: 'http://mu.semte.ch/vocabularies/ext/besluitPublicatieLinkedBvap', escapeObjectF: sparqlEscapeUri }
            ];

  trs = triples.filter(t => trs.find(a => a.subject == t.subject) && poi.find(p => p.predicate == t.predicate));
  return { trs, poi };
}

function getAgendaPunten(triples){
  let trs = triples.filter(e => e.predicate == 'a' && e.object == 'http://data.vlaanderen.be/ns/besluit#Agendapunt');

  //We are conservative in what to persist; we respect applicatieprofiel
  let poi = [{ escapeSubjectF: sparqlEscapeUri, predicate: 'a', escapeObjectF: sparqlEscapeUri },
             { escapeSubjectF: sparqlEscapeUri, predicate: 'http://data.vlaanderen.be/ns/besluit#aangebrachtNa', escapeObjectF: sparqlEscapeUri },
             { escapeSubjectF: sparqlEscapeUri, predicate: 'http://purl.org/dc/terms/description', escapeObjectF: sparqlEscapeString },
             { escapeSubjectF: sparqlEscapeUri, predicate: 'http://data.vlaanderen.be/ns/besluit#geplandOpenbaar', escapeObjectF: sparqlEscapeBool },
             { escapeSubjectF: sparqlEscapeUri, predicate: 'http://data.vlaanderen.be/ns/besluit#heeftOntwerpbesluit', escapeObjectF: sparqlEscapeUri },
             { escapeSubjectF: sparqlEscapeUri, predicate: 'http://purl.org/dc/terms/references', escapeObjectF: sparqlEscapeUri },
             { escapeSubjectF: sparqlEscapeUri, predicate: 'http://purl.org/dc/terms/title', escapeObjectF: sparqlEscapeString },
             { escapeSubjectF: sparqlEscapeUri, predicate: 'http://data.vlaanderen.be/ns/besluit#Agendapunt.type', escapeObjectF: sparqlEscapeUri },
             { escapeSubjectF: sparqlEscapeUri, predicate: 'http://schema.org/position', escapeObjectF: sparqlEscapeInt },
             { escapeSubjectF: sparqlEscapeUri, predicate: 'http://www.w3.org/ns/prov#wasDerivedFrom', escapeObjectF: sparqlEscapeUri },
             { escapeSubjectF: sparqlEscapeUri, predicate: 'http://data.vlaanderen.be/ns/besluit#behandelt', escapeObjectF: sparqlEscapeUri }
            ];

  trs = triples.filter(t => trs.find(a => a.subject == t.subject) && poi.find(p => p.predicate == t.predicate));
  return { trs, poi };
};


function getZittingResource(triples){
  let trs = triples.filter(isAZitting);

  //We are conservative in what to persist; we respect applicatieprofiel
  let poi = [{ escapeSubjectF: sparqlEscapeUri, predicate: 'a', escapeObjectF: sparqlEscapeUri},
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
             { escapeSubjectF: sparqlEscapeUri, predicate: 'http://www.w3.org/ns/prov#atLocation', escapeObjectF: sparqlEscapeUri },
             { escapeSubjectF: sparqlEscapeUri, predicate: 'http://www.w3.org/ns/prov#wasDerivedFrom', escapeObjectF: sparqlEscapeUri }
            ];

  trs = triples.filter(t => trs.find(a => a.subject == t.subject) && poi.find(p => p.predicate == t.predicate));
  return { trs, poi };
};

export { startPipeline }
