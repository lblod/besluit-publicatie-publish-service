import {findFirstNodeOfType, findAllNodesOfType} from '@lblod/marawa/dist/dom-helpers';
import rdfaDomDocument from './rdfa-dom-document';
import { analyse, resolvePrefixes } from '@lblod/marawa/dist/rdfa-context-scanner';
import { getRelationDataForZitting, persistExtractedData, belongsToType, cleanUpResource,
         IS_PUBLISHED_AGENDA, IS_PUBLISHED_BEHANDELING,
         IS_PUBLISHED_BESLUITENLIJST, IS_PUBLISHED_NOTULEN } from './queries';
import { uuid } from 'mu';
import crypto from 'crypto';

/**
 * Main entry point for extraction of data.
 * Assumes
 * ------
 *  - All Rdfa snippets contain a Zitting
 *  - All extracted resources, are linked to a Zitting.
 *  - We extend AP with agenda, uittreksel and besluiten lijst. This eases management of extracted data.
 **/
async function startPipeline(resourceToPublish){
  let doc = new rdfaDomDocument(resourceToPublish.rdfaSnippet);
  let triples = flatTriples(doc.getTopDomNode()); //let's not make an assumption about how the document is structured. But, might explode memory?
  triples = preProcess(triples);

  await insertZitting(triples, resourceToPublish);
  await insertAgenda(triples, resourceToPublish);
  await insertUittreksel(triples, resourceToPublish);
  await insertBesluitenlijst(triples, resourceToPublish);
  await insertNotulen(triples, resourceToPublish);
};

/**
 * Extracts besluitenlijst from triples and saves them.
 *
 *  - Creates a besluitenlijst resource and links the besluiten to this newly created resource
 *  - Besluitenlijst is attached to zitting.
 *  - The behandeling van agendapunten are extracted and linked to the besluiten too.
 *  - bvaps are provided with order index
 */
async function insertBesluitenlijst(triples, resourceToPublish){

  if(!(await belongsToType(resourceToPublish, IS_PUBLISHED_BESLUITENLIJST))){
    return;
  }

  let besluitenlijst = {subject: `http://mu.semte.ch/vocabularies/ext/besluitenlijsten/${uuid()}`,
                        predicate: 'a',
                        object: 'http://mu.semte.ch/vocabularies/ext/Besluitenlijst'};
  let besluitenlijstTrps = linkToZitting([besluitenlijst], triples, 'http://mu.semte.ch/vocabularies/ext/besluitenlijst');
  besluitenlijstTrps = linkToPublishedResource(besluitenlijstTrps, resourceToPublish.resource);
  besluitenlijstTrps.push({subject: besluitenlijst.subject,
                 predicate: 'http://www.w3.org/ns/prov#value',
                 object: resourceToPublish.rdfaSnippet});

  //Extract bvap
  //Postprocessing: make sure uri's are provided to reorder them
  let trs = triples.filter(t =>
                           (t.predicate !== 'http://data.vlaanderen.be/ns/besluit#gebeurtNa') ||
                           isURI(t.object));

  let bvaps = getBvap(trs);
  bvaps = postProcess(bvaps);
  bvaps = orderGebeurtNa(bvaps,
                        'http://data.vlaanderen.be/ns/besluit#BehandelingVanAgendapunt',
                        'http://data.vlaanderen.be/ns/besluit#gebeurtNa');

  let besluiten = getBesluiten(triples);
  besluiten = linkToContainerResource(besluiten, besluitenlijst.subject, 'http://mu.semte.ch/vocabularies/ext/besluitenlijstBesluit');
  besluiten = postProcess(besluiten);

  await persistExtractedData([...besluitenlijstTrps, ...bvaps, ...besluiten]);
}

/**
 * Extracts uittreksel from triples and saves them.
 *
 *  - Creates a uittreksel resource and links the bvaps to this newly created resource
 *  - Uittreksel is attached to zitting.
 *  - The besluiten are extracted and linked to the besluiten too.
 */
async function insertUittreksel(triples, resourceToPublish){
  if(!(await belongsToType(resourceToPublish, IS_PUBLISHED_BEHANDELING))){
    return;
  }

  let uittreksel = {subject: `http://mu.semte.ch/vocabularies/ext/uittreksels/${uuid()}`,
                    predicate: 'a',
                    object: 'http://mu.semte.ch/vocabularies/ext/Uittreksel'};
  let uittrekselTrps = linkToZitting([uittreksel], triples, 'http://mu.semte.ch/vocabularies/ext/uittreksel');
  uittrekselTrps = linkToPublishedResource(uittrekselTrps, resourceToPublish.resource);
  uittrekselTrps.push({subject: uittreksel.subject,
                   predicate: 'http://www.w3.org/ns/prov#value',
                   object: resourceToPublish.rdfaSnippet});

  //TODO: check whether adding order makes sense here...
  let bvaps = getBvap(triples);
  bvaps = postProcess(bvaps);
  bvaps = linkToContainerResource(bvaps, uittreksel.subject, 'http://mu.semte.ch/vocabularies/ext/uittrekselBvap');


  let besluiten = getBesluiten(triples);
  besluiten = postProcess(besluiten);

  await persistExtractedData([...uittrekselTrps, ...bvaps, ...besluiten]);

};

async function insertZitting(triples, resourceToPublish){
  let data = getZittingResource(triples);
  linkToPublishedResource(data, resourceToPublish.resource);
  data = postProcess(data);

  await persistExtractedData(data);
};

/**
 * Extracts agenda from triples and saves them.
 *
 *  - Creates a agenda resource and links the agendapunten to this newly created resource
 *  - Agenda and agendapunten is attached to zitting.
 *  - Order is provided to agendapunten
 */
async function insertAgenda(triples, resourceToPublish){
  if(!(await belongsToType(resourceToPublish, IS_PUBLISHED_AGENDA))){
    return;
  }
  let agenda = {subject: `http://mu.semte.ch/vocabularies/ext/agendas/${uuid()}`,
                predicate: 'a',
                object: 'http://mu.semte.ch/vocabularies/ext/Agenda'};

  let agendaTrps = linkToZitting([agenda], triples, 'http://mu.semte.ch/vocabularies/ext/agenda');
  agendaTrps.push({subject: agenda.subject,
                   predicate: 'http://www.w3.org/ns/prov#value',
                   object: resourceToPublish.rdfaSnippet});

  agendaTrps = linkToPublishedResource(agendaTrps, resourceToPublish.resource);


  //keep on preprocessing. We really need this predicate to point to a uri
  let trs = triples.filter(t =>
                           (t.predicate !== 'http://data.vlaanderen.be/ns/besluit#aangebrachtNa') ||
                           isURI(t.object));

  let agendapunten = getAgendaPunten(trs);
  agendapunten = linkToZitting(agendapunten, triples, "http://data.vlaanderen.be/ns/besluit#behandelt");
  agendapunten = postProcess(agendapunten);
  agendapunten = orderGebeurtNa(agendapunten);
  agendapunten = linkToContainerResource(agendapunten, agenda.subject, 'http://mu.semte.ch/vocabularies/ext/agendaAgendapunt');

  await persistExtractedData([...agendaTrps, ...agendapunten]);
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

  await persistExtractedData(trs);
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

function linkToContainerResource(preparedTriples, containerResource, predicate){
  let resources = preparedTriples.filter(t => t.predicate == 'a');
  resources.forEach(t => {
    preparedTriples.push({subject: containerResource, predicate: predicate, object: t.subject});
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
  return /^https?:\/\/(.*)/.test(str);
}

/*
 * hash string
 */
async function hashStr(message){
  return crypto.createHmac('sha256', message).digest('hex');
}

function getBesluiten(triples){
  let trs = triples.filter(e => e.predicate == 'a' && e.object == 'http://data.vlaanderen.be/ns/besluit#Besluit');

  //We are conservative in what to persist; we respect applicatieprofiel
  let poi = [
    'a',
    'http://data.europa.eu/eli/ontology#description',
    'http://data.europa.eu/eli/ontology#title_short',
    'http://data.vlaanderen.be/ns/besluit#motivering',
    'http://data.europa.eu/eli/ontology#date_publication',
    'http://data.europa.eu/eli/ontology#realizes',
    'http://www.w3.org/ns/prov#wasGeneratedBy',
    'http://data.europa.eu/eli/ontology#title',
    'http://data.europa.eu/eli/ontology#language',
    'http://data.europa.eu/eli/ontology#description',
    'http://data.europa.eu/eli/ontology#has_part',
    'http://www.w3.org/ns/prov#value',
    'http://www.w3.org/ns/prov#wasDerivedFrom',
    'http://mu.semte.ch/vocabularies/ext/besluitPublicatieLinkedBesluit',
  ];

  trs = triples.filter(t => trs.find(a => a.subject == t.subject) && poi.find(p => p == t.predicate));
  return trs;
}

function getBvap(triples){
  let trs = triples.filter(e => e.predicate == 'a' && e.object == 'http://data.vlaanderen.be/ns/besluit#BehandelingVanAgendapunt');

  //We are conservative in what to persist; we respect applicatieprofiel
  let poi = [
    'a',
    'http://data.vlaanderen.be/ns/besluit#gebeurtNa',
    'http://www.w3.org/ns/prov#generated',
    'http://data.vlaanderen.be/ns/besluit#heeftAanwezige',
    'http://purl.org/dc/terms/subject',
    'http://data.vlaanderen.be/ns/besluit#heeftSecretaris',
    'http://data.vlaanderen.be/ns/besluit#heeftStemming',
    'http://data.vlaanderen.be/ns/besluit#heeftVoorzitter',
    'http://data.vlaanderen.be/ns/besluit#openbaar',
    'http://schema.org/position',
    'http://www.w3.org/ns/prov#wasDerivedFrom',
    'http://mu.semte.ch/vocabularies/ext/besluitPublicatieLinkedBvap',
  ];

  trs = triples.filter(t => trs.find(a => a.subject == t.subject) && poi.find(p => p == t.predicate));
  return trs;
}

function getAgendaPunten(triples){
  let trs = triples.filter(e => e.predicate == 'a' && e.object == 'http://data.vlaanderen.be/ns/besluit#Agendapunt');

  //We are conservative in what to persist; we respect applicatieprofiel
  let poi = [
    'a',
    'http://data.vlaanderen.be/ns/besluit#aangebrachtNa',
    'http://purl.org/dc/terms/description',
    'http://data.vlaanderen.be/ns/besluit#geplandOpenbaar',
    'http://data.vlaanderen.be/ns/besluit#heeftOntwerpbesluit',
    'http://purl.org/dc/terms/references',
    'http://purl.org/dc/terms/title',
    'http://data.vlaanderen.be/ns/besluit#Agendapunt.type',
    'http://schema.org/position',
    'http://www.w3.org/ns/prov#wasDerivedFrom',
    'http://data.vlaanderen.be/ns/besluit#behandelt',
  ];

  trs = triples.filter(t => trs.find(a => a.subject == t.subject) && poi.find(p => p == t.predicate));
  return trs;
};


function getZittingResource(triples){
  let trs = triples.filter(isAZitting);

  //We are conservative in what to persist; we respect applicatieprofiel
  let poi = [
    'a',
    'http://data.vlaanderen.be/ns/besluit#geplandeStart',
    'http://www.w3.org/ns/prov#startedAtTime',
    'http://data.vlaanderen.be/ns/besluit#isGehoudenDoor',
    'http://data.vlaanderen.be/ns/besluit#behandelt',
    'http://www.w3.org/ns/prov#endedAtTime',
    'http://data.vlaanderen.be/ns/besluit#heeftAanwezigeBijStart',
    'http://data.vlaanderen.be/ns/besluit#heeftNotulen',
    'http://data.vlaanderen.be/ns/besluit#heeftSecretaris',
    'http://data.vlaanderen.be/ns/besluit#heeftVoorzitter',
    'http://data.vlaanderen.be/ns/besluit#heeftZittingsverslag',
    'http://www.w3.org/ns/prov#atLocation',
    'http://www.w3.org/ns/prov#wasDerivedFrom',
  ];

  trs = triples.filter(t => trs.find(a => a.subject == t.subject) && poi.find(p => p == t.predicate));
  return trs;
};

export { startPipeline }
