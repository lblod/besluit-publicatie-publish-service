import {findFirstNodeOfType, findAllNodesOfType} from '@lblod/marawa/dist/dom-helpers';
import rdfaDomDocument from './rdfa-dom-document';
import { analyse, resolvePrefixes } from '@lblod/marawa/dist/rdfa-context-scanner';
import { persistExtractedData, belongsToType, IS_PUBLISHED_AGENDA, IS_PUBLISHED_BESLUITENLIJST, IS_PUBLISHED_NOTULEN } from './queries';
import {uuid, sparqlEscapeUri, sparqlEscapeString, sparqlEscapeInt, sparqlEscapeDate, sparqlEscapeDateTime, sparqlEscapeBool } from 'mu';

//TODO: posities agendapunten, behandeling van AP en besluiten orde
async function startPipeline(resourceToPublish){
  let doc = new rdfaDomDocument(resourceToPublish.rdfaSnippet);
  let triples = flatTriples(doc.getTopDomNode()); //let's not make an assumption about how the document is structured. Might explode memory?
  triples = preProcess(triples);

  await insertZitting(triples, resourceToPublish);
  await insertAgendaPunten(triples, resourceToPublish);
  await insertBvap(triples, resourceToPublish);
  await insertBesluiten(triples, resourceToPublish);
  await insertNotulen(triples, resourceToPublish);
};

async function insertBesluiten(triples, resourceToPublish){
  if(!(await belongsToType(resourceToPublish, IS_PUBLISHED_BESLUITENLIJST))){
    return;
  }
  let trs = getBesluiten(triples);
  linkToZitting(trs, triples, "http://mu.semte.ch/vocabularies/ext/besluit-publicatie-publish-service/linked/besluit");
  linkToPublishedResource(trs, resourceToPublish.resource);
  trs = postProcess(trs);
  await persistExtractedData(trs);
}

async function insertBvap(triples, resourceToPublish){
  if(!(await belongsToType(resourceToPublish, IS_PUBLISHED_BESLUITENLIJST))){
    return;
  }
  let trs = getBvap(triples);
  linkToZitting(trs, triples, "http://mu.semte.ch/vocabularies/ext/besluit-publicatie-publish-service/linked/behandeling-van-agendapunt");
  linkToPublishedResource(trs, resourceToPublish.resource);
  trs = postProcess(trs);
  await persistExtractedData(trs);
};

async function insertZitting(triples, resourceToPublish){
  let trs = getZittingResource(triples);
  linkToPublishedResource(trs, resourceToPublish.resource);
  trs = postProcess(trs);
  await persistExtractedData(trs);
};

async function insertAgendaPunten(triples, resourceToPublish){
  if(!(await belongsToType(resourceToPublish, IS_PUBLISHED_AGENDA))){
    return;
  }
  let trs = getAgendaPunten(triples);
  linkToZitting(trs, triples, "http://data.vlaanderen.be/ns/besluit#behandelt");
  linkToPublishedResource(trs, resourceToPublish.resource);
  trs = postProcess(trs);
  await persistExtractedData(trs);
};

async function insertNotulen(triples, resourceToPublish){
  if(!(await belongsToType(resourceToPublish, IS_PUBLISHED_NOTULEN))){
    return;
  }
  let subject = sparqlEscapeUri(`http://data.lblod.info/vocabularies/lblod/notulen/${uuid()}`);
  let trs = [];
  trs.push({subject, predicate: "a", object: sparqlEscapeUri(`http://xmlns.com/foaf/0.1/Document`)});
  trs.push({subject,
            predicate:
            sparqlEscapeUri('http://www.w3.org/ns/prov#value'),
            object: sparqlEscapeString(resourceToPublish.rdfaSnippet)});
  linkToZitting(trs, triples, "http://data.vlaanderen.be/ns/besluit#heeftNotulen");
  linkToPublishedResource(trs, resourceToPublish.resource);
  trs = postProcess(trs);
  await persistExtractedData(trs);
}

function getBesluiten(triples, resourceToPublish){
  let tois = triples.filter(e => e.predicate == 'a' && e.object == 'http://data.vlaanderen.be/ns/besluit#Besluit');

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
             { escapeSubjectF: sparqlEscapeUri, predicate: 'http://www.w3.org/ns/prov#value', escapeObjectF: sparqlEscapeString }
            ];

  tois = triples.filter(t => tois.find(a => a.subject == t.subject) && poi.find(p => p.predicate == t.predicate));

  return tois.map(t => {
    let p =  poi.find(p => p.predicate == t.predicate);
    let escapedPredicate = 'a' == p.predicate ? 'a': sparqlEscapeUri(t.predicate);
    return {subject: p.escapeSubjectF(t.subject), predicate: escapedPredicate, object: p.escapeObjectF(t.object) };
  });

}

function getBvap(triples){
  let tois = triples.filter(e => e.predicate == 'a' && e.object == 'http://data.vlaanderen.be/ns/besluit#BehandelingVanAgendapunt');

  //We are conservative in what to persist; we respect applicatieprofiel
  let poi = [{ escapeSubjectF: sparqlEscapeUri, predicate: 'a', escapeObjectF: sparqlEscapeUri },
             { escapeSubjectF: sparqlEscapeUri, predicate: 'http://data.vlaanderen.be/ns/besluit#gebeurtNa', escapeObjectF: sparqlEscapeUri },
             { escapeSubjectF: sparqlEscapeUri, predicate: 'http://www.w3.org/ns/prov#generated', escapeObjectF: sparqlEscapeUri },
             { escapeSubjectF: sparqlEscapeUri, predicate: 'http://data.vlaanderen.be/ns/besluit#heeftAanwezige', escapeObjectF: sparqlEscapeUri },
             { escapeSubjectF: sparqlEscapeUri, predicate: 'http://purl.org/dc/terms/subject', escapeObjectF: sparqlEscapeString },
             { escapeSubjectF: sparqlEscapeUri, predicate: 'http://data.vlaanderen.be/ns/besluit#heeftSecretaris', escapeObjectF: sparqlEscapeUri },
             { escapeSubjectF: sparqlEscapeUri, predicate: 'http://data.vlaanderen.be/ns/besluit#heeftStemming', escapeObjectF: sparqlEscapeUri },
             { escapeSubjectF: sparqlEscapeUri, predicate: 'http://data.vlaanderen.be/ns/besluit#heeftVoorzitter', escapeObjectF: sparqlEscapeUri },
             { escapeSubjectF: sparqlEscapeUri, predicate: 'http://data.vlaanderen.be/ns/besluit#openbaar', escapeObjectF: sparqlEscapeBool },
            ];

  tois = triples.filter(t => tois.find(a => a.subject == t.subject) && poi.find(p => p.predicate == t.predicate));

  return tois.map(t => {
    let p =  poi.find(p => p.predicate == t.predicate);
    let escapedPredicate = 'a' == p.predicate?'a':sparqlEscapeUri(t.predicate);
    return {subject: p.escapeSubjectF(t.subject), predicate: escapedPredicate, object: p.escapeObjectF(t.object) };
  });

}

function getAgendaPunten(triples){
  let tois = triples.filter(e => e.predicate == 'a' && e.object == 'http://data.vlaanderen.be/ns/besluit#Agendapunt');

  //We are conservative in what to persist; we respect applicatieprofiel
  let poi = [{ escapeSubjectF: sparqlEscapeUri, predicate: 'a', escapeObjectF: sparqlEscapeUri },
             { escapeSubjectF: sparqlEscapeUri, predicate: 'http://data.vlaanderen.be/ns/besluit#aangebrachtNa', escapeObjectF: sparqlEscapeUri },
             { escapeSubjectF: sparqlEscapeUri, predicate: 'http://purl.org/dc/terms/description', escapeObjectF: sparqlEscapeString },
             { escapeSubjectF: sparqlEscapeUri, predicate: 'http://data.vlaanderen.be/ns/besluit#geplandOpenbaar', escapeObjectF: sparqlEscapeBool },
             { escapeSubjectF: sparqlEscapeUri, predicate: 'http://data.vlaanderen.be/ns/besluit#heeftOntwerpbesluit', escapeObjectF: sparqlEscapeUri },
             { escapeSubjectF: sparqlEscapeUri, predicate: 'http://purl.org/dc/terms/references', escapeObjectF: sparqlEscapeUri },
             { escapeSubjectF: sparqlEscapeUri, predicate: 'http://purl.org/dc/terms/title', escapeObjectF: sparqlEscapeString },
             { escapeSubjectF: sparqlEscapeUri, predicate: 'http://data.vlaanderen.be/ns/besluit#Agendapunt.type', escapeObjectF: sparqlEscapeUri }
            ];

  tois = triples.filter(t => tois.find(a => a.subject == t.subject) && poi.find(p => p.predicate == t.predicate));

  return tois.map(t => {
    let p =  poi.find(p => p.predicate == t.predicate);
    let escapedPredicate = 'a' == p.predicate?'a':sparqlEscapeUri(t.predicate);
    return {subject: p.escapeSubjectF(t.subject), predicate: escapedPredicate, object: p.escapeObjectF(t.object) };
  });
};


function getZittingResource(triples){
  let tois = triples.filter(e => e.predicate == 'a' && e.object == 'http://data.vlaanderen.be/ns/besluit#Zitting');

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
             { escapeSubjectF: sparqlEscapeUri, predicate: 'http://www.w3.org/ns/prov#atLocation', escapeObjectF: sparqlEscapeUri}];

  tois = triples.filter(t => tois.find(a => a.subject == t.subject) && poi.find(p => p.predicate == t.predicate));

  return tois.map(t => {
    let p =  poi.find(p => p.predicate == t.predicate);
    let escapedPredicate = 'a' == p.predicate?'a':sparqlEscapeUri(t.predicate);
    return {subject: p.escapeSubjectF(t.subject), predicate: escapedPredicate, object: p.escapeObjectF(t.object) };
  });
};

/*************************************************************
 * HELPERS
 *************************************************************/
function linkToZitting(preparedTriples, origTriples, predicate){
  let zitting = origTriples.find(e => e.predicate == 'a' && e.object == 'http://data.vlaanderen.be/ns/besluit#Zitting');
  let resources = preparedTriples.filter(t => t.predicate == 'a');
  resources.forEach(t => {
    preparedTriples.push({subject: sparqlEscapeUri(zitting.subject), predicate: sparqlEscapeUri(predicate), object: t.subject});
  });
  return preparedTriples;
}

function linkToPublishedResource(preparedTriples, resourceUri){
  let predicate = sparqlEscapeUri('http://www.w3.org/ns/prov#wasDerivedFrom');
  let resources = preparedTriples.filter(t => t.predicate == 'a'); //extract the types
    resources.forEach(t => {
      preparedTriples.push({subject: t.subject, predicate, object: sparqlEscapeUri(resourceUri)});
    });
  return preparedTriples;
}

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

function flatTriples(node){
  let contexts = analyse( node ).map((c) => c.context);
  return contexts.reduce((acc, e) => { return [ ...acc, ...e]; }, []);
}

export { startPipeline }
