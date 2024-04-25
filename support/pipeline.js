/* eslint-disable no-use-before-define */
import { analyse } from "@lblod/marawa/rdfa-context-scanner";
// eslint-disable-next-line import/no-unresolved
import { uuid } from "mu";
import crypto from "crypto";
import {
  persistExtractedData,
  belongsToType,
  IS_PUBLISHED_AGENDA,
  IS_PUBLISHED_BEHANDELING,
  IS_PUBLISHED_BESLUITENLIJST,
  IS_PUBLISHED_NOTULEN,
  insertZittingPermalinkQuery,
} from "./queries";
import RdfaDomDocument from "./rdfa-dom-document";
import { persistContentToFile, writeFileMetadataToDb } from "./file-utils";
import { RdfaParser } from "rdfa-streaming-parser";
import { annotateDecisions } from "./annotate-decisions";
import {
  isURI,
  dedupeTriples,
  findTripleWithObject,
  expandURI,
  RDF_TYPE,
} from "./rdf-utils";

/** @typedef {import('./rdf-utils').Triple} Triple */
/**
 * Main entry point for extraction of data.
 * Assumes
 * ------
 *  - All Rdfa snippets contain a Zitting
 *  - All extracted resources, are linked to a Zitting.
 *  - We extend AP with agenda, uittreksel and besluiten lijst. This eases management of extracted data.
 * */
async function startPipeline(resourceToPublish) {
  console.profile('pipeline');
  const doc = new RdfaDomDocument(resourceToPublish.rdfaSnippet);
  const triples = preProcess(
    dedupeTriples(parseRdfaIntoFlatTriples(resourceToPublish.rdfaSnippet)),
  );
  await insertZitting(triples, resourceToPublish);
  await insertAgenda(triples, resourceToPublish);
  await insertUittreksel(triples, resourceToPublish, doc);
  await insertBesluitenlijst(triples, resourceToPublish);
  await insertNotulen(triples, resourceToPublish, doc);

  await insertZittingPermalink(triples);
  console.profileEnd('pipeline');
}
/**
 * @param {string} html
 * @returns {Triple[]}
 */
function parseRdfaIntoFlatTriples(html) {
  const triples = [];
  const parser = new RdfaParser({
    baseIRI: "https://www.rubensworks.net/",
    contentType: "text/html",
  });
  parser
    .on("data", (triple) =>
      triples.push({
        subject: triple.subject.value,
        predicate: triple.predicate.value,
        object: triple.object.value,
        datatype: triple.object.datatype?.value,
      }),
    )
    .on("error", console.error)
    .on("end", () => console.log("All triples were parsed!"));
  parser.write(html);
  parser.end();
  return triples;
}
/**
 * Extracts besluitenlijst from triples and saves them.
 *
 *  - Creates a besluitenlijst resource and links the besluiten to this newly created resource
 *  - Besluitenlijst is attached to zitting.
 *  - The behandeling van agendapunten are extracted and linked to the besluiten too.
 *  - bvaps are provided with order index
 * @param {Triple[]} triples
 */
async function insertBesluitenlijst(triples, resourceToPublish) {
  if (!(await belongsToType(resourceToPublish, IS_PUBLISHED_BESLUITENLIJST))) {
    return;
  }

  const besluitenlijst = {
    subject: `http://data.lblod.info/id/lblod/besluitenlijsten/${uuid()}`,
    predicate: "a",
    object: "http://mu.semte.ch/vocabularies/ext/Besluitenlijst",
  };
  let besluitenlijstTrps = linkToZitting(
    [besluitenlijst],
    triples,
    "http://mu.semte.ch/vocabularies/ext/besluitenlijst",
  );
  besluitenlijstTrps = linkToPublishedResource(
    besluitenlijstTrps,
    resourceToPublish.resource,
  );
  besluitenlijstTrps.push({
    subject: besluitenlijst.subject,
    predicate: "http://www.w3.org/ns/prov#value",
    object: resourceToPublish.rdfaSnippet,
  });
  besluitenlijstTrps.push({
    subject: besluitenlijst.subject,
    predicate: "http://data.europa.eu/eli/ontology#date_publication",
    object: new Date().toISOString().substring(0, 10),
  });

  // Extract bvap
  // Postprocessing: make sure uri's are provided to reorder them
  const trs = triples.filter(
    (t) =>
      expandURI(t.predicate) !==
        "http://data.vlaanderen.be/ns/besluit#gebeurtNa" || isURI(t.object),
  );

  let bvaps = getBvap(trs);
  bvaps = postProcess(bvaps);
  bvaps = orderGebeurtNa(
    bvaps,
    "http://data.vlaanderen.be/ns/besluit#BehandelingVanAgendapunt",
    "http://data.vlaanderen.be/ns/besluit#gebeurtNa",
  );

  let besluiten = getBesluiten(triples);
  besluiten = linkToContainerResource(
    besluiten,
    besluitenlijst.subject,
    "http://mu.semte.ch/vocabularies/ext/besluitenlijstBesluit",
  );
  besluiten = postProcess(besluiten);

  let stemmingen = getStemmingen(triples);
  stemmingen = postProcess(stemmingen);

  await persistExtractedData([
    ...besluitenlijstTrps,
    ...bvaps,
    ...besluiten,
    ...stemmingen,
  ]);
}

/**
 * Extracts uittreksel from triples and saves them.
 *
 *  - Creates a uittreksel resource and links the bvaps to this newly created resource
 *  - Uittreksel is attached to zitting.
 *  - The besluiten are extracted and linked to the besluiten too.
 */
async function insertUittreksel(triples, resourceToPublish, doc) {
  if (!(await belongsToType(resourceToPublish, IS_PUBLISHED_BEHANDELING))) {
    return;
  }

  const uittreksel = {
    subject: `http://data.lblod.info/id/lblod/uittreksels/${uuid()}`,
    predicate: "a",
    object: "http://mu.semte.ch/vocabularies/ext/Uittreksel",
  };
  let uittrekselTrps = linkToZitting(
    [uittreksel],
    triples,
    "http://mu.semte.ch/vocabularies/ext/uittreksel",
  );
  uittrekselTrps = linkToPublishedResource(
    uittrekselTrps,
    resourceToPublish.resource,
  );
  annotateDecisions(doc, triples);
  const newSnippet = doc.getTopDomNode().innerHTML;
  uittrekselTrps.push({
    subject: uittreksel.subject,
    predicate: "http://www.w3.org/ns/prov#value",
    object: newSnippet,
  });
  // TODO: check whether adding order makes sense here...
  let bvaps = getBvap(triples);
  bvaps = postProcess(bvaps);
  bvaps = linkToContainerResource(
    bvaps,
    uittreksel.subject,
    "http://mu.semte.ch/vocabularies/ext/uittrekselBvap",
  );

  let besluiten = getBesluiten(triples);
  besluiten = postProcess(besluiten);

  await persistExtractedData([...uittrekselTrps, ...bvaps, ...besluiten]);
}

async function insertZitting(triples, resourceToPublish) {
  let data = getZittingResource(triples);
  linkToPublishedResource(data, resourceToPublish.resource);
  data = postProcess(data);

  await persistExtractedData(data);
}

/**
 * Extracts agenda from triples and saves them.
 *
 *  - Creates a agenda resource and links the agendapunten to this newly created resource
 *  - Agenda and agendapunten is attached to zitting.
 *  - Order is provided to agendapunten
 */
async function insertAgenda(triples, resourceToPublish) {
  if (!(await belongsToType(resourceToPublish, IS_PUBLISHED_AGENDA))) {
    return;
  }
  const agenda = {
    subject: `http://data.lblod.info/id/lblod/agendas/${uuid()}`,
    predicate: "a",
    object: "http://mu.semte.ch/vocabularies/ext/Agenda",
  };

  let agendaTrps = linkToZitting(
    [agenda],
    triples,
    "http://mu.semte.ch/vocabularies/ext/agenda",
  );
  agendaTrps.push({
    subject: agenda.subject,
    predicate: "http://www.w3.org/ns/prov#value",
    object: resourceToPublish.rdfaSnippet,
  });

  agendaTrps = linkToPublishedResource(agendaTrps, resourceToPublish.resource);

  // keep on preprocessing. We really need this predicate to point to a uri
  const trs = triples.filter(
    (t) =>
      expandURI(t.predicate) !==
        "http://data.vlaanderen.be/ns/besluit#aangebrachtNa" || isURI(t.object),
  );

  let agendapunten = getAgendaPunten(trs);
  agendapunten = linkToZitting(
    agendapunten,
    triples,
    "http://data.vlaanderen.be/ns/besluit#behandelt",
  );
  agendapunten = postProcess(agendapunten);
  agendapunten = orderGebeurtNa(agendapunten);
  agendapunten = linkToContainerResource(
    agendapunten,
    agenda.subject,
    "http://mu.semte.ch/vocabularies/ext/agendaAgendapunt",
  );

  await persistExtractedData([...agendaTrps, ...agendapunten]);
}

async function insertNotulen(triples, resourceToPublish, doc) {
  if (!(await belongsToType(resourceToPublish, IS_PUBLISHED_NOTULEN))) {
    return;
  }

  // Make stable uri.
  const zitting = triples.find(isAZitting);
  const subject = `http://data.lblod.info/id/lblod/notulen/${await hashStr(zitting.subject)}`;
  let trs = [];
  trs.push({
    subject,
    predicate: "a",
    object: "http://mu.semte.ch/vocabularies/ext/Notulen",
  });

  annotateDecisions(doc, triples);
  const fileMetadata = await persistContentToFile(
    doc.getTopDomNode().innerHTML,
    ["enriched-notulen"],
  );
  const logicalFileUri = await writeFileMetadataToDb(fileMetadata);
  // using the prov:generated predicate here to mirror how we link file data to
  // PublishedResource objects coming from GN
  trs.push({
    subject,
    predicate: "http://www.w3.org/ns/prov#generated",
    object: logicalFileUri,
  });
  linkToZitting(
    trs,
    triples,
    "http://data.vlaanderen.be/ns/besluit#heeftNotulen",
  );
  linkToPublishedResource(trs, resourceToPublish.resource);
  trs = postProcess(trs);

  await persistExtractedData(trs);
}

async function insertZittingPermalink(triples) {
  const zittingUri = await getZittingUri(triples);

  await insertZittingPermalinkQuery(zittingUri);
}

/** ***********************************************************
 * HELPERS
 ************************************************************ */

function isAZitting(triple) {
  return (
    expandURI(triple.predicate) === RDF_TYPE &&
    expandURI(triple.object) === "http://data.vlaanderen.be/ns/besluit#Zitting"
  );
}

function getZittingUri(triples) {
  const zitting = triples.find(isAZitting);

  return zitting.subject;
}

/*
 * Adds order to bvap and agendapunten (AP extension)
 */
function orderGebeurtNa(
  triples,
  type = "http://data.vlaanderen.be/ns/besluit#Agendapunt",
  gebeurtNa = "http://data.vlaanderen.be/ns/besluit#aangebrachtNa",
) {
  // written for Agendapunten, works on behandeling van agendapunten too.
  // assumes AP's are a list.
  // assumes no duplicates
  const orderInformation = [];
  const childAps = triples.filter((t) => expandURI(t.predicate) === gebeurtNa);

  if (childAps.length === 0) return triples;

  // TODO: this filtering seems a bit complex...
  const rootAps = triples
    .filter(
      (e) =>
        expandURI(e.predicate) === RDF_TYPE && expandURI(e.object) === type,
    )
    .map((t) => t.subject)
    .filter(
      (t) =>
        !childAps
          .map((apTriple) => apTriple.subject)
          .find((uri) => expandURI(uri) === expandURI(t)),
    );

  const ap1 = rootAps[0];

  if (!ap1) return triples;

  try {
    let currIndex = 0;
    let currAp = ap1;

    orderInformation.push({
      subject: ap1,
      predicate: "http://schema.org/position",
      object: currIndex,
    });

    while (currIndex < childAps.length) {
      const nextAp = findTripleWithObject(childAps, currAp);

      if (!nextAp) {
        throw new Error(
          `Ordering of ${type} is unexpected, we expect linear ordering`,
        );
      }

      currIndex += 1;
      orderInformation.push({
        subject: nextAp.subject,
        predicate: "http://schema.org/position",
        object: currIndex,
      });
      currAp = nextAp.subject;
    }

    return [...triples, ...orderInformation];
  } catch (e) {
    console.warn(e);

    if (rootAps.length > 1) {
      console.warn(`Found ${rootAps.length} potential root ${type}`);
      console.warn(`See also ${rootAps.join("\n")} for broken data.`);
    }

    console.warn("Returning random order");
    return triples;
  }
}

/*
 * links resources to zitting
 */
function linkToZitting(preparedTriples, origTriples, predicate) {
  const zitting = origTriples.find(isAZitting);
  const resources = preparedTriples.filter(
    (t) => expandURI(t.predicate) === RDF_TYPE,
  );
  resources.forEach((t) => {
    preparedTriples.push({
      subject: zitting.subject,
      predicate,
      object: t.subject,
    });
  });
  return preparedTriples;
}

function linkToContainerResource(
  preparedTriples,
  containerResource,
  predicate,
) {
  const resources = preparedTriples.filter(
    (t) => expandURI(t.predicate) === RDF_TYPE,
  );
  resources.forEach((t) => {
    preparedTriples.push({
      subject: containerResource,
      predicate,
      object: t.subject,
    });
  });
  return preparedTriples;
}

/*
 * links resources to publishedResource
 */
function linkToPublishedResource(preparedTriples, resourceUri) {
  const predicate = "http://www.w3.org/ns/prov#wasDerivedFrom";
  const resources = preparedTriples.filter(
    (t) => expandURI(t.predicate) === RDF_TYPE,
  ); // extract the types
  resources.forEach((t) => {
    preparedTriples.push({
      subject: t.subject,
      predicate,
      object: resourceUri,
    });
  });
  return preparedTriples;
}

/*
 * encapsulate some preprocessing, e.g. remapping stuff etc
 */
function preProcess(incomingTriples) {
  let triples = incomingTriples;
  // remap triples (for backwards compatibilty, will be deleted one day)
  const remapP = {
    "http://data.vlaanderen.be/ns/besluit#heeftAgendapunt":
      "http://data.vlaanderen.be/ns/besluit#behandelt",
  };
  triples = triples.map((t) => {
    if (remapP[t.predicate]) {
      return { ...t, predicate: remapP[t.predicate] };
    }
    return t;
  });

  // Remove triples with empty objects
  // We found that the RDFa parser did not handle spaces correctly and created triples where the object URI is a ' '.
  triples = triples.filter(
    (t) =>
      !(
        expandURI(t.datatype) ===
          "http://www.w3.org/2000/01/rdf-schema#Resource" &&
        t.object.trim().length < 1
      ),
  );

  return triples;
}

/*
 * encapsulate some post processing, e.g removing duplicates
 */
function postProcess(triples) {
  // remove duplicates
  const cleanedT = [];
  for (const ot of triples) {
    const existingT = cleanedT.find(
      (t) => JSON.stringify(t) === JSON.stringify(ot),
    ); // fair enough
    if (!existingT) {
      cleanedT.push(ot);
    }
  }
  return cleanedT;
}

/*
 * flatten output from contextscanner
 */
function flatTriples(contexts) {
  return contexts.reduce((acc, e) => [...acc, ...e], []);
}

/*
 * hash string
 */
async function hashStr(message) {
  return crypto.createHmac("sha256", message).digest("hex");
}

function getBesluiten(triples) {
  let trs = triples.filter(
    (e) =>
      expandURI(e.predicate) === RDF_TYPE &&
      expandURI(e.object) === "http://data.vlaanderen.be/ns/besluit#Besluit",
  );
  // We are conservative in what to persist; we respect applicatieprofiel
  const poi = [
    RDF_TYPE,
    "http://data.europa.eu/eli/ontology#description",
    "http://data.europa.eu/eli/ontology#title_short",
    "http://data.vlaanderen.be/ns/besluit#motivering",
    "http://data.europa.eu/eli/ontology#date_publication",
    "http://data.europa.eu/eli/ontology#realizes",
    "http://www.w3.org/ns/prov#wasGeneratedBy",
    "http://data.europa.eu/eli/ontology#title",
    "http://data.europa.eu/eli/ontology#language",
    "http://data.europa.eu/eli/ontology#description",
    "http://data.europa.eu/eli/ontology#has_part",
    "http://www.w3.org/ns/prov#value",
    "http://www.w3.org/ns/prov#wasDerivedFrom",
    "http://mu.semte.ch/vocabularies/ext/besluitPublicatieLinkedBesluit",
    "http://data.europa.eu/eli/ontology#related_to",
  ];

  trs = triples.filter(
    (t) =>
      trs.find((a) => expandURI(a.subject) === expandURI(t.subject)) &&
      poi.find((p) => p === expandURI(t.predicate)),
  );
  return trs;
}

function getBvap(triples) {
  let trs = triples.filter(
    (e) =>
      expandURI(e.predicate) === RDF_TYPE &&
      expandURI(e.object) ===
        "http://data.vlaanderen.be/ns/besluit#BehandelingVanAgendapunt",
  );

  // We are conservative in what to persist; we respect applicatieprofiel
  const poi = [
    RDF_TYPE,
    "http://data.vlaanderen.be/ns/besluit#gebeurtNa",
    "http://www.w3.org/ns/prov#generated",
    "http://data.vlaanderen.be/ns/besluit#heeftAanwezige",
    "http://purl.org/dc/terms/subject",
    "http://data.vlaanderen.be/ns/besluit#heeftSecretaris",
    "http://data.vlaanderen.be/ns/besluit#heeftStemming",
    "http://data.vlaanderen.be/ns/besluit#heeftVoorzitter",
    "http://data.vlaanderen.be/ns/besluit#openbaar",
    "http://schema.org/position",
    "http://www.w3.org/ns/prov#wasDerivedFrom",
    "http://mu.semte.ch/vocabularies/ext/besluitPublicatieLinkedBvap",
  ];

  trs = triples.filter(
    (t) =>
      trs.find((a) => expandURI(a.subject) === expandURI(t.subject)) &&
      poi.find((p) => p === expandURI(t.predicate)),
  );
  return trs;
}
function getStemmingen(triples) {
  let trs = triples.filter(
    (e) =>
      expandURI(e.predicate) === RDF_TYPE &&
      expandURI(e.object) === "http://data.vlaanderen.be/ns/besluit#Stemming",
  );
  // We are conservative in what to persist; we respect applicatieprofiel
  const poi = [
    RDF_TYPE,
    "http://data.vlaanderen.be/ns/besluit#onderwerp",
    "http://data.vlaanderen.be/ns/besluit#gevolg",
  ];
  trs = triples.filter(
    (t) =>
      trs.find((a) => expandURI(a.subject) === expandURI(t.subject)) &&
      poi.find((p) => p === expandURI(t.predicate)),
  );
  return trs;
}
function getAgendaPunten(triples) {
  let trs = triples.filter(
    (e) =>
      expandURI(e.predicate) === RDF_TYPE &&
      expandURI(e.object) === "http://data.vlaanderen.be/ns/besluit#Agendapunt",
  );

  // We are conservative in what to persist; we respect applicatieprofiel
  const poi = [
    RDF_TYPE,
    "http://data.vlaanderen.be/ns/besluit#aangebrachtNa",
    "http://purl.org/dc/terms/description",
    "http://data.vlaanderen.be/ns/besluit#geplandOpenbaar",
    "http://data.vlaanderen.be/ns/besluit#heeftOntwerpbesluit",
    "http://purl.org/dc/terms/references",
    "http://purl.org/dc/terms/title",
    "http://data.vlaanderen.be/ns/besluit#Agendapunt.type",
    "http://schema.org/position",
    "http://www.w3.org/ns/prov#wasDerivedFrom",
    "http://data.vlaanderen.be/ns/besluit#behandelt",
  ];

  trs = triples.filter(
    (t) =>
      trs.find((a) => expandURI(a.subject) === expandURI(t.subject)) &&
      poi.find((p) => p === expandURI(t.predicate)),
  );
  return trs;
}

function getZittingResource(triples) {
  let trs = triples.filter(isAZitting);

  // We are conservative in what to persist; we respect applicatieprofiel
  const poi = [
    RDF_TYPE,
    "http://data.vlaanderen.be/ns/besluit#geplandeStart",
    "http://www.w3.org/ns/prov#startedAtTime",
    "http://data.vlaanderen.be/ns/besluit#isGehoudenDoor",
    "http://data.vlaanderen.be/ns/besluit#behandelt",
    "http://www.w3.org/ns/prov#endedAtTime",
    "http://data.vlaanderen.be/ns/besluit#heeftAanwezigeBijStart",
    "http://data.vlaanderen.be/ns/besluit#heeftNotulen",
    "http://data.vlaanderen.be/ns/besluit#heeftSecretaris",
    "http://data.vlaanderen.be/ns/besluit#heeftVoorzitter",
    "http://data.vlaanderen.be/ns/besluit#heeftZittingsverslag",
    "http://www.w3.org/ns/prov#atLocation",
    "http://www.w3.org/ns/prov#wasDerivedFrom",
  ];

  trs = triples.filter(
    (t) =>
      trs.find((a) => expandURI(a.subject) === expandURI(t.subject)) &&
      poi.find((p) => p === expandURI(t.predicate)),
  );
  return trs;
}

/**
 * returns the first domNode where resource is the subject */
function findNodeForResource(orderedContexts, resource) {
  for (let idx = 0; idx < orderedContexts.length; idx += 1) {
    const ctxObj = orderedContexts[idx];
    for (let cdx = 0; cdx < ctxObj.context.length; cdx += 1) {
      const triple = ctxObj.context[cdx];
      if (expandURI(triple.subject) === expandURI(resource))
        return ctxObj.semanticNode.domNode;
    }
  }
  console.log(`Could not find resource ${resource}`);
  return null;
}
export { startPipeline };
