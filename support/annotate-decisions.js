import { JSDOM } from "jsdom";
import { expandURI } from "./rdf-utils";
/** @typedef {import('./rdf-utils').Triple} Triple */
/** @typedef {import('./rdfa-dom-document').default} RdfaDomDocument */


function enrichBesluit(dom, besluitIRI, triples) {
  const { document } = new JSDOM("").window;
  // There should be one, but in case the document is in a weird state
  const behandelingen = triples.filter(
    (t) =>
      expandURI(t.object) === expandURI(besluitIRI) &&
      expandURI(t.predicate) === "http://www.w3.org/ns/prov#generated",
  );
  let agendapuntTriple;
  let behandeling
  for (const bh of behandelingen) {
    // Find the first one which is linked to an AP
    agendapuntTriple = triples.find(
      (t) =>
        expandURI(t.subject) === expandURI(bh.subject) &&
        expandURI(t.predicate) === "http://purl.org/dc/terms/subject",
    );
    if (agendapuntTriple) {
      behandeling = bh;
      break;
    }
  }
  if (behandeling && agendapuntTriple) {
    const generatedLink = document.createElement("link");
    generatedLink.setAttribute("property", "prov:wasGeneratedBy");
    generatedLink.setAttribute("resource", behandeling.subject);
    dom.append(generatedLink);

    const zittingTriple = triples.find(
      (t) =>
        expandURI(t.object) === "http://data.vlaanderen.be/ns/besluit#Zitting",
    ); // TODO: assumes one zitting!
    if (zittingTriple) {
      const agendapuntToZitting = document.createElement("link");
      agendapuntToZitting.setAttribute("about", zittingTriple.subject);
      agendapuntToZitting.setAttribute(
        "property",
        "http://data.vlaanderen.be/ns/besluit#behandelt",
      );
      agendapuntToZitting.setAttribute("resource", agendapuntTriple.object);
      dom.append(agendapuntToZitting);
    }
  }
  const pubDate = document.createElement("link");
  pubDate.setAttribute(
    "property",
    "http://data.europa.eu/eli/ontology#date_publication",
  );
  pubDate.setAttribute("datatype", "xsd:date");
  pubDate.setAttribute("content", new Date().toISOString().substring(0, 10));
  dom.append(pubDate);
  const gehoudenDoor = triples.find(
    (t) =>
      expandURI(t.predicate) ===
      "http://data.vlaanderen.be/ns/besluit#isGehoudenDoor",
  );
  if (gehoudenDoor) {
    const generatedLink = document.createElement("link");
    generatedLink.setAttribute(
      "property",
      "http://data.europa.eu/eli/ontology#passed_by",
    );
    generatedLink.setAttribute("resource", gehoudenDoor.object);
    dom.append(generatedLink);
  }
}
/**
 * Modifies doc in-place to add extra links to the decision nodes
 * @param {RdfaDomDocument} doc
 * @param {Triple[]} triples
 * @returns {void}
 */
export function annotateDecisions(doc, triples) {
  const decisionType = "http://data.vlaanderen.be/ns/besluit#Besluit";

  const dom = doc.getTopDomNode();
  // this is a rough selection which won't work in all cases, but I hope
  // to eliminate this whole fudzing around by skipping most of the parsing this
  // service does
  const decisionNodes = dom.querySelectorAll(
    // typeof is for pre-rdfa-aware documents, the chain is to catch those
    `[typeof~="${decisionType}"], [about] > [data-rdfa-container=true] > [resource~="${decisionType}"]`
  );
  for (const node of decisionNodes) {
    const decisionIRI =
      node.getAttribute("about") ?? node.getAttribute("resource");
    enrichBesluit(node, decisionIRI, triples);
  }
}
