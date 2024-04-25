/**
 * @typedef {Object} Triple
 * @property {string} subject
 * @property {string} predicate
 * @property {string} object
 * @property {string?} datatype
 */

/*
 * checks if URI (stolen from SO)
 * @param {string} str
 * @returns {boolean}
 */
export function isURI(str) {
  return /^https?:\/\/(.*)/.test(str);
}

/**
 * Returns a naive ttl representation of a triple.
 * TODO: this ignores languages, but that's how it's always been in this service
 *
 * @param {Triple} triple
 * @returns {string}
 */
export function hashTriple({ subject, predicate, object, datatype }) {
  const hashSubject = isURI(subject) ? `<${subject}>` : subject;
  const hashPredicate = isURI(predicate) ? `<${predicate}>` : predicate;
  let hashObject;
  if (isURI(object)) {
    hashObject = `<${object}>`;
  } else if (datatype) {
    if (isURI(datatype)) {
      hashObject = `"${object}"^^<${datatype}>`;
    } else {
      hashObject = `"${object}^^${datatype}`;
    }
  } else {
    hashObject = `"${object}"`;
  }
  return `${hashSubject} ${hashPredicate} ${hashObject}`;
}

/**
 * @param {Triple[]} triples
 * @param {string} objectUri
 * @returns {Triple?}
 */
export function findTripleWithObject(triples, objectUri) {
  return triples.find((t) => t.object === objectUri);
}

/**
 * @param {Triple[]} triples
 * @returns {Triple[]}
 */
export function dedupeTriples(triples) {
  const seenTriples = new Set();
  const deduped = [];
  for (const triple of triples) {
    if (!seenTriples.has(hashTriple(triple))) {
      deduped.push(triple);
      seenTriples.add(hashTriple(triple));
    }
  }
  return deduped;
}
const DEFAULT_PREFIX_MAP = new Map([
  ["ext", "http://mu.semte.ch/vocabularies/ext/"],
  ["mu", "http://mu.semte.ch/vocabularies/core/"],
  ["muSession", "http://mu.semte.ch/vocabularies/session/"],
  ["tmp", "http://mu.semte.ch/vocabularies/tmp/"],
  ["besluit", "http://data.vlaanderen.be/ns/besluit#"],
  ["bv", "http://data.vlaanderen.be/ns/besluitvorming#"],
  ["mandaat", "http://data.vlaanderen.be/ns/mandaat#"],
  ["persoon", "http://data.vlaanderen.be/ns/persoon#"],
  ["generiek", "http://data.vlaanderen.be/ns/generiek#"],
  ["mobiliteit", "https://data.vlaanderen.be/ns/mobiliteit#"],
  [
    "publicationStatus",
    "http://mu.semte.ch/vocabularies/ext/signing/publication-status/",
  ],
  ["eli", "http://data.europa.eu/eli/ontology#"],
  ["m8g", "http://data.europa.eu/m8g/"],
  ["dct", "http://purl.org/dc/terms/"],
  ["cpsv", "http://purl.org/vocab/cpsv#"],
  ["dul", "http://www.ontologydesignpatterns.org/ont/dul/DUL.owl#"],
  ["adms", "http://www.w3.org/ns/adms#"],
  ["person", "http://www.w3.org/ns/person#"],
  ["org", "http://www.w3.org/ns/org#"],
  ["prov", "http://www.w3.org/ns/prov#"],
  ["regorg", "https://www.w3.org/ns/regorg#"],
  ["skos", "http://www.w3.org/2004/02/skos/core#"],
  ["foaf", "http://xmlns.com/foaf/0.1/"],
  ["nao", "http://www.semanticdesktop.org/ontologies/2007/08/15/nao#"],
  ["pav", "http://purl.org/pav/"],
  ["schema", "http://schema.org/"],
  ["rdfs", "http://www.w3.org/2000/01/rdf-schema#"],
  ["sign", "http://mu.semte.ch/vocabularies/ext/signing/"],
  ["lblodlg", "http://data.lblod.info/vocabularies/leidinggevenden/"],
  ["lblodmow", "http://data.lblod.info/vocabularies/mobiliteit/"],
  ["locn", "http://www.w3.org/ns/locn#"],
  ["adres", "https://data.vlaanderen.be/ns/adres#"],
  ["persoon", "http://data.vlaanderen.be/ns/persoon#"],
  ["notulen", "http://lblod.data.gift/vocabularies/notulen/"],
  ["nfo", "http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#"],
  ["nie", "http://www.semanticdesktop.org/ontologies/2007/01/19/nie#"],
  ["dbpedia", "http://dbpedia.org/ontology/"],
  ["besluittype", "https://data.vlaanderen.be/id/concept/BesluitType/"],
]);
export const RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
/**
 * @param {string} uri
 */
export function expandURI(uri, prefixMap = DEFAULT_PREFIX_MAP) {
  if (isURI(uri)) {
    return uri;
  }
  if (uri === "a") {
    return RDF_TYPE;
  }
  const split = uri.split(":");
  if (split.length > 1) {
    const prefix = split[0];
    const expansion = prefixMap.get(prefix);
    if (expansion) {
      return `${expansion}${split.slice(1).join(":")}`;
    }
    console.warn(
      `Prefix ${prefix} not found in given prefixMap ${prefixMap}, not expanding uri: ${uri}`,
    );
  }
  return uri;
}
