import { updateSudo as update } from '@lblod/mu-auth-sudo';
import { sparqlEscapeString, sparqlEscapeUri, sparqlEscapeDateTime, uuid } from 'mu';

/**
 * Save the log into the database.
*/
export async function saveLog(logsGraph, classNameUri, message, specificInformation) {
  const logEntryUuid = uuid();
  const logEntryUri = "http://data.lblod.info/id/log-entries/".concat(logEntryUuid);

  const stringifiedSpecificInformation = JSON.stringify(specificInformation);

  const result = await update(`
    PREFIX rlog: <http://persistence.uni-leipzig.org/nlp2rdf/ontologies/rlog#>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    INSERT DATA {
       GRAPH ${sparqlEscapeUri(logsGraph)} {
          ${sparqlEscapeUri(logEntryUri)} a rlog:Entry ;
              <http://mu.semte.ch/vocabularies/core/uuid> ${sparqlEscapeString(logEntryUuid)} ;
              dct:source <https://github.com/lblod/besluit-publicatie-publish-service> ;
              rlog:className ${sparqlEscapeUri(classNameUri)} ;
              rlog:message ${sparqlEscapeString(message)} ;
              rlog:date ${sparqlEscapeDateTime(new Date())} ;
              rlog:level <http://data.lblod.info/id/log-levels/3af9ebe1-e6a8-495c-a392-16ced1f38ef1> ;
              ext:specificInformation ${sparqlEscapeString(stringifiedSpecificInformation)} .
        }
    }
  `);
};

export const DEFAULT_LOGS_GRAPH = 'http://mu.semte.ch/graphs/logs';
