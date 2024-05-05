import { stat, writeFile, readFile } from "fs/promises";
import { existsSync, mkdirSync } from "fs";
import { updateSudo as update } from "@lblod/mu-auth-sudo";
// @ts-ignore
import {
  sparqlEscapeUri,
  sparqlEscapeString,
  sparqlEscapeDateTime,
  uuid,
  // eslint-disable-next-line import/no-unresolved
} from "mu";
import { PUBLIC_GRAPH } from "./constants";

/**
 * These utils are copied from https://github.com/lblod/notulen-prepublish-service/tree/6fc93af34659e70db3683d5f6f6223a269c1e65d/support/file-utils.js
 */

/**
 * reads a file from the shared drive and returns its content
 * @param {string} shareUri the uri of the file to read
 * @return string
 */
export async function getFileContentForUri(shareUri) {
  const path = shareUri.replace("share://", "/share/");
  const content = await readFile(path, "utf8");
  return content;
}

/**
 * write contents to a file in the shared drive and return its path
 * @param {string} content The content of the file
 * @param {string[]?} pathPrefix Path entries joined with "/" and infixed like `/share/<pathPrefix>/filename.hml`. Entries should not contain slashes.
 */
export async function persistContentToFile(content, pathPrefix = []) {
  const fileId = uuid();
  const filename = `${fileId}.html`;
  let path;
  if (pathPrefix.length) {
    const dir = `/share/${pathPrefix.join("/")}`;
    if (!existsSync(dir)) {
      mkdirSync(dir, { recursive: true });
    }
    path = `${dir}/${filename}`;
  } else {
    path = `/share/${filename}`;
  }
  await writeFile(path, content, "utf8");
  return { uuid: fileId, path, filename };
}

export async function writeFileMetadataToDb(metadata) {
  const logicalFileUuid = uuid();
  const logicalFileUri = `http://lblod.data.gift/files/${logicalFileUuid}`;
  const logicalFileName = metadata.filename;
  const fileStats = await stat(metadata.path);
  const fileSize = fileStats.size;
  const created = new Date();
  const physicalFilename = metadata.filename;
  const physicalFileUri = metadata.path.replace("/share/", "share://");
  const fileQuery = `
   PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
   PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
   PREFIX prov: <http://www.w3.org/ns/prov#>
   PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
   PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
   PREFIX dct: <http://purl.org/dc/terms/>
   PREFIX dbpedia: <http://dbpedia.org/ontology/>
   INSERT DATA {
    GRAPH ${sparqlEscapeUri(PUBLIC_GRAPH)}{
         ${sparqlEscapeUri(logicalFileUri)} a nfo:FileDataObject;
                    nfo:fileName ${sparqlEscapeString(logicalFileName)};
                    mu:uuid ${sparqlEscapeString(logicalFileUuid)};
                    dct:format "text/html";
                    dbpedia:fileExtension "html";
                    nfo:fileSize ${fileSize};
                    dct:created ${sparqlEscapeDateTime(created)};
                    dct:modified ${sparqlEscapeDateTime(created)}.
         ${sparqlEscapeUri(physicalFileUri)} a nfo:FileDataObject;
                    nie:dataSource ${sparqlEscapeUri(logicalFileUri)};
                    nfo:fileName ${sparqlEscapeUri(physicalFilename)};
                    mu:uuid ${sparqlEscapeUri(metadata.uuid)};
                    nfo:fileSize ${fileSize};
                    dbpedia:fileExtension "html";
                    dct:created ${sparqlEscapeDateTime(created)};
                    dct:modified ${sparqlEscapeDateTime(created)}.
            }
  }`;

  await update(fileQuery);
  return logicalFileUri;
}
