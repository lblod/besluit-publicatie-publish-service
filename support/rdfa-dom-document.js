/**
 * Represents an rdfa-document as entered by the user in the frontend.
 */

import { query, sparqlEscapeString } from "mu";
import jsdom from "jsdom";

class RdfaDomDocument {
  constructor(content) {
    this.content = content;
  }

  getDom() {
    if (this.dom) {
      return this.dom;
    }

    const dom = new jsdom.JSDOM(`<body>${this.content}</body>`);
    this.dom = dom;
    return dom;
  }

  getTopDomNode() {
    if (this.topDomNode) {
      return this.topDomNode;
    }
    const dom = this.getDom();
    const topDomNode = dom.window.document.querySelector("body");
    this.topDomNode = topDomNode;
    return topDomNode;
  }

  resetDom() {
    this.dom = undefined;
    this.topDomNode = undefined;
  }
}

export default RdfaDomDocument;
