/**
 * Represents an rdfa-document as entered by the user in the frontend.
 */

import jsdom from "jsdom";

class RdfaDomDocument {
  /**
   * @private
   * @type {string}
   */
  content;

  /**
   * @private
   * @type {HTMLHtmlElement?}
   */
  dom;

  /**
   * @private
   * @type {HTMLBodyElement?}
   */
  topDomNode;

  /**
   * @param {string} content
   */
  constructor(content) {
    this.content = content;
  }

  /**
   * @returns {HTMLHtmlElement}
   */
  getDom() {
    if (this.dom) {
      return this.dom;
    }

    const dom = new jsdom.JSDOM(`<body>${this.content}</body>`);
    this.dom = dom;
    return dom;
  }

  /**
   * @returns {HTMLBodyElement}
   */
  getTopDomNode() {
    if (this.topDomNode) {
      return this.topDomNode;
    }
    const dom = this.getDom();
    const topDomNode = dom.window.document.querySelector("body");
    this.topDomNode = topDomNode;
    return topDomNode;
  }

  /**
   * @returns {void}
   */
  resetDom() {
    this.dom = undefined;
    this.topDomNode = undefined;
  }
}

export default RdfaDomDocument;
