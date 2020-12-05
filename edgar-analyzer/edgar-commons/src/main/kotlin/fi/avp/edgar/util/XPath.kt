package fi.avp.edgar.util

import org.w3c.dom.Document
import org.w3c.dom.Node
import org.w3c.dom.NodeList
import javax.xml.xpath.XPathConstants
import javax.xml.xpath.XPathExpression

fun Node.find(name: String): Node? {
    for (i in 0..childNodes.length) {
        val node = childNodes.item(i)
        if (node?.localName == name) {
            return node
        }
    }
    return null;
}

fun NodeList.list(): List<Node> {
    return List(this.length) { item(it) }
}

fun Node.attr(attrId: String): String? {
   return attributes.getNamedItem(attrId)?.textContent
}

fun Node.attrList(): Map<String, String> {
    return (0..attributes.length).map { attributes.item(it) }.map {it.localName to it.textContent }.toMap()
}

fun XPathExpression.nodeSet(doc: Document): List<Node> {
    return (evaluate(doc, XPathConstants.NODESET) as NodeList).list()
}

fun XPathExpression.singleNode(doc: Document): Node? {
    return evaluate(doc, XPathConstants.NODE) as Node?
}

