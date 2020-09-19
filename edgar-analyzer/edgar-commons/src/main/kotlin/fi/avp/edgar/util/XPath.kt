package fi.avp.edgar.util

import org.w3c.dom.Node
import org.w3c.dom.NodeList

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
