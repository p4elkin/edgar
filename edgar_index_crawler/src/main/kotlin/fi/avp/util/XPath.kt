package fi.avp.util

import org.w3c.dom.Node

fun Node.find(name: String): Node? {
    for (i in 0..childNodes.length) {
        val node = childNodes.item(i)
        if (node?.localName == name) {
            return node
        }
    }
    return null;
}
