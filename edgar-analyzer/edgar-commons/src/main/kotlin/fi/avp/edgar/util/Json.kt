package fi.avp.edgar.util

import com.fasterxml.jackson.databind.JsonNode

inline fun <reified T> (JsonNode).getAs(id: String): T {
    val type = T::class
    val result: Any = when {
        (type == Boolean::class) -> get(id).asBoolean()
        (type == String::class) -> get(id)?.asText() ?: ""
        (type == Long::class) -> get(id).asLong()
        else -> get(id)
    }

    return result as T
}

fun (JsonNode).text(prop: String): String = getAs(prop)
fun (JsonNode).long(prop: String): Long = getAs(prop)
