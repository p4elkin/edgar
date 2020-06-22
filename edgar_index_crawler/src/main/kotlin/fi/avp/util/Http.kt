package fi.avp.util

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import kotlinx.coroutines.suspendCancellableCoroutine
import okhttp3.*
import java.io.IOException
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

val client = OkHttpClient.Builder().build()
val objectMapper: ObjectMapper =
    jacksonObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

suspend fun awaitCallback(block: (Callback) -> Unit) : ResponseBody =
    suspendCancellableCoroutine { cont ->
        block(object : Callback {
            override fun onResponse(call: Call, response: Response) {
                cont.resume(response.body!!)
            }

            override fun onFailure(call: Call, e: IOException) {
                e.let { cont.resumeWithException(it) }
            }
        })
    }


suspend fun asyncJson(request: Request): JsonNode {
    val response: ResponseBody = awaitCallback {
        client
            .newCall(
                Request.Builder()
                    .url(request.url.newBuilder().build())
                    .build()
            )
            .enqueue(it)
    }

    return try {
        objectMapper.readTree(response.string()) } catch (e: Exception) {
        throw RuntimeException(e)
    }
}

suspend fun asyncJson(request: String): JsonNode {
    val response: ResponseBody = awaitCallback {
        client
            .newCall(
                Request.Builder()
                    .url(request)
                    .build())
            .enqueue(it)
    }

    return try {
        objectMapper.readTree(response.string()) } catch (e: Exception) {
        throw RuntimeException(e)
    }
}

suspend fun asyncGet(url: String): ResponseBody = awaitCallback {
    client
        .newCall(
            Request.Builder()
                .url(url)
                .build()
        )
        .enqueue(it)
}

suspend fun asyncGetText(url: String): String {
    val awaitCallback = awaitCallback {
        client
            .newCall(
                Request.Builder()
                    .url(url)
                    .build()
            )
            .enqueue(it)
    }
    return awaitCallback.string()
}
