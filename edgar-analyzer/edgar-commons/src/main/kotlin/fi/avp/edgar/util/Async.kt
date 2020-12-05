package fi.avp.edgar.util

import kotlinx.coroutines.*
import java.util.concurrent.Executors

suspend inline fun <T, R> Iterable<T>.mapAsync(crossinline transform: suspend CoroutineScope.(T) -> R): List<Deferred<R>> = coroutineScope {
    map {
        async {
            transform(it)
        }
    }
}

suspend inline fun <T> Iterable<T>.forEachAsync(crossinline transform: suspend CoroutineScope.(T) -> Unit) = coroutineScope {
    forEach {
        async {
            transform(it)
        }
    }
}

private val taskDispatcher: CoroutineDispatcher = Executors.newFixedThreadPool(4).asCoroutineDispatcher()

suspend fun <T> runOnComputationThreadPool(block: suspend CoroutineScope.() -> T) = withContext(taskDispatcher, block)

