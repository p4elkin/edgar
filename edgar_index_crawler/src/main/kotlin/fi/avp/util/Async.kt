package fi.avp.util

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import java.util.stream.Stream

suspend inline fun <T, R> Iterable<T>.mapAsync(crossinline transform: suspend CoroutineScope.(T) -> R): List<Deferred<R>> = coroutineScope {
    map {
        async {
            transform(it)
        }
    }
}
