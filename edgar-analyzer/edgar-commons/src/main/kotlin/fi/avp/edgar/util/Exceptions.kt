package fi.avp.edgar.util

inline fun <T> nullOnFailure(errorMessage: (Throwable) -> String = {""}, action: () -> T?): T? {
    return try {
        action()
    } catch (t: Throwable) {
        println(errorMessage(t))
        null
    }
}
