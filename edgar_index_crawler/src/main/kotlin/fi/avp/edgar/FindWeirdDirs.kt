package fi.avp.edgar

import fi.avp.util.Locations
import java.nio.file.Files

fun main() {
    val notCovered = Files.walk(Locations.xbrlDir).filter {
//        Files.find(it, 1, matcher - );
        it.toString().contains("QTR") && Files.isDirectory(it) && !Files.exists(it.resolve("visited_"))
    }

    println(notCovered.forEach {println(it)})
}
