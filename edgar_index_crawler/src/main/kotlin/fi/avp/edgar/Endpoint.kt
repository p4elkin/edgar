package fi.avp.edgar
//
//import org.litote.kmongo.KMongo
//import org.litote.kmongo.aggregate
//import org.springframework.boot.autoconfigure.SpringBootApplication
//import org.springframework.boot.runApplication
//import org.springframework.http.MediaType
//import org.springframework.web.bind.annotation.GetMapping
//import org.springframework.web.bind.annotation.PathVariable
//import org.springframework.web.bind.annotation.RestController
//import reactor.core.publisher.Flux
//import reactor.core.publisher.toFlux

//
//@SpringBootApplication
//open class SecReportDataApplication {
//
//}
//
//fun main(args: Array<String>) {
//    runApplication<SecReportDataApplication>(*args)
//}
//
//
//
//@RestController
//class RestController() {
//
//    val client = KMongo.createClient() //get com.mongodb.MongoClient new instance
//    val database = client.getDatabase("sec-report") //normal java driver usage
//    val collection = database.getCollection("annual", ReportRecord::class.java)
//
//    @GetMapping(value = ["/{ticker}/{reportType}"], produces = [MediaType.APPLICATION_JSON_VALUE])
//    fun prices(@PathVariable(value = "ticker") ticker: String, @PathVariable(value = "reportType") reportType: String): Flux<CompanyReports> {
//        val aggregationResult = collection.aggregate<CompanyReports>(
//            "{\$match: {ticker: '$ticker'}}",
//            "{\$project: {ticker: 1, reports: {\$filter: {input: '\$reports', as: 'rep', cond: {\$eq: ['\$\$rep._id', '$reportType']}}}}}"
//        ).toList()
//
//        return aggregationResult.toFlux()
//    }
//}
