package uy.kohesive.elasticsearch.dataimport

import org.apache.spark.sql.SparkSession
import org.jsoup.Jsoup
import org.jsoup.parser.Parser
import org.jsoup.safety.Whitelist
import uy.kohesive.elasticsearch.dataimport.udf.Udfs

object DataImportHandlerUdfs {
    fun registerSparkUdfs(spark: SparkSession) {
        Udfs.registerStringToStringUdf(spark, "fluffly", fluffly)
        Udfs.registerStringToStringUdf(spark, "stripHtml", stripHtmlCompletely)
        Udfs.registerStringToStringUdf(spark, "normalizeQuotes", normalizeQuotes)
        Udfs.registerStringToStringUdf(spark, "unescapeHtmlEntites", unescapeHtmlEntities)
    }

    @JvmStatic val whiteListMap = mapOf(
            "none" to Whitelist.none(),
            "basic" to Whitelist.basic(),
            "basicwithimages" to Whitelist.basicWithImages(),
            "relaxed" to Whitelist.relaxed(),
            "simpletext" to Whitelist.simpleText(),
            "simple" to Whitelist.simpleText()
    )

    @JvmStatic val fluffly = fun (v: String): String = "fluffly " + v

    @JvmStatic val stripHtmlCompletely = fun (v: String): String {
        return Jsoup.parseBodyFragment(v).text()
    }

    @JvmStatic val normalizeQuotes = fun (v: String): String {
        return v.replace("\\'", "'").replace("''", "\"")
    }

    @JvmStatic val unescapeHtmlEntities = fun (v: String): String {
        return Parser.unescapeEntities(v, false)
    }

}