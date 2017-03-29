package uy.kohesive.elasticsearch.dataimport

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import java.time.format.DateTimeFormatter
import java.time.temporal.Temporal

fun isoDateFormat(): DateTimeFormatter = DateTimeFormatter.ISO_INSTANT
fun Temporal.toIsoString(): String = isoDateFormat().format(this)

val JSON = jacksonObjectMapper().registerModules(JavaTimeModule(), Jdk8Module()).apply {
    configure(SerializationFeature.WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS, false)
    configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, true)
    configure(DeserializationFeature.READ_DATE_TIMESTAMPS_AS_NANOSECONDS, false)
}