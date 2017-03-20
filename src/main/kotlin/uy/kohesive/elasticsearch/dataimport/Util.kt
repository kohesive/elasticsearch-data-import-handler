package uy.kohesive.elasticsearch.dataimport

import java.time.format.DateTimeFormatter
import java.time.temporal.Temporal

fun isoDateFormat(): DateTimeFormatter = DateTimeFormatter.ISO_INSTANT
fun Temporal.toIsoString(): String = isoDateFormat().format(this)
