package uy.kohesive.elasticsearch.dataimport

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigRenderOptions
import com.typesafe.config.ConfigResolveOptions
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.parser.ParseException
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL
import java.io.File
import java.io.InputStream
import java.io.InputStreamReader
import java.net.URL
import java.net.URLClassLoader
import java.sql.Timestamp
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit
import java.time.temporal.TemporalUnit
import java.util.*

class App {
    companion object {
        @JvmStatic fun main(args: Array<String>) {
            println("Kohesive - Elasticsearch Data Import Utility")
            val configFile = args.takeIf { it.isNotEmpty() }?.let { File(it[0]).normalize().absoluteFile }

            if (configFile == null || !configFile.exists()) {
                println("  ERROR:  configFile must be specified and must exist")
                configFile?.let { println("${it.absolutePath} does not exist") }
                println("  usage:  App <configFile.json>")
                println()
                System.exit(-1)
            }

            try {
                App().run(configFile!!.inputStream())
            } catch (ex: Throwable) {
                System.err.println("Data import failed due to:")
                System.err.println(ex.message)
                System.err.println()
                System.err.println("Debug stack trace:")
                ex.printStackTrace()
                System.exit(-1)
            }
        }
    }

    fun run(configInput: InputStream) {
        val hoconCfg = ConfigFactory.parseReader(InputStreamReader(configInput)).resolve(ConfigResolveOptions.noSystem())
        val jsonCfg = hoconCfg.root().render(ConfigRenderOptions.concise().setJson(true))
        val cfg = jacksonObjectMapper().readValue<EsDataImportHandlerConfig>(jsonCfg)

        val uniqueId = UUID.randomUUID().toString()

        val sparkMaster = cfg.sparkMaster ?: "local[${(Runtime.getRuntime().availableProcessors() - 1).coerceAtLeast(1)}]"

        println("Connecting to target ES clusters to check state...")
        val stateMap: Map<String, StateManager> = cfg.importSteps.map { importStep ->
            val mgr = ElasticSearchStateManager(importStep.targetElasticsearch.nodes, importStep.targetElasticsearch.basicAuth)
            mgr.init()
            importStep.statements.map { statement ->
                statement.id to mgr
            }
        }.flatten().toMap()

        val NOSTATE = LocalDateTime.of(1900,1,1,0,0,0,0).atZone(ZoneOffset.UTC).toInstant()

        val lastRuns: Map<String, Instant> = cfg.importSteps.map { importStep ->
            importStep.statements.map { statement ->
                val lastState = stateMap.get(statement.id)!!.readStateForStatement(uniqueId, statement.id)?.truncatedTo(ChronoUnit.SECONDS) ?: NOSTATE
                println("  Statement ${statement.id} - ${statement.description}")
                println("     LAST RUN: ${if (lastState === NOSTATE) "never" else lastState.toIsoString()}")
                statement.id to lastState
            }
        }.flatten().toMap()

        val thisRunDate = Instant.now().truncatedTo(ChronoUnit.SECONDS)

        val oldClassLoader = Thread.currentThread().contextClassLoader
        val extraClasspath = cfg.sources.jdbc?.map { it.driverJars }?.filterNotNull()?.flatten()?.map { URL("file://${File(it).normalize().absolutePath}") }?.toTypedArray() ?: emptyArray()
        val newClassLoader = URLClassLoader(extraClasspath, oldClassLoader)

        Thread.currentThread().contextClassLoader = newClassLoader
        try {
            SparkSession.builder().appName("esDataImport-${uniqueId}").master(sparkMaster).getOrCreate().use { spark ->

                // setup FILE inputs
                println()
                cfg.sources.filesystem?.forEach { filesystem ->
                    println("Mounting filesystem ${filesystem.directory}")
                    val dir = File(filesystem.directory).normalize()
                    if (!dir.exists()) {
                        throw DataImportException("Invalid filesystem directory: ${dir} does not exist")
                    }
                    filesystem.tables.forEach { table ->
                        println("Create table ${table.sparkTable} from filespec ${table.filespecs.joinToString()}")
                        val options = mutableMapOf<String, String>()
                        filesystem.settings?.let { options.putAll(it) }
                        table.settings?.let { options.putAll(it) }
                        val fileSpecs = table.filespecs.map { "${dir.absolutePath}${File.separatorChar}${it}" }.toTypedArray()
                        spark.read().format(table.format)
                                .options(options)
                                .load(*fileSpecs)
                                .registerTempTable(table.sparkTable)
                    }
                }

                // setup JDBC inputs
                println()
                cfg.sources.jdbc?.forEach { jdbc ->
                    println("Mounting JDBC source ${jdbc.jdbcUrl}")
                    jdbc.tables.forEach { table ->
                        println("Creating table ${table.sparkTable} from JDBC ${table.sourceTable}")
                        val sourceTable = table.sourceTable.takeIf { '.' in it } ?: if (jdbc.defaultSchema.isNullOrBlank()) table.sourceTable else "${jdbc.defaultSchema}.${table.sourceTable}"
                        val options = mutableMapOf("url" to jdbc.jdbcUrl,
                                "driver" to jdbc.driverClass,
                                "user" to jdbc.auth.username,
                                "password" to jdbc.auth.password,
                                "dbtable" to sourceTable)
                        jdbc.settings?.let { options.putAll(it) }
                        table.settings?.let { options.putAll(it) }
                        spark.read().format("jdbc")
                                .options(options)
                                .load()
                                .registerTempTable(table.sparkTable)
                    }
                }

                fun indexSpec(indexName: String, type: String?): String {
                    return "${indexName}/${type?.trim() ?: ""}"
                }

                // setup ES inputs
                println()
                cfg.sources.elasticsearch?.forEach { es ->
                    println("Mounting Elasticsearch source ${es.nodes.joinToString()}")
                    es.tables.forEach { table ->
                        println("Creating table ${table.sparkTable} from Elasticsearch index ${table.indexName}")
                        val options = mutableMapOf("es.nodes" to es.nodes.joinToString(","))
                        es.basicAuth?.let {
                            options.put("es.net.http.auth.user", it.username)
                            options.put("es.net.http.auth.pass", it.password)
                        }
                        es.settings?.let { options.putAll(it) }
                        table.settings?.let { options.putAll(it) }

                        if (table.esQuery != null) {
                            if (table.esQuery is Map<*, *>) {
                                @Suppress("UNCHECKED_CAST")
                                val root = if (table.esQuery.containsKey("query")) table.esQuery else mapOf("query" to table.esQuery)
                                options.put("es.query", ObjectMapper().writeValueAsString(root).replace('\n', ' '))
                            } else {
                                options.put("es.query", table.esQuery.toString())
                            }
                        } else {
                            // defaults to match_all
                        }

                        spark.read().format("org.elasticsearch.spark.sql")
                                .options(options)
                                .load(indexSpec(table.indexName, table.type))
                                .registerTempTable(table.sparkTable)
                    }
                }

                fun Throwable.toNiceMessage(): String = when (this) {
                    is AnalysisException -> """Error:(${this.line().takeIf { it.isDefined }?.toString() ?: "?"},${this.startPosition().takeIf { it.isDefined }?.toString() ?: "?"}) ${this.message}"""
                    is ParseException -> """Error:(${this.line().takeIf { it.isDefined }?.toString() ?: "?"},${this.startPosition().takeIf { it.isDefined }?.toString() ?: "?"}) ${this.message}"""
                    else -> this.message ?: "unknown error"
                }

                // run prep-queries
                println()
                cfg.prepStatements?.forEach { statement ->
                    try {
                        println("\nRunning prep-statement:\n${statement.description.replaceIndent("  ")}")
                        spark.sql(statement.sqlQuery)
                    } catch (ex: Throwable) {
                        val msg = ex.toNiceMessage()
                        throw DataImportException("Prep Statement: ${statement.description}\n$msg", ex)
                    }
                    println()
                }

                // run importers
                println()
                cfg.importSteps.forEach { import ->
                    println("\nRunning importer:\n${import.description.replaceIndent("  ")}")
                    import.statements.forEach { statement ->
                        val stateMgr = stateMap.get(statement.id)!!
                        val lastRun = lastRuns.get(statement.id)!!

                        // SQL times will be local time zone, so much match the server
                        val sqlMinDate = Timestamp.from(lastRun).toString()
                        val sqlMaxDate = Timestamp.from(thisRunDate).toString()

                        println("\n    Execute statement:  (range $sqlMinDate' to '$sqlMaxDate')\n${statement.description.replaceIndent("        ")}")
                        if (!stateMgr.lockStatement(uniqueId, statement.id)) {
                            System.err.println("        Cannot aquire lock for statement ${statement.id}")
                        } else {
                            try {
                                val subDataInQuery = statement.sqlQuery.replace("{lastRun}", sqlMinDate).replace("{thisRun}", sqlMaxDate)
                                val sqlResults = try {
                                    spark.sql(subDataInQuery)
                                } catch (ex: Throwable) {
                                    val msg = ex.toNiceMessage()
                                    throw DataImportException(msg, ex)
                                }

                                val options = mutableMapOf("es.nodes" to import.targetElasticsearch.nodes.joinToString(","))
                                import.targetElasticsearch.basicAuth?.let {
                                    options.put("es.net.http.auth.user", it.username)
                                    options.put("es.net.http.auth.pass", it.password)
                                }
                                import.targetElasticsearch.settings?.let { options.putAll(it) }
                                statement.settings?.let { options.putAll(it) }

                                JavaEsSparkSQL.saveToEs(sqlResults, indexSpec(statement.indexName, statement.type), options)
                                stateMgr.writeStateForStatement(uniqueId, statement.id, thisRunDate, "success", null)
                                stateMgr.logStatement(uniqueId, statement.id, thisRunDate, "sucess", null)
                            }
                            catch (ex: Exception) {
                                val msg = ex.message ?: "unknown failure"
                                stateMgr.writeStateForStatement(uniqueId, statement.id, thisRunDate, "error", msg)
                                stateMgr.logStatement(uniqueId, statement.id, thisRunDate, "error", msg)
                            }
                            finally {
                                stateMgr.unlockStatemnt(uniqueId, statement.id)
                            }
                        }
                    }
                }

                println("\nShutting down...")
            }
            println("\nDONE.")
        } finally {
            Thread.currentThread().contextClassLoader = oldClassLoader
        }
    }
}