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
import org.apache.spark.storage.StorageLevel
import java.io.File
import java.io.InputStream
import java.io.InputStreamReader
import java.net.URL
import java.net.URLClassLoader
import java.sql.Timestamp
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit
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
                App().run(configFile!!.inputStream(), configFile.parentFile)
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

    fun run(configInput: InputStream, configRelativeDir: File) {
        val hoconCfg = ConfigFactory.parseReader(InputStreamReader(configInput)).resolve(ConfigResolveOptions.noSystem())
        val jsonCfg  = hoconCfg.root().render(ConfigRenderOptions.concise().setJson(true))
        val cfg      = jacksonObjectMapper().readValue<DataImportHandlerConfig>(jsonCfg)

        val uniqueId = UUID.randomUUID().toString()

        val sparkMaster = cfg.sparkMaster ?: "local[${(Runtime.getRuntime().availableProcessors() - 1).coerceAtLeast(1)}]"

        fun fileRelativeToConfig(filename: String): File {
            return configRelativeDir.resolve(filename).canonicalFile
        }

        println("Connecting to target ES/Algolia to check state...")
        val stateMap: Map<String, StateManager> = cfg.importSteps.map { importStep ->
            val mgr = if (importStep.targetElasticsearch != null) {
                ElasticSearchStateManager(
                    nodes     = importStep.targetElasticsearch.nodes,
                    port      = importStep.targetElasticsearch.port ?: 9200,
                    enableSsl = importStep.targetElasticsearch.enableSsl ?: false,
                    auth      = importStep.targetElasticsearch.basicAuth
                )
            } else if (importStep.targetAlgolia != null) {
                AlgoliaStateManager(
                    applicationId = importStep.targetAlgolia.applicationId,
                    apiKey        = importStep.targetAlgolia.apiKey
                )
            } else {
                throw IllegalStateException(importStep.description + " import step neither declares ES nor Algolia target")
            }

            mgr.init()

            importStep.statements.map { statement ->
                statement.id to mgr
            }
        }.flatten().toMap()

        val NOSTATE = LocalDateTime.of(1900, 1, 1, 0, 0, 0, 0).atZone(ZoneOffset.UTC).toInstant()

        fun DataImportStatement.validate() {
            // do a little validation of the ..
            if (newIndexSettingsFile != null) {
                val checkFile = fileRelativeToConfig(newIndexSettingsFile)
                if (!checkFile.exists()) {
                    throw IllegalStateException("The statement '${id}' new-index mapping file must exist: $checkFile")
                }
            }
            if (indexType == null && type == null) {
                throw IllegalArgumentException("The statement '${id}' is missing `indexType`")
            }
            if (type != null && indexType == null) {
                System.err.println("     Statement configuration parameter `type` is deprecated, use `indexType`")
            }
            if (sqlQuery == null && sqlFile == null) {
                throw IllegalArgumentException("The statement '${id}' is missing one of `sqlQuery` or `sqlFile`")
            }
            if (sqlQuery != null && sqlFile != null) {
                throw IllegalArgumentException("The statement '${id}' should have only one of `sqlQuery` or `sqlFile`")
            }
            if (sqlFile != null && !fileRelativeToConfig(sqlFile).exists()) {
                throw IllegalArgumentException("The statement '${id}' `sqlFile` must exist")
            }
        }
        
        val lastRuns: Map<String, Instant> = cfg.importSteps.map { importStep ->
            importStep.statements.map { statement ->
                val lastState = stateMap.get(statement.id)!!.readStateForStatement(uniqueId, statement)?.truncatedTo(ChronoUnit.SECONDS) ?: NOSTATE
                println("  Statement ${statement.id} - ${statement.description}")
                println("     LAST RUN: ${if (lastState == NOSTATE) "never" else lastState.toIsoString()}")

                statement.validate()
                statement.id to lastState
            }
        }.flatten().toMap()

        cfg.prepStatements?.forEach { statement ->
            if (statement.sqlQuery == null && statement.sqlFile == null) {
                throw IllegalArgumentException("A prepStatement is missing one of `sqlQuery` or `sqlFile`")
            }
            if (statement.sqlQuery != null && statement.sqlFile != null) {
                throw IllegalArgumentException("A prepStatement should have only one of `sqlQuery` or `sqlFile`")
            }
            if (statement.sqlFile != null && !fileRelativeToConfig(statement.sqlFile).exists()) {
                throw IllegalArgumentException("A prepStatement `sqlFile` must exist:  ${statement.sqlFile}")
            }
        }

        val thisRunDate = Instant.now().truncatedTo(ChronoUnit.SECONDS)

        val oldClassLoader = Thread.currentThread().contextClassLoader
        val extraClasspath = cfg.sources.jdbc?.map { it.driverJars }?.filterNotNull()?.flatten()?.map { URL("file://${File(it).normalize().absolutePath}") }?.toTypedArray() ?: emptyArray()
        val newClassLoader = URLClassLoader(extraClasspath, oldClassLoader)

        Thread.currentThread().contextClassLoader = newClassLoader
        try {
            SparkSession.builder()
                    .appName("esDataImport-${uniqueId}")
                    .config("spark.ui.enabled", false)
                    .apply {
                        cfg.sparkConfig?.forEach {
                            config(it.key,  it.value)
                        }
                    }
                    .master(sparkMaster).getOrCreate().use { spark ->

                // add extra UDF functions
                DataImportHandlerUdfs.registerSparkUdfs(spark)

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
                                .createOrReplaceTempView(table.sparkTable)
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
                                .createOrReplaceTempView(table.sparkTable)
                    }
                }

                // setup ES inputs
                println()
                cfg.sources.elasticsearch?.forEach { es ->
                    println("Mounting Elasticsearch source ${es.nodes.joinToString()}")
                    es.tables.forEach { table ->
                        val indexType = table.indexType ?: table.type
                        println("Creating table ${table.sparkTable} from Elasticsearch index ${table.indexName}/${indexType}")
                        if (indexType == null) {
                            throw IllegalArgumentException("  Source configuration is missing parameter `indexType`")
                        }
                        if (table.type != null) {
                            System.err.println("  Source configuration parameter `type` is deprecated, use `indexType`")
                        }
                        val options = mutableMapOf("es.nodes" to es.nodes.joinToString(","))
                        es.basicAuth?.let {
                            options.put("es.net.http.auth.user", it.username)
                            options.put("es.net.http.auth.pass", it.password)
                        }
                        es.port?.let {
                            options.put("es.port", it.toString())
                        }
                        es.enableSsl?.let {
                            options.put("es.net.ssl", it.toString())
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
                                .load(indexSpec(table.indexName, indexType))
                                .createOrReplaceTempView(table.sparkTable)
                    }
                }

                // run prep-queries
                println()
                cfg.prepStatements?.forEach { statement ->
                    try {
                        println("\nRunning prep-statement:\n${statement.description.replaceIndent("  ")}")
                        val rawQuery = statement.sqlQuery ?: fileRelativeToConfig(statement.sqlFile!!).readText()
                        spark.sql(rawQuery).let {
                            if (statement.cache ?: false) {
                                val storeLevel = statement.persist?.let { StorageLevel.fromString(it) }
                                if (storeLevel != null) {
                                    it.persist(storeLevel)
                                } else {
                                    it.cache()
                                }
                            } else {
                                it
                            }
                        }
                    } catch (ex: Throwable) {
                        val msg = ex.toNiceMessage()
                        throw DataImportException("Prep Statement: ${statement.description}\n$msg", ex)
                    }
                    println()
                }

                fun Importer.getDataImportHandler(statement: DataImportStatement): StatementDataImportHandler {
                    return if (targetElasticsearch != null) {
                        EsDataImportHandler(statement, configRelativeDir, targetElasticsearch)
                    } else if (targetAlgolia != null) {
                        AlgoliaDataImportHandler(statement, configRelativeDir, targetAlgolia)
                    } else {
                        throw IllegalStateException(description + " import step neither declares ES nor Algolia target")
                    }
                }

                // run importers
                println()
                cfg.importSteps.forEach { import ->
                    println("\nRunning importer:\n${import.description.replaceIndent("  ")}")
                    import.statements.forEach { statement ->
                        val stateMgr = stateMap.get(statement.id)!!
                        val lastRun  = lastRuns.get(statement.id)!!

                        // SQL times will be local time zone, so much match the server
                        val sqlMinDate = Timestamp.from(lastRun).toString()
                        val sqlMaxDate = Timestamp.from(thisRunDate).toString()

                        val dateMsg = if (lastRun == NOSTATE) {
                            "range NEVER to '$sqlMaxDate'"
                        } else {
                            "range '$sqlMinDate' to '$sqlMaxDate'"
                        }

                        println("\n    Execute statement:  ($dateMsg)\n${statement.description.replaceIndent("        ")}")

                        if (!stateMgr.lockStatement(uniqueId, statement)) {
                            System.err.println("        Cannot acquire lock for statement ${statement.id}")
                        } else {
                            try {
                                val importHandler = import.getDataImportHandler(statement)
                                importHandler.prepareIndex()

                                val rawQuery       = statement.sqlQuery ?: fileRelativeToConfig(statement.sqlFile!!).readText()
                                val subDataInQuery = rawQuery.replace("{lastRun}", sqlMinDate).replace("{thisRun}", sqlMaxDate)
                                val sqlResults     = try {
                                    spark.sql(subDataInQuery).let {
                                        if (statement.cache ?: false) {
                                            val storeLevel = statement.persist?.let { StorageLevel.fromString(it) }
                                            if (storeLevel != null) {
                                                it.persist(storeLevel)
                                            } else {
                                                it.cache()
                                            }
                                        } else {
                                            it
                                        }
                                    }
                                } catch (ex: Throwable) {
                                    val msg = ex.toNiceMessage()
                                    throw DataImportException(msg, ex)
                                }

                                val rowCount = importHandler.import(sqlResults)
                                println("        Rows processed: $rowCount")

                                stateMgr.writeStateForStatement(uniqueId, statement, thisRunDate, "success", rowCount, null)
                                stateMgr.logStatement(uniqueId, statement, thisRunDate, "success", rowCount, null)
                            } catch (ex: Throwable) {
                                val msg = ex.message ?: "unknown failure"
                                stateMgr.writeStateForStatement(uniqueId, statement, lastRun, "error", 0, msg)
                                stateMgr.logStatement(uniqueId, statement, thisRunDate, "error", 0, msg)
                                // System.err.println("\nProcess FAILED:  \n$msg\n")
                                throw ex
                            } finally {
                                stateMgr.unlockStatement(uniqueId, statement)
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

fun Throwable.toNiceMessage(): String = when (this) {
    is AnalysisException -> """Error:(${this.line().takeIf { it.isDefined }?.toString() ?: "?"},${this.startPosition().takeIf { it.isDefined }?.toString() ?: "?"}) ${this.message}"""
    is ParseException -> """Error:(${this.line().takeIf { it.isDefined }?.toString() ?: "?"},${this.startPosition().takeIf { it.isDefined }?.toString() ?: "?"}) ${this.message}"""
    else -> this.message ?: "unknown error"
}


fun indexSpec(indexName: String, type: String?): String {
    return "${indexName}/${type?.trim() ?: ""}"
}
