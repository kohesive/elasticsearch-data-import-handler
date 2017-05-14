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
        val jsonCfg = hoconCfg.root().render(ConfigRenderOptions.concise().setJson(true))
        val cfg = jacksonObjectMapper().readValue<EsDataImportHandlerConfig>(jsonCfg)

        val uniqueId = UUID.randomUUID().toString()

        val sparkMaster = cfg.sparkMaster ?: "local[${(Runtime.getRuntime().availableProcessors() - 1).coerceAtLeast(1)}]"

        fun fileRelativeToConfig(filename: String): File {
            return configRelativeDir.resolve(filename).canonicalFile
        }

        println("Connecting to target ES clusters to check state...")
        val stateMap: Map<String, StateManager> = cfg.importSteps.map { importStep ->
            val mgr = ElasticSearchStateManager(importStep.targetElasticsearch.nodes, importStep.targetElasticsearch.port ?: 9200,
                    importStep.targetElasticsearch.enableSsl ?: false, importStep.targetElasticsearch.basicAuth)
            mgr.init()
            importStep.statements.map { statement ->
                statement.id to mgr
            }
        }.flatten().toMap()

        val NOSTATE = LocalDateTime.of(1900, 1, 1, 0, 0, 0, 0).atZone(ZoneOffset.UTC).toInstant()

        val lastRuns: Map<String, Instant> = cfg.importSteps.map { importStep ->
            importStep.statements.map { statement ->
                val lastState = stateMap.get(statement.id)!!.readStateForStatement(uniqueId, statement)?.truncatedTo(ChronoUnit.SECONDS) ?: NOSTATE
                println("  Statement ${statement.id} - ${statement.description}")
                println("     LAST RUN: ${if (lastState == NOSTATE) "never" else lastState.toIsoString()}")

                // do a little validation of the statement...
                if (statement.newIndexSettingsFile != null) {
                    val checkFile = fileRelativeToConfig(statement.newIndexSettingsFile)
                    if (!checkFile.exists()) {
                        throw IllegalStateException("The statement '${statement.id}' new-index mapping file must exist: $checkFile")
                    }
                }
                if (statement.indexType == null && statement.type == null) {
                    throw IllegalArgumentException("The statement '${statement.id}' is missing `indexType`")
                }
                if (statement.type != null && statement.indexType == null) {
                    System.err.println("     Statement configuration parameter `type` is deprecated, use `indexType`")
                }
                if (statement.sqlQuery == null && statement.sqlFile == null) {
                    throw IllegalArgumentException("The statement '${statement.id}' is missing one of `sqlQuery` or `sqlFile`")
                }
                if (statement.sqlQuery != null && statement.sqlFile != null) {
                    throw IllegalArgumentException("The statement '${statement.id}' should have only one of `sqlQuery` or `sqlFile`")
                }
                if (statement.sqlFile != null && !fileRelativeToConfig(statement.sqlFile).exists()) {
                    throw IllegalArgumentException("The statement '${statement.id}' `sqlFile` must exist")
                }
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

                fun indexSpec(indexName: String, type: String?): String {
                    return "${indexName}/${type?.trim() ?: ""}"
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
                        val rawQuery = statement.sqlQuery ?: fileRelativeToConfig(statement.sqlFile!!).readText()
                        spark.sql(rawQuery)
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
                                // look for table create setting

                                val options = mutableMapOf("es.nodes" to import.targetElasticsearch.nodes.joinToString(","))
                                import.targetElasticsearch.basicAuth?.let {
                                    options.put("es.net.http.auth.user", it.username)
                                    options.put("es.net.http.auth.pass", it.password)
                                }
                                import.targetElasticsearch.port?.let { port ->
                                    options.put("es.port", port.toString())
                                }
                                import.targetElasticsearch.enableSsl?.let { enableSsl ->
                                    options.put("es.net.ssl", enableSsl.toString())
                                }
                                import.targetElasticsearch.settings?.let { options.putAll(it) }
                                statement.settings?.let { options.putAll(it) }

                                val autocreate: Boolean = options.getOrDefault("es.index.auto.create", "true").toBoolean()
                                val esClient = MicroEsClient(import.targetElasticsearch.nodes,
                                        import.targetElasticsearch.port ?: 9200,
                                        import.targetElasticsearch.enableSsl ?: false,
                                        import.targetElasticsearch.basicAuth)
                                val indexExists = esClient.checkIndexExists(statement.indexName)
                                if (!autocreate) {
                                    if (!indexExists) {
                                        throw IllegalStateException("Index auto-create setting 'es.index.auto.create' is false and index ${statement.indexName} does not exist.")
                                    }
                                } else {
                                    if (!indexExists) {
                                        println("        Index ${statement.indexName} does not exist, auto creating")
                                        if (statement.newIndexSettingsFile != null) {
                                            val checkFile = fileRelativeToConfig(statement.newIndexSettingsFile)
                                            println("        Creating ${statement.indexName} with settings/mapping file: $checkFile")
                                            val response = esClient.createIndex(statement.indexName, checkFile.readText())
                                            if (!response.isSuccess) {
                                                throw IllegalStateException("Could not create index ${statement.indexName} with settings/mapping file: $checkFile, due to:\n${response.responseJson}")
                                            }
                                        }  else {
                                            println("        Index will be created without settings/mappings file, will use index templates or dynamic mappings")
                                        }
                                    }
                                }

                                val rawQuery = statement.sqlQuery ?: fileRelativeToConfig(statement.sqlFile!!).readText()
                                val subDataInQuery = rawQuery.replace("{lastRun}", sqlMinDate).replace("{thisRun}", sqlMaxDate)
                                val sqlResults = try {
                                    spark.sql(subDataInQuery).cache()
                                } catch (ex: Throwable) {
                                    val msg = ex.toNiceMessage()
                                    throw DataImportException(msg, ex)
                                }

                                JavaEsSparkSQL.saveToEs(sqlResults, indexSpec(statement.indexName, statement.indexType ?: statement.type), options)
                                val rowCount = sqlResults.count()
                                println("        Rows processed: $rowCount")

                                stateMgr.writeStateForStatement(uniqueId, statement, thisRunDate, "success", rowCount, null)
                                stateMgr.logStatement(uniqueId, statement, thisRunDate, "sucess", rowCount, null)
                            } catch (ex: Throwable) {
                                val msg = ex.message ?: "unknown failure"
                                stateMgr.writeStateForStatement(uniqueId, statement, lastRun, "error", 0, msg)
                                stateMgr.logStatement(uniqueId, statement, thisRunDate, "error", 0, msg)
                                // System.err.println("\nProcess FAILED:  \n$msg\n")
                                throw ex
                            } finally {
                                stateMgr.unlockStatemnt(uniqueId, statement)
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