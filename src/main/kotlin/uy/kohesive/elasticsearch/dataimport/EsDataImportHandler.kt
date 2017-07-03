package uy.kohesive.elasticsearch.dataimport

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL
import java.io.File

class EsDataImportHandler(
    override val statement: DataImportStatement,
    override val configRelativeDir: File,

    val sqlMinDate: String,
    val sqlMaxDate: String,
    val spark: SparkSession,

    targetElasticsearch: EsTargetConnection
) : StatementDataImportHandler {

    val options = mutableMapOf("es.nodes" to targetElasticsearch.nodes.joinToString(","))

    init {
        // look for table create setting
        targetElasticsearch.basicAuth?.let {
            options.put("es.net.http.auth.user", it.username)
            options.put("es.net.http.auth.pass", it.password)
        }
        targetElasticsearch.port?.let { port ->
            options.put("es.port", port.toString())
        }
        targetElasticsearch.enableSsl?.let { enableSsl ->
            options.put("es.net.ssl", enableSsl.toString())
        }
        targetElasticsearch.settings?.let { options.putAll(it) }
        statement.settings?.let { options.putAll(it) }
    }

    val esClient = MicroEsClient(targetElasticsearch.nodes,
        targetElasticsearch.port ?: 9200,
        targetElasticsearch.enableSsl ?: false,
        targetElasticsearch.basicAuth
    )

    override fun import(): Long {
        val rawQuery = statement.sqlQuery ?: fileRelativeToConfig(statement.sqlFile!!).readText()
        val subDataInQuery = rawQuery.replace("{lastRun}", sqlMinDate).replace("{thisRun}", sqlMaxDate)
        val sqlResults = try {
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

        JavaEsSparkSQL.saveToEs(sqlResults, indexSpec(statement.indexName, statement.indexType ?: statement.type), options)
        return sqlResults.count()
    }

    override fun prepareIndex() {
        val autocreate: Boolean = options.getOrDefault("es.index.auto.create", "true").toBoolean()

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
    }
}