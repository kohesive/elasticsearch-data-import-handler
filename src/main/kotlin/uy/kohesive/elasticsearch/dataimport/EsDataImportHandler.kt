package uy.kohesive.elasticsearch.dataimport

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL
import java.io.File

class EsDataImportHandler(
    override val statement: DataImportStatement,
    override val configRelativeDir: File,

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

    override fun import(dataSet: Dataset<Row>): Long {
        JavaEsSparkSQL.saveToEs(dataSet, indexSpec(statement.indexName, statement.indexType ?: statement.type), options)
        return dataSet.count()
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