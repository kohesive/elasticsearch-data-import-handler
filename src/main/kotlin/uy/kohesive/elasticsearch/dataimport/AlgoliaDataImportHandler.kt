package uy.kohesive.elasticsearch.dataimport

import com.algolia.search.APIClient
import com.algolia.search.ApacheAPIClientBuilder
import com.fasterxml.jackson.module.kotlin.readValue
import org.apache.spark.TaskContext
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.elasticsearch.hadoop.cfg.PropertiesSettings
import org.elasticsearch.hadoop.serialization.json.JacksonJsonGenerator
import org.elasticsearch.hadoop.util.FastByteArrayOutputStream
import org.elasticsearch.spark.cfg.SparkSettingsManager
import org.elasticsearch.spark.sql.DataFrameValueWriter
import scala.Tuple2
import scala.collection.Iterator
import scala.runtime.AbstractFunction2
import java.io.File
import java.io.Serializable

class AlgoliaDataImportHandler(
    override val statement: DataImportStatement,
    override val configRelativeDir: File,

    targetAlgolia: AlgoliaTargetConnection
) : StatementDataImportHandler {

    val options = mapOf(
        "algolia.write.applicationid" to targetAlgolia.applicationId,
        "algolia.write.apikey"        to targetAlgolia.apiKey,
        "algolia.write.idfield"       to statement.idField,
        "algolia.write.index"         to statement.indexName
    )

    override fun prepareIndex() {
        // TODO: implement
    }

    override fun import(dataSet: Dataset<Row>): Long {
        AlgoliaSparkSQL.saveToAlgolia(dataSet, options)
        return dataSet.count()
    }
}

object AlgoliaSparkSQL {

    fun saveToAlgolia(ds: Dataset<Row>, cfg: Map<String, String?>) {
        val sparkCtx = ds.sqlContext().sparkContext()
        val sparkCfg = SparkSettingsManager().load(sparkCtx.conf)

        val algoliaCfg = PropertiesSettings().load(sparkCfg.save())
        algoliaCfg.merge(cfg)

        val rdd = ds.toDF().rdd()

        val serializedSettings = algoliaCfg.save()
        val schema = ds.schema()

        sparkCtx.runJob<Row, Long>(
            rdd,
            object : AbstractFunction2<TaskContext, Iterator<Row>, Long>(), Serializable {
                override fun apply(taskContext: TaskContext, data: Iterator<Row>): Long {
                    AlgoliaDataFrameWriter(schema, serializedSettings).write(taskContext, data)
                    return 0L
                }
            },
            scala.reflect.`ClassTag$`.`MODULE$`.apply<Long>(Long::class.java)
        )
    }

}

class AlgoliaDataFrameWriter(val schema: StructType, serializedSettings: String) {

    companion object {
        val DefaultBulkSize = 50
    }

    private val settings = PropertiesSettings().load(serializedSettings)

    private val bulkSize = settings.getProperty("algolia.write.bulkSize")?.toInt() ?: DefaultBulkSize

    private val idField: String? = settings.getProperty("algolia.write.idfield")

    private val algoliaClient: APIClient = ApacheAPIClientBuilder(
        settings.getProperty("algolia.write.applicationid"),
        settings.getProperty("algolia.write.apikey")
    ).setObjectMapper(JSON).build()

    private val targetIndex = algoliaClient.initIndex(settings.getProperty("algolia.write.index"), Map::class.java)

    private val buffer = ArrayList<String>()

    private fun flush() {
        val objectsToWrite = buffer.map { rowStr ->
            JSON.readValue<Map<String, Any>>(rowStr).let { map ->
                if (idField != null) {
                    map + ("objectID" to map[idField])
                } else {
                    map
                }
            }
        }
        if (objectsToWrite.isNotEmpty()) {
            targetIndex.addObjects(objectsToWrite)
            buffer.clear()
        }
    }

    fun write(taskContext: TaskContext, data: Iterator<Row>) {
        fun tryFlush() {
            if (buffer.size >= bulkSize) {
                flush()
            }
        }

        while (data.hasNext()) {
            val row       = data.next()
            val out       = FastByteArrayOutputStream()
            val generator = JacksonJsonGenerator(out)

            generator.use { generator ->
                DataFrameValueWriter().write(Tuple2(row, schema), generator)
            }

            buffer.add(out.toString())
            tryFlush()
        }

        flush()
    }

}