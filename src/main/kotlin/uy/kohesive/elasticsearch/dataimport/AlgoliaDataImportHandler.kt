package uy.kohesive.elasticsearch.dataimport

import org.apache.spark.TaskContext
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.elasticsearch.hadoop.cfg.PropertiesSettings
import org.elasticsearch.spark.cfg.SparkSettingsManager
import scala.collection.Iterator
import scala.runtime.AbstractFunction2
import java.io.File

class AlgoliaDataImportHandler(
    override val statement: DataImportStatement,
    override val configRelativeDir: File,

    targetAlgolia: AlgoliaTargetConnection
) : StatementDataImportHandler {

    val options = mutableMapOf(
        "algolia.write.applicationid" to targetAlgolia.applicationId,
        "algolia.write.apikey"        to targetAlgolia.apiKey,
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

    fun saveToAlgolia(ds: Dataset<Row>, cfg: Map<String, String>) {
        val sparkCtx = ds.sqlContext().sparkContext()
        val sparkCfg = SparkSettingsManager().load(sparkCtx.conf)

        val algoliaCfg = PropertiesSettings().load(sparkCfg.save())
        algoliaCfg.merge(cfg)

        val rdd = ds.toDF().rdd()

        sparkCtx.runJob<Row, Unit>(
            rdd,
            object : AbstractFunction2<TaskContext, Iterator<Row>, Unit>() {
                override fun apply(taskContext: TaskContext, data: Iterator<Row>) {
                    AlgoliaDataFrameWriter(ds.schema(), algoliaCfg.save()).write(taskContext, data)
                }
            },
            scala.reflect.`ClassTag$`.`MODULE$`.apply<Unit>(Unit::class.java)
        )
    }

}

class AlgoliaDataFrameWriter(val schema: StructType, val serializedSettings: String) {

    fun write(taskContext: TaskContext, data: Iterator<Row>) {
        while (data.hasNext()) {
            // TODO: implement
        }
    }

}