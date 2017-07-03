package uy.kohesive.elasticsearch.dataimport

import org.apache.spark.sql.SparkSession
import java.io.File

class AlgoliaDataImportHandler(
    override val statement: DataImportStatement,
    override val configRelativeDir: File,

    val sqlMinDate: String,
    val sqlMaxDate: String,
    val spark: SparkSession,

    targetAlgolia: AlgoliaTargetConnection
) : StatementDataImportHandler {

    override fun prepareIndex() {
        TODO("not implemented") // TODO: implement
    }

    override fun import(): Long {
        TODO("not implemented") // TODO: implement
    }
}