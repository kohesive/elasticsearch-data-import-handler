package uy.kohesive.elasticsearch.dataimport

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import java.io.File

interface StatementDataImportHandler {

    val configRelativeDir: File

    val statement: DataImportStatement

    fun prepareIndex()

    /**
     * Returns processed rows count.
     */
    fun import(dataSet: Dataset<Row>): Long

    fun fileRelativeToConfig(filename: String): File = configRelativeDir.resolve(filename).canonicalFile

}

