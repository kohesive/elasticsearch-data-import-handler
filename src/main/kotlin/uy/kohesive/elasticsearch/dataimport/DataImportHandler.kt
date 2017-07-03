package uy.kohesive.elasticsearch.dataimport

import java.io.File

interface StatementDataImportHandler {

    val configRelativeDir: File

    val statement: DataImportStatement

    fun prepareIndex()

    /**
     * Returns processed rows count.
     */
    fun import(): Long

    fun fileRelativeToConfig(filename: String): File = configRelativeDir.resolve(filename).canonicalFile

}

