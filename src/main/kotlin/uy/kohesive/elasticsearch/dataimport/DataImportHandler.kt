package uy.kohesive.elasticsearch.dataimport

interface StatementDataImportHandler {

    val statement: DataImportStatement

    fun prepareIndex()

}

//class EsDataImportHandler : StatementDataImportHandler {
//
//
//
//}