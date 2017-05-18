package uy.kohesive.elasticsearch.dataimport


data class EsDataImportHandlerConfig(val sparkMaster: String? = null,
                                     val sources: Connections,
                                     val prepStatements: List<PrepStatement>? = null,
                                     val importSteps: List<Importer>,
                                     val sparkConfig: Map<String, String>? = null)

data class AuthInfo(val username: String, val password: String)

data class Connections(val elasticsearch: List<EsConnection>?,
                       val jdbc: List<JdbcConnection>? = null,
                       val filesystem: List<FileDir>? = null)

data class EsConnection(val nodes: List<String>,
                        val basicAuth: AuthInfo? = null,
                        val port: Int? = 9200,
                        val enableSsl: Boolean? = false,
                        val tables: List<EsSource>,
                        val settings: Map<String, String>? = null) {
}

data class JdbcConnection(val jdbcUrl: String,
                          val driverClass: String,
                          val defaultSchema: String,
                          val auth: AuthInfo,
                          val driverJars: List<String>? = null,
                          val tables: List<JdbcSource>,
                          val settings: Map<String, String>? = null)

data class FileDir(val directory: String,
                   val tables: List<FileSource>,
                   val settings: Map<String, String>? = null)

data class JdbcSource(val sparkTable: String,
                      val sourceTable: String,
                      val settings: Map<String, String>? = null)

data class FileSource(val sparkTable: String, val format: String, val filespecs: List<String>,
                      val settings: Map<String, String>? = null)

data class EsSource(val sparkTable: String, val indexName: String, val type: String?, val indexType: String?, val esQuery: Any? = null,
                    val settings: Map<String, String>? = null)

data class PrepStatement(val description: String, val sqlQuery: String?, val sqlFile: String?, val cache: Boolean? = null, val persist: String? = "MEMORY_ONLY")

data class Importer(val description: String, val targetElasticsearch: EsTargetConnection, val statements: List<EsImportStatement>)
data class EsTargetConnection(val nodes: List<String>,
                              val basicAuth: AuthInfo? = null,
                              val port: Int? = 9200,
                              val enableSsl: Boolean? = false,
                              val settings: Map<String, String>? = null) {

}

data class EsImportStatement(val id: String, val description: String,
                             val indexName: String, val indexType: String?,
                             val type: String?,
                             val newIndexSettingsFile: String?,
                             val sqlQuery: String?,
                             val sqlFile: String?,
                             val cache: Boolean? = null,
                             val persist: String? = "MEMORY_ONLY",
                             val settings: Map<String, String>? = null)

