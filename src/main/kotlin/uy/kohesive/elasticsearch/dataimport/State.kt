package uy.kohesive.elasticsearch.dataimport

import java.time.Instant

interface StateManager {
    fun init()
    fun lockStatement(runId: String, statement: DataImportStatement): Boolean
    fun pingLockStatement(runId: String, statement: DataImportStatement): Boolean
    fun unlockStatement(runId: String, statement: DataImportStatement)
    fun writeStateForStatement(runId: String, statement: DataImportStatement, lastRunStart: Instant, status: String, lastRowCount: Long, errMsg: String? = null)
    fun readStateForStatement(runId: String, statement: DataImportStatement): Instant?
    fun logStatement(runId: String, statement: DataImportStatement, lastRunStart: Instant, status: String, rowCount: Long, errMsg: String? = null)
}

private fun DataImportStatement.stateKey(): String = this.indexName + "-" + this.id

// TODO: implement
class AlgoliaStateManager(val applicationId: String, apiKey: String) : StateManager {
    override fun init() {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun lockStatement(runId: String, statement: DataImportStatement): Boolean {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun pingLockStatement(runId: String, statement: DataImportStatement): Boolean {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun unlockStatement(runId: String, statement: DataImportStatement) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun writeStateForStatement(runId: String, statement: DataImportStatement, lastRunStart: Instant, status: String, lastRowCount: Long, errMsg: String?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun readStateForStatement(runId: String, statement: DataImportStatement): Instant? {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun logStatement(runId: String, statement: DataImportStatement, lastRunStart: Instant, status: String, rowCount: Long, errMsg: String?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}

// TODO: better state management
// This is NOT using the ES client because we do not want conflicts with Spark dependencies
class ElasticSearchStateManager(val nodes: List<String>, val port: Int = 9200, val enableSsl: Boolean = false, val auth: AuthInfo?) : StateManager {
    val STATE_INDEX = ".kohesive-dih-state-v2"
    val esClient = MicroEsClient(nodes, port, enableSsl, auth)

    override fun init() {
        if (esClient.checkIndexExists(STATE_INDEX)) {
            esClient.waitForIndexGreen(STATE_INDEX)
        } else {
            val response = esClient.createIndex(STATE_INDEX, """
               {
                  "settings": {
                      "number_of_shards": 1,
                      "number_of_replicas": "0"
                  },
                  "mappings": {
                     "state": {
                         "properties": {
                             "targetIndex": { "type": "keyword" },
                             "statementId": { "type": "keyword" },
                             "lastRunDate": { "type": "date" },
                             "status": { "type": "keyword" },
                             "lastRunId": { "type": "keyword" },
                             "lastErrorMsg": { "type": "text" },
                             "lastRowCount": { "type": "long" }
                         }
                     },
                     "log": {
                        "properties": {
                             "targetIndex": { "type": "keyword" },
                             "statementId": { "type": "keyword" },
                             "runId": { "type": "keyword" },
                             "runDate": { "type": "date" },
                             "status": { "type": "keyword" },
                             "errorMsg": { "type": "text" },
                             "rowCount": { "type": "long" }
                        }
                     },
                     "lock": {
                        "properties": {
                             "targetIndex": { "type": "keyword" },
                             "statementId": { "type": "keyword" },
                             "runId": { "type": "keyword" },
                             "lockDate": { "type": "date" }
                        }
                     }
                  }
               }
            """)

            if (response.isSuccess) {
                esClient.waitForIndexGreen(STATE_INDEX)
            } else {
                throw DataImportException("State manager failed, cannot create state index\n${response.responseJson}")
            }
        }

        ttlKillOldLocks()
    }

    data class Lock(val runId: String, val targetIndex: String, val statementId: String, val lockDate: Instant)

    private fun ttlKillOldLocks() {
        val response = esClient.indexTypePOST(STATE_INDEX, "lock", "/_delete_by_query?refresh",
                """
                       { "query": { "range": { "lockDate": { "lt": "now-15m" } } } }
                    """)
        if (!response.isSuccess) throw DataImportException("State manager failed, TTL delete query for locks failed\n${response.responseJson}")
    }

    override fun lockStatement(runId: String, statement: DataImportStatement): Boolean {
        val response = esClient.indexTypeIdPOST(STATE_INDEX, "lock", statement.stateKey(), "?op_type=create",
                JSON.writeValueAsString(Lock(runId, statement.indexName, statement.id, Instant.now())))

        if (!response.isSuccess) {
            ttlKillOldLocks()
            return pingLockStatement(runId, statement)
        }
        return true
    }

    override fun pingLockStatement(runId: String, statement: DataImportStatement): Boolean {
        val response = esClient.indexTypeIdGET(STATE_INDEX, "lock", statement.stateKey())
        if (response.isSuccess) {
            val lock = esClient.mapFromSource<Lock>(response.responseJson)
            if (lock.runId == runId) {
                // TODO: we did a get, so have the version, change to a update with version ID
                val updResponse = esClient.indexTypePOST(STATE_INDEX, "lock", "/_update_by_query?refresh", """
                    {
                        "script": {
                            "inline": "ctx._source.lockDate = Instant.ofEpochMilli(${Instant.now().toEpochMilli()}L)",
                            "lang": "painless"
                        },
                        "query": {
                            "bool": {
                               "must": [
                                  { "term": { "runId": "$runId" } },
                                  { "term": { "targetIndex": "${statement.indexName}" } },
                                  { "term": { "statementId": "${statement.id}" } }
                               ]
                            }
                        }
                    }
                """)
                if (!updResponse.isSuccess) {
                    throw DataImportException("State manager failed, cannot acquire lock for ${statement.stateKey()} - had conflict on pinging of lock\n${updResponse.responseJson}")
                }
            } else {
                throw DataImportException("State manager failed, cannot acquire lock for ${statement.stateKey()} -- it is held by ${lock.runId} since ${lock.lockDate.toIsoString()}\n${response.responseJson}")
            }
        }
        return true
    }

    override fun unlockStatement(runId: String, statement: DataImportStatement) {
        if (pingLockStatement(runId, statement)) {
            val response =  esClient.indexTypeIdDELETE(STATE_INDEX, "lock", statement.stateKey(), "?refresh")
            if (!response.isSuccess) {
                throw DataImportException("State manager failed, cannot delete lock for ${statement.stateKey()}\n${response.responseJson}")
            }
        }
    }

    data class State(val targetIndex: String, val statementId: String, val lastRunDate: Instant, val status: String, val lastRunId: String, val lastErrorMesasge: String?, val lastRowCount: Long)

    data class StateLog(val targetIndex: String, val statementId: String, val runId: String, val runDate: Instant, val status: String, val errorMsg: String?, val rowCount: Long)

    override fun writeStateForStatement(runId: String, statement: DataImportStatement, lastRunStart: Instant, status: String, lastRowCount: Long, errMsg: String?) {
        val response = esClient.indexTypeIdPOST(STATE_INDEX, "state", statement.stateKey(), "?refresh",
                JSON.writeValueAsString(State(statement.indexName, statement.id, lastRunStart, status, runId, errMsg, lastRowCount)))
        if (!response.isSuccess) {
            throw DataImportException("State manager failed, cannot update state for ${statement.stateKey()}\n${response.responseJson}")
        }
    }

    override fun readStateForStatement(runId: String, statement: DataImportStatement): Instant? {
        val response = esClient.indexTypeIdGET(STATE_INDEX, "state", statement.stateKey())
        if (response.isSuccess) {
            val state = esClient.mapFromSource<State>(response.responseJson)
            return state.lastRunDate
        } else {
            return null
        }
    }

    override fun logStatement(runId: String, statement: DataImportStatement, lastRunStart: Instant, status: String, rowCount: Long, errMsg: String?) {
        val response = esClient.indexTypeIdPOST(STATE_INDEX, "log", "${statement.stateKey()}_run_${runId}", "?refresh",
                JSON.writeValueAsString(StateLog(statement.indexName, statement.id, runId, lastRunStart, status, errMsg, rowCount)))
        if (!response.isSuccess) {
            throw DataImportException("State manager failed, cannot log state for ${statement.stateKey()}\n${response.responseJson}")
        }
    }
}
