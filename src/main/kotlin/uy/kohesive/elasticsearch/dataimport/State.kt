package uy.kohesive.elasticsearch.dataimport

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import okhttp3.MediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import java.time.Instant

interface StateManager {
    fun init()
    fun lockStatement(runId: String, statementId: String): Boolean
    fun pingLockStatement(runId: String, statementId: String): Boolean
    fun unlockStatemnt(runId: String, statementId: String)
    fun writeStateForStatement(runId: String, statementId: String, lastRunStart: Instant, status: String, lastRowCount: Long, errMsg: String? = null)
    fun readStateForStatement(runId: String, statementId: String): Instant?
    fun logStatement(runId: String, statementId: String, lastRunStart: Instant, status: String, rowCount: Long, errMsg: String? = null)
}

// TODO: better state management
// This is NOT using the ES client because we do not want conflicts with Spark dependencies
class ElasticSearchStateManager(val nodes: List<String>, val auth: AuthInfo?) : StateManager {
    val http = OkHttpClient()
    val url = if (auth != null) "http://${auth.username}:${auth.password}@${nodes.first()}" else "http://${nodes.first()}"
    val JSON = jacksonObjectMapper().registerModules(JavaTimeModule(), Jdk8Module()).apply {
        configure(SerializationFeature.WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS, false)
        configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, true)
        configure(DeserializationFeature.READ_DATE_TIMESTAMPS_AS_NANOSECONDS, false)
    }
    val STATE_INDEX = ".kohesive-dih-state"

    private fun OkHttpClient.get(url: String): Pair<Int, String> {
        val request = Request.Builder().url(url).build()
        val response = http.newCall(request).execute()
        return response.code() to response.use { it.body().string() }
    }

    private fun OkHttpClient.delete(url: String): Pair<Int, String> {
        val request = Request.Builder().url(url).delete().build()
        val response = http.newCall(request).execute()
        return response.code() to response.use { it.body().string() }
    }

    private fun OkHttpClient.delete(url: String, jsonBody: String): Pair<Int, String> {
        val jsonMediaType = MediaType.parse("application/json; charset=utf-8")
        val body = RequestBody.create(jsonMediaType, jsonBody)
        val request = Request.Builder().url(url).delete(body).build()
        val response = http.newCall(request).execute()
        return response.code() to response.use { it.body().string() }
    }

    private fun OkHttpClient.post(url: String, jsonBody: String): Pair<Int, String> {
        val jsonMediaType = MediaType.parse("application/json; charset=utf-8")
        val body = RequestBody.create(jsonMediaType, jsonBody)
        val request = Request.Builder().url(url).post(body).build()
        val response = http.newCall(request).execute()
        return response.code() to response.use { it.body().string() }
    }

    private fun OkHttpClient.put(url: String, jsonBody: String): Pair<Int, String> {
        val jsonMediaType = MediaType.parse("application/json; charset=utf-8")
        val body = RequestBody.create(jsonMediaType, jsonBody)
        val request = Request.Builder().url(url).put(body).build()
        val response = http.newCall(request).execute()
        return response.code() to response.use { it.body().string() }
    }

    private fun waitForIndexGreen(indexName: String) {
        val (code, response) = http.get("${url}/_cluster/health/${indexName}?wait_for_status=green&timeout=10s")
        if (!code.isSuccess()) throw DataImportException("State manager failed, cannot check state index status")
        val state = JSON.readTree(response)
        if (state.get("timed_out").asBoolean()) throw DataImportException("State manager failed, timeout waiting on state index to be 'green'")
        if (state.get("status").asText() != "green") throw DataImportException("State manager failed, state index must be 'green' but was '${state.get("status")}'")
    }

    private fun checkIndexExists(indexName: String): Boolean {
        val (code, _) = http.get("${url}/${indexName}")
        return code.isSuccess()
    }

    private fun Int.isSuccess(): Boolean = this in 200..299

    override fun init() {
        if (checkIndexExists(STATE_INDEX)) {
            waitForIndexGreen(STATE_INDEX)
        } else {
            val (code, response) = http.put("${url}/${STATE_INDEX}", """
               {
                  "settings": {
                      "number_of_shards": 1,
                      "number_of_replicas": "0"
                  },
                  "mappings": {
                     "state": {
                         "properties": {
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
                             "statementId": { "type": "keyword" },
                             "runId": { "type": "keyword" },
                             "lockDate": { "type": "date" }
                        }
                     }
                  }
               }
            """)

            if (code.isSuccess()) {
                waitForIndexGreen(STATE_INDEX)
            } else {
                throw DataImportException("State manager failed, cannot create state index\n$response")
            }
        }

        ttlKillOldLocks()
    }

    data class Lock(val runId: String, val statementId: String, val lockDate: Instant)

    fun makeUrl(index: String, type: String) = "$url/$index/$type"
    fun makeUrl(type: String) = makeUrl(STATE_INDEX, type)
    inline fun <reified T : Any> mapFromSource(response: String): T = JSON.readTree(response).get("_source").traverse().let { JSON.readValue<T>(it) }!!

    private fun ttlKillOldLocks() {
        val (delCode, response) = http.post("${makeUrl("lock")}/_delete_by_query?refresh",
                """
                       { "query": { "range": { "lockDate": { "lt": "now-15m" } } } }
                    """)
        if (!delCode.isSuccess()) throw DataImportException("State manager failed, TTL delete query for locks failed\n$response")
    }

    override fun lockStatement(runId: String, statementId: String): Boolean {
        val lockUrl = "${makeUrl("lock")}/${statementId}"
        val (code, response) = http.post("$lockUrl?op_type=create", JSON.writeValueAsString(Lock(runId, statementId, Instant.now())))

        if (!code.isSuccess()) {
            ttlKillOldLocks()
            return pingLockStatement(runId, statementId)
        }
        return true
    }

    override fun pingLockStatement(runId: String, statementId: String): Boolean {
        val lockUrl = "${makeUrl("lock")}/${statementId}"
        val (getCode, response) = http.get(lockUrl)
        if (getCode.isSuccess()) {
            val lock = mapFromSource<Lock>(response)
            if (lock.runId == runId) {
                // TODO: we did a get, so have the version, change to a update with version ID
                val (updCode, updResponse) = http.post("${makeUrl("lock")}/_update_by_query?refresh", """
                    {
                        "script": {
                            "inline": "ctx._source.lockDate = Instant.ofEpochMilli(${Instant.now().toEpochMilli()}L)",
                            "lang": "painless"
                        },
                        "query": {
                            "bool": {
                               "must": [
                                  { "term": { "runId": "$runId" } },
                                  { "term": { "statementId": "$statementId" } }
                               ]
                            }
                        }
                    }
                """)
                if (!updCode.isSuccess()) {
                    throw DataImportException("State manager failed, cannot acquire lock for $statementId - had conflict on pinging of lock\n$updResponse")
                }
            } else {
                throw DataImportException("State manager failed, cannot acquire lock for $statementId -- it is held by ${lock.runId} since ${lock.lockDate.toIsoString()}\n$response")
            }
        }
        return true
    }

    override fun unlockStatemnt(runId: String, statementId: String) {
        if (pingLockStatement(runId, statementId)) {
            val lockUrl = "${makeUrl("lock")}/${statementId}?refresh"
            val (code, response) = http.delete(lockUrl)
            if (!code.isSuccess()) {
                throw DataImportException("State manager failed, cannot delete lock for $statementId\n$response")
            }
        }
    }

    data class State(val statementId: String, val lastRunDate: Instant, val status: String, val lastRunId: String, val lastErrorMesasge: String?, val lastRowCount: Long)

    data class StateLog(val statementId: String, val runId: String, val runDate: Instant, val status: String, val errorMsg: String?, val rowCount: Long)

    override fun writeStateForStatement(runId: String, statementId: String, lastRunStart: Instant, status: String, lastRowCount: Long, errMsg: String?) {
        val (code, response) = http.post("${makeUrl("state")}/${statementId}?refresh", JSON.writeValueAsString(State(statementId, lastRunStart, status, runId, errMsg, lastRowCount)))
        if (!code.isSuccess()) {
            throw DataImportException("State manager failed, cannot update state for $statementId\n$response")
        }
    }

    override fun readStateForStatement(runId: String, statementId: String): Instant? {
        val (code, response) = http.get("${makeUrl("state")}/${statementId}")
        if (code.isSuccess()) {
            val state = mapFromSource<State>(response)
            return state.lastRunDate
        } else {
            return null
        }
    }

    override fun logStatement(runId: String, statementId: String, lastRunStart: Instant, status: String, rowCount: Long, errMsg: String?) {
        val (code, response) = http.post("${makeUrl("log")}/${statementId}_run_${runId}?refresh", JSON.writeValueAsString(StateLog(statementId, runId, lastRunStart, status, errMsg, rowCount)))
        if (!code.isSuccess()) {
            throw DataImportException("State manager failed, cannot log state for $statementId\n$response")
        }
    }
}
