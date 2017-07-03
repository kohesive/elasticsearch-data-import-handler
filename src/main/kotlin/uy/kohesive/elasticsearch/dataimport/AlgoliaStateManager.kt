package uy.kohesive.elasticsearch.dataimport

import com.algolia.search.APIClient
import com.algolia.search.ApacheAPIClientBuilder
import com.algolia.search.Index
import com.algolia.search.objects.IndexSettings
import com.algolia.search.objects.Query
import java.time.Instant

data class AlgoliaState(
    val targetIndex: String,
    val statementId: String,
    val status: String,
    val lastRunId: String,
    val lastErrorMsg: String?,
    val lastRunDate: Long,
    val lastRowCount: Long
)

data class AlgoliaLog(
    val targetIndex: String,
    val statementId: String,
    val runId: String,
    val status: String,
    val errorMsg: String?,
    val runDate: Long,
    val rowCount: Long
)

data class AlgoliaLock(
    val targetIndex: String,
    val statementId: String,
    val runId: String,
    val lockDate: Long
)

// TODO: this 'locking' mechanism is not safe, Algolia doesn't allow atomic updates
class AlgoliaStateManager(applicationId: String, apiKey: String) : StateManager {

    companion object {
        val StateIndex = "kohesive-dih-state"
        val LogIndex   = "kohesive-dih-log"
        val LockIndex  = "kohesive-dih-lock"
    }

    private lateinit var stateIndex: Index<AlgoliaState>
    private lateinit var logIndex: Index<AlgoliaLog>
    private lateinit var lockIndex: Index<AlgoliaLock>

    val algoliaClient: APIClient = ApacheAPIClientBuilder(applicationId, apiKey).build()

    override fun init() {
        fun <T> initIndex(indexName: String, clazz: Class<T>, settings: IndexSettings.() -> Unit): Index<T> {
            return algoliaClient.listIndices().firstOrNull { it.name == indexName }?.let {
                algoliaClient.initIndex(indexName, clazz)
            } ?: kotlin.run {
                algoliaClient.initIndex(indexName, clazz).apply {
                    this.settings = IndexSettings().apply {
                        settings()
                    }
                }
            }
        }

        stateIndex = initIndex(StateIndex, AlgoliaState::class.java) {
            searchableAttributes          = listOf("targetIndex", "statementId", "status", "lastRunId", "lastErrorMsg")
            numericAttributesForFiltering = listOf("lastRunDate", "lastRowCount")
        }
        logIndex = initIndex(LogIndex, AlgoliaLog::class.java) {
            searchableAttributes          = listOf("targetIndex", "statementId", "runId", "status", "errorMsg")
            numericAttributesForFiltering = listOf("runDate", "rowCount")
        }
        lockIndex = initIndex(LockIndex, AlgoliaLock::class.java) {
            searchableAttributes          = listOf("targetIndex", "statementId", "runId")
            numericAttributesForFiltering = listOf("lockDate")
        }

        ttlKillOldLocks()
    }

    override fun lockStatement(runId: String, statement: DataImportStatement): Boolean {
        if (lockIndex.getObject(statement.stateKey()).isPresent) {
            ttlKillOldLocks()
            return pingLockStatement(runId, statement)
        } else {
            lockIndex.addObject(statement.stateKey(), AlgoliaLock(
                targetIndex = statement.indexName,
                statementId = statement.id,
                runId       = runId,
                lockDate    = now()
            ))
            return true
        }
    }

    override fun pingLockStatement(runId: String, statement: DataImportStatement): Boolean {
        return lockIndex.getObject(statement.stateKey()).map { lock ->
            if (lock.runId == runId) {
                lockIndex.partialUpdateObject(statement.stateKey(), lock.copy(
                    lockDate = now()
                ))
                true
            } else {
                throw DataImportException("State manager failed, cannot acquire lock for ${statement.stateKey()} -- it is held by ${lock.runId} since ${lock.lockDate}")
            }
        }.orElse(false)
    }

    private fun now() = Instant.now().toEpochMilli()

    private fun ttlKillOldLocks() {
        try {
            lockIndex.deleteByQuery(Query().setNumericFilters(listOf("lockDate < ${ now() - (1000 * 60 * 15) }")))
        } catch (t: Throwable) {
            throw DataImportException("State manager failed, TTL delete query for locks failed", t)
        }
    }

    override fun unlockStatement(runId: String, statement: DataImportStatement) {
        if (pingLockStatement(runId, statement)) {
            try {
                lockIndex.deleteObject(statement.stateKey())
            } catch (t: Throwable) {
                throw DataImportException("State manager failed, cannot delete lock for ${statement.stateKey()}", t)
            }
        }
    }

    override fun writeStateForStatement(runId: String, statement: DataImportStatement, lastRunStart: Instant, status: String, lastRowCount: Long, errMsg: String?) {
        stateIndex.addObject(statement.stateKey(), AlgoliaState(
            targetIndex  = statement.indexName,
            statementId  = statement.id,
            lastErrorMsg = errMsg,
            lastRowCount = lastRowCount,
            lastRunDate  = lastRunStart.toEpochMilli(),
            lastRunId    = runId,
            status       = status
        ))
    }

    override fun readStateForStatement(runId: String, statement: DataImportStatement): Instant? {
        return stateIndex.getObject(statement.stateKey()).map { Instant.ofEpochMilli(it.lastRunDate) }.orElse(null)
    }

    override fun logStatement(runId: String, statement: DataImportStatement, lastRunStart: Instant, status: String, rowCount: Long, errMsg: String?) {
        logIndex.addObject("${statement.stateKey()}_run_${runId}", AlgoliaLog(
            targetIndex = statement.indexName,
            status      = status,
            statementId = statement.id,
            runId       = runId,
            errorMsg    = errMsg,
            rowCount    = rowCount,
            runDate     = lastRunStart.toEpochMilli()
        ))
    }
}