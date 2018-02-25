package uy.kohesive.elasticsearch.dataimport

import com.fasterxml.jackson.module.kotlin.readValue
import okhttp3.*

/**
 * TODO:  Change to use RestClient from ES / Spark integration
 */

class MicroEsClient(nodes: List<String>, port: Int = 9200, enableSsl: Boolean = false, auth: AuthInfo? = null) {
    val http = OkHttpClient().newBuilder().apply {
        auth?.let { addInterceptor(BasicAuthInterceptor(it)) }
    }.build()
    val protocol = if (enableSsl) "https" else "http"
    val host = nodes.first()
    val hostWithPort = if (':' in host.substringAfter('@', host)) host else "${host}:${port}"
    val url = "${protocol}://${hostWithPort}"

    fun String.fixRestAppendage(): String {
        if (this.startsWith("?")) return this
        if (this.startsWith("/")) return this
        return "/" + this
    }

    fun makeIndexTypeUrl(index: String, type: String) = "$url/$index/$type"
    fun makeIndexTypeUrl(index: String, type: String, restOfUrl: String) = "$url/$index/$type${restOfUrl.fixRestAppendage()}"
    fun makeIndexTypeIdUrl(index: String, type: String, id: String, restOfUrl: String) = "$url/$index/$type/$id${restOfUrl.fixRestAppendage()}"

    private fun OkHttpClient.get(url: String): CallResponse {
        val request = Request.Builder().url(url).build()
        val response = http.newCall(request).execute()
        return CallResponse(response.code(), response.use { it.body()?.string() ?: "" })
    }

    private fun OkHttpClient.delete(url: String): CallResponse {
        val request = Request.Builder().url(url).delete().build()
        val response = http.newCall(request).execute()
        return CallResponse(response.code(), response.use { it.body()?.string() ?: "" })
    }

    private fun OkHttpClient.delete(url: String, jsonBody: String): CallResponse {
        val jsonMediaType = MediaType.parse("application/json; charset=utf-8")
        val body = RequestBody.create(jsonMediaType, jsonBody)
        val request = Request.Builder().url(url).delete(body).build()
        val response = http.newCall(request).execute()
        return CallResponse(response.code(), response.use { it.body()?.string() ?: "" })
    }

    private fun OkHttpClient.post(url: String, jsonBody: String): CallResponse {
        val jsonMediaType = MediaType.parse("application/json; charset=utf-8")
        val body = RequestBody.create(jsonMediaType, jsonBody)
        val request = Request.Builder().url(url).post(body).build()
        val response = http.newCall(request).execute()
        return CallResponse(response.code(), response.use { it.body()?.string() ?: "" })
    }

    private fun OkHttpClient.put(url: String, jsonBody: String): CallResponse {
        val jsonMediaType = MediaType.parse("application/json; charset=utf-8")
        val body = RequestBody.create(jsonMediaType, jsonBody)
        val request = Request.Builder().url(url).put(body).build()
        val response = http.newCall(request).execute()
        return CallResponse(response.code(), response.use { it.body()?.string() ?: "" })
    }

    fun indexTypePOST(indexName: String, indexType: String, restOfUrl: String, postJson: String): CallResponse {
        return http.post(makeIndexTypeUrl(indexName, indexType, restOfUrl), postJson)
    }

    fun indexTypeIdPOST(indexName: String, indexType: String, id: String, restOfUrl: String, postJson: String): CallResponse {
        return http.post(makeIndexTypeIdUrl(indexName, indexType, id, restOfUrl), postJson)
    }

    fun indexTypeDELETE(indexName: String, indexType: String, restOfUrl: String): CallResponse {
        return http.delete(makeIndexTypeUrl(indexName, indexType, restOfUrl))
    }

    fun indexTypeIdDELETE(indexName: String, indexType: String, id: String, restOfUrl: String): CallResponse {
        return http.delete(makeIndexTypeIdUrl(indexName, indexType, id, restOfUrl))
    }

    fun indexTypeGET(indexName: String, indexType: String): CallResponse {
        return http.get(makeIndexTypeUrl(indexName, indexType))
    }

    fun indexTypeGET(indexName: String, indexType: String, restOfUrl: String): CallResponse {
        return http.get(makeIndexTypeUrl(indexName, indexType, restOfUrl))
    }

    fun indexTypeIdGET(indexName: String, indexType: String, id: String): CallResponse {
        return http.get(makeIndexTypeUrl(indexName, indexType, id))
    }

    fun indexTypeIdGET(indexName: String, indexType: String, id: String, restOfUrl: String): CallResponse {
        return http.get(makeIndexTypeIdUrl(indexName, indexType, id, restOfUrl))
    }

    fun createIndex(indexName: String, settingsJson: String): CallResponse {
        return http.put("${url}/${indexName}", settingsJson)
    }

    fun waitForIndexGreen(indexName: String) {
        val response = http.get("${url}/_cluster/health/${indexName}?wait_for_status=green&timeout=10s")
        if (!response.isSuccess) throw DataImportException("State manager failed, cannot check state index status")
        val state = JSON.readTree(response.responseJson)
        if (state.get("timed_out").asBoolean()) throw DataImportException("State manager failed, timeout waiting on state index to be 'green'")
        if (state.get("status").asText() != "green") throw DataImportException("State manager failed, state index must be 'green' but was '${state.get("status")}'")
    }

    fun checkIndexExists(indexName: String): Boolean {
        return http.get("${url}/${indexName}").isSuccess
    }

    inline fun <reified T : Any> mapFromSource(response: String): T = JSON.readTree(response).get("_source").traverse().let { JSON.readValue<T>(it) }!!

    data class CallResponse(val code: Int, val responseJson: String) {
        val isSuccess: Boolean get() = code in 200..299
    }
}

class BasicAuthInterceptor(val authInfo: AuthInfo) : Interceptor {
    override fun intercept(chain: Interceptor.Chain): Response {
        val request = chain.request()
        val requestWithAuth = request.newBuilder().header("Authorization", Credentials.basic(authInfo.username, authInfo.password)).build()
        return chain.proceed(requestWithAuth)
    }
}