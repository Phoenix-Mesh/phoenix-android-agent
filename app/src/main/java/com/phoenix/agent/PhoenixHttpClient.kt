// SPDX-License-Identifier: Apache-2.0

package com.phoenix.agent

import java.io.IOException
import java.io.InputStream
import java.net.HttpURLConnection
import java.net.URI

/** A non-2xx response from Phoenix Core. */
class HttpStatusException(val code: Int, val body: String) : IOException("HTTP $code: $body")

/**
 * Minimal blocking JSON-over-HTTP client for the Phoenix node-agent protocol.
 *
 * Every call opens a fresh connection and always disconnects it.
 */
open class PhoenixHttpClient(private val timeoutMs: Int = 5_000) {

    /**
     * POSTs [body] as JSON and returns the response body.
     *
     * @throws HttpStatusException for any non-2xx response
     */
    open fun postJson(endpoint: String, body: String): String {
        val conn = open(endpoint, "POST")
        try {
            conn.setRequestProperty("Content-Type", "application/json")
            conn.doOutput = true
            conn.outputStream.use { it.write(body.toByteArray(Charsets.UTF_8)) }

            val code = conn.responseCode
            if (code !in 200..299) {
                throw HttpStatusException(code, read(conn.errorStream))
            }
            return read(conn.inputStream)
        } finally {
            conn.disconnect()
        }
    }

    /** GETs [endpoint]; the body is read only for 200, otherwise it is empty. */
    open fun get(endpoint: String): Pair<Int, String> {
        val conn = open(endpoint, "GET")
        try {
            val code = conn.responseCode
            return code to if (code == 200) read(conn.inputStream) else ""
        } finally {
            conn.disconnect()
        }
    }

    private fun open(endpoint: String, method: String): HttpURLConnection =
        (URI.create(endpoint).toURL().openConnection() as HttpURLConnection).apply {
            requestMethod = method
            connectTimeout = timeoutMs
            readTimeout = timeoutMs
        }

    private fun read(stream: InputStream?): String =
        stream?.bufferedReader(Charsets.UTF_8)?.use { it.readText() } ?: ""
}
