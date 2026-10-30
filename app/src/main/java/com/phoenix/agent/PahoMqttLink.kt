// SPDX-License-Identifier: Apache-2.0

package com.phoenix.agent

import org.eclipse.paho.client.mqttv3.DisconnectedBufferOptions
import org.eclipse.paho.client.mqttv3.IMqttActionListener
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken
import org.eclipse.paho.client.mqttv3.IMqttToken
import org.eclipse.paho.client.mqttv3.MqttAsyncClient
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended
import org.eclipse.paho.client.mqttv3.MqttConnectOptions
import org.eclipse.paho.client.mqttv3.MqttException
import org.eclipse.paho.client.mqttv3.MqttMessage
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
import java.security.cert.CertificateException
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import javax.net.ssl.SSLHandshakeException
import javax.net.ssl.SSLPeerUnverifiedException

/**
 * [MqttLink] on Eclipse Paho 1.2.5 (plain Java client; the deprecated Paho
 * Android service is not used).
 *
 * - MQTT 3.1.1, persistent session (`cleanSession=false`), so tasks published
 *   while the device is briefly away are queued by the broker.
 * - Retained last will on `up/status`, as the protocol requires.
 * - TLS through [MqttTls], with [TlsHostnameVerifier] checking the host name.
 * - The first connection is retried here with a capped back-off; once
 *   connected, Paho reconnects on its own. Bad credentials and failed TLS
 *   verification are reported as fatal instead of being retried forever.
 * - Reports published while reconnecting are buffered in memory and sent
 *   when the connection returns.
 */
class PahoMqttLink(private val settings: MqttLinkSettings) : MqttLink {

    private companion object {
        const val FIRST_RETRY_MS = 1_000L
        const val MAX_RETRY_MS = 30_000L
        const val CONNECT_TIMEOUT_S = 10
        const val BUFFER_SIZE = 200
        const val MAX_INFLIGHT = 100
        const val CLOSE_TIMEOUT_MS = 2_000L
    }

    @Volatile
    private var client: MqttAsyncClient? = null

    @Volatile
    private var listener: MqttLink.Listener? = null

    @Volatile
    private var closed = false

    private var retryDelayMs = FIRST_RETRY_MS

    private val retries = Executors.newSingleThreadScheduledExecutor { runnable ->
        Thread(runnable, "phoenix-mqtt-connect").apply { isDaemon = true }
    }

    override val isConnected: Boolean
        get() = client?.isConnected == true

    override fun open(listener: MqttLink.Listener) {
        this.listener = listener

        val options = connectOptions() // throws IllegalArgumentException for a bad CA

        val created = try {
            MqttAsyncClient(settings.brokerUri, settings.clientId, MemoryPersistence())
        } catch (e: MqttException) {
            throw IllegalArgumentException("Cannot use broker address ${settings.brokerUri}: ${e.message}", e)
        }

        created.setCallback(Callback())
        created.setBufferOpts(DisconnectedBufferOptions().apply {
            isBufferEnabled = true
            bufferSize = BUFFER_SIZE
            isPersistBuffer = false
            isDeleteOldestMessages = false
        })

        client = created
        attempt(created, options)
    }

    override fun subscribe(filters: List<String>, qos: Int): Boolean {
        val current = client ?: return false
        return try {
            current.subscribe(filters.toTypedArray(), IntArray(filters.size) { qos })
            true
        } catch (e: MqttException) {
            false
        }
    }

    override fun publish(message: MqttLink.Message): Boolean {
        val current = client ?: return false
        if (closed) return false
        return try {
            current.publish(
                message.topic,
                message.payload.toByteArray(Charsets.UTF_8),
                message.qos,
                message.retained
            )
            true
        } catch (e: MqttException) {
            false
        }
    }

    override fun close(farewell: MqttLink.Message?) {
        if (closed) return
        closed = true
        retries.shutdownNow()

        val current = client ?: return

        try {
            if (current.isConnected) {
                if (farewell != null) {
                    current.publish(
                        farewell.topic,
                        farewell.payload.toByteArray(Charsets.UTF_8),
                        farewell.qos,
                        farewell.retained
                    ).waitForCompletion(CLOSE_TIMEOUT_MS)
                }
                current.disconnect(CLOSE_TIMEOUT_MS / 2).waitForCompletion(CLOSE_TIMEOUT_MS)
            }
        } catch (e: MqttException) {
            // Closing anyway.
        }

        try {
            current.close()
        } catch (e: MqttException) {
            // Already closed, or still busy; nothing more to do.
        }
    }

    private fun connectOptions(): MqttConnectOptions = MqttConnectOptions().apply {
        mqttVersion = MqttConnectOptions.MQTT_VERSION_3_1_1
        isCleanSession = false
        isAutomaticReconnect = true
        maxReconnectDelay = MAX_RETRY_MS.toInt()
        keepAliveInterval = settings.keepAliveSeconds
        connectionTimeout = CONNECT_TIMEOUT_S
        maxInflight = MAX_INFLIGHT
        userName = settings.username
        password = settings.password.toCharArray()
        setWill(
            settings.will.topic,
            settings.will.payload.toByteArray(Charsets.UTF_8),
            settings.will.qos,
            settings.will.retained
        )

        if (settings.brokerUri.startsWith("ssl://")) {
            socketFactory = MqttTls.socketFactory(settings.caCertificatePem)
            // Paho's built-in check is skipped before Android 7.0; ours is not.
            isHttpsHostnameVerificationEnabled = false
            sslHostnameVerifier = TlsHostnameVerifier
        }
    }

    /** One connection attempt; the first connection is retried here, later ones by Paho. */
    private fun attempt(target: MqttAsyncClient, options: MqttConnectOptions) {
        if (closed) return

        try {
            target.connect(options, null, object : IMqttActionListener {
                override fun onSuccess(asyncActionToken: IMqttToken?) {
                    retryDelayMs = FIRST_RETRY_MS
                    // Callback.connectComplete reports the connection.
                }

                override fun onFailure(asyncActionToken: IMqttToken?, exception: Throwable?) {
                    failed(target, options, exception)
                }
            })
        } catch (e: MqttException) {
            failed(target, options, e)
        }
    }

    private fun failed(target: MqttAsyncClient, options: MqttConnectOptions, error: Throwable?) {
        if (closed) return

        val fatal = isFatal(error)
        listener?.connectFailed(describe(error), fatal)

        if (fatal) {
            close(null)
            return
        }

        val delay = retryDelayMs
        retryDelayMs = minOf(delay * 2, MAX_RETRY_MS)

        try {
            retries.schedule({ attempt(target, options) }, delay, TimeUnit.MILLISECONDS)
        } catch (e: java.util.concurrent.RejectedExecutionException) {
            // Closed meanwhile.
        }
    }

    private fun isFatal(error: Throwable?): Boolean {
        val code = (error as? MqttException)?.reasonCode
        if (code == MqttException.REASON_CODE_FAILED_AUTHENTICATION.toInt() ||
            code == MqttException.REASON_CODE_NOT_AUTHORIZED.toInt() ||
            code == MqttException.REASON_CODE_INVALID_CLIENT_ID.toInt() ||
            code == MqttException.REASON_CODE_INVALID_PROTOCOL_VERSION.toInt()
        ) {
            return true
        }
        return causes(error).any {
            it is SSLPeerUnverifiedException || it is SSLHandshakeException || it is CertificateException
        }
    }

    private fun describe(error: Throwable?): String {
        val code = (error as? MqttException)?.reasonCode
        return when {
            code == MqttException.REASON_CODE_FAILED_AUTHENTICATION.toInt() ||
                code == MqttException.REASON_CODE_NOT_AUTHORIZED.toInt() ->
                "the broker refused this device's user name or password"

            code == MqttException.REASON_CODE_INVALID_CLIENT_ID.toInt() ->
                "the broker refused the client id ${settings.clientId}"

            causes(error).any { it is SSLPeerUnverifiedException } ->
                "the broker certificate does not match the host name in ${settings.brokerUri}"

            causes(error).any { it is CertificateException || it is SSLHandshakeException } ->
                "TLS failed: the broker certificate is not trusted (check the CA certificate)"

            else -> {
                val root = causes(error).lastOrNull()
                (root?.message ?: error?.message ?: error?.javaClass?.simpleName ?: "unknown error")
            }
        }
    }

    private fun causes(error: Throwable?): List<Throwable> {
        val chain = mutableListOf<Throwable>()
        var current = error
        while (current != null && current !in chain && chain.size < 10) {
            chain += current
            current = current.cause
        }
        return chain
    }

    private inner class Callback : MqttCallbackExtended {

        override fun connectComplete(reconnect: Boolean, serverURI: String?) {
            if (!closed) listener?.connected(reconnect)
        }

        override fun connectionLost(cause: Throwable?) {
            if (!closed) listener?.connectionLost(cause?.message ?: "connection closed")
        }

        override fun messageArrived(topic: String?, message: MqttMessage?) {
            if (closed || topic == null || message == null) return
            try {
                listener?.messageArrived(topic, message.payload)
            } catch (e: Exception) {
                // An exception here would make Paho drop the connection.
            }
        }

        override fun deliveryComplete(token: IMqttDeliveryToken?) = Unit
    }
}
