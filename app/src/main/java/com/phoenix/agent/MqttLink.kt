// SPDX-License-Identifier: Apache-2.0

package com.phoenix.agent

/**
 * The MQTT connection [PhoenixMqttNodeAgent] talks through: [PahoMqttLink]
 * in the app, a scripted fake in unit tests.
 *
 * Implementations connect in the background, reconnect on their own after a
 * lost connection, and report what happens through a [Listener]. They never
 * block the caller.
 */
interface MqttLink {

    /** One MQTT message. */
    data class Message(
        val topic: String,
        val payload: String,
        val qos: Int,
        val retained: Boolean
    )

    /** Called on the link's own threads. */
    interface Listener {

        /** Connected, the first time or again. The caller (re)subscribes. */
        fun connected(reconnect: Boolean)

        /** The connection dropped; the link is reconnecting. */
        fun connectionLost(cause: String)

        /**
         * A connection attempt failed. If [fatal], retrying cannot help (the
         * broker refused the credentials, or TLS could not verify the broker)
         * and the link has stopped; otherwise it retries with a back-off.
         */
        fun connectFailed(cause: String, fatal: Boolean)

        fun messageArrived(topic: String, payload: ByteArray)
    }

    val isConnected: Boolean

    /** Starts connecting. Throws [IllegalArgumentException] for unusable settings. */
    fun open(listener: Listener)

    /** Subscribes to [filters]; false if the request could not be made. */
    fun subscribe(filters: List<String>, qos: Int): Boolean

    /** Queues [message]; false if it could not even be queued. */
    fun publish(message: Message): Boolean

    /**
     * Publishes [farewell] if still connected, then disconnects for good.
     * May block briefly; call it off the UI thread. Idempotent.
     */
    fun close(farewell: Message? = null)
}

/**
 * Everything needed to open an [MqttLink].
 *
 * The user name and client id are the device id (see [MqttAgentConfig]).
 */
class MqttLinkSettings(
    val brokerUri: String,
    val clientId: String,
    val username: String,
    val password: String,
    val caCertificatePem: String?,
    val keepAliveSeconds: Int,
    val will: MqttLink.Message
) {
    override fun toString(): String =
        "MqttLinkSettings(brokerUri=$brokerUri, clientId=$clientId, username=$username, password=***)"
}
