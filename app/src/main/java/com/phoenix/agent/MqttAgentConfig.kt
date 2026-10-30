// SPDX-License-Identifier: Apache-2.0

package com.phoenix.agent

import java.net.URI
import java.net.URISyntaxException
import java.util.Locale

/**
 * Settings of the MQTT/TLS transport (Phoenix Core: docs/mqtt-agent-protocol.md).
 *
 * The broker user name and the MQTT client id are always the device id: the
 * broker ACL confines each user name to its own topics, so they are not
 * settings.
 *
 * @param brokerUri where the broker listens, see [normalizeBrokerUri]
 * @param password this device's broker password (deploy/mosquitto/add-device.sh)
 * @param enrollmentToken Core's enrollment token, if Core requires one
 * @param caCertificatePem the CA (PEM) that signed the broker certificate;
 *        null trusts the platform's CA store instead
 * @param validationHoldSeconds validation aid, normally 0: hold every task
 *        this long after reporting `started`, so a task can be interrupted
 *        mid-execution on purpose (issue #23). The hold ends early when Core
 *        cancels the attempt or the agent stops.
 */
class MqttAgentConfig(
    brokerUri: String,
    val password: String,
    enrollmentToken: String? = null,
    val caCertificatePem: String? = null,
    val topicPrefix: String = DEFAULT_TOPIC_PREFIX,
    val validationHoldSeconds: Int = 0
) {

    /** The normalized broker URI: `ssl://host:port` (or `tcp://` on this device only). */
    val brokerUri: String = normalizeBrokerUri(brokerUri)

    val enrollmentToken: String? = enrollmentToken?.trim()?.takeIf { it.isNotEmpty() }

    val usesTls: Boolean get() = brokerUri.startsWith("ssl://")

    init {
        require(password.isNotEmpty()) { "Enter this device's broker password." }
        require(TOPIC_PREFIX.matches(topicPrefix)) { "Invalid topic prefix: $topicPrefix" }
        require(validationHoldSeconds in 0..MAX_VALIDATION_HOLD_SECONDS) {
            "The validation hold must be between 0 and $MAX_VALIDATION_HOLD_SECONDS seconds."
        }
    }

    /** Never prints the password or the enrollment token. */
    override fun toString(): String =
        "MqttAgentConfig(brokerUri=$brokerUri, password=***, " +
            "enrollmentToken=${if (enrollmentToken == null) "none" else "***"}, " +
            "trust=${if (caCertificatePem == null) "platform CA store" else "configured CA"}, " +
            "topicPrefix=$topicPrefix, validationHoldSeconds=$validationHoldSeconds)"

    companion object {
        const val DEFAULT_TOPIC_PREFIX = "phoenix"
        const val DEFAULT_TLS_PORT = 8883
        const val DEFAULT_TCP_PORT = 1883
        const val MAX_VALIDATION_HOLD_SECONDS = 300

        private val TOPIC_PREFIX = Regex("[A-Za-z0-9._-]+(/[A-Za-z0-9._-]+)*")
        private val LOOPBACK = setOf("localhost", "127.0.0.1", "[::1]", "::1")

        /**
         * Accepts `host`, `host:port`, `ssl://host[:port]` or `mqtts://host[:port]`
         * and returns `ssl://host:port` (port 8883 by default).
         *
         * Plain `tcp://` is accepted only for a broker on this device, because
         * the broker password would otherwise cross the network unencrypted.
         *
         * @throws IllegalArgumentException with a message fit for the user
         */
        fun normalizeBrokerUri(input: String): String {
            val trimmed = input.trim()
            require(trimmed.isNotEmpty()) { "Enter the broker address, for example ssl://192.168.1.10:8883" }

            val withScheme = if (trimmed.contains("://")) trimmed else "ssl://$trimmed"

            val uri = try {
                URI(withScheme)
            } catch (e: URISyntaxException) {
                throw IllegalArgumentException("Not a valid broker address: $trimmed")
            }

            val scheme = when (uri.scheme?.lowercase(Locale.ROOT)) {
                "ssl", "mqtts" -> "ssl"
                "tcp", "mqtt" -> "tcp"
                else -> throw IllegalArgumentException(
                    "Use ssl://host:port for the broker (got ${uri.scheme}://)"
                )
            }

            val host = uri.host
                ?: throw IllegalArgumentException("Not a valid broker host name: $trimmed")

            require(uri.rawPath.isNullOrEmpty() || uri.rawPath == "/") {
                "The broker address must not contain a path: $trimmed"
            }
            require(uri.rawQuery == null && uri.rawFragment == null && uri.rawUserInfo == null) {
                "The broker address must be just host and port: $trimmed"
            }

            val port = if (uri.port == -1) {
                if (scheme == "ssl") DEFAULT_TLS_PORT else DEFAULT_TCP_PORT
            } else {
                uri.port
            }
            require(port in 1..65_535) { "Invalid broker port: $port" }

            require(scheme == "ssl" || host.lowercase(Locale.ROOT) in LOOPBACK) {
                "Unencrypted tcp:// is only allowed for a broker on this device; use ssl://"
            }

            return "$scheme://$host:$port"
        }
    }
}
