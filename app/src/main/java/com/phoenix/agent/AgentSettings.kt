// SPDX-License-Identifier: Apache-2.0

package com.phoenix.agent

import android.app.Activity
import android.content.Context

/**
 * What the user entered, kept between app starts.
 *
 * Ordinary settings live in the activity's preferences (where the Core URL
 * always was). The broker password and the enrollment token live in a
 * separate, app-private file that is excluded from backups
 * (res/xml/backup_rules.xml, res/xml/data_extraction_rules.xml).
 *
 * The validation hold is deliberately not stored: it is reset to 0 on every
 * app start, so it cannot be left on by accident.
 */
class AgentSettings(activity: Activity) {

    enum class Transport { HTTP, MQTT }

    companion object {
        /** Excluded from backups; keep in sync with the backup rule files. */
        const val SECRETS_FILE = "phoenix_agent_secrets"

        private const val TRANSPORT = "transport"
        private const val CORE_URL = "core_url"
        private const val BROKER_URI = "broker_uri"
        private const val CA_PEM = "broker_ca_pem"
        private const val BROKER_PASSWORD = "broker_password"
        private const val ENROLLMENT_TOKEN = "enrollment_token"
    }

    private val prefs = activity.getPreferences(Context.MODE_PRIVATE)
    private val secrets = activity.getSharedPreferences(SECRETS_FILE, Context.MODE_PRIVATE)

    var transport: Transport
        get() = if (prefs.getString(TRANSPORT, null) == Transport.MQTT.name) Transport.MQTT else Transport.HTTP
        set(value) = prefs.edit().putString(TRANSPORT, value.name).apply()

    var coreUrl: String
        get() = prefs.getString(CORE_URL, "") ?: ""
        set(value) = prefs.edit().putString(CORE_URL, value).apply()

    var brokerUri: String
        get() = prefs.getString(BROKER_URI, "") ?: ""
        set(value) = prefs.edit().putString(BROKER_URI, value).apply()

    /** The broker CA in PEM, or null to trust the platform's CA store. */
    var caCertificatePem: String?
        get() = prefs.getString(CA_PEM, null)
        set(value) = prefs.edit().putString(CA_PEM, value).apply()

    var brokerPassword: String
        get() = secrets.getString(BROKER_PASSWORD, "") ?: ""
        set(value) = secrets.edit().putString(BROKER_PASSWORD, value).apply()

    var enrollmentToken: String
        get() = secrets.getString(ENROLLMENT_TOKEN, "") ?: ""
        set(value) = secrets.edit().putString(ENROLLMENT_TOKEN, value).apply()
}
