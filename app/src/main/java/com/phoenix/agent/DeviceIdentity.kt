// SPDX-License-Identifier: Apache-2.0

package com.phoenix.agent

import android.content.Context
import java.util.UUID

/**
 * Stable identity of this device, generated once and kept in shared
 * preferences so it survives restarts and re-registrations.
 */
class DeviceIdentity(context: Context) : NodeIdentity {
    private val prefs = context.getSharedPreferences("phoenix_agent", Context.MODE_PRIVATE)
    override val deviceId: String by lazy {
        prefs.getString("device_id", null) ?: ("android-" + UUID.randomUUID()).also {
            prefs.edit().putString("device_id", it).apply()
        }
    }
}