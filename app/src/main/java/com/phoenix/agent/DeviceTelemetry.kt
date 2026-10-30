// SPDX-License-Identifier: Apache-2.0

package com.phoenix.agent

import android.app.ActivityManager
import android.content.Context
import android.os.Build

/** Static device facts and live memory figures reported to Phoenix Core. */
class DeviceTelemetry(private val context: Context) : NodeTelemetry {
    private fun memInfo(): ActivityManager.MemoryInfo {
        val info = ActivityManager.MemoryInfo()
        (context.getSystemService(Context.ACTIVITY_SERVICE) as ActivityManager).getMemoryInfo(info)
        return info
    }

    override val architecture get() = Build.SUPPORTED_ABIS.firstOrNull() ?: "unknown"
    override val cpuCores get() = Runtime.getRuntime().availableProcessors()
    override val totalRamMb get() = memInfo().totalMem / 1024 / 1024
    override val availableRamMb get() = memInfo().availMem / 1024 / 1024
    override val displayName get() = "${Build.MANUFACTURER}-${Build.MODEL}"
}
