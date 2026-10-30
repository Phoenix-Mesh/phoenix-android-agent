// SPDX-License-Identifier: Apache-2.0

package com.phoenix.agent

/** Stable identity of this node. */
interface NodeIdentity {
    val deviceId: String
}

/** Hardware facts and live memory figures reported to Phoenix Core. */
interface NodeTelemetry {
    val displayName: String
    val architecture: String
    val cpuCores: Int
    val totalRamMb: Long
    val availableRamMb: Long
}

/**
 * Relative energy cost per task reported in heartbeats. A heuristic, not a
 * measurement: battery-powered mobile hardware is weighted above a
 * mains-powered Linux machine (4.5) until per-device profiling exists.
 */
const val ANDROID_ENERGY_COST_ESTIMATE = 6.0
