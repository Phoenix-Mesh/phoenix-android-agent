// SPDX-License-Identifier: Apache-2.0

package com.phoenix.agent

/**
 * What the UI needs from a node agent, whichever transport it speaks.
 *
 * [PhoenixNodeAgent] is the HTTP pull agent (the v0.2 baseline);
 * [PhoenixMqttNodeAgent] speaks the MQTT/TLS protocol. Each has its own
 * `connect`, because their settings differ.
 */
interface NodeAgent {

    /** True from `connect` until [disconnect], or until the connection fails for good. */
    val running: Boolean

    /** Stops the agent. Safe to call at any time, also repeatedly. */
    fun disconnect()
}
