// SPDX-License-Identifier: Apache-2.0

package com.phoenix.agent

import org.json.JSONArray
import org.json.JSONObject
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Phoenix pull-mode node agent: registers with Core, sends heartbeats, polls
 * for work and reports results.
 *
 * Log lines passed to [onLog] are also parsed by [MainActivity]; keep the
 * documented prefixes stable ("Registered with Phoenix.", "Heartbeat sent:",
 * "Heartbeat failed:", "Received task <id>,", "Task <id> done:",
 * "Task <id> failed:", "Poll failed:", "Agent stopped.").
 */
class PhoenixNodeAgent(
    private val identity: NodeIdentity,
    private val telemetry: NodeTelemetry,
    private val http: PhoenixHttpClient,
    private val onLog: (String) -> Unit
) : NodeAgent {

    private companion object {
        const val REGISTERED = "Registered with Phoenix."
        const val HEARTBEAT_PERIOD_S = 5L
        const val POLL_PERIOD_S = 3L
        const val DEFAULT_SAMPLES = 2_000_000L

        /** Error code Core returns from /results for a result it no longer expects. */
        const val UNKNOWN_TASK = "unknown_task"

        /**
         * Protocol features announced at registration. "attempt-id" means
         * this agent echoes the attemptId Core sends with each task, verbatim,
         * on /tasks/started and /results. Once announced, Core rejects reports
         * without it, so only announce what this class implements.
         */
        val PROTOCOL_FEATURES = listOf("attempt-id")
    }

    private val executor = MonteCarloExecutor()

    /** Guards [generation], [running] and the scheduler swap. */
    private val lifecycle = Any()

    /** Incremented on every connect and disconnect; identifies the current session. */
    private var generation = 0

    private var scheduler: ScheduledExecutorService = newScheduler()

    @Volatile
    override var running = false
        private set

    /** True while a task is executing; reported to Core as the load signal. */
    private val busy = AtomicBoolean(false)

    /** Guards against concurrent re-registration from the heartbeat and poll threads. */
    private val registering = AtomicBoolean(false)

    /** RTT of the previous heartbeat, reported in the next one. */
    @Volatile
    private var lastHeartbeatRttMs = 0.0

    /**
     * One connection to one Core. Every request made for a session uses its
     * own [coreUrl], so work started by an earlier session can never reach a
     * Core selected later.
     */
    private class Session(val generation: Int, val coreUrl: String)

    /**
     * Registers with Core on a background thread and, if still connected,
     * starts the heartbeat and poll loops.
     *
     * A [disconnect] issued while registration is in flight cancels the
     * attempt: neither [onConnected] nor [onFailed] is invoked and no work
     * is scheduled.
     */
    fun connect(coreUrl: String, onConnected: () -> Unit, onFailed: (String) -> Unit) {
        val session = synchronized(lifecycle) {
            running = true
            Session(++generation, coreUrl)
        }

        Thread {
            try {
                register(session)
            } catch (e: Exception) {
                val current = synchronized(lifecycle) {
                    isCurrent(session).also { if (it) running = false }
                }
                if (current) {
                    onFailed(e.message ?: e.javaClass.simpleName)
                }
                return@Thread
            }

            synchronized(lifecycle) {
                if (!isCurrent(session)) {
                    return@Thread
                }
                onLog(REGISTERED)
                onConnected()
                startLoops(session)
            }
        }.start()
    }

    override fun disconnect() {
        synchronized(lifecycle) {
            generation++
            running = false
            scheduler.shutdownNow()
            scheduler = newScheduler()
        }
        onLog("Agent stopped.")
    }

    private fun newScheduler(): ScheduledExecutorService = Executors.newScheduledThreadPool(2)

    /** Must be called with [lifecycle] held. */
    private fun isCurrent(session: Session) = running && generation == session.generation

    private fun isStillCurrent(session: Session) = synchronized(lifecycle) { isCurrent(session) }

    /**
     * Runs [action] only if [session] is still the current one, holding the
     * lifecycle lock so a concurrent [disconnect] cannot interleave.
     */
    private inline fun ifCurrent(session: Session, action: () -> Unit) {
        synchronized(lifecycle) {
            if (isCurrent(session)) {
                action()
            }
        }
    }

    /** Must be called with [lifecycle] held. */
    private fun startLoops(session: Session) {
        scheduler.scheduleWithFixedDelay(
            { if (isStillCurrent(session)) heartbeat(session) },
            0, HEARTBEAT_PERIOD_S, TimeUnit.SECONDS
        )
        scheduler.scheduleWithFixedDelay(
            { if (isStillCurrent(session)) pollForTask(session) },
            1, POLL_PERIOD_S, TimeUnit.SECONDS
        )
    }

    private fun register(session: Session) {
        val payload = JSONObject()
            .put("deviceId", identity.deviceId)
            .put("displayName", telemetry.displayName)
            .put("hardwareClass", "ANDROID")
            .put("architecture", telemetry.architecture)
            .put("cpuCores", telemetry.cpuCores)
            .put("memoryMb", telemetry.totalRamMb)
            .put("adapterType", "android")
            .put("capabilities", JSONArray())
            .put("protocolFeatures", JSONArray(PROTOCOL_FEATURES))

        http.postJson("${session.coreUrl}/register", payload.toString())
    }

    /**
     * Core forgets every device when it restarts and then answers 404 to its
     * heartbeats and polls. Register again instead of failing until the user
     * reconnects by hand.
     *
     * The request cannot be canceled once sent, but its outcome is only
     * reported if [session] is still current when it completes.
     */
    private fun reRegister(session: Session) {
        if (!registering.compareAndSet(false, true)) {
            return
        }

        try {
            if (!isStillCurrent(session)) {
                return
            }
            onLog("Phoenix Core does not know this device; registering again.")
            register(session)
            ifCurrent(session) { onLog(REGISTERED) }
        } catch (e: Exception) {
            ifCurrent(session) { onLog("Re-registration failed: ${e.message}") }
        } finally {
            registering.set(false)
        }
    }

    private fun heartbeat(session: Session) {
        try {
            val body = JSONObject()
                .put("deviceId", identity.deviceId)
                .put("cpuUsagePercent", -1.0) // not measurable on this platform
                .put("availableRamMb", telemetry.availableRamMb)
                .put("activeTasks", if (busy.get()) 1 else 0)
                .put("queueLength", 0)
                .put("networkLatencyMs", lastHeartbeatRttMs)
                .put("energyCostEstimate", ANDROID_ENERGY_COST_ESTIMATE)

            val start = System.nanoTime()
            http.postJson("${session.coreUrl}/heartbeat", body.toString())
            val rttMs = (System.nanoTime() - start) / 1_000_000.0

            lastHeartbeatRttMs = rttMs
            onLog("Heartbeat sent: RAM=${telemetry.availableRamMb}MB RTT=${"%.1f".format(rttMs)}ms")
        } catch (e: HttpStatusException) {
            if (e.code == 404) {
                reRegister(session)
            } else {
                onLog("Heartbeat failed: ${e.message}")
            }
        } catch (e: Exception) {
            onLog("Heartbeat failed: ${e.message}")
        }
    }

    private fun pollForTask(session: Session) {
        if (!busy.compareAndSet(false, true)) {
            return
        }

        try {
            val (code, body) = http.get("${session.coreUrl}/tasks/poll?deviceId=${identity.deviceId}")

            if (code == 404) {
                reRegister(session)
                return
            }
            if (code != 200 || body.isBlank()) {
                return
            }

            execute(session, JSONObject(body))
        } catch (e: Exception) {
            onLog("Poll failed: ${e.message}")
        } finally {
            busy.set(false)
        }
    }

    private fun execute(session: Session, task: JSONObject) {
        val taskId = task.optString("taskId", task.optString("id"))
        if (taskId.isBlank()) {
            throw IllegalStateException("Task response missing task id: $task")
        }

        val samples = task.optLong("samples", DEFAULT_SAMPLES)

        // Echoed back unchanged and never interpreted. Absent for a LEGACY
        // node. ("as? String": optString would turn JSON null into "null".)
        val attemptId = task.opt("attemptId") as? String
        val attemptNumber = task.optLong("attemptNumber", -1)

        // MainActivity parses the task id from the text before the first comma.
        onLog(
            "Received task $taskId, executing Monte-Carlo (samples=$samples" +
                (if (attemptId != null) ", attempt $attemptNumber, id ${shortId(attemptId)}" else "") +
                ")..."
        )

        // Best effort: Core works without the start acknowledgement.
        try {
            val started = JSONObject().put("taskId", taskId)
            if (attemptId != null) started.put("attemptId", attemptId)
            http.postJson("${session.coreUrl}/tasks/started", started.toString())
        } catch (e: Exception) {
            onLog("Start notification failed (continuing): ${e.message}")
        }

        val resultBody = JSONObject().put("taskId", taskId)
        if (attemptId != null) resultBody.put("attemptId", attemptId)

        // Core treats any status other than SUCCESS as a failed attempt.
        val summary = try {
            val result = executor.execute(samples)
            resultBody
                .put("status", "SUCCESS")
                .put("outputData", result.output)
                .put("executionTimeMs", result.executionTimeMs)
            "Task $taskId done: ${result.output} (${result.executionTimeMs}ms)"
        } catch (e: Exception) {
            // Report the failure so Core does not wait out the execution timeout.
            resultBody
                .put("status", "FAILED")
                .put("outputData", "Execution failed: ${e.message}")
            "Task $taskId failed: ${e.message}"
        }

        try {
            http.postJson("${session.coreUrl}/results", resultBody.toString())
            onLog(summary)
        } catch (e: HttpStatusException) {
            // Core answers 404 "unknown_task" when it no longer expects this
            // attempt (stale, duplicate or already recovered elsewhere).
            if (e.code == 404 && e.body.contains(UNKNOWN_TASK)) {
                onLog("Task $taskId result rejected: Core no longer expects this attempt.")
            } else {
                throw e
            }
        }
    }

    /** First 8 characters, for log lines only. */
    private fun shortId(id: String) = if (id.length <= 8) id else id.substring(0, 8)
}
