// SPDX-License-Identifier: Apache-2.0

package com.phoenix.agent

import org.json.JSONArray
import org.json.JSONObject
import java.util.UUID
import java.util.concurrent.CancellationException
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit

/**
 * Phoenix node agent speaking the MQTT/TLS node-agent protocol, version 1
 * (Phoenix Core: docs/mqtt-agent-protocol.md; the Python agent in
 * agents/python is the reference implementation).
 *
 * - Registers with a fresh `requestId` (and the enrollment token, if set),
 *   keeps the session token, and resends an unanswered request.
 * - Sends heartbeats with the session token. It never decides whether the
 *   node is ONLINE, DEGRADED or OFFLINE: Phoenix Core does, from these
 *   heartbeats, exactly as for every other node.
 * - For each task: acknowledges it at once, runs the Monte-Carlo-Pi workload,
 *   reports `started` and the result, always echoing the `attemptId`.
 * - Idempotent: a redelivered attempt is acknowledged again but never run
 *   twice (its known result is re-sent); cancelled attempts are skipped or
 *   stopped.
 * - Registers again when Core restarts (new `coreInstanceId`) or asks to.
 *
 * Log lines passed to [onLog] are also parsed by [MainActivity]; the prefixes
 * shared with [PhoenixNodeAgent] keep their meaning ("Registered with
 * Phoenix.", "Heartbeat sent:", "Received task <id>,", "Task <id> done:",
 * "Task <id> failed:", "Agent stopped."), and this agent adds
 * "Broker connection lost:" and "Reconnected to the broker.".
 */
class PhoenixMqttNodeAgent(
    private val identity: NodeIdentity,
    private val telemetry: NodeTelemetry,
    private val onLog: (String) -> Unit,
    private val linkFactory: (MqttLinkSettings) -> MqttLink = { PahoMqttLink(it) },
    private val workload: (Long, () -> Boolean) -> MonteCarloExecutor.Result =
        MonteCarloExecutor()::execute,
    private val timing: Timing = Timing()
) : NodeAgent {

    /** Timings; tests shorten them. */
    data class Timing(
        /** Resend an unanswered registration after this long. */
        val registrationRetryMs: Long = 10_000,
        /** Heartbeat at least this often, even if Core allows longer. */
        val maxHeartbeatIntervalSeconds: Long = 5,
        val keepAliveSeconds: Int = 30
    )

    private companion object {
        const val PROTOCOL_VERSION = 1
        const val QOS = 1
        const val REGISTERED = "Registered with Phoenix."
        const val TASK_TYPE = "Monte-Carlo-Pi"
        const val DEFAULT_SAMPLES = 2_000_000L
        const val MAX_SAMPLES = 200_000_000L

        /** How many handled and cancelled attempt ids are remembered. */
        const val REMEMBERED_ATTEMPTS = 1_024

        val PROTOCOL_FEATURES = listOf("attempt-id")
        val DEVICE_ID = Regex("[A-Za-z0-9][A-Za-z0-9._-]{0,127}")

        val ONLINE: String = JSONObject().put("state", "online").toString()
        val OFFLINE: String = JSONObject().put("state", "offline").toString()

        fun daemon(name: String) = ThreadFactory { runnable ->
            Thread(runnable, name).apply { isDaemon = true }
        }

        fun shortId(id: String) = if (id.length <= 8) id else id.substring(0, 8)
    }

    /** Topic names for this device (protocol version 1). */
    private class Topics(prefix: String, deviceId: String) {
        val coreStatus = "$prefix/v$PROTOCOL_VERSION/core/status"
        private val node = "$prefix/v$PROTOCOL_VERSION/nodes/$deviceId"
        val downAll = "$node/down/#"
        fun up(kind: String) = "$node/up/$kind"
        fun down(kind: String) = "$node/down/$kind"
    }

    /** One `connect`: its link, threads and Core session. Guarded by [lock]. */
    private inner class Session(
        val generation: Int,
        val config: MqttAgentConfig,
        var onConnected: (() -> Unit)?,
        var onFailed: ((String) -> Unit)?
    ) {
        val topics = Topics(config.topicPrefix, identity.deviceId)
        val scheduler: ScheduledExecutorService =
            Executors.newSingleThreadScheduledExecutor(daemon("phoenix-mqtt-timer"))
        val worker: ExecutorService = Executors.newSingleThreadExecutor(daemon("phoenix-mqtt-worker"))
        var link: MqttLink? = null

        var registeredOnce = false
        var sessionToken: String? = null
        var nodeId: String? = null
        var coreInstanceId: String? = null

        var requestId: String? = null
        var requestBody: String? = null
        var requestSentAtNs = 0L
        var answered = false
        var retry: ScheduledFuture<*>? = null

        var heartbeats: ScheduledFuture<*>? = null
        var heartbeatIntervalSeconds = timing.maxHeartbeatIntervalSeconds
        var latencyMs = 0.0

        var queued = 0
        var active = 0
    }

    private val lock = Any()

    @Volatile
    private var generation = 0

    private var session: Session? = null

    @Volatile
    override var running = false
        private set

    /** Attempt id -> its result body, or null while it runs. Bounded, oldest first. */
    private val handled = object : LinkedHashMap<String, JSONObject?>() {
        override fun removeEldestEntry(eldest: MutableMap.MutableEntry<String, JSONObject?>?) =
            size > REMEMBERED_ATTEMPTS
    }

    /** Attempt ids Core cancelled; a cancel may overtake its task. */
    private val cancelled = object : LinkedHashMap<String, Boolean>() {
        override fun removeEldestEntry(eldest: MutableMap.MutableEntry<String, Boolean>?) =
            size > REMEMBERED_ATTEMPTS
    }

    // ------------------------------------------------------------------
    // lifecycle
    // ------------------------------------------------------------------

    /**
     * Connects to the broker and registers with Phoenix Core.
     *
     * [onConnected] runs once, when Core accepted the registration.
     * [onFailed] runs at most once, if the agent gives up before that: the
     * broker refused the credentials, TLS could not verify the broker, or
     * Core refused the registration. Unreachable brokers are retried.
     */
    fun connect(config: MqttAgentConfig, onConnected: () -> Unit, onFailed: (String) -> Unit) {
        val previous: Session?
        val current: Session

        synchronized(lock) {
            previous = session
            running = true
            current = Session(++generation, config, onConnected, onFailed)
            session = current
        }
        previous?.let { shutdown(it) }

        if (!DEVICE_ID.matches(identity.deviceId)) {
            fail(current, "Device id ${identity.deviceId} cannot be used as an MQTT user name.")
            return
        }

        val link = try {
            linkFactory(
                MqttLinkSettings(
                    brokerUri = config.brokerUri,
                    clientId = identity.deviceId,
                    username = identity.deviceId,
                    password = config.password,
                    caCertificatePem = config.caCertificatePem,
                    keepAliveSeconds = timing.keepAliveSeconds,
                    will = MqttLink.Message(current.topics.up("status"), OFFLINE, QOS, retained = true)
                )
            )
        } catch (e: Exception) {
            fail(current, "Cannot create the MQTT connection: ${e.message}")
            return
        }

        synchronized(lock) {
            if (!isCurrent(current)) {
                closeLater(link, null)
                return
            }
            current.link = link
        }

        onLog(
            "Connecting to ${config.brokerUri} as ${identity.deviceId} " +
                "(${if (config.usesTls) "TLS" else "plain TCP"})..."
        )

        try {
            link.open(LinkListener(current))
        } catch (e: Exception) {
            fail(current, e.message ?: e.javaClass.simpleName)
        }
    }

    override fun disconnect() {
        val stopped = synchronized(lock) {
            generation++
            running = false
            session.also { session = null }
        }
        stopped?.let { shutdown(it, farewell = true) }
        onLog("Agent stopped.")
    }

    /** Gives up on [s] before Core accepted it, or stops if it was accepted earlier. */
    private fun fail(s: Session, message: String) {
        val onFailed = synchronized(lock) {
            if (!isCurrent(s)) return
            val callback = s.onFailed
            s.onFailed = null
            s.onConnected = null
            if (callback == null) null else {
                generation++
                running = false
                session = null
                callback
            }
        }

        if (onFailed == null) {
            // Core had accepted this device before; stop and say why.
            onLog(message)
            disconnect()
            return
        }

        shutdown(s)
        onFailed(message)
    }

    private fun shutdown(s: Session, farewell: Boolean = false) {
        val link = synchronized(lock) {
            s.retry?.cancel(false)
            s.heartbeats?.cancel(false)
            s.link
        }
        s.scheduler.shutdownNow()
        s.worker.shutdownNow()

        if (link != null) {
            closeLater(link, if (farewell) MqttLink.Message(s.topics.up("status"), OFFLINE, QOS, true) else null)
        }
    }

    /** Closing may block on the network; never do it on the caller's (UI) thread. */
    private fun closeLater(link: MqttLink, farewell: MqttLink.Message?) {
        Thread({ link.close(farewell) }, "phoenix-mqtt-close").apply { isDaemon = true }.start()
    }

    private fun isCurrent(s: Session) = running && generation == s.generation

    // ------------------------------------------------------------------
    // connection events
    // ------------------------------------------------------------------

    private inner class LinkListener(private val s: Session) : MqttLink.Listener {

        override fun connected(reconnect: Boolean) {
            val link = synchronized(lock) { if (isCurrent(s)) s.link else null } ?: return

            link.subscribe(listOf(s.topics.downAll, s.topics.coreStatus), QOS)
            link.publish(MqttLink.Message(s.topics.up("status"), ONLINE, QOS, retained = true))

            onLog(if (reconnect) "Reconnected to the broker." else "Connected to the broker.")

            synchronized(lock) {
                if (!isCurrent(s)) return
                when {
                    // Keep the token across a reconnect; if Core forgot it,
                    // Core answers the next heartbeat with "reregister".
                    s.sessionToken != null -> Unit
                    s.requestId != null && !s.answered -> sendRegistration(s)
                    else -> register(s)
                }
            }

            sendHeartbeat(s)
        }

        override fun connectionLost(cause: String) {
            if (isCurrent(s)) onLog("Broker connection lost: $cause; reconnecting...")
        }

        override fun connectFailed(cause: String, fatal: Boolean) {
            if (!isCurrent(s)) return
            if (fatal) {
                fail(s, "Could not connect to the broker: $cause")
            } else {
                onLog("Broker not reachable ($cause); retrying...")
            }
        }

        override fun messageArrived(topic: String, payload: ByteArray) {
            if (!isCurrent(s)) return

            val kind = when (topic) {
                s.topics.coreStatus -> "core/status"
                s.topics.down("registered") -> "registered"
                s.topics.down("task") -> "task"
                s.topics.down("cancel") -> "cancel"
                s.topics.down("control") -> "control"
                else -> return // a kind this version does not know: ignore it
            }

            val body = try {
                JSONObject(String(payload, Charsets.UTF_8))
            } catch (e: Exception) {
                onLog("Ignored a malformed $kind message.")
                return
            }

            when (kind) {
                "core/status" -> onCoreStatus(s, body)
                "registered" -> onRegistered(s, body)
                "task" -> onTask(s, body)
                "cancel" -> onCancel(body)
                "control" -> onControl(s, body)
            }
        }
    }

    // ------------------------------------------------------------------
    // registration
    // ------------------------------------------------------------------

    /** A new registration request. Must be called with [lock] held. */
    private fun register(s: Session) {
        val requestId = UUID.randomUUID().toString()

        val body = JSONObject()
            .put("requestId", requestId)
            .put("deviceId", identity.deviceId)
            .put("displayName", telemetry.displayName)
            .put("hardwareClass", "ANDROID")
            .put("architecture", telemetry.architecture)
            .put("cpuCores", telemetry.cpuCores)
            .put("memoryMb", telemetry.totalRamMb)
            .put("adapterType", "android")
            .put("capabilities", JSONArray())
            .put("protocolFeatures", JSONArray(PROTOCOL_FEATURES))
            .put("protocolVersion", PROTOCOL_VERSION)

        s.config.enrollmentToken?.let { body.put("enrollmentToken", it) }

        s.requestId = requestId
        s.requestBody = body.toString()
        s.answered = false
        sendRegistration(s)
    }

    /**
     * Sends the current request (again: the same `requestId` gets the same
     * answer) and schedules a resend. Must be called with [lock] held.
     */
    private fun sendRegistration(s: Session) {
        val requestId = s.requestId ?: return
        val body = s.requestBody ?: return

        s.requestSentAtNs = System.nanoTime()
        s.link?.publish(MqttLink.Message(s.topics.up("register"), body, QOS, retained = false))

        s.retry?.cancel(false)
        s.retry = schedule(s, timing.registrationRetryMs) { retryRegistration(s, requestId) }
    }

    private fun retryRegistration(s: Session, requestId: String) {
        val resend = synchronized(lock) {
            isCurrent(s) && s.requestId == requestId && !s.answered
        }
        if (!resend) return

        onLog("No answer from Phoenix Core yet; sending the registration again.")
        synchronized(lock) {
            if (isCurrent(s) && s.requestId == requestId && !s.answered) sendRegistration(s)
        }
    }

    private fun onRegistered(s: Session, body: JSONObject) {
        val requestId = body.opt("requestId") as? String

        val outcome: String? = synchronized(lock) {
            // An answer to an earlier request, or a duplicate of this one.
            if (requestId == null || requestId != s.requestId || s.answered) return
            s.answered = true
            s.retry?.cancel(false)

            if (body.optString("status") != "ok") {
                return@synchronized body.optString("message", "no reason given")
            }

            val token = body.opt("sessionToken") as? String
            val nodeId = body.opt("nodeId") as? String
            if (token.isNullOrEmpty() || nodeId.isNullOrEmpty()) {
                return@synchronized "malformed registration answer"
            }

            s.sessionToken = token
            s.nodeId = nodeId
            s.coreInstanceId = body.opt("coreInstanceId") as? String
            s.latencyMs = Math.round((System.nanoTime() - s.requestSentAtNs) / 100_000.0) / 10.0
            s.heartbeatIntervalSeconds = body.optLong("heartbeatIntervalSeconds", timing.maxHeartbeatIntervalSeconds)
                .coerceIn(1, timing.maxHeartbeatIntervalSeconds)

            s.heartbeats?.cancel(false)
            s.heartbeats = scheduleRepeating(s, s.heartbeatIntervalSeconds) { sendHeartbeat(s) }
            null
        }

        if (outcome != null) {
            fail(s, "Phoenix Core refused the registration: $outcome")
            return
        }

        var first = false
        var callback: (() -> Unit)? = null
        var nodeId: String? = null
        var latencyMs = 0.0

        synchronized(lock) {
            first = !s.registeredOnce
            s.registeredOnce = true
            callback = s.onConnected
            s.onConnected = null
            s.onFailed = null
            nodeId = s.nodeId
            latencyMs = s.latencyMs
        }

        onLog(REGISTERED)
        onLog("Phoenix node id $nodeId (device ${identity.deviceId}, registration round trip $latencyMs ms)")
        if (first) callback?.invoke()

        sendHeartbeat(s)
    }

    private fun onCoreStatus(s: Session, body: JSONObject) {
        val state = body.optString("state")
        val instance = body.opt("coreInstanceId") as? String

        val message = synchronized(lock) {
            if (!isCurrent(s)) return
            when {
                state == "online" && s.sessionToken != null && instance != null &&
                    instance != s.coreInstanceId -> {
                    register(s)
                    "Phoenix Core restarted; registering again."
                }

                // Core just came (back) online while our request waits.
                state == "online" && s.sessionToken == null && s.requestId != null && !s.answered -> {
                    sendRegistration(s)
                    null
                }

                state == "offline" && s.sessionToken != null ->
                    "Phoenix Core is offline; waiting for it to return."

                else -> null
            }
        }

        message?.let(onLog)
    }

    private fun onControl(s: Session, body: JSONObject) {
        if (body.optString("type") != "reregister") return // unknown control types are ignored

        onLog("Phoenix Core asked this device to register again (${body.optString("reason", "no reason given")}).")
        synchronized(lock) {
            if (isCurrent(s)) register(s)
        }
    }

    // ------------------------------------------------------------------
    // heartbeats
    // ------------------------------------------------------------------

    private fun sendHeartbeat(s: Session) {
        val snapshot = synchronized(lock) {
            val link = s.link
            val token = s.sessionToken
            if (!isCurrent(s) || link == null || token == null || !link.isConnected) return
            HeartbeatState(link, token, s.active, s.queued, s.latencyMs)
        }

        try {
            val availableRamMb = telemetry.availableRamMb
            val body = JSONObject()
                .put("sessionToken", snapshot.token)
                .put("deviceId", identity.deviceId)
                .put("cpuUsagePercent", -1.0) // not measurable on this platform: unknown, never 0 %
                .put("availableRamMb", availableRamMb)
                .put("activeTasks", snapshot.active)
                .put("queueLength", snapshot.queued)
                .put("networkLatencyMs", snapshot.latencyMs)
                .put("energyCostEstimate", ANDROID_ENERGY_COST_ESTIMATE)

            if (snapshot.link.publish(MqttLink.Message(s.topics.up("heartbeat"), body.toString(), 0, false))) {
                onLog("Heartbeat sent: RAM=${availableRamMb}MB active=${snapshot.active} queued=${snapshot.queued}")
            } else {
                onLog("Heartbeat failed: not connected to the broker")
            }
        } catch (e: Exception) {
            onLog("Heartbeat failed: ${e.message}")
        }
    }

    private class HeartbeatState(
        val link: MqttLink,
        val token: String,
        val active: Int,
        val queued: Int,
        val latencyMs: Double
    )

    // ------------------------------------------------------------------
    // tasks
    // ------------------------------------------------------------------

    private enum class Decision { RUN, DUPLICATE_RUNNING, DUPLICATE_DONE, CANCELLED, NOT_REGISTERED }

    private fun onTask(s: Session, task: JSONObject) {
        val taskId = task.opt("taskId") as? String
        val attemptId = task.opt("attemptId") as? String

        if (taskId.isNullOrEmpty() || attemptId.isNullOrEmpty()) {
            onLog("Ignored a task without taskId or attemptId.")
            return
        }

        var knownResult: JSONObject? = null

        val decision = synchronized(lock) {
            if (!isCurrent(s)) return

            // 1. Acknowledge every delivery, duplicates included.
            report(s, "ack", taskId, attemptId)

            when {
                handled.containsKey(attemptId) -> {
                    knownResult = handled[attemptId]
                    if (knownResult == null) Decision.DUPLICATE_RUNNING else Decision.DUPLICATE_DONE
                }

                cancelled.containsKey(attemptId) -> Decision.CANCELLED
                s.sessionToken == null -> Decision.NOT_REGISTERED

                else -> {
                    handled[attemptId] = null
                    s.queued++
                    Decision.RUN
                }
            }
        }

        val attempt = "attempt ${shortId(attemptId)}"

        when (decision) {
            Decision.RUN -> try {
                s.worker.execute { runTask(s, task, taskId, attemptId) }
            } catch (e: RejectedExecutionException) {
                // The agent stopped meanwhile.
            }

            Decision.DUPLICATE_RUNNING ->
                onLog("Duplicate delivery of task $taskId ($attempt) ignored: it is already running.")

            Decision.DUPLICATE_DONE -> {
                // 2. Never run an attempt twice; re-send the known result.
                resendResult(s, knownResult!!)
                onLog("Duplicate delivery of task $taskId ($attempt): result sent again, not executed twice.")
            }

            Decision.CANCELLED ->
                onLog("Task $taskId ($attempt) was cancelled by Phoenix Core before it arrived; skipped.")

            Decision.NOT_REGISTERED ->
                onLog("Task $taskId ($attempt) arrived before registration finished; Phoenix Core will place it again.")
        }
    }

    private fun onCancel(body: JSONObject) {
        val attemptId = body.opt("attemptId") as? String ?: return
        val taskId = body.optString("taskId", "?")
        val reason = body.optString("reason", "no reason given")

        synchronized(lock) { cancelled[attemptId] = true }
        onLog("Phoenix Core cancelled task $taskId (attempt ${shortId(attemptId)}, $reason).")
    }

    private fun runTask(s: Session, task: JSONObject, taskId: String, attemptId: String) {
        val skip = synchronized(lock) {
            if (!isCurrent(s)) return
            s.queued--
            if (cancelled.containsKey(attemptId)) {
                handled.remove(attemptId)
                true
            } else {
                s.active++
                false
            }
        }

        if (skip) {
            onLog("Task $taskId was cancelled by Phoenix Core before it started; skipped.")
            return
        }

        try {
            execute(s, task, taskId, attemptId)
        } finally {
            synchronized(lock) { s.active-- }
        }
    }

    private fun execute(s: Session, task: JSONObject, taskId: String, attemptId: String) {
        val type = task.optString("type", TASK_TYPE)
        val samples = task.optLong("samples", DEFAULT_SAMPLES)
        val attemptNumber = task.optLong("attemptNumber", -1)

        // MainActivity parses the task id from the text before the first comma.
        onLog(
            "Received task $taskId, executing Monte-Carlo " +
                "(samples=$samples, attempt $attemptNumber, id ${shortId(attemptId)})..."
        )

        synchronized(lock) {
            if (!isCurrent(s)) return
            report(s, "started", taskId, attemptId)
        }

        val stop = { !isCurrent(s) || synchronized(lock) { cancelled.containsKey(attemptId) } }

        if (!hold(s.config.validationHoldSeconds, taskId, stop)) {
            discard(s, taskId, attemptId)
            return
        }

        val started = System.nanoTime()
        var status = "SUCCESS"
        var output: String
        var executionTimeMs: Long

        try {
            when {
                type != TASK_TYPE -> {
                    status = "FAILED"
                    output = "Unsupported task type: $type"
                    executionTimeMs = 0
                }

                samples <= 0 || samples > MAX_SAMPLES -> {
                    status = "FAILED"
                    output = "Invalid sample count: $samples"
                    executionTimeMs = 0
                }

                else -> {
                    val result = workload(samples, stop)
                    output = result.output
                    executionTimeMs = result.executionTimeMs
                }
            }
        } catch (e: CancellationException) {
            discard(s, taskId, attemptId)
            return
        } catch (e: Exception) {
            status = "FAILED"
            output = "Execution failed: ${e.message}"
            executionTimeMs = (System.nanoTime() - started) / 1_000_000
        }

        if (stop()) {
            discard(s, taskId, attemptId)
            return
        }

        val published = synchronized(lock) {
            if (!isCurrent(s)) return
            val body = JSONObject()
                .put("sessionToken", s.sessionToken)
                .put("taskId", taskId)
                .put("attemptId", attemptId)
                .put("status", status)
                .put("outputData", output)
                .put("executionTimeMs", executionTimeMs)
            handled[attemptId] = body
            s.link?.publish(MqttLink.Message(s.topics.up("result"), body.toString(), QOS, false)) == true
        }

        val delivery = if (published) "" else " (result queued until the broker is reachable)"

        onLog(
            if (status == "SUCCESS") "Task $taskId done: $output (${executionTimeMs}ms)$delivery"
            else "Task $taskId failed: $output$delivery"
        )
    }

    /**
     * The validation hold (normally 0 s): waits [seconds], checking [stop]
     * every 100 ms. False if the attempt was stopped meanwhile.
     */
    private fun hold(seconds: Int, taskId: String, stop: () -> Boolean): Boolean {
        if (seconds <= 0) return true

        onLog("Holding task $taskId for $seconds s before executing (validation hold)...")
        val until = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds.toLong())
        try {
            while (System.nanoTime() < until) {
                if (stop()) return false
                Thread.sleep(100)
            }
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            return false
        }
        return !stop()
    }

    /** A cancelled or interrupted attempt: no result is sent, Core re-places the task if needed. */
    private fun discard(s: Session, taskId: String, attemptId: String) {
        val cancelledByCore = synchronized(lock) {
            val wasCancelled = cancelled.containsKey(attemptId)
            if (handled[attemptId] == null) handled.remove(attemptId)
            wasCancelled
        }

        if (cancelledByCore) {
            onLog("Task $taskId stopped: cancelled by Phoenix Core; no result sent.")
        } else if (!isCurrent(s)) {
            onLog("Task $taskId interrupted: the agent stopped; Phoenix Core will place it again.")
        }
    }

    private fun resendResult(s: Session, known: JSONObject) {
        val body = synchronized(lock) {
            // The current token: an earlier one stops counting after a re-registration.
            JSONObject(known.toString()).put("sessionToken", s.sessionToken)
        }
        s.link?.publish(MqttLink.Message(s.topics.up("result"), body.toString(), QOS, false))
    }

    /** Publishes `{sessionToken, taskId, attemptId}` on `up/<kind>`. Must be called with [lock] held. */
    private fun report(s: Session, kind: String, taskId: String, attemptId: String) {
        val body = JSONObject()
            .put("sessionToken", s.sessionToken)
            .put("taskId", taskId)
            .put("attemptId", attemptId)
        s.link?.publish(MqttLink.Message(s.topics.up(kind), body.toString(), QOS, false))
    }

    // ------------------------------------------------------------------
    // timers
    // ------------------------------------------------------------------

    private fun schedule(s: Session, delayMs: Long, action: () -> Unit): ScheduledFuture<*>? =
        try {
            s.scheduler.schedule({ guarded(action) }, delayMs, TimeUnit.MILLISECONDS)
        } catch (e: RejectedExecutionException) {
            null
        }

    private fun scheduleRepeating(s: Session, periodSeconds: Long, action: () -> Unit): ScheduledFuture<*>? =
        try {
            s.scheduler.scheduleWithFixedDelay({ guarded(action) }, periodSeconds, periodSeconds, TimeUnit.SECONDS)
        } catch (e: RejectedExecutionException) {
            null
        }

    /** A timer task that throws would silently cancel its schedule. */
    private fun guarded(action: () -> Unit) {
        try {
            action()
        } catch (e: Exception) {
            onLog("Agent error: ${e.message}")
        }
    }
}
