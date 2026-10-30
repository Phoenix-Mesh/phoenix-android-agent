// SPDX-License-Identifier: Apache-2.0

package com.phoenix.agent

import org.json.JSONObject
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import java.util.concurrent.CancellationException
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/** The MQTT agent against a scripted broker connection. */
class PhoenixMqttNodeAgentTest {

    private companion object {
        const val DEVICE = "android-test"
        const val DOWN = "phoenix/v1/nodes/$DEVICE/down/"
        const val CORE_STATUS = "phoenix/v1/core/status"
    }

    private class FakeIdentity : NodeIdentity {
        override val deviceId = DEVICE
    }

    private class FakeTelemetry : NodeTelemetry {
        override val displayName = "Test-Device"
        override val architecture = "arm64-v8a"
        override val cpuCores = 4
        override val totalRamMb = 4096L
        override val availableRamMb = 2048L
    }

    /** A broker connection the test drives by hand. */
    private class FakeLink : MqttLink {
        lateinit var settings: MqttLinkSettings
        lateinit var listener: MqttLink.Listener
        val published = CopyOnWriteArrayList<MqttLink.Message>()
        val subscriptions = CopyOnWriteArrayList<String>()
        val closed = CountDownLatch(1)

        @Volatile
        var farewell: MqttLink.Message? = null

        @Volatile
        var up = false

        override val isConnected get() = up

        override fun open(listener: MqttLink.Listener) {
            this.listener = listener
        }

        override fun subscribe(filters: List<String>, qos: Int): Boolean {
            subscriptions += filters
            return true
        }

        override fun publish(message: MqttLink.Message): Boolean {
            published += message
            return true
        }

        override fun close(farewell: MqttLink.Message?) {
            this.farewell = farewell
            up = false
            closed.countDown()
        }

        fun connect(reconnect: Boolean = false) {
            up = true
            listener.connected(reconnect)
        }

        fun drop() {
            up = false
            listener.connectionLost("network gone")
        }

        fun deliver(kind: String, body: String) =
            listener.messageArrived(DOWN + kind, body.toByteArray(Charsets.UTF_8))

        fun coreStatus(body: String) =
            listener.messageArrived(CORE_STATUS, body.toByteArray(Charsets.UTF_8))

        fun sent(kind: String): List<MqttLink.Message> =
            published.filter { it.topic == "phoenix/v1/nodes/$DEVICE/up/$kind" }

        fun bodies(kind: String): List<JSONObject> = sent(kind).map { JSONObject(it.payload) }

        fun lastRequestId(): String = bodies("register").last().getString("requestId")

        fun answer(token: String = "tok-1", instance: String = "core-1", requestId: String = lastRequestId()) =
            deliver(
                "registered",
                """{"requestId":"$requestId","status":"ok","nodeId":"node-1","sessionToken":"$token",
                   "heartbeatIntervalSeconds":5,"coreInstanceId":"$instance","protocolVersion":1}"""
            )
    }

    private class Outcome {
        val logs = CopyOnWriteArrayList<String>()
        val connected = AtomicInteger()
        val failures = CopyOnWriteArrayList<String>()
    }

    private val link = FakeLink()
    private val outcome = Outcome()
    private val runs = CopyOnWriteArrayList<Long>()

    /** Default workload: instant and counted. */
    private var workload: (Long, () -> Boolean) -> MonteCarloExecutor.Result = { samples, _ ->
        runs += samples
        MonteCarloExecutor.Result("Computed Monte-Carlo pi ~= 3.14", 7)
    }

    private val agent = PhoenixMqttNodeAgent(
        FakeIdentity(),
        FakeTelemetry(),
        onLog = { outcome.logs += it },
        linkFactory = { settings -> link.also { it.settings = settings } },
        workload = { samples, stop -> workload(samples, stop) },
        timing = PhoenixMqttNodeAgent.Timing(registrationRetryMs = 150)
    )

    @After
    fun stop() = agent.disconnect()

    private fun config(token: String? = "enroll-123456789", holdSeconds: Int = 0) =
        MqttAgentConfig("broker.test", "pw", enrollmentToken = token, validationHoldSeconds = holdSeconds)

    private fun start(token: String? = "enroll-123456789", holdSeconds: Int = 0) {
        agent.connect(
            config(token, holdSeconds),
            onConnected = { outcome.connected.incrementAndGet() },
            onFailed = { outcome.failures += it }
        )
    }

    /** Connected and registered as node-1 with session token tok-1. */
    private fun registered(holdSeconds: Int = 0) {
        start(holdSeconds = holdSeconds)
        link.connect()
        link.answer()
        assertTrue(eventually { outcome.connected.get() == 1 })
    }

    private fun eventually(timeoutMs: Long = 5_000, condition: () -> Boolean): Boolean {
        val deadline = System.currentTimeMillis() + timeoutMs
        while (System.currentTimeMillis() < deadline) {
            if (condition()) return true
            Thread.sleep(10)
        }
        return condition()
    }

    private fun task(attemptId: String, taskId: String = "task-001", samples: Long = 1_000, type: String = "Monte-Carlo-Pi") =
        """{"taskId":"$taskId","type":"$type","samples":$samples,"attemptId":"$attemptId","attemptNumber":1}"""

    // ------------------------------------------------------------------
    // connection and registration
    // ------------------------------------------------------------------

    @Test
    fun theDeviceIdIsTheBrokerUserAndTheWillSaysOffline() {
        start()

        assertEquals("ssl://broker.test:8883", link.settings.brokerUri)
        assertEquals(DEVICE, link.settings.username)
        assertEquals(DEVICE, link.settings.clientId)
        assertEquals("phoenix/v1/nodes/$DEVICE/up/status", link.settings.will.topic)
        assertEquals("offline", JSONObject(link.settings.will.payload).getString("state"))
        assertTrue(link.settings.will.retained)
    }

    @Test
    fun onConnectingItSubscribesAnnouncesItselfAndRegisters() {
        start()
        link.connect()

        assertEquals(listOf(DOWN + "#", CORE_STATUS), link.subscriptions)

        val status = link.sent("status").single()
        assertTrue(status.retained)
        assertEquals("online", JSONObject(status.payload).getString("state"))

        val register = link.bodies("register").single()
        assertEquals(DEVICE, register.getString("deviceId"))
        assertEquals("ANDROID", register.getString("hardwareClass"))
        assertEquals("android", register.getString("adapterType"))
        assertEquals(4, register.getInt("cpuCores"))
        assertEquals(4096, register.getInt("memoryMb"))
        assertEquals("attempt-id", register.getJSONArray("protocolFeatures").getString(0))
        assertEquals(1, register.getInt("protocolVersion"))
        assertEquals("enroll-123456789", register.getString("enrollmentToken"))
        assertTrue(register.getString("requestId").isNotBlank())
    }

    @Test
    fun withoutAnEnrollmentTokenNoneIsSent() {
        start(token = null)
        link.connect()

        assertFalse(link.bodies("register").single().has("enrollmentToken"))
    }

    @Test
    fun anAcceptedRegistrationConnectsOnceAndStartsHeartbeats() {
        start()
        link.connect()

        link.answer(requestId = "someone-elses-request")
        assertEquals(0, outcome.connected.get())

        link.answer()
        link.answer() // duplicate answer

        assertTrue(eventually { outcome.connected.get() == 1 })
        assertTrue(outcome.logs.contains("Registered with Phoenix."))
        assertEquals(1, outcome.logs.count { it == "Registered with Phoenix." })

        val heartbeat = link.sent("heartbeat").first()
        assertEquals(0, heartbeat.qos)
        val body = JSONObject(heartbeat.payload)
        assertEquals("tok-1", body.getString("sessionToken"))
        assertEquals(-1.0, body.getDouble("cpuUsagePercent"), 0.0)
        assertEquals(2048, body.getInt("availableRamMb"))
        assertEquals(6.0, body.getDouble("energyCostEstimate"), 0.0)
        assertTrue(outcome.logs.any { it.startsWith("Heartbeat sent:") })
    }

    @Test
    fun aRefusedRegistrationFailsTheConnection() {
        start()
        link.connect()
        link.deliver(
            "registered",
            """{"requestId":"${link.lastRequestId()}","status":"error","message":"enrollment_rejected"}"""
        )

        assertTrue(eventually { outcome.failures.isNotEmpty() })
        assertTrue(outcome.failures.single().contains("enrollment_rejected"))
        assertFalse(agent.running)
        assertTrue(link.closed.await(5, TimeUnit.SECONDS))
        assertEquals(0, outcome.connected.get())
    }

    @Test
    fun anUnansweredRegistrationIsSentAgainWithTheSameRequestId() {
        start()
        link.connect()

        assertTrue(eventually { link.sent("register").size >= 2 })
        val ids = link.bodies("register").map { it.getString("requestId") }.toSet()
        assertEquals(1, ids.size)

        link.answer()
        val after = link.sent("register").size
        Thread.sleep(400)
        assertEquals(after, link.sent("register").size)
    }

    @Test
    fun aBrokerThatIsNotReachableIsRetriedButRefusedCredentialsAreFatal() {
        start()

        link.listener.connectFailed("connection refused", fatal = false)
        assertTrue(agent.running)
        assertTrue(outcome.logs.any { it.startsWith("Broker not reachable") })

        link.listener.connectFailed("the broker refused this device's user name or password", fatal = true)
        assertFalse(agent.running)
        assertTrue(outcome.failures.single().contains("user name or password"))
    }

    @Test
    fun aReconnectKeepsTheSessionAndHeartbeatsAtOnce() {
        registered()
        val heartbeats = link.sent("heartbeat").size

        link.drop()
        assertTrue(outcome.logs.any { it.startsWith("Broker connection lost:") })

        link.connect(reconnect = true)

        assertEquals(1, link.sent("register").size)
        assertTrue(link.sent("heartbeat").size > heartbeats)
        assertTrue(outcome.logs.contains("Reconnected to the broker."))
    }

    @Test
    fun heartbeatsAreNotSentWhileDisconnected() {
        registered()
        link.drop()
        val heartbeats = link.sent("heartbeat").size

        Thread.sleep(1_200)

        assertEquals(heartbeats, link.sent("heartbeat").size)
    }

    @Test
    fun aReregisterRequestOrANewCoreInstanceRegistersAgain() {
        registered()

        link.coreStatus("""{"state":"online","coreInstanceId":"core-1"}""")
        assertEquals(1, link.sent("register").size)

        link.deliver("control", """{"type":"reregister","reason":"invalid_session"}""")
        assertEquals(2, link.sent("register").size)
        link.answer(token = "tok-2")

        link.coreStatus("""{"state":"online","coreInstanceId":"core-2"}""")
        assertEquals(3, link.sent("register").size)

        val ids = link.bodies("register").map { it.getString("requestId") }
        assertEquals(3, ids.toSet().size)

        link.deliver("control", """{"type":"something-new"}""")
        assertEquals(3, link.sent("register").size)
    }

    @Test
    fun coreComingOnlineResendsAPendingRegistration() {
        start()
        link.connect()
        val requestId = link.lastRequestId()

        link.coreStatus("""{"state":"online","coreInstanceId":"core-9"}""")

        assertEquals(listOf(requestId, requestId), link.bodies("register").map { it.getString("requestId") })
    }

    // ------------------------------------------------------------------
    // tasks
    // ------------------------------------------------------------------

    @Test
    fun aTaskIsAcknowledgedRunAndReportedWithItsAttemptId() {
        registered()

        link.deliver("task", task("attempt-1"))

        assertTrue(eventually { link.sent("result").isNotEmpty() })

        val ack = link.bodies("ack").single()
        val started = link.bodies("started").single()
        val result = link.bodies("result").single()

        for (report in listOf(ack, started, result)) {
            assertEquals("tok-1", report.getString("sessionToken"))
            assertEquals("task-001", report.getString("taskId"))
            assertEquals("attempt-1", report.getString("attemptId"))
        }
        assertEquals("SUCCESS", result.getString("status"))
        assertEquals("Computed Monte-Carlo pi ~= 3.14", result.getString("outputData"))
        assertEquals(7, result.getLong("executionTimeMs"))
        assertEquals(listOf(1_000L), runs)

        val order = link.published.map { it.topic.substringAfterLast('/') }.filter { it in setOf("ack", "started", "result") }
        assertEquals(listOf("ack", "started", "result"), order)
        assertTrue(outcome.logs.any { it.startsWith("Received task task-001,") })
        assertTrue(outcome.logs.any { it.startsWith("Task task-001 done:") })
    }

    @Test
    fun aRedeliveredAttemptIsAcknowledgedAgainButRunOnlyOnce() {
        registered()

        link.deliver("task", task("attempt-1"))
        assertTrue(eventually { link.sent("result").isNotEmpty() })

        link.deliver("task", task("attempt-1"))

        assertEquals(2, link.sent("ack").size)
        assertEquals(1, runs.size)
        // The known result is sent again, for Core to ignore if it has it.
        assertEquals(2, link.sent("result").size)
        assertEquals(link.bodies("result")[0].getString("outputData"), link.bodies("result")[1].getString("outputData"))
        assertTrue(outcome.logs.any { it.contains("not executed twice") })
    }

    @Test
    fun aDuplicateWhileTheAttemptRunsIsOnlyAcknowledged() {
        val release = CountDownLatch(1)
        workload = { samples, _ ->
            runs += samples
            release.await(5, TimeUnit.SECONDS)
            MonteCarloExecutor.Result("done", 1)
        }
        registered()

        link.deliver("task", task("attempt-1"))
        assertTrue(eventually { runs.size == 1 })
        link.deliver("task", task("attempt-1"))
        release.countDown()

        assertTrue(eventually { link.sent("result").isNotEmpty() })
        Thread.sleep(100)
        assertEquals(2, link.sent("ack").size)
        assertEquals(1, runs.size)
        assertEquals(1, link.sent("result").size)
    }

    @Test
    fun aRetryOfTheSameTaskIsANewAttemptAndRuns() {
        registered()

        link.deliver("task", task("attempt-1"))
        assertTrue(eventually { link.sent("result").size == 1 })
        link.deliver("task", task("attempt-2"))
        assertTrue(eventually { link.sent("result").size == 2 })

        assertEquals(2, runs.size)
        assertEquals(listOf("attempt-1", "attempt-2"), link.bodies("result").map { it.getString("attemptId") })
    }

    @Test
    fun aCancelThatOvertakesItsTaskMeansTheTaskIsNotRun() {
        registered()

        link.deliver("cancel", """{"taskId":"task-001","attemptId":"attempt-1","reason":"delivery_timeout"}""")
        link.deliver("task", task("attempt-1"))
        Thread.sleep(200)

        assertEquals(1, link.sent("ack").size)
        assertTrue(runs.isEmpty())
        assertTrue(link.sent("started").isEmpty())
        assertTrue(link.sent("result").isEmpty())
    }

    @Test
    fun cancellingARunningAttemptStopsItWithoutAResult() {
        workload = { samples, stop ->
            runs += samples
            while (!stop()) Thread.sleep(5)
            throw CancellationException("stopped")
        }
        registered()

        link.deliver("task", task("attempt-1"))
        assertTrue(eventually { runs.size == 1 })
        link.deliver("cancel", """{"taskId":"task-001","attemptId":"attempt-1","reason":"execution_timeout"}""")

        assertTrue(eventually { outcome.logs.any { it.contains("cancelled by Phoenix Core; no result sent") } })
        assertTrue(link.sent("result").isEmpty())
    }

    @Test
    fun theValidationHoldDelaysExecutionAndACancelEndsIt() {
        registered(holdSeconds = 30)

        link.deliver("task", task("attempt-1"))
        assertTrue(eventually { outcome.logs.any { it.startsWith("Holding task task-001 for 30 s") } })
        assertEquals(1, link.sent("started").size)
        assertTrue(runs.isEmpty())

        link.deliver("cancel", """{"taskId":"task-001","attemptId":"attempt-1","reason":"execution_timeout"}""")

        assertTrue(eventually { outcome.logs.any { it.contains("cancelled by Phoenix Core; no result sent") } })
        assertTrue(runs.isEmpty())
        assertTrue(link.sent("result").isEmpty())
    }

    @Test
    fun disconnectingDuringTheValidationHoldSendsNoResult() {
        registered(holdSeconds = 30)

        link.deliver("task", task("attempt-1"))
        assertTrue(eventually { outcome.logs.any { it.startsWith("Holding task task-001") } })
        agent.disconnect()
        Thread.sleep(300)

        assertTrue(runs.isEmpty())
        assertTrue(link.sent("result").isEmpty())
    }

    @Test
    fun unsupportedWorkReportsAFailedResult() {
        registered()

        link.deliver("task", task("attempt-1", type = "Matrix-Multiply"))
        link.deliver("task", task("attempt-2", taskId = "task-002", samples = 0))

        assertTrue(eventually { link.sent("result").size == 2 })
        val results = link.bodies("result")
        assertEquals(listOf("FAILED", "FAILED"), results.map { it.getString("status") })
        assertTrue(results[0].getString("outputData").contains("Unsupported task type"))
        assertTrue(results[1].getString("outputData").contains("Invalid sample count"))
        assertTrue(runs.isEmpty())
        assertTrue(outcome.logs.any { it.startsWith("Task task-001 failed:") })
    }

    @Test
    fun aResultAfterReRegisteringCarriesTheNewToken() {
        val release = CountDownLatch(1)
        workload = { samples, _ ->
            runs += samples
            release.await(5, TimeUnit.SECONDS)
            MonteCarloExecutor.Result("done", 1)
        }
        registered()

        link.deliver("task", task("attempt-1"))
        assertTrue(eventually { runs.size == 1 })

        link.deliver("control", """{"type":"reregister","reason":"invalid_session"}""")
        link.answer(token = "tok-2")
        release.countDown()

        assertTrue(eventually { link.sent("result").isNotEmpty() })
        assertEquals("tok-2", link.bodies("result").single().getString("sessionToken"))
    }

    @Test
    fun aTaskBeforeRegistrationFinishedIsNotRun() {
        start()
        link.connect()

        link.deliver("task", task("attempt-1"))
        Thread.sleep(150)

        assertEquals(1, link.sent("ack").size)
        assertTrue(runs.isEmpty())
    }

    @Test
    fun malformedOrIncompleteMessagesAreIgnored() {
        registered()

        link.deliver("task", "not json")
        link.deliver("task", """{"taskId":"task-001"}""")
        link.deliver("registered", "[]")
        link.listener.messageArrived("phoenix/v1/nodes/$DEVICE/down/unknown-kind", "{}".toByteArray())

        assertTrue(link.sent("ack").isEmpty())
        assertTrue(outcome.logs.any { it == "Ignored a malformed task message." })

        // Still working.
        link.deliver("task", task("attempt-1"))
        assertTrue(eventually { link.sent("result").isNotEmpty() })
    }

    // ------------------------------------------------------------------
    // stopping
    // ------------------------------------------------------------------

    @Test
    fun disconnectingSaysOfflineAndIgnoresLaterMessages() {
        registered()

        agent.disconnect()

        assertTrue(link.closed.await(5, TimeUnit.SECONDS))
        val farewell = link.farewell!!
        assertEquals("phoenix/v1/nodes/$DEVICE/up/status", farewell.topic)
        assertEquals("offline", JSONObject(farewell.payload).getString("state"))
        assertTrue(farewell.retained)
        assertFalse(agent.running)
        assertEquals("Agent stopped.", outcome.logs.last())

        link.deliver("task", task("attempt-1"))
        assertTrue(link.sent("ack").isEmpty())
    }

    @Test
    fun aReconnectIsANewSessionWithANewRegistration() {
        registered()
        val first = link.lastRequestId()

        agent.disconnect()
        start()
        link.connect()

        assertNotEquals(first, link.lastRequestId())
        assertEquals(2, link.sent("register").size)
    }
}
