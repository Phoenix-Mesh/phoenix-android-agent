// SPDX-License-Identifier: Apache-2.0

package com.phoenix.agent

import org.json.JSONObject
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class PhoenixNodeAgentTest {

    private class FakeIdentity : NodeIdentity {
        override val deviceId = "android-test"
    }

    private class FakeTelemetry : NodeTelemetry {
        override val displayName = "Test-Device"
        override val architecture = "arm64-v8a"
        override val cpuCores = 4
        override val totalRamMb = 4096L
        override val availableRamMb = 2048L
    }

    private data class Post(val endpoint: String, val body: String) {
        val path: String get() = endpoint.substringAfter("://").substringAfter('/')
    }

    /**
     * Scriptable Core. [onPost] decides each POST response and may throw;
     * [onPoll] answers every poll. All POSTs are recorded.
     */
    private class FakeHttp(
        private val onPost: (Post) -> String = { "" },
        private val onPoll: () -> Pair<Int, String> = { 204 to "" }
    ) : PhoenixHttpClient() {
        val posts = CopyOnWriteArrayList<Post>()

        override fun postJson(endpoint: String, body: String): String {
            val post = Post(endpoint, body)
            posts.add(post)
            return onPost(post)
        }

        override fun get(endpoint: String): Pair<Int, String> = onPoll()

        fun paths(): List<String> = posts.map { it.path }
    }

    /**
     * Blocks like a real HttpURLConnection, which does not respond to
     * Thread.interrupt(): disconnect() cannot cut the request short.
     */
    private fun CountDownLatch.awaitIgnoringInterrupts() {
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
        while (count > 0 && System.nanoTime() < deadline) {
            try {
                await(50, TimeUnit.MILLISECONDS)
            } catch (e: InterruptedException) {
                // keep waiting, as a blocking socket read would
            }
        }
    }

    private class Outcome {
        val logs = CopyOnWriteArrayList<String>()
        val connected = AtomicInteger()
        val failed = AtomicInteger()
        val connectedLatch = CountDownLatch(1)
        val failedLatch = CountDownLatch(1)

        fun count(message: String) = logs.count { it == message }
    }

    private fun agent(http: PhoenixHttpClient, outcome: Outcome) =
        PhoenixNodeAgent(FakeIdentity(), FakeTelemetry(), http) { outcome.logs.add(it) }

    private fun connect(agent: PhoenixNodeAgent, outcome: Outcome, url: String = "http://core.invalid") =
        agent.connect(
            url,
            onConnected = {
                outcome.connected.incrementAndGet()
                outcome.connectedLatch.countDown()
            },
            onFailed = {
                outcome.failed.incrementAndGet()
                outcome.failedLatch.countDown()
            }
        )

    private fun awaitLog(outcome: Outcome, timeoutMs: Long = 5_000, predicate: (String) -> Boolean): Boolean {
        val deadline = System.currentTimeMillis() + timeoutMs
        while (System.currentTimeMillis() < deadline) {
            if (outcome.logs.any(predicate)) return true
            Thread.sleep(20)
        }
        return false
    }

    @Test
    fun connectRegistersAndStartsHeartbeats() {
        val outcome = Outcome()
        val http = FakeHttp()
        val agent = agent(http, outcome)

        connect(agent, outcome)

        assertTrue(outcome.connectedLatch.await(5, TimeUnit.SECONDS))
        assertTrue(agent.running)
        assertTrue(awaitLog(outcome) { it.startsWith("Heartbeat sent:") })
        assertEquals("register", http.paths().first())
        agent.disconnect()
    }

    @Test
    fun disconnectDuringRegistrationCancelsTheAttempt() {
        val started = CountDownLatch(1)
        val release = CountDownLatch(1)
        val http = FakeHttp(onPost = {
            started.countDown()
            release.awaitIgnoringInterrupts()
            ""
        })
        val outcome = Outcome()
        val agent = agent(http, outcome)

        connect(agent, outcome)
        assertTrue(started.await(5, TimeUnit.SECONDS))

        agent.disconnect()
        release.countDown()
        Thread.sleep(300)

        assertFalse(agent.running)
        assertEquals(0, outcome.connected.get())
        assertEquals(0, outcome.failed.get())
        assertEquals(0, outcome.count("Registered with Phoenix."))
        assertEquals(listOf("register"), http.paths())
    }

    @Test
    fun registrationFailureAfterDisconnectIsNotReported() {
        val started = CountDownLatch(1)
        val release = CountDownLatch(1)
        val http = FakeHttp(onPost = {
            started.countDown()
            release.awaitIgnoringInterrupts()
            throw HttpStatusException(500, "boom")
        })
        val outcome = Outcome()
        val agent = agent(http, outcome)

        connect(agent, outcome)
        assertTrue(started.await(5, TimeUnit.SECONDS))

        agent.disconnect()
        release.countDown()
        Thread.sleep(300)

        assertEquals(0, outcome.failed.get())
        assertEquals(0, outcome.connected.get())
    }

    @Test
    fun registrationFailureIsReportedWhileStillConnecting() {
        val outcome = Outcome()
        val agent = agent(FakeHttp(onPost = { throw HttpStatusException(500, "boom") }), outcome)

        connect(agent, outcome)

        assertTrue(outcome.failedLatch.await(5, TimeUnit.SECONDS))
        assertFalse(agent.running)
        assertEquals(0, outcome.connected.get())
    }

    @Test
    fun staleAttemptDoesNotOverrideAReconnect() {
        val started = CountDownLatch(1)
        val release = CountDownLatch(1)
        val http = FakeHttp(onPost = { post ->
            if (post.endpoint.startsWith("http://old.invalid")) {
                started.countDown()
                release.awaitIgnoringInterrupts()
            }
            ""
        })
        val outcome = Outcome()
        val agent = agent(http, outcome)

        connect(agent, outcome, "http://old.invalid")
        assertTrue(started.await(5, TimeUnit.SECONDS))
        agent.disconnect()
        connect(agent, outcome, "http://new.invalid")
        release.countDown()

        assertTrue(outcome.connectedLatch.await(5, TimeUnit.SECONDS))
        assertTrue(awaitLog(outcome) { it.startsWith("Heartbeat sent:") })
        Thread.sleep(300)
        agent.disconnect()

        assertEquals(1, outcome.connected.get())
        // Only the first registration went to the old Core.
        assertEquals(1, http.posts.count { it.endpoint.startsWith("http://old.invalid") })
    }

    @Test
    fun reRegistersWhenCoreForgetsTheDevice() {
        val heartbeats = AtomicInteger()
        val http = FakeHttp(onPost = { post ->
            if (post.path == "heartbeat" && heartbeats.incrementAndGet() == 1) {
                throw HttpStatusException(404, """{"status":"error","message":"unknown_device"}""")
            }
            ""
        })
        val outcome = Outcome()
        val agent = agent(http, outcome)

        connect(agent, outcome)

        assertTrue(awaitLog(outcome) { it.startsWith("Phoenix Core does not know this device") })
        assertTrue(awaitLog(outcome) { outcome.count("Registered with Phoenix.") == 2 })
        assertEquals(2, http.paths().count { it == "register" })
        agent.disconnect()
    }

    @Test
    fun reRegistrationCompletingAfterDisconnectIsNotReported() {
        val registrations = AtomicInteger()
        val reRegisterStarted = CountDownLatch(1)
        val release = CountDownLatch(1)
        val http = FakeHttp(onPost = { post ->
            when (post.path) {
                "heartbeat" -> throw HttpStatusException(404, "unknown_device")
                "register" -> if (registrations.incrementAndGet() == 2) {
                    reRegisterStarted.countDown()
                    release.awaitIgnoringInterrupts()
                }
            }
            ""
        })
        val outcome = Outcome()
        val agent = agent(http, outcome)

        connect(agent, outcome)
        assertTrue(reRegisterStarted.await(5, TimeUnit.SECONDS))

        agent.disconnect()
        release.countDown()
        Thread.sleep(300)

        assertEquals(1, outcome.count("Registered with Phoenix."))
        assertFalse(outcome.logs.any { it.startsWith("Re-registration failed") })
        assertEquals("Agent stopped.", outcome.logs.last())
    }

    @Test
    fun staleReRegistrationDoesNotReportIntoANewSession() {
        val oldRegistrations = AtomicInteger()
        val reRegisterStarted = CountDownLatch(1)
        val release = CountDownLatch(1)
        val http = FakeHttp(onPost = { post ->
            val old = post.endpoint.startsWith("http://old.invalid")
            when {
                old && post.path == "heartbeat" -> throw HttpStatusException(404, "unknown_device")
                old && post.path == "register" && oldRegistrations.incrementAndGet() == 2 -> {
                    reRegisterStarted.countDown()
                    release.awaitIgnoringInterrupts()
                }
            }
            ""
        })
        val outcome = Outcome()
        val agent = agent(http, outcome)

        connect(agent, outcome, "http://old.invalid")
        assertTrue(reRegisterStarted.await(5, TimeUnit.SECONDS))

        agent.disconnect()
        connect(agent, outcome, "http://new.invalid")
        assertTrue(awaitLog(outcome) { outcome.count("Registered with Phoenix.") == 2 })

        release.countDown()
        Thread.sleep(300)
        agent.disconnect()

        // Initial connect plus the reconnect; the stale re-registration is silent.
        assertEquals(2, outcome.count("Registered with Phoenix."))
    }

    @Test
    fun executionFailureIsReportedAsFailed() {
        val delivered = AtomicInteger()
        val http = FakeHttp(onPoll = {
            if (delivered.incrementAndGet() == 1) 200 to """{"taskId":"t1","samples":0,"attemptId":"a-1"}"""
            else 204 to ""
        })
        val outcome = Outcome()
        val agent = agent(http, outcome)

        connect(agent, outcome)

        assertTrue(awaitLog(outcome) { it.startsWith("Task t1 failed:") })
        agent.disconnect()

        val result = JSONObject(http.posts.first { it.path == "results" }.body)
        assertEquals("FAILED", result.optString("status"))
        assertEquals("t1", result.optString("taskId"))
        assertEquals("a-1", result.optString("attemptId"))
    }

    @Test
    fun resultRejectedAsUnknownTaskIsReportedAsStale() {
        val outcome = runOneTaskWithResultError(
            HttpStatusException(404, """{"status":"error","message":"unknown_task"}""")
        )

        assertTrue(outcome.logs.any { it.startsWith("Task t1 result rejected") })
        assertFalse(outcome.logs.any { it.startsWith("Poll failed:") })
    }

    @Test
    fun otherNotFoundOnResultsIsReportedAsAFailure() {
        val outcome = runOneTaskWithResultError(HttpStatusException(404, "Not Found"))

        assertTrue(outcome.logs.any { it.startsWith("Poll failed:") })
        assertFalse(outcome.logs.any { it.startsWith("Task t1 result rejected") })
    }

    private fun runOneTaskWithResultError(error: HttpStatusException): Outcome {
        val delivered = AtomicInteger()
        val http = FakeHttp(
            onPost = { post -> if (post.path == "results") throw error else "" },
            onPoll = {
                if (delivered.incrementAndGet() == 1) 200 to """{"taskId":"t1","samples":1000}"""
                else 204 to ""
            }
        )
        val outcome = Outcome()
        val agent = agent(http, outcome)

        connect(agent, outcome)
        awaitLog(outcome) { it.startsWith("Task t1 result rejected") || it.startsWith("Poll failed:") }
        agent.disconnect()
        return outcome
    }
}
