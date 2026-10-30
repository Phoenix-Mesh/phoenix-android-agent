// SPDX-License-Identifier: Apache-2.0

package com.phoenix.agent

import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import java.util.concurrent.CancellationException

class MonteCarloExecutorTest {

    private val executor = MonteCarloExecutor()

    @Test
    fun estimatesPiWithinASensibleTolerance() {
        val result = executor.execute(200_000)
        val pi = result.output.substringAfter("~= ").toDouble()

        assertEquals(Math.PI, pi, 0.05)
        assertTrue(result.executionTimeMs >= 0)
    }

    @Test
    fun aSingleSampleStillProducesAResult() {
        assertTrue(executor.execute(1).output.startsWith("Computed Monte-Carlo pi"))
    }

    @Test(expected = IllegalArgumentException::class)
    fun rejectsZeroSamples() {
        executor.execute(0)
    }

    @Test(expected = IllegalArgumentException::class)
    fun rejectsNegativeSamples() {
        executor.execute(-5)
    }

    @Test(expected = CancellationException::class)
    fun aRunStopsWhenAskedTo() {
        executor.execute(1_000_000_000L) { true }
    }

    @Test
    fun aRunThatIsNotAskedToStopCompletes() {
        var asked = 0
        executor.execute(200_000) { asked++; false }
        assertTrue(asked in 1..10)
    }
}
