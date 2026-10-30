// SPDX-License-Identifier: Apache-2.0

package com.phoenix.agent

import java.util.concurrent.CancellationException
import java.util.concurrent.ThreadLocalRandom

/** Estimates pi by Monte-Carlo sampling of the unit square. */
class MonteCarloExecutor {

    private companion object {
        /** [execute] asks whether to stop every 65,536 samples (a few ms). */
        const val STOP_CHECK_MASK = 0xFFFFL
    }

    data class Result(val output: String, val executionTimeMs: Long)

    /**
     * @param shouldStop polled while sampling; returning true abandons the run
     * @throws IllegalArgumentException if [samples] is not positive
     * @throws CancellationException if [shouldStop] returned true
     */
    fun execute(samples: Long, shouldStop: () -> Boolean = { false }): Result {
        require(samples > 0) { "samples must be positive, was $samples" }

        val random = ThreadLocalRandom.current()
        val start = System.nanoTime()
        var inside = 0L
        var i = 0L

        while (i < samples) {
            if ((i and STOP_CHECK_MASK) == 0L && shouldStop()) {
                throw CancellationException("stopped after $i of $samples samples")
            }
            val x = random.nextDouble()
            val y = random.nextDouble()
            if (x * x + y * y <= 1.0) {
                inside++
            }
            i++
        }

        val pi = 4.0 * inside / samples
        return Result("Computed Monte-Carlo pi ~= $pi", (System.nanoTime() - start) / 1_000_000)
    }
}
