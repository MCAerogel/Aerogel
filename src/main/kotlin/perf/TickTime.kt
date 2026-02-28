package org.macaroon3145.perf

object TickTime {
    @Volatile
    var deltaNanos: Long = 0L
        private set

    @Volatile
    var deltaSeconds: Double = 0.0
        private set

    @Volatile
    var lastTickStartNanos: Long = 0L
        private set

    fun reset(nowNanos: Long = System.nanoTime()) {
        lastTickStartNanos = nowNanos
        deltaNanos = 0L
        deltaSeconds = 0.0
    }

    fun advanceAndGetDeltaSeconds(nowNanos: Long): Double {
        val previous = lastTickStartNanos
        val elapsedNanos = if (previous == 0L) 0L else (nowNanos - previous).coerceAtLeast(0L)
        lastTickStartNanos = nowNanos
        deltaNanos = elapsedNanos
        deltaSeconds = elapsedNanos / 1_000_000_000.0
        return deltaSeconds
    }
}
