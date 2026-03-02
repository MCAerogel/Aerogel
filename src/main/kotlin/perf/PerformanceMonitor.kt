package org.macaroon3145.perf

import org.macaroon3145.i18n.ServerI18n
import kotlin.math.abs

object PerformanceMonitor {
    private const val LOG_TO_CONSOLE = false
    private const val EWMA_KEEP = 0.8
    private const val EWMA_NEW = 0.2
    private var lastTickEndNanos: Long = 0L
    private var initialized = false

    @Volatile
    var tps: Double = 20.0
        private set

    @Volatile
    var mspt: Double = 0.0
        private set
    @Volatile
    var rawMspt: Double = 0.0
        private set

    fun start() {
        lastTickEndNanos = 0L
        initialized = false
        rawMspt = 0.0
        mspt = 0.0
        tps = 0.0
    }

    fun recordTick(tickStartNanos: Long, tickEndNanos: Long) {
        val tickDurationNanos = (tickEndNanos - tickStartNanos).coerceAtLeast(0L)
        val tickMs = tickDurationNanos / 1_000_000.0
        rawMspt = tickMs
        val instantTps = if (lastTickEndNanos > 0L) {
            val intervalNanos = (tickEndNanos - lastTickEndNanos).coerceAtLeast(1L)
            1_000_000_000.0 / intervalNanos.toDouble()
        } else {
            0.0
        }
        lastTickEndNanos = tickEndNanos
        if (!initialized) {
            mspt = tickMs
            tps = instantTps.coerceAtLeast(0.0)
            initialized = true
        } else {
            mspt = (mspt * EWMA_KEEP) + (tickMs * EWMA_NEW)
            tps = (tps * EWMA_KEEP) + (instantTps.coerceAtLeast(0.0) * EWMA_NEW)
        }
        if (LOG_TO_CONSOLE) {
            ServerI18n.log("aerogel.log.perf", format(tps), format(mspt))
        }
    }

    fun tpsString(): String = format(tps)

    fun msptString(): String = format(mspt)

    private fun format(value: Double): String {
        val magnitude = abs(value)
        val decimals = when {
            magnitude >= 1.0 -> 2
            magnitude >= 0.1 -> 3
            magnitude >= 0.01 -> 4
            magnitude >= 0.001 -> 5
            else -> 6
        }
        return String.format("%.${decimals}f", value)
    }
}
