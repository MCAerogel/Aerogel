package org.macaroon3145.perf

import org.macaroon3145.i18n.ServerI18n
import kotlin.math.abs

object PerformanceMonitor {
    private const val REPORT_INTERVAL_NANOS = 500_000_000L
    private const val LOG_TO_CONSOLE = false

    private val lock = Any()
    private var windowStartNanos: Long = System.nanoTime()
    private var ticks: Long = 0L
    private var msptSumNanos: Long = 0L

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
        synchronized(lock) {
            windowStartNanos = System.nanoTime()
            ticks = 0L
            msptSumNanos = 0L
            rawMspt = 0.0
            mspt = 0.0
            tps = 0.0
        }
    }

    fun recordTick(tickStartNanos: Long, tickEndNanos: Long) {
        val tickDurationNanos = (tickEndNanos - tickStartNanos).coerceAtLeast(0L)
        val tickMs = tickDurationNanos / 1_000_000.0
        rawMspt = tickMs

        synchronized(lock) {
            mspt = tickMs
            msptSumNanos += tickDurationNanos
            ticks += 1

            val windowElapsed = tickEndNanos - windowStartNanos
            if (windowElapsed >= REPORT_INTERVAL_NANOS) {
                val seconds = windowElapsed / 1_000_000_000.0
                tps = if (seconds <= 0.0) 0.0 else ticks / seconds
                val avgMs = if (ticks == 0L) 0.0 else (msptSumNanos / ticks) / 1_000_000.0
                mspt = avgMs
                if (LOG_TO_CONSOLE) {
                    ServerI18n.log("aerogel.log.perf", format(tps), format(avgMs))
                }

                windowStartNanos = tickEndNanos
                ticks = 0L
                msptSumNanos = 0L
            }
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
