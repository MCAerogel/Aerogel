package org.macaroon3145.perf

import org.macaroon3145.config.ServerConfig
import org.macaroon3145.network.handler.PlayerSessionManager
import org.macaroon3145.ui.ServerDashboard
import org.macaroon3145.world.storage.VanillaAnvilWorldSaver
import kotlin.math.roundToLong

object GameLoop {
    private const val MAX_LAG_TICKS_BEFORE_RESYNC = 10L

    @Volatile
    private var running = false

    @Volatile
    private var loopThread: Thread? = null

    fun start() {
        if (running) return
        running = true
        TickTime.reset()
        loopThread = Thread(::runLoop, "aerogel-game-loop").apply {
            isDaemon = true
            start()
        }
    }

    fun stop() {
        running = false
        loopThread?.interrupt()
        loopThread = null
    }

    private fun runLoop() {
        var lastConfiguredMaxTps = Double.NaN
        var tickIntervalNanos = 50_000_000L
        var uncapped = false
        var nextTickDeadline = System.nanoTime()

        while (running) {
            val configuredMaxTps = ServerConfig.maxTps
            if (configuredMaxTps != lastConfiguredMaxTps) {
                lastConfiguredMaxTps = configuredMaxTps
                if (configuredMaxTps == -1.0) {
                    uncapped = true
                } else {
                    uncapped = false
                    tickIntervalNanos = (1_000_000_000.0 / configuredMaxTps)
                        .roundToLong()
                        .coerceAtLeast(1L)
                    nextTickDeadline = System.nanoTime() + tickIntervalNanos
                }
            }

            if (!uncapped) {
                var now = System.nanoTime()
                var remaining = nextTickDeadline - now

                while (remaining > 0L) {
                    Thread.onSpinWait()
                    now = System.nanoTime()
                    remaining = nextTickDeadline - now
                }
            }

            val tickStartNanos = System.nanoTime()
            val deltaSeconds = TickTime.advanceAndGetDeltaSeconds(tickStartNanos)
            PlayerSessionManager.tick(deltaSeconds)
            VanillaAnvilWorldSaver.tickAutosave(tickStartNanos)
            val tickEndNanos = System.nanoTime()
            PerformanceMonitor.recordTick(tickStartNanos, tickEndNanos)
            ServerDashboard.onTick()

            if (!uncapped) {
                nextTickDeadline += tickIntervalNanos
                val lateBy = tickEndNanos - nextTickDeadline
                if (lateBy > tickIntervalNanos * MAX_LAG_TICKS_BEFORE_RESYNC) {
                    nextTickDeadline = tickEndNanos + tickIntervalNanos
                }
            }
        }
    }
}
