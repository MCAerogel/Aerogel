package org.macaroon3145.world

import org.macaroon3145.config.ServerConfig
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.LongAdder

class ChunkProcessingProfiler {
    data class ChunkStatSnapshot(
        val chunkPos: ChunkPos,
        val mspt: Double,
        val tps: Double
    )

    class Frame internal constructor(
        private val owner: ChunkProcessingProfiler,
        internal val tickSequence: Long
    ) {
        private val totalsByChunk = ConcurrentHashMap<ChunkPos, LongAdder>()
        private val closed = AtomicBoolean(false)

        fun record(chunkPos: ChunkPos, elapsedNanos: Long) {
            if (closed.get()) {
                owner.recordImmediate(
                    chunkPos = chunkPos,
                    elapsedNanos = elapsedNanos,
                    accumulateIntoLast = true,
                    tickSequence = tickSequence
                )
                return
            }
            totalsByChunk
                .computeIfAbsent(chunkPos) { LongAdder() }
                .add(elapsedNanos.coerceAtLeast(0L))
        }

        internal fun close() {
            closed.set(true)
        }

        internal fun snapshotNanosByChunk(): Map<ChunkPos, Long> {
            if (totalsByChunk.isEmpty()) return emptyMap()
            val out = HashMap<ChunkPos, Long>(totalsByChunk.size)
            for ((chunkPos, total) in totalsByChunk) {
                out[chunkPos] = total.sum()
            }
            return out
        }
    }

    private data class ChunkStat(
        val lastMs: Double,
        val ewmaMs: Double,
        val lastIntervalMs: Double,
        val ewmaIntervalMs: Double,
        val lastTps: Double,
        val ewmaTps: Double,
        val updatedAtNanos: Long,
        val meaningfulUpdatedAtNanos: Long,
        val lastTickSequence: Long
    )

    private val statsByChunk = ConcurrentHashMap<ChunkPos, ChunkStat>()

    fun beginFrame(tickSequence: Long = NO_TICK_SEQUENCE): Frame = Frame(this, tickSequence)

    fun finishFrame(
        frame: Frame,
        activeChunks: Set<ChunkPos>,
        includeZeroForInactive: Boolean = true,
        accumulateIntoLast: Boolean = false
    ) {
        frame.close()
        val frameTotals = frame.snapshotNanosByChunk()
        val nowNanos = System.nanoTime()

        for ((chunkPos, totalNanos) in frameTotals) {
            updateChunkStat(
                chunkPos = chunkPos,
                lastMs = totalNanos / 1_000_000.0,
                updatedAtNanos = nowNanos,
                accumulateIntoLast = accumulateIntoLast,
                tickSequence = frame.tickSequence
            )
        }

        if (includeZeroForInactive) {
            for (chunkPos in activeChunks) {
                if (frameTotals.containsKey(chunkPos)) continue
                updateChunkStat(
                    chunkPos = chunkPos,
                    lastMs = 0.0,
                    updatedAtNanos = nowNanos,
                    accumulateIntoLast = accumulateIntoLast,
                    tickSequence = frame.tickSequence
                )
            }
        }
    }

    fun mspt(chunkX: Int, chunkZ: Int): Double {
        val stat = statsByChunk[ChunkPos(chunkX, chunkZ)]
        return stat?.lastMs ?: 0.0
    }

    fun tps(chunkX: Int, chunkZ: Int): Double {
        val stat = statsByChunk[ChunkPos(chunkX, chunkZ)] ?: return configuredTargetTps()
        val ageMs = ((System.nanoTime() - stat.updatedAtNanos).coerceAtLeast(0L)) / 1_000_000.0
        val intervalMs = maxOf(stat.lastIntervalMs, ageMs)
        return chunkTpsFromIntervalMs(intervalMs)
    }

    fun ewmaMspt(chunkX: Int, chunkZ: Int): Double {
        val stat = statsByChunk[ChunkPos(chunkX, chunkZ)]
        return stat?.ewmaMs ?: 0.0
    }

    fun ewmaTps(chunkX: Int, chunkZ: Int): Double {
        val stat = statsByChunk[ChunkPos(chunkX, chunkZ)] ?: return configuredTargetTps()
        val ageMs = ((System.nanoTime() - stat.updatedAtNanos).coerceAtLeast(0L)) / 1_000_000.0
        val intervalMs = maxOf(stat.ewmaIntervalMs, ageMs)
        return chunkTpsFromIntervalMs(intervalMs)
    }

    fun isChunkIdle(chunkX: Int, chunkZ: Int): Boolean {
        val stat = statsByChunk[ChunkPos(chunkX, chunkZ)] ?: return true
        val ageMs = ((System.nanoTime() - stat.meaningfulUpdatedAtNanos).coerceAtLeast(0L)) / 1_000_000.0
        val idleThresholdMs = maxOf(stat.lastIntervalMs * IDLE_INTERVAL_MULTIPLIER, IDLE_MIN_AGE_MS)
        return ageMs > idleThresholdMs
    }

    fun topChunksByMspt(limit: Int, minMspt: Double = 0.0): List<ChunkStatSnapshot> {
        if (limit <= 0) return emptyList()
        val snapshots = ArrayList<ChunkStatSnapshot>(statsByChunk.size)
        for ((chunkPos, stat) in statsByChunk) {
            val mspt = stat.lastMs
            if (!mspt.isFinite() || mspt < minMspt) continue
            val tps = stat.lastTps
            snapshots += ChunkStatSnapshot(
                chunkPos = chunkPos,
                mspt = mspt,
                tps = if (tps.isFinite()) tps else 0.0
            )
        }
        if (snapshots.isEmpty()) return emptyList()
        snapshots.sortWith(
            compareByDescending<ChunkStatSnapshot> { it.mspt }
                .thenByDescending { it.tps }
                .thenBy { it.chunkPos.x }
                .thenBy { it.chunkPos.z }
        )
        return if (snapshots.size <= limit) snapshots else snapshots.subList(0, limit)
    }

    private fun updateChunkStat(
        chunkPos: ChunkPos,
        lastMs: Double,
        updatedAtNanos: Long,
        accumulateIntoLast: Boolean,
        tickSequence: Long
    ) {
        statsByChunk.compute(chunkPos) { _, previous ->
            val defaultIntervalMs = 1000.0 / configuredTargetTps()
            val observedIntervalMs = if (previous == null) {
                defaultIntervalMs
            } else {
                ((updatedAtNanos - previous.updatedAtNanos).coerceAtLeast(0L)) / 1_000_000.0
            }
            val sameLogicalTick = accumulateIntoLast &&
                previous != null &&
                tickSequence != NO_TICK_SEQUENCE &&
                previous.lastTickSequence == tickSequence
            val effectiveLastMs = if (sameLogicalTick) {
                previous!!.lastMs + lastMs
            } else {
                lastMs
            }
            val effectiveIntervalMs = if (previous == null) {
                observedIntervalMs
            } else if (sameLogicalTick) {
                previous.lastIntervalMs
            } else {
                observedIntervalMs.coerceAtLeast(0.001)
            }
            val lastTps = chunkTpsFromIntervalMs(effectiveIntervalMs)
            val meaningfulUpdatedAtNanos = if (effectiveLastMs >= IDLE_NOISE_MSPT) {
                updatedAtNanos
            } else {
                previous?.meaningfulUpdatedAtNanos ?: updatedAtNanos
            }
            if (previous == null) {
                ChunkStat(
                    lastMs = effectiveLastMs,
                    ewmaMs = effectiveLastMs,
                    lastIntervalMs = effectiveIntervalMs,
                    ewmaIntervalMs = effectiveIntervalMs,
                    lastTps = lastTps,
                    ewmaTps = lastTps,
                    updatedAtNanos = updatedAtNanos,
                    meaningfulUpdatedAtNanos = meaningfulUpdatedAtNanos,
                    lastTickSequence = tickSequence
                )
            } else {
                ChunkStat(
                    lastMs = effectiveLastMs,
                    ewmaMs = previous.ewmaMs * EWMA_KEEP + effectiveLastMs * EWMA_NEW,
                    lastIntervalMs = effectiveIntervalMs,
                    ewmaIntervalMs = previous.ewmaIntervalMs * EWMA_KEEP + effectiveIntervalMs * EWMA_NEW,
                    lastTps = lastTps,
                    ewmaTps = previous.ewmaTps * EWMA_KEEP + lastTps * EWMA_NEW,
                    updatedAtNanos = updatedAtNanos,
                    meaningfulUpdatedAtNanos = meaningfulUpdatedAtNanos,
                    lastTickSequence = if (tickSequence != NO_TICK_SEQUENCE) tickSequence else previous.lastTickSequence
                )
            }
        }
    }

    private fun recordImmediate(
        chunkPos: ChunkPos,
        elapsedNanos: Long,
        accumulateIntoLast: Boolean,
        tickSequence: Long
    ) {
        updateChunkStat(
            chunkPos = chunkPos,
            lastMs = elapsedNanos.coerceAtLeast(0L) / 1_000_000.0,
            updatedAtNanos = System.nanoTime(),
            accumulateIntoLast = accumulateIntoLast,
            tickSequence = tickSequence
        )
    }

    private fun chunkTpsFromIntervalMs(intervalMs: Double): Double {
        val cap = configuredTargetTps()
        if (!intervalMs.isFinite() || intervalMs <= 0.0) return cap
        val raw = 1000.0 / intervalMs
        if (!raw.isFinite() || raw <= 0.0) return 0.0
        return raw.coerceAtMost(cap)
    }

    private fun configuredTargetTps(): Double {
        val configured = ServerConfig.maxTps
        return if (configured.isFinite() && configured > 0.0) configured else 20.0
    }

    private companion object {
        private const val EWMA_KEEP = 0.8
        private const val EWMA_NEW = 0.2
        private const val NO_TICK_SEQUENCE = Long.MIN_VALUE
        private const val IDLE_INTERVAL_MULTIPLIER = 1.5
        private const val IDLE_MIN_AGE_MS = 100.0
        private const val IDLE_NOISE_MSPT = 0.05
    }
}
