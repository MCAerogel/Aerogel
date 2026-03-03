package org.macaroon3145.world

import org.macaroon3145.config.ServerConfig
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.LongAdder

class ChunkProcessingProfiler {
    data class ChunkStatSnapshot(
        val chunkPos: ChunkPos,
        val mspt: Double,
        val tps: Double,
        val breakdownMs: Map<String, Double>
    )

    class Frame internal constructor(
        private val owner: ChunkProcessingProfiler,
        internal val tickSequence: Long
    ) {
        private val totalsByChunk = ConcurrentHashMap<ChunkPos, LongAdder>()
        private val totalsByChunkAndCategory = ConcurrentHashMap<ChunkPos, ConcurrentHashMap<String, LongAdder>>()
        private val closed = AtomicBoolean(false)

        fun record(chunkPos: ChunkPos, elapsedNanos: Long, category: String = CATEGORY_OTHER) {
            if (closed.get()) {
                owner.recordImmediate(
                    chunkPos = chunkPos,
                    elapsedNanos = elapsedNanos,
                    accumulateIntoLast = true,
                    tickSequence = tickSequence,
                    breakdownMs = mapOf(category to (elapsedNanos.coerceAtLeast(0L) / 1_000_000.0))
                )
                return
            }
            val clamped = elapsedNanos.coerceAtLeast(0L)
            totalsByChunk
                .computeIfAbsent(chunkPos) { LongAdder() }
                .add(clamped)
            totalsByChunkAndCategory
                .computeIfAbsent(chunkPos) { ConcurrentHashMap() }
                .computeIfAbsent(category) { LongAdder() }
                .add(clamped)
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

        internal fun snapshotNanosByChunkAndCategory(): Map<ChunkPos, Map<String, Long>> {
            if (totalsByChunkAndCategory.isEmpty()) return emptyMap()
            val out = HashMap<ChunkPos, Map<String, Long>>(totalsByChunkAndCategory.size)
            for ((chunkPos, categoryTotals) in totalsByChunkAndCategory) {
                val byCategory = HashMap<String, Long>(categoryTotals.size)
                for ((category, total) in categoryTotals) {
                    byCategory[category] = total.sum()
                }
                out[chunkPos] = byCategory
            }
            return out
        }
    }

    private data class ChunkStat(
        val lastMs: Double,
        val lastIntervalMs: Double,
        val lastTps: Double,
        val measuredMs: Double,
        val measuredTps: Double,
        val updatedAtNanos: Long,
        val meaningfulUpdatedAtNanos: Long,
        val lastTickSequence: Long,
        val lastBreakdownMs: Map<String, Double>,
        val windowStartedAtNanos: Long,
        val windowAccumulatedWorkMs: Double,
        val windowAccumulatedIntervalMs: Double,
        val windowSampleCount: Int,
        val windowAccumulatedBreakdownMs: Map<String, Double>
    )

    private val statsByChunk = ConcurrentHashMap<ChunkPos, ChunkStat>()
    private val dirtyChunksQueue = ConcurrentLinkedQueue<ChunkPos>()
    private val dirtyChunksDedup = ConcurrentHashMap.newKeySet<ChunkPos>()

    fun beginFrame(tickSequence: Long = NO_TICK_SEQUENCE): Frame = Frame(this, tickSequence)

    fun finishFrame(
        frame: Frame,
        activeChunks: Set<ChunkPos>,
        includeZeroForInactive: Boolean = true,
        accumulateIntoLast: Boolean = false
    ) {
        frame.close()
        val frameTotals = frame.snapshotNanosByChunk()
        val frameBreakdowns = frame.snapshotNanosByChunkAndCategory()
        val nowNanos = System.nanoTime()

        for ((chunkPos, totalNanos) in frameTotals) {
            updateChunkStat(
                chunkPos = chunkPos,
                lastMs = totalNanos / 1_000_000.0,
                updatedAtNanos = nowNanos,
                accumulateIntoLast = accumulateIntoLast,
                tickSequence = frame.tickSequence,
                breakdownMs = toMsBreakdown(frameBreakdowns[chunkPos].orEmpty())
            )
            if (totalNanos > 0L) {
                markChunkDirty(chunkPos)
            }
        }

        if (includeZeroForInactive) {
            for (chunkPos in activeChunks) {
                if (frameTotals.containsKey(chunkPos)) continue
                updateChunkStat(
                    chunkPos = chunkPos,
                    lastMs = 0.0,
                    updatedAtNanos = nowNanos,
                    accumulateIntoLast = accumulateIntoLast,
                    tickSequence = frame.tickSequence,
                    breakdownMs = emptyMap()
                )
            }
        }
    }

    fun mspt(chunkX: Int, chunkZ: Int): Double {
        val stat = statsByChunk[ChunkPos(chunkX, chunkZ)]
        return stat?.measuredMs ?: 0.0
    }

    fun tps(chunkX: Int, chunkZ: Int): Double {
        val stat = statsByChunk[ChunkPos(chunkX, chunkZ)] ?: return configuredTargetTps()
        return stat.measuredTps
    }

    fun ewmaMspt(chunkX: Int, chunkZ: Int): Double {
        val stat = statsByChunk[ChunkPos(chunkX, chunkZ)]
        return stat?.measuredMs ?: 0.0
    }

    fun ewmaTps(chunkX: Int, chunkZ: Int): Double {
        val stat = statsByChunk[ChunkPos(chunkX, chunkZ)] ?: return configuredTargetTps()
        return stat.measuredTps
    }

    fun isChunkIdle(chunkX: Int, chunkZ: Int): Boolean {
        val stat = statsByChunk[ChunkPos(chunkX, chunkZ)] ?: return true
        val ageMs = ((System.nanoTime() - stat.meaningfulUpdatedAtNanos).coerceAtLeast(0L)) / 1_000_000.0
        val dynamicThresholdMs = maxOf(stat.lastIntervalMs * IDLE_INTERVAL_MULTIPLIER, IDLE_MIN_AGE_MS)
        val idleThresholdMs = dynamicThresholdMs.coerceAtMost(IDLE_MAX_AGE_MS)
        return ageMs > idleThresholdMs
    }

    fun topChunksByMspt(limit: Int, minMspt: Double = 0.0): List<ChunkStatSnapshot> {
        if (limit <= 0) return emptyList()
        val snapshots = ArrayList<ChunkStatSnapshot>(statsByChunk.size)
        for ((chunkPos, stat) in statsByChunk) {
            val mspt = stat.measuredMs
            if (!mspt.isFinite() || mspt < minMspt) continue
            val tps = stat.measuredTps
            snapshots += ChunkStatSnapshot(
                chunkPos = chunkPos,
                mspt = mspt,
                tps = if (tps.isFinite()) tps else 0.0,
                breakdownMs = stat.lastBreakdownMs
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

    fun topChunksByEwmaMspt(limit: Int, minMspt: Double = 0.0): List<ChunkStatSnapshot> {
        if (limit <= 0) return emptyList()
        val snapshots = ArrayList<ChunkStatSnapshot>(statsByChunk.size)
        for ((chunkPos, stat) in statsByChunk) {
            val mspt = stat.measuredMs
            if (!mspt.isFinite() || mspt < minMspt) continue
            val tps = stat.measuredTps
            snapshots += ChunkStatSnapshot(
                chunkPos = chunkPos,
                mspt = mspt,
                tps = if (tps.isFinite()) tps else 0.0,
                breakdownMs = stat.lastBreakdownMs
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

    fun consumeDirtyChunks(): Set<ChunkPos> {
        if (dirtyChunksQueue.isEmpty()) return emptySet()
        val out = HashSet<ChunkPos>()
        while (true) {
            val chunkPos = dirtyChunksQueue.poll() ?: break
            dirtyChunksDedup.remove(chunkPos)
            out.add(chunkPos)
        }
        return out
    }

    private fun updateChunkStat(
        chunkPos: ChunkPos,
        lastMs: Double,
        updatedAtNanos: Long,
        accumulateIntoLast: Boolean,
        tickSequence: Long,
        breakdownMs: Map<String, Double>
    ) {
        statsByChunk.compute(chunkPos) { _, previous ->
            if (previous != null &&
                tickSequence != NO_TICK_SEQUENCE &&
                previous.lastTickSequence != NO_TICK_SEQUENCE &&
                tickSequence < previous.lastTickSequence &&
                !accumulateIntoLast
            ) {
                // Ignore stale non-accumulating samples.
                return@compute previous
            }
            val defaultIntervalMs = 1000.0 / configuredTargetTps()
            val observedIntervalMs = if (previous == null) {
                defaultIntervalMs
            } else {
                ((updatedAtNanos - previous.updatedAtNanos).coerceAtLeast(0L)) / 1_000_000.0
            }
            val staleAccumulatingTick = accumulateIntoLast &&
                previous != null &&
                tickSequence != NO_TICK_SEQUENCE &&
                previous.lastTickSequence != NO_TICK_SEQUENCE &&
                tickSequence < previous.lastTickSequence
            val sameLogicalTick = !staleAccumulatingTick && accumulateIntoLast &&
                previous != null &&
                tickSequence != NO_TICK_SEQUENCE &&
                previous.lastTickSequence == tickSequence
            val aggregateIntoPreviousSample = staleAccumulatingTick || sameLogicalTick
            val effectiveLastMs = if (aggregateIntoPreviousSample) {
                previous.lastMs + lastMs
            } else {
                lastMs
            }
            val effectiveIntervalMs = if (previous == null || staleAccumulatingTick) {
                observedIntervalMs
            } else if (sameLogicalTick) {
                previous.lastIntervalMs
            } else {
                observedIntervalMs.coerceAtLeast(0.001)
            }
            val effectiveLastTps = chunkTpsFromIntervalMs(effectiveIntervalMs)
            val meaningfulUpdatedAtNanos = if (effectiveLastMs >= IDLE_NOISE_MSPT) {
                updatedAtNanos
            } else {
                previous?.meaningfulUpdatedAtNanos ?: updatedAtNanos
            }

            var windowStartedAtNanos = previous?.windowStartedAtNanos ?: updatedAtNanos
            var windowAccumulatedWorkMs = previous?.windowAccumulatedWorkMs ?: 0.0
            var windowAccumulatedIntervalMs = previous?.windowAccumulatedIntervalMs ?: 0.0
            var windowSampleCount = previous?.windowSampleCount ?: 0
            var windowAccumulatedBreakdownMs = previous?.windowAccumulatedBreakdownMs ?: emptyMap()
            var measuredMs = previous?.measuredMs ?: 0.0
            var measuredTps = previous?.measuredTps ?: configuredTargetTps()
            var measuredBreakdownMs = previous?.lastBreakdownMs ?: emptyMap()

            val windowAgeNanos = (updatedAtNanos - windowStartedAtNanos).coerceAtLeast(0L)
            if (windowSampleCount > 0 && windowAgeNanos >= MEASUREMENT_WINDOW_NANOS) {
                measuredMs = windowAccumulatedWorkMs / windowSampleCount.toDouble()
                val avgIntervalMs = (windowAccumulatedIntervalMs / windowSampleCount.toDouble()).coerceAtLeast(0.001)
                measuredTps = chunkTpsFromIntervalMs(avgIntervalMs)
                measuredBreakdownMs = scaleBreakdown(windowAccumulatedBreakdownMs, 1.0 / windowSampleCount.toDouble())

                windowStartedAtNanos = updatedAtNanos
                windowAccumulatedWorkMs = 0.0
                windowAccumulatedIntervalMs = 0.0
                windowSampleCount = 0
                windowAccumulatedBreakdownMs = emptyMap()
            }

            windowAccumulatedWorkMs += lastMs
            windowAccumulatedBreakdownMs = mergeBreakdown(windowAccumulatedBreakdownMs, breakdownMs)
            if (!aggregateIntoPreviousSample) {
                windowAccumulatedIntervalMs += effectiveIntervalMs
                windowSampleCount += 1
            }
            if (windowSampleCount > 0) {
                measuredMs = windowAccumulatedWorkMs / windowSampleCount.toDouble()
                val avgIntervalMs = (windowAccumulatedIntervalMs / windowSampleCount.toDouble()).coerceAtLeast(0.001)
                measuredTps = chunkTpsFromIntervalMs(avgIntervalMs)
                measuredBreakdownMs = scaleBreakdown(windowAccumulatedBreakdownMs, 1.0 / windowSampleCount.toDouble())
            } else {
                measuredMs = 0.0
                measuredTps = 0.0
                measuredBreakdownMs = emptyMap()
            }

            ChunkStat(
                lastMs = effectiveLastMs,
                lastIntervalMs = effectiveIntervalMs,
                lastTps = effectiveLastTps,
                measuredMs = measuredMs,
                measuredTps = measuredTps,
                updatedAtNanos = updatedAtNanos,
                meaningfulUpdatedAtNanos = meaningfulUpdatedAtNanos,
                lastTickSequence = if (tickSequence != NO_TICK_SEQUENCE) tickSequence else previous?.lastTickSequence ?: NO_TICK_SEQUENCE,
                lastBreakdownMs = measuredBreakdownMs,
                windowStartedAtNanos = windowStartedAtNanos,
                windowAccumulatedWorkMs = windowAccumulatedWorkMs,
                windowAccumulatedIntervalMs = windowAccumulatedIntervalMs,
                windowSampleCount = windowSampleCount,
                windowAccumulatedBreakdownMs = windowAccumulatedBreakdownMs
            )
        }
    }

    private fun recordImmediate(
        chunkPos: ChunkPos,
        elapsedNanos: Long,
        accumulateIntoLast: Boolean,
        tickSequence: Long,
        breakdownMs: Map<String, Double> = emptyMap()
    ) {
        updateChunkStat(
            chunkPos = chunkPos,
            lastMs = elapsedNanos.coerceAtLeast(0L) / 1_000_000.0,
            updatedAtNanos = System.nanoTime(),
            accumulateIntoLast = accumulateIntoLast,
            tickSequence = tickSequence,
            breakdownMs = breakdownMs
        )
        if (elapsedNanos > 0L) {
            markChunkDirty(chunkPos)
        }
    }

    private fun markChunkDirty(chunkPos: ChunkPos) {
        if (!dirtyChunksDedup.add(chunkPos)) return
        dirtyChunksQueue.add(chunkPos)
    }

    private fun toMsBreakdown(nanosByCategory: Map<String, Long>): Map<String, Double> {
        if (nanosByCategory.isEmpty()) return emptyMap()
        val out = HashMap<String, Double>(nanosByCategory.size)
        for ((category, nanos) in nanosByCategory) {
            out[category] = nanos.coerceAtLeast(0L) / 1_000_000.0
        }
        return out
    }

    private fun mergeBreakdown(previous: Map<String, Double>, added: Map<String, Double>): Map<String, Double> {
        if (previous.isEmpty()) return added
        if (added.isEmpty()) return previous
        val out = HashMap<String, Double>(previous.size + added.size)
        for ((key, value) in previous) {
            out[key] = value
        }
        for ((key, value) in added) {
            out[key] = (out[key] ?: 0.0) + value
        }
        return out
    }

    private fun scaleBreakdown(values: Map<String, Double>, scale: Double): Map<String, Double> {
        if (values.isEmpty() || !scale.isFinite() || scale <= 0.0) return emptyMap()
        val out = HashMap<String, Double>(values.size)
        for ((key, value) in values) {
            val scaled = value * scale
            if (scaled.isFinite() && scaled > 0.0) {
                out[key] = scaled
            }
        }
        return out
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
        const val CATEGORY_OTHER = "other"
        private const val NO_TICK_SEQUENCE = Long.MIN_VALUE
        private const val MEASUREMENT_WINDOW_NANOS = 1_000_000_000L
        private const val IDLE_INTERVAL_MULTIPLIER = 1.5
        private const val IDLE_MIN_AGE_MS = 100.0
        private const val IDLE_MAX_AGE_MS = 500.0
        private const val IDLE_NOISE_MSPT = 0.005
    }
}
