package org.macaroon3145.world

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.LongAdder

class ChunkProcessingProfiler {
    class Frame {
        private val totalsByChunk = ConcurrentHashMap<ChunkPos, LongAdder>()

        fun record(chunkPos: ChunkPos, elapsedNanos: Long) {
            totalsByChunk
                .computeIfAbsent(chunkPos) { LongAdder() }
                .add(elapsedNanos.coerceAtLeast(0L))
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
        val updatedAtNanos: Long
    )

    private val statsByChunk = ConcurrentHashMap<ChunkPos, ChunkStat>()

    fun beginFrame(): Frame = Frame()

    fun finishFrame(frame: Frame, activeChunks: Set<ChunkPos>) {
        val frameTotals = frame.snapshotNanosByChunk()
        val nowNanos = System.nanoTime()

        for ((chunkPos, totalNanos) in frameTotals) {
            updateChunkStat(chunkPos, totalNanos / 1_000_000.0, nowNanos)
        }

        for (chunkPos in activeChunks) {
            if (frameTotals.containsKey(chunkPos)) continue
            updateChunkStat(chunkPos, 0.0, nowNanos)
        }
    }

    fun mspt(chunkX: Int, chunkZ: Int): Double {
        return statsByChunk[ChunkPos(chunkX, chunkZ)]?.lastMs ?: 0.0
    }

    private fun updateChunkStat(chunkPos: ChunkPos, lastMs: Double, updatedAtNanos: Long) {
        statsByChunk.compute(chunkPos) { _, previous ->
            if (previous == null) {
                ChunkStat(
                    lastMs = lastMs,
                    ewmaMs = lastMs,
                    updatedAtNanos = updatedAtNanos
                )
            } else {
                ChunkStat(
                    lastMs = lastMs,
                    ewmaMs = previous.ewmaMs * EWMA_KEEP + lastMs * EWMA_NEW,
                    updatedAtNanos = updatedAtNanos
                )
            }
        }
    }

    private companion object {
        private const val EWMA_KEEP = 0.8
        private const val EWMA_NEW = 0.2
    }
}
