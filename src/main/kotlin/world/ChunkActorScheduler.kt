package org.macaroon3145.world

import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.LockSupport

/**
 * Per-chunk actor scheduler.
 * Each chunk owns exactly one virtual thread that executes submitted tasks sequentially.
 */
class ChunkActorScheduler {
    private class ChunkActor(
        val chunkPos: ChunkPos
    ) {
        private val queue = ConcurrentLinkedQueue<() -> Unit>()
        private val running = AtomicBoolean(true)
        private val workerRunning = AtomicBoolean(false)
        @Volatile
        private var thread: Thread? = null

        init {
            ensureWorkerRunning()
        }

        private fun ensureWorkerRunning() {
            if (!running.get()) return
            if (!workerRunning.compareAndSet(false, true)) return
            thread = Thread.ofVirtual()
                .name("aerogel-chunk-actor-${chunkPos.x}-${chunkPos.z}")
                .start {
                    runWorker()
                }
        }

        private fun runWorker() {
            try {
                loop()
            } catch (_: Throwable) {
                // Keep actor alive unless explicitly stopped.
            } finally {
                if (thread === Thread.currentThread()) thread = null
                workerRunning.set(false)
                if (running.get()) {
                    ensureWorkerRunning()
                }
            }
        }

        private fun loop() {
            while (running.get()) {
                var progressed = false
                while (true) {
                    val task = queue.poll() ?: break
                    progressed = true
                    try {
                        CURRENT_CHUNK_POS.set(chunkPos)
                        task()
                    } catch (_: Throwable) {
                        // Keep actor alive even if one task fails.
                    } finally {
                        CURRENT_CHUNK_POS.remove()
                    }
                }
                if (!running.get()) {
                    break
                }
                if (!progressed) {
                    LockSupport.parkNanos(PARK_NANOS)
                }
            }
        }

        fun <T> submit(task: () -> T): CompletableFuture<T> {
            ensureWorkerRunning()
            val future = CompletableFuture<T>()
            queue.add {
                try {
                    future.complete(task())
                } catch (t: Throwable) {
                    future.completeExceptionally(t)
                }
            }
            thread?.let { LockSupport.unpark(it) }
            return future
        }

        fun stop() {
            if (!running.compareAndSet(true, false)) return
            val current = thread
            if (current != null) {
                LockSupport.unpark(current)
            }
            queue.clear()
        }
    }

    private val actors = ConcurrentHashMap<ChunkPos, ChunkActor>()

    fun <T> submit(chunkPos: ChunkPos, task: () -> T): CompletableFuture<T> {
        val actor = actors.computeIfAbsent(chunkPos) { ChunkActor(it) }
        return actor.submit(task)
    }

    fun currentChunkPos(): ChunkPos? = CURRENT_CHUNK_POS.get()

    fun stopAll() {
        for ((_, actor) in actors) {
            actor.stop()
        }
        actors.clear()
    }

    private companion object {
        private const val PARK_NANOS = 1_000_000L
        private val CURRENT_CHUNK_POS = ThreadLocal<ChunkPos?>()
    }
}
