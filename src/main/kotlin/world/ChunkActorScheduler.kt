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
        private val thread: Thread = Thread.ofVirtual()
            .name("aerogel-chunk-actor-${chunkPos.x}-${chunkPos.z}")
            .start {
                loop()
            }

        private fun loop() {
            while (running.get()) {
                var progressed = false
                while (true) {
                    val task = queue.poll() ?: break
                    progressed = true
                    try {
                        task()
                    } catch (_: Throwable) {
                        // Keep actor alive even if one task fails.
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
            val future = CompletableFuture<T>()
            queue.add {
                try {
                    future.complete(task())
                } catch (t: Throwable) {
                    future.completeExceptionally(t)
                }
            }
            LockSupport.unpark(thread)
            return future
        }

        fun stop() {
            if (!running.compareAndSet(true, false)) return
            LockSupport.unpark(thread)
            queue.clear()
        }
    }

    private val actors = ConcurrentHashMap<ChunkPos, ChunkActor>()

    fun <T> submit(chunkPos: ChunkPos, task: () -> T): CompletableFuture<T> {
        val actor = actors.computeIfAbsent(chunkPos) { ChunkActor(it) }
        return actor.submit(task)
    }

    fun stopAll() {
        for ((_, actor) in actors) {
            actor.stop()
        }
        actors.clear()
    }

    private companion object {
        private const val PARK_NANOS = 1_000_000L
    }
}
