package org.macaroon3145.api.scheduler

import java.util.concurrent.CompletableFuture
import kotlin.time.Duration

interface TaskScheduler {
    fun runSync(task: () -> Unit)

    fun <T> supplyAsync(task: () -> T): CompletableFuture<T>

    fun runAsync(task: () -> Unit): ScheduledTask

    fun runAsyncLater(delay: Duration, task: () -> Unit): ScheduledTask

    fun runAsyncRepeating(
        initialDelay: Duration,
        period: Duration,
        task: () -> Unit
    ): ScheduledTask

    fun shutdownOwner(owner: String)
}

interface ScheduledTask {
    val isCancelled: Boolean
    val isDone: Boolean
    fun cancel(mayInterruptIfRunning: Boolean = true): Boolean
}
