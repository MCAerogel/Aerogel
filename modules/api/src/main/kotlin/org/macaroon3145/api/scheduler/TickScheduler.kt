package org.macaroon3145.api.scheduler

interface TickTask {
    val isCancelled: Boolean
    val isDone: Boolean
    fun cancel(): Boolean
}

interface TickScheduler {
    val currentTick: Long

    fun runLater(
        delayTicks: Long,
        task: () -> Unit
    ): TickTask

    fun runRepeating(
        initialDelayTicks: Long,
        periodTicks: Long,
        task: () -> Unit
    ): TickTask
}
