package org.macaroon3145

import java.util.concurrent.atomic.AtomicLong

object StartupTiming {
    private val interactiveInputNanos = AtomicLong(0L)

    fun addInteractiveInputNanos(nanos: Long) {
        if (nanos <= 0L) return
        interactiveInputNanos.addAndGet(nanos)
    }

    fun interactiveInputNanos(): Long = interactiveInputNanos.get()
}
