package org.macaroon3145.api.event

import org.macaroon3145.api.plugin.PluginRuntime
import java.util.concurrent.ConcurrentHashMap

interface Event

interface CancellableEvent : Event {
    var cancelled: Boolean
    var cancelReason: String?
}

fun CancellableEvent.cancel(reason: String? = null) {
    cancelled = true
    cancelReason = reason
    EventDispatchHints.requestSkipRemainingHandlers(this)
}

object EventDispatchHints {
    private val stopFurtherHandlers = ConcurrentHashMap<Event, Boolean>()

    fun requestSkipRemainingHandlers(event: Event) {
        stopFurtherHandlers[event] = true
    }

    fun shouldSkipRemainingHandlers(event: Event): Boolean {
        return stopFurtherHandlers[event] == true
    }

    fun clear(event: Event) {
        stopFurtherHandlers.remove(event)
    }
}

enum class EventPriority {
    LOWEST,
    LOW,
    NORMAL,
    HIGH,
    HIGHEST,
    MONITOR
}

@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class Subscribe(
    val priority: EventPriority = EventPriority.NORMAL,
    val receiveCancelled: Boolean = false
)

@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
// Intent marker:
// When used on annotated listeners, runtime may dispatch the handler
// as fire-and-forget and not wait for completion.
annotation class NoCancel

fun interface EventFilter<E : Event> {
    fun test(event: E): Boolean
}

interface EventSubscription {
    val active: Boolean
    fun unregister()
}

interface EventBus {
    fun <E : Event> listen(
        owner: String,
        eventType: Class<E>,
        priority: EventPriority = EventPriority.NORMAL,
        receiveCancelled: Boolean = false,
        filter: EventFilter<E>? = null,
        handler: (E) -> Unit
    ): EventSubscription

    fun registerAnnotated(owner: String, listener: Any): List<EventSubscription>

    fun unregisterOwner(owner: String)

    fun <E : Event> post(event: E): E

    fun hasListeners(eventType: Class<out Event>): Boolean
}

inline fun <reified E : Event> EventBus.listen(
    owner: String,
    priority: EventPriority = EventPriority.NORMAL,
    receiveCancelled: Boolean = false,
    filter: EventFilter<E>? = null,
    noinline handler: (E) -> Unit
): EventSubscription {
    return listen(
        owner = owner,
        eventType = E::class.java,
        priority = priority,
        receiveCancelled = receiveCancelled,
        filter = filter,
        handler = handler
    )
}

inline fun <reified E : Event> EventBus.listenNoCancel(
    owner: String,
    priority: EventPriority = EventPriority.NORMAL,
    receiveCancelled: Boolean = false,
    filter: EventFilter<E>? = null,
    noinline handler: (E) -> Unit
): EventSubscription {
    // Intent marker only: delegates to standard listen.
    return listen(
        owner = owner,
        eventType = E::class.java,
        priority = priority,
        receiveCancelled = receiveCancelled,
        filter = filter,
        handler = handler
    )
}

inline fun <reified E : Event> EventBus.hasListeners(): Boolean {
    return hasListeners(E::class.java)
}

inline fun <reified E : Event> listen(
    priority: EventPriority = EventPriority.NORMAL,
    receiveCancelled: Boolean = false,
    filter: EventFilter<E>? = null,
    noinline handler: E.() -> Unit
): EventSubscription {
    val context = PluginRuntime.requireCurrentContext()
    return context.events.listen(
        owner = context.metadata.id,
        eventType = E::class.java,
        priority = priority,
        receiveCancelled = receiveCancelled,
        filter = filter,
        handler = { event -> handler(event) }
    )
}

inline fun <reified E : Event> listenNoCancel(
    priority: EventPriority = EventPriority.NORMAL,
    receiveCancelled: Boolean = false,
    filter: EventFilter<E>? = null,
    noinline handler: E.() -> Unit
): EventSubscription {
    return listen(
        priority = priority,
        receiveCancelled = receiveCancelled,
        filter = filter,
        handler = handler
    )
}
