package org.macaroon3145.api.event

interface Event

interface CancellableEvent : Event {
    var cancelled: Boolean
    var cancelReason: String?
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
