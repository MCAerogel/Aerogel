package org.macaroon3145.api.service

interface ServiceHandle<T : Any> {
    val owner: String
    val generation: Long
    fun isValid(): Boolean
    fun getOrNull(): T?
}

interface ServiceRegistry {
    fun <T : Any> publish(owner: String, key: Class<T>, service: T): ServiceHandle<T>
    fun <T : Any> resolve(key: Class<T>): ServiceHandle<T>?
    fun invalidateOwner(owner: String)
}
