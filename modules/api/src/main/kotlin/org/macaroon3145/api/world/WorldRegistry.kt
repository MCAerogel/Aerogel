package org.macaroon3145.api.world

interface WorldRegistry {
    fun defaultWorld(): World
    fun world(key: String): World?
    fun allWorlds(): List<World>
}
