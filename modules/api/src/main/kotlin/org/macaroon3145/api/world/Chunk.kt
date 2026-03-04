package org.macaroon3145.api.world

import org.macaroon3145.api.entity.Entity
import org.macaroon3145.api.entity.EntityType
import org.macaroon3145.api.scheduler.TickScheduler
import java.util.UUID

abstract class Chunk {
    abstract val world: World
    abstract val x: Int
    abstract val z: Int
    abstract val state: ChunkState
    val loaded: Boolean
        get() = state == ChunkState.LOADED
    abstract val simulationChunk: Boolean
    abstract val tps: Double
    abstract val mspt: Double
    abstract val virtualThread: Thread?
    abstract val tickScheduler: TickScheduler
    abstract fun load(): Boolean
    abstract fun unload(): Boolean
    protected abstract fun pluginInternalEntitiesByType(type: EntityType): List<Entity>
    protected abstract fun pluginInternalEntityById(entityId: UUID): Entity?

    fun getEntityByType(type: EntityType): List<Entity> = pluginInternalEntitiesByType(type)
    fun getEntityById(entityId: UUID): Entity? = pluginInternalEntityById(entityId)
}
