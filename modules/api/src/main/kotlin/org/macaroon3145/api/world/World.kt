package org.macaroon3145.api.world

import org.macaroon3145.api.entity.DroppedItem
import org.macaroon3145.api.entity.Entity
import org.macaroon3145.api.entity.EntityType
import org.macaroon3145.api.entity.Item
import java.util.UUID

abstract class World {
    abstract val key: String
    abstract val elapsedTicks: Long
    abstract var timeOfDayTicks: Long
    abstract fun blockAt(x: Int, y: Int, z: Int): Block
    abstract fun chunkAt(chunkX: Int, chunkZ: Int): Chunk
    abstract fun addTime(deltaTicks: Long): Boolean
    protected abstract fun pluginInternalDropItem(location: Location, item: Item, impulse: Boolean = false): DroppedItem?
    protected abstract fun pluginInternalSpawnEntity(location: Location, entityType: EntityType): Entity?
    protected abstract fun pluginInternalEntitiesByType(type: EntityType): List<Entity>
    protected abstract fun pluginInternalEntityById(entityId: UUID): Entity?

    fun blockAt(location: Location): Block = blockAt(location.blockX, location.blockY, location.blockZ)
    fun chunkAt(location: Location): Chunk = chunkAt(location.chunkX, location.chunkZ)
    fun dropItem(location: Location, item: Item, impulse: Boolean = false): DroppedItem {
        return requireNotNull(pluginInternalDropItem(location, item, impulse)) {
            "Failed to drop item at ${location.world.key} (${location.x}, ${location.y}, ${location.z}): id=${item.id}, amount=${item.amount}"
        }
    }
    fun spawn(location: Location, entityType: EntityType): Entity {
        require(entityType != EntityType.PLAYER) {
            "PLAYER entities cannot be spawned via the plugin API."
        }
        return requireNotNull(pluginInternalSpawnEntity(location, entityType)) {
            "Failed to spawn $entityType at ${location.world.key} (${location.x}, ${location.y}, ${location.z})"
        }
    }
    fun getEntityByType(type: EntityType): List<Entity> = pluginInternalEntitiesByType(type)
    fun getEntityById(entityId: UUID): Entity? = pluginInternalEntityById(entityId)

    val elapsedYears: Long
        get() = elapsedSecondsTotal / SECONDS_PER_YEAR

    val elapsedMonths: Long
        get() = (elapsedSecondsTotal % SECONDS_PER_YEAR) / SECONDS_PER_MONTH

    val elapsedDays: Long
        get() = (elapsedSecondsTotal % SECONDS_PER_MONTH) / SECONDS_PER_DAY

    val elapsedHours: Long
        get() = (elapsedSecondsTotal % SECONDS_PER_DAY) / SECONDS_PER_HOUR

    val elapsedMinutes: Long
        get() = (elapsedSecondsTotal % SECONDS_PER_HOUR) / SECONDS_PER_MINUTE

    val elapsedSeconds: Long
        get() = elapsedSecondsTotal % SECONDS_PER_MINUTE

    val elapsedTickRemainder: Long
        get() = elapsedTicks % TICKS_PER_SECOND

    private val elapsedSecondsTotal: Long
        get() = elapsedTicks / TICKS_PER_SECOND

    companion object {
        private const val TICKS_PER_SECOND = 20L
        private const val SECONDS_PER_MINUTE = 60L
        private const val SECONDS_PER_HOUR = SECONDS_PER_MINUTE * 60L
        private const val SECONDS_PER_DAY = SECONDS_PER_HOUR * 24L
        private const val DAYS_PER_MONTH = 30L
        private const val SECONDS_PER_MONTH = SECONDS_PER_DAY * DAYS_PER_MONTH
        private const val SECONDS_PER_YEAR = SECONDS_PER_DAY * 365L
    }
}
