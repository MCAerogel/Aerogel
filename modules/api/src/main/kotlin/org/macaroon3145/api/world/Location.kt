package org.macaroon3145.api.world

import org.macaroon3145.api.entity.DroppedItem
import org.macaroon3145.api.entity.Entity
import org.macaroon3145.api.entity.EntityType
import org.macaroon3145.api.entity.Item
import kotlin.math.floor

data class Location(
    val world: World,
    val x: Double,
    val y: Double,
    val z: Double,
    val yaw: Float = 0.0f,
    val pitch: Float = 0.0f
) {
    val blockX: Int
        get() = floor(x).toInt()

    val blockY: Int
        get() = floor(y).toInt()

    val blockZ: Int
        get() = floor(z).toInt()

    val block: Block
        get() = world.blockAt(blockX, blockY, blockZ)

    val chunkX: Int
        get() = blockX shr 4

    val chunkZ: Int
        get() = blockZ shr 4

    val chunk: Chunk
        get() = world.chunkAt(chunkX, chunkZ)

    fun clone(): Location {
        return Location(
            world = world,
            x = x,
            y = y,
            z = z,
            yaw = yaw,
            pitch = pitch
        )
    }

    fun dropItem(item: Item, impulse: Boolean = false): DroppedItem {
        return world.dropItem(this, item, impulse)
    }

    fun spawn(entityType: EntityType): Entity {
        return world.spawn(this, entityType)
    }
}
