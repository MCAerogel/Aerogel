package org.macaroon3145.api.entity

import org.macaroon3145.api.type.ItemType
import org.macaroon3145.api.world.Location
import org.macaroon3145.api.world.Chunk
import org.macaroon3145.api.world.World
import java.util.UUID
import kotlin.math.cos
import kotlin.math.sin

abstract class Entity(
    open val uniqueId: UUID
) {
    abstract val type: EntityType

    abstract var location: Location
    abstract var onGround: Boolean
    abstract var speedX: Double
    abstract var speedY: Double
    abstract var speedZ: Double
    abstract var accelerationX: Double
    abstract var accelerationY: Double
    abstract var accelerationZ: Double

    val speed: Double
        get() = kotlin.math.sqrt(speedX * speedX + speedY * speedY + speedZ * speedZ)

    val acceleration: Double
        get() = kotlin.math.sqrt(
            accelerationX * accelerationX +
                accelerationY * accelerationY +
                accelerationZ * accelerationZ
        )

    open fun setSpeed(x: Double, y: Double, z: Double) {
        speedX = x
        speedY = y
        speedZ = z
    }

    open fun setAcceleration(x: Double, y: Double, z: Double) {
        accelerationX = x
        accelerationY = y
        accelerationZ = z
    }

    open fun accelerate(x: Double, y: Double, z: Double): Boolean {
        setSpeed(speedX + x, speedY + y, speedZ + z)
        return true
    }

    fun accelerateForward(power: Double): Boolean {
        if (power == 0.0) return false
        val yawRad = Math.toRadians(location.yaw.toDouble())
        val pitchRad = Math.toRadians(location.pitch.toDouble())
        val cosPitch = cos(pitchRad)
        val x = -sin(yawRad) * cosPitch * power
        val y = -sin(pitchRad) * power
        val z = cos(yawRad) * cosPitch * power
        return accelerate(x, y, z)
    }
}

abstract class LivingEntity(
    override val uniqueId: UUID
) : Entity(uniqueId) {
    abstract var health: Float?
}

abstract class DroppedItem(
    final override val uniqueId: UUID
) : Entity(uniqueId) {
    final override val type: EntityType
        get() = EntityType.DROPPED_ITEM

    abstract val entityId: Int
    abstract var x: Double
    abstract var y: Double
    abstract var z: Double
    abstract var item: Item
    abstract var pickupDelaySeconds: Double
    abstract override var onGround: Boolean

    fun setPosition(x: Double, y: Double, z: Double): Boolean {
        this.x = x
        this.y = y
        this.z = z
        return true
    }
}

abstract class Thrown(
    final override val uniqueId: UUID
) : Entity(uniqueId) {
    abstract val entityId: Int
    abstract var ownerEntityId: Int
    abstract var x: Double
    abstract var y: Double
    abstract var z: Double
    abstract val prevX: Double
    abstract val prevY: Double
    abstract val prevZ: Double

    fun setPosition(x: Double, y: Double, z: Double): Boolean {
        this.x = x
        this.y = y
        this.z = z
        return true
    }
}

abstract class Snowball(
    uniqueId: UUID
) : Thrown(uniqueId) {
    final override val type: EntityType
        get() = EntityType.SNOWBALL
}

abstract class Egg(
    uniqueId: UUID
) : Thrown(uniqueId) {
    final override val type: EntityType
        get() = EntityType.EGG
}

abstract class EnderPearl(
    uniqueId: UUID
) : Thrown(uniqueId) {
    final override val type: EntityType
        get() = EntityType.ENDER_PEARL
}

abstract class Player(
    final override val uniqueId: UUID
) : LivingEntity(uniqueId) {
    final override val type: EntityType
        get() = EntityType.PLAYER

    abstract val name: String?
    abstract val online: Boolean
    abstract val op: Boolean
    abstract val gameMode: GameMode
    abstract val sprinting: Boolean
    abstract val swimming: Boolean
    abstract val flying: Boolean
    abstract var displayName: String?
}

abstract class ConnectedPlayer(
    uniqueId: UUID
) : Player(uniqueId) {
    abstract override val name: String
    abstract val world: World
    abstract override var location: Location
    abstract val inventory: PlayerInventorySnapshot
    abstract val locale: String
    abstract val eyeLocation: Location
    abstract val viewChunk: Chunk
    abstract val viewChunkRadius: Int
    abstract val loadedViewChunks: List<Chunk>
    abstract val food: Int
    abstract val saturation: Float
    abstract fun setHotbarItem(slot: Int, item: Item?): Boolean
    abstract fun setMainItem(slot: Int, item: Item?): Boolean
    abstract fun setArmorItem(slot: ArmorSlot, item: Item?): Boolean
    abstract fun setOffhandItem(item: Item?): Boolean

    fun setHotbar(slot: Int, itemType: ItemType, amount: Int = 1): Boolean {
        return setHotbarItem(slot, Item(type = itemType, amount = amount))
    }

    fun clearHotbar(slot: Int): Boolean {
        return setHotbarItem(slot, null)
    }

    fun setMain(slot: Int, itemType: ItemType, amount: Int = 1): Boolean {
        return setMainItem(slot, Item(type = itemType, amount = amount))
    }

    fun clearMain(slot: Int): Boolean {
        return setMainItem(slot, null)
    }

    fun setArmor(slot: ArmorSlot, itemType: ItemType, amount: Int = 1): Boolean {
        return setArmorItem(slot, Item(type = itemType, amount = amount))
    }

    fun clearArmor(slot: ArmorSlot): Boolean {
        return setArmorItem(slot, null)
    }

    fun setOffhand(itemType: ItemType, amount: Int = 1): Boolean {
        return setOffhandItem(Item(type = itemType, amount = amount))
    }

    fun clearOffhand(): Boolean {
        return setOffhandItem(null)
    }

    abstract fun tr(key: String, vararg args: String): String
    abstract fun sendMessage(message: String)
}
