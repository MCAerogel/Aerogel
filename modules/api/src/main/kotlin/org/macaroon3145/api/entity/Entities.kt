package org.macaroon3145.api.entity

import org.macaroon3145.api.command.CommandSender
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

    final override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is Entity) return false
        return uniqueId == other.uniqueId
    }

    final override fun hashCode(): Int {
        return uniqueId.hashCode()
    }

    override fun toString(): String {
        return "${javaClass.simpleName}(type=$type, uniqueId=$uniqueId)"
    }
}

abstract class LivingEntity(
    override val uniqueId: UUID
) : Entity(uniqueId) {
    abstract var health: Float?

    open fun jump(heightBlocks: Double = VANILLA_DEFAULT_JUMP_HEIGHT_BLOCKS): Boolean {
        val targetHeightBlocks = heightBlocks.coerceAtLeast(0.0)
        if (targetHeightBlocks <= 0.0) return false
        val launchVelocityPerTick = launchVelocityForJumpHeight(targetHeightBlocks)
        setSpeed(speedX, launchVelocityPerTick * MINECRAFT_TICKS_PER_SECOND, speedZ)
        return true
    }

    private fun launchVelocityForJumpHeight(heightBlocks: Double): Double {
        var low = 0.0
        var high = VANILLA_DEFAULT_JUMP_VELOCITY_PER_TICK
        while (jumpApexHeightForLaunchVelocity(high) < heightBlocks && high < MAX_JUMP_LAUNCH_VELOCITY_PER_TICK) {
            high = (high * 2.0).coerceAtMost(MAX_JUMP_LAUNCH_VELOCITY_PER_TICK)
            if (high == MAX_JUMP_LAUNCH_VELOCITY_PER_TICK) break
        }
        repeat(JUMP_HEIGHT_BINARY_SEARCH_STEPS) {
            val mid = (low + high) * 0.5
            if (jumpApexHeightForLaunchVelocity(mid) >= heightBlocks) {
                high = mid
            } else {
                low = mid
            }
        }
        return high
    }

    private fun jumpApexHeightForLaunchVelocity(initialVelocityPerTick: Double): Double {
        if (initialVelocityPerTick <= 0.0) return 0.0
        var y = 0.0
        var vy = initialVelocityPerTick
        var apex = 0.0
        repeat(MAX_JUMP_SIMULATION_TICKS) {
            y += vy
            if (y > apex) apex = y
            val nextVy = (vy - VANILLA_GRAVITY_PER_TICK) * VANILLA_AIR_DRAG_PER_TICK
            if (nextVy <= 0.0) return apex
            vy = nextVy
        }
        return apex
    }

    private companion object {
        private const val MINECRAFT_TICKS_PER_SECOND = 20.0
        private const val VANILLA_GRAVITY_PER_TICK = 0.08
        private const val VANILLA_AIR_DRAG_PER_TICK = 0.98
        private const val VANILLA_DEFAULT_JUMP_VELOCITY_PER_TICK = 0.42
        private const val VANILLA_DEFAULT_JUMP_HEIGHT_BLOCKS = 1.252203352512
        private const val MAX_JUMP_LAUNCH_VELOCITY_PER_TICK = 32.0
        private const val MAX_JUMP_SIMULATION_TICKS = 1024
        private const val JUMP_HEIGHT_BINARY_SEARCH_STEPS = 32
    }
}

abstract class DroppedItem(
    final override val uniqueId: UUID
) : Entity(uniqueId) {
    final override val type: EntityType
        get() = EntityType.DROPPED_ITEM

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
    abstract var ownerUniqueId: UUID?
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
    override val type: EntityType
        get() = EntityType.PLAYER

    abstract val online: Boolean
    abstract val op: Boolean

    open fun hasPermission(node: String): Boolean = op
}

abstract class Bot(
    uniqueId: UUID
) : LivingEntity(uniqueId) {
    final override val type: EntityType
        get() = EntityType.BOT

    abstract var name: String
    abstract var tabList: Boolean
    abstract var pushable: Boolean
    abstract var attackable: Boolean
    abstract var gravity: Boolean
    abstract var collision: Boolean
    abstract var invulnerable: Boolean
    abstract var fallDamage: Boolean
    abstract var damageDelaySeconds: Double
    abstract var damageDelayRemainingSeconds: Double
    abstract var maxHealth: Float
    abstract override var health: Float?
}

abstract class ConnectedPlayer(
    uniqueId: UUID
) : Player(uniqueId), CommandSender {
    data class PlayerSettings(
        val locale: String,
        val viewDistance: Int
    )

    abstract override val name: String
    abstract val world: World
    abstract override var location: Location
    abstract val settings: PlayerSettings
    abstract val gameMode: GameMode
    abstract val sneaking: Boolean
    abstract val sprinting: Boolean
    abstract val swimming: Boolean
    abstract val flying: Boolean
    abstract val pingMs: Double
    abstract val fallDistance: Double
    abstract val dead: Boolean
    abstract val ridingEntityId: UUID?
    abstract var displayName: String?
    abstract val inventory: PlayerInventory
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
    abstract fun respawn(): Boolean

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
    abstract override fun sendMessage(message: String)

    override fun hasPermission(node: String): Boolean = op
}
