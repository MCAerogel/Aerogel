package org.macaroon3145.api.event

import org.macaroon3145.api.entity.ConnectedPlayer
import org.macaroon3145.api.entity.Entity
import org.macaroon3145.api.entity.Hand
import org.macaroon3145.api.entity.Item
import org.macaroon3145.api.world.Block
import org.macaroon3145.api.world.BlockState
import org.macaroon3145.api.world.BlockFace
import org.macaroon3145.api.world.Location
import org.macaroon3145.api.world.World
import org.macaroon3145.api.type.BlockType

data class PlayerJoinEvent(
    val player: ConnectedPlayer,
    var message: String
) : Event

data class PlayerChatEvent(
    val player: ConnectedPlayer,
    var message: String,
    override var cancelled: Boolean = false,
    override var cancelReason: String? = null
) : CancellableEvent

data class PlayerQuitEvent(
    val player: ConnectedPlayer,
    var message: String
) : Event

data class PlayerJumpEvent(
    val player: ConnectedPlayer,
    val location: Location,
    val fromGround: Boolean,
    val airJumpAttemptCount: Int
) : Event

enum class MoveReason {
    PLAYER_INPUT,
    TELEPORT,
    COMMAND,
    WORLD_CHANGE,
    ENDER_PEARL,
    CHORUS_FRUIT,
    RESPAWN,
    DISMOUNT,
    API,
    SYSTEM
}

enum class MoveMedium {
    PLAYER_FLYING,
    PLAYER_SWIMMING,
    PLAYER_RIDING,
    CLIMBING,
    WATER,
    LAVA,
    GROUND,
    AIR,
    UNKNOWN
}

data class PlayerMoveEvent(
    val player: ConnectedPlayer,
    val from: Location,
    val to: Location,
    val reason: MoveReason,
    val fromMedium: MoveMedium,
    val toMedium: MoveMedium,
    val isRotationOnly: Boolean,
    val deltaX: Double,
    val deltaY: Double,
    val deltaZ: Double,
    val rawDeltaYaw: Float,
    val deltaYaw: Float,
    val deltaPitch: Float,
    var cancelPosition: Boolean = false,
    var cancelRotation: Boolean = false,
    override var cancelled: Boolean = false,
    override var cancelReason: String? = null
) : CancellableEvent

data class EntityMoveEvent(
    val entity: Entity,
    val from: Location,
    val to: Location,
    val reason: MoveReason,
    val fromMedium: MoveMedium,
    val toMedium: MoveMedium,
    val isRotationOnly: Boolean,
    val deltaX: Double,
    val deltaY: Double,
    val deltaZ: Double,
    val rawDeltaYaw: Float,
    val deltaYaw: Float,
    val deltaPitch: Float,
    var cancelPosition: Boolean = false,
    var cancelRotation: Boolean = false,
    override var cancelled: Boolean = false,
    override var cancelReason: String? = null
) : CancellableEvent

data class BlockBreakEvent(
    val player: ConnectedPlayer,
    val block: Block,
    val useItem: Item?,
    var dropItems: List<Item>,
    override var cancelled: Boolean = false,
    override var cancelReason: String? = null
) : CancellableEvent

data class BlockPlaceEvent(
    val player: ConnectedPlayer,
    val block: Block,
    val clickedBlock: Block,
    val hand: Hand,
    val face: BlockFace,
    override var cancelled: Boolean = false,
    override var cancelReason: String? = null
) : CancellableEvent

enum class BlockChangeReason {
    PLAYER_BREAK,
    PLAYER_BREAK_CHAIN,
    PLAYER_PLACE,
    PLAYER_BUCKET_EMPTY,
    PLAYER_BUCKET_FILL,
    PLUGIN,
    WATER_SPREAD,
    WATER_RETRACT,
    WATER_LEVEL_CHANGE,
    FLUID_SUPPORT_BREAK,
    FALLING_BLOCK_START,
    FURNACE_LIT_UPDATE,
    SYSTEM
}

data class BlockSnapshot(
    val type: BlockType,
    val state: BlockState
)

data class BlockChangeEvent(
    val player: ConnectedPlayer?,
    val world: World,
    val location: Location,
    val block: Block,
    val reason: BlockChangeReason,
    val previous: BlockSnapshot,
    val changed: BlockSnapshot,
    override var cancelled: Boolean = false,
    override var cancelReason: String? = null
) : CancellableEvent

enum class PlayerInteractAction {
    LEFT_CLICK_BLOCK,
    LEFT_CLICK_AIR,
    RIGHT_CLICK_BLOCK,
    RIGHT_CLICK_AIR;

    val isRightClick: Boolean
        get() = this == RIGHT_CLICK_AIR || this == RIGHT_CLICK_BLOCK

    val isLeftClick: Boolean
        get() = this == LEFT_CLICK_AIR || this == LEFT_CLICK_BLOCK

    val isBlock: Boolean
        get() = this == LEFT_CLICK_BLOCK || this == RIGHT_CLICK_BLOCK

    val isAir: Boolean
        get() = this == LEFT_CLICK_AIR || this == RIGHT_CLICK_AIR
}

data class PlayerInteractEvent(
    val player: ConnectedPlayer,
    val hand: Hand,
    val item: Item? = null,
    val isRightClick: Boolean,
    val isLeftClick: Boolean,
    val isBlockInteract: Boolean,
    val isAirInteract: Boolean,
    val clickedBlock: Block? = null,
    val face: BlockFace? = null,
    override var cancelled: Boolean = false,
    override var cancelReason: String? = null
) : CancellableEvent

fun PlayerMoveEvent.cancelPosition(reason: String? = null, skipRemainingHandlers: Boolean = true) {
    cancelPosition = true
    cancelReason = reason
    if (skipRemainingHandlers) {
        EventDispatchHints.requestSkipRemainingHandlers(this)
    }
}

fun PlayerMoveEvent.cancelRotation(reason: String? = null, skipRemainingHandlers: Boolean = true) {
    cancelRotation = true
    cancelReason = reason
    if (skipRemainingHandlers) {
        EventDispatchHints.requestSkipRemainingHandlers(this)
    }
}

fun PlayerMoveEvent.cancel(reason: String? = null, skipRemainingHandlers: Boolean = true) {
    cancelled = true
    cancelPosition = true
    cancelRotation = true
    cancelReason = reason
    if (skipRemainingHandlers) {
        EventDispatchHints.requestSkipRemainingHandlers(this)
    }
}

fun EntityMoveEvent.cancelPosition(reason: String? = null, skipRemainingHandlers: Boolean = true) {
    cancelPosition = true
    cancelReason = reason
    if (skipRemainingHandlers) {
        EventDispatchHints.requestSkipRemainingHandlers(this)
    }
}

fun EntityMoveEvent.cancelRotation(reason: String? = null, skipRemainingHandlers: Boolean = true) {
    cancelRotation = true
    cancelReason = reason
    if (skipRemainingHandlers) {
        EventDispatchHints.requestSkipRemainingHandlers(this)
    }
}

fun EntityMoveEvent.cancel(reason: String? = null, skipRemainingHandlers: Boolean = true) {
    cancelled = true
    cancelPosition = true
    cancelRotation = true
    cancelReason = reason
    if (skipRemainingHandlers) {
        EventDispatchHints.requestSkipRemainingHandlers(this)
    }
}
