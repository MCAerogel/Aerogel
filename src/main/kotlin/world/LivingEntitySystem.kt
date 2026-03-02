package org.macaroon3145.world

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.macaroon3145.network.codec.BlockStateRegistry
import org.slf4j.LoggerFactory
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.ThreadLocalRandom
import kotlin.math.abs
import kotlin.math.ceil
import kotlin.math.floor
import kotlin.math.pow
import kotlin.math.roundToInt
import kotlin.math.sqrt

enum class AnimalKind(
    val entityTypeKey: String,
    val ambientSoundKey: String,
    val hurtSoundKey: String,
    val deathSoundKey: String,
    val maxHealth: Float
) {
    PIG(
        entityTypeKey = "minecraft:pig",
        ambientSoundKey = "minecraft:entity.pig.ambient",
        hurtSoundKey = "minecraft:entity.pig.hurt",
        deathSoundKey = "minecraft:entity.pig.death",
        maxHealth = 10f
    )
}

enum class AnimalDamageCause {
    GENERIC,
    FALL,
    PLAYER_ATTACK
}

abstract class LivingEntity(
    open val entityId: Int,
    open val uuid: UUID,
    open var x: Double,
    open var y: Double,
    open var z: Double,
    open var vx: Double,
    open var vy: Double,
    open var vz: Double,
    open var yaw: Float,
    open var pitch: Float,
    open var onGround: Boolean,
    open var health: Float,
    open var maxHealth: Float,
    open var fallDistance: Double,
    open var chunkX: Int,
    open var chunkZ: Int
)

data class AnimalEntity(
    val kind: AnimalKind,
    override val entityId: Int,
    override val uuid: UUID,
    override var x: Double,
    override var y: Double,
    override var z: Double,
    override var vx: Double,
    override var vy: Double,
    override var vz: Double,
    override var yaw: Float,
    var headYaw: Float,
    override var pitch: Float,
    override var onGround: Boolean,
    var dead: Boolean,
    override var health: Float,
    override var maxHealth: Float,
    override var fallDistance: Double,
    var fireSeconds: Double,
    var fireDamageAccumulatorSeconds: Double,
    var hurtInvulnerableSeconds: Double,
    var lastHurtAmount: Float,
    override var chunkX: Int,
    override var chunkZ: Int,
    var hitboxWidth: Double,
    var hitboxHeight: Double
) : LivingEntity(
    entityId = entityId,
    uuid = uuid,
    x = x,
    y = y,
    z = z,
    vx = vx,
    vy = vy,
    vz = vz,
    yaw = yaw,
    pitch = pitch,
    onGround = onGround,
    health = health,
    maxHealth = maxHealth,
    fallDistance = fallDistance,
    chunkX = chunkX,
    chunkZ = chunkZ
)

data class AnimalSnapshot(
    val entityId: Int,
    val uuid: UUID,
    val kind: AnimalKind,
    val x: Double,
    val y: Double,
    val z: Double,
    val vx: Double,
    val vy: Double,
    val vz: Double,
    val yaw: Float,
    val headYaw: Float,
    val pitch: Float,
    val onGround: Boolean,
    val health: Float,
    val maxHealth: Float,
    val hitboxWidth: Double,
    val hitboxHeight: Double,
    val chunkPos: ChunkPos
)

data class AnimalTemptSource(
    val x: Double,
    val y: Double,
    val z: Double,
    val range: Double
)

data class AnimalLookSource(
    val x: Double,
    val y: Double,
    val z: Double
)

data class AnimalRideControl(
    val entityId: Int,
    val riderYaw: Float,
    val riderPitch: Float,
    val forwardInput: Double,
    val boostMultiplier: Double,
    val hasClientVehiclePose: Boolean,
    val clientVehicleX: Double,
    val clientVehicleY: Double,
    val clientVehicleZ: Double,
    val clientVehicleYaw: Float,
    val clientVehiclePitch: Float,
    val clientVehicleOnGround: Boolean
)

data class AnimalRemovedEvent(
    val entityId: Int,
    val kind: AnimalKind,
    val chunkPos: ChunkPos,
    val x: Double,
    val y: Double,
    val z: Double,
    val died: Boolean
)

data class AnimalDamagedEvent(
    val entityId: Int,
    val kind: AnimalKind,
    val chunkPos: ChunkPos,
    val amount: Float,
    val cause: AnimalDamageCause,
    val health: Float,
    val died: Boolean
)

data class AnimalTickEvents(
    val spawned: List<AnimalSnapshot>,
    val updated: List<AnimalSnapshot>,
    val removed: List<AnimalRemovedEvent>,
    val damaged: List<AnimalDamagedEvent>
) {
    fun isEmpty(): Boolean {
        return spawned.isEmpty() && updated.isEmpty() && removed.isEmpty() && damaged.isEmpty()
    }
}

data class AnimalDamageResult(
    val damage: AnimalDamagedEvent,
    val removed: AnimalRemovedEvent?
)

class AnimalSystem(
    private val blockStateAt: (Int, Int, Int) -> Int,
    private val rawBrightnessAt: (Int, Int, Int) -> Int = { _, _, _ -> 0 }
) {
    private data class AnimalBrain(
        var retargetCooldownSeconds: Double = 0.0,
        var repathCooldownSeconds: Double = 0.0,
        var repathFailureStreak: Int = 0,
        var repathDirectionCursor: Int = 0,
        var bodyYaw: Float = 0f,
        var lastStableHeadYaw: Float = 0f,
        var rotationInitialized: Boolean = false,
        var panicDangerSeconds: Double = 0.0,
        var panicActive: Boolean = false,
        var targetX: Double = 0.0,
        var targetY: Double = 0.0,
        var targetZ: Double = 0.0,
        var hasTarget: Boolean = false,
        var lastWanderTargetX: Double = 0.0,
        var lastWanderTargetZ: Double = 0.0,
        var hasLastWanderTarget: Boolean = false,
        var waypointX: Double = 0.0,
        var waypointY: Double = 0.0,
        var waypointZ: Double = 0.0,
        var hasWaypoint: Boolean = false,
        var navigationJumping: Boolean = false,
        var lookTargetX: Double = 0.0,
        var lookTargetY: Double = 0.0,
        var lookTargetZ: Double = 0.0,
        var hasLookTarget: Boolean = false,
        var lookAtCooldownSeconds: Double = 0.0,
        var lookYawMaxRotSpeed: Float = 0f,
        var lookPitchMaxRotAngle: Float = 0f,
        var lookAroundSecondsRemaining: Double = 0.0,
        var lookAroundRelX: Double = 0.0,
        var lookAroundRelZ: Double = 0.0,
        var lookAtPlayerSecondsRemaining: Double = 0.0,
        var temptCalmDownSeconds: Double = 0.0,
        var wasTempted: Boolean = false,
        var blockedXLastTick: Boolean = false,
        var blockedZLastTick: Boolean = false,
        var headStableSeconds: Double = 0.0
    )

    private data class CollisionBox(
        val minX: Double,
        val minY: Double,
        val minZ: Double,
        val maxX: Double,
        val maxY: Double,
        val maxZ: Double
    )

    private data class RepathWaypointResult(
        val waypoint: AnimalWaypoint,
        val selectedTargetX: Double,
        val selectedTargetY: Double,
        val selectedTargetZ: Double
    )

    private data class WanderTargetCandidate(
        val targetX: Double,
        val targetY: Double,
        val targetZ: Double,
        val firstWaypoint: AnimalWaypoint,
        val score: Double
    )

    private val entities = ConcurrentHashMap<Int, AnimalEntity>()
    private val snapshots = ConcurrentHashMap<Int, AnimalSnapshot>()
    private val chunkIndex = ConcurrentHashMap<ChunkPos, MutableSet<Int>>()
    private val pendingSpawned = ConcurrentLinkedQueue<Int>()
    private val brains = ConcurrentHashMap<Int, AnimalBrain>()
    private val collisionBoxesByState = ConcurrentHashMap<Int, Array<CollisionBox>>()
    @Volatile private var temptSources: List<AnimalTemptSource> = emptyList()
    @Volatile private var lookSources: List<AnimalLookSource> = emptyList()
    @Volatile private var rideControlsByEntityId: Map<Int, AnimalRideControl> = emptyMap()
    private val navigator = AnimalRecastNavigator(
        blockStateAt = blockStateAt,
        isSolidState = { stateId -> collisionBoxesForState(stateId).isNotEmpty() }
    )

    fun spawn(
        entityId: Int,
        kind: AnimalKind,
        x: Double,
        y: Double,
        z: Double,
        yaw: Float = 0f,
        pitch: Float = 0f
    ): Boolean {
        if (entityId < 0) return false
        val chunkX = chunkXFromBlockX(x)
        val chunkZ = chunkZFromBlockZ(z)
        val dimension = EntityHitboxRegistry.dimension(kind.entityTypeKey) ?: return false
        val entity = AnimalEntity(
            kind = kind,
            entityId = entityId,
            uuid = UUID.randomUUID(),
            x = x,
            y = y,
            z = z,
            vx = 0.0,
            vy = 0.0,
            vz = 0.0,
            yaw = yaw,
            headYaw = yaw,
            pitch = pitch,
            onGround = false,
            dead = false,
            health = kind.maxHealth,
            maxHealth = kind.maxHealth,
            fallDistance = 0.0,
            fireSeconds = 0.0,
            fireDamageAccumulatorSeconds = 0.0,
            hurtInvulnerableSeconds = 0.0,
            lastHurtAmount = 0f,
            chunkX = chunkX,
            chunkZ = chunkZ,
            hitboxWidth = dimension.width,
            hitboxHeight = dimension.height
        )
        if (entities.putIfAbsent(entityId, entity) != null) return false
        brains[entityId] = AnimalBrain()
        val snapshot = toSnapshot(entity)
        snapshots[entityId] = snapshot
        addToChunkIndex(snapshot.chunkPos, entityId)
        pendingSpawned.add(entityId)
        return true
    }

    fun hasAnimals(): Boolean = snapshots.isNotEmpty()

    fun snapshot(entityId: Int): AnimalSnapshot? = snapshots[entityId]

    fun snapshotsInChunk(chunkX: Int, chunkZ: Int): List<AnimalSnapshot> {
        val chunkPos = ChunkPos(chunkX, chunkZ)
        val ids = chunkIndex[chunkPos] ?: return emptyList()
        if (ids.isEmpty()) return emptyList()
        val out = ArrayList<AnimalSnapshot>(ids.size)
        for (id in ids) {
            val snapshot = snapshots[id] ?: continue
            if (snapshot.chunkPos == chunkPos) out.add(snapshot)
        }
        return out
    }

    fun allSnapshots(): List<AnimalSnapshot> = snapshots.values.toList()

    fun setTemptSources(sources: List<AnimalTemptSource>) {
        temptSources = if (sources.isEmpty()) emptyList() else sources.toList()
    }

    fun setLookSources(sources: List<AnimalLookSource>) {
        lookSources = if (sources.isEmpty()) emptyList() else sources.toList()
    }

    fun setRideControls(controls: List<AnimalRideControl>) {
        if (controls.isEmpty()) {
            rideControlsByEntityId = emptyMap()
            return
        }
        val next = HashMap<Int, AnimalRideControl>(controls.size)
        for (control in controls) {
            next[control.entityId] = control
        }
        rideControlsByEntityId = next
    }

    fun entityIdsInChunk(chunkX: Int, chunkZ: Int): IntArray {
        val chunkPos = ChunkPos(chunkX, chunkZ)
        val ids = chunkIndex[chunkPos] ?: return IntArray(0)
        if (ids.isEmpty()) return IntArray(0)
        val out = IntArray(ids.size)
        var i = 0
        for (id in ids) {
            val snapshot = snapshots[id] ?: continue
            if (snapshot.chunkPos != chunkPos) continue
            out[i++] = id
        }
        return if (i == out.size) out else out.copyOf(i)
    }

    fun drainSpawnedSnapshots(): List<AnimalSnapshot> {
        if (pendingSpawned.isEmpty()) return emptyList()
        val out = ArrayList<AnimalSnapshot>()
        while (true) {
            val entityId = pendingSpawned.poll() ?: break
            val snapshot = snapshots[entityId] ?: continue
            out.add(snapshot)
        }
        return out
    }

    fun tickChunk(
        chunkPos: ChunkPos,
        deltaSeconds: Double,
        chunkTimeRecorder: ((ChunkPos, Long) -> Unit)? = null
    ): AnimalTickEvents {
        if (deltaSeconds <= 0.0) return AnimalTickEvents(emptyList(), emptyList(), emptyList(), emptyList())
        val tickScale = deltaSeconds * TICKS_PER_SECOND
        if (tickScale <= 0.0 || !tickScale.isFinite()) {
            return AnimalTickEvents(emptyList(), emptyList(), emptyList(), emptyList())
        }
        val ids = chunkIndex[chunkPos]?.toList().orEmpty()
        if (ids.isEmpty()) {
            chunkTimeRecorder?.invoke(chunkPos, 0L)
            return AnimalTickEvents(emptyList(), emptyList(), emptyList(), emptyList())
        }
        val startedAt = System.nanoTime()
        val updated = ArrayList<AnimalSnapshot>()
        val removed = ArrayList<AnimalRemovedEvent>()
        val damaged = ArrayList<AnimalDamagedEvent>()

        for (entityId in ids) {
            val entity = entities[entityId] ?: continue
            if (entity.chunkX != chunkPos.x || entity.chunkZ != chunkPos.z) continue
            processEntityTick(entityId, entity, tickScale, updated, removed, damaged)
        }

        chunkTimeRecorder?.invoke(chunkPos, System.nanoTime() - startedAt)
        return AnimalTickEvents(
            spawned = emptyList(),
            updated = updated,
            removed = removed,
            damaged = damaged
        )
    }

    fun tick(
        deltaSeconds: Double,
        activeSimulationChunks: Set<ChunkPos>? = null,
        chunkTimeRecorder: ((ChunkPos, Long) -> Unit)? = null
    ): AnimalTickEvents {
        if (deltaSeconds <= 0.0) return AnimalTickEvents(emptyList(), emptyList(), emptyList(), emptyList())
        if (entities.isEmpty()) return AnimalTickEvents(emptyList(), emptyList(), emptyList(), emptyList())
        val spawned = drainSpawnedSnapshots().toMutableList()
        val updated = ArrayList<AnimalSnapshot>()
        val removed = ArrayList<AnimalRemovedEvent>()
        val damaged = ArrayList<AnimalDamagedEvent>()
        val chunks = if (activeSimulationChunks == null) {
            chunkIndex.keys.toList()
        } else {
            activeSimulationChunks
        }
        for (chunkPos in chunks) {
            val lane = tickChunk(chunkPos, deltaSeconds, chunkTimeRecorder)
            if (lane.updated.isNotEmpty()) updated.addAll(lane.updated)
            if (lane.removed.isNotEmpty()) removed.addAll(lane.removed)
            if (lane.damaged.isNotEmpty()) damaged.addAll(lane.damaged)
        }

        return AnimalTickEvents(spawned, updated, removed, damaged)
    }

    fun damage(entityId: Int, amount: Float, cause: AnimalDamageCause): AnimalDamageResult? {
        if (amount <= 0f) return null
        val entity = entities[entityId] ?: return null
        if (entity.dead) return null
        return applyDamageInternal(entity, amount, cause)
    }

    fun remove(entityId: Int, died: Boolean = false): AnimalRemovedEvent? {
        val entity = entities[entityId] ?: return null
        return removeInternal(entity, died)
    }

    fun setVelocity(entityId: Int, vx: Double, vy: Double, vz: Double): Boolean {
        val entity = entities[entityId] ?: return false
        entity.vx = vx
        entity.vy = vy
        entity.vz = vz
        snapshots[entityId] = toSnapshot(entity)
        return true
    }

    fun addHorizontalImpulse(entityId: Int, impulseX: Double, impulseZ: Double): Boolean {
        val entity = entities[entityId] ?: return false
        entity.vx += impulseX
        entity.vz += impulseZ
        snapshots[entityId] = toSnapshot(entity)
        return true
    }

    private fun removeInternal(entity: AnimalEntity, died: Boolean): AnimalRemovedEvent? {
        if (!entities.remove(entity.entityId, entity)) return null
        brains.remove(entity.entityId)
        val chunkPos = ChunkPos(entity.chunkX, entity.chunkZ)
        snapshots.remove(entity.entityId)
        removeFromChunkIndex(chunkPos, entity.entityId)
        return AnimalRemovedEvent(
            entityId = entity.entityId,
            kind = entity.kind,
            chunkPos = chunkPos,
            x = entity.x,
            y = entity.y,
            z = entity.z,
            died = died
        )
    }

    private fun processEntityTick(
        entityId: Int,
        entity: AnimalEntity,
        tickScale: Double,
        updated: MutableList<AnimalSnapshot>,
        removed: MutableList<AnimalRemovedEvent>,
        damaged: MutableList<AnimalDamagedEvent>
    ) {
        val brain = brains.computeIfAbsent(entityId) { AnimalBrain() }
        val preAiX = entity.x
        val preAiY = entity.y
        val preAiZ = entity.z
        val preAiVx = entity.vx
        val preAiVy = entity.vy
        val preAiVz = entity.vz
        val preAiYaw = entity.yaw
        val preAiHeadYaw = entity.headYaw
        val preAiPitch = entity.pitch
        val preAiOnGround = entity.onGround
        applyAiIntent(entity, brain, tickScale)
        val rideControl = rideControlsByEntityId[entity.entityId]
        val clientDrivenRide =
            entity.kind == AnimalKind.PIG &&
                rideControl != null
        tickHurtInvulnerability(entity, tickScale)
        val oldChunk = ChunkPos(entity.chunkX, entity.chunkZ)
        val oldX = if (clientDrivenRide) preAiX else entity.x
        val oldY = if (clientDrivenRide) preAiY else entity.y
        val oldZ = if (clientDrivenRide) preAiZ else entity.z
        val oldVx = if (clientDrivenRide) preAiVx else entity.vx
        val oldVy = if (clientDrivenRide) preAiVy else entity.vy
        val oldVz = if (clientDrivenRide) preAiVz else entity.vz
        val oldYaw = if (clientDrivenRide) preAiYaw else entity.yaw
        val oldHeadYaw = if (clientDrivenRide) preAiHeadYaw else entity.headYaw
        val oldPitch = if (clientDrivenRide) preAiPitch else entity.pitch
        val oldOnGround = if (clientDrivenRide) preAiOnGround else entity.onGround

        if (clientDrivenRide) {
            entity.vx = 0.0
            entity.vy = 0.0
            entity.vz = 0.0
            entity.fallDistance = 0.0
        } else {
            stepEntity(entity, brain, tickScale)
        }
        if (tickFireState(entity, brain, tickScale, removed, damaged)) {
            return
        }
        if (!clientDrivenRide) {
            applyBodyAndHeadRotationControl(entity, brain, oldX, oldZ, tickScale)
            updateFallDistance(entity, oldY)
            if (!entity.dead && !oldOnGround && entity.onGround) {
                val fallDamage = ceil(entity.fallDistance - SAFE_FALL_DISTANCE_BLOCKS).toInt()
                entity.fallDistance = 0.0
                if (fallDamage > 0) {
                    val result = applyDamageInternal(entity, fallDamage.toFloat(), AnimalDamageCause.FALL)
                    if (result != null) {
                        damaged.add(result.damage)
                        if (result.removed != null) {
                            removed.add(result.removed)
                            return
                        }
                    }
                }
            }
        }

        val fellOut = entity.y < DESPAWN_Y || entity.y > WORLD_MAX_Y + 2.0
        if (fellOut) {
            val removedEvent = removeInternal(entity, died = false)
            if (removedEvent != null) removed.add(removedEvent)
            return
        }

        val newChunkX = chunkXFromBlockX(entity.x)
        val newChunkZ = chunkZFromBlockZ(entity.z)
        val newChunk = ChunkPos(newChunkX, newChunkZ)
        if (newChunk != oldChunk) {
            entity.chunkX = newChunkX
            entity.chunkZ = newChunkZ
            moveChunkIndex(entityId, oldChunk, newChunk)
        }

        val moved =
            abs(entity.x - oldX) > POSITION_EPSILON ||
                abs(entity.y - oldY) > POSITION_EPSILON ||
                abs(entity.z - oldZ) > POSITION_EPSILON ||
                abs(entity.vx - oldVx) > VELOCITY_EPSILON ||
                abs(entity.vy - oldVy) > VELOCITY_EPSILON ||
                abs(entity.vz - oldVz) > VELOCITY_EPSILON ||
                abs(wrapDegrees(entity.yaw - oldYaw)) > ROTATION_EPSILON_DEGREES ||
                abs(wrapDegrees(entity.headYaw - oldHeadYaw)) > ROTATION_EPSILON_DEGREES ||
                abs(wrapDegrees(entity.pitch - oldPitch)) > ROTATION_EPSILON_DEGREES ||
                oldOnGround != entity.onGround ||
                newChunk != oldChunk
        if (moved) {
            val snapshot = toSnapshot(entity)
            snapshots[entityId] = snapshot
            updated.add(snapshot)
        }
    }

    private fun updateFallDistance(entity: AnimalEntity, oldY: Double) {
        val deltaY = entity.y - oldY
        when {
            entity.onGround -> Unit
            deltaY < 0.0 -> entity.fallDistance += -deltaY
            deltaY > 0.0 -> entity.fallDistance = 0.0
        }
    }

    private fun tickFireState(
        entity: AnimalEntity,
        brain: AnimalBrain,
        tickScale: Double,
        removed: MutableList<AnimalRemovedEvent>,
        damaged: MutableList<AnimalDamagedEvent>
    ): Boolean {
        val deltaSeconds = tickScale / TICKS_PER_SECOND
        if (deltaSeconds <= 0.0 || !deltaSeconds.isFinite()) return false
        val inWater = isWaterAt(entity.x, entity.y, entity.z) || isWaterAt(entity.x, entity.y + entity.hitboxHeight * 0.5, entity.z)
        val inIgniter = isIgniterAt(entity.x, entity.y, entity.z) || isIgniterAt(entity.x, entity.y + entity.hitboxHeight * 0.5, entity.z)
        if (inWater) {
            entity.fireSeconds = 0.0
            entity.fireDamageAccumulatorSeconds = 0.0
        } else {
            if (inIgniter) {
                entity.fireSeconds = maxOf(entity.fireSeconds, FIRE_IGNITE_SECONDS)
            } else if (entity.fireSeconds > 0.0) {
                entity.fireSeconds = (entity.fireSeconds - deltaSeconds).coerceAtLeast(0.0)
            }
        }
        if (entity.fireSeconds <= 0.0) return false

        // EscapeDangerGoal parity: burning state should trigger panic escape behavior.
        brain.panicDangerSeconds = maxOf(brain.panicDangerSeconds, PANIC_RECENT_DAMAGE_SECONDS)
        entity.fireDamageAccumulatorSeconds += deltaSeconds
        while (entity.fireDamageAccumulatorSeconds + FIRE_DAMAGE_TIME_EPSILON >= FIRE_DAMAGE_INTERVAL_SECONDS) {
            entity.fireDamageAccumulatorSeconds -= FIRE_DAMAGE_INTERVAL_SECONDS
            val result = applyDamageInternal(entity, FIRE_DAMAGE_AMOUNT, AnimalDamageCause.GENERIC)
            if (result != null) {
                damaged.add(result.damage)
                if (result.removed != null) {
                    removed.add(result.removed)
                    return true
                }
            }
            if (entity.dead) return true
        }
        return false
    }

    private fun isWaterAt(x: Double, y: Double, z: Double): Boolean {
        val bx = floor(x).toInt()
        val by = floor(y).toInt()
        val bz = floor(z).toInt()
        val stateId = blockStateAt(bx, by, bz)
        if (stateId <= 0) return false
        val key = BlockStateRegistry.parsedState(stateId)?.blockKey ?: return false
        return key == "minecraft:water"
    }

    private fun isIgniterAt(x: Double, y: Double, z: Double): Boolean {
        val bx = floor(x).toInt()
        val by = floor(y).toInt()
        val bz = floor(z).toInt()
        val stateId = blockStateAt(bx, by, bz)
        if (stateId <= 0) return false
        val key = BlockStateRegistry.parsedState(stateId)?.blockKey ?: return false
        return key in FIRE_IGNITER_BLOCK_KEYS
    }

    private fun stepEntity(entity: AnimalEntity, brain: AnimalBrain, tickScale: Double) {
        entity.vy -= GRAVITY_PER_TICK * tickScale
        val totalDx = entity.vx * tickScale
        val totalDy = entity.vy * tickScale
        val totalDz = entity.vz * tickScale
        val steps = movementSubsteps(totalDx, totalDy, totalDz)
        val subScale = tickScale / steps
        var onGround = false
        var blockedXThisTick = false
        var blockedZThisTick = false
        repeat(steps) {
            val wasGrounded = entity.onGround || onGround
            var steppedThisSubstep = false
            var jumpedThisSubstep = false
            val nextY = entity.y + entity.vy * subScale
            if (!collidesAt(entity, entity.x, nextY, entity.z)) {
                entity.y = nextY
            } else {
                if (entity.vy < 0.0) onGround = true
                entity.vy = 0.0
            }

            val nextX = entity.x + entity.vx * subScale
            if (!collidesAt(entity, nextX, entity.y, entity.z)) {
                entity.x = nextX
            } else {
                val canStepUp = (entity.onGround || onGround) && !brain.navigationJumping && entity.vy <= 0.0
                if (canStepUp && tryStepUp(entity, nextX, entity.y, entity.z)) {
                    steppedThisSubstep = true
                    onGround = false
                } else if (canStepUp && tryJumpStep(entity, nextX, entity.y, entity.z, subScale, tickScale)) {
                    jumpedThisSubstep = true
                    brain.navigationJumping = true
                    onGround = false
                    // Preserve a wall-snag feel: the axis that triggered jump stays blocked this tick.
                    entity.vx = 0.0
                    blockedXThisTick = true
                } else {
                    entity.vx = 0.0
                    blockedXThisTick = true
                }
            }

            val nextZ = entity.z + entity.vz * subScale
            if (!collidesAt(entity, entity.x, entity.y, nextZ)) {
                entity.z = nextZ
            } else {
                val canStepUp = !steppedThisSubstep &&
                    !jumpedThisSubstep &&
                    (entity.onGround || onGround) &&
                    !brain.navigationJumping &&
                    entity.vy <= 0.0
                if (canStepUp && tryStepUp(entity, entity.x, entity.y, nextZ)) {
                    onGround = false
                } else if (canStepUp && tryJumpStep(entity, entity.x, entity.y, nextZ, subScale, tickScale)) {
                    brain.navigationJumping = true
                    onGround = false
                    // Preserve a wall-snag feel: the axis that triggered jump stays blocked this tick.
                    entity.vz = 0.0
                    blockedZThisTick = true
                } else {
                    entity.vz = 0.0
                    blockedZThisTick = true
                }
            }

            if (!onGround && wasGrounded && entity.vy <= 0.0) {
                if (tryStepDown(entity, entity.x, entity.y, entity.z)) {
                    onGround = true
                    entity.vy = 0.0
                }
            }
        }
        entity.onGround = onGround
        if (entity.onGround) {
            brain.navigationJumping = false
        }
        brain.blockedXLastTick = blockedXThisTick
        brain.blockedZLastTick = blockedZThisTick

        val drag = if (entity.onGround) GROUND_DRAG_PER_TICK.pow(tickScale) else AIR_DRAG_PER_TICK.pow(tickScale)
        entity.vx *= drag
        entity.vy *= VERTICAL_DRAG_PER_TICK.pow(tickScale)
        entity.vz *= drag
        if (abs(entity.vx) < VELOCITY_EPSILON) entity.vx = 0.0
        if (abs(entity.vy) < VELOCITY_EPSILON) entity.vy = 0.0
        if (abs(entity.vz) < VELOCITY_EPSILON) entity.vz = 0.0

    }

    private fun tryStepUp(entity: AnimalEntity, targetX: Double, currentY: Double, targetZ: Double): Boolean {
        val steppedY = currentY + MAX_STEP_UP_HEIGHT
        if (collidesAt(entity, entity.x, steppedY, entity.z)) return false
        if (collidesAt(entity, targetX, steppedY, targetZ)) return false
        entity.y = steppedY
        entity.x = targetX
        entity.z = targetZ
        return true
    }

    private fun tryJumpStep(
        entity: AnimalEntity,
        targetX: Double,
        currentY: Double,
        targetZ: Double,
        subScale: Double,
        tickScale: Double
    ): Boolean {
        val halfStepY = currentY + MAX_STEP_UP_HEIGHT
        if (!collidesAt(entity, targetX, halfStepY, targetZ)) return false

        val jumpStepY = currentY + MAX_JUMP_STEP_HEIGHT
        if (collidesAt(entity, entity.x, jumpStepY, entity.z)) return false
        // Vanilla LivingEntity#jumpFromGround semantics:
        // set vertical velocity only; position advances through normal travel/collision.
        entity.vy = entity.vy.coerceAtLeast(JUMP_VELOCITY_PER_TICK)
        val jumpNextY = entity.y + entity.vy * subScale
        if (!collidesAt(entity, entity.x, jumpNextY, entity.z)) {
            entity.y = jumpNextY
        }
        return true
    }

    private fun tryStepDown(entity: AnimalEntity, targetX: Double, currentY: Double, targetZ: Double): Boolean {
        val samples = maxOf(1, ceil(MAX_STEP_DOWN_HEIGHT / STEP_DOWN_SAMPLE_HEIGHT).toInt())
        for (i in 1..samples) {
            val depth = i * STEP_DOWN_SAMPLE_HEIGHT
            if (depth > MAX_STEP_DOWN_HEIGHT + POSITION_EPSILON) break
            val candidateY = currentY - depth
            if (collidesAt(entity, targetX, candidateY, targetZ)) return false
            if (collidesAt(entity, targetX, candidateY - GROUND_PROBE_EPSILON, targetZ)) {
                entity.x = targetX
                entity.y = candidateY
                entity.z = targetZ
                return true
            }
        }
        return false
    }

    private fun applyDamageInternal(entity: AnimalEntity, amount: Float, cause: AnimalDamageCause): AnimalDamageResult? {
        if (amount <= 0f || entity.dead) return null
        val effectiveAmount = effectiveDamageAfterInvulnerability(entity, amount)
        if (effectiveAmount <= DAMAGE_EPSILON) return null
        val nextHealth = (entity.health - effectiveAmount).coerceAtLeast(0f)
        if (nextHealth >= entity.health) return null
        entity.health = nextHealth
        entity.hurtInvulnerableSeconds = HURT_INVULNERABILITY_SECONDS
        entity.lastHurtAmount = amount
        val brain = brains.computeIfAbsent(entity.entityId) { AnimalBrain() }
        if (isPanicCause(cause)) {
            brain.panicDangerSeconds = maxOf(brain.panicDangerSeconds, PANIC_RECENT_DAMAGE_SECONDS)
            brain.retargetCooldownSeconds = 0.0
            brain.repathCooldownSeconds = 0.0
        }
        val chunkPos = ChunkPos(entity.chunkX, entity.chunkZ)
        val died = nextHealth <= 0f
        val damageEvent = AnimalDamagedEvent(
            entityId = entity.entityId,
            kind = entity.kind,
            chunkPos = chunkPos,
            amount = effectiveAmount,
            cause = cause,
            health = nextHealth,
            died = died
        )
        return if (died) {
            entity.dead = true
            AnimalDamageResult(
                damage = damageEvent,
                removed = AnimalRemovedEvent(
                    entityId = entity.entityId,
                    kind = entity.kind,
                    chunkPos = chunkPos,
                    x = entity.x,
                    y = entity.y,
                    z = entity.z,
                    died = true
                )
            )
        } else {
            snapshots[entity.entityId] = toSnapshot(entity)
            AnimalDamageResult(damage = damageEvent, removed = null)
        }
    }

    private fun applyAiIntent(entity: AnimalEntity, brain: AnimalBrain, tickScale: Double) {
        if (entity.dead) return
        val deltaSeconds = tickScale / TICKS_PER_SECOND
        if (deltaSeconds <= 0.0 || !deltaSeconds.isFinite()) return
        val rideControl = rideControlsByEntityId[entity.entityId]
        if (rideControl != null && entity.kind == AnimalKind.PIG) {
            applyRiderControlIntent(entity, brain, tickScale, rideControl)
            return
        }
        brain.panicDangerSeconds = (brain.panicDangerSeconds - deltaSeconds).coerceAtLeast(0.0)
        brain.temptCalmDownSeconds = (brain.temptCalmDownSeconds - deltaSeconds).coerceAtLeast(0.0)
        brain.retargetCooldownSeconds = (brain.retargetCooldownSeconds - deltaSeconds).coerceAtLeast(0.0)
        brain.repathCooldownSeconds = (brain.repathCooldownSeconds - deltaSeconds).coerceAtLeast(0.0)
        if (!brain.panicActive && brain.panicDangerSeconds > 0.0) {
            startPanicEscape(entity, brain)
        }
        val isPanicking = brain.panicActive
        val temptSource = if (!isPanicking && brain.temptCalmDownSeconds <= 0.0) nearestTemptSource(entity) else null
        val isTempted = temptSource != null
        val wasTempted = brain.wasTempted
        brain.wasTempted = isTempted
        if (!isTempted) {
            brain.hasLookTarget = false
            brain.lookAtCooldownSeconds = 0.0
            if (wasTempted) {
                // Vanilla TemptGoal#stop stops navigation immediately when temptation ends.
                brain.hasTarget = false
                brain.hasWaypoint = false
                brain.temptCalmDownSeconds = TEMPT_CALM_DOWN_SECONDS
                brain.retargetCooldownSeconds = randomWanderPauseSeconds(
                    WANDER_RESTART_DELAY_MIN_SECONDS,
                    WANDER_RESTART_DELAY_MAX_SECONDS
                )
            }
        }
        if (!isTempted && !isPanicking) {
            val lookedAtPlayer = tickLookAtNearbyPlayer(entity, brain, deltaSeconds)
            if (!lookedAtPlayer) {
                tickRandomLookAround(entity, brain, deltaSeconds)
            }
        } else {
            brain.lookAroundSecondsRemaining = 0.0
            brain.lookAtPlayerSecondsRemaining = 0.0
        }

        if (brain.hasTarget) {
            val dx = brain.targetX - entity.x
            val dz = brain.targetZ - entity.z
            if ((dx * dx) + (dz * dz) <= TARGET_REACHED_DISTANCE_SQ) {
                brain.hasTarget = false
                brain.hasWaypoint = false
                if (isPanicking) {
                    brain.panicActive = false
                } else if (!isTempted) {
                    brain.retargetCooldownSeconds = randomWanderPauseSeconds(
                        WANDER_RESTART_DELAY_MIN_SECONDS,
                        WANDER_RESTART_DELAY_MAX_SECONDS
                    )
                }
            }
        }

        if (temptSource != null) {
            brain.targetX = temptSource.x
            brain.targetY = temptSource.y
            brain.targetZ = temptSource.z
            brain.hasTarget = true
            brain.lookTargetX = temptSource.x
            brain.lookTargetY = temptSource.y
            brain.lookTargetZ = temptSource.z
            brain.hasLookTarget = true
            brain.lookYawMaxRotSpeed = TEMPT_LOOK_YAW_SPEED_DEGREES_PER_TICK
            brain.lookPitchMaxRotAngle = TEMPT_LOOK_PITCH_SPEED_DEGREES_PER_TICK
            brain.lookAtCooldownSeconds = LOOK_CONTROL_TARGET_COOLDOWN_SECONDS
            brain.retargetCooldownSeconds = 0.0
            val dx = brain.targetX - entity.x
            val dz = brain.targetZ - entity.z
            if ((dx * dx) + (dz * dz) <= TEMPT_STOP_DISTANCE_SQ) {
                brain.hasWaypoint = false
                brain.repathCooldownSeconds = AI_TEMPT_REPATH_RETRY_SECONDS
                return
            }
        }

        if (!isPanicking && !isTempted && !brain.hasTarget && brain.retargetCooldownSeconds <= 0.0) {
            val random = ThreadLocalRandom.current()
            // Delta-time equivalent of vanilla RandomStrollGoal gate:
            // per-tick chance p = 1/60, over dt ticks => 1 - (1 - p)^dt.
            val simulatedTicks = (deltaSeconds * TICKS_PER_SECOND).coerceAtLeast(0.0)
            val perTickMiss = (VANILLA_WANDER_TICK_CHANCE_BASE - 1).toDouble() / VANILLA_WANDER_TICK_CHANCE_BASE.toDouble()
            val startChance = (1.0 - perTickMiss.pow(simulatedTicks)).coerceIn(0.0, 1.0)
            if (random.nextDouble() < startChance) {
                assignNewTarget(entity, brain)
            }
        }

        val shouldRepath = if (isPanicking) {
            !brain.hasWaypoint || brain.repathCooldownSeconds <= 0.0
        } else if (isTempted) {
            !brain.hasWaypoint || brain.repathCooldownSeconds <= 0.0
        } else {
            !brain.hasWaypoint
        }
        if (brain.hasTarget && shouldRepath) {
            val repath = if (isPanicking || isTempted) {
                val direct = navigator.findNextWaypoint(
                    startX = entity.x,
                    startY = entity.y,
                    startZ = entity.z,
                    targetX = brain.targetX,
                    targetY = brain.targetY,
                    targetZ = brain.targetZ,
                    agentRadius = entity.hitboxWidth * 0.5,
                    agentHeight = entity.hitboxHeight,
                    agentMaxClimb = NAVIGATION_MAX_CLIMB_HEIGHT
                )
                if (direct == null) {
                    null
                } else {
                    RepathWaypointResult(
                        waypoint = direct,
                        selectedTargetX = brain.targetX,
                        selectedTargetY = brain.targetY,
                        selectedTargetZ = brain.targetZ
                    )
                }
            } else {
                findWaypointWithDirectionalFallback(
                    entity = entity,
                    brain = brain,
                    targetX = brain.targetX,
                    targetY = brain.targetY,
                    targetZ = brain.targetZ
                )
            }
            if (repath != null) {
                brain.waypointX = repath.waypoint.x
                brain.waypointY = repath.waypoint.y
                brain.waypointZ = repath.waypoint.z
                brain.hasWaypoint = true
                brain.repathFailureStreak = 0
                brain.repathCooldownSeconds = if (isPanicking) {
                    AI_PANIC_REPATH_INTERVAL_SECONDS
                } else if (isTempted) {
                    AI_TEMPT_REPATH_INTERVAL_SECONDS
                } else {
                    AI_REPATH_INTERVAL_SECONDS
                }
            } else {
                brain.repathDirectionCursor = (brain.repathDirectionCursor + 1) and 7
                brain.repathFailureStreak = (brain.repathFailureStreak + 1).coerceAtMost(MAX_REPATH_FAILURE_STREAK)
                if (isPanicking) {
                    brain.panicActive = false
                    brain.hasWaypoint = false
                    brain.hasTarget = false
                    brain.repathCooldownSeconds = 0.0
                } else if (isTempted) {
                    // Keep target, but avoid direct-line fallback so path and straight steering do not conflict.
                    brain.hasWaypoint = false
                    brain.repathCooldownSeconds = repathBackoffSeconds(
                        baseSeconds = AI_TEMPT_REPATH_RETRY_SECONDS,
                        failureStreak = brain.repathFailureStreak
                    )
                } else {
                    brain.hasWaypoint = false
                    brain.hasTarget = false
                    brain.retargetCooldownSeconds = randomWanderPauseSeconds(
                        WANDER_REPATH_FAIL_DELAY_MIN_SECONDS,
                        WANDER_REPATH_FAIL_DELAY_MAX_SECONDS
                    )
                    brain.repathCooldownSeconds = repathBackoffSeconds(
                        baseSeconds = AI_REPATH_INTERVAL_SECONDS,
                        failureStreak = brain.repathFailureStreak
                    )
                }
            }
        }

        if (isPanicking && !brain.hasWaypoint) {
            brain.panicActive = false
            if (brain.panicDangerSeconds > 0.0) {
                startPanicEscape(entity, brain)
            }
        }
        if (!brain.hasWaypoint) return

        val toWaypointX = brain.waypointX - entity.x
        val toWaypointZ = brain.waypointZ - entity.z
        val distSq = (toWaypointX * toWaypointX) + (toWaypointZ * toWaypointZ)
        val reachedDistSq = if (isPanicking || isTempted) TEMPT_WAYPOINT_REACHED_DISTANCE_SQ else WAYPOINT_REACHED_DISTANCE_SQ
        if (distSq <= reachedDistSq) {
            brain.hasWaypoint = false
            if (isPanicking) {
                brain.panicActive = false
                brain.hasTarget = false
            } else if (!isTempted) {
                brain.hasTarget = false
                brain.retargetCooldownSeconds = randomWanderPauseSeconds(
                    WANDER_RESTART_DELAY_MIN_SECONDS,
                    WANDER_RESTART_DELAY_MAX_SECONDS
                )
            }
            brain.repathCooldownSeconds = 0.0
            return
        }
        val invLen = 1.0 / sqrt(distSq)
        val dirX = toWaypointX * invLen
        val dirZ = toWaypointZ * invLen
        // Vanilla knockback preserves airborne momentum better than our direct velocity steering model.
        // While recently hurt and airborne, suppress AI turn/steer so knockback direction is not snapped.
        val suppressAirborneSteering = !entity.onGround && entity.hurtInvulnerableSeconds > 0.0
        if (suppressAirborneSteering) {
            return
        }
        val targetYaw = (Math.toDegrees(kotlin.math.atan2(dirZ, dirX)).toFloat() - 90.0f).normalizeYaw()
        entity.yaw = rotLerp(entity.yaw, targetYaw, MOVE_CONTROL_MAX_TURN_DEGREES_PER_TICK * tickScale.toFloat())
        val desiredSpeed = when {
            isPanicking -> ANIMAL_PANIC_SPEED
            isTempted -> ANIMAL_TEMPT_SPEED
            else -> ANIMAL_WALK_SPEED
        } * ANIMAL_NAV_SPEED_TO_VELOCITY_SCALE
        val desiredVx = dirX * desiredSpeed
        val desiredVz = dirZ * desiredSpeed
        // Vanilla MoveControl applies movement speed immediately while pathing.
        // Keep gentle steering only for wander; tempted/panic should respond at full speed.
        if (brain.blockedXLastTick) {
            entity.vx = 0.0
        }
        if (brain.blockedZLastTick) {
            entity.vz = 0.0
        }
        val steerBlend = if (isTempted || isPanicking) {
            AI_CHASE_STEER_BLEND
        } else {
            AI_STEER_BLEND
        }
        val steerBlendScaled = blendForTickScale(steerBlend, tickScale)
        entity.vx += (desiredVx - entity.vx) * steerBlendScaled
        entity.vz += (desiredVz - entity.vz) * steerBlendScaled
    }

    private fun applyRiderControlIntent(
        entity: AnimalEntity,
        brain: AnimalBrain,
        tickScale: Double,
        rideControl: AnimalRideControl
    ) {
        brain.panicDangerSeconds = 0.0
        brain.panicActive = false
        brain.hasTarget = false
        brain.hasWaypoint = false
        brain.hasLookTarget = false
        brain.lookAtCooldownSeconds = 0.0
        brain.temptCalmDownSeconds = 0.0
        brain.wasTempted = false

        if (rideControl.hasClientVehiclePose) {
            entity.x = rideControl.clientVehicleX
            entity.y = rideControl.clientVehicleY
            entity.z = rideControl.clientVehicleZ
            entity.onGround = rideControl.clientVehicleOnGround
            entity.yaw = rideControl.clientVehicleYaw.normalizeYaw()
            entity.pitch = rideControl.clientVehiclePitch
            entity.headYaw = entity.yaw
            brain.bodyYaw = entity.yaw
            entity.vx = 0.0
            entity.vy = 0.0
            entity.vz = 0.0
            return
        }
        // Vanilla PigEntity#tickControlled:
        // setRotation(riderYaw, riderPitch * 0.5F); headYaw/bodyYaw/lastYaw = yaw.
        val riderYaw = rideControl.riderYaw.normalizeYaw()
        entity.yaw = riderYaw
        entity.headYaw = riderYaw
        brain.bodyYaw = riderYaw
        val riderPitchTarget = (rideControl.riderPitch * 0.5f).coerceIn(-MAX_HEAD_PITCH_DEGREES, MAX_HEAD_PITCH_DEGREES)
        entity.pitch = riderPitchTarget

        val forwardInput = rideControl.forwardInput
        if (forwardInput <= 0.0) {
            return
        }
        val yawRad = Math.toRadians(entity.yaw.toDouble())
        val dirX = -kotlin.math.sin(yawRad)
        val dirZ = kotlin.math.cos(yawRad)
        val boost = rideControl.boostMultiplier.coerceAtLeast(1.0)
        val desiredSpeed = ANIMAL_RIDE_BASE_SPEED * boost
        val desiredVx = dirX * desiredSpeed
        val desiredVz = dirZ * desiredSpeed
        val steerBlendScaled = blendForTickScale(AI_RIDE_STEER_BLEND, tickScale)
        entity.vx += (desiredVx - entity.vx) * steerBlendScaled
        entity.vz += (desiredVz - entity.vz) * steerBlendScaled
    }

    private fun startPanicEscape(entity: AnimalEntity, brain: AnimalBrain): Boolean {
        if (entity.fireSeconds > 0.0) {
            val water = locateClosestWaterTarget(entity)
            if (water != null) {
                brain.targetX = water.x
                brain.targetY = water.y
                brain.targetZ = water.z
                brain.hasTarget = true
                brain.waypointX = water.x
                brain.waypointY = water.y
                brain.waypointZ = water.z
                brain.hasWaypoint = true
                brain.repathFailureStreak = 0
                brain.retargetCooldownSeconds = 0.0
                brain.repathCooldownSeconds = AI_PANIC_REPATH_INTERVAL_SECONDS
                brain.panicActive = true
                return true
            }
        }
        val random = ThreadLocalRandom.current()
        var bestCandidate: WanderTargetCandidate? = null
        repeat(PANIC_TARGET_ATTEMPTS) {
            val offsetX = random.nextInt(PANIC_TARGET_HORIZONTAL_RANGE * 2 + 1) - PANIC_TARGET_HORIZONTAL_RANGE
            val offsetZ = random.nextInt(PANIC_TARGET_HORIZONTAL_RANGE * 2 + 1) - PANIC_TARGET_HORIZONTAL_RANGE
            if (offsetX == 0 && offsetZ == 0) return@repeat
            val offsetY = random.nextInt(PANIC_TARGET_VERTICAL_RANGE * 2 + 1) - PANIC_TARGET_VERTICAL_RANGE
            val targetX = entity.x + offsetX.toDouble()
            val targetY = entity.y + offsetY.toDouble()
            val targetZ = entity.z + offsetZ.toDouble()
            val waypoint = navigator.findNextWaypoint(
                startX = entity.x,
                startY = entity.y,
                startZ = entity.z,
                targetX = targetX,
                targetY = targetY,
                targetZ = targetZ,
                agentRadius = entity.hitboxWidth * 0.5,
                agentHeight = entity.hitboxHeight,
                agentMaxClimb = NAVIGATION_MAX_CLIMB_HEIGHT
            ) ?: return@repeat
            val dx = targetX - entity.x
            val dz = targetZ - entity.z
            val score = (dx * dx) + (dz * dz)
            val currentBest = bestCandidate
            if (currentBest == null || score > currentBest.score) {
                bestCandidate = WanderTargetCandidate(
                    targetX = targetX,
                    targetY = targetY,
                    targetZ = targetZ,
                    firstWaypoint = waypoint,
                    score = score
                )
            }
        }
        val selected = bestCandidate ?: return false
        brain.targetX = selected.targetX
        brain.targetY = selected.targetY
        brain.targetZ = selected.targetZ
        brain.hasTarget = true
        brain.waypointX = selected.firstWaypoint.x
        brain.waypointY = selected.firstWaypoint.y
        brain.waypointZ = selected.firstWaypoint.z
        brain.hasWaypoint = true
        brain.repathFailureStreak = 0
        brain.retargetCooldownSeconds = 0.0
        brain.repathCooldownSeconds = AI_PANIC_REPATH_INTERVAL_SECONDS
        brain.panicActive = true
        return true
    }

    private data class WaterTarget(val x: Double, val y: Double, val z: Double, val distSq: Double)

    private fun locateClosestWaterTarget(entity: AnimalEntity): WaterTarget? {
        val originX = floor(entity.x).toInt()
        val originY = floor(entity.y).toInt()
        val originZ = floor(entity.z).toInt()
        var best: WaterTarget? = null
        for (dy in -WATER_SEARCH_VERTICAL_RANGE..WATER_SEARCH_VERTICAL_RANGE) {
            val by = originY + dy
            if (by !in -64..WORLD_MAX_Y) continue
            for (dx in -WATER_SEARCH_HORIZONTAL_RANGE..WATER_SEARCH_HORIZONTAL_RANGE) {
                val bx = originX + dx
                for (dz in -WATER_SEARCH_HORIZONTAL_RANGE..WATER_SEARCH_HORIZONTAL_RANGE) {
                    val bz = originZ + dz
                    val stateId = blockStateAt(bx, by, bz)
                    if (stateId <= 0) continue
                    val blockKey = BlockStateRegistry.parsedState(stateId)?.blockKey ?: continue
                    if (blockKey != "minecraft:water") continue
                    val targetX = bx + 0.5
                    val targetY = by.toDouble()
                    val targetZ = bz + 0.5
                    val pdx = targetX - entity.x
                    val pdy = targetY - entity.y
                    val pdz = targetZ - entity.z
                    val distSq = (pdx * pdx) + (pdy * pdy) + (pdz * pdz)
                    val currentBest = best
                    if (currentBest == null || distSq < currentBest.distSq) {
                        val waypoint = navigator.findNextWaypoint(
                            startX = entity.x,
                            startY = entity.y,
                            startZ = entity.z,
                            targetX = targetX,
                            targetY = targetY,
                            targetZ = targetZ,
                            agentRadius = entity.hitboxWidth * 0.5,
                            agentHeight = entity.hitboxHeight,
                            agentMaxClimb = NAVIGATION_MAX_CLIMB_HEIGHT
                        ) ?: continue
                        best = WaterTarget(waypoint.x, waypoint.y, waypoint.z, distSq)
                    }
                }
            }
        }
        return best
    }

    private fun nearestTemptSource(entity: AnimalEntity): AnimalTemptSource? {
        if (entity.kind != AnimalKind.PIG) return null
        val sources = temptSources
        if (sources.isEmpty()) return null
        var best: AnimalTemptSource? = null
        var bestDistSq = Double.MAX_VALUE
        for (source in sources) {
            val dx = source.x - entity.x
            val dy = source.y - entity.y
            val dz = source.z - entity.z
            val distSq = (dx * dx) + (dy * dy) + (dz * dz)
            val maxRangeSq = source.range * source.range
            if (distSq > maxRangeSq) continue
            if (!canSeeTemptSourceByFacing(entity, source)) continue
            if (!hasLineOfSightToTemptSource(entity, source)) continue
            if (distSq < bestDistSq) {
                bestDistSq = distSq
                best = source
            }
        }
        return best
    }

    private fun canSeeTemptSourceByFacing(entity: AnimalEntity, source: AnimalTemptSource): Boolean {
        val dx = source.x - entity.x
        val dz = source.z - entity.z
        val horizontalSq = (dx * dx) + (dz * dz)
        if (horizontalSq <= TEMPT_FACING_EPSILON_SQ) return true
        val targetYaw = (Math.toDegrees(kotlin.math.atan2(dz, dx)).toFloat() - 90.0f).normalizeYaw()
        val yawDelta = abs(wrapDegrees(targetYaw - entity.yaw))
        return yawDelta <= TEMPT_VIEW_HALF_ANGLE_DEGREES
    }

    private fun hasLineOfSightToTemptSource(entity: AnimalEntity, source: AnimalTemptSource): Boolean {
        val startX = entity.x
        val startY = entity.y + (entity.hitboxHeight * PIG_EYE_HEIGHT_RATIO)
        val startZ = entity.z
        val dx = source.x - startX
        val dy = source.y - startY
        val dz = source.z - startZ
        val distance = sqrt((dx * dx) + (dy * dy) + (dz * dz))
        if (distance <= TEMPT_LOS_EPSILON) return true
        val steps = maxOf(1, ceil(distance / TEMPT_LOS_STEP_BLOCKS).toInt())
        for (i in 1 until steps) {
            val t = i.toDouble() / steps.toDouble()
            val sampleX = startX + (dx * t)
            val sampleY = startY + (dy * t)
            val sampleZ = startZ + (dz * t)
            val blockX = floor(sampleX).toInt()
            val blockY = floor(sampleY).toInt()
            val blockZ = floor(sampleZ).toInt()
            val stateId = blockStateAt(blockX, blockY, blockZ)
            if (stateId <= 0) continue
            if (collisionBoxesForState(stateId).isNotEmpty()) {
                return false
            }
        }
        return true
    }

    private fun assignNewTarget(entity: AnimalEntity, brain: AnimalBrain) {
        val random = ThreadLocalRandom.current()
        val horizontalRange = AI_WANDER_TARGET_RADIUS
        val verticalRange = AI_WANDER_TARGET_VERTICAL_RANGE
        var bestCandidate: WanderTargetCandidate? = null
        repeat(AI_TARGET_ATTEMPTS) {
            val offsetX = random.nextInt(horizontalRange * 2 + 1) - horizontalRange
            val offsetZ = random.nextInt(horizontalRange * 2 + 1) - horizontalRange
            if (offsetX == 0 && offsetZ == 0) return@repeat
            val offsetY = random.nextInt(verticalRange * 2 + 1) - verticalRange
            val targetX = entity.x + offsetX.toDouble()
            val targetY = entity.y + offsetY.toDouble()
            val targetZ = entity.z + offsetZ.toDouble()
            if (brain.hasLastWanderTarget) {
                val pdx = targetX - brain.lastWanderTargetX
                val pdz = targetZ - brain.lastWanderTargetZ
                if ((pdx * pdx) + (pdz * pdz) < WANDER_TARGET_REPEAT_MIN_DISTANCE_SQ) {
                    return@repeat
                }
            }
            val waypoint = navigator.findNextWaypoint(
                startX = entity.x,
                startY = entity.y,
                startZ = entity.z,
                targetX = targetX,
                targetY = targetY,
                targetZ = targetZ,
                agentRadius = entity.hitboxWidth * 0.5,
                agentHeight = entity.hitboxHeight,
                agentMaxClimb = NAVIGATION_MAX_CLIMB_HEIGHT
            ) ?: return@repeat
            val score = walkTargetValue(targetX, targetY, targetZ) + random.nextDouble() * 0.01
            val currentBest = bestCandidate
            if (currentBest == null || score > currentBest.score) {
                bestCandidate = WanderTargetCandidate(
                    targetX = targetX,
                    targetY = targetY,
                    targetZ = targetZ,
                    firstWaypoint = waypoint,
                    score = score
                )
            }
        }
        val selected = bestCandidate
        if (selected != null) {
            brain.targetX = selected.targetX
            brain.targetY = selected.targetY
            brain.targetZ = selected.targetZ
            brain.hasTarget = true
            brain.lastWanderTargetX = selected.targetX
            brain.lastWanderTargetZ = selected.targetZ
            brain.hasLastWanderTarget = true
            brain.waypointX = selected.firstWaypoint.x
            brain.waypointY = selected.firstWaypoint.y
            brain.waypointZ = selected.firstWaypoint.z
            brain.hasWaypoint = true
            brain.repathFailureStreak = 0
            brain.retargetCooldownSeconds = 0.0
            brain.repathCooldownSeconds = AI_REPATH_INTERVAL_SECONDS
            return
        }
        brain.hasTarget = false
        brain.hasWaypoint = false
        brain.retargetCooldownSeconds = randomWanderPauseSeconds(
            WANDER_TARGET_FAIL_DELAY_MIN_SECONDS,
            WANDER_TARGET_FAIL_DELAY_MAX_SECONDS
        )
        brain.repathFailureStreak = (brain.repathFailureStreak + 1).coerceAtMost(MAX_REPATH_FAILURE_STREAK)
        brain.repathCooldownSeconds = repathBackoffSeconds(
            baseSeconds = AI_REPATH_INTERVAL_SECONDS,
            failureStreak = brain.repathFailureStreak
        )
    }

    private fun randomWanderPauseSeconds(minSeconds: Double, maxSeconds: Double): Double {
        if (maxSeconds <= minSeconds) return minSeconds.coerceAtLeast(0.0)
        return ThreadLocalRandom.current().nextDouble(minSeconds, maxSeconds).coerceAtLeast(0.0)
    }

    private fun tickRandomLookAround(entity: AnimalEntity, brain: AnimalBrain, deltaSeconds: Double) {
        if (brain.lookAroundSecondsRemaining > 0.0) {
            brain.lookAroundSecondsRemaining = (brain.lookAroundSecondsRemaining - deltaSeconds).coerceAtLeast(0.0)
        }
        if (brain.lookAroundSecondsRemaining <= 0.0) {
            val simulatedTicks = (deltaSeconds * TICKS_PER_SECOND).coerceAtLeast(0.0)
            val perTickMiss = (1.0 - RANDOM_LOOK_AROUND_TICK_START_CHANCE).coerceIn(0.0, 1.0)
            val startChance = (1.0 - perTickMiss.pow(simulatedTicks)).coerceIn(0.0, 1.0)
            val random = ThreadLocalRandom.current()
            if (random.nextDouble() >= startChance) return
            val angle = random.nextDouble(0.0, Math.PI * 2.0)
            brain.lookAroundRelX = kotlin.math.cos(angle)
            brain.lookAroundRelZ = kotlin.math.sin(angle)
            val durationTicks = RANDOM_LOOK_AROUND_MIN_TICKS + random.nextInt(RANDOM_LOOK_AROUND_DURATION_SPREAD_TICKS)
            brain.lookAroundSecondsRemaining = durationTicks.toDouble() / TICKS_PER_SECOND
        }
        brain.lookTargetX = entity.x + brain.lookAroundRelX
        brain.lookTargetY = entity.y + (entity.hitboxHeight * PIG_EYE_HEIGHT_RATIO)
        brain.lookTargetZ = entity.z + brain.lookAroundRelZ
        brain.hasLookTarget = true
        brain.lookYawMaxRotSpeed = RANDOM_LOOK_YAW_SPEED_DEGREES_PER_TICK
        brain.lookPitchMaxRotAngle = MAX_HEAD_PITCH_DEGREES
        brain.lookAtCooldownSeconds = LOOK_CONTROL_TARGET_COOLDOWN_SECONDS
    }

    private fun tickLookAtNearbyPlayer(entity: AnimalEntity, brain: AnimalBrain, deltaSeconds: Double): Boolean {
        val nearest = nearestLookSource(entity) ?: run {
            brain.lookAtPlayerSecondsRemaining = 0.0
            return false
        }
        if (brain.lookAtPlayerSecondsRemaining <= 0.0) {
            val simulatedTicks = (deltaSeconds * TICKS_PER_SECOND).coerceAtLeast(0.0)
            val perTickMiss = (1.0 - LOOK_AT_PLAYER_TICK_START_CHANCE).coerceIn(0.0, 1.0)
            val startChance = (1.0 - perTickMiss.pow(simulatedTicks)).coerceIn(0.0, 1.0)
            if (ThreadLocalRandom.current().nextDouble() >= startChance) return false
            val durationTicks = LOOK_AT_PLAYER_MIN_TICKS + ThreadLocalRandom.current().nextInt(LOOK_AT_PLAYER_DURATION_SPREAD_TICKS)
            brain.lookAtPlayerSecondsRemaining = durationTicks.toDouble() / TICKS_PER_SECOND
        }
        brain.lookAtPlayerSecondsRemaining = (brain.lookAtPlayerSecondsRemaining - deltaSeconds).coerceAtLeast(0.0)
        brain.lookTargetX = nearest.x
        brain.lookTargetY = nearest.y
        brain.lookTargetZ = nearest.z
        brain.hasLookTarget = true
        brain.lookYawMaxRotSpeed = RANDOM_LOOK_YAW_SPEED_DEGREES_PER_TICK
        brain.lookPitchMaxRotAngle = MAX_HEAD_PITCH_DEGREES
        brain.lookAtCooldownSeconds = LOOK_CONTROL_TARGET_COOLDOWN_SECONDS
        return true
    }

    private fun nearestLookSource(entity: AnimalEntity): AnimalLookSource? {
        if (lookSources.isEmpty()) return null
        val maxDistanceSq = LOOK_AT_PLAYER_RANGE_BLOCKS * LOOK_AT_PLAYER_RANGE_BLOCKS
        val eyeY = entity.y + (entity.hitboxHeight * PIG_EYE_HEIGHT_RATIO)
        var best: AnimalLookSource? = null
        var bestDistSq = Double.POSITIVE_INFINITY
        for (source in lookSources) {
            val dx = source.x - entity.x
            val dy = source.y - eyeY
            val dz = source.z - entity.z
            val distSq = (dx * dx) + (dy * dy) + (dz * dz)
            if (distSq > maxDistanceSq || distSq >= bestDistSq) continue
            bestDistSq = distSq
            best = source
        }
        return best
    }

    private fun repathBackoffSeconds(baseSeconds: Double, failureStreak: Int): Double {
        val exponent = failureStreak.coerceIn(0, MAX_REPATH_BACKOFF_SHIFT)
        val scale = 1 shl exponent
        return (baseSeconds * scale).coerceAtMost(MAX_REPATH_BACKOFF_SECONDS)
    }

    private fun blendForTickScale(perTickBlend: Double, tickScale: Double): Double {
        if (tickScale <= 0.0 || !tickScale.isFinite()) return 0.0
        val clamped = perTickBlend.coerceIn(0.0, 1.0)
        if (clamped <= 0.0) return 0.0
        if (clamped >= 1.0) return 1.0
        return 1.0 - (1.0 - clamped).pow(tickScale)
    }

    private fun walkTargetValue(x: Double, y: Double, z: Double): Double {
        val blockX = floor(x).toInt()
        val blockY = floor(y).toInt()
        val blockZ = floor(z).toInt()
        val belowState = blockStateAt(blockX, blockY - 1, blockZ)
        val belowKey = BlockStateRegistry.parsedState(belowState)?.blockKey
        if (belowKey == "minecraft:grass_block") return 10.0

        val raw = rawBrightnessAt(blockX, blockY, blockZ).coerceIn(0, 15).toDouble()
        val normalized = raw / 15.0
        val lightMagic = if (normalized >= 1.0) 1.0 else normalized / (4.0 - 3.0 * normalized)
        return lightMagic - 0.5
    }

    private fun applyBodyAndHeadRotationControl(
        entity: AnimalEntity,
        brain: AnimalBrain,
        oldX: Double,
        oldZ: Double,
        tickScale: Double
    ) {
        if (!brain.rotationInitialized) {
            brain.bodyYaw = entity.yaw
            brain.lastStableHeadYaw = entity.headYaw
            brain.headStableSeconds = 0.0
            brain.rotationInitialized = true
        }

        // Vanilla LookControl semantics:
        // reset X rot each tick; when look target is active, rotate toward target for cooldown ticks.
        entity.pitch = 0f
        if (brain.hasLookTarget && brain.lookAtCooldownSeconds > 0.0) {
            brain.lookAtCooldownSeconds = (brain.lookAtCooldownSeconds - (tickScale / TICKS_PER_SECOND)).coerceAtLeast(0.0)
            val dx = brain.lookTargetX - entity.x
            val dy = brain.lookTargetY - (entity.y + entity.hitboxHeight * PIG_EYE_HEIGHT_RATIO)
            val dz = brain.lookTargetZ - entity.z
            val horizontal = sqrt((dx * dx) + (dz * dz))
            if (horizontal > 1.0e-5 || kotlin.math.abs(dy) > 1.0e-5) {
                val targetHeadYaw = (Math.toDegrees(kotlin.math.atan2(dz, dx)).toFloat() - 90.0f).normalizeYaw()
                entity.headYaw = rotLerp(
                    entity.headYaw,
                    targetHeadYaw,
                    brain.lookYawMaxRotSpeed * tickScale.toFloat()
                )
                val targetPitch = (-Math.toDegrees(kotlin.math.atan2(dy, horizontal.coerceAtLeast(1.0e-6))).toFloat())
                    .coerceIn(-brain.lookPitchMaxRotAngle, brain.lookPitchMaxRotAngle)
                entity.pitch = rotLerp(
                    entity.pitch,
                    targetPitch,
                    brain.lookPitchMaxRotAngle * tickScale.toFloat()
                )
            }
        } else {
            entity.headYaw = rotateTowards(entity.headYaw, brain.bodyYaw, HEAD_ROT_SPEED_DEGREES_PER_TICK * tickScale.toFloat())
        }

        // Vanilla BodyRotationControl semantics.
        val maxHeadYaw = if (brain.hasLookTarget) TEMPT_MAX_HEAD_YAW_DEGREES else MAX_HEAD_YAW_DEGREES
        val movedX = entity.x - oldX
        val movedZ = entity.z - oldZ
        val isMoving = (movedX * movedX) + (movedZ * movedZ) > MIN_MOVE_SPEED_SQR
        if (isMoving) {
            brain.bodyYaw = entity.yaw
            entity.headYaw = rotateIfNecessary(entity.headYaw, brain.bodyYaw, maxHeadYaw)
            brain.lastStableHeadYaw = entity.headYaw
            brain.headStableSeconds = 0.0
        } else {
            val headDelta = abs(wrapDegrees(entity.headYaw - brain.lastStableHeadYaw))
            if (headDelta > HEAD_STABLE_ANGLE_DEGREES) {
                brain.headStableSeconds = 0.0
                brain.lastStableHeadYaw = entity.headYaw
                brain.bodyYaw = rotateIfNecessary(brain.bodyYaw, entity.headYaw, maxHeadYaw)
            } else {
                brain.headStableSeconds += tickScale / TICKS_PER_SECOND
                if (brain.headStableSeconds > BODY_FACE_FORWARD_DELAY_SECONDS) {
                    val elapsed = brain.headStableSeconds - BODY_FACE_FORWARD_DELAY_SECONDS
                    val progress = (elapsed / BODY_FACE_FORWARD_DURATION_SECONDS).toFloat().coerceIn(0f, 1f)
                    val maxDiff = maxHeadYaw * (1.0f - progress)
                    brain.bodyYaw = rotateIfNecessary(brain.bodyYaw, entity.headYaw, maxDiff)
                }
            }
        }

        // Vanilla LookControl.clampHeadRotationToBody() when navigating.
        if (brain.hasWaypoint) {
            entity.headYaw = rotateIfNecessary(entity.headYaw, brain.bodyYaw, maxHeadYaw)
        }
    }

    private fun rotateIfNecessary(rotationToAdjust: Float, actualRotation: Float, maxDifference: Float): Float {
        val delta = wrapDegrees(actualRotation - rotationToAdjust)
        val clamped = delta.coerceIn(-maxDifference, maxDifference)
        return actualRotation - clamped
    }

    private fun rotLerp(sourceAngle: Float, targetAngle: Float, maximumChange: Float): Float {
        val delta = wrapDegrees(targetAngle - sourceAngle).coerceIn(-maximumChange, maximumChange)
        return (sourceAngle + delta).normalizeYaw()
    }

    private fun rotateTowards(current: Float, target: Float, stepSize: Float): Float {
        val diff = wrapDegrees(target - current)
        val targetAngle = current + diff
        return approach(current, targetAngle, kotlin.math.abs(stepSize)).normalizeYaw()
    }

    private fun approach(value: Float, limit: Float, stepSize: Float): Float {
        return if (value < limit) {
            (value + stepSize).coerceAtMost(limit)
        } else {
            (value - stepSize).coerceAtLeast(limit)
        }
    }

    private fun wrapDegrees(value: Float): Float {
        var out = value % 360f
        if (out >= 180f) out -= 360f
        if (out < -180f) out += 360f
        return out
    }

    private fun Float.normalizeYaw(): Float {
        var out = this % 360f
        if (out < 0f) out += 360f
        return out
    }

    private fun findWaypointWithDirectionalFallback(
        entity: AnimalEntity,
        brain: AnimalBrain,
        targetX: Double,
        targetY: Double,
        targetZ: Double
    ): RepathWaypointResult? {
        val baseDx = targetX - entity.x
        val baseDz = targetZ - entity.z
        val baseLen = sqrt((baseDx * baseDx) + (baseDz * baseDz))
        val dirX: Double
        val dirZ: Double
        if (baseLen > POSITION_EPSILON) {
            dirX = baseDx / baseLen
            dirZ = baseDz / baseLen
        } else {
            val yawRad = Math.toRadians(entity.yaw.toDouble())
            dirX = -kotlin.math.sin(yawRad)
            dirZ = kotlin.math.cos(yawRad)
        }
        val candidateDistance = baseLen.coerceIn(AI_MULTI_REPATH_MIN_DISTANCE, AI_MULTI_REPATH_MAX_DISTANCE)
        val randomStart = ThreadLocalRandom.current().nextInt(REPATH_DIRECTION_OFFSETS.size)
        val startOffset = (brain.repathDirectionCursor + randomStart) % REPATH_DIRECTION_OFFSETS.size
        for (i in 0 until REPATH_DIRECTION_OFFSETS.size) {
            val offsetIndex = (startOffset + i) % REPATH_DIRECTION_OFFSETS.size
            val angleOffset = REPATH_DIRECTION_OFFSETS[offsetIndex]
            val candidateTargetX: Double
            val candidateTargetY: Double
            val candidateTargetZ: Double
            if (offsetIndex == 0) {
                candidateTargetX = targetX
                candidateTargetY = targetY
                candidateTargetZ = targetZ
            } else {
                val rotatedX = (dirX * kotlin.math.cos(angleOffset)) - (dirZ * kotlin.math.sin(angleOffset))
                val rotatedZ = (dirX * kotlin.math.sin(angleOffset)) + (dirZ * kotlin.math.cos(angleOffset))
                candidateTargetX = entity.x + (rotatedX * candidateDistance)
                candidateTargetY = entity.y
                candidateTargetZ = entity.z + (rotatedZ * candidateDistance)
            }
            val waypoint = navigator.findNextWaypoint(
                startX = entity.x,
                startY = entity.y,
                startZ = entity.z,
                targetX = candidateTargetX,
                targetY = candidateTargetY,
                targetZ = candidateTargetZ,
                agentRadius = entity.hitboxWidth * 0.5,
                agentHeight = entity.hitboxHeight,
                agentMaxClimb = NAVIGATION_MAX_CLIMB_HEIGHT
            ) ?: continue
            brain.repathDirectionCursor = (offsetIndex + 1) % REPATH_DIRECTION_OFFSETS.size
            return RepathWaypointResult(
                waypoint = waypoint,
                selectedTargetX = candidateTargetX,
                selectedTargetY = candidateTargetY,
                selectedTargetZ = candidateTargetZ
            )
        }
        return null
    }

    private fun collidesAt(entity: AnimalEntity, x: Double, y: Double, z: Double): Boolean {
        val halfWidth = entity.hitboxWidth * 0.5
        val minX = x - halfWidth
        val maxX = x + halfWidth
        val minY = y
        val maxY = y + entity.hitboxHeight
        val minZ = z - halfWidth
        val maxZ = z + halfWidth

        val startX = floor(minX).toInt()
        val endX = floor(maxX - 1.0e-7).toInt()
        val startY = floor(minY).toInt()
        val endY = floor(maxY - 1.0e-7).toInt()
        val startZ = floor(minZ).toInt()
        val endZ = floor(maxZ - 1.0e-7).toInt()

        for (bx in startX..endX) {
            for (by in startY..endY) {
                for (bz in startZ..endZ) {
                    val stateId = blockStateAt(bx, by, bz)
                    val boxes = collisionBoxesForState(stateId)
                    if (boxes.isEmpty()) continue
                    val blockBaseX = bx.toDouble()
                    val blockBaseY = by.toDouble()
                    val blockBaseZ = bz.toDouble()
                    for (box in boxes) {
                        val boxMinX = blockBaseX + box.minX
                        val boxMinY = blockBaseY + box.minY
                        val boxMinZ = blockBaseZ + box.minZ
                        val boxMaxX = blockBaseX + box.maxX
                        val boxMaxY = blockBaseY + box.maxY
                        val boxMaxZ = blockBaseZ + box.maxZ
                        if (aabbIntersects(
                                minX, minY, minZ,
                                maxX, maxY, maxZ,
                                boxMinX, boxMinY, boxMinZ,
                                boxMaxX, boxMaxY, boxMaxZ
                            )
                        ) {
                            return true
                        }
                    }
                }
            }
        }
        return false
    }

    private fun movementSubsteps(dx: Double, dy: Double, dz: Double): Int {
        val maxAbs = maxOf(abs(dx), abs(dy), abs(dz))
        if (maxAbs <= 0.0) return 1
        val required = ceil(maxAbs / MAX_STEP_DISTANCE).toLong()
        return required.coerceAtLeast(1L).coerceAtMost(Int.MAX_VALUE.toLong()).toInt()
    }

    private fun collisionBoxesForState(stateId: Int): Array<CollisionBox> {
        if (stateId <= 0) return EMPTY_COLLISION_BOXES
        return collisionBoxesByState.computeIfAbsent(stateId) { id ->
            val parsed = BlockStateRegistry.parsedState(id) ?: return@computeIfAbsent EMPTY_COLLISION_BOXES
            val blockKey = parsed.blockKey
            if (blockKey in PASS_THROUGH_BLOCK_KEYS) return@computeIfAbsent EMPTY_COLLISION_BOXES
            val resolved = BlockCollisionRegistry.boxesForStateId(id, parsed)
            if (resolved != null) {
                if (resolved.isEmpty()) return@computeIfAbsent EMPTY_COLLISION_BOXES
                return@computeIfAbsent Array(resolved.size) { i ->
                    val b = resolved[i]
                    CollisionBox(b.minX, b.minY, b.minZ, b.maxX, b.maxY, b.maxZ)
                }
            }
            FULL_BLOCK_COLLISION_BOXES
        }
    }

    private fun tickHurtInvulnerability(entity: AnimalEntity, tickScale: Double) {
        if (entity.hurtInvulnerableSeconds <= 0.0) return
        val deltaSeconds = tickScale / TICKS_PER_SECOND
        entity.hurtInvulnerableSeconds = (entity.hurtInvulnerableSeconds - deltaSeconds).coerceAtLeast(0.0)
        if (entity.hurtInvulnerableSeconds <= 0.0) {
            entity.lastHurtAmount = 0f
        }
    }

    private fun effectiveDamageAfterInvulnerability(entity: AnimalEntity, incomingAmount: Float): Float {
        if (entity.hurtInvulnerableSeconds <= 0.0) return incomingAmount
        return 0f
    }

    private fun isPanicCause(cause: AnimalDamageCause): Boolean {
        // Vanilla EscapeDangerGoal checks DamageTypeTags.PANIC_CAUSES via getRecentDamageSource().
        // Our local damage model keeps coarse causes, so treat direct attacks/generic hazard as panic causes.
        return cause != AnimalDamageCause.FALL
    }

    private fun aabbIntersects(
        aMinX: Double,
        aMinY: Double,
        aMinZ: Double,
        aMaxX: Double,
        aMaxY: Double,
        aMaxZ: Double,
        bMinX: Double,
        bMinY: Double,
        bMinZ: Double,
        bMaxX: Double,
        bMaxY: Double,
        bMaxZ: Double
    ): Boolean {
        return aMinX < bMaxX - COLLISION_EPSILON &&
            aMaxX > bMinX + COLLISION_EPSILON &&
            aMinY < bMaxY - COLLISION_EPSILON &&
            aMaxY > bMinY + COLLISION_EPSILON &&
            aMinZ < bMaxZ - COLLISION_EPSILON &&
            aMaxZ > bMinZ + COLLISION_EPSILON
    }

    private fun addToChunkIndex(chunkPos: ChunkPos, entityId: Int) {
        chunkIndex
            .computeIfAbsent(chunkPos) { ConcurrentHashMap.newKeySet() }
            .add(entityId)
    }

    private fun removeFromChunkIndex(chunkPos: ChunkPos, entityId: Int) {
        val set = chunkIndex[chunkPos] ?: return
        set.remove(entityId)
        if (set.isEmpty()) {
            chunkIndex.remove(chunkPos, set)
        }
    }

    private fun moveChunkIndex(entityId: Int, oldChunk: ChunkPos, newChunk: ChunkPos) {
        removeFromChunkIndex(oldChunk, entityId)
        addToChunkIndex(newChunk, entityId)
    }

    private fun toSnapshot(entity: AnimalEntity): AnimalSnapshot {
        return AnimalSnapshot(
            entityId = entity.entityId,
            uuid = entity.uuid,
            kind = entity.kind,
            x = entity.x,
            y = entity.y,
            z = entity.z,
            vx = entity.vx,
            vy = entity.vy,
            vz = entity.vz,
            yaw = entity.yaw,
            headYaw = entity.headYaw,
            pitch = entity.pitch,
            onGround = entity.onGround,
            health = entity.health,
            maxHealth = entity.maxHealth,
            hitboxWidth = entity.hitboxWidth,
            hitboxHeight = entity.hitboxHeight,
            chunkPos = ChunkPos(entity.chunkX, entity.chunkZ)
        )
    }

    private fun chunkXFromBlockX(x: Double): Int = floor(x / 16.0).toInt()
    private fun chunkZFromBlockZ(z: Double): Int = floor(z / 16.0).toInt()

    companion object {
        private val logger = LoggerFactory.getLogger(AnimalSystem::class.java)
        private val json = Json { ignoreUnknownKeys = true }
        private const val SAFE_FALL_DISTANCE_BLOCKS = 3.0
        private const val TICKS_PER_SECOND = 20.0
        private const val GRAVITY_PER_TICK = 0.08
        // Vanilla living-entity horizontal damping:
        // air: 0.91, ground (default block slipperiness 0.6): 0.91 * 0.6 = 0.546
        private const val AIR_DRAG_PER_TICK = 0.91
        private const val GROUND_DRAG_PER_TICK = 0.546
        // Vanilla vertical velocity damping in LivingEntity travel.
        private const val VERTICAL_DRAG_PER_TICK = 0.98
        private const val POSITION_EPSILON = 1.0e-4
        private const val VELOCITY_EPSILON = 1.0e-4
        private const val MAX_STEP_DISTANCE = 0.5
        private const val MAX_STEP_UP_HEIGHT = 0.5
        private const val MAX_JUMP_STEP_HEIGHT = 1.0
        private const val NAVIGATION_MAX_CLIMB_HEIGHT = 1.0
        private const val MAX_STEP_DOWN_HEIGHT = 1.0
        private const val STEP_DOWN_SAMPLE_HEIGHT = 0.05
        private const val GROUND_PROBE_EPSILON = 1.0e-3
        // Vanilla Attributes.JUMP_STRENGTH default for most mobs (including pig) is 0.42.
        private const val JUMP_VELOCITY_PER_TICK = 0.42
        private const val HURT_INVULNERABILITY_SECONDS = 0.5
        private const val FIRE_IGNITE_SECONDS = 8.0
        private const val FIRE_DAMAGE_INTERVAL_SECONDS = 1.0
        private const val FIRE_DAMAGE_AMOUNT = 1.0f
        private const val FIRE_DAMAGE_TIME_EPSILON = 1.0e-9
        // Vanilla LivingEntity#getRecentDamageSource() keeps source for 40 ticks.
        private const val PANIC_RECENT_DAMAGE_SECONDS = 40.0 / TICKS_PER_SECOND
        // Vanilla Pig goal speed modifiers:
        // walk=1.0, tempt=1.2, panic=1.25 with movement_speed attribute 0.25
        private const val ANIMAL_BASE_SPEED = 0.25
        private const val ANIMAL_WALK_SPEED = ANIMAL_BASE_SPEED * 1.0
        private const val ANIMAL_TEMPT_SPEED = ANIMAL_BASE_SPEED * 1.2
        private const val ANIMAL_PANIC_SPEED = ANIMAL_BASE_SPEED * 1.25
        // Vanilla Pig uses movement_speed(0.25) * goal speed modifier directly
        // (TemptGoal=1.2, PanicGoal=1.25). Keep scale neutral.
        private const val ANIMAL_NAV_SPEED_TO_VELOCITY_SCALE = 1.0
        private const val AI_STEER_BLEND = 0.35
        // Our kinematics apply velocity more directly than vanilla MoveControl+travel,
        // so follow/panic uses a moderated blend to match vanilla chase pace.
        private const val AI_CHASE_STEER_BLEND = 0.55
        private const val AI_RIDE_STEER_BLEND = 1.0
        // Keep ride baseline aligned with pig follow/tempt movement in this simulation model
        // to avoid velocity discontinuity between AI-driven and rider-driven phases.
        private const val ANIMAL_RIDE_BASE_SPEED = ANIMAL_TEMPT_SPEED
        private const val MOVE_CONTROL_MAX_TURN_DEGREES_PER_TICK = 90f
        private const val HEAD_ROT_SPEED_DEGREES_PER_TICK = 10f
        // Vanilla TemptGoal uses LookControl#setLookAt with yaw limit maxHeadYRot + 20.
        private const val TEMPT_LOOK_YAW_SPEED_DEGREES_PER_TICK = 95f
        private const val TEMPT_LOOK_PITCH_SPEED_DEGREES_PER_TICK = 40f
        private const val LOOK_CONTROL_TARGET_COOLDOWN_TICKS = 2
        private const val LOOK_CONTROL_TARGET_COOLDOWN_SECONDS = LOOK_CONTROL_TARGET_COOLDOWN_TICKS / TICKS_PER_SECOND
        private const val MAX_HEAD_YAW_DEGREES = 75f
        private const val TEMPT_MAX_HEAD_YAW_DEGREES = 95f
        private const val MAX_HEAD_PITCH_DEGREES = 40f
        // Custom cone gate (vanilla TemptGoal has no explicit facing-angle gate on acquisition).
        private const val TEMPT_VIEW_HALF_ANGLE_DEGREES = 90f
        private const val TEMPT_FACING_EPSILON_SQ = 1.0e-6
        private const val PIG_EYE_HEIGHT_RATIO = 0.85
        private const val TEMPT_LOS_STEP_BLOCKS = 0.25
        private const val TEMPT_LOS_EPSILON = 1.0e-6
        private const val RANDOM_LOOK_AROUND_TICK_START_CHANCE = 0.02
        private const val RANDOM_LOOK_AROUND_MIN_TICKS = 20
        private const val RANDOM_LOOK_AROUND_DURATION_SPREAD_TICKS = 20
        // Vanilla PigEntity goal: new LookAtEntityGoal(this, PlayerEntity.class, 6.0F)
        // with LookAtEntityGoal default chance=0.02, duration=40+random(40) ticks.
        private const val LOOK_AT_PLAYER_RANGE_BLOCKS = 6.0
        private const val LOOK_AT_PLAYER_TICK_START_CHANCE = 0.02
        private const val LOOK_AT_PLAYER_MIN_TICKS = 40
        private const val LOOK_AT_PLAYER_DURATION_SPREAD_TICKS = 40
        private const val RANDOM_LOOK_YAW_SPEED_DEGREES_PER_TICK = HEAD_ROT_SPEED_DEGREES_PER_TICK
        private const val HEAD_STABLE_ANGLE_DEGREES = 15f
        private const val BODY_FACE_FORWARD_DELAY_TICKS = 10
        private const val BODY_FACE_FORWARD_DURATION_TICKS = 10
        private const val BODY_FACE_FORWARD_DELAY_SECONDS = BODY_FACE_FORWARD_DELAY_TICKS / TICKS_PER_SECOND
        private const val BODY_FACE_FORWARD_DURATION_SECONDS = BODY_FACE_FORWARD_DURATION_TICKS / TICKS_PER_SECOND
        private const val MIN_MOVE_SPEED_SQR = 2.5000003E-7
        private const val ROTATION_EPSILON_DEGREES = 0.5f
        private const val AI_REPATH_INTERVAL_SECONDS = 0.20
        private const val AI_TEMPT_REPATH_INTERVAL_SECONDS = 1.0 / 20.0
        private const val VANILLA_WANDER_TICK_CHANCE_BASE = 60
        private const val AI_WANDER_TARGET_RADIUS = 10
        private const val AI_WANDER_TARGET_VERTICAL_RANGE = 7
        // Vanilla EscapeDangerGoal#findTarget delegates to NoPenaltyTargeting.find(mob, 5, 4).
        private const val PANIC_TARGET_HORIZONTAL_RANGE = 5
        private const val PANIC_TARGET_VERTICAL_RANGE = 4
        private const val PANIC_TARGET_ATTEMPTS = 10
        private const val AI_PANIC_REPATH_INTERVAL_SECONDS = 1.0 / 20.0
        private const val WATER_SEARCH_HORIZONTAL_RANGE = 5
        private const val WATER_SEARCH_VERTICAL_RANGE = 1
        private const val AI_TARGET_ATTEMPTS = 10
        private const val TARGET_REACHED_DISTANCE_SQ = 1.4 * 1.4
        private const val WAYPOINT_REACHED_DISTANCE_SQ = 0.7 * 0.7
        private const val TEMPT_WAYPOINT_REACHED_DISTANCE_SQ = 0.45 * 0.45
        private const val TEMPT_STOP_DISTANCE_SQ = 2.5 * 2.5
        private const val AI_TEMPT_REPATH_RETRY_SECONDS = 1.0 / 20.0
        private const val MAX_REPATH_FAILURE_STREAK = 8
        private const val MAX_REPATH_BACKOFF_SHIFT = 6
        private const val MAX_REPATH_BACKOFF_SECONDS = 3.0
        private const val AI_MULTI_REPATH_MIN_DISTANCE = 2.5
        private const val AI_MULTI_REPATH_MAX_DISTANCE = 10.0
        private const val WANDER_TARGET_REPEAT_MIN_DISTANCE_SQ = 9.0 // 3 blocks
        private const val WANDER_RESTART_DELAY_MIN_SECONDS = 1.2
        private const val WANDER_RESTART_DELAY_MAX_SECONDS = 3.2
        private const val WANDER_REPATH_FAIL_DELAY_MIN_SECONDS = 1.5
        private const val WANDER_REPATH_FAIL_DELAY_MAX_SECONDS = 4.0
        private const val WANDER_TARGET_FAIL_DELAY_MIN_SECONDS = 2.0
        private const val WANDER_TARGET_FAIL_DELAY_MAX_SECONDS = 5.0
        private const val TEMPT_CALM_DOWN_TICKS = 100
        private const val TEMPT_CALM_DOWN_SECONDS = TEMPT_CALM_DOWN_TICKS / TICKS_PER_SECOND
        private const val DAMAGE_EPSILON = 1.0e-4f
        private const val COLLISION_EPSILON = 1.0e-7
        private const val DESPAWN_Y = -64.0
        private const val WORLD_MAX_Y = 320
        private val FIRE_IGNITER_BLOCK_KEYS = setOf(
            "minecraft:fire",
            "minecraft:soul_fire",
            "minecraft:lava"
        )
        private val FULL_BLOCK_COLLISION_BOXES = arrayOf(CollisionBox(0.0, 0.0, 0.0, 1.0, 1.0, 1.0))
        private val EMPTY_COLLISION_BOXES = emptyArray<CollisionBox>()
        private val PASS_THROUGH_BLOCK_KEYS = loadBlockTag("living_entity_pass_through")
        private val REPATH_DIRECTION_OFFSETS = doubleArrayOf(
            0.0,
            Math.PI / 4.0,
            -Math.PI / 4.0,
            Math.PI / 2.0,
            -Math.PI / 2.0,
            (3.0 * Math.PI) / 4.0,
            -(3.0 * Math.PI) / 4.0,
            Math.PI
        )

        private fun loadBlockTag(tagName: String): Set<String> {
            return resolveBlockTag(tagName, HashSet())
        }

        private fun resolveBlockTag(tagName: String, visited: MutableSet<String>): Set<String> {
            if (!visited.add(tagName)) return emptySet()
            val resourcePath = "/data/minecraft/tags/block/$tagName.json"
            val stream = AnimalSystem::class.java.getResourceAsStream(resourcePath) ?: return emptySet()
            val root = stream.bufferedReader().use { json.parseToJsonElement(it.readText()).jsonObject }
            val out = LinkedHashSet<String>()
            for (value in root["values"]?.jsonArray.orEmpty()) {
                val raw = value.jsonPrimitive.content
                if (raw.startsWith("#minecraft:")) {
                    out.addAll(resolveBlockTag(raw.removePrefix("#minecraft:"), visited))
                } else if (raw.startsWith("minecraft:")) {
                    out.add(raw)
                }
            }
            return out
        }
    }
}
