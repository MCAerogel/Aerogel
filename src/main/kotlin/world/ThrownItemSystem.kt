package org.macaroon3145.world

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.macaroon3145.network.codec.BlockStateRegistry
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.math.abs
import kotlin.math.ceil
import kotlin.math.floor
import kotlin.math.max
import kotlin.math.min
import kotlin.math.pow
import kotlin.math.sqrt

enum class ThrownItemKind {
    SNOWBALL,
    EGG,
    BLUE_EGG,
    BROWN_EGG,
    ENDER_PEARL
}

data class ThrownItemSnapshot(
    val entityId: Int,
    val ownerEntityId: Int,
    val uuid: UUID,
    val kind: ThrownItemKind,
    val prevX: Double,
    val prevY: Double,
    val prevZ: Double,
    val x: Double,
    val y: Double,
    val z: Double,
    val vx: Double,
    val vy: Double,
    val vz: Double,
    val accelerationX: Double,
    val accelerationY: Double,
    val accelerationZ: Double,
    val onGround: Boolean,
    val chunkPos: ChunkPos
)

data class ThrownItemRemovedEvent(
    val entityId: Int,
    val ownerEntityId: Int,
    val kind: ThrownItemKind,
    val chunkPos: ChunkPos,
    val hit: Boolean,
    val x: Double,
    val y: Double,
    val z: Double,
    val hitAxis: Int? = null,
    val hitDirection: Int? = null,
    val hitBoundary: Double? = null
)

data class ThrownItemTickEvents(
    val spawned: List<ThrownItemSnapshot>,
    val updated: List<ThrownItemSnapshot>,
    val removed: List<ThrownItemRemovedEvent>
) {
    fun isEmpty(): Boolean = spawned.isEmpty() && updated.isEmpty() && removed.isEmpty()
}

class ThrownItemSystem(
    private val blockStateAt: (Int, Int, Int) -> Int
) {
    private sealed interface PendingOp {
        data class Spawn(
            val entityId: Int,
            val ownerEntityId: Int,
            val kind: ThrownItemKind,
            val x: Double,
            val y: Double,
            val z: Double,
            val vx: Double,
            val vy: Double,
            val vz: Double,
            val uuid: UUID
        ) : PendingOp
        data class Remove(
            val entityId: Int
        ) : PendingOp
        data class UpdatePosition(
            val entityId: Int,
            val x: Double,
            val y: Double,
            val z: Double
        ) : PendingOp
        data class UpdateVelocity(
            val entityId: Int,
            val vx: Double,
            val vy: Double,
            val vz: Double
        ) : PendingOp
        data class UpdatePreviousPosition(
            val entityId: Int,
            val prevX: Double,
            val prevY: Double,
            val prevZ: Double
        ) : PendingOp
        data class UpdateAcceleration(
            val entityId: Int,
            val ax: Double,
            val ay: Double,
            val az: Double
        ) : PendingOp
        data class UpdateOwner(
            val entityId: Int,
            val ownerEntityId: Int
        ) : PendingOp
        data class UpdateKind(
            val entityId: Int,
            val kind: ThrownItemKind
        ) : PendingOp
        data class UpdateOnGround(
            val entityId: Int,
            val onGround: Boolean
        ) : PendingOp
    }

    private data class CollisionHit(
        val t: Double,
        val axis: Int?,
        val boundary: Double?,
        val direction: Int?
    )

    private data class SegmentAabbHit(
        val t: Double,
        val axis: Int?,
        val boundary: Double?,
        val direction: Int?
    )

    private data class CollisionBox(
        val minX: Double,
        val minY: Double,
        val minZ: Double,
        val maxX: Double,
        val maxY: Double,
        val maxZ: Double
    )

    private data class MutableThrownItem(
        val entityId: Int,
        var ownerEntityId: Int,
        val uuid: UUID,
        var kind: ThrownItemKind,
        var prevX: Double,
        var prevY: Double,
        var prevZ: Double,
        var x: Double,
        var y: Double,
        var z: Double,
        var vx: Double,
        var vy: Double,
        var vz: Double,
        var accelerationX: Double,
        var accelerationY: Double,
        var accelerationZ: Double,
        var onGround: Boolean,
        var ageTicks: Double,
        var chunkX: Int,
        var chunkZ: Int
    ) {
        fun snapshot(): ThrownItemSnapshot {
            return ThrownItemSnapshot(
                entityId = entityId,
                ownerEntityId = ownerEntityId,
                uuid = uuid,
                kind = kind,
                prevX = prevX,
                prevY = prevY,
                prevZ = prevZ,
                x = x,
                y = y,
                z = z,
                vx = vx,
                vy = vy,
                vz = vz,
                accelerationX = accelerationX,
                accelerationY = accelerationY,
                accelerationZ = accelerationZ,
                onGround = onGround,
                chunkPos = ChunkPos(chunkX, chunkZ)
            )
        }
    }

    private val entities = ConcurrentHashMap<Int, MutableThrownItem>()
    private val snapshots = ConcurrentHashMap<Int, ThrownItemSnapshot>()
    private val chunkIndex = ConcurrentHashMap<ChunkPos, MutableSet<Int>>()
    private val pendingSpawned = ConcurrentLinkedQueue<Int>()
    private val pendingOps = ConcurrentLinkedQueue<PendingOp>()
    private val collisionBoxesByState = ConcurrentHashMap<Int, Array<CollisionBox>>()

    fun spawn(
        entityId: Int,
        ownerEntityId: Int,
        kind: ThrownItemKind,
        x: Double,
        y: Double,
        z: Double,
        vx: Double,
        vy: Double,
        vz: Double,
        uuid: UUID = UUID.randomUUID()
    ): Boolean {
        if (entityId < 0 || !x.isFinite() || !y.isFinite() || !z.isFinite() || !vx.isFinite() || !vy.isFinite() || !vz.isFinite()) return false
        pendingOps.add(
            PendingOp.Spawn(
                entityId = entityId,
                ownerEntityId = ownerEntityId,
                kind = kind,
                x = x,
                y = y,
                z = z,
                vx = vx,
                vy = vy,
                vz = vz,
                uuid = uuid
            )
        )
        return true
    }

    fun remove(
        entityId: Int,
        hit: Boolean,
        x: Double? = null,
        y: Double? = null,
        z: Double? = null,
        hitAxis: Int? = null,
        hitDirection: Int? = null,
        hitBoundary: Double? = null
    ): ThrownItemRemovedEvent? {
        val snapshot = snapshots[entityId] ?: return null
        pendingOps.add(PendingOp.Remove(entityId))
        return ThrownItemRemovedEvent(
            entityId = entityId,
            ownerEntityId = snapshot.ownerEntityId,
            kind = snapshot.kind,
            chunkPos = snapshot.chunkPos,
            hit = hit,
            x = x ?: snapshot.x,
            y = y ?: snapshot.y,
            z = z ?: snapshot.z,
            hitAxis = hitAxis,
            hitDirection = hitDirection,
            hitBoundary = hitBoundary
        )
    }

    fun hasEntities(): Boolean = snapshots.isNotEmpty() || pendingOps.isNotEmpty()

    fun snapshot(entityId: Int): ThrownItemSnapshot? = snapshots[entityId]

    fun snapshots(): List<ThrownItemSnapshot> {
        return snapshots.values.toList()
    }

    fun snapshotByUuid(uuid: UUID): ThrownItemSnapshot? {
        return snapshots.values.firstOrNull { it.uuid == uuid }
    }

    fun updatePosition(entityId: Int, x: Double, y: Double, z: Double): ThrownItemSnapshot? {
        if (!x.isFinite() || !y.isFinite() || !z.isFinite()) return null
        if (snapshots[entityId] == null) return null
        pendingOps.add(PendingOp.UpdatePosition(entityId, x, y, z))
        return snapshots[entityId]
    }

    fun updateVelocity(entityId: Int, vx: Double, vy: Double, vz: Double): ThrownItemSnapshot? {
        if (!vx.isFinite() || !vy.isFinite() || !vz.isFinite()) return null
        if (snapshots[entityId] == null) return null
        pendingOps.add(PendingOp.UpdateVelocity(entityId, vx, vy, vz))
        return snapshots[entityId]
    }

    fun updatePreviousPosition(entityId: Int, prevX: Double, prevY: Double, prevZ: Double): ThrownItemSnapshot? {
        if (!prevX.isFinite() || !prevY.isFinite() || !prevZ.isFinite()) return null
        if (snapshots[entityId] == null) return null
        pendingOps.add(PendingOp.UpdatePreviousPosition(entityId, prevX, prevY, prevZ))
        return snapshots[entityId]
    }

    fun updateAcceleration(entityId: Int, ax: Double, ay: Double, az: Double): ThrownItemSnapshot? {
        if (!ax.isFinite() || !ay.isFinite() || !az.isFinite()) return null
        if (snapshots[entityId] == null) return null
        pendingOps.add(PendingOp.UpdateAcceleration(entityId, ax, ay, az))
        return snapshots[entityId]
    }

    fun updateOwnerEntityId(entityId: Int, ownerEntityId: Int): ThrownItemSnapshot? {
        if (snapshots[entityId] == null) return null
        pendingOps.add(PendingOp.UpdateOwner(entityId, ownerEntityId))
        return snapshots[entityId]
    }

    fun updateKind(entityId: Int, kind: ThrownItemKind): ThrownItemSnapshot? {
        if (snapshots[entityId] == null) return null
        pendingOps.add(PendingOp.UpdateKind(entityId, kind))
        return snapshots[entityId]
    }

    fun updateOnGround(entityId: Int, onGround: Boolean): ThrownItemSnapshot? {
        if (snapshots[entityId] == null) return null
        pendingOps.add(PendingOp.UpdateOnGround(entityId, onGround))
        return snapshots[entityId]
    }

    fun snapshotsInChunk(chunkX: Int, chunkZ: Int): List<ThrownItemSnapshot> {
        val chunkPos = ChunkPos(chunkX, chunkZ)
        val ids = chunkIndex[chunkPos] ?: return emptyList()
        if (ids.isEmpty()) return emptyList()
        val out = ArrayList<ThrownItemSnapshot>(ids.size)
        for (id in ids) {
            val snapshot = snapshots[id] ?: continue
            if (snapshot.chunkPos == chunkPos) out.add(snapshot)
        }
        return out
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

    fun tick(
        deltaSeconds: Double,
        activeSimulationChunks: Set<ChunkPos>? = null,
        chunkDeltaSecondsProvider: ((ChunkPos) -> Double)? = null,
        chunkTimeRecorder: ((ChunkPos, Long) -> Unit)? = null
    ): ThrownItemTickEvents {
        if (deltaSeconds <= 0.0) return ThrownItemTickEvents(emptyList(), emptyList(), emptyList())
        flushPendingOps()
        if (entities.isEmpty()) return ThrownItemTickEvents(emptyList(), emptyList(), emptyList())

        val spawned = ArrayList<ThrownItemSnapshot>()
        val updated = ArrayList<ThrownItemSnapshot>()
        val removed = ArrayList<ThrownItemRemovedEvent>()
        val chunkDeltaSecondsByChunk = HashMap<ChunkPos, Double>()

        while (true) {
            val entityId = pendingSpawned.poll() ?: break
            val snapshot = snapshots[entityId] ?: continue
            spawned.add(snapshot)
        }

        for ((entityId, entity) in entities.toList()) {
            val oldChunk = ChunkPos(entity.chunkX, entity.chunkZ)
            if (activeSimulationChunks != null && oldChunk !in activeSimulationChunks) continue
            // Chunk delta must be sampled once per chunk per tick frame.
            // Sampling per-entity causes later entities in the same chunk
            // to get near-zero elapsed time and appear frozen briefly.
            val chunkDeltaSeconds = chunkDeltaSecondsByChunk.getOrPut(oldChunk) {
                chunkDeltaSecondsProvider?.invoke(oldChunk) ?: deltaSeconds
            }
            if (chunkDeltaSeconds <= 0.0) continue
            val tickScale = chunkDeltaSeconds * 20.0
            val startedAt = System.nanoTime()

            val oldX = entity.x
            val oldY = entity.y
            val oldZ = entity.z
            val oldVx = entity.vx
            val oldVy = entity.vy
            val oldVz = entity.vz

            stepEntity(entity, tickScale)
            entity.prevX = oldX
            entity.prevY = oldY
            entity.prevZ = oldZ
            entity.ageTicks += tickScale

            val collisionHit = firstCollisionHit(oldX, oldY, oldZ, entity.x, entity.y, entity.z)
            val hit = collisionHit != null
            val despawn =
                entity.y < DESPAWN_Y ||
                    entity.y > WORLD_MAX_Y + 2.0 ||
                    entity.ageTicks > MAX_LIFETIME_TICKS ||
                    hit
            if (despawn) {
                val (removeX, removeY, removeZ) = if (hit) {
                    impactPointAtCollisionFace(oldX, oldY, oldZ, entity.x, entity.y, entity.z, collisionHit!!)
                } else {
                    Triple(entity.x, entity.y, entity.z)
                }
                if (entities.remove(entityId, entity)) {
                    snapshots.remove(entityId)
                    removeFromChunkIndex(oldChunk, entityId)
                    removed.add(
                        ThrownItemRemovedEvent(
                            entityId = entityId,
                            ownerEntityId = entity.ownerEntityId,
                            kind = entity.kind,
                            chunkPos = oldChunk,
                            hit = hit,
                            x = removeX,
                            y = removeY,
                            z = removeZ,
                            hitAxis = collisionHit?.axis,
                            hitDirection = collisionHit?.direction,
                            hitBoundary = collisionHit?.boundary
                        )
                    )
                }
                chunkTimeRecorder?.invoke(oldChunk, System.nanoTime() - startedAt)
                continue
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
                    newChunk != oldChunk
            if (moved) {
                val snapshot = entity.snapshot()
                snapshots[entityId] = snapshot
                updated.add(snapshot)
            }
            chunkTimeRecorder?.invoke(newChunk, System.nanoTime() - startedAt)
        }

        return ThrownItemTickEvents(spawned, updated, removed)
    }

    private fun stepEntity(entity: MutableThrownItem, tickScale: Double) {
        val deltaSeconds = tickScale / 20.0
        if (entity.accelerationX != 0.0 || entity.accelerationY != 0.0 || entity.accelerationZ != 0.0) {
            entity.vx += (entity.accelerationX * deltaSeconds) / 20.0
            entity.vy += (entity.accelerationY * deltaSeconds) / 20.0
            entity.vz += (entity.accelerationZ * deltaSeconds) / 20.0
        }
        entity.vy -= GRAVITY_PER_TICK * tickScale
        entity.x += entity.vx * tickScale
        entity.y += entity.vy * tickScale
        entity.z += entity.vz * tickScale

        val drag = AIR_DRAG.pow(tickScale)
        entity.vx *= drag
        entity.vy *= drag
        entity.vz *= drag
        if (abs(entity.vx) < VELOCITY_EPSILON) entity.vx = 0.0
        if (abs(entity.vy) < VELOCITY_EPSILON) entity.vy = 0.0
        if (abs(entity.vz) < VELOCITY_EPSILON) entity.vz = 0.0
        entity.onGround = collidesAtPoint(entity.x, entity.y - PROJECTILE_COLLISION_RADIUS - 0.01, entity.z)
    }

    private inline fun mutate(entityId: Int, mutator: (MutableThrownItem) -> Unit): ThrownItemSnapshot? {
        val entity = entities[entityId] ?: return null
        val oldChunk = ChunkPos(entity.chunkX, entity.chunkZ)
        mutator(entity)
        val newChunkX = chunkXFromBlockX(entity.x)
        val newChunkZ = chunkZFromBlockZ(entity.z)
        val newChunk = ChunkPos(newChunkX, newChunkZ)
        entity.chunkX = newChunkX
        entity.chunkZ = newChunkZ
        val snapshot = entity.snapshot()
        snapshots[entityId] = snapshot
        if (newChunk != oldChunk) {
            moveChunkIndex(entityId, oldChunk, newChunk)
        }
        return snapshot
    }

    internal fun flushPendingOps() {
        while (true) {
            when (val op = pendingOps.poll() ?: break) {
                is PendingOp.Spawn -> {
                    if (entities.containsKey(op.entityId) || snapshots.containsKey(op.entityId)) continue
                    val chunkX = chunkXFromBlockX(op.x)
                    val chunkZ = chunkZFromBlockZ(op.z)
                    val entity = MutableThrownItem(
                        entityId = op.entityId,
                        ownerEntityId = op.ownerEntityId,
                        uuid = op.uuid,
                        kind = op.kind,
                        prevX = op.x,
                        prevY = op.y,
                        prevZ = op.z,
                        x = op.x,
                        y = op.y,
                        z = op.z,
                        vx = op.vx,
                        vy = op.vy,
                        vz = op.vz,
                        accelerationX = 0.0,
                        accelerationY = 0.0,
                        accelerationZ = 0.0,
                        onGround = false,
                        ageTicks = 0.0,
                        chunkX = chunkX,
                        chunkZ = chunkZ
                    )
                    entities[op.entityId] = entity
                    val snapshot = entity.snapshot()
                    snapshots[op.entityId] = snapshot
                    addToChunkIndex(snapshot.chunkPos, op.entityId)
                    pendingSpawned.add(op.entityId)
                }
                is PendingOp.Remove -> {
                    val entity = entities.remove(op.entityId) ?: continue
                    val oldChunk = ChunkPos(entity.chunkX, entity.chunkZ)
                    snapshots.remove(op.entityId)
                    removeFromChunkIndex(oldChunk, op.entityId)
                }
                is PendingOp.UpdatePosition -> mutate(op.entityId) { entity ->
                    entity.x = op.x
                    entity.y = op.y
                    entity.z = op.z
                    entity.prevX = op.x
                    entity.prevY = op.y
                    entity.prevZ = op.z
                }
                is PendingOp.UpdateVelocity -> mutate(op.entityId) { entity ->
                    entity.vx = op.vx
                    entity.vy = op.vy
                    entity.vz = op.vz
                }
                is PendingOp.UpdatePreviousPosition -> mutate(op.entityId) { entity ->
                    entity.prevX = op.prevX
                    entity.prevY = op.prevY
                    entity.prevZ = op.prevZ
                }
                is PendingOp.UpdateAcceleration -> mutate(op.entityId) { entity ->
                    entity.accelerationX = op.ax
                    entity.accelerationY = op.ay
                    entity.accelerationZ = op.az
                }
                is PendingOp.UpdateOwner -> mutate(op.entityId) { entity ->
                    entity.ownerEntityId = op.ownerEntityId
                }
                is PendingOp.UpdateKind -> mutate(op.entityId) { entity ->
                    entity.kind = op.kind
                }
                is PendingOp.UpdateOnGround -> mutate(op.entityId) { entity ->
                    entity.onGround = op.onGround
                }
            }
        }
    }

    private fun collidesAtPoint(x: Double, y: Double, z: Double): Boolean {
        val blockX = floor(x).toInt()
        val blockY = floor(y).toInt()
        val blockZ = floor(z).toInt()
        val stateId = blockStateAt(blockX, blockY, blockZ)
        if (stateId <= 0) return false
        val boxes = collisionBoxesForState(stateId)
        if (boxes.isEmpty()) return false
        val localX = x - blockX
        val localY = y - blockY
        val localZ = z - blockZ
        for (box in boxes) {
            if (localX >= box.minX && localX <= box.maxX &&
                localY >= box.minY && localY <= box.maxY &&
                localZ >= box.minZ && localZ <= box.maxZ
            ) {
                return true
            }
        }
        return false
    }

    private fun firstCollisionHit(
        oldX: Double,
        oldY: Double,
        oldZ: Double,
        newX: Double,
        newY: Double,
        newZ: Double
    ): CollisionHit? {
        val radius = PROJECTILE_COLLISION_RADIUS
        val minX = min(oldX, newX) - radius
        val maxX = max(oldX, newX) + radius
        val minY = min(oldY, newY) - radius
        val maxY = max(oldY, newY) + radius
        val minZ = min(oldZ, newZ) - radius
        val maxZ = max(oldZ, newZ) + radius
        val startX = floor(minX).toInt()
        val endX = floor(maxX - 1.0e-7).toInt()
        val startY = floor(minY).toInt()
        val endY = floor(maxY - 1.0e-7).toInt()
        val startZ = floor(minZ).toInt()
        val endZ = floor(maxZ - 1.0e-7).toInt()

        var earliest: CollisionHit? = null
        for (bx in startX..endX) {
            for (by in startY..endY) {
                for (bz in startZ..endZ) {
                    val stateId = blockStateAt(bx, by, bz)
                    if (stateId <= 0) continue
                    val boxes = collisionBoxesForState(stateId)
                    if (boxes.isEmpty()) continue
                    for (box in boxes) {
                        val expandedMinX = bx + box.minX - radius
                        val expandedMinY = by + box.minY - radius
                        val expandedMinZ = bz + box.minZ - radius
                        val expandedMaxX = bx + box.maxX + radius
                        val expandedMaxY = by + box.maxY + radius
                        val expandedMaxZ = bz + box.maxZ + radius
                        if (oldX in expandedMinX..expandedMaxX &&
                            oldY in expandedMinY..expandedMaxY &&
                            oldZ in expandedMinZ..expandedMaxZ
                        ) {
                            return CollisionHit(t = 0.0, axis = null, boundary = null, direction = null)
                        }
                        val hit = segmentAabbEntryHit(
                            x0 = oldX,
                            y0 = oldY,
                            z0 = oldZ,
                            x1 = newX,
                            y1 = newY,
                            z1 = newZ,
                            minX = expandedMinX,
                            maxX = expandedMaxX,
                            minY = expandedMinY,
                            maxY = expandedMaxY,
                            minZ = expandedMinZ,
                            maxZ = expandedMaxZ
                        ) ?: continue
                        val current = earliest?.t
                        if (current == null || hit.t < current) {
                            earliest = CollisionHit(
                                t = hit.t,
                                axis = hit.axis,
                                boundary = hit.boundary,
                                direction = hit.direction
                            )
                        }
                    }
                }
            }
        }
        return earliest
    }

    private fun impactPointAtCollisionFace(
        oldX: Double,
        oldY: Double,
        oldZ: Double,
        newX: Double,
        newY: Double,
        newZ: Double,
        collisionHit: CollisionHit
    ): Triple<Double, Double, Double> {
        var x = lerp(oldX, newX, collisionHit.t)
        var y = lerp(oldY, newY, collisionHit.t)
        var z = lerp(oldZ, newZ, collisionHit.t)
        val axis = collisionHit.axis
        val boundary = collisionHit.boundary
        if (axis != null && boundary != null) {
            when (axis) {
                AXIS_X -> x = boundary
                AXIS_Y -> y = boundary
                AXIS_Z -> z = boundary
            }
        }
        return Triple(x, y, z)
    }

    private fun lerp(a: Double, b: Double, t: Double): Double = a + (b - a) * t

    private fun collisionBoxesForState(stateId: Int): Array<CollisionBox> {
        if (stateId <= 0) return EMPTY_COLLISION_BOXES
        return collisionBoxesByState.computeIfAbsent(stateId) { id ->
            val parsed = BlockStateRegistry.parsedState(id) ?: return@computeIfAbsent FULL_BLOCK_COLLISION_BOXES
            if (parsed.blockKey in NON_COLLIDING_BLOCK_KEYS) return@computeIfAbsent EMPTY_COLLISION_BOXES
            val resolved = BlockCollisionRegistry.boxesForStateId(id, parsed)
            if (resolved == null) return@computeIfAbsent FULL_BLOCK_COLLISION_BOXES
            if (resolved.isEmpty()) return@computeIfAbsent EMPTY_COLLISION_BOXES
            Array(resolved.size) { i ->
                val box = resolved[i]
                CollisionBox(
                    minX = box.minX,
                    minY = box.minY,
                    minZ = box.minZ,
                    maxX = box.maxX,
                    maxY = box.maxY,
                    maxZ = box.maxZ
                )
            }
        }
    }

    private fun segmentAabbEntryHit(
        x0: Double,
        y0: Double,
        z0: Double,
        x1: Double,
        y1: Double,
        z1: Double,
        minX: Double,
        maxX: Double,
        minY: Double,
        maxY: Double,
        minZ: Double,
        maxZ: Double
    ): SegmentAabbHit? {
        val epsilon = 1.0e-8
        var tMin = 0.0
        var tMax = 1.0
        var enterAxis: Int? = null
        var enterBoundary: Double? = null
        var enterDirection: Int? = null

        fun clip(p0: Double, p1: Double, min: Double, max: Double, axis: Int): Boolean {
            val d = p1 - p0
            if (abs(d) <= epsilon) return p0 in min..max
            val inv = 1.0 / d
            var t1 = (min - p0) * inv
            var t2 = (max - p0) * inv
            if (t1 > t2) {
                val tmp = t1
                t1 = t2
                t2 = tmp
            }
            if (t2 < tMin || t1 > tMax) return false
            if (t1 > tMin) {
                tMin = t1
                enterAxis = axis
                enterBoundary = if (d > 0.0) min else max
                enterDirection = if (d > 0.0) 1 else -1
            }
            if (t2 < tMax) tMax = t2
            return tMin <= tMax
        }

        if (!clip(x0, x1, minX, maxX, AXIS_X)) return null
        if (!clip(y0, y1, minY, maxY, AXIS_Y)) return null
        if (!clip(z0, z1, minZ, maxZ, AXIS_Z)) return null
        return SegmentAabbHit(
            t = tMin.coerceIn(0.0, 1.0),
            axis = enterAxis,
            boundary = enterBoundary,
            direction = enterDirection
        )
    }

    private fun addToChunkIndex(chunkPos: ChunkPos, entityId: Int) {
        chunkIndex.computeIfAbsent(chunkPos) { ConcurrentHashMap.newKeySet<Int>() }.add(entityId)
    }

    private fun removeFromChunkIndex(chunkPos: ChunkPos, entityId: Int) {
        val ids = chunkIndex[chunkPos] ?: return
        ids.remove(entityId)
        if (ids.isEmpty()) chunkIndex.remove(chunkPos, ids)
    }

    private fun moveChunkIndex(entityId: Int, from: ChunkPos, to: ChunkPos) {
        if (from == to) return
        removeFromChunkIndex(from, entityId)
        addToChunkIndex(to, entityId)
    }

    private fun chunkXFromBlockX(x: Double): Int = floor(x / 16.0).toInt()
    private fun chunkZFromBlockZ(z: Double): Int = floor(z / 16.0).toInt()

    private companion object {
        private val json = Json { ignoreUnknownKeys = true }
        private const val GRAVITY_PER_TICK = 0.03
        private const val AIR_DRAG = 0.99
        private const val POSITION_EPSILON = 1.0e-4
        private const val VELOCITY_EPSILON = 1.0e-6
        private const val DESPAWN_Y = -128.0
        private const val WORLD_MAX_Y = 319
        private const val MAX_LIFETIME_TICKS = 1200.0
        private const val PROJECTILE_COLLISION_RADIUS = 0.125
        private const val AXIS_X = 0
        private const val AXIS_Y = 1
        private const val AXIS_Z = 2
        private val EMPTY_COLLISION_BOXES = emptyArray<CollisionBox>()
        private val FULL_BLOCK_COLLISION_BOXES = arrayOf(
            CollisionBox(0.0, 0.0, 0.0, 1.0, 1.0, 1.0)
        )
        private val NON_COLLIDING_BLOCK_KEYS: Set<String> = run {
            val loaded = loadBlockTag("dropped_item_non_colliding")
            if (loaded.isNotEmpty()) return@run loaded
            // Fallback for environments where the tag resource is unavailable.
            setOf(
                "minecraft:air",
                "minecraft:cave_air",
                "minecraft:void_air",
                "minecraft:water",
                "minecraft:lava",
                "minecraft:short_grass",
                "minecraft:tall_grass",
                "minecraft:fern",
                "minecraft:large_fern",
                "minecraft:dead_bush",
                "minecraft:vine",
                "minecraft:glow_lichen",
                "minecraft:fire",
                "minecraft:soul_fire"
            )
        }

        private fun loadBlockTag(tagName: String): Set<String> {
            return resolveBlockTag(tagName, HashSet())
        }

        private fun resolveBlockTag(tagName: String, visited: MutableSet<String>): Set<String> {
            if (!visited.add(tagName)) return emptySet()
            val resourcePath = "/data/minecraft/tags/block/$tagName.json"
            val stream = ThrownItemSystem::class.java.getResourceAsStream(resourcePath) ?: return emptySet()
            val root = stream.bufferedReader().use {
                json.parseToJsonElement(it.readText()).jsonObject
            }
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
