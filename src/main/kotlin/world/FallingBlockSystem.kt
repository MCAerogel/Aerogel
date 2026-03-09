package org.macaroon3145.world

import org.macaroon3145.network.codec.BlockStateRegistry
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.math.abs
import kotlin.math.floor
import kotlin.math.pow

data class FallingBlockSnapshot(
    val entityId: Int,
    val uuid: UUID,
    val blockStateId: Int,
    val x: Double,
    val y: Double,
    val z: Double,
    val vx: Double,
    val vy: Double,
    val vz: Double,
    val onGround: Boolean,
    val chunkPos: ChunkPos
)

data class FallingBlockRemovedEvent(
    val entityId: Int,
    val chunkPos: ChunkPos
)

data class FallingBlockLandedEvent(
    val blockX: Int,
    val blockY: Int,
    val blockZ: Int,
    val blockStateId: Int,
    val chunkPos: ChunkPos
)

data class FallingBlockDroppedEvent(
    val blockX: Int,
    val blockY: Int,
    val blockZ: Int,
    val blockStateId: Int,
    val chunkPos: ChunkPos
)

data class FallingBlockTickEvents(
    val spawned: List<FallingBlockSnapshot>,
    val updated: List<FallingBlockSnapshot>,
    val removed: List<FallingBlockRemovedEvent>,
    val landed: List<FallingBlockLandedEvent>,
    val dropped: List<FallingBlockDroppedEvent>
) {
    fun isEmpty(): Boolean {
        return spawned.isEmpty() && updated.isEmpty() && removed.isEmpty() && landed.isEmpty() && dropped.isEmpty()
    }
}

class FallingBlockSystem(
    private val blockStateAt: (Int, Int, Int) -> Int,
    private val setBlockState: (Int, Int, Int, Int) -> Unit,
    private val clearBlockEntity: (Int, Int, Int) -> Unit
) {
    private data class PendingSpawn(
        val entityId: Int,
        val blockStateId: Int,
        val x: Double,
        val y: Double,
        val z: Double,
        val vx: Double,
        val vy: Double,
        val vz: Double
    )

    private data class MutableFallingBlock(
        val entityId: Int,
        val uuid: UUID,
        val blockStateId: Int,
        var x: Double,
        var y: Double,
        var z: Double,
        var vx: Double,
        var vy: Double,
        var vz: Double,
        var onGround: Boolean,
        var chunkX: Int,
        var chunkZ: Int
    ) {
        fun snapshot(): FallingBlockSnapshot {
            return FallingBlockSnapshot(
                entityId = entityId,
                uuid = uuid,
                blockStateId = blockStateId,
                x = x,
                y = y,
                z = z,
                vx = vx,
                vy = vy,
                vz = vz,
                onGround = onGround,
                chunkPos = ChunkPos(chunkX, chunkZ)
            )
        }
    }

    private val entities = ConcurrentHashMap<Int, MutableFallingBlock>()
    private val snapshots = ConcurrentHashMap<Int, FallingBlockSnapshot>()
    private val chunkIndex = ConcurrentHashMap<ChunkPos, MutableSet<Int>>()
    private val pendingSpawnRequests = ConcurrentLinkedQueue<PendingSpawn>()
    private val reservedPendingEntityIds = ConcurrentHashMap.newKeySet<Int>()
    private val collidableStateCache = ConcurrentHashMap<Int, Boolean>()
    private val fallThroughStateCache = ConcurrentHashMap<Int, Boolean>()
    private val impactFragileStateCache = ConcurrentHashMap<Int, Boolean>()

    fun spawn(
        entityId: Int,
        blockStateId: Int,
        x: Double,
        y: Double,
        z: Double,
        vx: Double,
        vy: Double,
        vz: Double
    ): Boolean {
        if (entityId < 0 || blockStateId <= 0) return false
        if (!reservedPendingEntityIds.add(entityId)) return false
        pendingSpawnRequests.add(
            PendingSpawn(
                entityId = entityId,
                blockStateId = blockStateId,
                x = x,
                y = y,
                z = z,
                vx = vx,
                vy = vy,
                vz = vz
            )
        )
        return true
    }

    fun hasEntities(): Boolean {
        return snapshots.isNotEmpty() || pendingSpawnRequests.isNotEmpty()
    }

    fun snapshot(entityId: Int): FallingBlockSnapshot? {
        return snapshots[entityId]
    }

    fun snapshotsInChunk(chunkX: Int, chunkZ: Int): List<FallingBlockSnapshot> {
        val chunkPos = ChunkPos(chunkX, chunkZ)
        val ids = chunkIndex[chunkPos] ?: return emptyList()
        if (ids.isEmpty()) return emptyList()
        val out = ArrayList<FallingBlockSnapshot>(ids.size)
        for (id in ids) {
            val snapshot = snapshots[id] ?: continue
            if (snapshot.chunkPos == chunkPos) {
                out.add(snapshot)
            }
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
            out[i] = id
            i++
        }
        return if (i == out.size) out else out.copyOf(i)
    }

    fun tick(
        deltaSeconds: Double,
        activeSimulationChunks: Set<ChunkPos>? = null,
        chunkDeltaSecondsProvider: ((ChunkPos) -> Double)? = null,
        chunkTimeRecorder: ((ChunkPos, Long) -> Unit)? = null
    ): FallingBlockTickEvents {
        if (deltaSeconds <= 0.0) return FallingBlockTickEvents(emptyList(), emptyList(), emptyList(), emptyList(), emptyList())

        val spawned = ArrayList<FallingBlockSnapshot>()
        val updated = ArrayList<FallingBlockSnapshot>()
        val removed = ArrayList<FallingBlockRemovedEvent>()
        val landed = ArrayList<FallingBlockLandedEvent>()
        val dropped = ArrayList<FallingBlockDroppedEvent>()
        val chunkDeltaSecondsByChunk = HashMap<ChunkPos, Double>()

        flushPendingSpawnsInto(spawned)
        if (entities.isEmpty()) return FallingBlockTickEvents(spawned, updated, removed, landed, dropped)

        for ((entityId, entity) in entities.toList()) {
            val chunkPos = ChunkPos(entity.chunkX, entity.chunkZ)
            val shouldSimulate = activeSimulationChunks == null || activeSimulationChunks.contains(chunkPos)
            if (!shouldSimulate) continue
            // Sample once per chunk per tick-frame so entities in the same chunk
            // advance with identical time slices.
            val chunkDeltaSeconds = chunkDeltaSecondsByChunk.getOrPut(chunkPos) {
                chunkDeltaSecondsProvider?.invoke(chunkPos) ?: deltaSeconds
            }
            if (chunkDeltaSeconds <= 0.0) continue
            val tickScale = chunkDeltaSeconds * 20.0

            val startedAtNanos = System.nanoTime()
            val oldX = entity.x
            val oldY = entity.y
            val oldZ = entity.z
            val oldVx = entity.vx
            val oldVy = entity.vy
            val oldVz = entity.vz
            val oldOnGround = entity.onGround
            val oldChunk = chunkPos

            stepEntity(entity, tickScale)

            val fellOut = entity.y < DESPAWN_Y
            if (entity.onGround || fellOut) {
                if (entities.remove(entityId, entity)) {
                    snapshots.remove(entityId)
                    removeFromChunkIndex(oldChunk, entityId)
                    removed.add(FallingBlockRemovedEvent(entityId, oldChunk))
                    if (!fellOut) {
                        val landedY = floor(entity.y).toInt()
                        val landedX = floor(entity.x).toInt()
                        val landedZ = floor(entity.z).toInt()
                        val landingStateId = if (landedY in WORLD_MIN_Y..WORLD_MAX_Y) blockStateAt(landedX, landedY, landedZ) else 0
                        val supportStateId = if (landedY - 1 in WORLD_MIN_Y..WORLD_MAX_Y) blockStateAt(landedX, landedY - 1, landedZ) else 0
                        if (landedY in WORLD_MIN_Y..WORLD_MAX_Y && shouldDropOnLanding(landedX, landedY, landedZ)) {
                            dropped.add(
                                FallingBlockDroppedEvent(
                                    blockX = landedX,
                                    blockY = landedY,
                                    blockZ = landedZ,
                                    blockStateId = entity.blockStateId,
                                    chunkPos = ChunkPos(landedX shr 4, landedZ shr 4)
                                )
                            )
                        } else if (landedY in WORLD_MIN_Y..WORLD_MAX_Y) {
                            if (tryPlaceLandedBlock(landedX, landedY, landedZ, entity.blockStateId)) {
                                clearBlockEntity(landedX, landedY, landedZ)
                                landed.add(
                                    FallingBlockLandedEvent(
                                        blockX = landedX,
                                        blockY = landedY,
                                        blockZ = landedZ,
                                        blockStateId = entity.blockStateId,
                                        chunkPos = ChunkPos(landedX shr 4, landedZ shr 4)
                                    )
                                )
                            } else {
                                // Vanilla-like fallback: if landing placement is blocked, drop item instead.
                                dropped.add(
                                    FallingBlockDroppedEvent(
                                        blockX = landedX,
                                        blockY = landedY,
                                        blockZ = landedZ,
                                        blockStateId = entity.blockStateId,
                                        chunkPos = ChunkPos(landedX shr 4, landedZ shr 4)
                                    )
                                )
                            }
                        }
                    }
                }
                chunkTimeRecorder?.invoke(oldChunk, System.nanoTime() - startedAtNanos)
                continue
            }

            val newChunkX = chunkXFromBlockX(entity.x)
            val newChunkZ = chunkZFromBlockZ(entity.z)
            val newChunk = ChunkPos(newChunkX, newChunkZ)
            if (oldChunk != newChunk) {
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
                    oldOnGround != entity.onGround ||
                    oldChunk != newChunk
            if (moved) {
                val snapshot = entity.snapshot()
                snapshots[entityId] = snapshot
                updated.add(snapshot)
            }
            chunkTimeRecorder?.invoke(newChunk, System.nanoTime() - startedAtNanos)
        }
        return FallingBlockTickEvents(spawned, updated, removed, landed, dropped)
    }

    internal fun flushPendingSpawns() {
        flushPendingSpawnsInto(null)
    }

    private fun flushPendingSpawnsInto(spawnedOut: MutableList<FallingBlockSnapshot>?) {
        while (true) {
            val request = pendingSpawnRequests.poll() ?: break
            val chunkX = chunkXFromBlockX(request.x)
            val chunkZ = chunkZFromBlockZ(request.z)
            val entity = MutableFallingBlock(
                entityId = request.entityId,
                uuid = UUID.randomUUID(),
                blockStateId = request.blockStateId,
                x = request.x,
                y = request.y,
                z = request.z,
                vx = request.vx,
                vy = request.vy,
                vz = request.vz,
                onGround = false,
                chunkX = chunkX,
                chunkZ = chunkZ
            )
            val inserted = entities.putIfAbsent(request.entityId, entity) == null
            reservedPendingEntityIds.remove(request.entityId)
            if (!inserted) continue
            val snapshot = entity.snapshot()
            snapshots[request.entityId] = snapshot
            addToChunkIndex(snapshot.chunkPos, request.entityId)
            spawnedOut?.add(snapshot)
        }
    }

    private fun tryPlaceLandedBlock(x: Int, y: Int, z: Int, stateId: Int): Boolean {
        val current = blockStateAt(x, y, z)
        if (!isFallThroughState(current)) return false
        setBlockState(x, y, z, stateId)
        return true
    }

    private fun shouldDropOnLanding(x: Int, y: Int, z: Int): Boolean {
        val landingState = blockStateAt(x, y, z)
        if (isImpactFragileState(landingState)) return true
        if (y - 1 < WORLD_MIN_Y) return false
        val supportState = blockStateAt(x, y - 1, z)
        return isImpactFragileState(supportState)
    }

    private fun stepEntity(entity: MutableFallingBlock, tickScale: Double) {
        entity.vy -= GRAVITY_PER_TICK * tickScale

        val totalDx = entity.vx * tickScale
        val totalDy = entity.vy * tickScale
        val totalDz = entity.vz * tickScale
        val steps = movementSubsteps(totalDx, totalDy, totalDz)
        val subScale = tickScale / steps
        var grounded = false

        repeat(steps) {
            var nextY = entity.y + entity.vy * subScale
            var stepOnGround = false
            if (entity.vy > VELOCITY_EPSILON) {
                val head = nextY + ENTITY_HEIGHT
                if (collidesFootprint(entity.x, head, entity.z)) {
                    val blockY = floor(head).toInt()
                    nextY = blockY - ENTITY_HEIGHT - COLLISION_EPSILON
                    entity.vy = 0.0
                }
            } else if (entity.vy <= VELOCITY_EPSILON) {
                val feet = nextY - 0.01
                if (collidesFootprint(entity.x, feet, entity.z)) {
                    val supportY = floor(feet).toInt()
                    nextY = supportY + 1.0
                    entity.vy = 0.0
                    stepOnGround = true
                }
            }
            entity.y = nextY

            var nextX = entity.x + entity.vx * subScale
            if (entity.vx > VELOCITY_EPSILON) {
                val front = nextX + ENTITY_RADIUS
                if (collidesXFace(front, entity.y, entity.z)) {
                    val blockX = floor(front).toInt()
                    nextX = blockX - ENTITY_RADIUS - COLLISION_EPSILON
                    entity.vx = 0.0
                }
            } else if (entity.vx < -VELOCITY_EPSILON) {
                val front = nextX - ENTITY_RADIUS
                if (collidesXFace(front, entity.y, entity.z)) {
                    val blockX = floor(front).toInt()
                    nextX = blockX + 1.0 + ENTITY_RADIUS + COLLISION_EPSILON
                    entity.vx = 0.0
                }
            }
            entity.x = nextX

            var nextZ = entity.z + entity.vz * subScale
            if (entity.vz > VELOCITY_EPSILON) {
                val front = nextZ + ENTITY_RADIUS
                if (collidesZFace(entity.x, entity.y, front)) {
                    val blockZ = floor(front).toInt()
                    nextZ = blockZ - ENTITY_RADIUS - COLLISION_EPSILON
                    entity.vz = 0.0
                }
            } else if (entity.vz < -VELOCITY_EPSILON) {
                val front = nextZ - ENTITY_RADIUS
                if (collidesZFace(entity.x, entity.y, front)) {
                    val blockZ = floor(front).toInt()
                    nextZ = blockZ + 1.0 + ENTITY_RADIUS + COLLISION_EPSILON
                    entity.vz = 0.0
                }
            }
            entity.z = nextZ
            if (stepOnGround) grounded = true
        }

        entity.onGround = grounded
        entity.vx *= AIR_DRAG.pow(tickScale)
        entity.vz *= AIR_DRAG.pow(tickScale)
        entity.vy *= AIR_DRAG.pow(tickScale)

        if (abs(entity.vx) < VELOCITY_EPSILON) entity.vx = 0.0
        if (abs(entity.vy) < VELOCITY_EPSILON) entity.vy = 0.0
        if (abs(entity.vz) < VELOCITY_EPSILON) entity.vz = 0.0
    }

    private fun movementSubsteps(dx: Double, dy: Double, dz: Double): Int {
        val maxDistance = maxOf(abs(dx), abs(dy), abs(dz))
        if (maxDistance <= MAX_SUBSTEP_DISTANCE) return 1
        val required = kotlin.math.ceil(maxDistance / MAX_SUBSTEP_DISTANCE).toLong()
        return required.coerceAtLeast(1L).coerceAtMost(Int.MAX_VALUE.toLong()).toInt()
    }

    private fun collidesXFace(testX: Double, baseY: Double, centerZ: Double): Boolean {
        val y1 = baseY + 0.01
        val y2 = baseY + ENTITY_HEIGHT * 0.5
        return isSolidAt(testX, y1, centerZ + ENTITY_RADIUS) ||
            isSolidAt(testX, y1, centerZ - ENTITY_RADIUS) ||
            isSolidAt(testX, y2, centerZ + ENTITY_RADIUS) ||
            isSolidAt(testX, y2, centerZ - ENTITY_RADIUS)
    }

    private fun collidesZFace(centerX: Double, baseY: Double, testZ: Double): Boolean {
        val y1 = baseY + 0.01
        val y2 = baseY + ENTITY_HEIGHT * 0.5
        return isSolidAt(centerX + ENTITY_RADIUS, y1, testZ) ||
            isSolidAt(centerX - ENTITY_RADIUS, y1, testZ) ||
            isSolidAt(centerX + ENTITY_RADIUS, y2, testZ) ||
            isSolidAt(centerX - ENTITY_RADIUS, y2, testZ)
    }

    private fun collidesFootprint(x: Double, y: Double, z: Double): Boolean {
        return isSolidAt(x + ENTITY_RADIUS, y, z + ENTITY_RADIUS) ||
            isSolidAt(x + ENTITY_RADIUS, y, z - ENTITY_RADIUS) ||
            isSolidAt(x - ENTITY_RADIUS, y, z + ENTITY_RADIUS) ||
            isSolidAt(x - ENTITY_RADIUS, y, z - ENTITY_RADIUS)
    }

    private fun isSolidAt(x: Double, y: Double, z: Double): Boolean {
        val stateId = blockStateAt(floor(x).toInt(), floor(y).toInt(), floor(z).toInt())
        return isCollidableState(stateId)
    }

    private fun isCollidableState(stateId: Int): Boolean {
        if (stateId == 0) return false
        return collidableStateCache.computeIfAbsent(stateId) { id ->
            !isFallThroughState(id)
        }
    }

    private fun isFallThroughState(stateId: Int): Boolean {
        if (stateId == 0) return true
        return fallThroughStateCache.computeIfAbsent(stateId) { id ->
            val blockKey = BlockStateRegistry.parsedState(id)?.blockKey ?: return@computeIfAbsent false
            blockKey in FALL_THROUGH_BLOCK_KEYS
        }
    }

    private fun isImpactFragileState(stateId: Int): Boolean {
        if (stateId <= 0) return false
        return impactFragileStateCache.computeIfAbsent(stateId) { id ->
            val blockKey = BlockStateRegistry.parsedState(id)?.blockKey ?: return@computeIfAbsent false
            blockKey in FALLING_BLOCK_IMPACT_BREAK_KEYS ||
                blockKey.endsWith("_button") ||
                blockKey.endsWith("_pressure_plate")
        }
    }

    private fun addToChunkIndex(chunkPos: ChunkPos, entityId: Int) {
        chunkIndex.computeIfAbsent(chunkPos) { ConcurrentHashMap.newKeySet<Int>() }.add(entityId)
    }

    private fun removeFromChunkIndex(chunkPos: ChunkPos, entityId: Int) {
        val ids = chunkIndex[chunkPos] ?: return
        ids.remove(entityId)
        if (ids.isEmpty()) {
            chunkIndex.remove(chunkPos, ids)
        }
    }

    private fun moveChunkIndex(entityId: Int, from: ChunkPos, to: ChunkPos) {
        if (from == to) return
        removeFromChunkIndex(from, entityId)
        addToChunkIndex(to, entityId)
    }

    private fun chunkXFromBlockX(x: Double): Int {
        return floor(x / 16.0).toInt()
    }

    private fun chunkZFromBlockZ(z: Double): Int {
        return floor(z / 16.0).toInt()
    }

    private companion object {
        private const val GRAVITY_PER_TICK = 0.04
        private const val AIR_DRAG = 0.98
        private const val POSITION_EPSILON = 1.0e-4
        private const val VELOCITY_EPSILON = 1.0e-6
        private const val ENTITY_RADIUS = 0.49
        private const val ENTITY_HEIGHT = 0.98
        private const val COLLISION_EPSILON = 1.0e-4
        private const val MAX_SUBSTEP_DISTANCE = 0.5
        private const val DESPAWN_Y = -128.0
        private const val WORLD_MIN_Y = -64
        private const val WORLD_MAX_Y = 319
        private val json = Json { ignoreUnknownKeys = true }
        private val FALL_THROUGH_BLOCK_KEYS = loadBlockTag("falling_block_pass_through")
        private val FALLING_BLOCK_IMPACT_BREAK_KEYS = loadBlockTag("falling_block_impact_break")

        private fun loadBlockTag(tagName: String): Set<String> {
            return resolveBlockTag(tagName, HashSet())
        }

        private fun resolveBlockTag(tagName: String, visited: MutableSet<String>): Set<String> {
            if (!visited.add(tagName)) return emptySet()
            val resourcePath = "/data/minecraft/tags/block/$tagName.json"
            val stream = FallingBlockSystem::class.java.getResourceAsStream(resourcePath) ?: return emptySet()
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
