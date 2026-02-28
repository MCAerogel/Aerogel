package org.macaroon3145.world

import org.macaroon3145.network.codec.BlockStateRegistry
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors
import java.util.concurrent.Future
import kotlin.math.abs
import kotlin.math.floor
import kotlin.math.pow

data class DroppedItemSnapshot(
    val entityId: Int,
    val uuid: UUID,
    val itemId: Int,
    val itemCount: Int,
    val x: Double,
    val y: Double,
    val z: Double,
    val vx: Double,
    val vy: Double,
    val vz: Double,
    val pickupDelaySeconds: Double,
    val onGround: Boolean,
    val chunkPos: ChunkPos
)

data class DroppedItemRemovedEvent(
    val entityId: Int,
    val chunkPos: ChunkPos
)

data class DroppedItemTickEvents(
    val spawned: List<DroppedItemSnapshot>,
    val updated: List<DroppedItemSnapshot>,
    val removed: List<DroppedItemRemovedEvent>
) {
    fun isEmpty(): Boolean {
        return spawned.isEmpty() && updated.isEmpty() && removed.isEmpty()
    }
}

class DroppedItemSystem(
    private val blockStateAt: (Int, Int, Int) -> Int
) {
    private data class MutableDroppedItem(
        val entityId: Int,
        val uuid: UUID,
        val itemId: Int,
        val itemCount: Int,
        var x: Double,
        var y: Double,
        var z: Double,
        var vx: Double,
        var vy: Double,
        var vz: Double,
        var pickupDelaySeconds: Double,
        var onGround: Boolean,
        var ageTicks: Double,
        var restTicks: Double,
        var sleeping: Boolean,
        var chunkX: Int,
        var chunkZ: Int
    ) {
        fun snapshot(): DroppedItemSnapshot {
            return DroppedItemSnapshot(
                entityId = entityId,
                uuid = uuid,
                itemId = itemId,
                itemCount = itemCount,
                x = x,
                y = y,
                z = z,
                vx = vx,
                vy = vy,
                vz = vz,
                pickupDelaySeconds = pickupDelaySeconds,
                onGround = onGround,
                chunkPos = ChunkPos(chunkX, chunkZ)
            )
        }
    }

    private data class ChunkState(
        val entities: ConcurrentHashMap<Int, MutableDroppedItem> = ConcurrentHashMap(),
        val inbound: ConcurrentLinkedQueue<MutableDroppedItem> = ConcurrentLinkedQueue()
    )

    private data class LaneTickResult(
        val spawned: MutableList<DroppedItemSnapshot> = ArrayList(),
        val updated: MutableList<DroppedItemSnapshot> = ArrayList(),
        val removed: MutableList<DroppedItemRemovedEvent> = ArrayList()
    )

    private data class PendingUnstuckTarget(
        val entityId: Int,
        val block: BlockPos
    )

    private val laneCount = Runtime.getRuntime().availableProcessors().coerceIn(1, 8)
    private val workers = Executors.newVirtualThreadPerTaskExecutor()
    private val laneChunks = Array(laneCount) { ConcurrentHashMap<ChunkPos, ChunkState>() }
    private val dirtyChunksByLane = Array(laneCount) { ConcurrentHashMap.newKeySet<ChunkPos>() }
    private val snapshots = ConcurrentHashMap<Int, DroppedItemSnapshot>()
    private val chunkIndex = ConcurrentHashMap<ChunkPos, MutableSet<Int>>()
    private val collidableStateCache = ConcurrentHashMap<Int, Boolean>()
    private val pendingBlockUnstuckRequests = ConcurrentLinkedQueue<BlockPos>()
    private val pendingLaneUnstuckTargets = Array(laneCount) { ConcurrentLinkedQueue<PendingUnstuckTarget>() }

    fun spawn(
        entityId: Int,
        itemId: Int,
        itemCount: Int,
        x: Double,
        y: Double,
        z: Double,
        vx: Double,
        vy: Double,
        vz: Double,
        pickupDelaySeconds: Double = DEFAULT_PICKUP_DELAY_SECONDS
    ): Boolean {
        if (entityId < 0 || itemId < 0 || itemCount <= 0) return false
        val chunkX = chunkXFromBlockX(x)
        val chunkZ = chunkZFromBlockZ(z)
        val entity = MutableDroppedItem(
            entityId = entityId,
            uuid = UUID.randomUUID(),
            itemId = itemId,
            itemCount = itemCount,
            x = x,
            y = y,
            z = z,
            vx = vx,
            vy = vy,
            vz = vz,
            pickupDelaySeconds = pickupDelaySeconds.coerceAtLeast(0.0),
            onGround = false,
            ageTicks = 0.0,
            restTicks = 0.0,
            sleeping = false,
            chunkX = chunkX,
            chunkZ = chunkZ
        )
        resolveSpawnEmbedding(entity)
        val initial = entity.snapshot()
        if (snapshots.putIfAbsent(entityId, initial) != null) return false
        addToChunkIndex(initial.chunkPos, entityId)
        val lane = laneFor(chunkX, chunkZ)
        laneChunks[lane]
            .computeIfAbsent(initial.chunkPos) { ChunkState() }
            .inbound
            .add(entity)
        dirtyChunksByLane[lane].add(initial.chunkPos)
        return true
    }

    fun requestUnstuckForBlock(blockX: Int, blockY: Int, blockZ: Int) {
        if (snapshots.isEmpty()) return
        val stateId = blockStateAt(blockX, blockY, blockZ)
        if (!isCollidableState(stateId)) return
        pendingBlockUnstuckRequests.add(BlockPos(blockX, blockY, blockZ))
    }

    fun snapshot(entityId: Int): DroppedItemSnapshot? {
        return snapshots[entityId]
    }

    fun remove(entityId: Int): DroppedItemSnapshot? {
        val removed = snapshots.remove(entityId) ?: return null
        removeFromChunkIndex(removed.chunkPos, entityId)
        dirtyChunksByLane[laneFor(removed.chunkPos.x, removed.chunkPos.z)].add(removed.chunkPos)
        return removed
    }

    fun hasEntities(): Boolean {
        return snapshots.isNotEmpty()
    }

    fun snapshotsInChunk(chunkX: Int, chunkZ: Int): List<DroppedItemSnapshot> {
        val chunkPos = ChunkPos(chunkX, chunkZ)
        val ids = chunkIndex[chunkPos] ?: return emptyList()
        if (ids.isEmpty()) return emptyList()
        val out = ArrayList<DroppedItemSnapshot>(ids.size)
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
        chunkTimeRecorder: ((ChunkPos, Long) -> Unit)? = null
    ): DroppedItemTickEvents {
        return tickInternal(deltaSeconds, activeSimulationChunks, chunkTimeRecorder)
    }

    private fun tickInternal(
        deltaSeconds: Double,
        activeSimulationChunks: Set<ChunkPos>?,
        chunkTimeRecorder: ((ChunkPos, Long) -> Unit)?
    ): DroppedItemTickEvents {
        if (snapshots.isEmpty()) return DroppedItemTickEvents(emptyList(), emptyList(), emptyList())
        if (deltaSeconds <= 0.0) return DroppedItemTickEvents(emptyList(), emptyList(), emptyList())
        collectPendingUnstuckTargets()
        val tickScale = deltaSeconds * 20.0

        if (laneCount == 1) {
            val lane = processLane(0, deltaSeconds, tickScale, activeSimulationChunks, chunkTimeRecorder)
            return DroppedItemTickEvents(lane.spawned, lane.updated, lane.removed)
        }

        val futures = ArrayList<Future<LaneTickResult>>(laneCount)
        for (lane in 0 until laneCount) {
            futures.add(
                workers.submit<LaneTickResult> {
                    processLane(lane, deltaSeconds, tickScale, activeSimulationChunks, chunkTimeRecorder)
                }
            )
        }

        val spawned = ArrayList<DroppedItemSnapshot>()
        val updated = ArrayList<DroppedItemSnapshot>()
        val removed = ArrayList<DroppedItemRemovedEvent>()
        for (future in futures) {
            val lane = try {
                future.get()
            } catch (_: Throwable) {
                continue
            }
            spawned.addAll(lane.spawned)
            updated.addAll(lane.updated)
            removed.addAll(lane.removed)
        }
        return DroppedItemTickEvents(spawned, updated, removed)
    }

    private fun processLane(
        lane: Int,
        deltaSeconds: Double,
        tickScale: Double,
        activeSimulationChunks: Set<ChunkPos>?,
        chunkTimeRecorder: ((ChunkPos, Long) -> Unit)?
    ): LaneTickResult {
        val laneMap = laneChunks[lane]
        if (laneMap.isEmpty()) return LaneTickResult()

        val result = LaneTickResult()
        var laneUnstuckTargets = drainLaneUnstuckTargets(lane)
        val dirtyChunks = dirtyChunksByLane[lane]
        val chunkPositions = collectLaneChunkPositions(lane, laneMap, dirtyChunks, activeSimulationChunks)
        if (chunkPositions.isEmpty()) return result

        for (chunkPos in chunkPositions) {
            val startedAtNanos = System.nanoTime()
            val chunkState = laneMap[chunkPos] ?: continue
            val shouldSimulate = activeSimulationChunks == null || activeSimulationChunks.contains(chunkPos)
            val spawnedIds = HashSet<Int>()
            while (true) {
                val incoming = chunkState.inbound.poll() ?: break
                if (!snapshots.containsKey(incoming.entityId)) continue
                chunkState.entities[incoming.entityId] = incoming
                spawnedIds.add(incoming.entityId)
            }
            if (chunkState.entities.isEmpty()) {
                laneMap.remove(chunkPos, chunkState)
                dirtyChunks.remove(chunkPos)
                continue
            }

            for ((entityId, entity) in chunkState.entities) {
                if (!snapshots.containsKey(entityId)) {
                    chunkState.entities.remove(entityId, entity)
                    continue
                }
                val oldX = entity.x
                val oldY = entity.y
                val oldZ = entity.z
                val oldOnGround = entity.onGround
                val oldChunk = ChunkPos(entity.chunkX, entity.chunkZ)
                val oldPickupDelay = entity.pickupDelaySeconds
                if (laneUnstuckTargets != null) {
                    val targetBlock = laneUnstuckTargets.remove(entityId)
                    if (targetBlock != null) {
                        tryUnstuckFromBlock(entity, targetBlock)
                    }
                    if (laneUnstuckTargets.isEmpty()) {
                        laneUnstuckTargets = null
                    }
                }

                if (!shouldSimulate) {
                    if (spawnedIds.remove(entityId)) {
                        snapshots[entityId] = entity.snapshot()
                    }
                    continue
                }

                if (entity.sleeping) {
                    entity.ageTicks += tickScale
                    entity.pickupDelaySeconds = (entity.pickupDelaySeconds - deltaSeconds).coerceAtLeast(0.0)
                    val expired = entity.ageTicks >= MAX_AGE_TICKS || entity.y < DESPAWN_Y
                    if (expired) {
                        if (chunkState.entities.remove(entityId, entity)) {
                            snapshots.remove(entityId)
                            removeFromChunkIndex(oldChunk, entityId)
                            result.removed.add(DroppedItemRemovedEvent(entityId, oldChunk))
                        }
                    } else if (oldPickupDelay != entity.pickupDelaySeconds) {
                        snapshots[entityId] = entity.snapshot()
                    }
                    continue
                }

                stepEntity(entity, deltaSeconds, tickScale)

                val expired = entity.ageTicks >= MAX_AGE_TICKS || entity.y < DESPAWN_Y
                if (expired) {
                    if (chunkState.entities.remove(entityId, entity)) {
                        snapshots.remove(entityId)
                        removeFromChunkIndex(oldChunk, entityId)
                        result.removed.add(DroppedItemRemovedEvent(entityId, oldChunk))
                    }
                    continue
                }

                val newChunkX = chunkXFromBlockX(entity.x)
                val newChunkZ = chunkZFromBlockZ(entity.z)
                val newChunk = ChunkPos(newChunkX, newChunkZ)
                val hasActiveVelocity =
                    abs(entity.vx) > UPDATE_VELOCITY_EPSILON ||
                        abs(entity.vy) > UPDATE_VELOCITY_EPSILON ||
                        abs(entity.vz) > UPDATE_VELOCITY_EPSILON
                val pickupDelayChanged = oldPickupDelay != entity.pickupDelaySeconds
                val moved =
                    abs(entity.x - oldX) > POSITION_EPSILON ||
                        abs(entity.y - oldY) > POSITION_EPSILON ||
                        abs(entity.z - oldZ) > POSITION_EPSILON ||
                        oldChunk != newChunk ||
                        oldOnGround != entity.onGround ||
                        hasActiveVelocity

                if (oldChunk != newChunk) {
                    if (!chunkState.entities.remove(entityId, entity)) continue
                    entity.chunkX = newChunkX
                    entity.chunkZ = newChunkZ
                    moveChunkIndex(entityId, oldChunk, newChunk)
                    val targetLane = laneFor(newChunkX, newChunkZ)
                    laneChunks[targetLane]
                        .computeIfAbsent(newChunk) { ChunkState() }
                        .inbound
                        .add(entity)
                    dirtyChunksByLane[targetLane].add(newChunk)
                }

                if (spawnedIds.remove(entityId)) {
                    val snapshot = entity.snapshot()
                    snapshots[entityId] = snapshot
                    result.spawned.add(snapshot)
                } else if (moved) {
                    val snapshot = entity.snapshot()
                    snapshots[entityId] = snapshot
                    result.updated.add(snapshot)
                } else if (pickupDelayChanged) {
                    snapshots[entityId] = entity.snapshot()
                }
            }

            if (chunkState.entities.isEmpty() && chunkState.inbound.isEmpty()) {
                laneMap.remove(chunkPos, chunkState)
                dirtyChunks.remove(chunkPos)
            } else if (!shouldSimulate) {
                dirtyChunks.remove(chunkPos)
            }
            chunkTimeRecorder?.invoke(chunkPos, System.nanoTime() - startedAtNanos)
        }
        return result
    }

    private fun collectLaneChunkPositions(
        lane: Int,
        laneMap: ConcurrentHashMap<ChunkPos, ChunkState>,
        dirtyChunks: MutableSet<ChunkPos>,
        activeSimulationChunks: Set<ChunkPos>?
    ): LinkedHashSet<ChunkPos> {
        val out = LinkedHashSet<ChunkPos>()
        if (activeSimulationChunks == null) {
            out.addAll(laneMap.keys)
            return out
        }
        for (chunkPos in activeSimulationChunks) {
            if (laneFor(chunkPos.x, chunkPos.z) != lane) continue
            if (laneMap.containsKey(chunkPos)) {
                out.add(chunkPos)
            }
        }
        out.addAll(dirtyChunks)
        return out
    }

    private fun stepEntity(entity: MutableDroppedItem, deltaSeconds: Double, tickScale: Double) {
        entity.ageTicks += tickScale
        entity.pickupDelaySeconds = (entity.pickupDelaySeconds - deltaSeconds).coerceAtLeast(0.0)
        entity.vy -= GRAVITY_PER_TICK * tickScale

        // Split fast movement into a few sub-steps to avoid tunneling through side/top blocks.
        val totalDx = entity.vx * tickScale
        val totalDy = entity.vy * tickScale
        val totalDz = entity.vz * tickScale
        val steps = movementSubsteps(totalDx, totalDy, totalDz)
        val subScale = tickScale / steps
        var grounded = false

        repeat(steps) {
            var nextY = entity.y + entity.vy * subScale
            var stepOnGround = false
            if (entity.vy > VELOCITY_SNAP_EPSILON) {
                val head = nextY + ENTITY_HEIGHT
                if (collidesFootprint(entity.x, head, entity.z)) {
                    val blockY = floor(head).toInt()
                    nextY = blockY - ENTITY_HEIGHT - COLLISION_EPSILON
                    entity.vy = 0.0
                }
            } else if (entity.vy <= VELOCITY_SNAP_EPSILON) {
                val feet = nextY - 0.01
                if (collidesFootprint(entity.x, feet, entity.z)) {
                    val blockY = floor(feet).toInt()
                    nextY = blockY + 1.0
                    entity.vy = 0.0
                    stepOnGround = true
                }
            }
            entity.y = nextY

            var nextX = entity.x + entity.vx * subScale
            if (entity.vx > VELOCITY_SNAP_EPSILON) {
                val front = nextX + ENTITY_RADIUS
                if (collidesXFace(front, entity.y, entity.z)) {
                    val blockX = floor(front).toInt()
                    nextX = blockX - ENTITY_RADIUS - COLLISION_EPSILON
                    entity.vx = 0.0
                }
            } else if (entity.vx < -VELOCITY_SNAP_EPSILON) {
                val front = nextX - ENTITY_RADIUS
                if (collidesXFace(front, entity.y, entity.z)) {
                    val blockX = floor(front).toInt()
                    nextX = blockX + 1.0 + ENTITY_RADIUS + COLLISION_EPSILON
                    entity.vx = 0.0
                }
            }
            entity.x = nextX

            var nextZ = entity.z + entity.vz * subScale
            if (entity.vz > VELOCITY_SNAP_EPSILON) {
                val front = nextZ + ENTITY_RADIUS
                if (collidesZFace(entity.x, entity.y, front)) {
                    val blockZ = floor(front).toInt()
                    nextZ = blockZ - ENTITY_RADIUS - COLLISION_EPSILON
                    entity.vz = 0.0
                }
            } else if (entity.vz < -VELOCITY_SNAP_EPSILON) {
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

        val horizontalDrag = if (entity.onGround) GROUND_DRAG else AIR_DRAG
        val horizontalScale = horizontalDrag.pow(tickScale)
        entity.vx *= horizontalScale
        entity.vz *= horizontalScale
        entity.vy *= AIR_DRAG.pow(tickScale)

        if (abs(entity.vx) < VELOCITY_SNAP_EPSILON) entity.vx = 0.0
        if (abs(entity.vy) < VELOCITY_SNAP_EPSILON) entity.vy = 0.0
        if (abs(entity.vz) < VELOCITY_SNAP_EPSILON) entity.vz = 0.0

        if (entity.onGround && entity.vx == 0.0 && entity.vy == 0.0 && entity.vz == 0.0) {
            entity.restTicks += tickScale
            if (entity.restTicks >= SLEEP_AFTER_TICKS) {
                entity.sleeping = true
            }
        } else {
            entity.restTicks = 0.0
            entity.sleeping = false
        }
    }

    private fun resolveSpawnEmbedding(entity: MutableDroppedItem) {
        if (!isEmbedded(entity.x, entity.y, entity.z)) return
        val base = floor(entity.y).toInt()
        for (offset in 0..4) {
            val candidateY = base + offset + 1.0 + COLLISION_EPSILON
            if (isEmbedded(entity.x, candidateY, entity.z)) continue
            entity.y = candidateY
            entity.vy = 0.0
            entity.onGround = collidesFootprint(entity.x, entity.y - 0.01, entity.z)
            entity.sleeping = false
            entity.restTicks = 0.0
            return
        }
    }

    private fun isEmbedded(x: Double, y: Double, z: Double): Boolean {
        return isSolidAt(x, y + 0.01, z) ||
            isSolidAt(x, y + (ENTITY_HEIGHT * 0.5), z) ||
            isSolidAt(x, y + ENTITY_HEIGHT - 0.01, z)
    }

    private fun collectPendingUnstuckTargets() {
        var processed = 0
        while (processed < MAX_BLOCK_UNSTUCK_REQUESTS_PER_TICK) {
            val block = pendingBlockUnstuckRequests.poll() ?: break
            processed++
            val stateId = blockStateAt(block.x, block.y, block.z)
            if (!isCollidableState(stateId)) continue

            val minChunkX = chunkXFromBlockX(block.x.toDouble() - ENTITY_RADIUS)
            val maxChunkX = chunkXFromBlockX(block.x.toDouble() + 1.0 + ENTITY_RADIUS)
            val minChunkZ = chunkZFromBlockZ(block.z.toDouble() - ENTITY_RADIUS)
            val maxChunkZ = chunkZFromBlockZ(block.z.toDouble() + 1.0 + ENTITY_RADIUS)

            for (chunkX in minChunkX..maxChunkX) {
                for (chunkZ in minChunkZ..maxChunkZ) {
                    val ids = chunkIndex[ChunkPos(chunkX, chunkZ)] ?: continue
                    for (entityId in ids) {
                        val snapshot = snapshots[entityId] ?: continue
                        if (!isEntityAabbPenetratingBlock(snapshot.x, snapshot.y, snapshot.z, block)) continue
                        val lane = laneFor(snapshot.chunkPos.x, snapshot.chunkPos.z)
                        pendingLaneUnstuckTargets[lane].add(PendingUnstuckTarget(entityId, block))
                    }
                }
            }
        }
    }

    private fun drainLaneUnstuckTargets(lane: Int): MutableMap<Int, BlockPos>? {
        val queue = pendingLaneUnstuckTargets[lane]
        if (queue.isEmpty()) return null
        val collected = HashMap<Int, BlockPos>()
        while (true) {
            val target = queue.poll() ?: break
            val snapshot = snapshots[target.entityId] ?: continue
            if (laneFor(snapshot.chunkPos.x, snapshot.chunkPos.z) != lane) continue
            collected[target.entityId] = target.block
        }
        return if (collected.isEmpty()) null else collected
    }

    private fun tryUnstuckFromBlock(entity: MutableDroppedItem, block: BlockPos) {
        if (!isEntityAabbPenetratingBlock(entity.x, entity.y, entity.z, block)) return
        if (!isEmbedded(entity.x, entity.y, entity.z)) return

        val leftX = block.x - ENTITY_RADIUS - COLLISION_EPSILON
        val rightX = block.x + 1.0 + ENTITY_RADIUS + COLLISION_EPSILON
        val downY = block.y - ENTITY_HEIGHT - COLLISION_EPSILON
        val upY = block.y + 1.0 + COLLISION_EPSILON
        val backZ = block.z - ENTITY_RADIUS - COLLISION_EPSILON
        val frontZ = block.z + 1.0 + ENTITY_RADIUS + COLLISION_EPSILON

        val options = arrayOf(
            UnstuckOption(abs(leftX - entity.x), leftX, entity.y, entity.z, clearX = true, clearY = false, clearZ = false),
            UnstuckOption(abs(rightX - entity.x), rightX, entity.y, entity.z, clearX = true, clearY = false, clearZ = false),
            UnstuckOption(abs(downY - entity.y), entity.x, downY, entity.z, clearX = false, clearY = true, clearZ = false),
            UnstuckOption(abs(upY - entity.y), entity.x, upY, entity.z, clearX = false, clearY = true, clearZ = false),
            UnstuckOption(abs(backZ - entity.z), entity.x, entity.y, backZ, clearX = false, clearY = false, clearZ = true),
            UnstuckOption(abs(frontZ - entity.z), entity.x, entity.y, frontZ, clearX = false, clearY = false, clearZ = true)
        )
        options.sortBy { it.distance }

        for (option in options) {
            if (!canOccupy(option.x, option.y, option.z)) continue
            if (isEntityAabbPenetratingBlock(option.x, option.y, option.z, block)) continue
            entity.x = option.x
            entity.y = option.y
            entity.z = option.z
            if (option.clearX) entity.vx = 0.0
            if (option.clearY) entity.vy = 0.0
            if (option.clearZ) entity.vz = 0.0
            entity.onGround = collidesFootprint(entity.x, entity.y - 0.01, entity.z)
            entity.sleeping = false
            entity.restTicks = 0.0
            return
        }
    }

    private fun canOccupy(x: Double, y: Double, z: Double): Boolean {
        if (isSolidAt(x, y + 0.01, z)) return false
        if (isSolidAt(x, y + (ENTITY_HEIGHT * 0.5), z)) return false
        if (isSolidAt(x, y + ENTITY_HEIGHT - 0.01, z)) return false
        if (collidesFootprint(x, y + 0.01, z)) return false
        if (collidesFootprint(x, y + ENTITY_HEIGHT - 0.01, z)) return false
        return true
    }

    private fun isEntityAabbPenetratingBlock(x: Double, y: Double, z: Double, block: BlockPos): Boolean {
        val minX = x - ENTITY_RADIUS
        val maxX = x + ENTITY_RADIUS
        val minY = y
        val maxY = y + ENTITY_HEIGHT
        val minZ = z - ENTITY_RADIUS
        val maxZ = z + ENTITY_RADIUS

        val bx0 = block.x.toDouble()
        val by0 = block.y.toDouble()
        val bz0 = block.z.toDouble()
        val bx1 = bx0 + 1.0
        val by1 = by0 + 1.0
        val bz1 = bz0 + 1.0
        // Strict overlap on every axis: touching a face/edge/corner is not penetration.
        return maxX > bx0 + UNSTUCK_OVERLAP_EPS &&
            minX < bx1 - UNSTUCK_OVERLAP_EPS &&
            maxY > by0 + UNSTUCK_OVERLAP_EPS &&
            minY < by1 - UNSTUCK_OVERLAP_EPS &&
            maxZ > bz0 + UNSTUCK_OVERLAP_EPS &&
            minZ < bz1 - UNSTUCK_OVERLAP_EPS
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
            val blockKey = BlockStateRegistry.parsedState(id)?.blockKey ?: return@computeIfAbsent true
            blockKey !in NON_COLLIDING_BLOCK_KEYS
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

    private fun laneFor(chunkX: Int, chunkZ: Int): Int {
        val hash = (chunkX * 73_428_767) xor (chunkZ * 91_236_791)
        return (hash and Int.MAX_VALUE) % laneCount
    }

    private fun chunkXFromBlockX(x: Double): Int {
        return floor(x / 16.0).toInt()
    }

    private fun chunkZFromBlockZ(z: Double): Int {
        return floor(z / 16.0).toInt()
    }

    private companion object {
        private data class UnstuckOption(
            val distance: Double,
            val x: Double,
            val y: Double,
            val z: Double,
            val clearX: Boolean,
            val clearY: Boolean,
            val clearZ: Boolean
        )

        private const val GRAVITY_PER_TICK = 0.04
        private const val AIR_DRAG = 0.98
        private const val GROUND_DRAG = 0.60
        private const val POSITION_EPSILON = 1.0e-4
        private const val VELOCITY_SNAP_EPSILON = 1.0e-6
        private const val UPDATE_VELOCITY_EPSILON = 1.0e-7
        private const val ENTITY_RADIUS = 0.125
        private const val ENTITY_HEIGHT = 0.25
        private const val COLLISION_EPSILON = 1.0e-4
        private const val MAX_SUBSTEP_DISTANCE = 0.35
        private const val UNSTUCK_OVERLAP_EPS = 1.0e-3
        private const val MAX_BLOCK_UNSTUCK_REQUESTS_PER_TICK = 256
        private const val SLEEP_AFTER_TICKS = 8.0
        private const val DEFAULT_PICKUP_DELAY_SECONDS = 2.0
        private const val MAX_AGE_TICKS = 6000.0
        private const val DESPAWN_Y = -128.0
        private val json = Json { ignoreUnknownKeys = true }
        private val NON_COLLIDING_BLOCK_KEYS = loadBlockTag("dropped_item_non_colliding")

        private fun loadBlockTag(tagName: String): Set<String> {
            return resolveBlockTag(tagName, HashSet())
        }

        private fun resolveBlockTag(tagName: String, visited: MutableSet<String>): Set<String> {
            if (!visited.add(tagName)) return emptySet()
            val resourcePath = "/data/minecraft/tags/block/$tagName.json"
            val stream = DroppedItemSystem::class.java.getResourceAsStream(resourcePath) ?: return emptySet()
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
