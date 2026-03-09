package org.macaroon3145.world

import org.macaroon3145.network.codec.BlockStateRegistry
import org.macaroon3145.network.handler.ItemStackState
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import kotlin.math.abs
import kotlin.math.floor
import kotlin.math.sqrt
import kotlin.math.pow

data class DroppedItemSnapshot(
    val entityId: Int,
    val uuid: UUID,
    val stack: ItemStackState,
    val x: Double,
    val y: Double,
    val z: Double,
    val vx: Double,
    val vy: Double,
    val vz: Double,
    val accelerationX: Double = 0.0,
    val accelerationY: Double = 0.0,
    val accelerationZ: Double = 0.0,
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

data class DroppedItemConsumeResult(
    val consumedCount: Int,
    val removed: DroppedItemSnapshot?,
    val updated: DroppedItemSnapshot?
)

private fun copyDroppedItemStackState(stack: ItemStackState): ItemStackState {
    return ItemStackState.of(
        itemId = stack.itemId,
        count = stack.count,
        shulkerContents = copyDroppedChestState(stack.shulkerContents)
    )
}

private fun copyDroppedChestState(chest: org.macaroon3145.network.handler.ChestState?): org.macaroon3145.network.handler.ChestState? {
    if (chest == null) return null
    val nested = Array(chest.shulkerContents.size) { index ->
        copyDroppedChestState(chest.shulkerContents[index])
    }
    return org.macaroon3145.network.handler.ChestState(
        itemIds = chest.itemIds.copyOf(),
        itemCounts = chest.itemCounts.copyOf(),
        shulkerContents = nested
    )
}

class DroppedItemSystem(
    private val blockStateAt: (Int, Int, Int) -> Int
) {
    private data class FlowVector2(
        val x: Double,
        val z: Double
    )

    private data class WaterScanXzRange(
        val startX: Int,
        val endX: Int,
        val startZ: Int,
        val endZ: Int
    )

    private data class MutableDroppedItem(
        val entityId: Int,
        val uuid: UUID,
        var stack: ItemStackState,
        var x: Double,
        var y: Double,
        var z: Double,
        var vx: Double,
        var vy: Double,
        var vz: Double,
        var accelerationX: Double,
        var accelerationY: Double,
        var accelerationZ: Double,
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
                stack = copyDroppedItemStackState(stack),
                x = x,
                y = y,
                z = z,
                vx = vx,
                vy = vy,
                vz = vz,
                accelerationX = accelerationX,
                accelerationY = accelerationY,
                accelerationZ = accelerationZ,
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
    private val pendingBlockUnstuckRequests = ConcurrentLinkedQueue<BlockPos>()
    private val pendingLaneUnstuckTargets = Array(laneCount) { ConcurrentLinkedQueue<PendingUnstuckTarget>() }
    private val pendingChunkActorTickEvents = ConcurrentLinkedQueue<DroppedItemTickEvents>()
    private val chunkTickInFlight = ConcurrentHashMap<ChunkPos, AtomicBoolean>()
    private val pendingMergePullImpulses = ConcurrentHashMap<Int, DoubleArray>()

    internal fun spawn(
        entityId: Int,
        stack: ItemStackState,
        x: Double,
        y: Double,
        z: Double,
        vx: Double,
        vy: Double,
        vz: Double,
        uuid: UUID = UUID.randomUUID(),
        pickupDelaySeconds: Double = DEFAULT_PICKUP_DELAY_SECONDS
    ): Boolean {
        val normalizedStack = copyDroppedItemStackState(stack)
        if (entityId < 0 || normalizedStack.itemId < 0 || normalizedStack.count <= 0) return false
        val chunkX = chunkXFromBlockX(x)
        val chunkZ = chunkZFromBlockZ(z)
        val entity = MutableDroppedItem(
            entityId = entityId,
            uuid = uuid,
            stack = normalizedStack,
            x = x,
            y = y,
            z = z,
            vx = vx,
            vy = vy,
            vz = vz,
            accelerationX = 0.0,
            accelerationY = 0.0,
            accelerationZ = 0.0,
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

    internal fun requestUnstuckForBlock(blockX: Int, blockY: Int, blockZ: Int) {
        if (snapshots.isEmpty()) return
        val stateId = blockStateAt(blockX, blockY, blockZ)
        if (!isCollidableState(stateId)) return
        pendingBlockUnstuckRequests.add(BlockPos(blockX, blockY, blockZ))
    }

    fun snapshot(entityId: Int): DroppedItemSnapshot? {
        return snapshots[entityId]
    }

    fun snapshots(): List<DroppedItemSnapshot> {
        return snapshots.values.toList()
    }

    fun snapshotByUuid(uuid: UUID): DroppedItemSnapshot? {
        return snapshots.values.firstOrNull { it.uuid == uuid }
    }

    internal fun updateItem(entityId: Int, itemId: Int, itemCount: Int): DroppedItemSnapshot? {
        return updateStack(entityId, ItemStackState.of(itemId = itemId, count = itemCount))
    }

    internal fun updateStack(entityId: Int, stack: ItemStackState): DroppedItemSnapshot? {
        val normalized = copyDroppedItemStackState(stack)
        if (normalized.itemId < 0 || normalized.count <= 0) return null
        return mutate(entityId) { entity ->
            entity.stack = normalized
        }
    }

    internal fun updatePosition(entityId: Int, x: Double, y: Double, z: Double): DroppedItemSnapshot? {
        if (!x.isFinite() || !y.isFinite() || !z.isFinite()) return null
        return mutate(entityId) { entity ->
            entity.x = x
            entity.y = y
            entity.z = z
            entity.sleeping = false
            entity.restTicks = 0.0
        }
    }

    internal fun updateVelocity(entityId: Int, vx: Double, vy: Double, vz: Double): DroppedItemSnapshot? {
        if (!vx.isFinite() || !vy.isFinite() || !vz.isFinite()) return null
        return mutate(entityId) { entity ->
            entity.vx = vx
            entity.vy = vy
            entity.vz = vz
            entity.sleeping = false
            entity.restTicks = 0.0
        }
    }

    internal fun updateAcceleration(entityId: Int, ax: Double, ay: Double, az: Double): DroppedItemSnapshot? {
        if (!ax.isFinite() || !ay.isFinite() || !az.isFinite()) return null
        return mutate(entityId) { entity ->
            entity.accelerationX = ax
            entity.accelerationY = ay
            entity.accelerationZ = az
            entity.sleeping = false
            entity.restTicks = 0.0
        }
    }

    internal fun updatePickupDelay(entityId: Int, pickupDelaySeconds: Double): DroppedItemSnapshot? {
        if (!pickupDelaySeconds.isFinite()) return null
        return mutate(entityId) { entity ->
            entity.pickupDelaySeconds = pickupDelaySeconds.coerceAtLeast(0.0)
        }
    }

    internal fun updateOnGround(entityId: Int, onGround: Boolean): DroppedItemSnapshot? {
        return mutate(entityId) { entity ->
            entity.onGround = onGround
            if (!onGround) {
                entity.sleeping = false
                entity.restTicks = 0.0
            }
        }
    }

    internal fun remove(entityId: Int): DroppedItemSnapshot? {
        val removed = snapshots.remove(entityId) ?: return null
        clearPendingMergePull(entityId)
        removeFromChunkIndex(removed.chunkPos, entityId)
        dirtyChunksByLane[laneFor(removed.chunkPos.x, removed.chunkPos.z)].add(removed.chunkPos)
        return removed
    }

    internal fun removeIfUuidMatches(entityId: Int, expectedUuid: UUID): DroppedItemSnapshot? {
        var removed: DroppedItemSnapshot? = null
        snapshots.compute(entityId) { _, current ->
            if (current != null && current.uuid == expectedUuid) {
                removed = current
                null
            } else {
                current
            }
        }
        val removedSnapshot = removed ?: return null
        clearPendingMergePull(entityId)
        removeFromChunkIndex(removedSnapshot.chunkPos, entityId)
        dirtyChunksByLane[laneFor(removedSnapshot.chunkPos.x, removedSnapshot.chunkPos.z)].add(removedSnapshot.chunkPos)
        return removedSnapshot
    }

    internal fun removeIfMatches(entityId: Int, expected: DroppedItemSnapshot): DroppedItemSnapshot? {
        var removed: DroppedItemSnapshot? = null
        snapshots.compute(entityId) { _, current ->
            if (current != null && current == expected) {
                removed = current
                null
            } else {
                current
            }
        }
        val removedSnapshot = removed ?: return null
        clearPendingMergePull(entityId)
        removeFromChunkIndex(removedSnapshot.chunkPos, entityId)
        dirtyChunksByLane[laneFor(removedSnapshot.chunkPos.x, removedSnapshot.chunkPos.z)].add(removedSnapshot.chunkPos)
        return removedSnapshot
    }

    internal fun consumeIfMatches(entityId: Int, expected: DroppedItemSnapshot, consumeCount: Int): DroppedItemConsumeResult? {
        if (consumeCount <= 0) return null
        if (!tryLockEntity(entityId)) return null
        var consumed = 0
        var removedSnapshot: DroppedItemSnapshot? = null
        var updatedSnapshot: DroppedItemSnapshot? = null

        try {
            snapshots.compute(entityId) { _, current ->
                if (current == null) return@compute null
                if (current.uuid != expected.uuid) return@compute current
                if (current.stack.itemId != expected.stack.itemId) return@compute current
                if (!areStackMetadataEqual(current.stack, expected.stack)) return@compute current

                val clampedConsume = consumeCount.coerceAtMost(current.stack.count)
                if (clampedConsume <= 0) return@compute current
                consumed = clampedConsume

                if (clampedConsume >= current.stack.count) {
                    removedSnapshot = current
                    null
                } else {
                    val remainingCount = current.stack.count - clampedConsume
                    val updated = current.copy(
                        stack = copyDroppedItemStackState(current.stack.copy(count = remainingCount))
                    )
                    updatedSnapshot = updated
                    updated
                }
            }
        } finally {
            unlockEntity(entityId)
        }

        if (consumed <= 0) return null
        val removed = removedSnapshot
        if (removed != null) {
            clearPendingMergePull(entityId)
            removeFromChunkIndex(removed.chunkPos, entityId)
            dirtyChunksByLane[laneFor(removed.chunkPos.x, removed.chunkPos.z)].add(removed.chunkPos)
            return DroppedItemConsumeResult(consumedCount = consumed, removed = removed, updated = null)
        }

        val updated = updatedSnapshot ?: return null
        dirtyChunksByLane[laneFor(updated.chunkPos.x, updated.chunkPos.z)].add(updated.chunkPos)
        return DroppedItemConsumeResult(consumedCount = consumed, removed = null, updated = updated)
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
        return tickOnChunkActors(
            deltaSeconds = deltaSeconds,
            activeSimulationChunks = activeSimulationChunks,
            submitChunkTask = { _, task ->
                workers.submit<DroppedItemTickEvents> { task() }
            },
            chunkTimeRecorder = chunkTimeRecorder
        )
    }

    fun tickOnChunkActors(
        deltaSeconds: Double,
        activeSimulationChunks: Set<ChunkPos>? = null,
        submitChunkTask: (ChunkPos, () -> DroppedItemTickEvents) -> Future<DroppedItemTickEvents>,
        chunkDeltaSecondsProvider: ((ChunkPos) -> Double)? = null,
        onChunkEvents: ((DroppedItemTickEvents) -> Unit)? = null,
        onDispatchComplete: (() -> Unit)? = null,
        chunkTimeRecorder: ((ChunkPos, Long) -> Unit)? = null,
        awaitCompletion: Boolean = false
    ): DroppedItemTickEvents {
        val ready = drainPendingChunkActorTickEvents()
        if (snapshots.isEmpty() || deltaSeconds <= 0.0) {
            onDispatchComplete?.invoke()
            return ready
        }
        collectPendingUnstuckTargets()
        val pendingTasks = java.util.concurrent.atomic.AtomicInteger(0)
        val scheduled = if (awaitCompletion) ArrayList<Future<DroppedItemTickEvents>>() else null

        val laneUnstuckTargets = arrayOfNulls<ConcurrentHashMap<Int, BlockPos>>(laneCount)
        for (lane in 0 until laneCount) {
            val drained = drainLaneUnstuckTargets(lane)
            if (drained != null && drained.isNotEmpty()) {
                laneUnstuckTargets[lane] = ConcurrentHashMap(drained)
            }
        }

        for (lane in 0 until laneCount) {
            val laneMap = laneChunks[lane]
            if (laneMap.isEmpty()) continue
            val dirtyChunks = dirtyChunksByLane[lane]
            val chunkPositions = collectLaneChunkPositions(lane, laneMap, dirtyChunks, activeSimulationChunks)
            if (chunkPositions.isEmpty()) continue
            for (chunkPos in chunkPositions) {
                if (!tryAcquireChunkTick(chunkPos)) {
                    continue
                }
                pendingTasks.incrementAndGet()
                val submitted = submitChunkTask(chunkPos) {
                    try {
                        val chunkDeltaSeconds = (chunkDeltaSecondsProvider?.invoke(chunkPos) ?: deltaSeconds)
                        if (chunkDeltaSeconds <= 0.0) {
                            return@submitChunkTask DroppedItemTickEvents(emptyList(), emptyList(), emptyList())
                        }
                        val chunkResult = processChunk(
                            lane = lane,
                            chunkPos = chunkPos,
                            deltaSeconds = chunkDeltaSeconds,
                            tickScale = chunkDeltaSeconds * 20.0,
                            activeSimulationChunks = activeSimulationChunks,
                            chunkTimeRecorder = chunkTimeRecorder,
                            laneUnstuckTargets = laneUnstuckTargets[lane]
                        )
                        val events = DroppedItemTickEvents(
                            spawned = chunkResult.spawned,
                            updated = chunkResult.updated,
                            removed = chunkResult.removed
                        )
                        if (!events.isEmpty()) {
                            if (onChunkEvents != null) {
                                onChunkEvents(events)
                            } else if (!awaitCompletion) {
                                pendingChunkActorTickEvents.add(events)
                            }
                        }
                        return@submitChunkTask events
                    } finally {
                        releaseChunkTick(chunkPos)
                        if (onDispatchComplete != null) {
                            val remaining = pendingTasks.decrementAndGet()
                            if (remaining == 0) {
                                onDispatchComplete()
                            }
                        }
                    }
                }
                if (scheduled != null) {
                    scheduled.add(submitted)
                }
            }
        }
        if (pendingTasks.get() == 0) {
            onDispatchComplete?.invoke()
        }
        if (scheduled == null || scheduled.isEmpty()) {
            return combineTickEvents(ready, mergeNearbyDroppedItems(activeSimulationChunks, deltaSeconds))
        }
        val spawned = ArrayList<DroppedItemSnapshot>(ready.spawned.size)
        val updated = ArrayList<DroppedItemSnapshot>(ready.updated.size)
        val removed = ArrayList<DroppedItemRemovedEvent>(ready.removed.size)
        if (ready.spawned.isNotEmpty()) spawned.addAll(ready.spawned)
        if (ready.updated.isNotEmpty()) updated.addAll(ready.updated)
        if (ready.removed.isNotEmpty()) removed.addAll(ready.removed)
        for (future in scheduled) {
            val events = runCatching { future.get() }.getOrNull() ?: continue
            if (events.spawned.isNotEmpty()) spawned.addAll(events.spawned)
            if (events.updated.isNotEmpty()) updated.addAll(events.updated)
            if (events.removed.isNotEmpty()) removed.addAll(events.removed)
        }
        val base = DroppedItemTickEvents(spawned = spawned, updated = updated, removed = removed)
        return combineTickEvents(base, mergeNearbyDroppedItems(activeSimulationChunks, deltaSeconds))
    }

    private fun combineTickEvents(base: DroppedItemTickEvents, merged: DroppedItemTickEvents): DroppedItemTickEvents {
        if (merged.isEmpty()) return base
        val spawnedById = LinkedHashMap<Int, DroppedItemSnapshot>(base.spawned.size)
        val updatedById = LinkedHashMap<Int, DroppedItemSnapshot>(base.updated.size)
        val removedById = LinkedHashMap<Int, DroppedItemRemovedEvent>(base.removed.size + merged.removed.size)
        for (snapshot in base.spawned) spawnedById[snapshot.entityId] = snapshot
        for (snapshot in base.updated) updatedById[snapshot.entityId] = snapshot
        for (removed in base.removed) removedById[removed.entityId] = removed
        for (snapshot in merged.spawned) {
            removedById.remove(snapshot.entityId)
            if (spawnedById.containsKey(snapshot.entityId)) {
                spawnedById[snapshot.entityId] = snapshot
            } else {
                updatedById[snapshot.entityId] = snapshot
            }
        }
        for (snapshot in merged.updated) {
            removedById.remove(snapshot.entityId)
            if (spawnedById.containsKey(snapshot.entityId)) {
                spawnedById[snapshot.entityId] = snapshot
            } else {
                updatedById[snapshot.entityId] = snapshot
            }
        }
        for (removed in merged.removed) {
            spawnedById.remove(removed.entityId)
            updatedById.remove(removed.entityId)
            removedById[removed.entityId] = removed
        }
        return DroppedItemTickEvents(
            spawned = spawnedById.values.toList(),
            updated = updatedById.values.toList(),
            removed = removedById.values.toList()
        )
    }

    private fun drainPendingChunkActorTickEvents(): DroppedItemTickEvents {
        if (pendingChunkActorTickEvents.isEmpty()) {
            return DroppedItemTickEvents(emptyList(), emptyList(), emptyList())
        }
        val spawned = ArrayList<DroppedItemSnapshot>()
        val updated = ArrayList<DroppedItemSnapshot>()
        val removed = ArrayList<DroppedItemRemovedEvent>()
        while (true) {
            val events = pendingChunkActorTickEvents.poll() ?: break
            if (events.spawned.isNotEmpty()) spawned.addAll(events.spawned)
            if (events.updated.isNotEmpty()) updated.addAll(events.updated)
            if (events.removed.isNotEmpty()) removed.addAll(events.removed)
        }
        return DroppedItemTickEvents(spawned, updated, removed)
    }

    private fun processChunk(
        lane: Int,
        chunkPos: ChunkPos,
        deltaSeconds: Double,
        tickScale: Double,
        activeSimulationChunks: Set<ChunkPos>?,
        chunkTimeRecorder: ((ChunkPos, Long) -> Unit)?,
        laneUnstuckTargets: MutableMap<Int, BlockPos>?
    ): LaneTickResult {
        val laneMap = laneChunks[lane]
        val dirtyChunks = dirtyChunksByLane[lane]
        val result = LaneTickResult()
        val startedAtNanos = System.nanoTime()
        val chunkState = laneMap[chunkPos] ?: return result
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
            return result
        }

        for ((entityId, entity) in chunkState.entities) {
            if (!tryLockEntity(entityId)) continue
            try {
            val baselineSnapshot = snapshots[entityId]
            if (baselineSnapshot == null) {
                chunkState.entities.remove(entityId, entity)
                continue
            }
            if (baselineSnapshot.uuid != entity.uuid) {
                chunkState.entities.remove(entityId, entity)
                continue
            }
            syncMutableEntityToSnapshot(entity, baselineSnapshot)
            applyPendingMergePull(entity)
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
            }

            if (!shouldSimulate) {
                if (spawnedIds.remove(entityId)) {
                    if (!isLiveSnapshot(entityId, entity.uuid)) {
                        chunkState.entities.remove(entityId, entity)
                        continue
                    }
                    if (canCommitChunkSnapshot(entityId, entity.uuid, baselineSnapshot)) {
                        snapshots[entityId] = entity.snapshot()
                    }
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
                    if (!isLiveSnapshot(entityId, entity.uuid)) {
                        chunkState.entities.remove(entityId, entity)
                        continue
                    }
                    if (canCommitChunkSnapshot(entityId, entity.uuid, baselineSnapshot)) {
                        snapshots[entityId] = entity.snapshot()
                    }
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

            if (!isLiveSnapshot(entityId, entity.uuid)) {
                chunkState.entities.remove(entityId, entity)
                continue
            }

            if (spawnedIds.remove(entityId)) {
                val snapshot = entity.snapshot()
                if (canCommitChunkSnapshot(entityId, entity.uuid, baselineSnapshot)) {
                    snapshots[entityId] = snapshot
                    result.spawned.add(snapshot)
                } else {
                    val reconciled = reconcileMotionSnapshotWithLiveState(entityId, entity.uuid, entity)
                    if (reconciled != null) {
                        result.spawned.add(reconciled)
                    }
                }
            } else if (moved) {
                val snapshot = entity.snapshot()
                if (canCommitChunkSnapshot(entityId, entity.uuid, baselineSnapshot)) {
                    snapshots[entityId] = snapshot
                    result.updated.add(snapshot)
                } else {
                    val reconciled = reconcileMotionSnapshotWithLiveState(entityId, entity.uuid, entity)
                    if (reconciled != null) {
                        result.updated.add(reconciled)
                    }
                }
            } else if (pickupDelayChanged) {
                if (canCommitChunkSnapshot(entityId, entity.uuid, baselineSnapshot)) {
                    snapshots[entityId] = entity.snapshot()
                } else {
                    reconcileMotionSnapshotWithLiveState(entityId, entity.uuid, entity)
                }
            }
            } finally {
                unlockEntity(entityId)
            }
        }

        if (chunkState.entities.isEmpty() && chunkState.inbound.isEmpty()) {
            laneMap.remove(chunkPos, chunkState)
            dirtyChunks.remove(chunkPos)
        } else if (!shouldSimulate) {
            dirtyChunks.remove(chunkPos)
        }
        chunkTimeRecorder?.invoke(chunkPos, System.nanoTime() - startedAtNanos)
        return result
    }

    private fun isLiveSnapshot(entityId: Int, expectedUuid: UUID): Boolean {
        val snapshot = snapshots[entityId] ?: return false
        return snapshot.uuid == expectedUuid
    }

    private fun canCommitChunkSnapshot(entityId: Int, expectedUuid: UUID, baseline: DroppedItemSnapshot): Boolean {
        val current = snapshots[entityId] ?: return false
        if (current.uuid != expectedUuid) return false
        return current == baseline
    }

    private fun reconcileMotionSnapshotWithLiveState(
        entityId: Int,
        expectedUuid: UUID,
        entity: MutableDroppedItem
    ): DroppedItemSnapshot? {
        val current = snapshots[entityId] ?: return null
        if (current.uuid != expectedUuid) return null
        val reconciled = current.copy(
            x = entity.x,
            y = entity.y,
            z = entity.z,
            vx = entity.vx,
            vy = entity.vy,
            vz = entity.vz,
            accelerationX = entity.accelerationX,
            accelerationY = entity.accelerationY,
            accelerationZ = entity.accelerationZ,
            pickupDelaySeconds = entity.pickupDelaySeconds,
            onGround = entity.onGround,
            chunkPos = ChunkPos(entity.chunkX, entity.chunkZ)
        )
        snapshots[entityId] = reconciled
        return reconciled
    }

    private fun syncMutableEntityToSnapshot(entity: MutableDroppedItem, snapshot: DroppedItemSnapshot) {
        entity.stack = copyDroppedItemStackState(snapshot.stack)
        entity.x = snapshot.x
        entity.y = snapshot.y
        entity.z = snapshot.z
        entity.vx = snapshot.vx
        entity.vy = snapshot.vy
        entity.vz = snapshot.vz
        entity.accelerationX = snapshot.accelerationX
        entity.accelerationY = snapshot.accelerationY
        entity.accelerationZ = snapshot.accelerationZ
        entity.pickupDelaySeconds = snapshot.pickupDelaySeconds
        entity.onGround = snapshot.onGround
        entity.chunkX = snapshot.chunkPos.x
        entity.chunkZ = snapshot.chunkPos.z
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
        if (!canOccupy(entity.x, entity.y, entity.z)) {
            resolveSpawnEmbedding(entity)
        }
        entity.ageTicks += tickScale
        entity.pickupDelaySeconds = (entity.pickupDelaySeconds - deltaSeconds).coerceAtLeast(0.0)
        if (entity.accelerationX != 0.0 || entity.accelerationY != 0.0 || entity.accelerationZ != 0.0) {
            entity.vx += (entity.accelerationX * deltaSeconds) / 20.0
            entity.vy += (entity.accelerationY * deltaSeconds) / 20.0
            entity.vz += (entity.accelerationZ * deltaSeconds) / 20.0
        }
        val buoyancyProfile = buoyancyProfile(entity.stack.itemId)
        val buoyantItem = buoyancyProfile.buoyancyPerTick > buoyancyProfile.sinkPerTick

        // Split fast movement into a few sub-steps to avoid tunneling through side/top blocks.
        val totalDx = entity.vx * tickScale
        val totalDy = entity.vy * tickScale
        val totalDz = entity.vz * tickScale
        val steps = movementSubsteps(totalDx, totalDy, totalDz)
        val subScale = tickScale / steps
        var grounded = false

        repeat(steps) {
            val waterScanRange = waterScanXzRange(entity.x, entity.z)
            val waterSurfaceY = highestWaterSurfaceY(waterScanRange, entity.y)
            val submergence = if (waterSurfaceY != null) {
                ((waterSurfaceY - entity.y) / ENTITY_HEIGHT).coerceIn(0.0, 1.0)
            } else {
                0.0
            }
            if (submergence > WATER_PHYSICS_MIN_SUBMERGENCE) {
                val flow = waterFlowVector(waterScanRange, entity.y)
                if (flow != null) {
                    applyWaterFlow(entity, flow, subScale)
                }
                applyFluidPhysics(entity, subScale, submergence, buoyancyProfile)
            } else {
                entity.vy -= GRAVITY_PER_TICK * subScale
            }

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
            if (buoyantItem) {
                val waterSurfaceYAfterMove = highestWaterSurfaceY(entity.x, entity.y, entity.z)
                if (waterSurfaceYAfterMove != null) {
                    val submergenceAfterMove = ((waterSurfaceYAfterMove - entity.y) / ENTITY_HEIGHT).coerceIn(0.0, 1.0)
                    val surfaceEntityY = waterSurfaceYAfterMove - ENTITY_HEIGHT
                    val horizontalSpeed = sqrt((entity.vx * entity.vx) + (entity.vz * entity.vz))
                    if (submergenceAfterMove > WATER_SURFACE_CLAMP_MIN_SUBMERGENCE &&
                        horizontalSpeed <= WATER_SURFACE_CLAMP_MAX_HORIZONTAL_SPEED &&
                        (entity.y > surfaceEntityY || abs(entity.y - surfaceEntityY) <= WATER_SURFACE_CLAMP_EPSILON)
                    ) {
                        entity.y = surfaceEntityY
                        if (entity.vy > 0.0 || abs(entity.vy) <= WATER_SURFACE_CLAMP_VELOCITY_EPSILON) {
                            entity.vy = 0.0
                        }
                    }
                }
            }
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

    private fun applyFluidPhysics(
        entity: MutableDroppedItem,
        tickScale: Double,
        submergence: Double,
        profile: ItemBuoyancyProfile
    ) {
        if (submergence <= WATER_PHYSICS_MIN_SUBMERGENCE) return
        if (profile.buoyancyPerTick > 0.0 && entity.vy < profile.buoyancyVelocityCap) {
            entity.vy += profile.buoyancyPerTick * tickScale
        }
        if (profile.sinkPerTick > 0.0) {
            entity.vy -= profile.sinkPerTick * tickScale
        }
        val horizontalDrag = profile.horizontalDrag.pow(tickScale)
        val verticalDrag = profile.verticalDrag.pow(tickScale)
        entity.vx *= horizontalDrag
        entity.vz *= horizontalDrag
        entity.vy *= verticalDrag
    }

    private fun applyWaterFlow(entity: MutableDroppedItem, flow: FlowVector2, tickScale: Double) {
        var pushX = flow.x * WATER_FLOW_PUSH_PER_TICK * tickScale
        var pushZ = flow.z * WATER_FLOW_PUSH_PER_TICK * tickScale
        val pushMag = sqrt((pushX * pushX) + (pushZ * pushZ))
        if (abs(entity.vx) < WATER_FLOW_MIN_HORIZONTAL_SPEED &&
            abs(entity.vz) < WATER_FLOW_MIN_HORIZONTAL_SPEED &&
            pushMag < WATER_FLOW_MIN_PUSH
        ) {
            val baseMag = sqrt((flow.x * flow.x) + (flow.z * flow.z))
            if (baseMag > 1.0e-12) {
                val scale = (WATER_FLOW_MIN_PUSH * tickScale) / baseMag
                pushX = flow.x * scale
                pushZ = flow.z * scale
            }
        }
        entity.vx += pushX
        entity.vz += pushZ
    }

    private fun waterFlowVector(range: WaterScanXzRange, entityY: Double): FlowVector2? {
        val startY = floor(entityY).toInt()
        val endY = floor(entityY + ENTITY_HEIGHT).toInt()
        var flowX = 0.0
        var flowZ = 0.0
        var contributors = 0

        for (bx in range.startX..range.endX) {
            for (by in startY..endY) {
                for (bz in range.startZ..range.endZ) {
                    val currentHeight = waterHeightByStateId(blockStateAt(bx, by, bz))
                    if (currentHeight <= 0.0) continue
                    if ((by + currentHeight) < entityY) continue

                    var localX = 0.0
                    var localZ = 0.0
                    for ((dx, dz) in HORIZONTAL_FLOW_DIRECTIONS) {
                        val nx = bx + dx
                        val nz = bz + dz
                        val neighborState = blockStateAt(nx, by, nz)
                        val neighborHeight = waterHeightByStateId(neighborState)
                        if (neighborHeight >= 0.0) {
                            val diff = neighborHeight - currentHeight
                            localX += dx * diff
                            localZ += dz * diff
                            continue
                        }
                        val belowNeighborHeight = waterHeightByStateId(blockStateAt(nx, by - 1, nz))
                        if (belowNeighborHeight >= 0.0) {
                            val diff = belowNeighborHeight - (currentHeight - 1.0)
                            localX += dx * diff
                            localZ += dz * diff
                        }
                    }

                    if (localX != 0.0 || localZ != 0.0) {
                        flowX += localX
                        flowZ += localZ
                        contributors++
                    }
                }
            }
        }

        if (contributors > 1) {
            flowX /= contributors.toDouble()
            flowZ /= contributors.toDouble()
        }
        val mag = sqrt((flowX * flowX) + (flowZ * flowZ))
        if (mag <= 1.0e-12) return null
        // Our neighbor-height gradient is opposite to the push direction used by item motion.
        return FlowVector2(-(flowX / mag), -(flowZ / mag))
    }

    private fun highestWaterSurfaceY(x: Double, y: Double, z: Double): Double? {
        return highestWaterSurfaceY(waterScanXzRange(x, z), y)
    }

    private fun highestWaterSurfaceY(range: WaterScanXzRange, y: Double): Double? {
        val startY = floor(y).toInt()
        val endY = floor(y + ENTITY_HEIGHT).toInt()

        var highest: Double? = null
        for (bx in range.startX..range.endX) {
            for (by in startY..endY) {
                for (bz in range.startZ..range.endZ) {
                    val height = waterHeightByStateId(blockStateAt(bx, by, bz))
                    if (height <= 0.0) continue
                    val top = by + height
                    if (highest == null || top > highest) {
                        highest = top
                    }
                }
            }
        }
        return highest
    }

    private fun waterScanXzRange(x: Double, z: Double): WaterScanXzRange {
        val minX = x - ENTITY_RADIUS
        val maxX = x + ENTITY_RADIUS
        val minZ = z - ENTITY_RADIUS
        val maxZ = z + ENTITY_RADIUS
        return WaterScanXzRange(
            startX = floor(minX).toInt(),
            endX = floor(maxX).toInt(),
            startZ = floor(minZ).toInt(),
            endZ = floor(maxZ).toInt()
        )
    }

    private fun isWaterState(stateId: Int): Boolean {
        if (stateId <= 0) return false
        return globalWaterStateCache.computeIfAbsent(stateId) { id ->
            val parsed = BlockStateRegistry.parsedState(id) ?: return@computeIfAbsent false
            parsed.blockKey == "minecraft:water"
        }
    }

    private fun waterHeightByStateId(stateId: Int): Double {
        if (stateId <= 0) return -1.0
        val level = globalWaterLevelByStateCache.computeIfAbsent(stateId) { id ->
            val parsed = BlockStateRegistry.parsedState(id) ?: return@computeIfAbsent -1
            if (parsed.blockKey != "minecraft:water") return@computeIfAbsent -1
            val raw = parsed.properties["level"]?.toIntOrNull() ?: 0
            raw.coerceIn(0, 8)
        }
        if (level < 0) return -1.0
        return (8 - level) / 9.0
    }

    private fun resolveSpawnEmbedding(entity: MutableDroppedItem) {
        if (canOccupy(entity.x, entity.y, entity.z)) return
        val base = floor(entity.y).toInt()
        for (offset in 0..4) {
            val candidateY = base + offset + 1.0 + COLLISION_EPSILON
            if (!canOccupy(entity.x, candidateY, entity.z)) continue
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

    private fun mergeNearbyDroppedItems(activeSimulationChunks: Set<ChunkPos>?, deltaSeconds: Double): DroppedItemTickEvents {
        if (deltaSeconds <= 0.0) return DroppedItemTickEvents(emptyList(), emptyList(), emptyList())
        val candidates = snapshots.values
            .asSequence()
            .filter { activeSimulationChunks == null || it.chunkPos in activeSimulationChunks }
            .filter { it.stack.itemId >= 0 && it.stack.count > 0 }
            .toList()
        if (candidates.size < 2) return DroppedItemTickEvents(emptyList(), emptyList(), emptyList())

        val chunked = HashMap<ChunkPos, MutableList<DroppedItemSnapshot>>()
        for (snapshot in candidates) {
            chunked.computeIfAbsent(snapshot.chunkPos) { ArrayList() }.add(snapshot)
        }

        val updated = LinkedHashMap<Int, DroppedItemSnapshot>()
        val removed = LinkedHashMap<Int, DroppedItemRemovedEvent>()
        val consumed = HashSet<Int>()
        val ordered = candidates.sortedBy { it.entityId }

        for (snapshot in ordered) {
            if (snapshot.entityId in consumed) continue
            val self = this.snapshot(snapshot.entityId) ?: continue
            if (self.uuid != snapshot.uuid || self.stack.count <= 0 || self.stack.itemId < 0) continue
            val partner = findBestMergePartner(self, chunked, activeSimulationChunks, consumed) ?: continue
            val partnerLive = this.snapshot(partner.entityId) ?: continue
            if (partnerLive.uuid != partner.uuid || partnerLive.stack.count <= 0 || partnerLive.stack.itemId < 0) continue
            if (!canMergeStacks(self, partnerLive)) continue
            val maxStackSize = itemMaxStackSize(self.stack.itemId)
            if (maxStackSize <= 0) continue
            if (self.stack.count >= maxStackSize && partnerLive.stack.count >= maxStackSize) continue

            val dx = partnerLive.x - self.x
            val dy = partnerLive.y - self.y
            val dz = partnerLive.z - self.z
            val distanceSq = (dx * dx) + (dy * dy) + (dz * dz)
            val canSnapMergeNow = distanceSq <= MERGE_SNAP_DISTANCE_SQ
            if (canSnapMergeNow) {
                val keep = if (self.stack.count > partnerLive.stack.count) self else if (self.stack.count < partnerLive.stack.count) partnerLive else if (self.entityId <= partnerLive.entityId) self else partnerLive
                val merge = if (keep.entityId == self.entityId) partnerLive else self
                val transferable = minOf(maxStackSize - keep.stack.count, merge.stack.count)
                if (transferable <= 0) continue

                val tx = applyMergeTransaction(
                    keep = keep,
                    merge = merge
                ) ?: continue
                updated[tx.keep.entityId] = tx.keep
                if (tx.merge == null) {
                    removed[merge.entityId] = DroppedItemRemovedEvent(merge.entityId, merge.chunkPos)
                } else {
                    updated[tx.merge.entityId] = tx.merge
                }
                consumed.add(keep.entityId)
                consumed.add(merge.entityId)
                continue
            }

            if (distanceSq > MERGE_ATTRACT_DISTANCE_SQ) continue
            val distance = sqrt(distanceSq)
            if (distance <= 1.0e-6) continue
            val midpointX = (self.x + partnerLive.x) * 0.5
            val midpointZ = (self.z + partnerLive.z) * 0.5
            val pullStep = (MERGE_PULL_PER_SECOND * deltaSeconds).coerceAtMost(MERGE_MAX_PULL_STEP)
            val selfDirX = (midpointX - self.x) / distance
            val selfDirZ = (midpointZ - self.z) / distance
            val partnerDirX = (midpointX - partnerLive.x) / distance
            val partnerDirZ = (midpointZ - partnerLive.z) / distance
            enqueueMergePullImpulse(self.entityId, selfDirX * pullStep, selfDirZ * pullStep)
            enqueueMergePullImpulse(partnerLive.entityId, partnerDirX * pullStep, partnerDirZ * pullStep)
            consumed.add(self.entityId)
            consumed.add(partnerLive.entityId)
        }
        return DroppedItemTickEvents(
            spawned = emptyList(),
            updated = updated.values.toList(),
            removed = removed.values.toList()
        )
    }

    private data class MergeTxResult(
        val keep: DroppedItemSnapshot,
        val merge: DroppedItemSnapshot?
    )

    private fun applyMergeTransaction(
        keep: DroppedItemSnapshot,
        merge: DroppedItemSnapshot
    ): MergeTxResult? {
        return withPairEntityLocks(keep.entityId, merge.entityId) {
            val keepSnapshot = snapshot(keep.entityId) ?: return@withPairEntityLocks null
            val mergeSnapshot = snapshot(merge.entityId) ?: return@withPairEntityLocks null
            if (keepSnapshot.uuid != keep.uuid || mergeSnapshot.uuid != merge.uuid) return@withPairEntityLocks null
            if (!canMergeStacks(keepSnapshot, mergeSnapshot)) return@withPairEntityLocks null
            val maxStackSize = itemMaxStackSize(keepSnapshot.stack.itemId)
            if (maxStackSize <= 0) return@withPairEntityLocks null
            val transferable = minOf((maxStackSize - keepSnapshot.stack.count).coerceAtLeast(0), mergeSnapshot.stack.count)
            if (transferable <= 0) return@withPairEntityLocks null
            val keepCount = keepSnapshot.stack.count + transferable
            val mergeCount = mergeSnapshot.stack.count - transferable

            val mergedPickupDelay = minOf(keepSnapshot.pickupDelaySeconds, mergeSnapshot.pickupDelaySeconds)
            if (mergeCount <= 0) {
                val keepLive = mutateIfUuidMatches(keep.entityId, keep.uuid) { entity ->
                    entity.stack = copyDroppedItemStackState(entity.stack.copy(count = keepCount))
                    entity.vx = keepSnapshot.vx
                    entity.vy = keepSnapshot.vy
                    entity.vz = keepSnapshot.vz
                    entity.accelerationX = keepSnapshot.accelerationX
                    entity.accelerationY = keepSnapshot.accelerationY
                    entity.accelerationZ = keepSnapshot.accelerationZ
                    entity.pickupDelaySeconds = mergedPickupDelay
                    entity.sleeping = false
                    entity.restTicks = 0.0
                } ?: return@withPairEntityLocks null
                val removed = removeIfUuidMatches(merge.entityId, merge.uuid)
                if (removed == null) {
                    restoreSnapshotIfUuid(keepSnapshot)
                    return@withPairEntityLocks null
                }
                return@withPairEntityLocks MergeTxResult(keep = keepLive, merge = null)
            }
            val mergeLive = mutateIfUuidMatches(merge.entityId, merge.uuid) { entity ->
                entity.stack = copyDroppedItemStackState(entity.stack.copy(count = mergeCount))
                entity.pickupDelaySeconds = mergedPickupDelay
                entity.sleeping = false
                entity.restTicks = 0.0
            } ?: return@withPairEntityLocks null
            val keepLive = mutateIfUuidMatches(keep.entityId, keep.uuid) { entity ->
                entity.stack = copyDroppedItemStackState(entity.stack.copy(count = keepCount))
                entity.vx = keepSnapshot.vx
                entity.vy = keepSnapshot.vy
                entity.vz = keepSnapshot.vz
                entity.accelerationX = keepSnapshot.accelerationX
                entity.accelerationY = keepSnapshot.accelerationY
                entity.accelerationZ = keepSnapshot.accelerationZ
                entity.pickupDelaySeconds = mergedPickupDelay
                entity.sleeping = false
                entity.restTicks = 0.0
            } ?: run {
                restoreSnapshotIfUuid(mergeSnapshot)
                return@withPairEntityLocks null
            }
            MergeTxResult(keep = keepLive, merge = mergeLive)
        }
    }

    private fun enqueueMergePullImpulse(entityId: Int, dvx: Double, dvz: Double) {
        if (!dvx.isFinite() || !dvz.isFinite()) return
        if (abs(dvx) <= VELOCITY_SNAP_EPSILON && abs(dvz) <= VELOCITY_SNAP_EPSILON) return
        pendingMergePullImpulses.compute(entityId) { _, current ->
            val next = current ?: doubleArrayOf(0.0, 0.0)
            next[0] += dvx
            next[1] += dvz
            next
        }
    }

    private fun applyPendingMergePull(entity: MutableDroppedItem) {
        val impulse = pendingMergePullImpulses.remove(entity.entityId) ?: return
        val dvx = impulse[0]
        val dvz = impulse[1]
        if (!dvx.isFinite() || !dvz.isFinite()) return
        entity.vx = (entity.vx + dvx).coerceIn(-MERGE_MAX_PULL_SPEED, MERGE_MAX_PULL_SPEED)
        entity.vz = (entity.vz + dvz).coerceIn(-MERGE_MAX_PULL_SPEED, MERGE_MAX_PULL_SPEED)
        entity.sleeping = false
        entity.restTicks = 0.0
    }

    private fun clearPendingMergePull(entityId: Int) {
        pendingMergePullImpulses.remove(entityId)
    }

    private fun restoreSnapshotIfUuid(snapshot: DroppedItemSnapshot): DroppedItemSnapshot? {
        return mutateIfUuidMatches(snapshot.entityId, snapshot.uuid) { entity ->
            entity.stack = copyDroppedItemStackState(snapshot.stack)
            entity.x = snapshot.x
            entity.y = snapshot.y
            entity.z = snapshot.z
            entity.vx = snapshot.vx
            entity.vy = snapshot.vy
            entity.vz = snapshot.vz
            entity.accelerationX = snapshot.accelerationX
            entity.accelerationY = snapshot.accelerationY
            entity.accelerationZ = snapshot.accelerationZ
            entity.pickupDelaySeconds = snapshot.pickupDelaySeconds
            entity.onGround = snapshot.onGround
            entity.chunkX = snapshot.chunkPos.x
            entity.chunkZ = snapshot.chunkPos.z
            entity.sleeping = false
            entity.restTicks = 0.0
        }
    }

    private fun findBestMergePartner(
        origin: DroppedItemSnapshot,
        candidatesByChunk: Map<ChunkPos, List<DroppedItemSnapshot>>,
        activeSimulationChunks: Set<ChunkPos>?,
        consumed: Set<Int>
    ): DroppedItemSnapshot? {
        var best: DroppedItemSnapshot? = null
        var bestDistanceSq = Double.POSITIVE_INFINITY
        for (chunkX in (origin.chunkPos.x - 1)..(origin.chunkPos.x + 1)) {
            for (chunkZ in (origin.chunkPos.z - 1)..(origin.chunkPos.z + 1)) {
                val chunkPos = ChunkPos(chunkX, chunkZ)
                if (activeSimulationChunks != null && chunkPos !in activeSimulationChunks) continue
                val candidates = candidatesByChunk[chunkPos] ?: continue
                for (candidate in candidates) {
                    if (candidate.entityId == origin.entityId) continue
                    if (candidate.entityId in consumed) continue
                    val live = snapshot(candidate.entityId) ?: continue
                    if (live.uuid != candidate.uuid || live.stack.count <= 0 || live.stack.itemId < 0) continue
                    if (!canMergeStacks(origin, live)) continue
                    val maxStack = itemMaxStackSize(origin.stack.itemId)
                    if (maxStack <= 0) continue
                    if (origin.stack.count >= maxStack && live.stack.count >= maxStack) continue
                    val dx = live.x - origin.x
                    val dy = live.y - origin.y
                    val dz = live.z - origin.z
                    val distanceSq = (dx * dx) + (dy * dy) + (dz * dz)
                    if (distanceSq > MERGE_ATTRACT_DISTANCE_SQ) continue
                    if (distanceSq < bestDistanceSq ||
                        (abs(distanceSq - bestDistanceSq) <= MERGE_DISTANCE_EPSILON && live.entityId < (best?.entityId ?: Int.MAX_VALUE))
                    ) {
                        best = live
                        bestDistanceSq = distanceSq
                    }
                }
            }
        }
        return best
    }

    private fun canMergeStacks(left: DroppedItemSnapshot, right: DroppedItemSnapshot): Boolean {
        if (left.stack.itemId != right.stack.itemId) return false
        if (!areStackMetadataEqual(left.stack, right.stack)) return false
        val maxStackSize = itemMaxStackSize(left.stack.itemId)
        if (maxStackSize <= 0) return false
        return left.stack.count < maxStackSize || right.stack.count < maxStackSize
    }

    private fun areStackMetadataEqual(left: ItemStackState, right: ItemStackState): Boolean {
        return areChestStatesEqual(left.shulkerContents, right.shulkerContents)
    }

    private fun areChestStatesEqual(left: org.macaroon3145.network.handler.ChestState?, right: org.macaroon3145.network.handler.ChestState?): Boolean {
        if (left === right) return true
        if (left == null || right == null) return false
        if (!left.itemIds.contentEquals(right.itemIds)) return false
        if (!left.itemCounts.contentEquals(right.itemCounts)) return false
        if (left.shulkerContents.size != right.shulkerContents.size) return false
        for (i in left.shulkerContents.indices) {
            if (!areChestStatesEqual(left.shulkerContents[i], right.shulkerContents[i])) return false
        }
        return true
    }

    private fun itemMaxStackSize(itemId: Int): Int {
        if (itemId < 0 || itemId >= ITEM_MAX_STACK_SIZE_BY_ITEM_ID.size) return DEFAULT_MAX_STACK_SIZE
        return ITEM_MAX_STACK_SIZE_BY_ITEM_ID[itemId].coerceIn(1, DEFAULT_MAX_STACK_SIZE)
    }

    private fun collidesXFace(testX: Double, baseY: Double, centerZ: Double): Boolean {
        val y1 = baseY + 0.01
        val y2 = baseY + ENTITY_HEIGHT * 0.5
        val y3 = baseY + ENTITY_HEIGHT - 0.01
        return isSolidAt(testX, y1, centerZ + ENTITY_RADIUS) ||
            isSolidAt(testX, y1, centerZ - ENTITY_RADIUS) ||
            isSolidAt(testX, y2, centerZ + ENTITY_RADIUS) ||
            isSolidAt(testX, y2, centerZ - ENTITY_RADIUS) ||
            isSolidAt(testX, y3, centerZ + ENTITY_RADIUS) ||
            isSolidAt(testX, y3, centerZ - ENTITY_RADIUS)
    }

    private fun collidesZFace(centerX: Double, baseY: Double, testZ: Double): Boolean {
        val y1 = baseY + 0.01
        val y2 = baseY + ENTITY_HEIGHT * 0.5
        val y3 = baseY + ENTITY_HEIGHT - 0.01
        return isSolidAt(centerX + ENTITY_RADIUS, y1, testZ) ||
            isSolidAt(centerX - ENTITY_RADIUS, y1, testZ) ||
            isSolidAt(centerX + ENTITY_RADIUS, y2, testZ) ||
            isSolidAt(centerX - ENTITY_RADIUS, y2, testZ) ||
            isSolidAt(centerX + ENTITY_RADIUS, y3, testZ) ||
            isSolidAt(centerX - ENTITY_RADIUS, y3, testZ)
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
        return globalCollidableStateCache.computeIfAbsent(stateId) { id ->
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

    private fun lockIndexForEntity(entityId: Int): Int {
        return (entityId and Int.MAX_VALUE) % ENTITY_LOCK_STRIPES
    }

    private fun tryLockEntity(entityId: Int): Boolean {
        return entityLocks[lockIndexForEntity(entityId)].tryLock()
    }

    private fun unlockEntity(entityId: Int) {
        entityLocks[lockIndexForEntity(entityId)].unlock()
    }

    private inline fun <T> withPairEntityLocks(entityA: Int, entityB: Int, action: () -> T?): T? {
        val first = if (entityA <= entityB) entityA else entityB
        val second = if (entityA <= entityB) entityB else entityA
        if (!tryLockEntity(first)) return null
        if (second == first) {
            return try {
                action()
            } finally {
                unlockEntity(first)
            }
        }
        if (!tryLockEntity(second)) {
            unlockEntity(first)
            return null
        }
        return try {
            action()
        } finally {
            unlockEntity(second)
            unlockEntity(first)
        }
    }

    private fun tryAcquireChunkTick(chunkPos: ChunkPos): Boolean {
        return chunkTickInFlight.computeIfAbsent(chunkPos) { AtomicBoolean(false) }.compareAndSet(false, true)
    }

    private fun releaseChunkTick(chunkPos: ChunkPos) {
        chunkTickInFlight[chunkPos]?.set(false)
    }

    private fun chunkXFromBlockX(x: Double): Int {
        return floor(x / 16.0).toInt()
    }

    private fun chunkZFromBlockZ(z: Double): Int {
        return floor(z / 16.0).toInt()
    }

    companion object {
        private data class ItemBuoyancyProfile(
            val buoyancyPerTick: Double,
            val buoyancyVelocityCap: Double,
            val sinkPerTick: Double,
            val horizontalDrag: Double,
            val verticalDrag: Double
        )

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
        private const val DEFAULT_MAX_STACK_SIZE = 64
        private const val MERGE_ATTRACT_DISTANCE = 1.75
        private const val MERGE_ATTRACT_DISTANCE_SQ = MERGE_ATTRACT_DISTANCE * MERGE_ATTRACT_DISTANCE
        private const val MERGE_SNAP_DISTANCE = 0.35
        private const val MERGE_SNAP_DISTANCE_SQ = MERGE_SNAP_DISTANCE * MERGE_SNAP_DISTANCE
        private const val MERGE_PULL_PER_SECOND = 0.75
        private const val MERGE_MAX_PULL_STEP = 0.30
        private const val MERGE_MAX_PULL_SPEED = 0.80
        private const val MERGE_DISTANCE_EPSILON = 1.0e-6
        private const val AIR_DRAG = 0.98
        private const val GROUND_DRAG = 0.60
        private const val WATER_HORIZONTAL_DRAG = 0.99
        private const val WATER_VERTICAL_DRAG = 1.0
        // Default coefficients used when item-specific profile is missing.
        private const val DEFAULT_WATER_BUOYANCY_PER_TICK = 0.0005
        private const val DEFAULT_WATER_BUOYANCY_VELOCITY_CAP = 0.06
        private const val DEFAULT_WATER_SINK_PER_TICK = 0.0
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
        private const val WATER_SURFACE_CLAMP_EPSILON = 0.02
        private const val WATER_SURFACE_CLAMP_VELOCITY_EPSILON = 0.02
        private const val WATER_SURFACE_CLAMP_MIN_SUBMERGENCE = 0.1
        private const val WATER_SURFACE_CLAMP_MAX_HORIZONTAL_SPEED = 0.01
        private const val WATER_PHYSICS_MIN_SUBMERGENCE = 0.1
        private const val WATER_FLOW_PUSH_PER_TICK = 0.014
        private const val WATER_FLOW_MIN_HORIZONTAL_SPEED = 0.003
        private const val WATER_FLOW_MIN_PUSH = 0.0045
        private const val DEFAULT_PICKUP_DELAY_SECONDS = 2.0
        private const val MAX_AGE_TICKS = 6000.0
        private const val DESPAWN_Y = -128.0
        private const val ENTITY_LOCK_STRIPES = 4096
        private val json = Json { ignoreUnknownKeys = true }
        private val NON_COLLIDING_BLOCK_KEYS = loadBlockTag("dropped_item_non_colliding")
        private val ITEM_ID_BY_KEY = loadItemIdsByKey()
        private val ITEM_KEY_BY_ID = ITEM_ID_BY_KEY.entries.associate { (key, id) -> id to key }
        private val ITEM_MAX_STACK_SIZE_BY_ITEM_ID = loadItemMaxStackSizeByItemId()
        private val ITEM_BUOYANCY_PROFILE_BY_KEY = loadItemBuoyancyProfilesByKey()
        private val globalCollidableStateCache = ConcurrentHashMap<Int, Boolean>()
        private val globalWaterStateCache = ConcurrentHashMap<Int, Boolean>()
        private val globalWaterLevelByStateCache = ConcurrentHashMap<Int, Int>()
        private val globalBuoyancyProfileByItemId = ConcurrentHashMap<Int, ItemBuoyancyProfile>()
        private val entityLocks = Array(ENTITY_LOCK_STRIPES) { ReentrantLock() }
        private val HORIZONTAL_FLOW_DIRECTIONS = arrayOf(
            intArrayOf(-1, 0),
            intArrayOf(1, 0),
            intArrayOf(0, -1),
            intArrayOf(0, 1)
        )

        fun prewarm() {
            NON_COLLIDING_BLOCK_KEYS.size
            ITEM_ID_BY_KEY.size
            ITEM_KEY_BY_ID.size
            ITEM_BUOYANCY_PROFILE_BY_KEY.size
            for (stateId in BlockStateRegistry.allStateIds()) {
                if (stateId <= 0) continue
                globalCollidableStateCache.computeIfAbsent(stateId) { id ->
                    val blockKey = BlockStateRegistry.parsedState(id)?.blockKey ?: return@computeIfAbsent true
                    blockKey !in NON_COLLIDING_BLOCK_KEYS
                }
                globalWaterStateCache.computeIfAbsent(stateId) { id ->
                    val blockKey = BlockStateRegistry.parsedState(id)?.blockKey ?: return@computeIfAbsent false
                    blockKey == "minecraft:water"
                }
                globalWaterLevelByStateCache.computeIfAbsent(stateId) { id ->
                    val parsed = BlockStateRegistry.parsedState(id) ?: return@computeIfAbsent -1
                    if (parsed.blockKey != "minecraft:water") return@computeIfAbsent -1
                    val raw = parsed.properties["level"]?.toIntOrNull() ?: 0
                    raw.coerceIn(0, 8)
                }
            }
            for (itemId in ITEM_ID_BY_KEY.values) {
                globalBuoyancyProfileByItemId.computeIfAbsent(itemId, ::computeBuoyancyProfileByItemId)
            }
        }

        private fun loadItemMaxStackSizeByItemId(): IntArray {
            val maxId = ITEM_ID_BY_KEY.values.maxOrNull() ?: -1
            if (maxId < 0) return IntArray(0)
            val out = IntArray(maxId + 1) { DEFAULT_MAX_STACK_SIZE }
            val resource = DroppedItemSystem::class.java.classLoader
                .getResourceAsStream("data/minecraft/item/max_stack_size.json")
                ?: return out
            val root = runCatching {
                resource.bufferedReader().use { json.parseToJsonElement(it.readText()).jsonObject }
            }.getOrNull() ?: return out
            val defaultSize = root["default"]?.jsonPrimitive?.intOrNull
                ?.coerceIn(1, DEFAULT_MAX_STACK_SIZE)
                ?: DEFAULT_MAX_STACK_SIZE
            for (i in out.indices) {
                out[i] = defaultSize
            }
            val entries = root["entries"]?.jsonObject ?: return out
            for ((itemKey, maxElement) in entries) {
                val itemId = ITEM_ID_BY_KEY[itemKey] ?: continue
                if (itemId !in out.indices) continue
                val maxStack = maxElement.jsonPrimitive.intOrNull
                    ?.coerceIn(1, DEFAULT_MAX_STACK_SIZE)
                    ?: continue
                out[itemId] = maxStack
            }
            return out
        }

        private fun isBuoyantItem(itemId: Int): Boolean {
            val profile = buoyancyProfile(itemId)
            return profile.buoyancyPerTick > profile.sinkPerTick
        }

        private fun buoyancyProfile(itemId: Int): ItemBuoyancyProfile {
            if (itemId < 0) return defaultBuoyantProfile()
            return globalBuoyancyProfileByItemId.computeIfAbsent(itemId, ::computeBuoyancyProfileByItemId)
        }

        private fun computeBuoyancyProfileByItemId(itemId: Int): ItemBuoyancyProfile {
            val itemKey = ITEM_KEY_BY_ID[itemId]
            if (itemKey != null) {
                val override = ITEM_BUOYANCY_PROFILE_BY_KEY[itemKey]
                if (override != null) return override
            }
            return defaultBuoyantProfile()
        }

        private fun defaultBuoyantProfile(): ItemBuoyancyProfile {
            return ItemBuoyancyProfile(
                buoyancyPerTick = DEFAULT_WATER_BUOYANCY_PER_TICK,
                buoyancyVelocityCap = DEFAULT_WATER_BUOYANCY_VELOCITY_CAP,
                sinkPerTick = 0.0,
                horizontalDrag = WATER_HORIZONTAL_DRAG,
                verticalDrag = WATER_VERTICAL_DRAG
            )
        }

        private fun defaultNonBuoyantProfile(): ItemBuoyancyProfile {
            return ItemBuoyancyProfile(
                buoyancyPerTick = 0.0,
                buoyancyVelocityCap = DEFAULT_WATER_BUOYANCY_VELOCITY_CAP,
                sinkPerTick = DEFAULT_WATER_SINK_PER_TICK,
                horizontalDrag = WATER_HORIZONTAL_DRAG,
                verticalDrag = WATER_VERTICAL_DRAG
            )
        }

        private fun loadItemBuoyancyProfilesByKey(): Map<String, ItemBuoyancyProfile> {
            val stream = DroppedItemSystem::class.java.getResourceAsStream("/data/minecraft/item/dropped_item_buoyancy.json")
                ?: DroppedItemSystem::class.java.classLoader.getResourceAsStream("data/minecraft/item/dropped_item_buoyancy.json")
                ?: return emptyMap()
            val root = runCatching {
                stream.bufferedReader().use { reader ->
                    json.parseToJsonElement(reader.readText()).jsonObject
                }
            }.getOrNull() ?: return emptyMap()

            val defaultsNode = root["defaults"]?.jsonObject
            val defaultBuoyancy = readDouble(defaultsNode, "buoyancy_per_tick", "buoyancy")
                ?: DEFAULT_WATER_BUOYANCY_PER_TICK
            val defaultBuoyancyVelocityCap = readDouble(defaultsNode, "buoyancy_velocity_cap", "buoyancy_max_vy")
                ?: DEFAULT_WATER_BUOYANCY_VELOCITY_CAP
            val defaultSink = readDouble(defaultsNode, "sink_per_tick", "sink")
                ?: DEFAULT_WATER_SINK_PER_TICK
            val defaultHorizontalDrag = readDouble(defaultsNode, "horizontal_drag", "water_horizontal_drag")
                ?: WATER_HORIZONTAL_DRAG
            val defaultVerticalDrag = readDouble(defaultsNode, "vertical_drag", "water_vertical_drag")
                ?: WATER_VERTICAL_DRAG

            val items = root["items"]?.jsonObject ?: return emptyMap()
            val out = HashMap<String, ItemBuoyancyProfile>(items.size)
            for ((rawKey, value) in items) {
                val key = if (rawKey.startsWith("minecraft:")) rawKey else "minecraft:$rawKey"
                val profile = when {
                    value is JsonObject -> {
                        val buoyancy = readDouble(value, "buoyancy_per_tick", "buoyancy")
                            ?: defaultBuoyancy
                        val sink = readDouble(value, "sink_per_tick", "sink")
                            ?: defaultSink
                        val horizontalDrag = readDouble(value, "horizontal_drag", "water_horizontal_drag")
                            ?: defaultHorizontalDrag
                        val verticalDrag = readDouble(value, "vertical_drag", "water_vertical_drag")
                            ?: defaultVerticalDrag
                        ItemBuoyancyProfile(
                            buoyancyPerTick = buoyancy.coerceAtLeast(0.0),
                            buoyancyVelocityCap = readDouble(value, "buoyancy_velocity_cap", "buoyancy_max_vy")
                                ?.coerceAtLeast(0.0)
                                ?: defaultBuoyancyVelocityCap,
                            sinkPerTick = sink.coerceAtLeast(0.0),
                            horizontalDrag = horizontalDrag.coerceIn(0.0, 1.0),
                            verticalDrag = verticalDrag.coerceIn(0.0, 1.0)
                        )
                    }
                    else -> {
                        val buoyancy = value.jsonPrimitive.content.toDoubleOrNull() ?: continue
                        ItemBuoyancyProfile(
                            buoyancyPerTick = buoyancy.coerceAtLeast(0.0),
                            buoyancyVelocityCap = defaultBuoyancyVelocityCap,
                            sinkPerTick = 0.0,
                            horizontalDrag = defaultHorizontalDrag.coerceIn(0.0, 1.0),
                            verticalDrag = defaultVerticalDrag.coerceIn(0.0, 1.0)
                        )
                    }
                }
                out[key] = profile
            }
            return out
        }

        private fun readDouble(node: JsonObject?, vararg keys: String): Double? {
            if (node == null) return null
            for (key in keys) {
                val raw = node[key]?.jsonPrimitive?.content ?: continue
                val parsed = raw.toDoubleOrNull() ?: continue
                if (parsed.isFinite()) return parsed
            }
            return null
        }

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

        private fun loadItemTag(tagName: String): Set<String> {
            return resolveItemTag(tagName, HashSet())
        }

        private fun resolveItemTag(tagName: String, visited: MutableSet<String>): Set<String> {
            if (!visited.add(tagName)) return emptySet()
            val resourcePath = "/data/minecraft/tags/item/$tagName.json"
            val stream = DroppedItemSystem::class.java.getResourceAsStream(resourcePath) ?: return emptySet()
            val root = stream.bufferedReader().use {
                json.parseToJsonElement(it.readText()).jsonObject
            }
            val out = LinkedHashSet<String>()
            for (value in root["values"]?.jsonArray.orEmpty()) {
                val raw = value.jsonPrimitive.content
                if (raw.startsWith("#minecraft:")) {
                    out.addAll(resolveItemTag(raw.removePrefix("#minecraft:"), visited))
                } else if (raw.startsWith("#")) {
                    out.addAll(resolveItemTag(raw.substring(1).substringAfter("minecraft:"), visited))
                } else if (raw.startsWith("minecraft:")) {
                    out.add(raw)
                } else {
                    out.add("minecraft:$raw")
                }
            }
            return out
        }

        private fun loadItemIdsByKey(): Map<String, Int> {
            val stream = DroppedItemSystem::class.java.classLoader
                .getResourceAsStream("item-id-map-1.21.11.json")
                ?: return emptyMap()
            val root = stream.bufferedReader().use {
                json.parseToJsonElement(it.readText()).jsonObject
            }
            val entries = root["entries"]?.jsonObject ?: return emptyMap()
            val out = HashMap<String, Int>(entries.size)
            for ((itemKey, itemIdElement) in entries) {
                val itemId = itemIdElement.jsonPrimitive.intOrNull ?: continue
                out[itemKey] = itemId
            }
            return out
        }
    }

    private fun mutableFromSnapshot(snapshot: DroppedItemSnapshot): MutableDroppedItem {
        return MutableDroppedItem(
            entityId = snapshot.entityId,
            uuid = snapshot.uuid,
            stack = copyDroppedItemStackState(snapshot.stack),
            x = snapshot.x,
            y = snapshot.y,
            z = snapshot.z,
            vx = snapshot.vx,
            vy = snapshot.vy,
            vz = snapshot.vz,
            accelerationX = snapshot.accelerationX,
            accelerationY = snapshot.accelerationY,
            accelerationZ = snapshot.accelerationZ,
            pickupDelaySeconds = snapshot.pickupDelaySeconds,
            onGround = snapshot.onGround,
            ageTicks = 0.0,
            restTicks = 0.0,
            sleeping = false,
            chunkX = snapshot.chunkPos.x,
            chunkZ = snapshot.chunkPos.z
        )
    }

    private inline fun mutate(entityId: Int, mutator: (MutableDroppedItem) -> Unit): DroppedItemSnapshot? {
        val currentSnapshot = snapshots[entityId] ?: return null
        val oldChunk = currentSnapshot.chunkPos
        val oldLane = laneFor(oldChunk.x, oldChunk.z)
        val oldLaneMap = laneChunks[oldLane]
        val oldChunkState = oldLaneMap[oldChunk]

        val entity = oldChunkState?.entities?.get(entityId)
            ?: run {
                var foundInbound: MutableDroppedItem? = null
                if (oldChunkState != null) {
                    for (candidate in oldChunkState.inbound) {
                        if (candidate.entityId == entityId) {
                            foundInbound = candidate
                            break
                        }
                    }
                }
                foundInbound
            }
            ?: mutableFromSnapshot(currentSnapshot)

        mutator(entity)
        val newChunkX = chunkXFromBlockX(entity.x)
        val newChunkZ = chunkZFromBlockZ(entity.z)
        val newChunk = ChunkPos(newChunkX, newChunkZ)
        entity.chunkX = newChunkX
        entity.chunkZ = newChunkZ
        val newSnapshot = entity.snapshot()
        snapshots[entityId] = newSnapshot

        if (oldChunk != newChunk) {
            oldChunkState?.entities?.remove(entityId)
            moveChunkIndex(entityId, oldChunk, newChunk)
            val newLane = laneFor(newChunkX, newChunkZ)
            laneChunks[newLane]
                .computeIfAbsent(newChunk) { ChunkState() }
                .inbound
                .add(entity)
            dirtyChunksByLane[oldLane].add(oldChunk)
            dirtyChunksByLane[newLane].add(newChunk)
        } else {
            if (oldChunkState != null) {
                oldChunkState.entities[entityId] = entity
            } else {
                oldLaneMap.computeIfAbsent(oldChunk) { ChunkState() }.inbound.add(entity)
            }
            dirtyChunksByLane[oldLane].add(oldChunk)
        }
        return newSnapshot
    }

    private inline fun mutateIfUuidMatches(
        entityId: Int,
        expectedUuid: UUID,
        mutator: (MutableDroppedItem) -> Unit
    ): DroppedItemSnapshot? {
        val current = snapshots[entityId] ?: return null
        if (current.uuid != expectedUuid) return null
        val updated = mutate(entityId, mutator) ?: return null
        return if (updated.uuid == expectedUuid) updated else null
    }

}
