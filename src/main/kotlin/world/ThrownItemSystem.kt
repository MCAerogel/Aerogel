package org.macaroon3145.world

import org.macaroon3145.network.codec.BlockStateRegistry
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.math.abs
import kotlin.math.ceil
import kotlin.math.floor
import kotlin.math.pow
import kotlin.math.sqrt

enum class ThrownItemKind {
    SNOWBALL,
    EGG,
    BLUE_EGG,
    BROWN_EGG
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
    val chunkPos: ChunkPos
)

data class ThrownItemRemovedEvent(
    val entityId: Int,
    val chunkPos: ChunkPos,
    val hit: Boolean,
    val x: Double,
    val y: Double,
    val z: Double
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
    private data class MutableThrownItem(
        val entityId: Int,
        val ownerEntityId: Int,
        val uuid: UUID,
        val kind: ThrownItemKind,
        var prevX: Double,
        var prevY: Double,
        var prevZ: Double,
        var x: Double,
        var y: Double,
        var z: Double,
        var vx: Double,
        var vy: Double,
        var vz: Double,
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
                chunkPos = ChunkPos(chunkX, chunkZ)
            )
        }
    }

    private val entities = ConcurrentHashMap<Int, MutableThrownItem>()
    private val snapshots = ConcurrentHashMap<Int, ThrownItemSnapshot>()
    private val chunkIndex = ConcurrentHashMap<ChunkPos, MutableSet<Int>>()
    private val pendingSpawned = ConcurrentLinkedQueue<Int>()
    private val collidableStateCache = ConcurrentHashMap<Int, Boolean>()

    fun spawn(
        entityId: Int,
        ownerEntityId: Int,
        kind: ThrownItemKind,
        x: Double,
        y: Double,
        z: Double,
        vx: Double,
        vy: Double,
        vz: Double
    ): Boolean {
        if (entityId < 0) return false
        val chunkX = chunkXFromBlockX(x)
        val chunkZ = chunkZFromBlockZ(z)
        val entity = MutableThrownItem(
            entityId = entityId,
            ownerEntityId = ownerEntityId,
            uuid = UUID.randomUUID(),
            kind = kind,
            prevX = x,
            prevY = y,
            prevZ = z,
            x = x,
            y = y,
            z = z,
            vx = vx,
            vy = vy,
            vz = vz,
            ageTicks = 0.0,
            chunkX = chunkX,
            chunkZ = chunkZ
        )
        if (entities.putIfAbsent(entityId, entity) != null) return false
        val snapshot = entity.snapshot()
        snapshots[entityId] = snapshot
        addToChunkIndex(snapshot.chunkPos, entityId)
        pendingSpawned.add(entityId)
        return true
    }

    fun remove(entityId: Int, hit: Boolean, x: Double? = null, y: Double? = null, z: Double? = null): ThrownItemRemovedEvent? {
        val entity = entities.remove(entityId) ?: return null
        val oldChunk = ChunkPos(entity.chunkX, entity.chunkZ)
        snapshots.remove(entityId)
        removeFromChunkIndex(oldChunk, entityId)
        return ThrownItemRemovedEvent(
            entityId = entityId,
            chunkPos = oldChunk,
            hit = hit,
            x = x ?: entity.x,
            y = y ?: entity.y,
            z = z ?: entity.z
        )
    }

    fun hasEntities(): Boolean = snapshots.isNotEmpty()

    fun snapshot(entityId: Int): ThrownItemSnapshot? = snapshots[entityId]

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
        if (entities.isEmpty()) return ThrownItemTickEvents(emptyList(), emptyList(), emptyList())

        val spawned = ArrayList<ThrownItemSnapshot>()
        val updated = ArrayList<ThrownItemSnapshot>()
        val removed = ArrayList<ThrownItemRemovedEvent>()

        while (true) {
            val entityId = pendingSpawned.poll() ?: break
            val snapshot = snapshots[entityId] ?: continue
            spawned.add(snapshot)
        }

        for ((entityId, entity) in entities.toList()) {
            val oldChunk = ChunkPos(entity.chunkX, entity.chunkZ)
            if (activeSimulationChunks != null && oldChunk !in activeSimulationChunks) continue
            val chunkDeltaSeconds = (chunkDeltaSecondsProvider?.invoke(oldChunk) ?: deltaSeconds)
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

            val collisionT = firstCollisionT(oldX, oldY, oldZ, entity.x, entity.y, entity.z)
            val hit = collisionT != null
            val despawn =
                entity.y < DESPAWN_Y ||
                    entity.y > WORLD_MAX_Y + 2.0 ||
                    entity.ageTicks > MAX_LIFETIME_TICKS ||
                    hit
            if (despawn) {
                val (removeX, removeY, removeZ) = if (hit) {
                    impactPointAtT(oldX, oldY, oldZ, entity.x, entity.y, entity.z, collisionT!!)
                } else {
                    Triple(entity.x, entity.y, entity.z)
                }
                if (entities.remove(entityId, entity)) {
                    snapshots.remove(entityId)
                    removeFromChunkIndex(oldChunk, entityId)
                    removed.add(ThrownItemRemovedEvent(entityId, oldChunk, hit, removeX, removeY, removeZ))
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
    }

    private fun collides(x: Double, y: Double, z: Double): Boolean {
        val bx = floor(x).toInt()
        val by = floor(y).toInt()
        val bz = floor(z).toInt()
        return isCollidableState(blockStateAt(bx, by, bz))
    }

    private fun firstCollisionT(
        oldX: Double,
        oldY: Double,
        oldZ: Double,
        newX: Double,
        newY: Double,
        newZ: Double
    ): Double? {
        if (collides(oldX, oldY, oldZ)) return 0.0
        val dx = newX - oldX
        val dy = newY - oldY
        val dz = newZ - oldZ
        val distance = sqrt(dx * dx + dy * dy + dz * dz)
        val steps = ceil(distance / SWEEP_STEP_BLOCKS).toInt().coerceAtLeast(1)
        var prevT = 0.0
        for (i in 1..steps) {
            val t = i.toDouble() / steps.toDouble()
            val x = lerp(oldX, newX, t)
            val y = lerp(oldY, newY, t)
            val z = lerp(oldZ, newZ, t)
            if (!collides(x, y, z)) {
                prevT = t
                continue
            }
            var lo = prevT
            var hi = t
            repeat(8) {
                val mid = (lo + hi) * 0.5
                val mx = lerp(oldX, newX, mid)
                val my = lerp(oldY, newY, mid)
                val mz = lerp(oldZ, newZ, mid)
                if (collides(mx, my, mz)) {
                    hi = mid
                } else {
                    lo = mid
                }
            }
            return hi
        }
        return null
    }

    private fun impactPointAtT(
        oldX: Double,
        oldY: Double,
        oldZ: Double,
        newX: Double,
        newY: Double,
        newZ: Double,
        collisionT: Double
    ): Triple<Double, Double, Double> {
        val t = (collisionT - 1.0e-4).coerceAtLeast(0.0)
        return Triple(
            lerp(oldX, newX, t),
            lerp(oldY, newY, t),
            lerp(oldZ, newZ, t)
        )
    }

    private fun lerp(a: Double, b: Double, t: Double): Double = a + (b - a) * t

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
        private const val GRAVITY_PER_TICK = 0.03
        private const val AIR_DRAG = 0.99
        private const val POSITION_EPSILON = 1.0e-4
        private const val VELOCITY_EPSILON = 1.0e-6
        private const val DESPAWN_Y = -128.0
        private const val WORLD_MAX_Y = 319
        private const val MAX_LIFETIME_TICKS = 1200.0
        private const val SWEEP_STEP_BLOCKS = 0.125
        private val NON_COLLIDING_BLOCK_KEYS = setOf(
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
}
