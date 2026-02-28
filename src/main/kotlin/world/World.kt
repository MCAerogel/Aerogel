package org.macaroon3145.world

import org.macaroon3145.network.codec.BlockStateRegistry
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionException
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executor
import java.util.concurrent.atomic.AtomicReference
import java.util.UUID

data class SpawnPoint(
    val x: Double,
    val y: Double,
    val z: Double
)

class World(
    val key: String,
    @Volatile var seed: Long,
    @Volatile var generator: WorldGenerator,
    private val baseBlockStateProvider: (Int, Int, Int) -> Int
) {
    data class BlockEntityData(
        val typeId: Int,
        val nbtPayload: ByteArray
    )

    private val entityProcessors = CopyOnWriteArrayList<ChunkEntityProcessor>()
    private val changedBlockStates = ConcurrentHashMap<BlockPos, Int>()
    private val changedBlocksByChunk = ConcurrentHashMap<ChunkPos, MutableSet<BlockPos>>()
    private val changedBlockEntities = ConcurrentHashMap<BlockPos, BlockEntityData>()
    private val changedBlockEntitiesByChunk = ConcurrentHashMap<ChunkPos, MutableSet<BlockPos>>()
    private val droppedItemSystem = DroppedItemSystem { x, y, z -> blockStateAt(x, y, z) }
    private val inFlightChunkBuilds = ConcurrentHashMap<ChunkPos, CompletableFuture<GeneratedChunk>>()
    @Volatile private var warmedSeed: Long = Long.MIN_VALUE
    private val warmupFutureRef = AtomicReference<CompletableFuture<Long>?>(null)
    private val cachedSpawnPointRef = AtomicReference<SpawnPoint?>(null)

    fun registerEntityProcessor(processor: ChunkEntityProcessor) {
        entityProcessors.add(processor)
    }

    fun prewarmChunkGeneration() {
        val warmup = generator as? WarmupWorldGenerator ?: return
        val currentSeed = seed
        if (warmedSeed == currentSeed) return
        while (true) {
            if (warmedSeed == currentSeed) return

            val inFlight = warmupFutureRef.get()
            if (inFlight != null) {
                try {
                    inFlight.join()
                } catch (e: CompletionException) {
                    throw IllegalStateException(
                        "World warmup failed for key=$key seed=$currentSeed",
                        e.cause ?: e
                    )
                }
                continue
            }

            val created = CompletableFuture<Long>()
            if (!warmupFutureRef.compareAndSet(null, created)) continue

            runCatching {
                warmup.warmup(currentSeed)
                warmedSeed = currentSeed
                currentSeed
            }.onSuccess { warmed ->
                created.complete(warmed)
            }.onFailure { throwable ->
                created.completeExceptionally(throwable)
            }
            warmupFutureRef.compareAndSet(created, null)

            try {
                created.join()
            } catch (e: CompletionException) {
                throw IllegalStateException(
                    "World warmup failed for key=$key seed=$currentSeed",
                    e.cause ?: e
                )
            }
            return
        }
    }

    fun buildChunk(chunkPos: ChunkPos): GeneratedChunk {
        return awaitChunkBuild(buildChunkAsync(chunkPos, directExecutor))
    }

    fun buildChunkAsync(chunkPos: ChunkPos, fallbackExecutor: Executor): CompletableFuture<GeneratedChunk> {
        inFlightChunkBuilds[chunkPos]?.let { return it }

        val created = CompletableFuture<GeneratedChunk>()
        val existing = inFlightChunkBuilds.putIfAbsent(chunkPos, created)
        if (existing != null) return existing

        val context = ChunkGenerationContext(
            worldKey = key,
            seed = seed,
            chunkPos = chunkPos
        )

        val result = runCatching {
            val base = (generator as? AsyncWorldGenerator)?.generateChunkAsync(context)
                ?: CompletableFuture.supplyAsync({ generator.generateChunk(context) }, fallbackExecutor)
            if (entityProcessors.isEmpty()) {
                base
            } else {
                base.thenApply { generated ->
                    var out = generated
                    for (processor in entityProcessors) {
                        out = processor.process(context, out)
                    }
                    out
                }
            }
        }.getOrElse { throwable ->
            created.completeExceptionally(throwable)
            inFlightChunkBuilds.remove(chunkPos, created)
            when (throwable) {
                is RuntimeException -> throw throwable
                is Error -> throw throwable
                else -> throw RuntimeException(throwable)
            }
        }

        result.whenComplete { generated, error ->
            if (error != null) {
                created.completeExceptionally(error)
            } else {
                created.complete(generated)
            }
            inFlightChunkBuilds.remove(chunkPos, created)
        }

        return created
    }

    private fun awaitChunkBuild(future: CompletableFuture<GeneratedChunk>): GeneratedChunk {
        return try {
            future.join()
        } catch (e: CompletionException) {
            val cause = e.cause ?: e
            when (cause) {
                is RuntimeException -> throw cause
                is Error -> throw cause
                else -> throw RuntimeException(cause)
            }
        }
    }

    fun blockStateAt(x: Int, y: Int, z: Int): Int {
        if (changedBlockStates.isEmpty()) {
            return baseBlockStateProvider(x, y, z)
        }
        val pos = BlockPos(x, y, z)
        return changedBlockStates[pos] ?: baseBlockStateProvider(x, y, z)
    }

    fun setBlockState(x: Int, y: Int, z: Int, stateId: Int) {
        val pos = BlockPos(x, y, z)
        val base = baseBlockStateProvider(x, y, z)
        if (stateId == base) {
            changedBlockStates.remove(pos)
            changedBlocksByChunk[pos.chunkPos()]?.remove(pos)
            changedBlockEntities.remove(pos)
            changedBlockEntitiesByChunk[pos.chunkPos()]?.remove(pos)
            return
        }
        changedBlockStates[pos] = stateId
        changedBlocksByChunk
            .computeIfAbsent(pos.chunkPos()) { ConcurrentHashMap.newKeySet() }
            .add(pos)
    }

    fun setBlockEntity(x: Int, y: Int, z: Int, data: BlockEntityData?) {
        val pos = BlockPos(x, y, z)
        if (data == null || data.typeId < 0 || data.nbtPayload.isEmpty()) {
            changedBlockEntities.remove(pos)
            changedBlockEntitiesByChunk[pos.chunkPos()]?.remove(pos)
            return
        }

        changedBlockEntities[pos] = data
        changedBlockEntitiesByChunk
            .computeIfAbsent(pos.chunkPos()) { ConcurrentHashMap.newKeySet() }
            .add(pos)
    }

    fun changedBlocksInChunk(chunkX: Int, chunkZ: Int): List<Pair<BlockPos, Int>> {
        val key = ChunkPos(chunkX, chunkZ)
        val positions = changedBlocksByChunk[key] ?: return emptyList()
        val result = ArrayList<Pair<BlockPos, Int>>(positions.size)
        for (pos in positions) {
            val state = changedBlockStates[pos] ?: continue
            result.add(pos to state)
        }
        return result
    }

    fun blockEntityAt(x: Int, y: Int, z: Int): BlockEntityData? {
        return changedBlockEntities[BlockPos(x, y, z)]
    }

    fun changedBlockEntitiesInChunk(chunkX: Int, chunkZ: Int): List<Pair<BlockPos, BlockEntityData>> {
        val key = ChunkPos(chunkX, chunkZ)
        val positions = changedBlockEntitiesByChunk[key] ?: return emptyList()
        val result = ArrayList<Pair<BlockPos, BlockEntityData>>(positions.size)
        for (pos in positions) {
            val data = changedBlockEntities[pos] ?: continue
            result.add(pos to data)
        }
        return result
    }

    fun hasChunkDeltaChanges(): Boolean {
        return changedBlocksByChunk.isNotEmpty() || changedBlockEntitiesByChunk.isNotEmpty()
    }

    fun spawnDroppedItem(
        entityId: Int,
        itemId: Int,
        itemCount: Int,
        x: Double,
        y: Double,
        z: Double,
        vx: Double,
        vy: Double,
        vz: Double,
        pickupDelaySeconds: Double = 2.0
    ): Boolean {
        return droppedItemSystem.spawn(
            entityId = entityId,
            itemId = itemId,
            itemCount = itemCount,
            x = x,
            y = y,
            z = z,
            vx = vx,
            vy = vy,
            vz = vz,
            pickupDelaySeconds = pickupDelaySeconds
        )
    }

    fun removeDroppedItem(entityId: Int): DroppedItemSnapshot? {
        return droppedItemSystem.remove(entityId)
    }

    fun droppedItemsInChunk(chunkX: Int, chunkZ: Int): List<DroppedItemSnapshot> {
        return droppedItemSystem.snapshotsInChunk(chunkX, chunkZ)
    }

    fun droppedItemEntityIdsInChunk(chunkX: Int, chunkZ: Int): IntArray {
        return droppedItemSystem.entityIdsInChunk(chunkX, chunkZ)
    }

    fun droppedItemSnapshot(entityId: Int): DroppedItemSnapshot? {
        return droppedItemSystem.snapshot(entityId)
    }

    fun requestDroppedItemUnstuckAtBlock(x: Int, y: Int, z: Int) {
        droppedItemSystem.requestUnstuckForBlock(x, y, z)
    }

    fun hasDroppedItems(): Boolean {
        return droppedItemSystem.hasEntities()
    }

    fun tickDroppedItems(deltaSeconds: Double, activeSimulationChunks: Set<ChunkPos>? = null): DroppedItemTickEvents {
        return droppedItemSystem.tick(deltaSeconds, activeSimulationChunks)
    }

    fun spawnPointForPlayer(_playerUuid: UUID): SpawnPoint {
        cachedSpawnPointRef.get()?.let { return it }
        synchronized(cachedSpawnPointRef) {
            cachedSpawnPointRef.get()?.let { return it }
            val computed = findSharedSpawnNearOrigin()
            cachedSpawnPointRef.set(computed)
            return computed
        }
    }

    fun highestSpawnPointAt(blockX: Int = 0, blockZ: Int = 0): SpawnPoint {
        for (feetY in (WORLD_MAX_Y - 1) downTo (WORLD_MIN_Y + 1)) {
            val below = blockStateAt(blockX, feetY - 1, blockZ)
            val feet = blockStateAt(blockX, feetY, blockZ)
            val head = blockStateAt(blockX, feetY + 1, blockZ)
            if (!isSolidSpawnSupport(below)) continue
            if (feet != AIR_STATE_ID || head != AIR_STATE_ID) continue
            return SpawnPoint(blockX + 0.5, feetY.toDouble(), blockZ + 0.5)
        }

        val surfaceY = highestNonAirYInColumn(blockX, blockZ)
        if (surfaceY >= WORLD_MIN_Y) {
            val spawnY = (surfaceY + 1).coerceIn(WORLD_MIN_Y, WORLD_MAX_Y).toDouble()
            return SpawnPoint(blockX + 0.5, spawnY, blockZ + 0.5)
        }
        return SpawnPoint(blockX + 0.5, DEFAULT_FALLBACK_SPAWN_Y, blockZ + 0.5)
    }

    private fun highestNonAirYInColumn(x: Int, z: Int): Int {
        for (y in WORLD_MAX_Y downTo WORLD_MIN_Y) {
            if (blockStateAt(x, y, z) != AIR_STATE_ID) {
                return y
            }
        }
        return WORLD_MIN_Y - 1
    }

    private fun isSolidSpawnSupport(stateId: Int): Boolean {
        if (stateId == AIR_STATE_ID) return false
        return collidableStateCache.computeIfAbsent(stateId) { id ->
            val blockKey = BlockStateRegistry.parsedState(id)?.blockKey ?: return@computeIfAbsent true
            blockKey !in NON_COLLIDING_SPAWN_SUPPORT_KEYS
        }
    }

    private fun spawnYFromHeightmap(generated: GeneratedChunk, blockX: Int, blockZ: Int): Double? {
        val heightmap = generated.heightmaps.firstOrNull { it.typeId == MOTION_BLOCKING_NO_LEAVES_HEIGHTMAP_ID }
            ?: generated.heightmaps.firstOrNull { it.typeId == MOTION_BLOCKING_HEIGHTMAP_ID }
            ?: return null
        if (heightmap.values.isEmpty()) return null

        val bitsPerEntry = heightmapBitsPerEntry()
        if (bitsPerEntry <= 0) return null
        val valuesPerLong = 64 / bitsPerEntry
        if (valuesPerLong <= 0) return null
        val mask = (1L shl bitsPerEntry) - 1L

        val index = ((blockZ and 15) shl 4) or (blockX and 15)
        val dataIndex = index / valuesPerLong
        if (dataIndex !in heightmap.values.indices) return null
        val bitOffset = (index - dataIndex * valuesPerLong) * bitsPerEntry
        val relativeY = ((heightmap.values[dataIndex] ushr bitOffset) and mask).toInt()
        val spawnY = (WORLD_MIN_Y + relativeY).coerceIn(WORLD_MIN_Y, WORLD_MAX_Y).toDouble()
        return spawnY
    }

    private fun findSharedSpawnNearOrigin(): SpawnPoint {
        for (radius in 0..SPAWN_SEARCH_CHUNK_RADIUS) {
            if (radius == 0) {
                findSpawnInChunk(0, 0)?.let { return it }
                continue
            }

            for (chunkX in -radius..radius) {
                findSpawnInChunk(chunkX, -radius)?.let { return it }
                findSpawnInChunk(chunkX, radius)?.let { return it }
            }
            for (chunkZ in (-radius + 1) until radius) {
                findSpawnInChunk(-radius, chunkZ)?.let { return it }
                findSpawnInChunk(radius, chunkZ)?.let { return it }
            }
        }
        return highestSpawnPointAt()
    }

    private fun findSpawnInChunk(chunkX: Int, chunkZ: Int): SpawnPoint? {
        val generated = runCatching { buildChunk(ChunkPos(chunkX, chunkZ)) }.getOrNull() ?: return null
        for ((localX, localZ) in SPAWN_PROBE_LOCAL_COORDS) {
            val blockX = (chunkX shl 4) + localX
            val blockZ = (chunkZ shl 4) + localZ
            val feetY = spawnYFromHeightmap(generated, blockX, blockZ)?.toInt() ?: continue
            if (!isSpawnClearAt(blockX, feetY, blockZ)) continue
            return SpawnPoint(blockX + 0.5, feetY.toDouble(), blockZ + 0.5)
        }
        return null
    }

    private fun isSpawnClearAt(blockX: Int, feetY: Int, blockZ: Int): Boolean {
        if (feetY <= WORLD_MIN_Y || feetY >= WORLD_MAX_Y) return false
        val below = blockStateAt(blockX, feetY - 1, blockZ)
        val feet = blockStateAt(blockX, feetY, blockZ)
        val head = blockStateAt(blockX, feetY + 1, blockZ)
        return isSolidSpawnSupport(below) && feet == AIR_STATE_ID && head == AIR_STATE_ID
    }

    private fun heightmapBitsPerEntry(): Int {
        val worldHeight = WORLD_MAX_Y - WORLD_MIN_Y + 1
        val encodedRange = worldHeight + 1
        var bits = 0
        var value = 1
        while (value < encodedRange) {
            value = value shl 1
            bits++
        }
        return bits.coerceAtLeast(1)
    }

    private companion object {
        private val directExecutor = Executor { runnable -> runnable.run() }
        private const val AIR_STATE_ID = 0
        private const val WORLD_MIN_Y = -64
        private const val WORLD_MAX_Y = 319
        private const val DEFAULT_FALLBACK_SPAWN_Y = 65.0
        private const val MOTION_BLOCKING_HEIGHTMAP_ID = 4
        private const val MOTION_BLOCKING_NO_LEAVES_HEIGHTMAP_ID = 5
        private const val SPAWN_SEARCH_CHUNK_RADIUS = 8
        private val SPAWN_PROBE_LOCAL_COORDS = intArrayOf(
            8, 8,
            4, 4,
            12, 12,
            4, 12,
            12, 4,
            8, 4,
            8, 12,
            4, 8,
            12, 8
        ).toList().chunked(2).map { it[0] to it[1] }
        private val collidableStateCache = ConcurrentHashMap<Int, Boolean>()
        private val NON_COLLIDING_SPAWN_SUPPORT_KEYS = hashSetOf(
            "minecraft:air",
            "minecraft:cave_air",
            "minecraft:void_air",
            "minecraft:water",
            "minecraft:lava",
            "minecraft:bubble_column",
            "minecraft:short_grass",
            "minecraft:tall_grass",
            "minecraft:leaf_litter",
            "minecraft:fern",
            "minecraft:large_fern",
            "minecraft:dead_bush",
            "minecraft:seagrass",
            "minecraft:tall_seagrass",
            "minecraft:kelp",
            "minecraft:kelp_plant",
            "minecraft:lily_pad",
            "minecraft:vine",
            "minecraft:weeping_vines",
            "minecraft:weeping_vines_plant",
            "minecraft:twisting_vines",
            "minecraft:twisting_vines_plant",
            "minecraft:cave_vines",
            "minecraft:cave_vines_plant",
            "minecraft:glow_lichen",
            "minecraft:sugar_cane",
            "minecraft:torchflower_crop",
            "minecraft:pitcher_crop",
            "minecraft:wheat",
            "minecraft:carrots",
            "minecraft:potatoes",
            "minecraft:beetroots",
            "minecraft:melon_stem",
            "minecraft:pumpkin_stem",
            "minecraft:attached_melon_stem",
            "minecraft:attached_pumpkin_stem",
            "minecraft:cocoa",
            "minecraft:nether_wart",
            "minecraft:sweet_berry_bush",
            "minecraft:fire",
            "minecraft:soul_fire"
        )
    }
}
