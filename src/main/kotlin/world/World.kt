package org.macaroon3145.world

import org.macaroon3145.config.ServerConfig
import org.macaroon3145.network.codec.BlockStateRegistry
import org.macaroon3145.network.handler.ItemStackState
import org.macaroon3145.world.generators.FoliaSharedMemoryWorldGenerator
import org.macaroon3145.world.storage.VanillaLevelDatSeedStore
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionException
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executor
import java.util.concurrent.CancellationException
import java.util.concurrent.Future
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.UUID
import java.util.concurrent.locks.LockSupport

data class SpawnPoint(
    val x: Double,
    val y: Double,
    val z: Double
)

data class FluidBlockChange(
    val x: Int,
    val y: Int,
    val z: Int,
    val previousStateId: Int,
    val stateId: Int,
    val chunkPos: ChunkPos
)

data class FluidTickEvents(
    val changed: List<FluidBlockChange>
) {
    fun isEmpty(): Boolean = changed.isEmpty()
}

data class GrassBlockChange(
    val x: Int,
    val y: Int,
    val z: Int,
    val previousStateId: Int,
    val stateId: Int,
    val chunkPos: ChunkPos
)

data class GrassTickEvents(
    val changed: List<GrassBlockChange>
) {
    fun isEmpty(): Boolean = changed.isEmpty()
}

private data class PlannedFluidChange(
    val pos: BlockPos,
    val previousStateId: Int,
    val nextStateId: Int
)

class World(
    val key: String,
    @Volatile var seed: Long,
    @Volatile var generator: WorldGenerator,
    private val baseBlockStateProvider: (Int, Int, Int) -> Int,
    private val cachedBlockStateProvider: (Int, Int, Int) -> Int? = { _, _, _ -> null },
    private val rawBrightnessProvider: (Int, Int, Int) -> Int = { _, _, _ -> 0 },
    private val cachedRawBrightnessProvider: (Int, Int, Int) -> Int? = { _, _, _ -> null }
) {
    private fun configuredMaxInFlightChunkBuilds(): Int {
        val cpuBased = (Runtime.getRuntime().availableProcessors() * 6).coerceAtLeast(64)
        return System.getProperty("aerogel.chunk.max-inflight-builds")
            ?.toIntOrNull()
            ?.coerceAtLeast(16)
            ?: cpuBased
    }

    data class BlockEntityData(
        val typeId: Int,
        val nbtPayload: ByteArray
    )

    private val entityProcessors = CopyOnWriteArrayList<ChunkEntityProcessor>()
    private val changedBlockStates = ConcurrentHashMap<BlockPos, Int>()
    private val changedBlocksByChunk = ConcurrentHashMap<ChunkPos, MutableSet<BlockPos>>()
    private val changedBlockRevisionByChunk = ConcurrentHashMap<ChunkPos, AtomicLong>()
    private val changedBlockEntities = ConcurrentHashMap<BlockPos, BlockEntityData>()
    private val changedBlockEntitiesByChunk = ConcurrentHashMap<ChunkPos, MutableSet<BlockPos>>()
    private val dirtyTerrainChunksQueue = ConcurrentLinkedQueue<ChunkPos>()
    private val dirtyTerrainChunksDedup = ConcurrentHashMap.newKeySet<ChunkPos>()
    private val chunkProcessingProfiler = ChunkProcessingProfiler()
    private val droppedItemSystem = DroppedItemSystem { x, y, z -> blockStateAt(x, y, z) }
    private val thrownItemSystem = ThrownItemSystem { x, y, z -> blockStateAt(x, y, z) }
    private val animalSystem = AnimalSystem(
        blockStateAt = { x, y, z -> blockStateAt(x, y, z) },
        rawBrightnessAt = { x, y, z -> rawBrightnessAt(x, y, z) }
    )
    private val fallingBlockSystem = FallingBlockSystem(
        blockStateAt = { x, y, z -> blockStateAt(x, y, z) },
        setBlockState = { x, y, z, stateId -> setBlockState(x, y, z, stateId) },
        clearBlockEntity = { x, y, z -> setBlockEntity(x, y, z, null) }
    )
    private val inFlightChunkBuilds = ConcurrentHashMap<ChunkPos, InFlightChunkBuild>()
    private val inFlightChunkBuildCount = AtomicInteger(0)
    private val maxInFlightChunkBuilds = configuredMaxInFlightChunkBuilds()
    private val pendingFluidUpdates = ConcurrentHashMap.newKeySet<BlockPos>()
    private val pendingFluidTickChanges = ConcurrentLinkedQueue<FluidBlockChange>()
    private val pendingGrassUpdates = ConcurrentHashMap.newKeySet<BlockPos>()
    private val pendingGrassUpdatesByChunk = ConcurrentHashMap<ChunkPos, MutableSet<BlockPos>>()
    private val pendingGrassDelaySeconds = ConcurrentHashMap<BlockPos, Double>()
    private val pendingGrassTickChanges = ConcurrentLinkedQueue<GrassBlockChange>()
    private val pendingAnimalTickEvents = ConcurrentLinkedQueue<AnimalTickEvents>()
    private val droppedItemLastTickNanos = ConcurrentHashMap<ChunkPos, Long>()
    private val droppedItemChunkAccumulatorSeconds = ConcurrentHashMap<ChunkPos, Double>()
    private val animalLastTickNanos = ConcurrentHashMap<ChunkPos, Long>()
    private val thrownItemLastTickNanos = ConcurrentHashMap<ChunkPos, Long>()
    private val fallingBlockLastTickNanos = ConcurrentHashMap<ChunkPos, Long>()
    private val fluidLastTickNanos = ConcurrentHashMap<ChunkPos, Long>()
    private val fluidChunkAccumulatorSeconds = ConcurrentHashMap<ChunkPos, Double>()
    private val grassLastTickNanos = ConcurrentHashMap<ChunkPos, Long>()
    private val animalChunkTickInFlight = ConcurrentHashMap<ChunkPos, AtomicBoolean>()
    private val fluidChunkTickInFlight = ConcurrentHashMap<ChunkPos, AtomicBoolean>()
    private val grassChunkTickInFlight = ConcurrentHashMap<ChunkPos, AtomicBoolean>()
    private val chunkActorScheduler = ChunkActorScheduler()
    @Volatile private var warmedSeed: Long = Long.MIN_VALUE
    private val warmupFutureRef = AtomicReference<CompletableFuture<Long>?>(null)
    private val cachedSpawnPointRef = AtomicReference<SpawnPoint?>(null)
    private val spawnResolveInFlight = AtomicBoolean(false)

    private data class InFlightChunkBuild(
        val chunkPos: ChunkPos,
        val future: CompletableFuture<GeneratedChunk>,
        val sourceFutureRef: AtomicReference<CompletableFuture<GeneratedChunk>?> = AtomicReference(null),
        val activeWaiters: AtomicInteger = AtomicInteger(0),
        val lastDemandNanos: AtomicLong = AtomicLong(System.nanoTime()),
        val cancelRequested: AtomicBoolean = AtomicBoolean(false)
    )

    fun registerEntityProcessor(processor: ChunkEntityProcessor) {
        entityProcessors.add(processor)
    }

    fun prewarmChunkGeneration() {
        prewarmChunkGenerationAsync()
    }

    fun prewarmChunkGenerationAsync(): CompletableFuture<Long> {
        val warmup = generator as? WarmupWorldGenerator
            ?: return CompletableFuture.completedFuture(seed)
        val currentSeed = seed
        if (warmedSeed == currentSeed) return CompletableFuture.completedFuture(currentSeed)
        while (true) {
            if (warmedSeed == currentSeed) return CompletableFuture.completedFuture(currentSeed)

            val inFlight = warmupFutureRef.get()
            if (inFlight != null) {
                return inFlight
            }

            val created = CompletableFuture<Long>()
            if (!warmupFutureRef.compareAndSet(null, created)) continue

            Thread.ofVirtual()
                .name("aerogel-world-warmup-$key")
                .start {
                    runCatching {
                        warmup.warmup(currentSeed)
                        warmedSeed = currentSeed
                        currentSeed
                    }.onSuccess { warmed ->
                        created.complete(warmed)
                    }.onFailure { throwable ->
                        created.completeExceptionally(
                            IllegalStateException(
                                "World warmup failed for key=$key seed=$currentSeed",
                                throwable
                            )
                        )
                    }.also {
                        warmupFutureRef.compareAndSet(created, null)
                    }
                }
            return created
        }
    }

    fun buildChunk(chunkPos: ChunkPos): GeneratedChunk {
        return buildChunk(chunkPos) { true }
    }

    fun buildChunk(chunkPos: ChunkPos, shouldKeepWaiting: () -> Boolean): GeneratedChunk {
        val build = getOrCreateChunkBuild(chunkPos, directExecutor)
        build.activeWaiters.incrementAndGet()
        build.lastDemandNanos.set(System.nanoTime())
        return try {
            awaitChunkBuild(build, shouldKeepWaiting)
        } finally {
            releaseChunkBuildWaiter(build)
        }
    }

    fun buildChunkAsync(chunkPos: ChunkPos, fallbackExecutor: Executor): CompletableFuture<GeneratedChunk> {
        return getOrCreateChunkBuild(chunkPos, fallbackExecutor).future
    }

    private fun getOrCreateChunkBuild(chunkPos: ChunkPos, fallbackExecutor: Executor): InFlightChunkBuild {
        while (true) {
            inFlightChunkBuilds[chunkPos]?.let { existing ->
                if (!existing.future.isCancelled) {
                    existing.lastDemandNanos.set(System.nanoTime())
                    return existing
                }
                removeInFlightBuild(existing.chunkPos, existing)
            }

            if (!tryAcquireInFlightBuildPermit()) {
                // Hard backpressure to keep chunk build cost stable under movement churn.
                LockSupport.parkNanos(200_000L)
                continue
            }

            val created = InFlightChunkBuild(
                chunkPos = chunkPos,
                future = CompletableFuture<GeneratedChunk>()
            )
            val existing = inFlightChunkBuilds.putIfAbsent(chunkPos, created)
            if (existing != null) {
                releaseInFlightBuildPermit()
                existing.lastDemandNanos.set(System.nanoTime())
                return existing
            }

            val context = ChunkGenerationContext(
                worldKey = key,
                seed = seed,
                chunkPos = chunkPos,
                isCancelled = { created.cancelRequested.get() }
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
                created.future.completeExceptionally(throwable)
                removeInFlightBuild(chunkPos, created)
                when (throwable) {
                    is RuntimeException -> throw throwable
                    is Error -> throw throwable
                    else -> throw RuntimeException(throwable)
                }
            }
            created.sourceFutureRef.set(result)

            result.whenComplete { generated, error ->
                if (error != null) {
                    created.future.completeExceptionally(error)
                } else {
                    created.future.complete(generated)
                }
                removeInFlightBuild(chunkPos, created)
            }

            return created
        }
    }

    private fun enforceInFlightBuildBudget(preferredChunk: ChunkPos) {
        val currentSize = inFlightChunkBuilds.size
        if (currentSize < maxInFlightChunkBuilds) return

        val over = (currentSize - maxInFlightChunkBuilds + 1).coerceAtLeast(1)
        val candidates = inFlightChunkBuilds.values
            .asSequence()
            .filter { it.chunkPos != preferredChunk && !it.future.isDone }
            .sortedWith(
                compareBy<InFlightChunkBuild> { it.activeWaiters.get() }
                    .thenBy { it.lastDemandNanos.get() }
            )
            .toList()
        if (candidates.isEmpty()) return

        val zeroWaiterVictims = candidates
            .asSequence()
            .filter { it.activeWaiters.get() <= 0 }
            .take(over)
            .toList()
        val victims = if (zeroWaiterVictims.size >= over) {
            zeroWaiterVictims
        } else {
            val additional = candidates
                .asSequence()
                .filter { it.activeWaiters.get() > 0 }
                .take(over - zeroWaiterVictims.size)
                .toList()
            zeroWaiterVictims + additional
        }
        if (victims.isEmpty()) return

        for (victim in victims) {
            victim.cancelRequested.set(true)
            inFlightChunkBuilds.remove(victim.chunkPos, victim)
            victim.sourceFutureRef.get()?.cancel(true)
        }
    }

    private fun awaitChunkBuild(build: InFlightChunkBuild, shouldKeepWaiting: () -> Boolean): GeneratedChunk {
        while (true) {
            if (!shouldKeepWaiting()) {
                throw CancellationException("Chunk build wait cancelled")
            }
            val future = build.future
            if (!future.isDone) {
                LockSupport.parkNanos(200_000L)
                continue
            }
            try {
                return future.join()
            } catch (error: Throwable) {
                val cause = (error as? CompletionException)?.cause ?: error
                when (cause) {
                    is RuntimeException -> throw cause
                    is Error -> throw cause
                    else -> throw RuntimeException(cause)
                }
            }
        }
    }

    private fun releaseChunkBuildWaiter(build: InFlightChunkBuild) {
        val remaining = build.activeWaiters.decrementAndGet()
        if (remaining > 0) return
        if (build.future.isDone) return

        // No active demand remains for this build (e.g. stream target moved away).
        // Cancel promptly to prevent stale in-flight work from accumulating.
        build.cancelRequested.set(true)
        build.sourceFutureRef.get()?.cancel(true)
        build.future.cancel(true)
        removeInFlightBuild(build.chunkPos, build)
    }

    private fun tryAcquireInFlightBuildPermit(): Boolean {
        while (true) {
            val current = inFlightChunkBuildCount.get()
            if (current >= maxInFlightChunkBuilds) return false
            if (inFlightChunkBuildCount.compareAndSet(current, current + 1)) return true
        }
    }

    private fun releaseInFlightBuildPermit() {
        while (true) {
            val current = inFlightChunkBuildCount.get()
            if (current <= 0) return
            if (inFlightChunkBuildCount.compareAndSet(current, current - 1)) return
        }
    }

    private fun removeInFlightBuild(chunkPos: ChunkPos, build: InFlightChunkBuild) {
        if (inFlightChunkBuilds.remove(chunkPos, build)) {
            releaseInFlightBuildPermit()
        }
    }

    fun blockStateAt(x: Int, y: Int, z: Int): Int {
        if (changedBlockStates.isEmpty()) {
            return baseBlockStateProvider(x, y, z)
        }
        val pos = BlockPos(x, y, z)
        return changedBlockStates[pos] ?: baseBlockStateProvider(x, y, z)
    }

    fun blockStateAtIfCached(x: Int, y: Int, z: Int): Int? {
        if (changedBlockStates.isNotEmpty()) {
            val changed = changedBlockStates[BlockPos(x, y, z)]
            if (changed != null) return changed
        }
        return cachedBlockStateProvider(x, y, z)
    }

    fun rawBrightnessAt(x: Int, y: Int, z: Int): Int {
        return rawBrightnessProvider(x, y, z).coerceIn(0, 15)
    }

    fun rawBrightnessAtIfCached(x: Int, y: Int, z: Int): Int? {
        return cachedRawBrightnessProvider(x, y, z)?.coerceIn(0, 15)
    }

    fun setBlockState(x: Int, y: Int, z: Int, stateId: Int) {
        setBlockStateInternal(x, y, z, stateId, enqueueFluidUpdates = true)
    }

    fun setBlockStateWithoutFluidUpdates(x: Int, y: Int, z: Int, stateId: Int) {
        setBlockStateInternal(x, y, z, stateId, enqueueFluidUpdates = false)
    }

    fun setBlockStateFromChunkLoad(x: Int, y: Int, z: Int, stateId: Int) {
        setBlockStateInternalFromChunkLoad(x, y, z, stateId)
    }

    private fun setBlockStateInternal(x: Int, y: Int, z: Int, stateId: Int, enqueueFluidUpdates: Boolean) {
        val pos = BlockPos(x, y, z)
        val chunkPos = pos.chunkPos()
        val base = baseBlockStateProvider(x, y, z)
        val previous = changedBlockStates[pos] ?: base
        if (previous == stateId) return
        if (stateId == base) {
            changedBlockStates.remove(pos)
            changedBlocksByChunk[chunkPos]?.remove(pos)
            changedBlockEntities.remove(pos)
            changedBlockEntitiesByChunk[chunkPos]?.remove(pos)
            changedBlockRevisionByChunk.computeIfAbsent(chunkPos) { AtomicLong(0L) }.incrementAndGet()
            FoliaSharedMemoryWorldGenerator.invalidateChunkGeneratedAndLighting(key, chunkPos.x, chunkPos.z)
            markTerrainChunkDirty(chunkPos)
            if (enqueueFluidUpdates) {
                enqueueFluidUpdatesAround(x, y, z)
            }
            enqueueGrassUpdatesAround(x, y, z)
            return
        }
        changedBlockStates[pos] = stateId
        changedBlocksByChunk
            .computeIfAbsent(chunkPos) { ConcurrentHashMap.newKeySet() }
            .add(pos)
        changedBlockRevisionByChunk.computeIfAbsent(chunkPos) { AtomicLong(0L) }.incrementAndGet()
        FoliaSharedMemoryWorldGenerator.invalidateChunkGeneratedAndLighting(key, chunkPos.x, chunkPos.z)
        markTerrainChunkDirty(chunkPos)
        if (enqueueFluidUpdates) {
            enqueueFluidUpdatesAround(x, y, z)
        }
        enqueueGrassUpdatesAround(x, y, z)
    }

    private fun setBlockStateInternalFromChunkLoad(x: Int, y: Int, z: Int, stateId: Int) {
        val pos = BlockPos(x, y, z)
        val chunkPos = pos.chunkPos()
        val base = baseBlockStateProvider(x, y, z)
        val previous = changedBlockStates[pos] ?: base
        if (previous == stateId) return
        if (stateId == base) {
            changedBlockStates.remove(pos)
            changedBlocksByChunk[chunkPos]?.remove(pos)
            changedBlockEntities.remove(pos)
            changedBlockEntitiesByChunk[chunkPos]?.remove(pos)
            changedBlockRevisionByChunk.computeIfAbsent(chunkPos) { AtomicLong(0L) }.incrementAndGet()
            FoliaSharedMemoryWorldGenerator.invalidateChunkGeneratedAndLighting(key, chunkPos.x, chunkPos.z)
            markTerrainChunkDirty(chunkPos)
            return
        }
        changedBlockStates[pos] = stateId
        changedBlocksByChunk
            .computeIfAbsent(chunkPos) { ConcurrentHashMap.newKeySet() }
            .add(pos)
        changedBlockRevisionByChunk.computeIfAbsent(chunkPos) { AtomicLong(0L) }.incrementAndGet()
        FoliaSharedMemoryWorldGenerator.invalidateChunkGeneratedAndLighting(key, chunkPos.x, chunkPos.z)
        markTerrainChunkDirty(chunkPos)
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

    fun changedBlockRevision(chunkX: Int, chunkZ: Int): Long {
        return changedBlockRevisionByChunk[ChunkPos(chunkX, chunkZ)]?.get() ?: 0L
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

    fun changedChunkPositionsSnapshot(): Set<ChunkPos> {
        if (changedBlocksByChunk.isEmpty() && changedBlockEntitiesByChunk.isEmpty()) return emptySet()
        val out = HashSet<ChunkPos>(changedBlocksByChunk.size + changedBlockEntitiesByChunk.size)
        out.addAll(changedBlocksByChunk.keys)
        out.addAll(changedBlockEntitiesByChunk.keys)
        return out
    }

    fun consumeDirtyTerrainChunks(): Set<ChunkPos> {
        if (dirtyTerrainChunksQueue.isEmpty()) return emptySet()
        val out = HashSet<ChunkPos>()
        while (true) {
            val chunkPos = dirtyTerrainChunksQueue.poll() ?: break
            dirtyTerrainChunksDedup.remove(chunkPos)
            out.add(chunkPos)
        }
        return out
    }

    fun topBlockStateAt(blockX: Int, blockZ: Int): Int {
        var sawCached = false
        for (y in WORLD_MAX_Y downTo WORLD_MIN_Y) {
            val cached = blockStateAtIfCached(blockX, y, blockZ) ?: continue
            sawCached = true
            if (cached != AIR_STATE_ID) return cached
        }
        if (!sawCached) {
            for (y in WORLD_MAX_Y downTo WORLD_MIN_Y) {
                val state = blockStateAt(blockX, y, blockZ)
                if (state != AIR_STATE_ID) return state
            }
        }
        return AIR_STATE_ID
    }

    fun topBlockStateAtIfCached(blockX: Int, blockZ: Int): Int? {
        var sawCached = false
        for (y in WORLD_MAX_Y downTo WORLD_MIN_Y) {
            val cached = blockStateAtIfCached(blockX, y, blockZ) ?: continue
            sawCached = true
            if (cached != AIR_STATE_ID) return cached
        }
        return if (sawCached) AIR_STATE_ID else null
    }

    private fun markTerrainChunkDirty(chunkPos: ChunkPos) {
        if (!dirtyTerrainChunksDedup.add(chunkPos)) return
        dirtyTerrainChunksQueue.add(chunkPos)
    }

    fun spawnDroppedItem(
        entityId: Int,
        stack: ItemStackState,
        x: Double,
        y: Double,
        z: Double,
        vx: Double,
        vy: Double,
        vz: Double,
        uuid: UUID = UUID.randomUUID(),
        pickupDelaySeconds: Double = 2.0
    ): Boolean {
        val chunkPos = ChunkPos(kotlin.math.floor(x / 16.0).toInt(), kotlin.math.floor(z / 16.0).toInt())
        return runOnDroppedChunk(
            chunkPos = chunkPos,
            task = {
                droppedItemSystem.spawn(
                    entityId = entityId,
                    stack = stack,
                    x = x,
                    y = y,
                    z = z,
                    vx = vx,
                    vy = vy,
                    vz = vz,
                    uuid = uuid,
                    pickupDelaySeconds = pickupDelaySeconds
                )
            },
            enqueuedFromOtherChunkActor = { false }
        )
    }

    fun removeDroppedItem(entityId: Int): DroppedItemSnapshot? {
        val chunkPos = droppedItemSystem.snapshot(entityId)?.chunkPos ?: return null
        return runOnDroppedChunk(
            chunkPos = chunkPos,
            task = { droppedItemSystem.remove(entityId) },
            enqueuedFromOtherChunkActor = { null }
        )
    }

    fun removeDroppedItemIfUuidMatches(entityId: Int, expectedUuid: UUID): DroppedItemSnapshot? {
        val chunkPos = droppedItemSystem.snapshot(entityId)?.chunkPos ?: return null
        return runOnDroppedChunk(
            chunkPos = chunkPos,
            task = { droppedItemSystem.removeIfUuidMatches(entityId, expectedUuid) },
            enqueuedFromOtherChunkActor = { null }
        )
    }

    fun removeDroppedItemIfMatches(entityId: Int, expected: DroppedItemSnapshot): DroppedItemSnapshot? {
        return runOnDroppedChunk(
            chunkPos = expected.chunkPos,
            task = { droppedItemSystem.removeIfMatches(entityId, expected) },
            enqueuedFromOtherChunkActor = { null }
        )
    }

    fun consumeDroppedItemIfMatches(entityId: Int, expected: DroppedItemSnapshot, consumeCount: Int): DroppedItemConsumeResult? {
        return runOnDroppedChunk(
            chunkPos = expected.chunkPos,
            task = { droppedItemSystem.consumeIfMatches(entityId, expected, consumeCount) },
            enqueuedFromOtherChunkActor = { null }
        )
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

    fun droppedItemSnapshots(): List<DroppedItemSnapshot> {
        return droppedItemSystem.snapshots()
    }

    fun droppedItemSnapshotByUuid(uuid: UUID): DroppedItemSnapshot? {
        return droppedItemSystem.snapshotByUuid(uuid)
    }

    fun setDroppedItemStack(entityId: Int, itemId: Int, itemCount: Int): DroppedItemSnapshot? {
        return setDroppedItemStack(entityId, ItemStackState.of(itemId = itemId, count = itemCount))
    }

    fun setDroppedItemStack(entityId: Int, stack: ItemStackState): DroppedItemSnapshot? {
        val chunkPos = droppedItemSystem.snapshot(entityId)?.chunkPos ?: return null
        return runOnDroppedChunk(
            chunkPos = chunkPos,
            task = { droppedItemSystem.updateStack(entityId, stack) },
            enqueuedFromOtherChunkActor = { null }
        )
    }

    fun setDroppedItemPosition(entityId: Int, x: Double, y: Double, z: Double): DroppedItemSnapshot? {
        val chunkPos = droppedItemSystem.snapshot(entityId)?.chunkPos ?: return null
        return runOnDroppedChunk(
            chunkPos = chunkPos,
            task = { droppedItemSystem.updatePosition(entityId, x, y, z) },
            enqueuedFromOtherChunkActor = { null }
        )
    }

    fun setDroppedItemVelocity(entityId: Int, vx: Double, vy: Double, vz: Double): DroppedItemSnapshot? {
        val chunkPos = droppedItemSystem.snapshot(entityId)?.chunkPos ?: return null
        return runOnDroppedChunk(
            chunkPos = chunkPos,
            task = { droppedItemSystem.updateVelocity(entityId, vx, vy, vz) },
            enqueuedFromOtherChunkActor = { null }
        )
    }

    fun setDroppedItemAcceleration(entityId: Int, ax: Double, ay: Double, az: Double): DroppedItemSnapshot? {
        val chunkPos = droppedItemSystem.snapshot(entityId)?.chunkPos ?: return null
        return runOnDroppedChunk(
            chunkPos = chunkPos,
            task = { droppedItemSystem.updateAcceleration(entityId, ax, ay, az) },
            enqueuedFromOtherChunkActor = { null }
        )
    }

    fun setDroppedItemPickupDelay(entityId: Int, pickupDelaySeconds: Double): DroppedItemSnapshot? {
        val chunkPos = droppedItemSystem.snapshot(entityId)?.chunkPos ?: return null
        return runOnDroppedChunk(
            chunkPos = chunkPos,
            task = { droppedItemSystem.updatePickupDelay(entityId, pickupDelaySeconds) },
            enqueuedFromOtherChunkActor = { null }
        )
    }

    fun setDroppedItemOnGround(entityId: Int, onGround: Boolean): DroppedItemSnapshot? {
        val chunkPos = droppedItemSystem.snapshot(entityId)?.chunkPos ?: return null
        return runOnDroppedChunk(
            chunkPos = chunkPos,
            task = { droppedItemSystem.updateOnGround(entityId, onGround) },
            enqueuedFromOtherChunkActor = { null }
        )
    }

    fun beginChunkProcessingFrame(tickSequence: Long = Long.MIN_VALUE): ChunkProcessingProfiler.Frame {
        return chunkProcessingProfiler.beginFrame(tickSequence)
    }

    fun recordChunkProcessingNanos(
        frame: ChunkProcessingProfiler.Frame,
        chunkPos: ChunkPos,
        elapsedNanos: Long,
        category: String = "other"
    ) {
        frame.record(chunkPos, elapsedNanos, category)
    }

    fun finishChunkProcessingFrame(
        frame: ChunkProcessingProfiler.Frame,
        activeChunks: Set<ChunkPos>,
        includeZeroForInactive: Boolean = true,
        accumulateIntoLast: Boolean = false
    ) {
        chunkProcessingProfiler.finishFrame(
            frame = frame,
            activeChunks = activeChunks,
            includeZeroForInactive = includeZeroForInactive,
            accumulateIntoLast = accumulateIntoLast
        )
    }

    fun chunkMspt(chunkX: Int, chunkZ: Int): Double {
        return chunkProcessingProfiler.mspt(chunkX, chunkZ)
    }

    fun chunkTps(chunkX: Int, chunkZ: Int): Double {
        return chunkProcessingProfiler.tps(chunkX, chunkZ)
    }

    fun isChunkIdle(chunkX: Int, chunkZ: Int): Boolean {
        return chunkProcessingProfiler.isChunkIdle(chunkX, chunkZ)
    }

    fun chunkEwmaMspt(chunkX: Int, chunkZ: Int): Double {
        return chunkProcessingProfiler.ewmaMspt(chunkX, chunkZ)
    }

    fun chunkEwmaTps(chunkX: Int, chunkZ: Int): Double {
        return chunkProcessingProfiler.ewmaTps(chunkX, chunkZ)
    }

    fun topChunkStatsByMspt(limit: Int, minMspt: Double = 0.0): List<ChunkProcessingProfiler.ChunkStatSnapshot> {
        return chunkProcessingProfiler.topChunksByMspt(limit, minMspt)
    }

    fun topChunkStatsByEwmaMspt(limit: Int, minMspt: Double = 0.0): List<ChunkProcessingProfiler.ChunkStatSnapshot> {
        return chunkProcessingProfiler.topChunksByEwmaMspt(limit, minMspt)
    }

    fun consumeDirtyChunkStats(): Set<ChunkPos> {
        return chunkProcessingProfiler.consumeDirtyChunks()
    }

    fun requestDroppedItemUnstuckAtBlock(x: Int, y: Int, z: Int) {
        droppedItemSystem.requestUnstuckForBlock(x, y, z)
    }

    fun hasDroppedItems(): Boolean {
        return droppedItemSystem.hasEntities()
    }

    fun spawnThrownItem(
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
        val chunkPos = ChunkPos(kotlin.math.floor(x / 16.0).toInt(), kotlin.math.floor(z / 16.0).toInt())
        return runOnThrownChunk(
            chunkPos = chunkPos,
            task = {
                val spawned = thrownItemSystem.spawn(
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
                thrownItemSystem.flushPendingOps()
                spawned
            },
            enqueuedFromOtherChunkActor = { false }
        )
    }

    fun removeThrownItem(entityId: Int, hit: Boolean, x: Double? = null, y: Double? = null, z: Double? = null): ThrownItemRemovedEvent? {
        val chunkPos = thrownItemSystem.snapshot(entityId)?.chunkPos ?: return null
        return runOnThrownChunk(
            chunkPos = chunkPos,
            task = {
                val removed = thrownItemSystem.remove(entityId, hit, x, y, z)
                thrownItemSystem.flushPendingOps()
                removed
            },
            enqueuedFromOtherChunkActor = { null }
        )
    }

    fun hasThrownItems(): Boolean {
        return thrownItemSystem.hasEntities()
    }

    fun thrownItemSnapshot(entityId: Int): ThrownItemSnapshot? {
        return thrownItemSystem.snapshot(entityId)
    }

    fun thrownItemSnapshots(): List<ThrownItemSnapshot> {
        return thrownItemSystem.snapshots()
    }

    fun thrownItemSnapshotByUuid(uuid: UUID): ThrownItemSnapshot? {
        return thrownItemSystem.snapshotByUuid(uuid)
    }

    fun setThrownItemPosition(entityId: Int, x: Double, y: Double, z: Double): ThrownItemSnapshot? {
        val chunkPos = thrownItemSystem.snapshot(entityId)?.chunkPos ?: return null
        return runOnThrownChunk(
            chunkPos = chunkPos,
            task = {
                thrownItemSystem.updatePosition(entityId, x, y, z)
                thrownItemSystem.flushPendingOps()
                thrownItemSystem.snapshot(entityId)
            },
            enqueuedFromOtherChunkActor = { null }
        )
    }

    fun setThrownItemVelocity(entityId: Int, vx: Double, vy: Double, vz: Double): ThrownItemSnapshot? {
        val chunkPos = thrownItemSystem.snapshot(entityId)?.chunkPos ?: return null
        return runOnThrownChunk(
            chunkPos = chunkPos,
            task = {
                thrownItemSystem.updateVelocity(entityId, vx, vy, vz)
                thrownItemSystem.flushPendingOps()
                thrownItemSystem.snapshot(entityId)
            },
            enqueuedFromOtherChunkActor = { null }
        )
    }

    fun setThrownItemPreviousPosition(entityId: Int, prevX: Double, prevY: Double, prevZ: Double): ThrownItemSnapshot? {
        val chunkPos = thrownItemSystem.snapshot(entityId)?.chunkPos ?: return null
        return runOnThrownChunk(
            chunkPos = chunkPos,
            task = {
                thrownItemSystem.updatePreviousPosition(entityId, prevX, prevY, prevZ)
                thrownItemSystem.flushPendingOps()
                thrownItemSystem.snapshot(entityId)
            },
            enqueuedFromOtherChunkActor = { null }
        )
    }

    fun setThrownItemAcceleration(entityId: Int, ax: Double, ay: Double, az: Double): ThrownItemSnapshot? {
        val chunkPos = thrownItemSystem.snapshot(entityId)?.chunkPos ?: return null
        return runOnThrownChunk(
            chunkPos = chunkPos,
            task = {
                thrownItemSystem.updateAcceleration(entityId, ax, ay, az)
                thrownItemSystem.flushPendingOps()
                thrownItemSystem.snapshot(entityId)
            },
            enqueuedFromOtherChunkActor = { null }
        )
    }

    fun setThrownItemOwnerEntityId(entityId: Int, ownerEntityId: Int): ThrownItemSnapshot? {
        val chunkPos = thrownItemSystem.snapshot(entityId)?.chunkPos ?: return null
        return runOnThrownChunk(
            chunkPos = chunkPos,
            task = {
                thrownItemSystem.updateOwnerEntityId(entityId, ownerEntityId)
                thrownItemSystem.flushPendingOps()
                thrownItemSystem.snapshot(entityId)
            },
            enqueuedFromOtherChunkActor = { null }
        )
    }

    fun setThrownItemKind(entityId: Int, kind: ThrownItemKind): ThrownItemSnapshot? {
        val chunkPos = thrownItemSystem.snapshot(entityId)?.chunkPos ?: return null
        return runOnThrownChunk(
            chunkPos = chunkPos,
            task = {
                thrownItemSystem.updateKind(entityId, kind)
                thrownItemSystem.flushPendingOps()
                thrownItemSystem.snapshot(entityId)
            },
            enqueuedFromOtherChunkActor = { null }
        )
    }

    fun setThrownItemOnGround(entityId: Int, onGround: Boolean): ThrownItemSnapshot? {
        val chunkPos = thrownItemSystem.snapshot(entityId)?.chunkPos ?: return null
        return runOnThrownChunk(
            chunkPos = chunkPos,
            task = {
                thrownItemSystem.updateOnGround(entityId, onGround)
                thrownItemSystem.flushPendingOps()
                thrownItemSystem.snapshot(entityId)
            },
            enqueuedFromOtherChunkActor = { null }
        )
    }

    fun thrownItemsInChunk(chunkX: Int, chunkZ: Int): List<ThrownItemSnapshot> {
        return thrownItemSystem.snapshotsInChunk(chunkX, chunkZ)
    }

    fun thrownItemEntityIdsInChunk(chunkX: Int, chunkZ: Int): IntArray {
        return thrownItemSystem.entityIdsInChunk(chunkX, chunkZ)
    }

    fun tickThrownItems(
        deltaSeconds: Double,
        activeSimulationChunks: Set<ChunkPos>? = null,
        chunkTimeRecorder: ((ChunkPos, Long) -> Unit)? = null
    ): ThrownItemTickEvents {
        if (!thrownItemSystem.hasEntities()) {
            thrownItemLastTickNanos.clear()
            return thrownItemSystem.tick(
                deltaSeconds = deltaSeconds,
                activeSimulationChunks = activeSimulationChunks,
                chunkDeltaSecondsProvider = { chunkPos ->
                    consumeChunkElapsedDeltaSeconds(
                        chunkPos = chunkPos,
                        fallbackSeconds = deltaSeconds,
                        lastTickMap = thrownItemLastTickNanos
                    )
                },
                chunkTimeRecorder = chunkTimeRecorder
            )
        }
        if (activeSimulationChunks != null) {
            val activeEntityChunks = HashSet<ChunkPos>()
            for (chunkPos in activeSimulationChunks) {
                if (thrownItemSystem.entityIdsInChunk(chunkPos.x, chunkPos.z).isNotEmpty()) {
                    activeEntityChunks.add(chunkPos)
                }
            }
            pruneIdleChunkTimingState(thrownItemLastTickNanos, activeEntityChunks)
        } else if (!thrownItemSystem.hasEntities()) {
            thrownItemLastTickNanos.clear()
        }
        return thrownItemSystem.tick(
            deltaSeconds = deltaSeconds,
            activeSimulationChunks = activeSimulationChunks,
            chunkDeltaSecondsProvider = { chunkPos ->
                consumeChunkElapsedDeltaSeconds(
                    chunkPos = chunkPos,
                    fallbackSeconds = deltaSeconds,
                    lastTickMap = thrownItemLastTickNanos
                )
            },
            chunkTimeRecorder = chunkTimeRecorder
        )
    }

    fun spawnFallingBlock(
        entityId: Int,
        blockStateId: Int,
        x: Double,
        y: Double,
        z: Double,
        vx: Double = 0.0,
        vy: Double = 0.0,
        vz: Double = 0.0
    ): Boolean {
        val chunkPos = ChunkPos(kotlin.math.floor(x / 16.0).toInt(), kotlin.math.floor(z / 16.0).toInt())
        return runOnFallingChunk(
            chunkPos = chunkPos,
            task = {
                val spawned = fallingBlockSystem.spawn(
                    entityId = entityId,
                    blockStateId = blockStateId,
                    x = x,
                    y = y,
                    z = z,
                    vx = vx,
                    vy = vy,
                    vz = vz
                )
                fallingBlockSystem.flushPendingSpawns()
                spawned
            },
            enqueuedFromOtherChunkActor = { false }
        )
    }

    fun hasFallingBlocks(): Boolean {
        return fallingBlockSystem.hasEntities()
    }

    fun fallingBlockSnapshot(entityId: Int): FallingBlockSnapshot? {
        return fallingBlockSystem.snapshot(entityId)
    }

    fun fallingBlocksInChunk(chunkX: Int, chunkZ: Int): List<FallingBlockSnapshot> {
        return fallingBlockSystem.snapshotsInChunk(chunkX, chunkZ)
    }

    fun fallingBlockEntityIdsInChunk(chunkX: Int, chunkZ: Int): IntArray {
        return fallingBlockSystem.entityIdsInChunk(chunkX, chunkZ)
    }

    fun tickFallingBlocks(
        deltaSeconds: Double,
        activeSimulationChunks: Set<ChunkPos>? = null,
        chunkTimeRecorder: ((ChunkPos, Long) -> Unit)? = null
    ): FallingBlockTickEvents {
        if (!fallingBlockSystem.hasEntities()) {
            fallingBlockLastTickNanos.clear()
            return fallingBlockSystem.tick(
                deltaSeconds = deltaSeconds,
                activeSimulationChunks = activeSimulationChunks,
                chunkDeltaSecondsProvider = { chunkPos ->
                    consumeChunkElapsedDeltaSeconds(
                        chunkPos = chunkPos,
                        fallbackSeconds = deltaSeconds,
                        lastTickMap = fallingBlockLastTickNanos
                    )
                },
                chunkTimeRecorder = chunkTimeRecorder
            )
        }
        if (activeSimulationChunks != null) {
            val activeEntityChunks = HashSet<ChunkPos>()
            for (chunkPos in activeSimulationChunks) {
                if (fallingBlockSystem.entityIdsInChunk(chunkPos.x, chunkPos.z).isNotEmpty()) {
                    activeEntityChunks.add(chunkPos)
                }
            }
            pruneIdleChunkTimingState(fallingBlockLastTickNanos, activeEntityChunks)
        } else if (!fallingBlockSystem.hasEntities()) {
            fallingBlockLastTickNanos.clear()
        }
        return fallingBlockSystem.tick(
            deltaSeconds = deltaSeconds,
            activeSimulationChunks = activeSimulationChunks,
            chunkDeltaSecondsProvider = { chunkPos ->
                consumeChunkElapsedDeltaSeconds(
                    chunkPos = chunkPos,
                    fallbackSeconds = deltaSeconds,
                    lastTickMap = fallingBlockLastTickNanos
                )
            },
            chunkTimeRecorder = chunkTimeRecorder
        )
    }

    fun tickDroppedItems(
        deltaSeconds: Double,
        activeSimulationChunks: Set<ChunkPos>? = null,
        onChunkEvents: ((DroppedItemTickEvents) -> Unit)? = null,
        onDispatchComplete: (() -> Unit)? = null,
        chunkTimeRecorder: ((ChunkPos, Long) -> Unit)? = null
    ): DroppedItemTickEvents {
        if (!droppedItemSystem.hasEntities()) {
            droppedItemLastTickNanos.clear()
            droppedItemChunkAccumulatorSeconds.clear()
            return droppedItemSystem.tickOnChunkActors(
                deltaSeconds = deltaSeconds,
                activeSimulationChunks = activeSimulationChunks,
                submitChunkTask = { chunkPos, task -> chunkActorScheduler.submit(chunkPos, task) },
                chunkDeltaSecondsProvider = { chunkPos ->
                    consumeChunkFixedStepDeltaSeconds(
                        chunkPos = chunkPos,
                        fallbackSeconds = deltaSeconds,
                        lastTickMap = droppedItemLastTickNanos,
                        accumulatorMap = droppedItemChunkAccumulatorSeconds,
                        fixedStepSeconds = DROPPED_ITEM_FIXED_STEP_SECONDS,
                        maxStepsPerTick = DROPPED_ITEM_MAX_STEPS_PER_TICK,
                        applyCatchupCap = false
                    )
                },
                onChunkEvents = onChunkEvents,
                onDispatchComplete = onDispatchComplete,
                chunkTimeRecorder = chunkTimeRecorder,
                awaitCompletion = false
            )
        }
        if (activeSimulationChunks != null) {
            val activeEntityChunks = HashSet<ChunkPos>()
            for (chunkPos in activeSimulationChunks) {
                if (droppedItemSystem.entityIdsInChunk(chunkPos.x, chunkPos.z).isNotEmpty()) {
                    activeEntityChunks.add(chunkPos)
                }
            }
            pruneIdleChunkTimingState(droppedItemLastTickNanos, activeEntityChunks)
            pruneIdleChunkAccumulatorState(droppedItemChunkAccumulatorSeconds, activeEntityChunks)
        } else if (!droppedItemSystem.hasEntities()) {
            droppedItemLastTickNanos.clear()
            droppedItemChunkAccumulatorSeconds.clear()
        }
        return droppedItemSystem.tickOnChunkActors(
            deltaSeconds = deltaSeconds,
            activeSimulationChunks = activeSimulationChunks,
            submitChunkTask = { chunkPos, task -> chunkActorScheduler.submit(chunkPos, task) },
            chunkDeltaSecondsProvider = { chunkPos ->
                consumeChunkFixedStepDeltaSeconds(
                    chunkPos = chunkPos,
                    fallbackSeconds = deltaSeconds,
                    lastTickMap = droppedItemLastTickNanos,
                    accumulatorMap = droppedItemChunkAccumulatorSeconds,
                    fixedStepSeconds = DROPPED_ITEM_FIXED_STEP_SECONDS,
                    maxStepsPerTick = DROPPED_ITEM_MAX_STEPS_PER_TICK,
                    applyCatchupCap = false
                )
            },
            onChunkEvents = onChunkEvents,
            onDispatchComplete = onDispatchComplete,
            chunkTimeRecorder = chunkTimeRecorder,
            awaitCompletion = false
        )
    }

    fun spawnAnimal(
        entityId: Int,
        kind: AnimalKind,
        x: Double,
        y: Double,
        z: Double,
        yaw: Float = 0f,
        pitch: Float = 0f
    ): Boolean {
        // Spawn is backed by concurrent collections in AnimalSystem and does not require a chunk-actor join.
        // Keeping this non-blocking avoids long stalls when callers spawn many entities in a burst.
        return animalSystem.spawn(entityId, kind, x, y, z, yaw, pitch)
    }

    fun hasAnimals(): Boolean {
        return animalSystem.hasAnimals()
    }

    fun animalSnapshot(entityId: Int): AnimalSnapshot? {
        return animalSystem.snapshot(entityId)
    }

    fun animalsInChunk(chunkX: Int, chunkZ: Int): List<AnimalSnapshot> {
        return animalSystem.snapshotsInChunk(chunkX, chunkZ)
    }

    fun animalEntityIdsInChunk(chunkX: Int, chunkZ: Int): IntArray {
        return animalSystem.entityIdsInChunk(chunkX, chunkZ)
    }

    fun animalSnapshots(): List<AnimalSnapshot> {
        return animalSystem.allSnapshots()
    }

    fun setAnimalTemptSources(sources: List<AnimalTemptSource>) {
        runOnGlobalAnimalActor {
            animalSystem.setTemptSources(sources)
        }
    }

    fun setAnimalLookSources(sources: List<AnimalLookSource>) {
        runOnGlobalAnimalActor {
            animalSystem.setLookSources(sources)
        }
    }

    fun setAnimalRideControls(controls: List<AnimalRideControl>) {
        runOnGlobalAnimalActor {
            animalSystem.setRideControls(controls)
        }
    }

    fun damageAnimal(entityId: Int, amount: Float, cause: AnimalDamageCause): AnimalDamageResult? {
        val chunkPos = animalSystem.snapshot(entityId)?.chunkPos ?: return null
        return runOnAnimalChunk(
            chunkPos = chunkPos,
            task = { animalSystem.damage(entityId, amount, cause) },
            enqueuedFromOtherChunkActor = { null }
        )
    }

    fun damageAnimalWithKnockback(
        entityId: Int,
        amount: Float,
        cause: AnimalDamageCause,
        attackerX: Double,
        attackerZ: Double,
        strength: Double,
        preGravityYCompensation: Double = 0.0
    ): AnimalDamageWithKnockbackResult? {
        val chunkPos = animalSystem.snapshot(entityId)?.chunkPos ?: return null
        return runOnAnimalChunk(
            chunkPos = chunkPos,
            task = {
                animalSystem.damageWithKnockback(
                    entityId = entityId,
                    amount = amount,
                    cause = cause,
                    attackerX = attackerX,
                    attackerZ = attackerZ,
                    strength = strength,
                    preGravityYCompensation = preGravityYCompensation
                )
            },
            enqueuedFromOtherChunkActor = { null }
        )
    }

    fun removeAnimal(entityId: Int, died: Boolean = false): AnimalRemovedEvent? {
        val chunkPos = animalSystem.snapshot(entityId)?.chunkPos ?: return null
        return runOnAnimalChunk(
            chunkPos = chunkPos,
            task = { animalSystem.remove(entityId, died) },
            enqueuedFromOtherChunkActor = { null }
        )
    }

    fun setAnimalVelocity(entityId: Int, vx: Double, vy: Double, vz: Double): Boolean {
        val chunkPos = animalSystem.snapshot(entityId)?.chunkPos ?: return false
        return runOnAnimalChunk(
            chunkPos = chunkPos,
            task = { animalSystem.setVelocity(entityId, vx, vy, vz) },
            enqueuedFromOtherChunkActor = { true }
        )
    }

    fun setAnimalPosition(entityId: Int, x: Double, y: Double, z: Double, yaw: Float? = null, pitch: Float? = null): Boolean {
        val chunkPos = animalSystem.snapshot(entityId)?.chunkPos ?: return false
        return runOnAnimalChunk(
            chunkPos = chunkPos,
            task = { animalSystem.setPosition(entityId, x, y, z, yaw, pitch) },
            enqueuedFromOtherChunkActor = { true }
        )
    }

    fun setAnimalPushable(entityId: Int, value: Boolean): Boolean {
        val chunkPos = animalSystem.snapshot(entityId)?.chunkPos ?: return false
        return runOnAnimalChunk(
            chunkPos = chunkPos,
            task = { animalSystem.setPushable(entityId, value) },
            enqueuedFromOtherChunkActor = { true }
        )
    }

    fun setAnimalCollision(entityId: Int, value: Boolean): Boolean {
        val chunkPos = animalSystem.snapshot(entityId)?.chunkPos ?: return false
        return runOnAnimalChunk(
            chunkPos = chunkPos,
            task = { animalSystem.setCollision(entityId, value) },
            enqueuedFromOtherChunkActor = { true }
        )
    }

    fun setAnimalGravity(entityId: Int, value: Boolean): Boolean {
        val chunkPos = animalSystem.snapshot(entityId)?.chunkPos ?: return false
        return runOnAnimalChunk(
            chunkPos = chunkPos,
            task = { animalSystem.setGravity(entityId, value) },
            enqueuedFromOtherChunkActor = { true }
        )
    }

    fun addAnimalHorizontalImpulse(entityId: Int, impulseX: Double, impulseZ: Double): Boolean {
        val chunkPos = animalSystem.snapshot(entityId)?.chunkPos ?: return false
        return runOnAnimalChunk(
            chunkPos = chunkPos,
            task = { animalSystem.addHorizontalImpulse(entityId, impulseX, impulseZ) },
            enqueuedFromOtherChunkActor = { true }
        )
    }

    fun tickAnimals(
        deltaSeconds: Double,
        activeSimulationChunks: Set<ChunkPos>? = null,
        onChunkEvents: ((AnimalTickEvents) -> Unit)? = null,
        onDispatchComplete: (() -> Unit)? = null,
        chunkTimeRecorder: ((ChunkPos, Long) -> Unit)? = null
    ): AnimalTickEvents {
        val spawned = animalSystem.drainSpawnedSnapshots()
        val readyUpdated = ArrayList<AnimalSnapshot>()
        val readyRemoved = ArrayList<AnimalRemovedEvent>()
        val readyDamaged = ArrayList<AnimalDamagedEvent>()
        val readyAmbient = ArrayList<AnimalAmbientEvent>()
        while (true) {
            val ready = pendingAnimalTickEvents.poll() ?: break
            if (ready.updated.isNotEmpty()) readyUpdated.addAll(ready.updated)
            if (ready.removed.isNotEmpty()) readyRemoved.addAll(ready.removed)
            if (ready.damaged.isNotEmpty()) readyDamaged.addAll(ready.damaged)
            if (ready.ambient.isNotEmpty()) readyAmbient.addAll(ready.ambient)
        }
        val timeScale = ServerConfig.timeScale
        if (timeScale <= 0.0 || !timeScale.isFinite()) {
            animalLastTickNanos.clear()
            onDispatchComplete?.invoke()
            return AnimalTickEvents(spawned, readyUpdated, readyRemoved, readyDamaged, readyAmbient)
        }
        if (!animalSystem.hasAnimals()) {
            animalLastTickNanos.clear()
            onDispatchComplete?.invoke()
            return AnimalTickEvents(spawned, readyUpdated, readyRemoved, readyDamaged, readyAmbient)
        }

        val byChunk = LinkedHashMap<ChunkPos, MutableList<BlockPos>>()
        val chunkCandidates: Collection<ChunkPos> = activeSimulationChunks ?: animalSystem.allSnapshots().map { it.chunkPos }.toSet()
        for (chunkPos in chunkCandidates) {
            val ids = animalSystem.entityIdsInChunk(chunkPos.x, chunkPos.z)
            if (ids.isEmpty()) continue
            val placeholders = ArrayList<BlockPos>(ids.size)
            for (id in ids) {
                placeholders.add(BlockPos(id, 0, 0))
            }
            byChunk[chunkPos] = placeholders
        }

        if (byChunk.isEmpty()) {
            animalLastTickNanos.clear()
            onDispatchComplete?.invoke()
            return AnimalTickEvents(spawned, readyUpdated, readyRemoved, readyDamaged, readyAmbient)
        }
        pruneIdleChunkTimingState(animalLastTickNanos, byChunk.keys)

        val pendingTasks = AtomicInteger(0)
        for (chunkPos in byChunk.keys) {
            if (!tryAcquireChunkTick(animalChunkTickInFlight, chunkPos)) {
                continue
            }
            pendingTasks.incrementAndGet()
            chunkActorScheduler.submit(chunkPos) {
                try {
                    val chunkDelta = consumeChunkElapsedDeltaSeconds(
                        chunkPos = chunkPos,
                        fallbackSeconds = deltaSeconds,
                        lastTickMap = animalLastTickNanos
                    )
                    if (chunkDelta <= 0.0) {
                        return@submit AnimalTickEvents(emptyList(), emptyList(), emptyList(), emptyList(), emptyList())
                    }
                    val events = animalSystem.tickChunk(
                        chunkPos = chunkPos,
                        deltaSeconds = chunkDelta,
                        chunkTimeRecorder = chunkTimeRecorder
                    )
                    if (events.updated.isNotEmpty() || events.removed.isNotEmpty() || events.damaged.isNotEmpty() || events.ambient.isNotEmpty()) {
                        if (onChunkEvents != null) {
                            onChunkEvents(
                                AnimalTickEvents(
                                    spawned = emptyList(),
                                    updated = events.updated,
                                    removed = events.removed,
                                    damaged = events.damaged,
                                    ambient = events.ambient
                                )
                            )
                        } else {
                            pendingAnimalTickEvents.add(
                                AnimalTickEvents(
                                    spawned = emptyList(),
                                    updated = events.updated,
                                    removed = events.removed,
                                    damaged = events.damaged,
                                    ambient = events.ambient
                                )
                            )
                        }
                    }
                    events
                } finally {
                    releaseChunkTick(animalChunkTickInFlight, chunkPos)
                    if (onDispatchComplete != null && pendingTasks.decrementAndGet() == 0) {
                        onDispatchComplete()
                    }
                }
            }
        }
        if (pendingTasks.get() == 0) {
            onDispatchComplete?.invoke()
        }
        return AnimalTickEvents(
            spawned = spawned,
            updated = readyUpdated,
            removed = readyRemoved,
            damaged = readyDamaged,
            ambient = readyAmbient
        )
    }

    fun tickFluids(
        deltaSeconds: Double,
        activeSimulationChunks: Set<ChunkPos>? = null,
        onChunkChanged: ((List<FluidBlockChange>) -> Unit)? = null,
        onDispatchComplete: (() -> Unit)? = null,
        chunkTimeRecorder: ((ChunkPos, Long) -> Unit)? = null
    ): FluidTickEvents {
        val ready = ArrayList<FluidBlockChange>()
        while (true) {
            val changed = pendingFluidTickChanges.poll() ?: break
            ready.add(changed)
        }
        if (deltaSeconds <= 0.0) {
            onDispatchComplete?.invoke()
            return FluidTickEvents(ready)
        }
        if (pendingFluidUpdates.isEmpty()) {
            fluidLastTickNanos.clear()
            fluidChunkAccumulatorSeconds.clear()
            onDispatchComplete?.invoke()
            return FluidTickEvents(ready)
        }
        val byChunk = LinkedHashMap<ChunkPos, MutableList<BlockPos>>()
        for (pos in pendingFluidUpdates) {
            val chunkPos = pos.chunkPos()
            if (activeSimulationChunks != null && chunkPos !in activeSimulationChunks) continue
            byChunk.computeIfAbsent(chunkPos) { ArrayList() }.add(pos)
        }
        if (byChunk.isEmpty()) {
            fluidLastTickNanos.clear()
            fluidChunkAccumulatorSeconds.clear()
            onDispatchComplete?.invoke()
            return FluidTickEvents(ready)
        }
        pruneIdleChunkTimingState(fluidLastTickNanos, byChunk.keys)
        pruneIdleChunkAccumulatorState(fluidChunkAccumulatorSeconds, byChunk.keys)
        val pendingTasks = AtomicInteger(0)
        for ((chunkPos, positions) in byChunk) {
            if (!tryAcquireChunkTick(fluidChunkTickInFlight, chunkPos)) {
                continue
            }
            pendingTasks.incrementAndGet()
            chunkActorScheduler.submit(chunkPos) {
                try {
                    val chunkDeltaSeconds = consumeChunkElapsedDeltaSeconds(
                        chunkPos = chunkPos,
                        fallbackSeconds = deltaSeconds,
                        lastTickMap = fluidLastTickNanos
                    )
                    if (chunkDeltaSeconds <= 0.0) {
                        return@submit emptyList<FluidBlockChange>()
                    }
                    val previousAccumulator = fluidChunkAccumulatorSeconds[chunkPos] ?: 0.0
                    val totalSeconds = (previousAccumulator + chunkDeltaSeconds).coerceAtLeast(0.0)
                    val steps = (totalSeconds / FLUID_TICK_INTERVAL_SECONDS).toInt()
                    fluidChunkAccumulatorSeconds[chunkPos] = totalSeconds - (steps * FLUID_TICK_INTERVAL_SECONDS)
                    if (steps <= 0) {
                        return@submit emptyList<FluidBlockChange>()
                    }

                    var currentPositions = claimPendingFluidUpdates(chunkPos, positions)
                    if (currentPositions.isEmpty()) {
                        return@submit emptyList<FluidBlockChange>()
                    }
                    val localChanged = ArrayList<FluidBlockChange>()
                    for (stepIndex in 0 until steps) {
                        if (currentPositions.isEmpty()) break
                        val localPlanned = planFluidChunkChanges(chunkPos, currentPositions, chunkTimeRecorder)
                        if (localPlanned.isNotEmpty()) {
                            applyPlannedFluidChanges(localPlanned, localChanged)
                        }
                        if (stepIndex < steps - 1) {
                            // Keep freshly enqueued updates in pending set for the next server tick.
                            currentPositions = drainPendingFluidUpdatesForChunk(chunkPos)
                        }
                    }
                    if (localChanged.isNotEmpty()) {
                        if (onChunkChanged != null) {
                            onChunkChanged(localChanged)
                        } else {
                            for (change in localChanged) {
                                pendingFluidTickChanges.add(change)
                            }
                        }
                    }
                    localChanged
                } finally {
                    releaseChunkTick(fluidChunkTickInFlight, chunkPos)
                    if (onDispatchComplete != null && pendingTasks.decrementAndGet() == 0) {
                        onDispatchComplete()
                    }
                }
            }
        }
        if (pendingTasks.get() == 0) {
            onDispatchComplete?.invoke()
        }
        return FluidTickEvents(ready)
    }

    fun tickGrass(
        deltaSeconds: Double,
        activeSimulationChunks: Set<ChunkPos>? = null,
        onChunkChanged: ((List<GrassBlockChange>) -> Unit)? = null,
        onDispatchComplete: (() -> Unit)? = null,
        chunkTimeRecorder: ((ChunkPos, Long) -> Unit)? = null
    ): GrassTickEvents {
        val ready = ArrayList<GrassBlockChange>()
        while (true) {
            val changed = pendingGrassTickChanges.poll() ?: break
            ready.add(changed)
        }
        if (deltaSeconds <= 0.0) {
            onDispatchComplete?.invoke()
            return GrassTickEvents(ready)
        }
        if (pendingGrassUpdatesByChunk.isEmpty()) {
            grassLastTickNanos.clear()
            onDispatchComplete?.invoke()
            return GrassTickEvents(ready)
        }

        val byChunk = LinkedHashMap<ChunkPos, MutableList<BlockPos>>()
        for ((chunkPos, positions) in pendingGrassUpdatesByChunk) {
            if (activeSimulationChunks != null && chunkPos !in activeSimulationChunks) continue
            if (positions.isEmpty()) continue
            byChunk.computeIfAbsent(chunkPos) { ArrayList() }.addAll(positions)
        }
        if (byChunk.isEmpty()) {
            grassLastTickNanos.clear()
            onDispatchComplete?.invoke()
            return GrassTickEvents(ready)
        }
        pruneIdleChunkTimingState(grassLastTickNanos, byChunk.keys)

        val pendingTasks = AtomicInteger(0)
        for ((chunkPos, positions) in byChunk) {
            if (!tryAcquireChunkTick(grassChunkTickInFlight, chunkPos)) {
                continue
            }
            pendingTasks.incrementAndGet()
            chunkActorScheduler.submit(chunkPos) {
                try {
                    val chunkDeltaSeconds = consumeChunkElapsedDeltaSeconds(
                        chunkPos = chunkPos,
                        fallbackSeconds = deltaSeconds,
                        lastTickMap = grassLastTickNanos
                    )
                    if (chunkDeltaSeconds <= 0.0) {
                        return@submit emptyList<GrassBlockChange>()
                    }
                    val currentPositions = claimPendingGrassUpdates(chunkPos, positions)
                    if (currentPositions.isEmpty()) {
                        return@submit emptyList<GrassBlockChange>()
                    }
                    val localChanged = ArrayList<GrassBlockChange>()
                    processGrassCandidatesChunk(
                        chunkPos = chunkPos,
                        positions = currentPositions,
                        chunkDeltaSeconds = chunkDeltaSeconds,
                        out = localChanged,
                        chunkTimeRecorder = chunkTimeRecorder
                    )
                    if (localChanged.isNotEmpty()) {
                        if (onChunkChanged != null) {
                            onChunkChanged(localChanged)
                        } else {
                            for (change in localChanged) {
                                pendingGrassTickChanges.add(change)
                            }
                        }
                    }
                    localChanged
                } finally {
                    releaseChunkTick(grassChunkTickInFlight, chunkPos)
                    if (onDispatchComplete != null && pendingTasks.decrementAndGet() == 0) {
                        onDispatchComplete()
                    }
                }
            }
        }
        if (pendingTasks.get() == 0) {
            onDispatchComplete?.invoke()
        }
        return GrassTickEvents(ready)
    }

    private fun consumeChunkElapsedDeltaSeconds(
        chunkPos: ChunkPos,
        fallbackSeconds: Double,
        lastTickMap: ConcurrentHashMap<ChunkPos, Long>,
        applyCatchupCap: Boolean = true
    ): Double {
        val timeScale = ServerConfig.timeScale
        if (!timeScale.isFinite() || timeScale <= 0.0) return 0.0
        val scaledFallbackSeconds = if (fallbackSeconds.isFinite() && fallbackSeconds > 0.0) {
            fallbackSeconds * timeScale
        } else {
            0.0
        }
        val nowNanos = System.nanoTime()
        val previous = lastTickMap.put(chunkPos, nowNanos)
        if (previous == null) {
            return scaledFallbackSeconds
        }
        val elapsedSeconds = ((nowNanos - previous).coerceAtLeast(0L)) / 1_000_000_000.0
        if (!elapsedSeconds.isFinite() || elapsedSeconds <= 0.0) return 0.0
        val scaledElapsedSeconds = elapsedSeconds * timeScale
        // If a chunk was dormant (e.g., simulation chunk off), avoid a single oversized catch-up tick.
        if (applyCatchupCap &&
            scaledFallbackSeconds > 0.0 &&
            scaledElapsedSeconds > scaledFallbackSeconds * MAX_CHUNK_CATCHUP_MULTIPLIER
        ) {
            return scaledFallbackSeconds
        }
        return scaledElapsedSeconds
    }

    private fun claimPendingGrassUpdates(chunkPos: ChunkPos, seedPositions: List<BlockPos>): List<BlockPos> {
        if (seedPositions.isEmpty()) return drainPendingGrassUpdatesForChunk(chunkPos)
        val bucket = pendingGrassUpdatesByChunk[chunkPos]
        val out = LinkedHashSet<BlockPos>(seedPositions.size)
        for (pos in seedPositions) {
            if (pos.chunkPos() != chunkPos) continue
            if (!pendingGrassUpdates.remove(pos)) continue
            bucket?.remove(pos)
            out.add(pos)
        }
        if (bucket != null && bucket.isEmpty()) {
            pendingGrassUpdatesByChunk.remove(chunkPos, bucket)
        }
        if (out.isNotEmpty()) return out.toList()
        return drainPendingGrassUpdatesForChunk(chunkPos)
    }

    private fun drainPendingGrassUpdatesForChunk(chunkPos: ChunkPos): List<BlockPos> {
        val bucket = pendingGrassUpdatesByChunk.remove(chunkPos) ?: return emptyList()
        if (bucket.isEmpty()) return emptyList()
        val out = ArrayList<BlockPos>(bucket.size)
        for (pos in bucket) {
            if (pendingGrassUpdates.remove(pos)) {
                out.add(pos)
            }
        }
        return out
    }

    private fun processGrassCandidatesChunk(
        chunkPos: ChunkPos,
        positions: List<BlockPos>,
        chunkDeltaSeconds: Double,
        out: MutableList<GrassBlockChange>,
        chunkTimeRecorder: ((ChunkPos, Long) -> Unit)?
    ) {
        val started = if (chunkTimeRecorder != null) System.nanoTime() else 0L
        var processed = 0
        try {
            for (pos in positions) {
                if (processed >= GRASS_MAX_CANDIDATES_PER_CHUNK_TICK) {
                    requeueGrassCandidate(pos)
                    continue
                }
                processed++
                if (pos.y !in WORLD_MIN_Y..WORLD_MAX_Y) continue
                val current = blockStateAt(pos.x, pos.y, pos.z)
                if (current != DIRT_BLOCK_STATE_ID && current != GRASS_BLOCK_STATE_ID) {
                    pendingGrassDelaySeconds.remove(pos)
                    continue
                }
                val remaining = pendingGrassDelaySeconds[pos] ?: continue
                val nextRemaining = remaining - chunkDeltaSeconds
                if (nextRemaining > 0.0) {
                    pendingGrassDelaySeconds[pos] = nextRemaining
                    requeueGrassCandidate(pos)
                    continue
                }
                pendingGrassDelaySeconds.remove(pos)
                if (current == GRASS_BLOCK_STATE_ID) {
                    when (canGrassStayAtCached(pos.x, pos.y, pos.z)) {
                        null -> {}
                        false -> {
                        setBlockStateWithoutFluidUpdates(pos.x, pos.y, pos.z, DIRT_BLOCK_STATE_ID)
                        out.add(
                            GrassBlockChange(
                                x = pos.x,
                                y = pos.y,
                                z = pos.z,
                                previousStateId = current,
                                stateId = DIRT_BLOCK_STATE_ID,
                                chunkPos = chunkPos
                            )
                        )
                        }
                        true -> {}
                    }
                    continue
                }
                if (current != DIRT_BLOCK_STATE_ID) continue
                val canSpread = canGrassSpreadToCached(pos.x, pos.y, pos.z)
                if (canSpread == null) continue
                if (!canSpread) continue
                setBlockStateWithoutFluidUpdates(pos.x, pos.y, pos.z, GRASS_BLOCK_STATE_ID)
                out.add(
                    GrassBlockChange(
                        x = pos.x,
                        y = pos.y,
                        z = pos.z,
                        previousStateId = current,
                        stateId = GRASS_BLOCK_STATE_ID,
                        chunkPos = chunkPos
                    )
                )
            }
        } finally {
            if (chunkTimeRecorder != null) {
                chunkTimeRecorder(chunkPos, System.nanoTime() - started)
            }
        }
    }

    private fun canGrassSpreadToCached(x: Int, y: Int, z: Int): Boolean? {
        val canStay = canGrassStayAtCached(x, y, z) ?: return null
        if (!canStay) return false
        if (y >= WORLD_MAX_Y) return false
        val brightness = rawBrightnessAtIfCached(x, y + 1, z) ?: return false
        if (brightness < 9) return false
        for ((dx, dz) in HORIZONTAL_NEIGHBOR_OFFSETS) {
            if (blockStateAtIfCached(x + dx, y, z + dz) != GRASS_BLOCK_STATE_ID) continue
            val neighborCanStay = canGrassStayAtCached(x + dx, y, z + dz) ?: continue
            if (neighborCanStay) {
                return true
            }
        }
        return false
    }

    private fun canGrassStayAtCached(x: Int, y: Int, z: Int): Boolean? {
        if (y >= WORLD_MAX_Y) return false
        val brightness = rawBrightnessAtIfCached(x, y + 1, z) ?: return null
        return brightness >= 4
    }

    private fun enqueueGrassUpdatesAround(x: Int, y: Int, z: Int) {
        enqueueGrassCandidate(x, y, z)
        if (y > WORLD_MIN_Y) {
            enqueueGrassCandidate(x, y - 1, z)
        }
        if (y < WORLD_MAX_Y) {
            enqueueGrassCandidate(x, y + 1, z)
        }
        for ((dx, dz) in HORIZONTAL_NEIGHBOR_OFFSETS) {
            enqueueGrassCandidate(x + dx, y, z + dz)
            if (y > WORLD_MIN_Y) {
                enqueueGrassCandidate(x + dx, y - 1, z + dz)
            }
        }
    }

    private fun enqueueGrassCandidate(x: Int, y: Int, z: Int) {
        val stateId = blockStateAt(x, y, z)
        enqueueGrassCandidateIfEligible(x, y, z, stateId)
    }

    private fun enqueueGrassCandidateIfEligible(x: Int, y: Int, z: Int, stateId: Int) {
        if (y !in WORLD_MIN_Y..WORLD_MAX_Y) return
        if (stateId != DIRT_BLOCK_STATE_ID && stateId != GRASS_BLOCK_STATE_ID) return
        val pos = BlockPos(x, y, z)
        pendingGrassDelaySeconds.computeIfAbsent(pos) {
            ThreadLocalRandom.current().nextDouble(GRASS_CANDIDATE_MIN_DELAY_SECONDS, GRASS_CANDIDATE_MAX_DELAY_SECONDS)
        }
        pendingGrassUpdates.add(pos)
        pendingGrassUpdatesByChunk
            .computeIfAbsent(pos.chunkPos()) { ConcurrentHashMap.newKeySet() }
            .add(pos)
    }

    private fun requeueGrassCandidate(pos: BlockPos) {
        pendingGrassUpdates.add(pos)
        pendingGrassUpdatesByChunk
            .computeIfAbsent(pos.chunkPos()) { ConcurrentHashMap.newKeySet() }
            .add(pos)
    }

    private fun consumeChunkFixedStepDeltaSeconds(
        chunkPos: ChunkPos,
        fallbackSeconds: Double,
        lastTickMap: ConcurrentHashMap<ChunkPos, Long>,
        accumulatorMap: ConcurrentHashMap<ChunkPos, Double>,
        fixedStepSeconds: Double,
        maxStepsPerTick: Int,
        applyCatchupCap: Boolean = true
    ): Double {
        if (!fixedStepSeconds.isFinite() || fixedStepSeconds <= 0.0) return 0.0
        val elapsedSeconds = consumeChunkElapsedDeltaSeconds(
            chunkPos = chunkPos,
            fallbackSeconds = fallbackSeconds,
            lastTickMap = lastTickMap,
            applyCatchupCap = applyCatchupCap
        )
        if (!elapsedSeconds.isFinite() || elapsedSeconds <= 0.0) return 0.0
        val previous = accumulatorMap[chunkPos] ?: 0.0
        val total = (previous + elapsedSeconds).coerceAtLeast(0.0)
        if (maxStepsPerTick <= 0) {
            accumulatorMap[chunkPos] = 0.0
            return total
        }
        val rawSteps = (total / fixedStepSeconds).toInt().coerceAtLeast(0)
        val steps = if (maxStepsPerTick > 0) rawSteps.coerceAtMost(maxStepsPerTick) else rawSteps
        if (steps <= 0) {
            accumulatorMap[chunkPos] = total
            return 0.0
        }
        val consumed = steps * fixedStepSeconds
        val remaining = (total - consumed).coerceAtLeast(0.0)
        accumulatorMap[chunkPos] = remaining
        return consumed
    }

    private fun tryAcquireChunkTick(
        inFlight: ConcurrentHashMap<ChunkPos, AtomicBoolean>,
        chunkPos: ChunkPos
    ): Boolean {
        return inFlight.computeIfAbsent(chunkPos) { AtomicBoolean(false) }.compareAndSet(false, true)
    }

    private fun releaseChunkTick(
        inFlight: ConcurrentHashMap<ChunkPos, AtomicBoolean>,
        chunkPos: ChunkPos
    ) {
        inFlight[chunkPos]?.set(false)
    }

    private fun pruneIdleChunkTimingState(
        lastTickMap: ConcurrentHashMap<ChunkPos, Long>,
        activeChunks: Collection<ChunkPos>
    ) {
        if (lastTickMap.isEmpty()) return
        if (activeChunks.isEmpty()) {
            lastTickMap.clear()
            return
        }
        val active = if (activeChunks is Set<ChunkPos>) activeChunks else HashSet(activeChunks)
        val iterator = lastTickMap.entries.iterator()
        while (iterator.hasNext()) {
            val entry = iterator.next()
            if (entry.key !in active) {
                iterator.remove()
            }
        }
    }

    private fun pruneIdleChunkAccumulatorState(
        accumulatorMap: ConcurrentHashMap<ChunkPos, Double>,
        activeChunks: Collection<ChunkPos>
    ) {
        if (accumulatorMap.isEmpty()) return
        if (activeChunks.isEmpty()) {
            accumulatorMap.clear()
            return
        }
        val active = if (activeChunks is Set<ChunkPos>) activeChunks else HashSet(activeChunks)
        val iterator = accumulatorMap.entries.iterator()
        while (iterator.hasNext()) {
            val entry = iterator.next()
            if (entry.key !in active) {
                iterator.remove()
            }
        }
    }

    fun <T> runOnFluidChunkWorkers(
        byChunk: Map<ChunkPos, Collection<BlockPos>>,
        task: (ChunkPos, List<BlockPos>) -> T
    ): List<T> {
        if (byChunk.isEmpty()) return emptyList()
        val out = ArrayList<T>(byChunk.size)
        for ((chunkPos, positions) in byChunk) {
            out.add(task(chunkPos, positions.toList()))
        }
        return out
    }

    fun <T> submitOnChunkActor(
        chunkPos: ChunkPos,
        task: () -> T
    ): CompletableFuture<T> {
        return chunkActorScheduler.submit(chunkPos, task)
    }

    private fun currentChunkActorThreadChunkPos(): ChunkPos? = chunkActorScheduler.currentChunkPos()

    fun chunkActorVirtualThread(chunkX: Int, chunkZ: Int): Thread? {
        return chunkActorScheduler.actorThread(ChunkPos(chunkX, chunkZ))
    }

    fun currentChunkActorVirtualThread(): Thread? {
        return chunkActorScheduler.currentChunkVirtualThread()
    }

    private fun <T> runOnAnimalChunk(
        chunkPos: ChunkPos,
        task: () -> T,
        enqueuedFromOtherChunkActor: () -> T
    ): T {
        val currentActorChunk = currentChunkActorThreadChunkPos()
        if (currentActorChunk == chunkPos) {
            return task()
        }
        if (currentActorChunk != null) {
            // Never block one chunk actor on another chunk actor.
            chunkActorScheduler.submit(chunkPos, task)
            return enqueuedFromOtherChunkActor()
        }
        return chunkActorScheduler.submit(chunkPos, task).join()
    }

    private fun runOnGlobalAnimalActor(task: () -> Unit) {
        val anchor = ChunkPos(0, 0)
        val currentActorChunk = currentChunkActorThreadChunkPos()
        if (currentActorChunk == anchor) {
            task()
            return
        }
        if (currentActorChunk != null) {
            chunkActorScheduler.submit(anchor, task)
            return
        }
        chunkActorScheduler.submit(anchor, task).join()
    }

    private fun <T> runOnDroppedChunk(
        chunkPos: ChunkPos,
        task: () -> T,
        enqueuedFromOtherChunkActor: () -> T
    ): T {
        val currentActorChunk = currentChunkActorThreadChunkPos()
        if (currentActorChunk == chunkPos) {
            return task()
        }
        if (currentActorChunk != null) {
            chunkActorScheduler.submit(chunkPos, task)
            return enqueuedFromOtherChunkActor()
        }
        return chunkActorScheduler.submit(chunkPos, task).join()
    }

    private fun <T> runOnThrownChunk(
        chunkPos: ChunkPos,
        task: () -> T,
        enqueuedFromOtherChunkActor: () -> T
    ): T {
        val currentActorChunk = currentChunkActorThreadChunkPos()
        if (currentActorChunk == chunkPos) {
            return task()
        }
        if (currentActorChunk != null) {
            chunkActorScheduler.submit(chunkPos, task)
            return enqueuedFromOtherChunkActor()
        }
        return chunkActorScheduler.submit(chunkPos, task).join()
    }

    private fun <T> runOnFallingChunk(
        chunkPos: ChunkPos,
        task: () -> T,
        enqueuedFromOtherChunkActor: () -> T
    ): T {
        val currentActorChunk = currentChunkActorThreadChunkPos()
        if (currentActorChunk == chunkPos) {
            return task()
        }
        if (currentActorChunk != null) {
            chunkActorScheduler.submit(chunkPos, task)
            return enqueuedFromOtherChunkActor()
        }
        return chunkActorScheduler.submit(chunkPos, task).join()
    }

    fun resetFluidTickAccumulator() {
        fluidChunkAccumulatorSeconds.clear()
        fluidLastTickNanos.clear()
    }

    private fun planFluidChunkChanges(
        chunkPos: ChunkPos,
        positions: List<BlockPos>,
        chunkTimeRecorder: ((ChunkPos, Long) -> Unit)?
    ): List<PlannedFluidChange> {
        val startedAt = System.nanoTime()
        val planned = ArrayList<PlannedFluidChange>(positions.size)
        val preferredMaskCache = HashMap<Long, Int>(positions.size * 2)
        val dropDistanceCache = HashMap<Long, Int>(positions.size * 4)

        // Phase 1: compute all next states against the same world snapshot.
        for (pos in positions) {
            if (pos.y !in WORLD_MIN_Y..WORLD_MAX_Y) continue
            val currentState = blockStateAt(pos.x, pos.y, pos.z)
            val nextState = desiredFluidStateAt(
                x = pos.x,
                y = pos.y,
                z = pos.z,
                currentStateId = currentState,
                preferredMaskCache = preferredMaskCache,
                dropDistanceCache = dropDistanceCache
            ) ?: continue
            if (nextState == currentState) continue
            planned.add(PlannedFluidChange(pos, currentState, nextState))
        }

        chunkTimeRecorder?.invoke(chunkPos, System.nanoTime() - startedAt)
        return planned
    }

    private fun applyPlannedFluidChanges(
        planned: List<PlannedFluidChange>,
        changedOut: MutableList<FluidBlockChange>
    ) {
        for (change in planned) {
            // Skip stale plans when another thread (e.g. interaction thread) already changed this block.
            if (blockStateAt(change.pos.x, change.pos.y, change.pos.z) != change.previousStateId) {
                continue
            }
            val removedWater = waterLevel(change.previousStateId) != null && change.nextStateId == AIR_STATE_ID
            // We manually enqueue neighbors below; avoid duplicate enqueue work.
            setBlockStateWithoutFluidUpdates(change.pos.x, change.pos.y, change.pos.z, change.nextStateId)
            if (change.nextStateId == AIR_STATE_ID) {
                setBlockEntity(change.pos.x, change.pos.y, change.pos.z, null)
            }
            changedOut.add(
                FluidBlockChange(
                    x = change.pos.x,
                    y = change.pos.y,
                    z = change.pos.z,
                    previousStateId = change.previousStateId,
                    stateId = change.nextStateId,
                    chunkPos = change.pos.chunkPos()
                )
            )
            enqueueFluidUpdatesAround(change.pos.x, change.pos.y, change.pos.z)
            if (removedWater) {
                enqueueFluidColumnBelow(change.pos.x, change.pos.y - 1, change.pos.z)
            }
        }
    }

    private fun enqueueFluidColumnBelow(x: Int, startY: Int, z: Int) {
        var y = startY
        while (y >= WORLD_MIN_Y) {
            val stateId = blockStateAt(x, y, z)
            if (waterLevel(stateId) == null) break
            pendingFluidUpdates.add(BlockPos(x, y, z))
            y--
        }
    }

    private fun desiredFluidStateAt(
        x: Int,
        y: Int,
        z: Int,
        currentStateId: Int,
        preferredMaskCache: MutableMap<Long, Int>,
        dropDistanceCache: MutableMap<Long, Int>
    ): Int? {
        val currentLevel = waterLevel(currentStateId)
        val currentIsWater = currentLevel != null
        if (!currentIsWater && !isFluidReplaceable(currentStateId)) return null
        val belowStateId = if (y > WORLD_MIN_Y) blockStateAt(x, y - 1, z) else AIR_STATE_ID
        val belowLevel = if (y > WORLD_MIN_Y) waterLevel(belowStateId) else null
        val aboveLevel = if (y < WORLD_MAX_Y) waterLevel(blockStateAt(x, y + 1, z)) else null
        val belowReplaceable = y > WORLD_MIN_Y && isFluidReplaceable(belowStateId)

        var maxNeighborAmount = 0
        var sourceNeighbors = 0
        for ((dx, dz) in HORIZONTAL_DIRS) {
            val nx = x + dx
            val nz = z + dz
            val neighborLevel = waterLevel(blockStateAt(nx, y, nz)) ?: continue
            if (neighborLevel == 0) sourceNeighbors++
            val parentLevel = horizontalParentLevelAt(nx, y, nz) ?: continue
            val amount = fluidAmountFromLevel(parentLevel)
            if (amount > maxNeighborAmount) maxNeighborAmount = amount
        }

        val supportsSourceCreation = !belowReplaceable || belowLevel == 0
        val canCreateSource = sourceNeighbors >= 2 && supportsSourceCreation
        val fromAbove = aboveLevel != null
        val computedLevel: Int? = when {
            currentIsWater && currentLevel == 0 -> 0
            canCreateSource -> 0
            fromAbove -> if (belowReplaceable) FALLING_WATER_VISUAL_LEVEL else 1
            else -> {
                val amount = maxNeighborAmount - 1 // water drop-off = 1
                if (amount <= 0) {
                    if (currentIsWater) FLUID_REMOVE_LEVEL else null
                } else {
                    flowingLevelFromAmount(amount)
                }
            }
        }

        if (computedLevel == null) return null
        if (computedLevel == FLUID_REMOVE_LEVEL) return AIR_STATE_ID
        if (!currentIsWater && !fromAbove && computedLevel in 1..7) {
            if (!isPreferredHorizontalFlowTarget(x, y, z, preferredMaskCache, dropDistanceCache)) return null
        }

        val waterStateId = WATER_STATE_IDS_BY_LEVEL.getOrNull(computedLevel) ?: return null
        return waterStateId
    }

    private fun isPreferredHorizontalFlowTarget(
        x: Int,
        y: Int,
        z: Int,
        preferredMaskCache: MutableMap<Long, Int>,
        dropDistanceCache: MutableMap<Long, Int>
    ): Boolean {
        var sawParent = false
        for (index in HORIZONTAL_DIRS.indices) {
            val (dx, dz) = HORIZONTAL_DIRS[index]
            val parentX = x - dx
            val parentZ = z - dz
            val parentLevel = horizontalParentLevelAt(parentX, y, parentZ) ?: continue
            sawParent = true
            val parentKey = packFluidPos(parentX, y, parentZ)
            val preferredMask = preferredMaskCache.computeIfAbsent(parentKey) {
                preferredHorizontalDirectionMask(parentX, y, parentZ, dropDistanceCache)
            }
            if ((preferredMask and (1 shl index)) != 0) return true
        }
        return !sawParent
    }

    private fun preferredHorizontalDirectionMask(
        x: Int,
        y: Int,
        z: Int,
        dropDistanceCache: MutableMap<Long, Int>
    ): Int {
        var minDistance = Int.MAX_VALUE
        var mask = 0
        for (index in HORIZONTAL_DIRS.indices) {
            val (dx, dz) = HORIZONTAL_DIRS[index]
            val targetX = x + dx
            val targetZ = z + dz
            if (!canFlowHorizontallyInto(targetX, y, targetZ)) continue
            val targetKey = packFluidPos(targetX, y, targetZ)
            val distance = dropDistanceCache.computeIfAbsent(targetKey) {
                horizontalDropDistance(targetX, y, targetZ, FLUID_SLOPE_SEARCH_MAX)
            }
            if (distance < minDistance) {
                minDistance = distance
                mask = 1 shl index
            } else if (distance == minDistance) {
                mask = mask or (1 shl index)
            }
        }
        return mask
    }

    private fun horizontalDropDistance(startX: Int, y: Int, startZ: Int, maxDistance: Int): Int {
        if (y <= WORLD_MIN_Y) return Int.MAX_VALUE
        if (isFluidReplaceable(blockStateAt(startX, y - 1, startZ))) return 0
        if (maxDistance <= 0) return Int.MAX_VALUE

        data class Node(val x: Int, val z: Int, val distance: Int)
        val queue = ArrayDeque<Node>()
        val visited = HashSet<Long>()
        queue.addLast(Node(startX, startZ, 0))
        visited.add(packFluidPos(startX, y, startZ))

        while (queue.isNotEmpty()) {
            val node = queue.removeFirst()
            if (node.distance >= maxDistance) continue
            for ((dx, dz) in HORIZONTAL_DIRS) {
                val nx = node.x + dx
                val nz = node.z + dz
                val candidateKey = packFluidPos(nx, y, nz)
                if (!visited.add(candidateKey)) continue
                if (!canFlowHorizontallyInto(nx, y, nz)) continue
                val nextDistance = node.distance + 1
                if (isFluidReplaceable(blockStateAt(nx, y - 1, nz))) return nextDistance
                queue.addLast(Node(nx, nz, nextDistance))
            }
        }
        return Int.MAX_VALUE
    }

    private fun canFlowHorizontallyInto(x: Int, y: Int, z: Int): Boolean {
        val stateId = blockStateAt(x, y, z)
        if (isFluidReplaceable(stateId)) return true
        return waterLevel(stateId) != null
    }

    private fun horizontalParentLevelAt(x: Int, y: Int, z: Int): Int? {
        if (isWaterHoleLikeAt(x, y, z)) return null
        val raw = waterLevel(blockStateAt(x, y, z)) ?: return null
        if (raw > 7) {
            return 0 // falling column behaves like amount 8 in vanilla spreadToSides.
        }
        return raw
    }

    private fun isWaterHoleLikeAt(x: Int, y: Int, z: Int): Boolean {
        if (y <= WORLD_MIN_Y) return false
        val fluidLevel = waterLevel(blockStateAt(x, y, z)) ?: return false
        if (fluidLevel == 0) return false
        val belowState = blockStateAt(x, y - 1, z)
        // Vanilla spread(): non-source fluid does not spread sideways when below is same fluid
        // or another fluid-holdable hole.
        if (waterLevel(belowState) != null) return true
        return isFluidReplaceable(belowState)
    }

    private fun fluidAmountFromLevel(level: Int): Int {
        return when {
            level <= 0 -> 8
            level in 1..7 -> 8 - level
            else -> 8 // falling column
        }
    }

    private fun flowingLevelFromAmount(amount: Int): Int {
        val clamped = amount.coerceIn(1, 7)
        return 8 - clamped
    }

    private fun waterLevel(stateId: Int): Int? {
        if (stateId <= 0) return null
        val table = waterLevelByStateIdTable
        if (stateId >= table.size) return null
        val encoded = table[stateId]
        return if (encoded == NO_WATER_LEVEL) null else encoded
    }

    private fun isFluidReplaceable(stateId: Int): Boolean {
        if (stateId == AIR_STATE_ID) return true
        val table = fluidReplaceableByStateIdTable
        if (stateId >= table.size) return false
        return table[stateId]
    }

    private fun enqueueFluidUpdatesAround(x: Int, y: Int, z: Int) {
        if (y !in WORLD_MIN_Y..WORLD_MAX_Y) return
        pendingFluidUpdates.add(BlockPos(x, y, z))
        if (y > WORLD_MIN_Y) pendingFluidUpdates.add(BlockPos(x, y - 1, z))
        if (y < WORLD_MAX_Y) pendingFluidUpdates.add(BlockPos(x, y + 1, z))
        pendingFluidUpdates.add(BlockPos(x + 1, y, z))
        pendingFluidUpdates.add(BlockPos(x - 1, y, z))
        pendingFluidUpdates.add(BlockPos(x, y, z + 1))
        pendingFluidUpdates.add(BlockPos(x, y, z - 1))
        // Scheduler parity for cell-based simulation:
        // if center changes, vertical neighbors may now spread sideways.
        if (y < WORLD_MAX_Y) {
            val ay = y + 1
            pendingFluidUpdates.add(BlockPos(x + 1, ay, z))
            pendingFluidUpdates.add(BlockPos(x - 1, ay, z))
            pendingFluidUpdates.add(BlockPos(x, ay, z + 1))
            pendingFluidUpdates.add(BlockPos(x, ay, z - 1))
        }
        if (y > WORLD_MIN_Y) {
            val by = y - 1
            pendingFluidUpdates.add(BlockPos(x + 1, by, z))
            pendingFluidUpdates.add(BlockPos(x - 1, by, z))
            pendingFluidUpdates.add(BlockPos(x, by, z + 1))
            pendingFluidUpdates.add(BlockPos(x, by, z - 1))
        }
    }

    private fun drainPendingFluidUpdates(): List<BlockPos> {
        if (pendingFluidUpdates.isEmpty()) return emptyList()
        val out = ArrayList<BlockPos>(pendingFluidUpdates.size)
        val iterator = pendingFluidUpdates.iterator()
        while (iterator.hasNext()) {
            val pos = iterator.next()
            if (pendingFluidUpdates.remove(pos)) {
                out.add(pos)
            }
        }
        return out
    }

    private fun claimPendingFluidUpdates(chunkPos: ChunkPos, preferred: List<BlockPos>): List<BlockPos> {
        val out = ArrayList<BlockPos>(preferred.size)
        for (pos in preferred) {
            if (pos.chunkPos() != chunkPos) continue
            if (pendingFluidUpdates.remove(pos)) {
                out.add(pos)
            }
        }
        if (out.isNotEmpty()) return out
        return drainPendingFluidUpdatesForChunk(chunkPos)
    }

    private fun drainPendingFluidUpdatesForChunk(chunkPos: ChunkPos): List<BlockPos> {
        if (pendingFluidUpdates.isEmpty()) return emptyList()
        val out = ArrayList<BlockPos>()
        val iterator = pendingFluidUpdates.iterator()
        while (iterator.hasNext()) {
            val pos = iterator.next()
            if (pos.chunkPos() != chunkPos) continue
            if (pendingFluidUpdates.remove(pos)) {
                out.add(pos)
            }
        }
        return out
    }

    fun spawnPointForPlayer(_playerUuid: UUID): SpawnPoint {
        (generator as? BlockStateLookupWorldGenerator)?.spawnPoint(key)?.let { generatorSpawn ->
            val rawForSave = SpawnPoint(
                x = generatorSpawn.x,
                y = generatorSpawn.y,
                z = generatorSpawn.z
            )
            val centered = SpawnPoint(
                x = generatorSpawn.x + 0.5,
                y = generatorSpawn.y,
                z = generatorSpawn.z + 0.5
            )
            cachedSpawnPointRef.compareAndSet(null, centered)
            val resolved = cachedSpawnPointRef.get() ?: centered
            if (key == "minecraft:overworld") {
                VanillaLevelDatSeedStore.saveSpawnPoint(rawForSave)
            }
            return resolved
        }
        cachedSpawnPointRef.get()?.let { return it }

        if (key == "minecraft:overworld") {
            VanillaLevelDatSeedStore.loadSpawnPoint()?.let { persisted ->
                cachedSpawnPointRef.compareAndSet(null, persisted)
                return cachedSpawnPointRef.get() ?: persisted
            }
        }

        val immediateFallback = SpawnPoint(0.5, DEFAULT_FALLBACK_SPAWN_Y, 0.5)
        if (spawnResolveInFlight.compareAndSet(false, true)) {
            Thread.ofVirtual()
                .name("aerogel-spawn-resolve-$key")
                .start {
                    runCatching { findSharedSpawnNearOrigin() }
                        .onSuccess { computed ->
                            cachedSpawnPointRef.compareAndSet(null, computed)
                            if (key == "minecraft:overworld") {
                                VanillaLevelDatSeedStore.saveSpawnPoint(computed)
                            }
                        }
                        .onFailure {
                            cachedSpawnPointRef.compareAndSet(null, immediateFallback)
                        }
                    spawnResolveInFlight.set(false)
                }
        }
        return cachedSpawnPointRef.get() ?: immediateFallback
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

    private fun isSpawnPointValid(spawn: SpawnPoint): Boolean {
        if (!spawn.x.isFinite() || !spawn.y.isFinite() || !spawn.z.isFinite()) return false
        val blockX = kotlin.math.floor(spawn.x).toInt()
        val feetY = spawn.y.toInt()
        val blockZ = kotlin.math.floor(spawn.z).toInt()
        return isSpawnClearAt(blockX, feetY, blockZ)
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

    companion object {
        private val directExecutor = Executor { runnable -> runnable.run() }
        private const val AIR_STATE_ID = 0
        private const val WORLD_MIN_Y = -64
        private const val WORLD_MAX_Y = 319
        private const val DEFAULT_FALLBACK_SPAWN_Y = 65.0
        private const val MOTION_BLOCKING_HEIGHTMAP_ID = 4
        private const val MOTION_BLOCKING_NO_LEAVES_HEIGHTMAP_ID = 5
        // Vanilla-like water spread cadence: 5 game ticks (0.25s at 20 TPS) per update step.
        private const val FLUID_TICK_INTERVAL_SECONDS = 0.25
        private const val DROPPED_ITEM_FIXED_STEP_SECONDS = 1.0 / 20.0
        private const val DROPPED_ITEM_MAX_STEPS_PER_TICK = 0
        private const val FLUID_SLOPE_SEARCH_MAX = 4
        private const val FALLING_WATER_VISUAL_LEVEL = 8
        private const val FLUID_REMOVE_LEVEL = -1
        private const val NO_WATER_LEVEL = -1
        private const val SPAWN_SEARCH_CHUNK_RADIUS = 8
        private const val MAX_CHUNK_CATCHUP_MULTIPLIER = 4.0
        private const val GRASS_CANDIDATE_MIN_DELAY_SECONDS = 8.0
        private const val GRASS_CANDIDATE_MAX_DELAY_SECONDS = 90.0
        private const val GRASS_MAX_CANDIDATES_PER_CHUNK_TICK = 2048
        private val DIRT_BLOCK_STATE_ID = BlockStateRegistry.defaultStateId("minecraft:dirt") ?: AIR_STATE_ID
        private val GRASS_BLOCK_STATE_ID = BlockStateRegistry.defaultStateId("minecraft:grass_block") ?: AIR_STATE_ID
        private val HORIZONTAL_DIRS = arrayOf(
            1 to 0,
            -1 to 0,
            0 to 1,
            0 to -1
        )
        private val HORIZONTAL_NEIGHBOR_OFFSETS = arrayOf(
            1 to 0,
            -1 to 0,
            0 to 1,
            0 to -1
        )
        private val WATER_STATE_IDS_BY_LEVEL = IntArray(16) { level ->
            BlockStateRegistry.stateId("minecraft:water", mapOf("level" to level.toString()))
                ?: BlockStateRegistry.defaultStateId("minecraft:water")
                ?: AIR_STATE_ID
        }
        private val json = Json { ignoreUnknownKeys = true }
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
        private val WATER_REPLACEABLE_BLOCK_KEYS = loadBlockTag("water_breakable")
        @Volatile private var waterLevelByStateIdTable: IntArray = intArrayOf(NO_WATER_LEVEL)
        @Volatile private var fluidReplaceableByStateIdTable: BooleanArray = booleanArrayOf(false)

        private fun packFluidPos(x: Int, y: Int, z: Int): Long {
            val xx = (x.toLong() and 0x3FFFFFFL) shl 38
            val zz = (z.toLong() and 0x3FFFFFFL) shl 12
            val yy = y.toLong() and 0xFFFL
            return xx or zz or yy
        }

        private fun loadBlockTag(tagName: String): Set<String> {
            return resolveBlockTag(tagName, HashSet())
        }

        private fun resolveBlockTag(tagName: String, visited: MutableSet<String>): Set<String> {
            if (!visited.add(tagName)) return emptySet()
            val resourcePath = "/data/minecraft/tags/block/$tagName.json"
            val stream = World::class.java.getResourceAsStream(resourcePath) ?: return emptySet()
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

        fun prewarm() {
            WATER_STATE_IDS_BY_LEVEL.size
            WATER_REPLACEABLE_BLOCK_KEYS.size
            val stateIds = BlockStateRegistry.allStateIds()
            var maxStateId = 0
            for (stateId in stateIds) {
                if (stateId > maxStateId) maxStateId = stateId
            }
            val levelTable = IntArray(maxStateId + 1) { NO_WATER_LEVEL }
            val replaceableTable = BooleanArray(maxStateId + 1)
            for (stateId in stateIds) {
                if (stateId <= 0) continue
                val parsed = BlockStateRegistry.parsedState(stateId) ?: continue
                if (parsed.blockKey == "minecraft:water") {
                    val raw = parsed.properties["level"]?.toIntOrNull() ?: 0
                    levelTable[stateId] = raw.coerceIn(0, 15)
                }
                replaceableTable[stateId] = parsed.blockKey in WATER_REPLACEABLE_BLOCK_KEYS
            }
            waterLevelByStateIdTable = levelTable
            fluidReplaceableByStateIdTable = replaceableTable
        }
    }
}
