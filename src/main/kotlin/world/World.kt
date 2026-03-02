package org.macaroon3145.world

import org.macaroon3145.config.ServerConfig
import org.macaroon3145.network.codec.BlockStateRegistry
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
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
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
    data class BlockEntityData(
        val typeId: Int,
        val nbtPayload: ByteArray
    )

    private val entityProcessors = CopyOnWriteArrayList<ChunkEntityProcessor>()
    private val changedBlockStates = ConcurrentHashMap<BlockPos, Int>()
    private val changedBlocksByChunk = ConcurrentHashMap<ChunkPos, MutableSet<BlockPos>>()
    private val changedBlockEntities = ConcurrentHashMap<BlockPos, BlockEntityData>()
    private val changedBlockEntitiesByChunk = ConcurrentHashMap<ChunkPos, MutableSet<BlockPos>>()
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
    private val pendingFluidUpdates = ConcurrentHashMap.newKeySet<BlockPos>()
    private val pendingFluidTickChanges = ConcurrentLinkedQueue<FluidBlockChange>()
    private val pendingAnimalTickEvents = ConcurrentLinkedQueue<AnimalTickEvents>()
    private val droppedItemLastTickNanos = ConcurrentHashMap<ChunkPos, Long>()
    private val animalLastTickNanos = ConcurrentHashMap<ChunkPos, Long>()
    private val thrownItemLastTickNanos = ConcurrentHashMap<ChunkPos, Long>()
    private val fallingBlockLastTickNanos = ConcurrentHashMap<ChunkPos, Long>()
    private val fluidLastTickNanos = ConcurrentHashMap<ChunkPos, Long>()
    private val fluidChunkAccumulatorSeconds = ConcurrentHashMap<ChunkPos, Double>()
    private val animalChunkTickInFlight = ConcurrentHashMap<ChunkPos, AtomicBoolean>()
    private val fluidChunkTickInFlight = ConcurrentHashMap<ChunkPos, AtomicBoolean>()
    private val chunkActorScheduler = ChunkActorScheduler()
    @Volatile private var warmedSeed: Long = Long.MIN_VALUE
    private val warmupFutureRef = AtomicReference<CompletableFuture<Long>?>(null)
    private val cachedSpawnPointRef = AtomicReference<SpawnPoint?>(null)

    private data class InFlightChunkBuild(
        val future: CompletableFuture<GeneratedChunk>,
        val activeWaiters: AtomicInteger = AtomicInteger(0),
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
        inFlightChunkBuilds[chunkPos]?.let { return it }

        val created = InFlightChunkBuild(CompletableFuture<GeneratedChunk>())
        val existing = inFlightChunkBuilds.putIfAbsent(chunkPos, created)
        if (existing != null) return existing

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
            inFlightChunkBuilds.remove(chunkPos, created)
            when (throwable) {
                is RuntimeException -> throw throwable
                is Error -> throw throwable
                else -> throw RuntimeException(throwable)
            }
        }

        result.whenComplete { generated, error ->
            if (error != null) {
                created.future.completeExceptionally(error)
            } else {
                created.future.complete(generated)
            }
            inFlightChunkBuilds.remove(chunkPos, created)
        }

        return created
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
            var completed = false
            var generated: GeneratedChunk? = null
            var failure: Throwable? = null
            future.whenComplete { value, error ->
                generated = value
                failure = error
                completed = true
            }
            if (!completed) {
                LockSupport.parkNanos(100_000L)
                continue
            }
            val error = failure
            if (error != null) {
                val cause = (error as? CompletionException)?.cause ?: error
                when (cause) {
                    is RuntimeException -> throw cause
                    is Error -> throw cause
                    else -> throw RuntimeException(cause)
                }
            }
            val chunk = generated
            if (chunk != null) {
                return chunk
            }
        }
    }

    private fun releaseChunkBuildWaiter(build: InFlightChunkBuild) {
        val remaining = build.activeWaiters.decrementAndGet()
        if (remaining <= 0 && !build.future.isDone) {
            build.cancelRequested.set(true)
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

    private fun setBlockStateInternal(x: Int, y: Int, z: Int, stateId: Int, enqueueFluidUpdates: Boolean) {
        val pos = BlockPos(x, y, z)
        val base = baseBlockStateProvider(x, y, z)
        if (stateId == base) {
            changedBlockStates.remove(pos)
            changedBlocksByChunk[pos.chunkPos()]?.remove(pos)
            changedBlockEntities.remove(pos)
            changedBlockEntitiesByChunk[pos.chunkPos()]?.remove(pos)
            if (enqueueFluidUpdates) {
                enqueueFluidUpdatesAround(x, y, z)
            }
            return
        }
        changedBlockStates[pos] = stateId
        changedBlocksByChunk
            .computeIfAbsent(pos.chunkPos()) { ConcurrentHashMap.newKeySet() }
            .add(pos)
        if (enqueueFluidUpdates) {
            enqueueFluidUpdatesAround(x, y, z)
        }
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
        vz: Double
    ): Boolean {
        return thrownItemSystem.spawn(
            entityId = entityId,
            ownerEntityId = ownerEntityId,
            kind = kind,
            x = x,
            y = y,
            z = z,
            vx = vx,
            vy = vy,
            vz = vz
        )
    }

    fun removeThrownItem(entityId: Int, hit: Boolean, x: Double? = null, y: Double? = null, z: Double? = null): ThrownItemRemovedEvent? {
        return thrownItemSystem.remove(entityId, hit, x, y, z)
    }

    fun hasThrownItems(): Boolean {
        return thrownItemSystem.hasEntities()
    }

    fun thrownItemSnapshot(entityId: Int): ThrownItemSnapshot? {
        return thrownItemSystem.snapshot(entityId)
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
        return fallingBlockSystem.spawn(
            entityId = entityId,
            blockStateId = blockStateId,
            x = x,
            y = y,
            z = z,
            vx = vx,
            vy = vy,
            vz = vz
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
        if (activeSimulationChunks != null) {
            val activeEntityChunks = HashSet<ChunkPos>()
            for (chunkPos in activeSimulationChunks) {
                if (droppedItemSystem.entityIdsInChunk(chunkPos.x, chunkPos.z).isNotEmpty()) {
                    activeEntityChunks.add(chunkPos)
                }
            }
            pruneIdleChunkTimingState(droppedItemLastTickNanos, activeEntityChunks)
        } else if (!droppedItemSystem.hasEntities()) {
            droppedItemLastTickNanos.clear()
        }
        return droppedItemSystem.tickOnChunkActors(
            deltaSeconds = deltaSeconds,
            activeSimulationChunks = activeSimulationChunks,
            submitChunkTask = { chunkPos, task -> chunkActorScheduler.submit(chunkPos, task) },
            chunkDeltaSecondsProvider = { chunkPos ->
                consumeChunkElapsedDeltaSeconds(
                    chunkPos = chunkPos,
                    fallbackSeconds = deltaSeconds,
                    lastTickMap = droppedItemLastTickNanos
                )
            },
            onChunkEvents = onChunkEvents,
            onDispatchComplete = onDispatchComplete,
            chunkTimeRecorder = chunkTimeRecorder
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
        animalSystem.setTemptSources(sources)
    }

    fun setAnimalLookSources(sources: List<AnimalLookSource>) {
        animalSystem.setLookSources(sources)
    }

    fun setAnimalRideControls(controls: List<AnimalRideControl>) {
        animalSystem.setRideControls(controls)
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

    private fun consumeChunkElapsedDeltaSeconds(
        chunkPos: ChunkPos,
        fallbackSeconds: Double,
        lastTickMap: ConcurrentHashMap<ChunkPos, Long>
    ): Double {
        val timeScale = ServerConfig.timeScale
        if (!timeScale.isFinite() || timeScale <= 0.0) return 0.0
        val nowNanos = System.nanoTime()
        val previous = lastTickMap.put(chunkPos, nowNanos)
        if (previous == null) {
            return if (fallbackSeconds.isFinite() && fallbackSeconds > 0.0) fallbackSeconds * timeScale else 0.0
        }
        val elapsedSeconds = ((nowNanos - previous).coerceAtLeast(0L)) / 1_000_000_000.0
        if (!elapsedSeconds.isFinite() || elapsedSeconds <= 0.0) return 0.0
        return elapsedSeconds * timeScale
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
        cachedSpawnPointRef.get()?.let { return it }
        val computed = findSharedSpawnNearOrigin()
        cachedSpawnPointRef.compareAndSet(null, computed)
        return cachedSpawnPointRef.get() ?: computed
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
        private const val FLUID_SLOPE_SEARCH_MAX = 4
        private const val FALLING_WATER_VISUAL_LEVEL = 8
        private const val FLUID_REMOVE_LEVEL = -1
        private const val NO_WATER_LEVEL = -1
        private const val SPAWN_SEARCH_CHUNK_RADIUS = 8
        private val HORIZONTAL_DIRS = arrayOf(
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
