package org.macaroon3145.network.handler

import io.netty.channel.ChannelHandlerContext
import org.macaroon3145.config.ServerConfig
import org.macaroon3145.world.ChunkPos
import org.macaroon3145.world.DroppedItemSnapshot
import org.macaroon3145.world.World
import org.slf4j.LoggerFactory
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CancellationException
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlin.math.min
import kotlin.math.floor

object ChunkStreamingService {
    private val logger = LoggerFactory.getLogger(ChunkStreamingService::class.java)
    private const val CHUNK_WRITE_FLUSH_THRESHOLD = 16
    private const val FORCED_REGION_GRID_EXPONENT = 0
    private const val DEFAULT_INTERLEAVE_CELL_EXPONENT = 4
    private val workerCount = configuredOrAutoWorkerParallelism()
    private val workerId = AtomicInteger(1)
    private val workerPool = Executors.newFixedThreadPool(workerCount) { runnable ->
        Thread(runnable, "aerogel-chunk-worker-${workerId.getAndIncrement()}").apply {
            isDaemon = true
            priority = Thread.NORM_PRIORITY
        }
    }
    private val packetWorkerCount = configuredPacketWorkerParallelism()
    private val packetWorkerId = AtomicInteger(1)
    private val packetPool = Executors.newFixedThreadPool(packetWorkerCount) { runnable ->
        Thread(runnable, "aerogel-chunk-packet-${packetWorkerId.getAndIncrement()}").apply {
            isDaemon = true
            priority = Thread.NORM_PRIORITY
        }
    }
    private fun configuredOrAutoWorkerParallelism(): Int {
        val configured = ServerConfig.chunkWorkerThreads
        if (configured > 0) return configured

        val jvmLogical = Runtime.getRuntime().availableProcessors().coerceAtLeast(1)
        val osLogical = detectOsLogicalCores()
        return maxOf(jvmLogical, osLogical).coerceAtLeast(1)
    }

    private fun detectOsLogicalCores(): Int {
        val linuxCpuInfo = runCatching {
            java.io.File("/proc/cpuinfo")
                .takeIf { it.isFile }
                ?.useLines { lines ->
                    lines.count { it.startsWith("processor\t:") || it.startsWith("processor :") }
                }
                ?: 0
        }.getOrDefault(0)
        val nprocValue = runCatching {
            val p = ProcessBuilder("nproc").redirectErrorStream(true).start()
            val out = p.inputStream.bufferedReader().use { it.readText().trim() }
            if (p.waitFor() == 0) out.toIntOrNull() ?: 0 else 0
        }.getOrDefault(0)
        val windowsEnv = System.getenv("NUMBER_OF_PROCESSORS")?.toIntOrNull() ?: 0
        return maxOf(linuxCpuInfo, nprocValue, windowsEnv, 1)
    }

    fun maxWorkerCount(): Int = workerCount

    private fun configuredPacketWorkerParallelism(): Int {
        val configured = System.getProperty("aerogel.chunk.packet-workers")
            ?.toIntOrNull()
            ?.coerceAtLeast(1)
        if (configured != null) return configured
        return workerCount
    }

    fun streamInitialChunks(
        ctx: ChannelHandlerContext,
        world: World,
        centerChunkX: Int,
        centerChunkZ: Int,
        radius: Int,
        loadedChunks: MutableSet<ChunkPos>,
        generatingChunks: MutableSet<ChunkPos>
    ): CompletableFuture<Int> {
        return streamAround(ctx, world, centerChunkX, centerChunkZ, radius, loadedChunks, generatingChunks)
    }

    fun streamAround(
        ctx: ChannelHandlerContext,
        world: World,
        centerChunkX: Int,
        centerChunkZ: Int,
        radius: Int,
        loadedChunks: MutableSet<ChunkPos>,
        generatingChunks: MutableSet<ChunkPos>,
        shouldSend: () -> Boolean = { true },
        onChunkSent: ((ChunkPos, List<DroppedItemSnapshot>) -> Unit)? = null
    ): CompletableFuture<Int> {
        val coords = buildSquareCoords(centerChunkX, centerChunkZ, radius)
        return streamCoords(ctx, world, coords, loadedChunks, generatingChunks, shouldSend, onChunkSent)
    }

    fun prewarmAsync(world: World): CompletableFuture<Void> {
        return CompletableFuture.runAsync({
            runCatching { world.prewarmChunkGeneration() }
        }, workerPool)
    }

    fun streamCoords(
        ctx: ChannelHandlerContext,
        world: World,
        coordsInput: List<ChunkPos>,
        loadedChunks: MutableSet<ChunkPos>,
        generatingChunks: MutableSet<ChunkPos>,
        shouldSend: () -> Boolean = { true },
        onChunkSent: ((ChunkPos, List<DroppedItemSnapshot>) -> Unit)? = null,
        workerLimit: Int = workerCount
    ): CompletableFuture<Int> {
        if (coordsInput.isEmpty()) return CompletableFuture.completedFuture(0)

        val coords = coordsInput
        val completion = CompletableFuture<Int>()
        val includeDeltas = world.hasChunkDeltaChanges()
        val includeDroppedItems = world.hasDroppedItems()
        val sent = AtomicInteger(0)
        val nextIndex = AtomicInteger(0)
        val pendingWrites = AtomicInteger(0)
        val pendingPacketTasks = AtomicInteger(0)
        val generationWorkersDone = AtomicBoolean(false)
        val flushScheduled = AtomicBoolean(false)
        val streamFailure = AtomicReference<Throwable?>(null)
        val parallelism = workerLimit
            .coerceAtLeast(1)
            .coerceAtMost(workerCount)
            .coerceAtMost(coords.size)
        val workers = ArrayList<CompletableFuture<Void>>(parallelism)

        fun scheduleIntermediateFlush() {
            if (!flushScheduled.compareAndSet(false, true)) return
            ctx.executor().execute {
                flushScheduled.set(false)
                if (ctx.channel().isActive && shouldSend()) {
                    ctx.flush()
                }
            }
        }

        fun failStream(cause: Throwable) {
            if (!streamFailure.compareAndSet(null, cause)) return
            completion.completeExceptionally(cause)
        }

        fun finishSuccessIfReady() {
            if (!generationWorkersDone.get()) return
            if (pendingPacketTasks.get() != 0) return
            if (!completion.complete(sent.get())) return
            ctx.executor().execute {
                if (ctx.channel().isActive && shouldSend()) {
                    ctx.flush()
                }
            }
        }

        repeat(parallelism) {
            workers += CompletableFuture.runAsync({
                while (true) {
                    if (completion.isDone) break
                    if (!shouldSend()) break
                    val index = nextIndex.getAndIncrement()
                    if (index >= coords.size) break
                    val coord = coords[index]
                    if (loadedChunks.contains(coord)) continue
                    if (!generatingChunks.add(coord)) continue

                    try {
                        val generated = world.buildChunk(coord) {
                            !completion.isDone && shouldSend()
                        }
                        pendingPacketTasks.incrementAndGet()
                        packetPool.execute {
                            try {
                                if (ctx.channel().isActive && shouldSend()) {
                                    val chunkPacket = PlayPackets.mapChunkPacket(coord.x, coord.z, generated)
                                    val deltaPackets = if (includeDeltas) buildChunkDeltaPackets(world, coord) else emptyList()
                                    val droppedItems = if (includeDroppedItems) world.droppedItemsInChunk(coord.x, coord.z) else emptyList()

                                    ctx.write(chunkPacket)
                                    for (packet in deltaPackets) {
                                        ctx.write(packet)
                                    }

                                    val writes = 1 + deltaPackets.size
                                    if (pendingWrites.addAndGet(writes) >= CHUNK_WRITE_FLUSH_THRESHOLD) {
                                        pendingWrites.set(0)
                                        scheduleIntermediateFlush()
                                    }

                                    loadedChunks.add(coord)
                                    sent.incrementAndGet()
                                    onChunkSent?.invoke(coord, droppedItems)
                                }
                            } catch (t: Throwable) {
                                logger.error("Chunk packet stage failed for {},{}", coord.x, coord.z, t)
                                failStream(RuntimeException("Chunk packet stage failed at ${coord.x},${coord.z}", t))
                            } finally {
                                generatingChunks.remove(coord)
                                if (pendingPacketTasks.decrementAndGet() == 0) {
                                    finishSuccessIfReady()
                                }
                            }
                        }
                    } catch (t: Throwable) {
                        generatingChunks.remove(coord)
                        if (isChunkRequestCancelled(t) || isChunkBridgeTimeout(t)) {
                            if (!shouldSend() || completion.isDone) {
                                break
                            }
                            continue
                        }
                        logger.error("Chunk stream failed for {},{}", coord.x, coord.z, t)
                        failStream(RuntimeException("Chunk stream failed at ${coord.x},${coord.z}", t))
                        break
                    }
                }
            }, workerPool)
        }

        CompletableFuture.allOf(*workers.toTypedArray()).whenComplete { _, error ->
            if (error != null) {
                failStream(error)
                return@whenComplete
            }
            generationWorkersDone.set(true)
            finishSuccessIfReady()
        }

        return completion
    }

    private fun isChunkRequestCancelled(throwable: Throwable): Boolean {
        var current: Throwable? = throwable
        while (current != null) {
            if (current is CancellationException) return true
            current = current.cause
        }
        return false
    }

    private fun isChunkBridgeTimeout(throwable: Throwable): Boolean {
        var current: Throwable? = throwable
        while (current != null) {
            if (current.javaClass.name.endsWith("ChunkBridgeTimeoutException")) return true
            current = current.cause
        }
        return false
    }

    private fun interleaveCoordsForRegionParallelism(coordsInput: List<ChunkPos>): List<ChunkPos> {
        if (coordsInput.size <= 3) return coordsInput
        val interleaveCellExponent = configuredInterleaveCellExponent()
        val interleaveCellSize = 1 shl interleaveCellExponent
        val windowSize = configuredRegionInterleaveWindow()

        val out = ArrayList<ChunkPos>(coordsInput.size)
        var start = 0
        while (start < coordsInput.size) {
            val end = min(coordsInput.size, start + windowSize)
            val buckets = LinkedHashMap<Long, ArrayDeque<ChunkPos>>()
            for (index in start until end) {
                val coord = coordsInput[index]
                val regionX = Math.floorDiv(coord.x, interleaveCellSize)
                val regionZ = Math.floorDiv(coord.z, interleaveCellSize)
                val key = (regionX.toLong() shl 32) xor (regionZ.toLong() and 0xFFFF_FFFFL)
                buckets.computeIfAbsent(key) { ArrayDeque() }.addLast(coord)
            }

            while (true) {
                var progressed = false
                for (bucket in buckets.values) {
                    val coord = bucket.removeFirstOrNull() ?: continue
                    out.add(coord)
                    progressed = true
                }
                if (!progressed) break
            }
            start = end
        }
        return out
    }

    private fun prioritizeNearFirstThenInterleave(coordsInput: List<ChunkPos>): List<ChunkPos> {
        if (coordsInput.size <= 3) return coordsInput
        val strictNearCount = configuredStrictNearFirstCount()
            .coerceIn(0, coordsInput.size)
        if (strictNearCount <= 0) {
            return interleaveCoordsForRegionParallelism(coordsInput)
        }
        if (strictNearCount >= coordsInput.size) {
            return ArrayList(coordsInput)
        }

        val out = ArrayList<ChunkPos>(coordsInput.size)
        out.addAll(coordsInput.subList(0, strictNearCount))
        out.addAll(interleaveCoordsForRegionParallelism(coordsInput.subList(strictNearCount, coordsInput.size)))
        return out
    }

    private fun configuredRegionGridExponent(): Int {
        return FORCED_REGION_GRID_EXPONENT
    }

    private fun configuredRegionInterleaveWindow(): Int {
        val minimum = maxOf(64, workerCount * 8)
        return System.getProperty("aerogel.folia.region-interleave-window")
            ?.toIntOrNull()
            ?.coerceAtLeast(minimum)
            ?: Int.MAX_VALUE
    }

    private fun configuredStrictNearFirstCount(): Int {
        return System.getProperty("aerogel.chunk.strict-near-first-count")
            ?.toIntOrNull()
            ?.coerceAtLeast(0)
            ?: maxOf(1024, workerCount * 32)
    }

    private fun configuredInterleaveCellExponent(): Int {
        return System.getProperty("aerogel.folia.interleave-cell-exponent")
            ?.toIntOrNull()
            ?.coerceIn(0, 6)
            ?: DEFAULT_INTERLEAVE_CELL_EXPONENT
    }

    fun buildSquareCoords(cx: Int, cz: Int, radius: Int): List<ChunkPos> {
        val clampedRadius = radius.coerceAtLeast(0)
        val result = ArrayList<ChunkPos>((clampedRadius * 2 + 1) * (clampedRadius * 2 + 1))
        val radiusSq = clampedRadius * clampedRadius
        for (dz in -clampedRadius..clampedRadius) {
            for (dx in -clampedRadius..clampedRadius) {
                val distSq = dx * dx + dz * dz
                if (distSq <= radiusSq) {
                    result.add(ChunkPos(cx + dx, cz + dz))
                }
            }
        }
        result.sortBy { coord ->
            val dx = coord.x - cx
            val dz = coord.z - cz
            (dx * dx + dz * dz)
        }
        return result
    }

    fun chunkXFromBlockX(x: Double): Int {
        return floor(x / 16.0).toInt()
    }

    fun chunkZFromBlockZ(z: Double): Int {
        return floor(z / 16.0).toInt()
    }

    fun fetchDroppedItemEntityIdsAsync(world: World, chunkPos: ChunkPos): CompletableFuture<IntArray> {
        return CompletableFuture.supplyAsync({
            runCatching { world.droppedItemEntityIdsInChunk(chunkPos.x, chunkPos.z) }
                .getOrElse { IntArray(0) }
        }, workerPool)
    }

    private fun buildChunkDeltaPackets(world: World, chunkPos: ChunkPos): List<ByteArray> {
        val changedBlocks = world.changedBlocksInChunk(chunkPos.x, chunkPos.z)
        val changedBlockEntities = world.changedBlockEntitiesInChunk(chunkPos.x, chunkPos.z)
        if (changedBlocks.isEmpty() && changedBlockEntities.isEmpty()) return emptyList()

        val packets = ArrayList<ByteArray>(changedBlocks.size + changedBlockEntities.size)
        for ((pos, stateId) in changedBlocks) {
            packets.add(PlayPackets.blockChangePacket(pos.x, pos.y, pos.z, stateId))
        }
        for ((pos, blockEntity) in changedBlockEntities) {
            packets.add(
                PlayPackets.blockEntityDataPacket(
                    pos.x,
                    pos.y,
                    pos.z,
                    blockEntity.typeId,
                    blockEntity.nbtPayload
                )
            )
        }
        return packets
    }
}
