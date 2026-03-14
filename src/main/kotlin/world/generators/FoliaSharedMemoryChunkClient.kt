package org.macaroon3145.world.generators

import org.macaroon3145.world.ChunkGenerationContext
import org.macaroon3145.world.ChunkPos
import org.macaroon3145.world.BlockPos
import org.macaroon3145.world.GeneratedChunk
import org.macaroon3145.world.HeightmapData
import org.macaroon3145.world.BlockStateLookupWorldGenerator
import org.macaroon3145.world.WorldGenerator
import org.macaroon3145.api.world.ChunkState
import org.slf4j.LoggerFactory
import java.io.ByteArrayInputStream
import java.io.DataInputStream
import java.nio.ByteOrder
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.Executors
import java.util.concurrent.ExecutorService
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicIntegerArray
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.LockSupport
import kotlin.math.min

object FoliaSharedMemoryWorldGenerator : WorldGenerator, BlockStateLookupWorldGenerator {
    private val client = FoliaSharedMemoryChunkClient()

    override fun generateChunk(context: ChunkGenerationContext): GeneratedChunk {
        return client.requestChunk(context)
    }

    override fun blockStateAt(worldKey: String, x: Int, y: Int, z: Int): Int {
        return client.blockStateAt(worldKey, x, y, z)
    }

    override fun blockStateAtIfCached(worldKey: String, x: Int, y: Int, z: Int): Int? {
        return client.blockStateAtIfCached(worldKey, x, y, z)
    }

    override fun rawBrightnessAt(worldKey: String, x: Int, y: Int, z: Int): Int {
        return client.rawBrightnessAt(worldKey, x, y, z)
    }

    override fun rawBrightnessAtIfCached(worldKey: String, x: Int, y: Int, z: Int): Int? {
        return client.rawBrightnessAtIfCached(worldKey, x, y, z)
    }

    fun retainLoadedChunks(worldKey: String, chunks: Set<ChunkPos>) {
        client.retainLoadedChunks(worldKey, chunks)
    }

    fun loadChunk(worldKey: String, chunkX: Int, chunkZ: Int): Boolean {
        return client.loadChunk(worldKey, chunkX, chunkZ)
    }

    fun unloadChunk(worldKey: String, chunkX: Int, chunkZ: Int): Boolean {
        return client.unloadChunk(worldKey, chunkX, chunkZ)
    }

    fun isChunkLoaded(worldKey: String, chunkX: Int, chunkZ: Int): Boolean {
        return client.isChunkLoaded(worldKey, chunkX, chunkZ)
    }

    fun chunkState(worldKey: String, chunkX: Int, chunkZ: Int): ChunkState {
        return client.chunkState(worldKey, chunkX, chunkZ)
    }

    fun invalidateChunkGeneratedAndLighting(worldKey: String, chunkX: Int, chunkZ: Int) {
        client.invalidateChunkGeneratedAndLighting(worldKey, chunkX, chunkZ)
    }

    fun pushBlockUpdatesIfRevisionChanged(
        worldKey: String,
        chunkX: Int,
        chunkZ: Int,
        revision: Long,
        changedBlocks: List<Pair<BlockPos, Int>>
    ) {
        client.pushBlockUpdatesIfRevisionChanged(worldKey, chunkX, chunkZ, revision, changedBlocks)
    }
}

private object FoliaSharedProtocol {
    const val REQUEST_MAGIC = 0x41525131 // ARQ1
    const val RESPONSE_MAGIC = 0x41525331 // ARS1

    const val REQUEST_FILE_SIZE = 4096
    const val RESPONSE_FILE_SIZE = 8 * 1024 * 1024

    const val REQUEST_STATE_OFFSET = 4
    const val REQUEST_ID_OFFSET = 8
    const val REQUEST_WORLD_LEN_OFFSET = 16
    const val REQUEST_WORLD_OFFSET = 20
    const val REQUEST_WORLD_MAX_BYTES = 256
    const val REQUEST_CHUNK_X_OFFSET = REQUEST_WORLD_OFFSET + REQUEST_WORLD_MAX_BYTES
    const val REQUEST_CHUNK_Z_OFFSET = REQUEST_CHUNK_X_OFFSET + 4

    const val REQUEST_STATE_EMPTY = 0
    const val REQUEST_STATE_READY = 1
    const val REQUEST_STATE_PROCESSING = 2
    const val REQUEST_STATE_CANCELLED = 3

    const val RESPONSE_STATE_OFFSET = 4
    const val RESPONSE_ID_OFFSET = 8
    const val RESPONSE_PAYLOAD_LEN_OFFSET = 16
    const val RESPONSE_PAYLOAD_OFFSET = 20

    const val RESPONSE_STATE_EMPTY = 0
    const val RESPONSE_STATE_SUCCESS = 1
    const val RESPONSE_STATE_ERROR = 2
}

private class FoliaSharedMemoryChunkClient {
    private companion object {
        private const val AIR_STATE_ID = 0
        private const val OVERWORLD_MIN_Y = -64
        private const val DEFAULT_MIN_Y = 0
        private const val BLOCK_SECTION_SIZE = 16 * 16 * 16
        private const val BIOME_SECTION_SIZE = 4 * 4 * 4
        private const val BLOCK_INDIRECT_PALETTE_MAX_BITS = 8
        private const val BIOME_INDIRECT_PALETTE_MAX_BITS = 3
        private const val MAX_PREFETCHES_PER_RETAIN_UPDATE = 16
        private const val AUTO_CACHE_HEAP_RATIO = 0.20
        private const val ESTIMATED_CACHED_CHUNK_BYTES = 192 * 1024
        private const val MIN_AUTO_CACHED_CHUNKS = 1024
        private const val MAX_AUTO_CACHED_CHUNKS = 65_536
        private const val RETAINED_CHUNK_HEADROOM = 512
        private const val CACHE_LIMIT_RECALCULATION_INTERVAL = 256
        private const val BLOCK_UPDATE_REQUESTS_FILE = "block-update-requests.tsv"
    }

    private val logger = LoggerFactory.getLogger(FoliaSharedMemoryChunkClient::class.java)
    private val runtimeDir: Path = Path.of(".aerogel-cache/folia/runtime")
    private val ipcDir: Path = runtimeDir.resolve("ipc")
    private val readyMarker: Path = ipcDir.resolve("bridge-ready")
    private val nextRequestId = AtomicLong(1L)
    private val slotCursor = AtomicInteger(0)
    private val threadSlot = ThreadLocal<Int>()
    private val slots = ArrayList<IpcSlot>()
    @Volatile private var slotBusy: AtomicIntegerArray? = null
    private val chunkCache = ConcurrentHashMap<ChunkCacheKey, ChunkCacheEntry>()
    private val pushedBlockUpdateRevisionByChunk = ConcurrentHashMap<ChunkCacheKey, Long>()
    private val decodedChunkOrder = ConcurrentLinkedDeque<ChunkCacheKey>()
    private val retainedChunkKeys = ConcurrentHashMap.newKeySet<ChunkCacheKey>()
    private val configuredMaxCachedChunks = configuredBlockStateCacheOverride()
    private val maxCachedChunks = AtomicInteger(configuredMaxCachedChunks ?: computeAutoCacheChunkLimit())
    private val cacheLimitRecalculationCounter = AtomicInteger(0)
    private val decodeThreadId = AtomicInteger(1)
    private val snapshotDecodeExecutor = Executors.newFixedThreadPool(configuredSnapshotDecodeWorkers()) { runnable ->
        Thread(runnable, "aerogel-folia-snapshot-decode-${decodeThreadId.getAndIncrement()}").apply {
            isDaemon = true
            priority = Thread.NORM_PRIORITY
        }
    }
    private val prefetchThreadId = AtomicInteger(1)
    private val snapshotPrefetchExecutor: ExecutorService = Executors.newFixedThreadPool(configuredSnapshotPrefetchWorkers()) { runnable ->
        Thread(runnable, "aerogel-folia-snapshot-prefetch-${prefetchThreadId.getAndIncrement()}").apply {
            isDaemon = true
            priority = Thread.NORM_PRIORITY
        }
    }

    @Volatile
    private var initialized = false
    private val mappingInProgress = AtomicBoolean(false)
    private val recoveryInProgress = AtomicBoolean(false)

    private data class ChunkCacheKey(
        val worldKey: String,
        val chunkX: Int,
        val chunkZ: Int
    )

    private class ChunkCacheEntry {
        val generated = AtomicReference<GeneratedChunk?>()
        val snapshot = AtomicReference<ChunkStateSnapshot?>()
        val light = AtomicReference<ChunkLightSnapshot?>()
        val decodeTask = AtomicReference<CompletableFuture<ChunkStateSnapshot>?>()
        val loadTask = AtomicReference<CompletableFuture<ChunkStateSnapshot?>?>()
    }

    private data class ChunkStateSnapshot(
        val minY: Int,
        val sections: Array<SectionState>
    ) {
        fun blockStateAt(x: Int, y: Int, z: Int): Int {
            val sectionIndex = (y - minY) shr 4
            if (sectionIndex !in sections.indices) return AIR_STATE_ID
            val section = sections[sectionIndex]
            val localIndex = ((y and 15) shl 8) or ((z and 15) shl 4) or (x and 15)
            return section.blockStateAt(localIndex)
        }
    }

    private data class ChunkLightSnapshot(
        val minSectionY: Int,
        val skySectionToArrayIndex: IntArray,
        val blockSectionToArrayIndex: IntArray,
        val skyLightArrays: List<ByteArray>,
        val blockLightArrays: List<ByteArray>
    ) {
        fun rawBrightnessAt(x: Int, y: Int, z: Int): Int {
            val sectionY = y shr 4
            val lightSection = sectionY - minSectionY + 1
            if (lightSection !in skySectionToArrayIndex.indices) return 0
            val localIndex = ((y and 15) shl 8) or ((z and 15) shl 4) or (x and 15)
            val sky = nibbleAt(skyLightArrays, skySectionToArrayIndex[lightSection], localIndex)
            val block = nibbleAt(blockLightArrays, blockSectionToArrayIndex[lightSection], localIndex)
            return if (sky >= block) sky else block
        }

        private fun nibbleAt(arrays: List<ByteArray>, arrayIndex: Int, blockIndex: Int): Int {
            if (arrayIndex < 0 || arrayIndex >= arrays.size) return 0
            val section = arrays[arrayIndex]
            val byteIndex = blockIndex ushr 1
            if (byteIndex !in section.indices) return 0
            val value = section[byteIndex].toInt() and 0xFF
            return if ((blockIndex and 1) == 0) {
                value and 0x0F
            } else {
                (value ushr 4) and 0x0F
            }
        }
    }

    private sealed interface SectionState {
        fun blockStateAt(index: Int): Int
    }

    private data class SingleSectionState(
        val stateId: Int
    ) : SectionState {
        override fun blockStateAt(index: Int): Int = stateId
    }

    private data class PackedSectionState(
        val bitsPerEntry: Int,
        val palette: IntArray?,
        val data: LongArray
    ) : SectionState {
        private val mask: Long = (1L shl bitsPerEntry) - 1L
        private val valuesPerLong: Int = 64 / bitsPerEntry

        override fun blockStateAt(index: Int): Int {
            if (bitsPerEntry <= 0) return AIR_STATE_ID
            if (valuesPerLong <= 0) return AIR_STATE_ID
            // Match Mojang SimpleBitStorage indexing used by LevelChunkSection#write.
            val dataIndex = index / valuesPerLong
            if (dataIndex !in data.indices) return AIR_STATE_ID
            val bitOffset = (index - dataIndex * valuesPerLong) * bitsPerEntry
            val rawId = ((data[dataIndex] ushr bitOffset) and mask).toInt()
            val localPalette = palette ?: return rawId
            return localPalette.getOrElse(rawId) { AIR_STATE_ID }
        }
    }

    private data class IpcSlot(
        val index: Int,
        val requestChannel: FileChannel,
        val responseChannel: FileChannel,
        val requestBuffer: MappedByteBuffer,
        val responseBuffer: MappedByteBuffer
    )

    fun requestChunk(context: ChunkGenerationContext): GeneratedChunk {
        val cachedGenerated = cachedGeneratedChunk(context)
        if (cachedGenerated != null) {
            return cachedGenerated
        }
        ensureMapped()
        val slotIndex = acquireSlotIndex()
        val slot = slots[slotIndex]
        var requestStarted = false
        try {
            if (context.isCancelled()) {
                throw java.util.concurrent.CancellationException("Folia chunk request cancelled before submit")
            }
            val requestId = nextRequestId.getAndIncrement()
            writeRequest(slot, requestId, context)
            requestStarted = true

            val timeoutMillis = configuredTimeoutMillis()
            val timeoutNanos = if (timeoutMillis <= 0L) Long.MAX_VALUE else timeoutMillis * 1_000_000L
            val startNanos = System.nanoTime()

            while (true) {
                if (context.isCancelled()) {
                    cancelRequest(slot)
                    throw java.util.concurrent.CancellationException(
                        "Folia chunk request cancelled for ${context.worldKey} ${context.chunkPos.x},${context.chunkPos.z}"
                    )
                }
                val state = slot.responseBuffer.getInt(FoliaSharedProtocol.RESPONSE_STATE_OFFSET)
                val responseId = slot.responseBuffer.getLong(FoliaSharedProtocol.RESPONSE_ID_OFFSET)
                if (responseId == requestId) {
                    when (state) {
                        FoliaSharedProtocol.RESPONSE_STATE_SUCCESS -> {
                            val decoded = decodeSuccessPayload(slot, context.worldKey)
                            clearRequestResponseState(slot)
                            cacheDecodedChunk(
                                context = context,
                                generated = decoded.chunk,
                                snapshot = decoded.snapshot,
                                light = decodeChunkLightSnapshot(context.worldKey, decoded.chunk)
                            )
                            return decoded.chunk
                        }
                        FoliaSharedProtocol.RESPONSE_STATE_ERROR -> {
                            val message = decodeErrorPayload(slot)
                            clearRequestResponseState(slot)
                            throw IllegalStateException("Folia chunk bridge error: $message")
                        }
                    }
                }
                val now = System.nanoTime()
                if (timeoutNanos != Long.MAX_VALUE && now - startNanos >= timeoutNanos) {
                    cancelRequest(slot)
                    throw ChunkBridgeTimeoutException(
                        "Folia chunk bridge timeout for ${context.worldKey} ${context.chunkPos.x},${context.chunkPos.z}"
                    )
                }
                LockSupport.parkNanos(200_000L)
            }
        } finally {
            if (!requestStarted) {
                runCatching { clearRequestResponseState(slot) }
            }
            releaseSlotIndex(slotIndex)
        }
    }

    fun blockStateAt(worldKey: String, x: Int, y: Int, z: Int): Int {
        val chunkKey = ChunkCacheKey(worldKey, x shr 4, z shr 4)
        val entry = chunkCache[chunkKey]
        var snapshot = entry?.snapshot?.get()
        if (snapshot == null) {
            val task = entry?.decodeTask?.get()
            if (task != null) {
                snapshot = runCatching { task.join() }.getOrNull()
            }
        }
        if (snapshot == null) {
            snapshot = loadSnapshotOnDemand(chunkKey)
        }
        snapshot ?: return AIR_STATE_ID
        return snapshot.blockStateAt(x, y, z)
    }

    fun blockStateAtIfCached(worldKey: String, x: Int, y: Int, z: Int): Int? {
        val chunkKey = ChunkCacheKey(worldKey, x shr 4, z shr 4)
        val entry = chunkCache[chunkKey] ?: return null
        val snapshot = entry.snapshot.get() ?: return null
        return snapshot.blockStateAt(x, y, z)
    }

    fun rawBrightnessAt(worldKey: String, x: Int, y: Int, z: Int): Int {
        val chunkKey = ChunkCacheKey(worldKey, x shr 4, z shr 4)
        var light = chunkCache[chunkKey]?.light?.get()
        if (light == null) {
            requestAndAwaitSnapshot(chunkKey)
            light = chunkCache[chunkKey]?.light?.get()
        }
        return light?.rawBrightnessAt(x, y, z) ?: 0
    }

    fun rawBrightnessAtIfCached(worldKey: String, x: Int, y: Int, z: Int): Int? {
        val chunkKey = ChunkCacheKey(worldKey, x shr 4, z shr 4)
        val entry = chunkCache[chunkKey] ?: return null
        val light = entry.light.get() ?: return null
        return light.rawBrightnessAt(x, y, z)
    }

    fun retainLoadedChunks(worldKey: String, chunks: Set<ChunkPos>) {
        retainedChunkKeys.removeIf { it.worldKey == worldKey }
        for (chunk in chunks) {
            retainedChunkKeys.add(ChunkCacheKey(worldKey, chunk.x, chunk.z))
        }
        maybeRefreshMaxCachedChunks(force = true)
        trimDecodedChunkCache()
    }

    fun loadChunk(worldKey: String, chunkX: Int, chunkZ: Int): Boolean {
        val chunkKey = ChunkCacheKey(worldKey, chunkX, chunkZ)
        return requestAndAwaitSnapshot(chunkKey) != null
    }

    fun unloadChunk(worldKey: String, chunkX: Int, chunkZ: Int): Boolean {
        val chunkKey = ChunkCacheKey(worldKey, chunkX, chunkZ)
        val bridgeRequested = requestBridgeChunkUnload(worldKey, chunkX, chunkZ)
        retainedChunkKeys.remove(chunkKey)
        pushedBlockUpdateRevisionByChunk.remove(chunkKey)
        val removed = chunkCache.remove(chunkKey)
        return bridgeRequested || removed != null
    }

    fun isChunkLoaded(worldKey: String, chunkX: Int, chunkZ: Int): Boolean {
        val chunkKey = ChunkCacheKey(worldKey, chunkX, chunkZ)
        val entry = chunkCache[chunkKey] ?: return false
        return entry.snapshot.get() != null
    }

    fun chunkState(worldKey: String, chunkX: Int, chunkZ: Int): ChunkState {
        val chunkKey = ChunkCacheKey(worldKey, chunkX, chunkZ)
        val entry = chunkCache[chunkKey] ?: return ChunkState.UNLOADED
        if (entry.snapshot.get() != null) return ChunkState.LOADED
        val decodeTask = entry.decodeTask.get()
        if (decodeTask != null && !decodeTask.isDone) return ChunkState.LOADING
        val loadTask = entry.loadTask.get()
        if (loadTask != null && !loadTask.isDone) return ChunkState.LOADING
        return ChunkState.UNLOADED
    }

    fun invalidateChunkGeneratedAndLighting(worldKey: String, chunkX: Int, chunkZ: Int) {
        val chunkKey = ChunkCacheKey(worldKey, chunkX, chunkZ)
        val entry = chunkCache[chunkKey] ?: return
        entry.generated.set(null)
        entry.light.set(null)
    }

    fun pushBlockUpdatesIfRevisionChanged(
        worldKey: String,
        chunkX: Int,
        chunkZ: Int,
        revision: Long,
        changedBlocks: List<Pair<BlockPos, Int>>
    ) {
        if (revision <= 0L || changedBlocks.isEmpty()) return
        val chunkKey = ChunkCacheKey(worldKey, chunkX, chunkZ)
        val previous = pushedBlockUpdateRevisionByChunk[chunkKey]
        if (previous != null && previous >= revision) return

        val payload = buildString(changedBlocks.size * 40) {
            val sanitizedWorldKey = worldKey.replace('\t', ' ').replace('\n', ' ').replace('\r', ' ')
            for ((pos, stateId) in changedBlocks) {
                append(sanitizedWorldKey)
                append('\t')
                append(pos.x)
                append('\t')
                append(pos.y)
                append('\t')
                append(pos.z)
                append('\t')
                append(stateId)
                append('\n')
            }
        }
        if (payload.isEmpty()) return
        val written = appendBridgeRequestFile(BLOCK_UPDATE_REQUESTS_FILE, payload)
        if (written) {
            pushedBlockUpdateRevisionByChunk[chunkKey] = revision
        }
    }

    private fun requestBridgeChunkUnload(worldKey: String, chunkX: Int, chunkZ: Int): Boolean {
        val sanitizedWorldKey = worldKey.replace('\t', ' ').replace('\n', ' ').replace('\r', ' ')
        val payload = "$sanitizedWorldKey\t$chunkX\t$chunkZ\n"
        val written = appendBridgeRequestFile("chunk-unload-requests.tsv", payload)
        if (!written) {
            logger.warn(
                "Failed to request bridge chunk unload for {} {},{}",
                worldKey,
                chunkX,
                chunkZ
            )
        }
        return written
    }

    private fun appendBridgeRequestFile(fileName: String, payload: String): Boolean {
        val requestPath = ipcDir.resolve(fileName)
        return runCatching {
            Files.createDirectories(ipcDir)
            FileChannel.open(
                requestPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE
            ).use { channel ->
                channel.lock().use {
                    channel.position(channel.size())
                    val bytes = payload.toByteArray(StandardCharsets.UTF_8)
                    var offset = 0
                    while (offset < bytes.size) {
                        val wrote = channel.write(java.nio.ByteBuffer.wrap(bytes, offset, bytes.size - offset))
                        if (wrote <= 0) break
                        offset += wrote
                    }
                }
            }
            true
        }.getOrElse { throwable ->
            logger.warn(
                "Failed to append bridge request file {}",
                requestPath.toAbsolutePath(),
                throwable
            )
            false
        }
    }

    private fun loadSnapshotOnDemand(chunkKey: ChunkCacheKey): ChunkStateSnapshot? {
        val entry = chunkCache.computeIfAbsent(chunkKey) { ChunkCacheEntry() }
        val existing = entry.loadTask.get()
        if (existing != null) {
            return runCatching { existing.join() }.getOrNull()
        }

        val created = CompletableFuture.supplyAsync({
            runCatching {
                requestAndAwaitSnapshot(chunkKey)
            }.onFailure { throwable ->
                logger.warn(
                    "Failed to reload block-state snapshot on demand for {} {},{}",
                    chunkKey.worldKey,
                    chunkKey.chunkX,
                    chunkKey.chunkZ,
                    throwable
                )
            }.getOrNull()
        }, snapshotPrefetchExecutor)
        val task = if (entry.loadTask.compareAndSet(null, created)) created else entry.loadTask.get() ?: created
        if (task === created) {
            task.whenComplete { _, _ ->
                entry.loadTask.compareAndSet(task, null)
            }
        }
        return runCatching { task.join() }.getOrNull()
    }

    private fun requestAndAwaitSnapshot(chunkKey: ChunkCacheKey, prefetchNeighbors: Boolean = true): ChunkStateSnapshot? {
        val entry = chunkCache.computeIfAbsent(chunkKey) { ChunkCacheEntry() }
        val cached = entry.snapshot.get()
        if (cached != null) return cached
        val pendingDecode = entry.decodeTask.get()
        if (pendingDecode != null) return pendingDecode.join()

        val context = ChunkGenerationContext(
            worldKey = chunkKey.worldKey,
            seed = 0L,
            chunkPos = ChunkPos(chunkKey.chunkX, chunkKey.chunkZ)
        )
        requestChunk(context)
        val snapshot = entry.snapshot.get() ?: entry.decodeTask.get()?.join()
        if (snapshot != null && prefetchNeighbors) {
            scheduleNeighborPrefetch(chunkKey)
        }
        return snapshot
    }

    private fun scheduleNeighborPrefetch(center: ChunkCacheKey) {
        for (dz in -1..1) {
            for (dx in -1..1) {
                if (dx == 0 && dz == 0) continue
                val neighbor = ChunkCacheKey(center.worldKey, center.chunkX + dx, center.chunkZ + dz)
                prefetchSnapshot(neighbor)
            }
        }
    }

    private fun prefetchSnapshot(chunkKey: ChunkCacheKey) {
        val entry = chunkCache.computeIfAbsent(chunkKey) { ChunkCacheEntry() }
        if (entry.snapshot.get() != null) return
        if (entry.decodeTask.get() != null) return
        if (entry.loadTask.get() != null) return
        val created = CompletableFuture.supplyAsync({
            runCatching { requestAndAwaitSnapshot(chunkKey, prefetchNeighbors = false) }.getOrNull()
        }, snapshotPrefetchExecutor)
        val task = if (entry.loadTask.compareAndSet(null, created)) created else entry.loadTask.get() ?: created
        if (task === created) {
            task.whenComplete { _, _ ->
                entry.loadTask.compareAndSet(task, null)
            }
        }
    }

    private fun configuredTimeoutMillis(): Long {
        return System.getProperty("aerogel.chunk.ipc.timeout-ms")
            ?.toLongOrNull()
            ?.coerceAtLeast(0L)
            ?: 30_000L
    }

    private fun configuredBlockStateCacheOverride(): Int? {
        return System.getProperty("aerogel.chunk.blockstate-cache.max-chunks")
            ?.toIntOrNull()
            ?.coerceAtLeast(1)
    }

    private fun maybeRefreshMaxCachedChunks(force: Boolean = false) {
        if (configuredMaxCachedChunks != null) return
        if (!force) {
            val count = cacheLimitRecalculationCounter.incrementAndGet()
            if (count % CACHE_LIMIT_RECALCULATION_INTERVAL != 0) return
        }
        maxCachedChunks.set(computeAutoCacheChunkLimit())
    }

    private fun computeAutoCacheChunkLimit(): Int {
        val runtime = Runtime.getRuntime()
        val heapMaxBytes = runtime.maxMemory().coerceAtLeast(64L * 1024L * 1024L)
        val byHeap = ((heapMaxBytes.toDouble() * AUTO_CACHE_HEAP_RATIO) / ESTIMATED_CACHED_CHUNK_BYTES.toDouble())
            .toInt()
            .coerceAtLeast(1)
        val cpuFloor = Runtime.getRuntime().availableProcessors().coerceAtLeast(1) * 64
        val retainedFloor = retainedChunkKeys.size + RETAINED_CHUNK_HEADROOM
        val floor = maxOf(MIN_AUTO_CACHED_CHUNKS, cpuFloor, retainedFloor).coerceAtMost(MAX_AUTO_CACHED_CHUNKS)
        return byHeap.coerceIn(floor, MAX_AUTO_CACHED_CHUNKS)
    }

    private fun configuredSnapshotDecodeWorkers(): Int {
        return System.getProperty("aerogel.chunk.blockstate-decode-workers")
            ?.toIntOrNull()
            ?.coerceAtLeast(1)
            ?: 4
    }

    private fun configuredSnapshotPrefetchWorkers(): Int {
        return System.getProperty("aerogel.chunk.blockstate-prefetch-workers")
            ?.toIntOrNull()
            ?.coerceIn(1, 4)
            ?: 2
    }

    private fun ensureMapped() {
        if (initialized) return
        if (mappingInProgress.compareAndSet(false, true)) {
            try {
                if (initialized) return

                val requestedSlotCount = configuredSlotCount()
                val availableSlotIndexes = waitForReadyAndDiscoverSlots(requestedSlotCount)
                slots.ensureCapacity(availableSlotIndexes.size)

                for (slotIndex in availableSlotIndexes) {
                    val requestFile = ipcDir.resolve("chunk-request-$slotIndex.mmap")
                    val responseFile = ipcDir.resolve("chunk-response-$slotIndex.mmap")

                    val requestChannel = FileChannel.open(
                        requestFile,
                        StandardOpenOption.READ,
                        StandardOpenOption.WRITE
                    )
                    val responseChannel = FileChannel.open(
                        responseFile,
                        StandardOpenOption.READ,
                        StandardOpenOption.WRITE
                    )

                    require(requestChannel.size() >= FoliaSharedProtocol.REQUEST_FILE_SIZE.toLong()) {
                        "Invalid request mmap size for slot=$slotIndex: ${requestChannel.size()}"
                    }
                    require(responseChannel.size() >= FoliaSharedProtocol.RESPONSE_FILE_SIZE.toLong()) {
                        "Invalid response mmap size for slot=$slotIndex: ${responseChannel.size()}"
                    }

                    val requestBuffer = requestChannel.map(
                        FileChannel.MapMode.READ_WRITE,
                        0L,
                        FoliaSharedProtocol.REQUEST_FILE_SIZE.toLong()
                    ) as MappedByteBuffer
                    requestBuffer.order(ByteOrder.BIG_ENDIAN)

                    val responseBuffer = responseChannel.map(
                        FileChannel.MapMode.READ_WRITE,
                        0L,
                        FoliaSharedProtocol.RESPONSE_FILE_SIZE.toLong()
                    ) as MappedByteBuffer
                    responseBuffer.order(ByteOrder.BIG_ENDIAN)

                    val slot = IpcSlot(
                        index = slotIndex,
                        requestChannel = requestChannel,
                        responseChannel = responseChannel,
                        requestBuffer = requestBuffer,
                        responseBuffer = responseBuffer
                    )
                    initializeHeadersIfNeeded(slot)
                    slots.add(slot)
                }

                require(slots.isNotEmpty()) { "Folia bridge exposed no IPC slots in $ipcDir" }
                if (slots.size != requestedSlotCount) {
                    logger.warn(
                        "Folia bridge slot mismatch (requested={}, available={}); using available slot count",
                        requestedSlotCount,
                        slots.size
                    )
                }

                slotBusy = AtomicIntegerArray(slots.size)
                initialized = true
            } finally {
                mappingInProgress.set(false)
            }
            return
        }
        while (mappingInProgress.get() && !initialized) {
            Thread.onSpinWait()
        }
    }

    private fun resetMappings() {
        while (!mappingInProgress.compareAndSet(false, true)) {
            Thread.onSpinWait()
        }
        try {
            if (!initialized && slots.isEmpty()) return
            for (slot in slots) {
                runCatching { slot.requestChannel.close() }
                runCatching { slot.responseChannel.close() }
            }
            slots.clear()
            slotBusy = null
            threadSlot.remove()
            initialized = false
        } finally {
            mappingInProgress.set(false)
        }
    }

    private fun waitForReadyAndDiscoverSlots(requestedSlotCount: Int): IntArray {
        val initTimeoutMillis = configuredInitTimeoutMillis()
        val startNanos = System.nanoTime()
        var nextWarnAt = startNanos + 2_000_000_000L

        while (true) {
            if (Files.isRegularFile(readyMarker)) {
                val discovered = discoverAvailableSlots(requestedSlotCount)
                if (discovered.isNotEmpty()) {
                    return discovered
                }
            }

            val now = System.nanoTime()
            if (now >= nextWarnAt) {
                logger.warn(
                    "Waiting for Folia IPC ready marker/slots at {} (elapsedMs={})",
                    readyMarker.toAbsolutePath(),
                    (now - startNanos) / 1_000_000L
                )
                nextWarnAt = now + 2_000_000_000L
            }
            if (now - startNanos >= initTimeoutMillis * 1_000_000L) {
                throw IllegalStateException(
                    "Folia IPC bridge not ready after ${initTimeoutMillis}ms (marker=${readyMarker.toAbsolutePath()})"
                )
            }
            LockSupport.parkNanos(20_000_000L)
        }
    }

    private fun discoverAvailableSlots(requestedSlotCount: Int): IntArray {
        if (requestedSlotCount <= 0) return IntArray(0)
        val found = ArrayList<Int>(requestedSlotCount)
        for (slotIndex in 0 until requestedSlotCount) {
            val requestFile = ipcDir.resolve("chunk-request-$slotIndex.mmap")
            val responseFile = ipcDir.resolve("chunk-response-$slotIndex.mmap")
            if (Files.isRegularFile(requestFile) && Files.isRegularFile(responseFile)) {
                found.add(slotIndex)
            }
        }
        return found.toIntArray()
    }

    private fun configuredInitTimeoutMillis(): Long {
        return System.getProperty("aerogel.chunk.ipc.init-timeout-ms")
            ?.toLongOrNull()
            ?.coerceAtLeast(1L)
            ?: 15_000L
    }

    private fun configuredSlotCount(): Int {
        val property = System.getProperty("aerogel.chunk.ipc.slots")
            ?.toIntOrNull()
            ?.coerceAtLeast(1)
        if (property != null) return property

        val jvmLogical = Runtime.getRuntime().availableProcessors().coerceAtLeast(1)
        val osLogical = detectOsLogicalCores()
        return resolveEffectiveLogicalCores(jvmLogical, osLogical)
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
            val process = ProcessBuilder("nproc").redirectErrorStream(true).start()
            val out = process.inputStream.bufferedReader().use { it.readText().trim() }
            if (process.waitFor() == 0) out.toIntOrNull() ?: 0 else 0
        }.getOrDefault(0)
        val windowsEnv = System.getenv("NUMBER_OF_PROCESSORS")?.toIntOrNull() ?: 0
        return maxOf(linuxCpuInfo, nprocValue, windowsEnv, 1)
    }

    private fun resolveEffectiveLogicalCores(jvmLogical: Int, osLogical: Int): Int {
        val jvm = jvmLogical.coerceAtLeast(1)
        val os = osLogical.coerceAtLeast(1)
        // Favor a cgroup-aware effective count rather than host over-reporting.
        return minOf(jvm, os).coerceAtLeast(1)
    }

    private fun acquireSlotIndex(): Int {
        val busy = slotBusy ?: throw IllegalStateException("Folia IPC slots are not initialized")
        val existing = threadSlot.get()
        if (existing != null && existing in slots.indices && busy.compareAndSet(existing, 0, 1)) {
            return existing
        }

        val slotCount = slots.size
        var spinCount = 0
        while (true) {
            val start = Math.floorMod(slotCursor.getAndIncrement(), slotCount)
            for (offset in 0 until slotCount) {
                val index = (start + offset) % slotCount
                if (busy.compareAndSet(index, 0, 1)) {
                    threadSlot.set(index)
                    return index
                }
            }
            if (spinCount < 64) {
                spinCount++
                Thread.onSpinWait()
                continue
            }
            LockSupport.parkNanos(200_000L)
            spinCount = 0
        }
    }

    private fun releaseSlotIndex(index: Int) {
        val busy = slotBusy ?: return
        busy.set(index, 0)
    }

    private fun initializeHeadersIfNeeded(slot: IpcSlot) {
        if (slot.requestBuffer.getInt(0) != FoliaSharedProtocol.REQUEST_MAGIC) {
            slot.requestBuffer.putInt(0, FoliaSharedProtocol.REQUEST_MAGIC)
        }
        if (slot.responseBuffer.getInt(0) != FoliaSharedProtocol.RESPONSE_MAGIC) {
            slot.responseBuffer.putInt(0, FoliaSharedProtocol.RESPONSE_MAGIC)
        }
        clearRequestResponseState(slot)
    }

    private fun writeRequest(slot: IpcSlot, requestId: Long, context: ChunkGenerationContext) {
        clearRequestResponseState(slot)
        val worldBytes = context.worldKey.toByteArray(StandardCharsets.UTF_8)
        val worldSize = min(worldBytes.size, FoliaSharedProtocol.REQUEST_WORLD_MAX_BYTES)

        slot.requestBuffer.putLong(FoliaSharedProtocol.REQUEST_ID_OFFSET, requestId)
        slot.requestBuffer.putInt(FoliaSharedProtocol.REQUEST_WORLD_LEN_OFFSET, worldSize)
        writeBytes(
            slot.requestBuffer,
            FoliaSharedProtocol.REQUEST_WORLD_OFFSET,
            worldBytes,
            worldSize
        )
        slot.requestBuffer.putInt(FoliaSharedProtocol.REQUEST_CHUNK_X_OFFSET, context.chunkPos.x)
        slot.requestBuffer.putInt(FoliaSharedProtocol.REQUEST_CHUNK_Z_OFFSET, context.chunkPos.z)
        slot.requestBuffer.putInt(FoliaSharedProtocol.REQUEST_STATE_OFFSET, FoliaSharedProtocol.REQUEST_STATE_READY)
    }

    private fun cancelRequest(slot: IpcSlot) {
        slot.requestBuffer.putInt(FoliaSharedProtocol.REQUEST_STATE_OFFSET, FoliaSharedProtocol.REQUEST_STATE_CANCELLED)
        slot.responseBuffer.putInt(FoliaSharedProtocol.RESPONSE_STATE_OFFSET, FoliaSharedProtocol.RESPONSE_STATE_EMPTY)
        slot.responseBuffer.putLong(FoliaSharedProtocol.RESPONSE_ID_OFFSET, 0L)
        slot.responseBuffer.putInt(FoliaSharedProtocol.RESPONSE_PAYLOAD_LEN_OFFSET, 0)
    }

    private data class DecodedChunkPayload(
        val chunk: GeneratedChunk,
        val snapshot: ChunkStateSnapshot
    )

    private fun decodeSuccessPayload(slot: IpcSlot, worldKey: String): DecodedChunkPayload {
        val payloadLength = slot.responseBuffer.getInt(FoliaSharedProtocol.RESPONSE_PAYLOAD_LEN_OFFSET)
        require(payloadLength in 0..(FoliaSharedProtocol.RESPONSE_FILE_SIZE - FoliaSharedProtocol.RESPONSE_PAYLOAD_OFFSET)) {
            "Invalid Folia chunk payload length: $payloadLength"
        }
        val payload = ByteArray(payloadLength)
        readBytes(slot.responseBuffer, FoliaSharedProtocol.RESPONSE_PAYLOAD_OFFSET, payload)

        DataInputStream(ByteArrayInputStream(payload)).use { input ->
            val heightmapCount = input.readInt()
            require(heightmapCount >= 0) { "Invalid heightmap count: $heightmapCount" }
            val heightmaps = ArrayList<HeightmapData>(heightmapCount)
            repeat(heightmapCount) {
                val typeId = input.readInt()
                val valueCount = input.readInt()
                require(valueCount >= 0) { "Invalid heightmap value count: $valueCount" }
                val values = LongArray(valueCount) { input.readLong() }
                heightmaps.add(HeightmapData(typeId, values))
            }

            val chunkDataLength = input.readInt()
            require(chunkDataLength >= 0) { "Invalid chunk data length: $chunkDataLength" }
            val chunkData = ByteArray(chunkDataLength)
            input.readFully(chunkData)
            val snapshot = decodeSnapshotPayload(input, minYForWorld(worldKey))

            val skyLightMask = readLongArray(input)
            val blockLightMask = readLongArray(input)
            val emptySkyLightMask = readLongArray(input)
            val emptyBlockLightMask = readLongArray(input)
            val skyLight = readByteArrayList(input)
            val blockLight = readByteArrayList(input)

            val generated = GeneratedChunk(
                heightmaps = heightmaps,
                chunkData = chunkData,
                skyLightMask = skyLightMask,
                blockLightMask = blockLightMask,
                emptySkyLightMask = emptySkyLightMask,
                emptyBlockLightMask = emptyBlockLightMask,
                skyLight = skyLight,
                blockLight = blockLight
            )

            return DecodedChunkPayload(
                chunk = generated,
                snapshot = snapshot
            )
        }
    }

    private fun decodeSnapshotPayload(input: DataInputStream, minY: Int): ChunkStateSnapshot {
        val sectionCount = input.readInt()
        require(sectionCount >= 0) { "Invalid snapshot section count: $sectionCount" }
        val sections = Array(sectionCount) {
            val bitsPerEntry = input.readInt()
            val paletteSize = input.readInt()
            require(paletteSize >= 0) { "Invalid snapshot palette size: $paletteSize" }
            val palette = IntArray(paletteSize) { input.readInt() }
            val storageSize = input.readInt()
            require(storageSize >= 0) { "Invalid snapshot storage size: $storageSize" }
            val storage = LongArray(storageSize) { input.readLong() }
            if (storage.isEmpty() && palette.size == 1) {
                SingleSectionState(palette[0])
            } else {
                PackedSectionState(
                    bitsPerEntry = bitsPerEntry.coerceAtLeast(1),
                    palette = palette,
                    data = storage
                )
            }
        }
        return ChunkStateSnapshot(minY = minY, sections = sections)
    }

    private fun scheduleSnapshotDecode(context: ChunkGenerationContext, chunkData: ByteArray) {
        val key = ChunkCacheKey(context.worldKey, context.chunkPos.x, context.chunkPos.z)
        val entry = chunkCache.computeIfAbsent(key) { ChunkCacheEntry() }
        if (entry.snapshot.get() != null) return

        val task = CompletableFuture.supplyAsync({
            runCatching { decodeChunkStateSnapshot(context.worldKey, chunkData) }
                .getOrElse { throwable ->
                    logger.warn("Failed to decode chunk block states from Folia payload; fallback to AIR base snapshot", throwable)
                    ChunkStateSnapshot(minY = minYForWorld(context.worldKey), sections = emptyArray())
                }
        }, snapshotDecodeExecutor)
        if (!entry.decodeTask.compareAndSet(null, task)) return

        task.whenComplete { snapshot, _ ->
            if (snapshot != null) {
                cacheDecodedChunk(context, snapshot)
            }
            entry.decodeTask.compareAndSet(task, null)
        }
    }

    private fun cacheSnapshotForContext(context: ChunkGenerationContext, chunkData: ByteArray) {
        val key = ChunkCacheKey(context.worldKey, context.chunkPos.x, context.chunkPos.z)
        val entry = chunkCache.computeIfAbsent(key) { ChunkCacheEntry() }
        if (entry.snapshot.get() != null) return
        if (retainedChunkKeys.contains(key)) {
            val snapshot = runCatching { decodeChunkStateSnapshot(context.worldKey, chunkData) }
                .getOrElse { throwable ->
                    logger.warn("Failed to decode retained chunk block states inline; falling back to async decode", throwable)
                    scheduleSnapshotDecode(context, chunkData)
                    return
                }
            cacheDecodedChunk(context, snapshot)
            return
        }
        scheduleSnapshotDecode(context, chunkData)
    }

    private fun cacheDecodedChunk(
        context: ChunkGenerationContext,
        snapshot: ChunkStateSnapshot
    ) {
        val key = ChunkCacheKey(context.worldKey, context.chunkPos.x, context.chunkPos.z)
        val entry = chunkCache.computeIfAbsent(key) { ChunkCacheEntry() }
        val previous = entry.snapshot.getAndSet(snapshot)
        if (previous == null) {
            decodedChunkOrder.addLast(key)
        }
        trimDecodedChunkCache()
    }

    private fun cacheDecodedChunk(
        context: ChunkGenerationContext,
        generated: GeneratedChunk,
        snapshot: ChunkStateSnapshot,
        light: ChunkLightSnapshot? = null
    ) {
        val key = ChunkCacheKey(context.worldKey, context.chunkPos.x, context.chunkPos.z)
        val entry = chunkCache.computeIfAbsent(key) { ChunkCacheEntry() }
        entry.generated.set(generated)
        val previous = entry.snapshot.getAndSet(snapshot)
        if (light != null) {
            entry.light.set(light)
        }
        if (previous == null) {
            decodedChunkOrder.addLast(key)
        }
        trimDecodedChunkCache()
    }

    private fun cachedGeneratedChunk(context: ChunkGenerationContext): GeneratedChunk? {
        val key = ChunkCacheKey(context.worldKey, context.chunkPos.x, context.chunkPos.z)
        return chunkCache[key]?.generated?.get()
    }

    private fun trimDecodedChunkCache() {
        maybeRefreshMaxCachedChunks()
        val maxChunks = maxCachedChunks.get().coerceAtLeast(1)
        var attempts = 0
        while (cachedSnapshotCount() > maxChunks) {
            val oldest = decodedChunkOrder.pollFirst() ?: break
            if (retainedChunkKeys.contains(oldest)) {
                decodedChunkOrder.addLast(oldest)
                attempts++
                if (attempts >= decodedChunkOrder.size.coerceAtLeast(1)) {
                    break
                }
                continue
            }
            attempts = 0
            val entry = chunkCache[oldest] ?: continue
            entry.generated.set(null)
            entry.snapshot.set(null)
            entry.light.set(null)
            if (entry.decodeTask.get() == null && entry.loadTask.get() == null && !retainedChunkKeys.contains(oldest)) {
                chunkCache.remove(oldest, entry)
            }
        }
    }

    private fun cachedSnapshotCount(): Int {
        var count = 0
        for (entry in chunkCache.values) {
            if (entry.snapshot.get() != null) {
                count++
            }
        }
        return count
    }

    private fun decodeChunkStateSnapshot(worldKey: String, chunkData: ByteArray): ChunkStateSnapshot {
        val minY = minYForWorld(worldKey)
        if (chunkData.isEmpty()) {
            return ChunkStateSnapshot(minY = minY, sections = emptyArray())
        }
        val reader = ByteArrayReader(chunkData)
        val sections = ArrayList<SectionState>(24)
        while (reader.hasRemaining()) {
            reader.readShort() // non-empty block count
            sections += readBlockSection(reader)
            skipPaletteContainer(reader, BIOME_SECTION_SIZE, BIOME_INDIRECT_PALETTE_MAX_BITS)
        }
        return ChunkStateSnapshot(
            minY = minY,
            sections = sections.toTypedArray()
        )
    }

    private fun minYForWorld(worldKey: String): Int {
        return when (worldKey) {
            "minecraft:overworld" -> OVERWORLD_MIN_Y
            "minecraft:the_nether", "minecraft:the_end" -> DEFAULT_MIN_Y
            else -> OVERWORLD_MIN_Y
        }
    }

    private fun decodeChunkLightSnapshot(worldKey: String, generated: GeneratedChunk): ChunkLightSnapshot? {
        if (generated.skyLightMask.isEmpty() && generated.blockLightMask.isEmpty()) return null
        val minSectionY = minYForWorld(worldKey) shr 4
        val sectionCount = maxOf(
            generated.skyLightMask.size * Long.SIZE_BITS,
            generated.blockLightMask.size * Long.SIZE_BITS,
            generated.emptySkyLightMask.size * Long.SIZE_BITS,
            generated.emptyBlockLightMask.size * Long.SIZE_BITS
        )
        if (sectionCount <= 0) return null
        return ChunkLightSnapshot(
            minSectionY = minSectionY,
            skySectionToArrayIndex = decodeLightSectionIndexes(
                presentMask = generated.skyLightMask,
                emptyMask = generated.emptySkyLightMask,
                sectionCount = sectionCount
            ),
            blockSectionToArrayIndex = decodeLightSectionIndexes(
                presentMask = generated.blockLightMask,
                emptyMask = generated.emptyBlockLightMask,
                sectionCount = sectionCount
            ),
            skyLightArrays = generated.skyLight,
            blockLightArrays = generated.blockLight
        )
    }

    private fun decodeLightSectionIndexes(
        presentMask: LongArray,
        emptyMask: LongArray,
        sectionCount: Int
    ): IntArray {
        val out = IntArray(sectionCount) { -1 }
        var dataArrayIndex = 0
        for (sectionIndex in 0 until sectionCount) {
            if (isMaskBitSet(emptyMask, sectionIndex)) {
                out[sectionIndex] = -1
                continue
            }
            if (!isMaskBitSet(presentMask, sectionIndex)) {
                continue
            }
            out[sectionIndex] = dataArrayIndex
            dataArrayIndex++
        }
        return out
    }

    private fun isMaskBitSet(mask: LongArray, bitIndex: Int): Boolean {
        val word = bitIndex ushr 6
        if (word !in mask.indices) return false
        val bit = bitIndex and 63
        return (mask[word] and (1L shl bit)) != 0L
    }

    private fun readBlockSection(reader: ByteArrayReader): SectionState {
        val bits = reader.readUnsignedByte()
        if (bits == 0) {
            return SingleSectionState(reader.readVarInt())
        }

        val palette = if (bits <= BLOCK_INDIRECT_PALETTE_MAX_BITS) {
            val size = reader.readVarInt()
            IntArray(size) { reader.readVarInt() }
        } else {
            null
        }

        val longCount = requiredPackedLongCount(BLOCK_SECTION_SIZE, bits)
        val packed = LongArray(longCount) { reader.readLong() }
        return PackedSectionState(bitsPerEntry = bits, palette = palette, data = packed)
    }

    private fun skipPaletteContainer(reader: ByteArrayReader, valueCount: Int, indirectPaletteMaxBits: Int) {
        val bits = reader.readUnsignedByte()
        if (bits == 0) {
            reader.readVarInt()
            return
        }

        if (bits <= indirectPaletteMaxBits) {
            val paletteSize = reader.readVarInt()
            repeat(paletteSize) {
                reader.readVarInt()
            }
        }

        val longCount = requiredPackedLongCount(valueCount, bits)
        repeat(longCount) {
            reader.readLong()
        }
    }

    private fun requiredPackedLongCount(valueCount: Int, bitsPerEntry: Int): Int {
        require(bitsPerEntry > 0) { "bitsPerEntry must be positive: $bitsPerEntry" }
        // Match Mojang SimpleBitStorage sizing:
        // valuesPerLong = floor(64 / bits), longCount = ceil(valueCount / valuesPerLong).
        // This is the format used by PalettedContainer.write(writeFixedSizeLongArray(...)).
        val valuesPerLong = 64 / bitsPerEntry
        require(valuesPerLong > 0) { "Invalid bitsPerEntry for packed storage: $bitsPerEntry" }
        return (valueCount + valuesPerLong - 1) / valuesPerLong
    }

    private fun decodeErrorPayload(slot: IpcSlot): String {
        val payloadLength = slot.responseBuffer.getInt(FoliaSharedProtocol.RESPONSE_PAYLOAD_LEN_OFFSET)
        if (payloadLength <= 0 || payloadLength > (FoliaSharedProtocol.RESPONSE_FILE_SIZE - FoliaSharedProtocol.RESPONSE_PAYLOAD_OFFSET)) {
            return "unknown"
        }
        val payload = ByteArray(payloadLength)
        readBytes(slot.responseBuffer, FoliaSharedProtocol.RESPONSE_PAYLOAD_OFFSET, payload)
        return runCatching { String(payload, StandardCharsets.UTF_8) }.getOrDefault("unknown")
    }

    private fun clearRequestResponseState(slot: IpcSlot) {
        slot.requestBuffer.putInt(FoliaSharedProtocol.REQUEST_STATE_OFFSET, FoliaSharedProtocol.REQUEST_STATE_EMPTY)
        slot.responseBuffer.putInt(FoliaSharedProtocol.RESPONSE_STATE_OFFSET, FoliaSharedProtocol.RESPONSE_STATE_EMPTY)
        slot.responseBuffer.putLong(FoliaSharedProtocol.RESPONSE_ID_OFFSET, 0L)
        slot.responseBuffer.putInt(FoliaSharedProtocol.RESPONSE_PAYLOAD_LEN_OFFSET, 0)
    }

    private fun readLongArray(input: DataInputStream): LongArray {
        val size = input.readInt()
        require(size >= 0) { "Invalid long array size: $size" }
        return LongArray(size) { input.readLong() }
    }

    private fun readByteArrayList(input: DataInputStream): List<ByteArray> {
        val count = input.readInt()
        require(count >= 0) { "Invalid byte array count: $count" }
        val list = ArrayList<ByteArray>(count)
        repeat(count) {
            val len = input.readInt()
            require(len >= 0) { "Invalid byte array length: $len" }
            val bytes = ByteArray(len)
            input.readFully(bytes)
            list.add(bytes)
        }
        return list
    }

    private class ByteArrayReader(
        private val source: ByteArray
    ) {
        private var index: Int = 0

        fun hasRemaining(): Boolean = index < source.size

        fun readUnsignedByte(): Int {
            ensureAvailable(1)
            return source[index++].toInt() and 0xFF
        }

        fun readShort(): Short {
            ensureAvailable(2)
            val out = ((source[index].toInt() and 0xFF) shl 8) or
                (source[index + 1].toInt() and 0xFF)
            index += 2
            return out.toShort()
        }

        fun readLong(): Long {
            ensureAvailable(8)
            var out = 0L
            repeat(8) {
                out = (out shl 8) or (source[index++].toLong() and 0xFFL)
            }
            return out
        }

        fun readVarInt(): Int {
            var value = 0
            var shift = 0
            while (shift < 32) {
                val next = readUnsignedByte()
                value = value or ((next and 0x7F) shl shift)
                if ((next and 0x80) == 0) return value
                shift += 7
            }
            throw IllegalArgumentException("VarInt too big in chunk section payload")
        }

        private fun ensureAvailable(length: Int) {
            if (index + length > source.size) {
                throw IllegalArgumentException(
                    "Chunk section payload underflow (need=$length, remaining=${source.size - index})"
                )
            }
        }
    }

    private fun writeBytes(buffer: MappedByteBuffer, offset: Int, source: ByteArray, length: Int) {
        val duplicate = buffer.duplicate().order(ByteOrder.BIG_ENDIAN)
        duplicate.position(offset)
        duplicate.put(source, 0, length)
    }

    private fun readBytes(buffer: MappedByteBuffer, offset: Int, destination: ByteArray) {
        val duplicate = buffer.duplicate().order(ByteOrder.BIG_ENDIAN)
        duplicate.position(offset)
        duplicate.get(destination)
    }

    private class ChunkBridgeTimeoutException(message: String) : IllegalStateException(message)
}
