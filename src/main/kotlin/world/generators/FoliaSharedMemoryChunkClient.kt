package org.macaroon3145.world.generators

import org.macaroon3145.world.ChunkGenerationContext
import org.macaroon3145.world.GeneratedChunk
import org.macaroon3145.world.HeightmapData
import org.macaroon3145.world.BlockStateLookupWorldGenerator
import org.macaroon3145.world.WorldGenerator
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
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicIntegerArray
import java.util.concurrent.atomic.AtomicLong
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
    private val decodedChunks = ConcurrentHashMap<ChunkCacheKey, ChunkStateSnapshot>()
    private val decodedChunkOrder = ConcurrentLinkedDeque<ChunkCacheKey>()
    private val decodeTasks = ConcurrentHashMap<ChunkCacheKey, CompletableFuture<ChunkStateSnapshot>>()
    private val maxCachedChunks: Int = configuredBlockStateCacheSize()
    private val decodeThreadId = AtomicInteger(1)
    private val snapshotDecodeExecutor = Executors.newFixedThreadPool(configuredSnapshotDecodeWorkers()) { runnable ->
        Thread(runnable, "aerogel-folia-snapshot-decode-${decodeThreadId.getAndIncrement()}").apply {
            isDaemon = true
            priority = Thread.NORM_PRIORITY
        }
    }

    @Volatile
    private var initialized = false

    private data class ChunkCacheKey(
        val worldKey: String,
        val chunkX: Int,
        val chunkZ: Int
    )

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
        ensureMapped()
        val slotIndex = acquireSlotIndex()
        val slot = slots[slotIndex]
        var cleared = false
        try {
            val requestId = nextRequestId.getAndIncrement()
            writeRequest(slot, requestId, context)

            val timeoutMillis = configuredTimeoutMillis()
            val timeoutNanos = if (timeoutMillis <= 0L) Long.MAX_VALUE else timeoutMillis * 1_000_000L
            val startNanos = System.nanoTime()
            var nextWarnAt = startNanos + 15_000_000_000L

            while (true) {
                val state = slot.responseBuffer.getInt(FoliaSharedProtocol.RESPONSE_STATE_OFFSET)
                val responseId = slot.responseBuffer.getLong(FoliaSharedProtocol.RESPONSE_ID_OFFSET)
                if (responseId == requestId) {
                    when (state) {
                        FoliaSharedProtocol.RESPONSE_STATE_SUCCESS -> {
                            val decoded = decodeSuccessPayload(slot)
                            scheduleSnapshotDecode(context, decoded.chunkData)
                            clearRequestResponseState(slot)
                            cleared = true
                            return decoded.chunk
                        }
                        FoliaSharedProtocol.RESPONSE_STATE_ERROR -> {
                            val message = decodeErrorPayload(slot)
                            clearRequestResponseState(slot)
                            cleared = true
                            throw IllegalStateException("Folia chunk bridge error: $message")
                        }
                    }
                }
                val now = System.nanoTime()
                if (now >= nextWarnAt) {
                    logger.warn(
                        "Folia chunk request waiting: slot={} requestId={} world={} chunk=({}, {}) elapsedMs={}",
                        slot.index,
                        requestId,
                        context.worldKey,
                        context.chunkPos.x,
                        context.chunkPos.z,
                        (now - startNanos) / 1_000_000L
                    )
                    nextWarnAt = now + 15_000_000_000L
                }
                if (timeoutNanos != Long.MAX_VALUE && now - startNanos >= timeoutNanos) break
                LockSupport.parkNanos(200_000L)
            }

            clearRequestResponseState(slot)
            cleared = true
            throw IllegalStateException(
                "Folia chunk bridge timeout for ${context.worldKey} ${context.chunkPos.x},${context.chunkPos.z}"
            )
        } finally {
            if (!cleared) {
                runCatching { clearRequestResponseState(slot) }
            }
            releaseSlotIndex(slotIndex)
        }
    }

    fun blockStateAt(worldKey: String, x: Int, y: Int, z: Int): Int {
        val chunkKey = ChunkCacheKey(worldKey, x shr 4, z shr 4)
        var snapshot = decodedChunks[chunkKey]
        if (snapshot == null) {
            val task = decodeTasks[chunkKey]
            if (task != null) {
                snapshot = runCatching { task.join() }.getOrNull()
            }
        }
        snapshot ?: return AIR_STATE_ID
        return snapshot.blockStateAt(x, y, z)
    }

    private fun configuredTimeoutMillis(): Long {
        return System.getProperty("aerogel.chunk.ipc.timeout-ms")
            ?.toLongOrNull()
            ?.coerceAtLeast(0L)
            ?: 0L
    }

    private fun configuredBlockStateCacheSize(): Int {
        return System.getProperty("aerogel.chunk.blockstate-cache.max-chunks")
            ?.toIntOrNull()
            ?.coerceAtLeast(1)
            ?: 4096
    }

    private fun configuredSnapshotDecodeWorkers(): Int {
        return System.getProperty("aerogel.chunk.blockstate-decode-workers")
            ?.toIntOrNull()
            ?.coerceAtLeast(1)
            ?: 4
    }

    private fun ensureMapped() {
        if (initialized) return
        synchronized(this) {
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
            val process = ProcessBuilder("nproc").redirectErrorStream(true).start()
            val out = process.inputStream.bufferedReader().use { it.readText().trim() }
            if (process.waitFor() == 0) out.toIntOrNull() ?: 0 else 0
        }.getOrDefault(0)
        val windowsEnv = System.getenv("NUMBER_OF_PROCESSORS")?.toIntOrNull() ?: 0
        return maxOf(linuxCpuInfo, nprocValue, windowsEnv, 1)
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

    private data class DecodedChunkPayload(
        val chunk: GeneratedChunk,
        val chunkData: ByteArray
    )

    private fun decodeSuccessPayload(slot: IpcSlot): DecodedChunkPayload {
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
                chunkData = chunkData
            )
        }
    }

    private fun scheduleSnapshotDecode(context: ChunkGenerationContext, chunkData: ByteArray) {
        val key = ChunkCacheKey(context.worldKey, context.chunkPos.x, context.chunkPos.z)
        if (decodedChunks.containsKey(key)) return

        val task = CompletableFuture.supplyAsync({
            runCatching { decodeChunkStateSnapshot(context.worldKey, chunkData) }
                .getOrElse { throwable ->
                    logger.warn("Failed to decode chunk block states from Folia payload; fallback to AIR base snapshot", throwable)
                    ChunkStateSnapshot(minY = minYForWorld(context.worldKey), sections = emptyArray())
                }
        }, snapshotDecodeExecutor)
        val existing = decodeTasks.putIfAbsent(key, task)
        if (existing != null) return

        task.whenComplete { snapshot, _ ->
            if (snapshot != null) {
                cacheDecodedChunk(context, snapshot)
            }
            decodeTasks.remove(key, task)
        }
    }

    private fun cacheDecodedChunk(context: ChunkGenerationContext, snapshot: ChunkStateSnapshot) {
        val key = ChunkCacheKey(context.worldKey, context.chunkPos.x, context.chunkPos.z)
        val previous = decodedChunks.put(key, snapshot)
        if (previous == null) {
            decodedChunkOrder.addLast(key)
        }

        while (decodedChunks.size > maxCachedChunks) {
            val oldest = decodedChunkOrder.pollFirst() ?: break
            decodedChunks.remove(oldest)
        }
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
}
