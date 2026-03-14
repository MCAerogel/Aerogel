package org.macaroon3145.world.generators

import chunkfuture.ChunkFutureHook
import chunkfuture.ChunkFutureRequest
import chunkfuture.ChunkFutureResult
import chunkfuture.ChunkFutureService
import chunkfuture.WorldCreateRequest
import chunkfuture.ported.runtime.StandaloneBiome
import chunkfuture.ported.runtime.StandaloneBlockId
import chunkfuture.ported.runtime.StandaloneChunkData
import chunkfuture.ported.runtime.StandaloneRuntimeIds
import chunkfuture.ported.runtime.StandaloneVanillaWorldFactory
import chunkfuture.ported.runtime.StandaloneWorld
import chunkfuture.ported.runtime.StandaloneWorldAdapter
import chunkfuture.ported.runtime.StandaloneWorldSpec
import chunkfuture.ported.runtime.StandaloneWorldType
import chunkfuture.ported.runtime.StandaloneServer
import org.macaroon3145.network.NetworkUtils
import org.macaroon3145.network.codec.BlockStateRegistry
import org.macaroon3145.network.codec.RegistryCodec
import org.macaroon3145.world.AsyncWorldGenerator
import org.macaroon3145.world.BlockStateLookupWorldGenerator
import org.macaroon3145.world.ChunkGenerationContext
import org.macaroon3145.world.ChunkPos
import org.macaroon3145.world.GeneratedChunk
import org.macaroon3145.world.HeightmapData
import org.macaroon3145.world.LoadedChunkCacheWorldGenerator
import org.macaroon3145.world.RetainedChunkSetWorldGenerator
import org.macaroon3145.world.SpawnPoint
import org.slf4j.LoggerFactory
import java.io.ByteArrayOutputStream
import java.io.DataOutputStream
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.ceil
import kotlin.math.max

class ChunkFutureConnectedWorldGenerator(
    private val worldKey: String,
    seed: Long,
) : AsyncWorldGenerator, BlockStateLookupWorldGenerator, LoadedChunkCacheWorldGenerator, RetainedChunkSetWorldGenerator {
    private data class LookupCacheEntry(
        val chunk: StandaloneChunkData,
        @Volatile var expiresAtNanos: Long
    )

    private val logger = LoggerFactory.getLogger(ChunkFutureConnectedWorldGenerator::class.java)
    private val service = ChunkFutureService.createDefault()
    private val worldSeed = seed
    private val chunkCache = ConcurrentHashMap<ChunkPos, StandaloneChunkData>()
    private val pinnedChunkCache = ConcurrentHashMap<ChunkPos, StandaloneChunkData>()
    private val retainedChunkSnapshot = ConcurrentHashMap.newKeySet<ChunkPos>()
    private val lookupCache = ConcurrentHashMap<ChunkPos, LookupCacheEntry>()
    private val lookupPrefetchInFlight = ConcurrentHashMap<ChunkPos, CompletableFuture<StandaloneChunkData>>()
    private val lookupMaintenanceCounter = AtomicLong(0L)
    private val retainedChunkRefCounts = ConcurrentHashMap<ChunkPos, AtomicInteger>()
    private val runtimeIds = StandaloneRuntimeIds(
        blockStateIdLookup = { blockKey ->
            BlockStateRegistry.stateIdForBlockOrStateKey(blockKey) ?: AIR_STATE_ID
        },
        biomeIdLookup = { biomeKey ->
            RegistryCodec.entryIndex("minecraft:worldgen/biome", biomeKey) ?: BIOME_PLAINS
        }
    )
    private val serializedToRuntimeStateId = IntArray(256) { serialized ->
        val block = runCatching { StandaloneBlockId.bySerializedId(serialized) }.getOrNull() ?: StandaloneBlockId.AIR
        blockStateIdFor(block)
    }
    private val runtimeWorldId = "$worldKey#${seed.toULong().toString(16)}#${INSTANCE_SEQ.incrementAndGet()}"
    private val world: StandaloneWorld
    private val standaloneHeightRange: IntRange
    private val expectedWorldType: StandaloneWorldType

    init {
        val server = StandaloneServer()
        val worldSpec = StandaloneWorldSpec(seed = seed, type = worldTypeForKey(worldKey))
        world = StandaloneVanillaWorldFactory().create(server, WorldCreateRequest(runtimeWorldId, worldSpec))
        expectedWorldType = worldSpec.type
        standaloneHeightRange = standaloneHeightRangeFor(worldSpec.type)

        service.addHook(
            object : ChunkFutureHook {
                override fun failure(request: ChunkFutureRequest, throwable: Throwable) {
                    logger.warn(
                        "ChunkFuture failure world={} chunk=({}, {}) status={} create={}",
                        request.worldId,
                        request.chunkX,
                        request.chunkZ,
                        request.statusKey,
                        request.create,
                        throwable
                    )
                }

                override fun success(request: ChunkFutureRequest, result: ChunkFutureResult) {
                    if (result.resolvedChunk == null) {
                        logger.warn(
                            "ChunkFuture returned empty chunk world={} chunk=({}, {}) status={}",
                            request.worldId,
                            request.chunkX,
                            request.chunkZ,
                            request.statusKey
                        )
                    }
                }
            }
        )
        service.registerWorld(world, STANDALONE_WORLD_ADAPTER)
    }

    override fun generateChunk(context: ChunkGenerationContext): GeneratedChunk {
        return try {
            generateChunkAsync(context).join()
        } catch (error: CompletionException) {
            val cause = error.cause ?: error
            when (cause) {
                is RuntimeException -> throw cause
                is Error -> throw cause
                else -> throw RuntimeException(cause)
            }
        }
    }

    private fun requestChunkData(context: ChunkGenerationContext): CompletableFuture<StandaloneChunkData> {
        return requestChunkDataCpu(context)
    }

    private fun requestChunkDataCpu(context: ChunkGenerationContext): CompletableFuture<StandaloneChunkData> {
        return service.request(
            worldId = runtimeWorldId,
            chunkX = context.chunkPos.x,
            chunkZ = context.chunkPos.z,
            statusKey = "FULL",
            create = true
        ).thenApply { result -> resolveStandaloneChunk(context, result) }
    }

    private fun resolveStandaloneChunk(context: ChunkGenerationContext, result: ChunkFutureResult): StandaloneChunkData {
        val chunk = (result.resolvedChunk as? StandaloneChunkData)
            ?: throw IllegalStateException(
                "ChunkFuture produced no standalone chunk for ${this.worldKey} ${context.chunkPos.x},${context.chunkPos.z}"
            )
        return resolveStandaloneChunk(context, chunk)
    }

    private fun resolveStandaloneChunk(context: ChunkGenerationContext, chunk: StandaloneChunkData): StandaloneChunkData {
        check(chunk.worldType == expectedWorldType) {
            "Chunk worldType mismatch: expected=$expectedWorldType actual=${chunk.worldType} world=$worldKey chunk=${context.chunkPos.x},${context.chunkPos.z}"
        }
        check(chunk.minY == standaloneHeightRange.first && chunk.maxY == standaloneHeightRange.last) {
            "Chunk height mismatch: expected=${standaloneHeightRange.first}..${standaloneHeightRange.last} actual=${chunk.minY}..${chunk.maxY} world=$worldKey chunk=${context.chunkPos.x},${context.chunkPos.z}"
        }
        cacheChunk(context.chunkPos, chunk)
        return chunk
    }

    override fun blockStateAt(worldKey: String, x: Int, y: Int, z: Int): Int {
        if (worldKey != this.worldKey) return AIR_STATE_ID
        if (y !in standaloneHeightRange) return AIR_STATE_ID
        val chunkPos = ChunkPos(x shr 4, z shr 4)
        // Never synchronously generate/load chunks from random gameplay queries.
        // Blocking here can run full worldgen on Netty event-loop threads.
        var chunk = cachedChunk(chunkPos)
        if (chunk == null && isChunkRetained(chunkPos)) {
            // Correctness path: for actively retained chunks, do one bounded blocking
            // fetch to avoid transient AIR reads during interaction checks.
            chunk = fetchRetainedChunkBlocking(chunkPos)
        }
        if (chunk == null) {
            scheduleLookupPrefetch(chunkPos)
            return AIR_STATE_ID
        }
        return blockStateId(chunk, x and 15, y, z and 15)
    }

    override fun spawnPoint(worldKey: String): SpawnPoint? {
        if (worldKey != this.worldKey) return null
        val spawn = world.spawnPoint
        return SpawnPoint(
            x = spawn.x.toDouble(),
            y = spawn.y.toDouble(),
            z = spawn.z.toDouble()
        )
    }

    override fun blockStateAtIfCached(worldKey: String, x: Int, y: Int, z: Int): Int? {
        if (worldKey != this.worldKey) return null
        if (y !in standaloneHeightRange) return AIR_STATE_ID
        val chunkPos = ChunkPos(x shr 4, z shr 4)
        val chunk = cachedChunk(chunkPos)
        if (chunk == null) {
            scheduleLookupPrefetch(chunkPos)
            return null
        }
        return blockStateId(chunk, x and 15, y, z and 15)
    }

    override fun rawBrightnessAt(worldKey: String, x: Int, y: Int, z: Int): Int {
        if (worldKey != this.worldKey) return 0
        val chunkPos = ChunkPos(x shr 4, z shr 4)
        val chunk = cachedChunk(chunkPos)
        if (chunk == null) {
            scheduleLookupPrefetch(chunkPos)
            return 0
        }
        val surface = chunk.surfaceHeightAt(x and 15, z and 15) ?: chunk.surfaceHeight
        return if (y > surface) 15 else 0
    }

    override fun rawBrightnessAtIfCached(worldKey: String, x: Int, y: Int, z: Int): Int? {
        if (worldKey != this.worldKey) return null
        val chunkPos = ChunkPos(x shr 4, z shr 4)
        val chunk = cachedChunk(chunkPos)
        if (chunk == null) {
            scheduleLookupPrefetch(chunkPos)
            return null
        }
        val surface = chunk.surfaceHeightAt(x and 15, z and 15) ?: chunk.surfaceHeight
        return if (y > surface) 15 else 0
    }

    override fun generateChunkAsync(context: ChunkGenerationContext): CompletableFuture<GeneratedChunk> {
        return requestChunkData(context).thenApply(::toGeneratedChunk)
    }

    override fun retainLoadedChunk(chunkPos: ChunkPos) {
        retainedChunkRefCounts.compute(chunkPos) { _, counter ->
            val out = counter ?: AtomicInteger(0)
            out.incrementAndGet()
            out
        }
        chunkCache[chunkPos]?.let { pinnedChunkCache[chunkPos] = it }
        lookupCache[chunkPos]?.let { pinnedChunkCache.putIfAbsent(chunkPos, it.chunk) }
    }

    override fun releaseLoadedChunk(chunkPos: ChunkPos) {
        val remaining = retainedChunkRefCounts.computeIfPresent(chunkPos) { _, counter ->
            if (counter.decrementAndGet() <= 0) null else counter
        }
        if (remaining == null) {
            pinnedChunkCache.remove(chunkPos)
            chunkCache.remove(chunkPos)
        }
    }

    override fun syncRetainedLoadedChunks(chunks: Set<ChunkPos>) {
        retainedChunkSnapshot.clear()
        retainedChunkSnapshot.addAll(chunks)
        for (chunkPos in chunks) {
            chunkCache[chunkPos]?.let { pinnedChunkCache[chunkPos] = it }
            lookupCache[chunkPos]?.let { pinnedChunkCache.putIfAbsent(chunkPos, it.chunk) }
        }
        val drop = ArrayList<ChunkPos>()
        for (chunkPos in pinnedChunkCache.keys) {
            if (!isChunkRetained(chunkPos)) {
                drop += chunkPos
            }
        }
        for (chunkPos in drop) {
            pinnedChunkCache.remove(chunkPos)
        }
    }

    private fun cacheChunk(chunkPos: ChunkPos, chunk: StandaloneChunkData) {
        cacheLookupChunk(chunkPos, chunk)
        chunkCache[chunkPos] = chunk
        if (isChunkRetained(chunkPos)) {
            pinnedChunkCache[chunkPos] = chunk
        } else {
            pinnedChunkCache.remove(chunkPos)
            chunkCache.remove(chunkPos, chunk)
        }
    }

    private fun cachedChunk(chunkPos: ChunkPos): StandaloneChunkData? {
        val pinned = pinnedChunkCache[chunkPos]
        if (pinned != null) {
            cacheLookupChunk(chunkPos, pinned)
            chunkCache[chunkPos] = pinned
            return pinned
        }

        val direct = chunkCache[chunkPos]
        if (direct != null) {
            cacheLookupChunk(chunkPos, direct)
            return direct
        }

        val now = System.nanoTime()
        val lookup = lookupCache[chunkPos] ?: return null
        // Retained (actively loaded/streamed) chunks must stay cacheable even if TTL elapsed.
        if (lookup.expiresAtNanos <= now && !isChunkRetained(chunkPos)) {
            lookupCache.remove(chunkPos, lookup)
            return null
        }
        lookup.expiresAtNanos = now + lookupTtlNanos
        maybeMaintainLookupCache()
        return lookup.chunk
    }

    private fun isChunkRetained(chunkPos: ChunkPos): Boolean {
        return (retainedChunkRefCounts[chunkPos]?.get()?.let { it > 0 } == true) ||
            retainedChunkSnapshot.contains(chunkPos)
    }

    private fun cacheLookupChunk(chunkPos: ChunkPos, chunk: StandaloneChunkData) {
        val now = System.nanoTime()
        lookupCache[chunkPos] = LookupCacheEntry(
            chunk = chunk,
            expiresAtNanos = now + lookupTtlNanos
        )
        maybeMaintainLookupCache(force = lookupCache.size > lookupMaxEntries * 2)
    }

    private fun scheduleLookupPrefetch(chunkPos: ChunkPos) {
        // Prevent background worldgen amplification from random miss lookups outside
        // actively retained (streamed/loaded) chunk footprints.
        if (!isChunkRetained(chunkPos)) return
        if (lookupCache.containsKey(chunkPos)) return
        val created = CompletableFuture<StandaloneChunkData>()
        val existing = lookupPrefetchInFlight.putIfAbsent(chunkPos, created)
        if (existing != null) return

        service.request(
            worldId = runtimeWorldId,
            chunkX = chunkPos.x,
            chunkZ = chunkPos.z,
            statusKey = "FULL",
            create = true
        ).thenApply { result ->
            resolveStandaloneChunk(
                ChunkGenerationContext(
                    worldKey = worldKey,
                    seed = worldSeed,
                    chunkPos = chunkPos,
                    isCancelled = { false }
                ),
                result
            )
        }.whenComplete { chunk, throwable ->
            try {
                if (throwable == null && chunk != null) {
                    cacheLookupChunk(chunkPos, chunk)
                    created.complete(chunk)
                } else if (throwable != null) {
                    created.completeExceptionally(throwable)
                }
            } finally {
                lookupPrefetchInFlight.remove(chunkPos, created)
            }
        }
    }

    private fun fetchRetainedChunkBlocking(chunkPos: ChunkPos): StandaloneChunkData? {
        val context = ChunkGenerationContext(
            worldKey = worldKey,
            seed = worldSeed,
            chunkPos = chunkPos,
            isCancelled = { false }
        )
        return runCatching {
            service.request(
                worldId = runtimeWorldId,
                chunkX = chunkPos.x,
                chunkZ = chunkPos.z,
                statusKey = "FULL",
                create = true
            )
                .thenApply { result -> resolveStandaloneChunk(context, result) }
                .get(BLOCK_LOOKUP_BLOCKING_TIMEOUT_MS, TimeUnit.MILLISECONDS)
        }.getOrNull()
    }

    private fun maybeMaintainLookupCache(force: Boolean = false) {
        val tick = lookupMaintenanceCounter.incrementAndGet()
        if (!force && (tick and 1023L) != 0L) return
        val now = System.nanoTime()
        var oversize = (lookupCache.size - lookupMaxEntries).coerceAtLeast(0)
        for ((key, entry) in lookupCache) {
            if (entry.expiresAtNanos <= now && !isChunkRetained(key)) {
                lookupCache.remove(key, entry)
                continue
            }
            if (oversize > 0 && retainedChunkRefCounts[key]?.get()?.let { it > 0 } != true) {
                if (lookupCache.remove(key, entry)) {
                    oversize--
                }
            }
        }
    }

    private fun toGeneratedChunk(chunk: StandaloneChunkData): GeneratedChunk {
        val lightPayload = buildChunkLightPayload(chunk)
        return GeneratedChunk(
            heightmaps = buildHeightmaps(chunk),
            chunkData = buildChunkData(chunk),
            skyLightMask = lightPayload.skyLightMask,
            blockLightMask = lightPayload.blockLightMask,
            emptySkyLightMask = lightPayload.emptySkyLightMask,
            emptyBlockLightMask = lightPayload.emptyBlockLightMask,
            skyLight = lightPayload.skyLight,
            blockLight = lightPayload.blockLight
        )
    }

    private data class ChunkLightPayload(
        val skyLightMask: LongArray,
        val blockLightMask: LongArray,
        val emptySkyLightMask: LongArray,
        val emptyBlockLightMask: LongArray,
        val skyLight: List<ByteArray>,
        val blockLight: List<ByteArray>
    )

    private fun buildChunkLightPayload(chunk: StandaloneChunkData): ChunkLightPayload {
        val sectionCount = ((chunk.maxY - chunk.minY + 1) / SECTION_SIZE).coerceAtLeast(0)
        if (sectionCount <= 0) {
            return ChunkLightPayload(
                skyLightMask = LongArray(0),
                blockLightMask = LongArray(0),
                emptySkyLightMask = LongArray(0),
                emptyBlockLightMask = LongArray(0),
                skyLight = emptyList(),
                blockLight = emptyList()
            )
        }

        val sky = buildLightSeries(sectionCount, chunk.skyLightSections)
        val block = buildLightSeries(sectionCount, chunk.blockLightSections)
        return ChunkLightPayload(
            skyLightMask = sky.first,
            blockLightMask = block.first,
            emptySkyLightMask = LongArray(0),
            emptyBlockLightMask = LongArray(0),
            skyLight = sky.second,
            blockLight = block.second
        )
    }

    private fun buildLightSeries(sectionCount: Int, sections: List<ByteArray>?): Pair<LongArray, List<ByteArray>> {
        if (sections.isNullOrEmpty() || sectionCount <= 0) return LongArray(0) to emptyList()

        // Vanilla light packet indexes include one section below min and one above max.
        val protocolSectionCount = sectionCount + 2
        val mask = LongArray((protocolSectionCount + Long.SIZE_BITS - 1) / Long.SIZE_BITS)
        val updates = ArrayList<ByteArray>(minOf(sectionCount, sections.size))
        val limit = minOf(sectionCount, sections.size)
        for (sectionIndex in 0 until limit) {
            val bytes = sections[sectionIndex]
            if (bytes.isEmpty()) continue
            val protocolSectionIndex = sectionIndex + 1
            setMaskBit(mask, protocolSectionIndex)
            updates += bytes.copyOf()
        }
        return mask to updates
    }

    private fun setMaskBit(mask: LongArray, bitIndex: Int) {
        if (bitIndex < 0) return
        val wordIndex = bitIndex ushr 6
        if (wordIndex !in mask.indices) return
        mask[wordIndex] = mask[wordIndex] or (1L shl (bitIndex and 63))
    }

    private fun blockStateId(chunk: StandaloneChunkData, localX: Int, y: Int, localZ: Int): Int {
        if (y !in chunk.minY..chunk.maxY) return AIR_STATE_ID
        val sections = chunk.sections ?: return AIR_STATE_ID
        val sectionIndex = Math.floorDiv(y - chunk.minY, SECTION_SIZE)
        val section = sections.getOrNull(sectionIndex) ?: return AIR_STATE_ID
        val serialized = section.blockStateId(localX, Math.floorMod(y, SECTION_SIZE), localZ)
        return runtimeStateIdFromSerialized(serialized)
    }

    private fun buildChunkData(chunk: StandaloneChunkData): ByteArray {
        val stream = ByteArrayOutputStream()
        val out = DataOutputStream(stream)
        val plainsBiome = biomeIdFor(chunk.biome)
        val sections = chunk.sections
        val chunkSectionOffset = Math.floorDiv(chunk.minY - WORLD_MIN_Y, SECTION_SIZE)

        repeat(WORLD_SECTION_COUNT) { sectionIndex ->
            val section = sections?.getOrNull(sectionIndex - chunkSectionOffset)
            if (section == null) {
                out.writeShort(0)
                writeSingleValuePalettedContainer(stream, AIR_STATE_ID)
                writeSingleValuePalettedContainer(stream, plainsBiome)
                return@repeat
            }

            val rawStateIds = section.rawBlockStateIdsUnsafe()
            val palette = LinkedHashMap<Int, Int>()
            val stateIndices = IntArray(SECTION_BLOCK_COUNT)
            var nonAirCount = 0

            val limit = minOf(rawStateIds.size, SECTION_BLOCK_COUNT)
            for (idx in 0 until limit) {
                val stateId = runtimeStateIdFromSerialized(rawStateIds[idx].toInt() and 0xFF)
                if (stateId != AIR_STATE_ID) nonAirCount++
                val paletteIndex = palette.computeIfAbsent(stateId) { palette.size }
                stateIndices[idx] = paletteIndex
            }
            for (idx in limit until SECTION_BLOCK_COUNT) {
                val paletteIndex = palette.computeIfAbsent(AIR_STATE_ID) { palette.size }
                stateIndices[idx] = paletteIndex
            }

            out.writeShort(nonAirCount)
            writePalettedContainer(stream, palette, stateIndices)
            writeSingleValuePalettedContainer(stream, plainsBiome)
        }
        return stream.toByteArray()
    }

    private fun writePalettedContainer(
        stream: ByteArrayOutputStream,
        palette: LinkedHashMap<Int, Int>,
        stateIndices: IntArray,
    ) {
        val out = DataOutputStream(stream)
        if (palette.size <= 1) {
            val value = palette.keys.firstOrNull() ?: AIR_STATE_ID
            writeSingleValuePalettedContainer(stream, value)
            return
        }

        val maxIndirectBits = 8
        if (palette.size <= (1 shl maxIndirectBits)) {
            val bitsPerEntry = max(4, ceilLog2(palette.size))
            val packed = packPalettedValues(stateIndices, bitsPerEntry)
            out.writeByte(bitsPerEntry)
            NetworkUtils.writeVarInt(stream, palette.size)
            palette.keys.forEach { NetworkUtils.writeVarInt(stream, it) }
            // PalettedContainer writes fixed-size long arrays without a length prefix.
            packed.forEach { out.writeLong(it) }
            return
        }

        // Direct palette mode: values are global block-state ids, no local palette payload.
        val byPaletteIndex = IntArray(palette.size)
        for ((stateId, paletteIndex) in palette) {
            if (paletteIndex in byPaletteIndex.indices) {
                byPaletteIndex[paletteIndex] = stateId
            }
        }
        val directStateIds = IntArray(stateIndices.size)
        var maxStateId = 0
        for (i in stateIndices.indices) {
            val stateId = byPaletteIndex[stateIndices[i]]
            directStateIds[i] = stateId
            if (stateId > maxStateId) maxStateId = stateId
        }
        val bitsPerEntry = max(maxIndirectBits + 1, ceilLog2(maxStateId + 1))
        val packed = packPalettedValues(directStateIds, bitsPerEntry)
        out.writeByte(bitsPerEntry)
        packed.forEach { out.writeLong(it) }
    }

    private fun packPalettedValues(values: IntArray, bitsPerEntry: Int): LongArray {
        val valuesPerLong = 64 / bitsPerEntry
        val longCount = ceil(values.size.toDouble() / valuesPerLong.toDouble()).toInt()
        val packed = LongArray(longCount)
        val mask = (1L shl bitsPerEntry) - 1L
        for (index in values.indices) {
            val dataIndex = index / valuesPerLong
            val bitOffset = (index - dataIndex * valuesPerLong) * bitsPerEntry
            val value = (values[index].toLong() and mask) shl bitOffset
            packed[dataIndex] = packed[dataIndex] or value
        }
        return packed
    }

    private fun writeSingleValuePalettedContainer(stream: ByteArrayOutputStream, valueId: Int) {
        val out = DataOutputStream(stream)
        out.writeByte(0)
        NetworkUtils.writeVarInt(stream, valueId)
    }

    private fun buildHeightmaps(chunk: StandaloneChunkData): List<HeightmapData> {
        val values = LongArray(HEIGHTMAP_LONG_COUNT)
        val heightMap = chunk.heightMap
        if (heightMap != null && heightMap.size >= CHUNK_COLUMN_COUNT) {
            for (index in 0 until CHUNK_COLUMN_COUNT) {
                val surface = heightMap[index]
                val yPlusOne = (surface + 1).coerceIn(WORLD_MIN_Y, WORLD_MAX_Y + 1)
                val relative = (yPlusOne - WORLD_MIN_Y).coerceAtLeast(0)
                packHeightmap(values, index, relative)
            }
        } else {
            for (z in 0 until SECTION_SIZE) {
                for (x in 0 until SECTION_SIZE) {
                    val index = (z shl 4) or x
                    val surface = chunk.surfaceHeightAt(x, z) ?: chunk.surfaceHeight
                    val yPlusOne = (surface + 1).coerceIn(WORLD_MIN_Y, WORLD_MAX_Y + 1)
                    val relative = (yPlusOne - WORLD_MIN_Y).coerceAtLeast(0)
                    packHeightmap(values, index, relative)
                }
            }
        }
        return listOf(
            HeightmapData(typeId = MOTION_BLOCKING_HEIGHTMAP_ID, values = values.copyOf()),
            HeightmapData(typeId = MOTION_BLOCKING_NO_LEAVES_HEIGHTMAP_ID, values = values.copyOf())
        )
    }

    private fun packHeightmap(target: LongArray, index: Int, value: Int) {
        val dataIndex = index / HEIGHTMAP_VALUES_PER_LONG
        val bitOffset = (index - dataIndex * HEIGHTMAP_VALUES_PER_LONG) * HEIGHTMAP_BITS
        target[dataIndex] = target[dataIndex] or ((value.toLong() and HEIGHTMAP_MASK) shl bitOffset)
    }

    private fun blockStateIdFor(block: StandaloneBlockId): Int {
        return runtimeIds.blockStateIdFor(block)
    }

    private fun biomeIdFor(biome: StandaloneBiome): Int {
        return runtimeIds.biomeIdFor(biome)
    }

    private fun runtimeStateIdFromSerialized(serialized: Int): Int {
        return if (serialized in 0..255) serializedToRuntimeStateId[serialized] else AIR_STATE_ID
    }

    private fun worldTypeForKey(key: String): StandaloneWorldType {
        return when (key) {
            "minecraft:the_nether" -> StandaloneWorldType.NETHER
            "minecraft:the_end" -> StandaloneWorldType.END
            else -> StandaloneWorldType.OVERWORLD
        }
    }

    private fun standaloneHeightRangeFor(type: StandaloneWorldType): IntRange {
        return when (type) {
            StandaloneWorldType.OVERWORLD,
            StandaloneWorldType.LARGE_BIOMES,
            StandaloneWorldType.AMPLIFIED -> -64..319
            StandaloneWorldType.NETHER,
            StandaloneWorldType.END -> 0..127
            StandaloneWorldType.CAVES -> -64..127
            StandaloneWorldType.FLOATING_ISLANDS -> 0..255
        }
    }

    private fun ceilLog2(value: Int): Int {
        var bits = 0
        var v = value - 1
        while (v > 0) {
            bits++
            v = v ushr 1
        }
        return bits.coerceAtLeast(1)
    }

    companion object {
        private val INSTANCE_SEQ = AtomicLong(0L)
        private val STANDALONE_WORLD_ADAPTER = StandaloneWorldAdapter()
        private val lookupTtlNanos: Long = TimeUnit.SECONDS.toNanos(
            System.getProperty("aerogel.chunk.lookup-cache-ttl-seconds")
                ?.toLongOrNull()
                ?.coerceIn(5L, 600L)
                ?: 45L
        )
        private val lookupMaxEntries: Int = (
            System.getProperty("aerogel.chunk.lookup-cache-max-entries")
                ?.toIntOrNull()
                ?.coerceAtLeast(2_048)
                ?: 65_536
            )
        private val BLOCK_LOOKUP_BLOCKING_TIMEOUT_MS: Long = (
            System.getProperty("aerogel.chunk.block-lookup-blocking-timeout-ms")
                ?.toLongOrNull()
                ?.coerceIn(1L, 1_000L)
                ?: 50L
            )
        private const val WORLD_MIN_Y = -64
        private const val WORLD_MAX_Y = 319
        private const val SECTION_SIZE = 16
        private const val CHUNK_COLUMN_COUNT = SECTION_SIZE * SECTION_SIZE
        private const val WORLD_SECTION_COUNT = (WORLD_MAX_Y - WORLD_MIN_Y + 1) / SECTION_SIZE
        private const val SECTION_BLOCK_COUNT = SECTION_SIZE * SECTION_SIZE * SECTION_SIZE
        private const val AIR_STATE_ID = 0
        private const val MOTION_BLOCKING_HEIGHTMAP_ID = 4
        private const val MOTION_BLOCKING_NO_LEAVES_HEIGHTMAP_ID = 5

        private const val HEIGHTMAP_BITS = 9
        private const val HEIGHTMAP_VALUES_PER_LONG = 64 / HEIGHTMAP_BITS
        private const val HEIGHTMAP_LONG_COUNT = ((256 + HEIGHTMAP_VALUES_PER_LONG - 1) / HEIGHTMAP_VALUES_PER_LONG)
        private const val HEIGHTMAP_MASK = (1L shl HEIGHTMAP_BITS) - 1L

        private val BIOME_PLAINS = RegistryCodec.entryIndex("minecraft:worldgen/biome", "minecraft:plains") ?: 0
    }
}
