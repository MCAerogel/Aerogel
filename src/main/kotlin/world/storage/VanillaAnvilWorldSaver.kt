package org.macaroon3145.world.storage

import org.macaroon3145.network.codec.BlockStateRegistry
import org.macaroon3145.network.handler.PlayerSessionManager
import org.macaroon3145.world.ChunkPos
import org.macaroon3145.world.GeneratedChunk
import org.macaroon3145.world.HeightmapData
import org.macaroon3145.world.World
import org.macaroon3145.world.WorldManager
import org.macaroon3145.world.generators.FoliaSharedMemoryWorldGenerator
import org.slf4j.LoggerFactory
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.BufferedInputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.EOFException
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import java.util.zip.DeflaterOutputStream
import java.util.zip.GZIPInputStream
import java.util.zip.InflaterInputStream
import java.util.UUID
import kotlin.math.ceil
import kotlin.math.max

object VanillaAnvilWorldSaver {
    data class LoadReport(
        val worldsScanned: Int,
        val worldsWithOverrides: Int,
        val loadedChunks: Int,
        val loadedOverrides: Int
    )

    data class PersistedPlayerData(
        val uuid: UUID,
        val worldKey: String,
        val x: Double,
        val y: Double,
        val z: Double,
        val yaw: Float,
        val pitch: Float,
        val gameMode: Int,
        val flying: Boolean,
        val health: Float,
        val food: Int,
        val saturation: Float,
        val foodExhaustion: Float,
        val hotbarItemIds: IntArray,
        val hotbarItemCounts: IntArray,
        val mainInventoryItemIds: IntArray,
        val mainInventoryItemCounts: IntArray,
        val armorItemIds: IntArray,
        val armorItemCounts: IntArray,
        val offhandItemId: Int,
        val offhandItemCount: Int,
        val enderChestItemIds: IntArray,
        val enderChestItemCounts: IntArray,
        val hotbarShulkerItemIds: IntArray,
        val hotbarShulkerItemCounts: IntArray,
        val mainShulkerItemIds: IntArray,
        val mainShulkerItemCounts: IntArray,
        val armorShulkerItemIds: IntArray,
        val armorShulkerItemCounts: IntArray,
        val offhandShulkerItemIds: IntArray,
        val offhandShulkerItemCounts: IntArray
    )

    private val logger = LoggerFactory.getLogger(VanillaAnvilWorldSaver::class.java)

    private const val REGION_HEADER_BYTES = 8192
    private const val SECTOR_BYTES = 4096
    private const val SECTION_SIZE = 16 * 16 * 16
    private const val WORLD_MIN_Y = -64
    private const val WORLD_MAX_Y = 319
    private const val MIN_SECTION_Y = WORLD_MIN_Y shr 4
    private const val MAX_SECTION_Y = WORLD_MAX_Y shr 4
    // Minecraft 1.21.11 world/chunk DataVersion (matches SharedConstants.WORLD_VERSION).
    private const val DEFAULT_DATA_VERSION = 4671
    private const val AIR_STATE_ID = 0
    private const val MAX_STACK_SIZE = 99
    private const val HEIGHTMAP_WORLD_SURFACE_WG_ID = 0
    private const val HEIGHTMAP_WORLD_SURFACE_ID = 1
    private const val HEIGHTMAP_OCEAN_FLOOR_WG_ID = 2
    private const val HEIGHTMAP_OCEAN_FLOOR_ID = 3
    private const val HEIGHTMAP_MOTION_BLOCKING_ID = 4
    private const val HEIGHTMAP_MOTION_BLOCKING_NO_LEAVES_ID = 5

    private const val TAG_END = 0
    private const val TAG_BYTE = 1
    private const val TAG_SHORT = 2
    private const val TAG_INT = 3
    private const val TAG_LONG = 4
    private const val TAG_FLOAT = 5
    private const val TAG_DOUBLE = 6
    private const val TAG_BYTE_ARRAY = 7
    private const val TAG_STRING = 8
    private const val TAG_LIST = 9
    private const val TAG_COMPOUND = 10
    private const val TAG_INT_ARRAY = 11
    private const val TAG_LONG_ARRAY = 12

    private val plainsBiomeName = "minecraft:plains"
    private val overworldDimensionId = "minecraft:overworld"
    private val netherDimensionId = "minecraft:the_nether"
    private val endDimensionId = "minecraft:the_end"
    private val regionFileRegex = Regex("""r\.(-?\d+)\.(-?\d+)\.mca""")
    private val loadedChunkOverridesByWorld = ConcurrentHashMap<String, MutableSet<ChunkPos>>()
    private val preloadedOverrideWorldKeys = ConcurrentHashMap.newKeySet<String>()
    private val worldHasPersistedOverrides = ConcurrentHashMap<String, Boolean>()
    private val persistedChunkLightingByWorld = ConcurrentHashMap<String, ConcurrentHashMap<ChunkPos, PersistedChunkLighting>>()
    private val persistedChunkHeightmapsByWorld = ConcurrentHashMap<String, ConcurrentHashMap<ChunkPos, List<HeightmapData>>>()
    private val autosaveInFlight = AtomicBoolean(false)
    private val manualSaveInFlight = AtomicBoolean(false)
    private val autosaveExecutor = Executors.newSingleThreadExecutor(AutosaveThreadFactory())
    private val autosaveThreadRef = AtomicReference<Thread?>(null)
    @Volatile private var autosaveIntervalNanos: Long = 30L * 1_000_000_000L
    @Volatile private var nextAutosaveDueNanos: Long = System.nanoTime() + autosaveIntervalNanos

    private data class PersistedChunkLighting(
        val skyLightMask: LongArray,
        val blockLightMask: LongArray,
        val emptySkyLightMask: LongArray,
        val emptyBlockLightMask: LongArray,
        val skyLight: List<ByteArray>,
        val blockLight: List<ByteArray>
    )

    fun saveAllDirtyWorlds() {
        runOnSaveExecutor {
            saveAllDirtyWorldsInternal()
        }
    }

    fun applyPersistedLightingIfPresent(worldKey: String, chunkPos: ChunkPos, generated: GeneratedChunk): GeneratedChunk {
        val worldMap = persistedChunkLightingByWorld[worldKey] ?: return generated
        val lighting = worldMap[chunkPos] ?: return generated
        return generated.copy(
            skyLightMask = lighting.skyLightMask,
            blockLightMask = lighting.blockLightMask,
            emptySkyLightMask = lighting.emptySkyLightMask,
            emptyBlockLightMask = lighting.emptyBlockLightMask,
            skyLight = lighting.skyLight,
            blockLight = lighting.blockLight
        )
    }

    fun applyPersistedLightingWithRuntimeSectionOverrides(
        worldKey: String,
        chunkPos: ChunkPos,
        generated: GeneratedChunk,
        changedSectionYs: Set<Int>
    ): GeneratedChunk {
        if (changedSectionYs.isEmpty()) return applyPersistedLightingIfPresent(worldKey, chunkPos, generated)
        val worldMap = persistedChunkLightingByWorld[worldKey] ?: return generated
        val persisted = worldMap[chunkPos] ?: return generated

        val mergedSkyBySection = persisted.sectionLightArraysBySectionY(worldKey = worldKey, useSky = true).toMutableMap()
        val mergedBlockBySection = persisted.sectionLightArraysBySectionY(worldKey = worldKey, useSky = false).toMutableMap()
        val runtimeSkyBySection = generated.sectionLightArraysBySectionY(worldKey = worldKey, useSky = true)
        val runtimeBlockBySection = generated.sectionLightArraysBySectionY(worldKey = worldKey, useSky = false)

        for (sectionY in changedSectionYs) {
            val runtimeSky = runtimeSkyBySection[sectionY]
            if (runtimeSky != null && runtimeSky.isNotEmpty()) {
                mergedSkyBySection[sectionY] = runtimeSky.copyOf()
            } else {
                mergedSkyBySection.remove(sectionY)
            }

            val runtimeBlock = runtimeBlockBySection[sectionY]
            if (runtimeBlock != null && runtimeBlock.isNotEmpty()) {
                mergedBlockBySection[sectionY] = runtimeBlock.copyOf()
            } else {
                mergedBlockBySection.remove(sectionY)
            }
        }

        val merged = sectionLightArraysToPersistedLighting(mergedSkyBySection, mergedBlockBySection) ?: return generated
        return generated.copy(
            skyLightMask = merged.skyLightMask,
            blockLightMask = merged.blockLightMask,
            emptySkyLightMask = merged.emptySkyLightMask,
            emptyBlockLightMask = merged.emptyBlockLightMask,
            skyLight = merged.skyLight,
            blockLight = merged.blockLight
        )
    }

    fun applyPersistedHeightmapsIfPresent(worldKey: String, chunkPos: ChunkPos, generated: GeneratedChunk): GeneratedChunk {
        val worldMap = persistedChunkHeightmapsByWorld[worldKey] ?: return generated
        val persisted = worldMap[chunkPos] ?: return generated
        if (persisted.isEmpty()) return generated
        val persistedByType = HashMap<Int, LongArray>(persisted.size)
        for (entry in persisted) {
            persistedByType[entry.typeId] = entry.values
        }
        if (persistedByType.isEmpty()) return generated

        val remapped = ArrayList<HeightmapData>(max(generated.heightmaps.size, persistedByType.size))
        for (entry in generated.heightmaps) {
            val values = persistedByType[entry.typeId] ?: entry.values
            remapped.add(HeightmapData(typeId = entry.typeId, values = values))
        }
        for ((typeId, values) in persistedByType) {
            if (remapped.any { it.typeId == typeId }) continue
            remapped.add(HeightmapData(typeId = typeId, values = values))
        }
        return generated.copy(heightmaps = remapped)
    }

    fun withoutLighting(generated: GeneratedChunk): GeneratedChunk {
        return generated.copy(
            skyLightMask = LongArray(0),
            blockLightMask = LongArray(0),
            emptySkyLightMask = LongArray(0),
            emptyBlockLightMask = LongArray(0),
            skyLight = emptyList(),
            blockLight = emptyList()
        )
    }

    fun applyRuntimeHeightmaps(world: World, chunkPos: ChunkPos, generated: GeneratedChunk): GeneratedChunk {
        if (generated.heightmaps.isEmpty()) return generated
        val values = buildHeightmapLongArray(world, chunkPos)
        val remapped = generated.heightmaps.map { entry ->
            HeightmapData(typeId = entry.typeId, values = values.copyOf())
        }
        return generated.copy(heightmaps = remapped)
    }

    fun loadAllWorlds(): LoadReport {
        return runOnSaveExecutor {
            loadAllWorldsInternal()
        }
    }

    fun loadChunkOverrideIfPresent(world: World, chunkPos: ChunkPos): Int {
        if (preloadedOverrideWorldKeys.contains(world.key)) return 0
        val hasPersistedOverrides = worldHasPersistedOverrides.computeIfAbsent(world.key) { key ->
            detectPersistedOverridesInWorld(key)
        }
        if (!hasPersistedOverrides) return 0
        return runOnSaveExecutor {
            val loadedForWorld = loadedChunkOverridesByWorld
                .computeIfAbsent(world.key) { ConcurrentHashMap.newKeySet() }
            if (!loadedForWorld.add(chunkPos)) return@runOnSaveExecutor 0

            runCatching {
                val regionX = Math.floorDiv(chunkPos.x, 32)
                val regionZ = Math.floorDiv(chunkPos.z, 32)
                val sectionX = Math.floorMod(chunkPos.x, 32)
                val sectionZ = Math.floorMod(chunkPos.z, 32)
                val locationIndex = sectionX + sectionZ * 32

                val regionPath = worldRegionDirectory(world.key).resolve("r.$regionX.$regionZ.mca")
                val chunkRecord = readRegionChunkRecord(regionPath, locationIndex) ?: run {
                    clearPersistedChunkLighting(world.key, chunkPos)
                    clearPersistedChunkHeightmaps(world.key, chunkPos)
                    return@runCatching 0
                }
                val applied = applySavedChunkToWorld(world, chunkPos.x, chunkPos.z, chunkRecord)
                if (applied > 0) {
                    syncChunkOverridesToBridge(world, chunkPos.x, chunkPos.z)
                }
                applied
            }.getOrElse { throwable ->
                loadedForWorld.remove(chunkPos)
                throw throwable
            }
        }
    }

    private fun <T> runOnSaveExecutor(task: () -> T): T {
        if (Thread.currentThread() === autosaveThreadRef.get()) {
            return task()
        }
        val future = CompletableFuture<T>()
        autosaveExecutor.execute {
            try {
                future.complete(task())
            } catch (t: Throwable) {
                future.completeExceptionally(t)
            }
        }
        return future.join()
    }

    fun configureAutosaveIntervalSeconds(seconds: Double) {
        val nanos = if (seconds.isFinite() && seconds > 0.0) {
            (seconds * 1_000_000_000.0).toLong().coerceAtLeast(1L)
        } else {
            0L
        }
        autosaveIntervalNanos = nanos
        nextAutosaveDueNanos = if (nanos > 0L) System.nanoTime() + nanos else Long.MAX_VALUE
    }

    fun tickAutosave(nowNanos: Long = System.nanoTime()) {
        val interval = autosaveIntervalNanos
        if (interval <= 0L) return
        if (nowNanos < nextAutosaveDueNanos) return
        if (!autosaveInFlight.compareAndSet(false, true)) return
        nextAutosaveDueNanos = nowNanos + interval
        autosaveExecutor.execute {
            try {
                saveAllDirtyWorlds()
            } catch (t: Throwable) {
                logger.warn("Autosave failed", t)
            } finally {
                autosaveInFlight.set(false)
            }
        }
    }

    fun flushAsyncAutosave() {
        val barrier = CompletableFuture<Unit>()
        autosaveExecutor.execute { barrier.complete(Unit) }
        runCatching { barrier.get(10, TimeUnit.SECONDS) }
    }

    fun saveAllDirtyWorldsAsync(onComplete: ((Boolean) -> Unit)? = null): Boolean {
        if (!manualSaveInFlight.compareAndSet(false, true)) return false
        autosaveExecutor.execute {
            var success = true
            try {
                saveAllDirtyWorlds()
            } catch (t: Throwable) {
                success = false
                logger.warn("Manual async save failed", t)
            } finally {
                manualSaveInFlight.set(false)
                if (onComplete != null) {
                    runCatching { onComplete(success) }
                }
            }
        }
        return true
    }

    private fun saveAllDirtyWorldsInternal() {
        val worlds = WorldManager.allWorlds()
        val dirtyChestChunksByWorld = PlayerSessionManager.consumeDirtyChestChunksByWorld()
        for (world in worlds) {
            val dirty = HashSet(world.changedChunkPositionsSnapshot())
            dirty.addAll(dirtyChestChunksByWorld[world.key].orEmpty())
            if (dirty.isEmpty()) continue
            saveWorldDirtyChunks(world, dirty)
        }
        saveOnlinePlayerData()
        val timeWeather = PlayerSessionManager.timeWeatherSnapshotForLevelDat()
        VanillaLevelDatSeedStore.save(worlds.associate { it.key to it.seed }, timeWeather = timeWeather)
    }

    private fun loadAllWorldsInternal(): LoadReport {
        val worlds = WorldManager.allWorlds()
        preloadedOverrideWorldKeys.clear()
        worldHasPersistedOverrides.clear()
        var worldsWithOverrides = 0
        var loadedChunks = 0
        var loadedOverrides = 0
        for (world in worlds) {
            val (chunks, overrides) = loadWorldChunkOverrides(world)
            worldHasPersistedOverrides[world.key] = chunks > 0
            preloadedOverrideWorldKeys.add(world.key)
            if (chunks > 0) {
                worldsWithOverrides++
                loadedChunks += chunks
                loadedOverrides += overrides
            }
        }
        return LoadReport(
            worldsScanned = worlds.size,
            worldsWithOverrides = worldsWithOverrides,
            loadedChunks = loadedChunks,
            loadedOverrides = loadedOverrides
        )
    }

    private fun detectPersistedOverridesInWorld(worldKey: String): Boolean {
        val regionDir = worldRegionDirectory(worldKey)
        if (!Files.isDirectory(regionDir)) return false
        return runCatching {
            Files.newDirectoryStream(regionDir).use { paths ->
                for (path in paths) {
                    if (!Files.isRegularFile(path)) continue
                    if (regionFileRegex.matches(path.fileName.toString())) {
                        return@use true
                    }
                }
                false
            }
        }.getOrElse { throwable ->
            logger.warn("Failed to detect persisted chunk overrides for world={}", worldKey, throwable)
            true
        }
    }

    fun loadPlayerData(uuid: UUID): PersistedPlayerData? {
        val path = playerDataPath(uuid)
        if (!Files.isRegularFile(path)) return null
        return runCatching {
            val root = readCompressedNbtRoot(path) ?: return@runCatching null
            decodePlayerData(uuid, root)
        }.onFailure { throwable ->
            logger.warn("Failed to load playerdata: {}", path.toAbsolutePath(), throwable)
        }.getOrNull()
    }

    fun savePlayerData(snapshot: PlayerSessionManager.PersistedPlayerState) {
        val path = playerDataPath(snapshot.uuid)
        val tempPath = path.resolveSibling("${path.fileName}.tmp")
        runCatching {
            Files.createDirectories(path.parent)
            val payload = encodePlayerData(snapshot)
            writeCompressedNbtRoot(tempPath, payload)
            runCatching {
                Files.move(tempPath, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
            }.getOrElse {
                Files.move(tempPath, path, StandardCopyOption.REPLACE_EXISTING)
            }
        }.onFailure { throwable ->
            runCatching { Files.deleteIfExists(tempPath) }
            logger.warn("Failed to save playerdata: {}", path.toAbsolutePath(), throwable)
        }
    }

    private fun saveOnlinePlayerData() {
        val onlineSnapshots = PlayerSessionManager.playerPersistenceSnapshots()
        for (snapshot in onlineSnapshots) {
            savePlayerData(snapshot)
        }
        val offlineSnapshots = PlayerSessionManager.pendingOfflinePlayerPersistenceSnapshots()
        for (snapshot in offlineSnapshots) {
            savePlayerData(snapshot)
            PlayerSessionManager.acknowledgeOfflinePlayerPersistenceSaved(snapshot.uuid)
        }
    }

    private fun loadWorldChunkOverrides(world: World): Pair<Int, Int> {
        val regionDir = worldRegionDirectory(world.key)
        if (!Files.isDirectory(regionDir)) return 0 to 0

        var loadedChunks = 0
        var loadedOverrides = 0
        Files.list(regionDir).use { paths ->
            paths.forEach { regionPath ->
                val match = regionFileRegex.matchEntire(regionPath.fileName.toString()) ?: return@forEach
                val regionX = match.groupValues[1].toIntOrNull() ?: return@forEach
                val regionZ = match.groupValues[2].toIntOrNull() ?: return@forEach
                val chunks = readRegionChunks(regionPath)
                for ((locationIndex, chunkRecord) in chunks) {
                    val chunkX = (regionX shl 5) + (locationIndex and 31)
                    val chunkZ = (regionZ shl 5) + (locationIndex ushr 5)
                    val applied = runCatching { applySavedChunkToWorld(world, chunkX, chunkZ, chunkRecord) }.getOrElse {
                        logger.warn(
                            "Failed to load saved chunk for world={} at {},{} from {}",
                            world.key,
                            chunkX,
                            chunkZ,
                            regionPath.toAbsolutePath(),
                            it
                        )
                        0
                    }
                    if (applied > 0) {
                        syncChunkOverridesToBridge(world, chunkX, chunkZ)
                        loadedChunks++
                        loadedOverrides += applied
                    }
                }
            }
        }
        return loadedChunks to loadedOverrides
    }

    private fun saveWorldDirtyChunks(world: World, dirtyChunks: Set<ChunkPos>) {
        val byRegion = LinkedHashMap<Pair<Int, Int>, MutableList<ChunkPos>>()
        for (chunkPos in dirtyChunks) {
            val regionX = Math.floorDiv(chunkPos.x, 32)
            val regionZ = Math.floorDiv(chunkPos.z, 32)
            byRegion.computeIfAbsent(regionX to regionZ) { ArrayList() }.add(chunkPos)
        }

        val regionDir = worldRegionDirectory(world.key)
        Files.createDirectories(regionDir)

        for ((regionCoord, chunksInRegion) in byRegion) {
            val (regionX, regionZ) = regionCoord
            val regionPath = regionDir.resolve("r.$regionX.$regionZ.mca")
            val existing = readRegionChunks(regionPath)

            for (chunkPos in chunksInRegion) {
                val sectionX = Math.floorMod(chunkPos.x, 32)
                val sectionZ = Math.floorMod(chunkPos.z, 32)
                val locationIndex = sectionX + sectionZ * 32
                val generated = runCatching { world.buildChunk(chunkPos) }.getOrNull()
                val hasBlockOverrides = world.changedBlocksInChunk(chunkPos.x, chunkPos.z).isNotEmpty()
                // Chunks with runtime block overrides are written without light arrays to force relight on load.
                // Do not overwrite in-memory persisted lighting cache from potentially stale generated lighting.
                if (generated != null && !hasBlockOverrides) {
                    cachePersistedChunkLighting(world.key, chunkPos, generated.toPersistedChunkLighting())
                }
                existing[locationIndex] = encodeChunkRecord(world, chunkPos, generated)
            }

            writeRegionFile(regionPath, existing)
        }
    }

    private fun syncChunkOverridesToBridge(world: World, chunkX: Int, chunkZ: Int) {
        val changedBlocks = world.changedBlocksInChunk(chunkX, chunkZ)
        if (changedBlocks.isEmpty()) return
        FoliaSharedMemoryWorldGenerator.pushBlockUpdatesIfRevisionChanged(
            worldKey = world.key,
            chunkX = chunkX,
            chunkZ = chunkZ,
            revision = world.changedBlockRevision(chunkX, chunkZ),
            changedBlocks = changedBlocks
        )
        FoliaSharedMemoryWorldGenerator.invalidateChunkGeneratedAndLighting(world.key, chunkX, chunkZ)
    }

    private fun worldRegionDirectory(worldKey: String): Path {
        val worldRoot = Path.of("world")
        return when (worldKey) {
            "minecraft:overworld" -> worldRoot.resolve("region")
            "minecraft:the_nether" -> worldRoot.resolve("DIM-1").resolve("region")
            "minecraft:the_end" -> worldRoot.resolve("DIM1").resolve("region")
            else -> {
                val namespace = worldKey.substringBefore(':', "minecraft")
                val path = worldKey.substringAfter(':', "overworld").replace(':', '/')
                worldRoot.resolve("dimensions").resolve(namespace).resolve(path).resolve("region")
            }
        }
    }

    private fun playerDataDirectory(): Path {
        return Path.of("world").resolve("playerdata")
    }

    private fun playerDataPath(uuid: UUID): Path {
        return playerDataDirectory().resolve("${uuid}.dat")
    }

    private fun readCompressedNbtRoot(path: Path): NbtCompoundTag? {
        BufferedInputStream(Files.newInputStream(path)).use { raw ->
            raw.mark(2)
            val first = raw.read()
            val second = raw.read()
            raw.reset()
            val stream = when {
                first == 0x1F && second == 0x8B -> GZIPInputStream(raw)
                first == 0x78 -> InflaterInputStream(raw)
                else -> raw
            }
            DataInputStream(stream).use { input -> return readRootCompound(input) }
        }
    }

    private fun writeCompressedNbtRoot(path: Path, payload: ByteArray) {
        DataOutputStream(java.util.zip.GZIPOutputStream(Files.newOutputStream(path))).use { output ->
            output.write(payload)
        }
    }

    private fun decodePlayerData(uuid: UUID, root: NbtCompoundTag): PersistedPlayerData? {
        val worldKey = worldKeyFromDimensionTag(root.entries["Dimension"])
        val pos = root.entries["Pos"] as? NbtListTag
        val rotation = root.entries["Rotation"] as? NbtListTag
        val x = asDoubleFromTagList(pos, 0) ?: return null
        val y = asDoubleFromTagList(pos, 1) ?: return null
        val z = asDoubleFromTagList(pos, 2) ?: return null
        val yaw = asFloatFromTagList(rotation, 0) ?: 0f
        val pitch = asFloatFromTagList(rotation, 1) ?: 0f
        val gameMode = (root.entries["playerGameType"] as? NbtIntTag)?.value?.toInt() ?: 0
        val abilities = root.entries["abilities"] as? NbtCompoundTag
        val flying = ((abilities?.entries?.get("flying") as? NbtByteTag)?.value?.toInt() ?: 0) != 0
        val health = (root.entries["Health"] as? NbtFloatTag)?.value ?: 20f
        val food = (root.entries["foodLevel"] as? NbtIntTag)?.value?.toInt() ?: 20
        val saturation = (root.entries["foodSaturationLevel"] as? NbtFloatTag)?.value ?: 5f
        val foodExhaustion = (root.entries["foodExhaustionLevel"] as? NbtFloatTag)?.value ?: 0f

        val hotbarIds = IntArray(9) { -1 }
        val hotbarCounts = IntArray(9)
        val mainIds = IntArray(27) { -1 }
        val mainCounts = IntArray(27)
        val armorIds = IntArray(4) { -1 }
        val armorCounts = IntArray(4)
        val hotbarShulkerIds = IntArray(9 * 27) { -1 }
        val hotbarShulkerCounts = IntArray(9 * 27)
        val mainShulkerIds = IntArray(27 * 27) { -1 }
        val mainShulkerCounts = IntArray(27 * 27)
        val armorShulkerIds = IntArray(4 * 27) { -1 }
        val armorShulkerCounts = IntArray(4 * 27)
        val offhandShulkerIds = IntArray(27) { -1 }
        val offhandShulkerCounts = IntArray(27)
        var offhandId = -1
        var offhandCount = 0
        decodeInventoryList(
            list = root.entries["Inventory"] as? NbtListTag,
            hotbarIds = hotbarIds,
            hotbarCounts = hotbarCounts,
            mainIds = mainIds,
            mainCounts = mainCounts,
            armorIds = armorIds,
            armorCounts = armorCounts,
            hotbarShulkerIds = hotbarShulkerIds,
            hotbarShulkerCounts = hotbarShulkerCounts,
            mainShulkerIds = mainShulkerIds,
            mainShulkerCounts = mainShulkerCounts,
            armorShulkerIds = armorShulkerIds,
            armorShulkerCounts = armorShulkerCounts,
            offhandShulkerIds = offhandShulkerIds,
            offhandShulkerCounts = offhandShulkerCounts
        ) { itemId, count ->
            offhandId = itemId
            offhandCount = count
        }

        val enderIds = IntArray(27) { -1 }
        val enderCounts = IntArray(27)
        decodeSimpleContainerList(root.entries["EnderItems"] as? NbtListTag, enderIds, enderCounts)

        return PersistedPlayerData(
            uuid = uuid,
            worldKey = worldKey,
            x = x,
            y = y,
            z = z,
            yaw = yaw,
            pitch = pitch,
            gameMode = gameMode,
            flying = flying,
            health = health,
            food = food,
            saturation = saturation,
            foodExhaustion = foodExhaustion,
            hotbarItemIds = hotbarIds,
            hotbarItemCounts = hotbarCounts,
            mainInventoryItemIds = mainIds,
            mainInventoryItemCounts = mainCounts,
            armorItemIds = armorIds,
            armorItemCounts = armorCounts,
            offhandItemId = offhandId,
            offhandItemCount = offhandCount,
            enderChestItemIds = enderIds,
            enderChestItemCounts = enderCounts,
            hotbarShulkerItemIds = hotbarShulkerIds,
            hotbarShulkerItemCounts = hotbarShulkerCounts,
            mainShulkerItemIds = mainShulkerIds,
            mainShulkerItemCounts = mainShulkerCounts,
            armorShulkerItemIds = armorShulkerIds,
            armorShulkerItemCounts = armorShulkerCounts,
            offhandShulkerItemIds = offhandShulkerIds,
            offhandShulkerItemCounts = offhandShulkerCounts
        )
    }

    private fun encodePlayerData(snapshot: PlayerSessionManager.PersistedPlayerState): ByteArray {
        val nbt = NbtOutput()
        nbt.writeRootCompound {
            intTag("DataVersion", DEFAULT_DATA_VERSION)
            stringTag("Dimension", dimensionTagFromWorldKey(snapshot.worldKey))
            intTag("playerGameType", snapshot.gameMode.coerceIn(0, 3))
            floatTag("Health", snapshot.health.coerceIn(0f, 20f))
            shortTag("Air", 300)
            shortTag("Fire", -20)
            shortTag("HurtTime", 0)
            intTag("foodLevel", snapshot.food.coerceIn(0, 20))
            floatTag("foodSaturationLevel", snapshot.saturation.coerceAtLeast(0f))
            floatTag("foodExhaustionLevel", snapshot.foodExhaustion.coerceAtLeast(0f))

            listTag("Pos", NbtType.DOUBLE) {
                doubleElement(snapshot.x)
                doubleElement(snapshot.y)
                doubleElement(snapshot.z)
            }
            listTag("Motion", NbtType.DOUBLE) {
                doubleElement(0.0)
                doubleElement(0.0)
                doubleElement(0.0)
            }
            listTag("Rotation", NbtType.FLOAT) {
                floatElement(snapshot.yaw)
                floatElement(snapshot.pitch)
            }
            compoundTag("abilities") {
                byteTag("invulnerable", 0)
                byteTag("mayfly", if (snapshot.gameMode == 1 || snapshot.gameMode == 3) 1 else 0)
                byteTag("instabuild", if (snapshot.gameMode == 1) 1 else 0)
                byteTag("mayBuild", 1)
                byteTag("flying", if (snapshot.flying) 1 else 0)
                floatTag("flySpeed", 0.05f)
                floatTag("walkSpeed", 0.1f)
            }

            listTag("Inventory", NbtType.COMPOUND) {
                encodePlayerInventoryElements(
                    this,
                    hotbarIds = snapshot.hotbarItemIds,
                    hotbarCounts = snapshot.hotbarItemCounts,
                    mainIds = snapshot.mainInventoryItemIds,
                    mainCounts = snapshot.mainInventoryItemCounts,
                    armorIds = snapshot.armorItemIds,
                    armorCounts = snapshot.armorItemCounts,
                    offhandItemId = snapshot.offhandItemId,
                    offhandItemCount = snapshot.offhandItemCount,
                    hotbarShulkerIds = snapshot.hotbarShulkerItemIds,
                    hotbarShulkerCounts = snapshot.hotbarShulkerItemCounts,
                    mainShulkerIds = snapshot.mainShulkerItemIds,
                    mainShulkerCounts = snapshot.mainShulkerItemCounts,
                    armorShulkerIds = snapshot.armorShulkerItemIds,
                    armorShulkerCounts = snapshot.armorShulkerItemCounts,
                    offhandShulkerIds = snapshot.offhandShulkerItemIds,
                    offhandShulkerCounts = snapshot.offhandShulkerItemCounts
                )
            }

            listTag("EnderItems", NbtType.COMPOUND) {
                for (slot in 0 until 27) {
                    val itemId = snapshot.enderChestItemIds.getOrElse(slot) { -1 }
                    val count = snapshot.enderChestItemCounts.getOrElse(slot) { 0 }
                    writeContainerItemElement(this, slot, itemId, count)
                }
            }
        }
        return nbt.toByteArray()
    }

    private fun encodePlayerInventoryElements(
        list: NbtListWriter,
        hotbarIds: IntArray,
        hotbarCounts: IntArray,
        mainIds: IntArray,
        mainCounts: IntArray,
        armorIds: IntArray,
        armorCounts: IntArray,
        offhandItemId: Int,
        offhandItemCount: Int,
        hotbarShulkerIds: IntArray,
        hotbarShulkerCounts: IntArray,
        mainShulkerIds: IntArray,
        mainShulkerCounts: IntArray,
        armorShulkerIds: IntArray,
        armorShulkerCounts: IntArray,
        offhandShulkerIds: IntArray,
        offhandShulkerCounts: IntArray
    ) {
        for (slot in 0 until 9) {
            writeContainerItemElement(
                list = list,
                slot = slot,
                itemId = hotbarIds.getOrElse(slot) { -1 },
                count = hotbarCounts.getOrElse(slot) { 0 },
                shulkerItemIds = hotbarShulkerIds,
                shulkerItemCounts = hotbarShulkerCounts,
                shulkerSectionIndex = slot
            )
        }
        for (slot in 0 until 27) {
            writeContainerItemElement(
                list = list,
                slot = slot + 9,
                itemId = mainIds.getOrElse(slot) { -1 },
                count = mainCounts.getOrElse(slot) { 0 },
                shulkerItemIds = mainShulkerIds,
                shulkerItemCounts = mainShulkerCounts,
                shulkerSectionIndex = slot
            )
        }
        // Armor slots are boots, leggings, chestplate, helmet.
        for (slot in 0 until 4) {
            writeContainerItemElement(
                list = list,
                slot = slot + 100,
                itemId = armorIds.getOrElse(slot) { -1 },
                count = armorCounts.getOrElse(slot) { 0 },
                shulkerItemIds = armorShulkerIds,
                shulkerItemCounts = armorShulkerCounts,
                shulkerSectionIndex = slot
            )
        }
        writeContainerItemElement(
            list = list,
            slot = 106,
            itemId = offhandItemId,
            count = offhandItemCount,
            shulkerItemIds = offhandShulkerIds,
            shulkerItemCounts = offhandShulkerCounts,
            shulkerSectionIndex = 0
        )
    }

    private fun writeContainerItemElement(
        list: NbtListWriter,
        slot: Int,
        itemId: Int,
        count: Int,
        shulkerItemIds: IntArray? = null,
        shulkerItemCounts: IntArray? = null,
        shulkerSectionIndex: Int = slot
    ) {
        if (itemId < 0 || count <= 0) return
        val itemKey = PlayerSessionManager.itemKeyForPersistence(itemId) ?: return
        list.compoundElement {
            byteTag("Slot", slot.coerceIn(0, 255))
            stringTag("id", itemKey)
            byteTag("Count", count.coerceIn(1, MAX_STACK_SIZE))
            if (
                shulkerItemIds != null &&
                shulkerItemCounts != null &&
                isShulkerItemKey(itemKey) &&
                hasAnyShulkerSectionItems(shulkerItemIds, shulkerItemCounts, shulkerSectionIndex)
            ) {
                compoundTag("tag") {
                    compoundTag("BlockEntityTag") {
                        stringTag("id", "minecraft:shulker_box")
                        listTag("Items", NbtType.COMPOUND) {
                            writeShulkerSectionItems(
                                out = this,
                                shulkerItemIds = shulkerItemIds,
                                shulkerItemCounts = shulkerItemCounts,
                                sectionIndex = shulkerSectionIndex
                            )
                        }
                    }
                }
            }
        }
    }

    private fun decodeInventoryList(
        list: NbtListTag?,
        hotbarIds: IntArray,
        hotbarCounts: IntArray,
        mainIds: IntArray,
        mainCounts: IntArray,
        armorIds: IntArray,
        armorCounts: IntArray,
        hotbarShulkerIds: IntArray,
        hotbarShulkerCounts: IntArray,
        mainShulkerIds: IntArray,
        mainShulkerCounts: IntArray,
        armorShulkerIds: IntArray,
        armorShulkerCounts: IntArray,
        offhandShulkerIds: IntArray,
        offhandShulkerCounts: IntArray,
        onOffhand: (itemId: Int, count: Int) -> Unit
    ) {
        if (list == null || list.elementType != TAG_COMPOUND) return
        for (tag in list.elements) {
            val entry = tag as? NbtCompoundTag ?: continue
            val slot = slotIndexFromItemEntry(entry)
            if (slot < 0) continue
            val key = (entry.entries["id"] as? NbtStringTag)?.value ?: continue
            val itemId = PlayerSessionManager.itemIdForPersistence(key)
            if (itemId < 0) continue
            val count = itemCountFromItemEntry(entry)
            if (count <= 0) continue
            when (slot) {
                in 0..8 -> {
                    hotbarIds[slot] = itemId
                    hotbarCounts[slot] = count
                    decodeItemShulkerSectionInto(entry, hotbarShulkerIds, hotbarShulkerCounts, sectionIndex = slot)
                }
                in 9..35 -> {
                    val index = slot - 9
                    mainIds[index] = itemId
                    mainCounts[index] = count
                    decodeItemShulkerSectionInto(entry, mainShulkerIds, mainShulkerCounts, sectionIndex = index)
                }
                100, 101, 102, 103 -> {
                    val index = slot - 100
                    armorIds[index] = itemId
                    armorCounts[index] = count
                    decodeItemShulkerSectionInto(entry, armorShulkerIds, armorShulkerCounts, sectionIndex = index)
                }
                106 -> {
                    onOffhand(itemId, count)
                    decodeItemShulkerSectionInto(entry, offhandShulkerIds, offhandShulkerCounts, sectionIndex = 0)
                }
            }
        }
    }

    private fun decodeSimpleContainerList(list: NbtListTag?, itemIds: IntArray, itemCounts: IntArray) {
        if (list == null || list.elementType != TAG_COMPOUND) return
        for (tag in list.elements) {
            val entry = tag as? NbtCompoundTag ?: continue
            val slot = slotIndexFromItemEntry(entry)
            if (slot !in itemIds.indices) continue
            val key = (entry.entries["id"] as? NbtStringTag)?.value ?: continue
            val itemId = PlayerSessionManager.itemIdForPersistence(key)
            if (itemId < 0) continue
            val count = itemCountFromItemEntry(entry)
            if (count <= 0) continue
            itemIds[slot] = itemId
            itemCounts[slot] = count
        }
    }

    private fun slotIndexFromItemEntry(entry: NbtCompoundTag): Int {
        val slotTag = entry.entries["Slot"] ?: entry.entries["slot"] ?: return -1
        val slot = when (slotTag) {
            is NbtByteTag -> slotTag.value.toInt() and 0xFF
            is NbtShortTag -> slotTag.value.toInt() and 0xFFFF
            is NbtIntTag -> slotTag.value
            else -> -1
        }
        return slot
    }

    private fun itemCountFromItemEntry(entry: NbtCompoundTag): Int {
        val countTag = entry.entries["Count"] ?: entry.entries["count"] ?: return 0
        val raw = when (countTag) {
            is NbtByteTag -> countTag.value.toInt()
            is NbtShortTag -> countTag.value.toInt()
            is NbtIntTag -> countTag.value
            else -> 0
        }
        return raw.coerceIn(1, MAX_STACK_SIZE)
    }

    private fun isShulkerItemKey(itemKey: String): Boolean {
        return itemKey == "minecraft:shulker_box" || itemKey.endsWith("_shulker_box")
    }

    private fun hasAnyShulkerSectionItems(
        shulkerItemIds: IntArray,
        shulkerItemCounts: IntArray,
        sectionIndex: Int
    ): Boolean {
        val base = sectionIndex * 27
        if (base < 0 || base + 26 >= shulkerItemIds.size || base + 26 >= shulkerItemCounts.size) return false
        for (i in 0 until 27) {
            val itemId = shulkerItemIds[base + i]
            val count = shulkerItemCounts[base + i]
            if (itemId >= 0 && count > 0) return true
        }
        return false
    }

    private fun writeShulkerSectionItems(
        out: NbtListWriter,
        shulkerItemIds: IntArray,
        shulkerItemCounts: IntArray,
        sectionIndex: Int
    ) {
        val base = sectionIndex * 27
        if (base < 0 || base + 26 >= shulkerItemIds.size || base + 26 >= shulkerItemCounts.size) return
        for (slot in 0 until 27) {
            val itemId = shulkerItemIds[base + slot]
            val count = shulkerItemCounts[base + slot]
            writeContainerItemElement(out, slot, itemId, count)
        }
    }

    private fun decodeItemShulkerSectionInto(
        entry: NbtCompoundTag,
        targetIds: IntArray,
        targetCounts: IntArray,
        sectionIndex: Int
    ) {
        val itemsList = extractShulkerItemsList(entry) ?: return
        val base = sectionIndex * 27
        if (base < 0 || base + 26 >= targetIds.size || base + 26 >= targetCounts.size) return
        val idsView = IntArray(27) { idx -> targetIds[base + idx] }
        val countsView = IntArray(27) { idx -> targetCounts[base + idx] }
        decodeSimpleContainerList(itemsList, idsView, countsView)
        for (i in 0 until 27) {
            targetIds[base + i] = idsView[i]
            targetCounts[base + i] = countsView[i]
        }
    }

    private fun decodeContainerShulkerSectionsFromItems(
        itemsList: NbtListTag?,
        targetIds: IntArray,
        targetCounts: IntArray
    ) {
        if (itemsList == null || itemsList.elementType != TAG_COMPOUND) return
        for (tag in itemsList.elements) {
            val entry = tag as? NbtCompoundTag ?: continue
            val slot = slotIndexFromItemEntry(entry)
            if (slot !in 0 until 27) continue
            decodeItemShulkerSectionInto(entry, targetIds, targetCounts, sectionIndex = slot)
        }
    }

    private fun extractShulkerItemsList(entry: NbtCompoundTag): NbtListTag? {
        val tagCompound = entry.entries["tag"] as? NbtCompoundTag
        val blockEntityTag = tagCompound?.entries?.get("BlockEntityTag") as? NbtCompoundTag
        val legacyItems = blockEntityTag?.entries?.get("Items") as? NbtListTag
        if (legacyItems != null) return legacyItems

        val components = entry.entries["components"] as? NbtCompoundTag
        val blockEntityData = components?.entries?.get("minecraft:block_entity_data") as? NbtCompoundTag
        val modernItems = blockEntityData?.entries?.get("Items") as? NbtListTag
        if (modernItems != null) return modernItems
        return null
    }

    private fun dimensionTagFromWorldKey(worldKey: String): String {
        return when (worldKey) {
            "minecraft:overworld" -> overworldDimensionId
            "minecraft:the_nether" -> netherDimensionId
            "minecraft:the_end" -> endDimensionId
            else -> worldKey
        }
    }

    private fun worldKeyFromDimensionTag(tag: NbtTag?): String {
        when (tag) {
            is NbtStringTag -> {
                return when (tag.value) {
                    overworldDimensionId -> "minecraft:overworld"
                    netherDimensionId -> "minecraft:the_nether"
                    endDimensionId -> "minecraft:the_end"
                    else -> tag.value
                }
            }
            is NbtIntTag -> {
                return when (tag.value.toInt()) {
                    -1 -> "minecraft:the_nether"
                    1 -> "minecraft:the_end"
                    else -> "minecraft:overworld"
                }
            }
            else -> {
                return "minecraft:overworld"
            }
        }
    }

    private fun asDoubleFromTagList(list: NbtListTag?, index: Int): Double? {
        val element = list?.elements?.getOrNull(index) ?: return null
        return when (element) {
            is NbtDoubleTag -> element.value
            is NbtFloatTag -> element.value.toDouble()
            is NbtIntTag -> element.value.toDouble()
            is NbtLongTag -> element.value.toDouble()
            else -> null
        }
    }

    private fun asFloatFromTagList(list: NbtListTag?, index: Int): Float? {
        val element = list?.elements?.getOrNull(index) ?: return null
        return when (element) {
            is NbtFloatTag -> element.value
            is NbtDoubleTag -> element.value.toFloat()
            is NbtIntTag -> element.value.toFloat()
            is NbtLongTag -> element.value.toFloat()
            else -> null
        }
    }

    private fun readRegionChunks(regionPath: Path): MutableMap<Int, ByteArray> {
        val out = HashMap<Int, ByteArray>()
        if (!Files.isRegularFile(regionPath)) return out

        val bytes = runCatching { Files.readAllBytes(regionPath) }.getOrElse {
            logger.warn("Failed to read existing region file: {}", regionPath.toAbsolutePath(), it)
            return out
        }

        if (bytes.size < REGION_HEADER_BYTES) return out

        for (i in 0 until 1024) {
            val record = extractRegionChunkRecord(bytes, i) ?: continue
            out[i] = record
        }
        return out
    }

    private fun readRegionChunkRecord(regionPath: Path, locationIndex: Int): ByteArray? {
        if (locationIndex !in 0 until 1024) return null
        if (!Files.isRegularFile(regionPath)) return null
        val bytes = runCatching { Files.readAllBytes(regionPath) }.getOrElse {
            logger.warn("Failed to read existing region file: {}", regionPath.toAbsolutePath(), it)
            return null
        }
        if (bytes.size < REGION_HEADER_BYTES) return null
        return extractRegionChunkRecord(bytes, locationIndex)
    }

    private fun extractRegionChunkRecord(regionBytes: ByteArray, locationIndex: Int): ByteArray? {
        if (locationIndex !in 0 until 1024) return null
        if (regionBytes.size < REGION_HEADER_BYTES) return null
        val headerOffset = locationIndex * 4
        if (headerOffset + 4 > REGION_HEADER_BYTES) return null

        val location = ByteBuffer.wrap(regionBytes, headerOffset, 4).order(ByteOrder.BIG_ENDIAN).int
        val offsetSectors = (location ushr 8) and 0x00FF_FFFF
        val sectorCount = location and 0xFF
        if (offsetSectors <= 0 || sectorCount <= 0) return null

        val start = offsetSectors * SECTOR_BYTES
        val maxBytes = sectorCount * SECTOR_BYTES
        if (start < 0 || start + 5 > regionBytes.size || start + maxBytes > regionBytes.size) return null

        val length = ByteBuffer.wrap(regionBytes, start, 4).order(ByteOrder.BIG_ENDIAN).int
        if (length <= 0 || length + 4 > maxBytes || start + 4 + length > regionBytes.size) return null
        return regionBytes.copyOfRange(start, start + 4 + length)
    }

    private fun writeRegionFile(regionPath: Path, chunksByIndex: Map<Int, ByteArray>) {
        val locations = IntArray(1024)
        val timestamps = IntArray(1024)
        val ordered = chunksByIndex.entries
            .filter { it.key in 0 until 1024 && it.value.isNotEmpty() }
            .sortedBy { it.key }

        var nextSector = 2
        val positioned = ArrayList<Triple<Int, Int, ByteArray>>(ordered.size)
        val nowEpochSeconds = (System.currentTimeMillis() / 1000L).toInt()

        for ((index, record) in ordered) {
            val sectors = ceil(record.size / SECTOR_BYTES.toDouble()).toInt().coerceAtLeast(1)
            val offset = nextSector
            nextSector += sectors
            locations[index] = (offset shl 8) or (sectors and 0xFF)
            timestamps[index] = nowEpochSeconds
            positioned.add(Triple(index, offset, record))
        }

        val totalBytes = nextSector * SECTOR_BYTES
        val output = ByteBuffer.allocate(totalBytes).order(ByteOrder.BIG_ENDIAN)

        for (location in locations) output.putInt(location)
        for (timestamp in timestamps) output.putInt(timestamp)

        for ((_, offset, record) in positioned) {
            val target = offset * SECTOR_BYTES
            output.position(target)
            output.put(record)
        }

        Files.createDirectories(regionPath.parent)
        Files.write(regionPath, output.array())
    }

    private fun encodeChunkRecord(world: World, chunkPos: ChunkPos, generated: GeneratedChunk?): ByteArray {
        val rawNbt = encodeChunkNbt(world, chunkPos, generated)

        val compressed = ByteArrayOutputStream()
        DeflaterOutputStream(compressed).use { it.write(rawNbt) }
        val payload = compressed.toByteArray()

        val record = ByteArrayOutputStream(payload.size + 5)
        val out = DataOutputStream(record)
        out.writeInt(payload.size + 1)
        out.writeByte(2)
        out.write(payload)
        out.flush()
        return record.toByteArray()
    }

    private fun applySavedChunkToWorld(world: World, expectedChunkX: Int, expectedChunkZ: Int, record: ByteArray): Int {
        if (record.size < 5) return 0
        val declaredLength = ByteBuffer.wrap(record, 0, 4).order(ByteOrder.BIG_ENDIAN).int
        if (declaredLength <= 1) return 0
        val availablePayloadBytes = record.size - 5
        if (availablePayloadBytes <= 0) return 0
        val payloadLength = minOf(declaredLength - 1, availablePayloadBytes)
        val compression = record[4].toInt() and 0xFF
        val payloadStream = ByteArrayInputStream(record, 5, payloadLength)
        val nbtBytes = when (compression) {
            1 -> GZIPInputStream(payloadStream).use { it.readBytes() }
            2 -> InflaterInputStream(payloadStream).use { it.readBytes() }
            3 -> payloadStream.readBytes()
            else -> return 0
        }
        if (nbtBytes.isEmpty()) return 0

        val root = DataInputStream(ByteArrayInputStream(nbtBytes)).use { input ->
            readRootCompound(input)
        } ?: return 0
        val chunk = (root.entries["Level"] as? NbtCompoundTag) ?: root
        val persistedHeightmaps = decodePersistedHeightmaps(chunk.entries["Heightmaps"] as? NbtCompoundTag)
        if (persistedHeightmaps.isEmpty()) {
            clearPersistedChunkHeightmaps(world.key, ChunkPos(expectedChunkX, expectedChunkZ))
        } else {
            cachePersistedChunkHeightmaps(world.key, ChunkPos(expectedChunkX, expectedChunkZ), persistedHeightmaps)
        }
        val sections = chunk.entries["sections"] as? NbtListTag ?: return 0

        val skyBySectionY = HashMap<Int, ByteArray>()
        val blockBySectionY = HashMap<Int, ByteArray>()
        var appliedOverrides = 0
        for (sectionTag in sections.elements) {
            val section = sectionTag as? NbtCompoundTag ?: continue
            val sectionY = (section.entries["Y"] as? NbtByteTag)?.value?.toInt() ?: continue
            val skyLight = (section.entries["SkyLight"] as? NbtByteArrayTag)?.value
            if (skyLight != null && skyLight.isNotEmpty()) skyBySectionY[sectionY] = skyLight.copyOf()
            val blockLight = (section.entries["BlockLight"] as? NbtByteArrayTag)?.value
            if (blockLight != null && blockLight.isNotEmpty()) blockBySectionY[sectionY] = blockLight.copyOf()
            val blockStates = section.entries["block_states"] as? NbtCompoundTag ?: continue
            val paletteTag = blockStates.entries["palette"] as? NbtListTag ?: continue
            if (paletteTag.elements.isEmpty()) continue

            val paletteStateIds = IntArray(paletteTag.elements.size) { AIR_STATE_ID }
            for (idx in paletteTag.elements.indices) {
                val state = paletteTag.elements[idx] as? NbtCompoundTag ?: continue
                val blockKey = (state.entries["Name"] as? NbtStringTag)?.value ?: "minecraft:air"
                val properties = LinkedHashMap<String, String>()
                val propertiesTag = state.entries["Properties"] as? NbtCompoundTag
                if (propertiesTag != null) {
                    for ((name, valueTag) in propertiesTag.entries) {
                        val value = (valueTag as? NbtStringTag)?.value ?: continue
                        properties[name] = value
                    }
                }
                val stateId = BlockStateRegistry.stateId(blockKey, properties)
                    ?: BlockStateRegistry.defaultStateId(blockKey)
                    ?: AIR_STATE_ID
                paletteStateIds[idx] = stateId
            }

            val packed = (blockStates.entries["data"] as? NbtLongArrayTag)?.value
            val paletteIndexes = unpackPaletteIndexes(packed, paletteStateIds.size)
            val baseY = sectionY shl 4
            var i = 0
            for (localY in 0 until 16) {
                val blockY = baseY + localY
                if (blockY < WORLD_MIN_Y || blockY > WORLD_MAX_Y) {
                    i += 16 * 16
                    continue
                }
                for (localZ in 0 until 16) {
                    for (localX in 0 until 16) {
                        val paletteIndex = paletteIndexes[i++]
                        val stateId = paletteStateIds.getOrElse(paletteIndex) { AIR_STATE_ID }
                        val blockX = (expectedChunkX shl 4) + localX
                        val blockZ = (expectedChunkZ shl 4) + localZ
                        val currentState = world.blockStateAt(blockX, blockY, blockZ)
                        if (currentState == stateId) continue
                        world.setBlockStateFromChunkLoad(blockX, blockY, blockZ, stateId)
                        appliedOverrides++
                    }
                }
            }
        }
        val lighting = sectionLightArraysToPersistedLighting(skyBySectionY, blockBySectionY)
        if (lighting == null || appliedOverrides > 0) {
            clearPersistedChunkLighting(world.key, ChunkPos(expectedChunkX, expectedChunkZ))
        } else {
            cachePersistedChunkLighting(
                world.key,
                ChunkPos(expectedChunkX, expectedChunkZ),
                lighting
            )
        }
        val blockEntities = chunk.entries["block_entities"] as? NbtListTag
        val restoredChests = decodeChestBlockEntities(world.key, expectedChunkX, expectedChunkZ, blockEntities)
        PlayerSessionManager.replaceChestPersistenceForChunk(world.key, expectedChunkX, expectedChunkZ, restoredChests)
        return appliedOverrides
    }

    private fun unpackPaletteIndexes(data: LongArray?, paletteSize: Int): IntArray {
        if (paletteSize <= 1) return IntArray(SECTION_SIZE)
        if (data == null || data.isEmpty()) return IntArray(SECTION_SIZE)
        val bitsPerEntry = max(4, ceilLog2(paletteSize))
        val mask = if (bitsPerEntry >= 64) -1L else (1L shl bitsPerEntry) - 1L
        val out = IntArray(SECTION_SIZE)
        for (i in 0 until SECTION_SIZE) {
            val bitIndex = i * bitsPerEntry
            val longIndex = bitIndex ushr 6
            val bitOffset = bitIndex and 63
            if (longIndex >= data.size) break

            var raw = data[longIndex] ushr bitOffset
            val overflow = bitOffset + bitsPerEntry - 64
            if (overflow > 0 && longIndex + 1 < data.size) {
                raw = raw or (data[longIndex + 1] shl (bitsPerEntry - overflow))
            }
            out[i] = (raw and mask).toInt()
        }
        return out
    }

    private fun encodeChunkNbt(world: World, chunkPos: ChunkPos, generated: GeneratedChunk?): ByteArray {
        val hasBlockOverrides = world.changedBlocksInChunk(chunkPos.x, chunkPos.z).isNotEmpty()
        val sectionSkyLight = if (hasBlockOverrides) {
            emptyMap()
        } else {
            generated?.sectionLightArraysBySectionY(
                worldKey = world.key,
                useSky = true
            ).orEmpty()
        }
        val sectionBlockLight = if (hasBlockOverrides) {
            emptyMap()
        } else {
            generated?.sectionLightArraysBySectionY(
                worldKey = world.key,
                useSky = false
            ).orEmpty()
        }
        val nbt = NbtOutput()
        nbt.writeRootCompound {
            intTag("DataVersion", DEFAULT_DATA_VERSION)
            intTag("xPos", chunkPos.x)
            intTag("zPos", chunkPos.z)
            intTag("yPos", MIN_SECTION_Y)
            stringTag("Status", "minecraft:full")
            longTag("LastUpdate", 0L)
            longTag("InhabitedTime", 0L)
            byteTag("isLightOn", if (sectionSkyLight.isNotEmpty() || sectionBlockLight.isNotEmpty()) 1 else 0)

            compoundTag("Heightmaps") {
                val generatedHeightmaps = generated?.heightmaps.orEmpty()
                var wroteAny = false
                for (entry in generatedHeightmaps) {
                    val name = heightmapNbtNameFromTypeId(entry.typeId) ?: continue
                    longArrayTag(name, entry.values)
                    wroteAny = true
                }
                if (!wroteAny) {
                    val heightmap = buildHeightmapLongArray(world, chunkPos)
                    longArrayTag("WORLD_SURFACE", heightmap)
                    longArrayTag("MOTION_BLOCKING", heightmap)
                }
            }

            listTag("sections", NbtType.COMPOUND) {
                for (sectionY in MIN_SECTION_Y..MAX_SECTION_Y) {
                    compoundElement {
                        byteTag("Y", sectionY)
                        val sky = sectionSkyLight[sectionY]
                        if (sky != null && sky.isNotEmpty()) {
                            byteArrayTag("SkyLight", sky)
                        }
                        val block = sectionBlockLight[sectionY]
                        if (block != null && block.isNotEmpty()) {
                            byteArrayTag("BlockLight", block)
                        }
                        compoundTag("block_states") {
                            writeBlockStateContainer(world, chunkPos, sectionY)
                        }
                        compoundTag("biomes") {
                            listTag("palette", NbtType.STRING) {
                                stringElement(plainsBiomeName)
                            }
                        }
                    }
                }
            }

            listTag("block_entities", NbtType.COMPOUND) {
                writeChestBlockEntities(this, world, chunkPos)
            }
        }
        return nbt.toByteArray()
    }

    private fun writeChestBlockEntities(out: NbtListWriter, world: World, chunkPos: ChunkPos) {
        val chestStates = PlayerSessionManager.chestPersistenceSnapshotForChunk(world.key, chunkPos.x, chunkPos.z)
        val stateByPos = HashMap<Triple<Int, Int, Int>, PlayerSessionManager.PersistedChestState>(chestStates.size)
        for (entry in chestStates) {
            stateByPos[Triple(entry.x, entry.y, entry.z)] = entry
        }

        val emitted = HashSet<Triple<Int, Int, Int>>(chestStates.size)

        fun writeBlockEntityAt(x: Int, y: Int, z: Int, blockKey: String, persisted: PlayerSessionManager.PersistedChestState?) {
            val blockEntityId = chestBlockEntityIdForBlockKey(blockKey)
            out.compoundElement {
                stringTag("id", blockEntityId)
                intTag("x", x)
                intTag("y", y)
                intTag("z", z)
                listTag("Items", NbtType.COMPOUND) {
                    for (slot in 0 until 27) {
                        writeContainerItemElement(
                            list = this,
                            slot = slot,
                            itemId = persisted?.itemIds?.getOrElse(slot) { -1 } ?: -1,
                            count = persisted?.itemCounts?.getOrElse(slot) { 0 } ?: 0,
                            shulkerItemIds = persisted?.shulkerItemIds,
                            shulkerItemCounts = persisted?.shulkerItemCounts,
                            shulkerSectionIndex = slot
                        )
                    }
                }
            }
        }

        // Persist existing chest inventory states first.
        for (entry in chestStates) {
            val parsed = BlockStateRegistry.parsedState(world.blockStateAt(entry.x, entry.y, entry.z)) ?: continue
            val blockKey = parsed.blockKey
            if (!isPersistentChestBlockEntityId(blockKey)) continue
            val pos = Triple(entry.x, entry.y, entry.z)
            if (!emitted.add(pos)) continue
            writeBlockEntityAt(entry.x, entry.y, entry.z, blockKey, entry)
        }

        // Ensure every chest-like block in chunk has a block entity entry, even if never opened.
        val baseX = chunkPos.x shl 4
        val baseZ = chunkPos.z shl 4
        for (y in WORLD_MIN_Y..WORLD_MAX_Y) {
            for (localZ in 0 until 16) {
                for (localX in 0 until 16) {
                    val x = baseX + localX
                    val z = baseZ + localZ
                    val parsed = BlockStateRegistry.parsedState(world.blockStateAt(x, y, z)) ?: continue
                    val blockKey = parsed.blockKey
                    if (!isPersistentChestBlockEntityId(blockKey)) continue
                    val pos = Triple(x, y, z)
                    if (!emitted.add(pos)) continue
                    writeBlockEntityAt(x, y, z, blockKey, stateByPos[pos])
                }
            }
        }
    }

    private fun decodeChestBlockEntities(
        worldKey: String,
        chunkX: Int,
        chunkZ: Int,
        list: NbtListTag?
    ): List<PlayerSessionManager.PersistedChestState> {
        if (list == null || list.elementType != TAG_COMPOUND) return emptyList()
        val out = ArrayList<PlayerSessionManager.PersistedChestState>()
        for (element in list.elements) {
            val blockEntity = element as? NbtCompoundTag ?: continue
            val id = (blockEntity.entries["id"] as? NbtStringTag)?.value ?: continue
            if (!isPersistentChestBlockEntityId(id)) continue
            val x = (blockEntity.entries["x"] as? NbtIntTag)?.value ?: continue
            val y = (blockEntity.entries["y"] as? NbtIntTag)?.value ?: continue
            val z = (blockEntity.entries["z"] as? NbtIntTag)?.value ?: continue
            if ((x shr 4) != chunkX || (z shr 4) != chunkZ) continue

            val itemIds = IntArray(27) { -1 }
            val itemCounts = IntArray(27)
            val shulkerItemIds = IntArray(27 * 27) { -1 }
            val shulkerItemCounts = IntArray(27 * 27)
            val items = blockEntity.entries["Items"] as? NbtListTag
            decodeSimpleContainerList(items, itemIds, itemCounts)
            decodeContainerShulkerSectionsFromItems(items, shulkerItemIds, shulkerItemCounts)
            out.add(
                PlayerSessionManager.PersistedChestState(
                    worldKey = worldKey,
                    x = x,
                    y = y,
                    z = z,
                    itemIds = itemIds,
                    itemCounts = itemCounts,
                    shulkerItemIds = shulkerItemIds,
                    shulkerItemCounts = shulkerItemCounts
                )
            )
        }
        return out
    }

    private fun isPersistentChestBlockEntityId(id: String): Boolean {
        if (id == "minecraft:shulker_box") return true
        if (id.endsWith("_shulker_box")) return true
        if (id == "minecraft:chest" || id == "minecraft:trapped_chest") return true
        return id != "minecraft:ender_chest" && id.endsWith("_chest")
    }

    private fun chestBlockEntityIdForBlockKey(blockKey: String): String {
        if (blockKey == "minecraft:shulker_box" || blockKey.endsWith("_shulker_box")) {
            return "minecraft:shulker_box"
        }
        return if (blockKey == "minecraft:trapped_chest") {
            "minecraft:trapped_chest"
        } else {
            "minecraft:chest"
        }
    }

    private fun buildHeightmapLongArray(world: World, chunkPos: ChunkPos): LongArray {
        val values = IntArray(16 * 16)
        var idx = 0
        for (localZ in 0 until 16) {
            for (localX in 0 until 16) {
                val blockX = (chunkPos.x shl 4) + localX
                val blockZ = (chunkPos.z shl 4) + localZ
                var topY = WORLD_MIN_Y - 1
                for (y in WORLD_MAX_Y downTo WORLD_MIN_Y) {
                    if (world.blockStateAt(blockX, y, blockZ) != AIR_STATE_ID) {
                        topY = y
                        break
                    }
                }
                values[idx++] = if (topY >= WORLD_MIN_Y) topY + 1 else WORLD_MIN_Y
            }
        }
        return packValues(values, bitsPerEntry = 9)
    }

    private fun decodePersistedHeightmaps(tag: NbtCompoundTag?): List<HeightmapData> {
        if (tag == null || tag.entries.isEmpty()) return emptyList()
        val out = ArrayList<HeightmapData>(tag.entries.size)
        for ((name, valueTag) in tag.entries) {
            val values = (valueTag as? NbtLongArrayTag)?.value ?: continue
            val typeId = heightmapTypeIdFromNbtName(name) ?: continue
            out.add(HeightmapData(typeId = typeId, values = values.copyOf()))
        }
        return out
    }

    private fun heightmapTypeIdFromNbtName(name: String): Int? {
        return when (name) {
            "WORLD_SURFACE_WG" -> HEIGHTMAP_WORLD_SURFACE_WG_ID
            "WORLD_SURFACE" -> HEIGHTMAP_WORLD_SURFACE_ID
            "OCEAN_FLOOR_WG" -> HEIGHTMAP_OCEAN_FLOOR_WG_ID
            "OCEAN_FLOOR" -> HEIGHTMAP_OCEAN_FLOOR_ID
            "MOTION_BLOCKING" -> HEIGHTMAP_MOTION_BLOCKING_ID
            "MOTION_BLOCKING_NO_LEAVES" -> HEIGHTMAP_MOTION_BLOCKING_NO_LEAVES_ID
            else -> null
        }
    }

    private fun heightmapNbtNameFromTypeId(typeId: Int): String? {
        return when (typeId) {
            HEIGHTMAP_WORLD_SURFACE_WG_ID -> "WORLD_SURFACE_WG"
            HEIGHTMAP_WORLD_SURFACE_ID -> "WORLD_SURFACE"
            HEIGHTMAP_OCEAN_FLOOR_WG_ID -> "OCEAN_FLOOR_WG"
            HEIGHTMAP_OCEAN_FLOOR_ID -> "OCEAN_FLOOR"
            HEIGHTMAP_MOTION_BLOCKING_ID -> "MOTION_BLOCKING"
            HEIGHTMAP_MOTION_BLOCKING_NO_LEAVES_ID -> "MOTION_BLOCKING_NO_LEAVES"
            else -> null
        }
    }

    private fun NbtCompoundWriter.writeBlockStateContainer(world: World, chunkPos: ChunkPos, sectionY: Int) {
        val states = IntArray(SECTION_SIZE)
        val paletteToIndex = LinkedHashMap<Int, Int>()
        val paletteIds = ArrayList<Int>()
        val baseY = sectionY shl 4

        var i = 0
        for (localY in 0 until 16) {
            val blockY = baseY + localY
            for (localZ in 0 until 16) {
                for (localX in 0 until 16) {
                    val blockX = (chunkPos.x shl 4) + localX
                    val blockZ = (chunkPos.z shl 4) + localZ
                    val stateId = world.blockStateAt(blockX, blockY, blockZ)
                    val paletteIndex = paletteToIndex.getOrPut(stateId) {
                        val next = paletteIds.size
                        paletteIds.add(stateId)
                        next
                    }
                    states[i++] = paletteIndex
                }
            }
        }

        listTag("palette", NbtType.COMPOUND) {
            for (stateId in paletteIds) {
                val parsed = BlockStateRegistry.parsedState(stateId)
                compoundElement {
                    stringTag("Name", parsed?.blockKey ?: "minecraft:air")
                    val properties = parsed?.properties.orEmpty()
                    if (properties.isNotEmpty()) {
                        compoundTag("Properties") {
                            for ((k, v) in properties) {
                                stringTag(k, v)
                            }
                        }
                    }
                }
            }
        }

        if (paletteIds.size > 1) {
            val bits = max(4, ceilLog2(paletteIds.size))
            longArrayTag("data", packValues(states, bits))
        }
    }

    private fun GeneratedChunk.sectionLightArraysBySectionY(
        worldKey: String,
        useSky: Boolean
    ): Map<Int, ByteArray> {
        val presentMask = if (useSky) skyLightMask else blockLightMask
        val emptyMask = if (useSky) emptySkyLightMask else emptyBlockLightMask
        val arrays = if (useSky) skyLight else blockLight
        if (presentMask.isEmpty() || arrays.isEmpty()) return emptyMap()

        val sectionCount = maxOf(
            presentMask.size * Long.SIZE_BITS,
            emptyMask.size * Long.SIZE_BITS
        )
        if (sectionCount <= 0) return emptyMap()
        val indexes = decodeLightSectionIndexes(presentMask, emptyMask, sectionCount)
        val minSectionY = minSectionYForWorld(worldKey)
        val out = HashMap<Int, ByteArray>()
        for (sectionY in MIN_SECTION_Y..MAX_SECTION_Y) {
            val lightSection = sectionY - minSectionY + 1
            if (lightSection !in indexes.indices) continue
            val arrayIndex = indexes[lightSection]
            if (arrayIndex !in arrays.indices) continue
            val bytes = arrays[arrayIndex]
            if (bytes.isEmpty()) continue
            out[sectionY] = bytes.copyOf()
        }
        return out
    }

    private fun GeneratedChunk.toPersistedChunkLighting(): PersistedChunkLighting? {
        if (skyLightMask.isEmpty() && blockLightMask.isEmpty()) return null
        return PersistedChunkLighting(
            skyLightMask = skyLightMask.copyOf(),
            blockLightMask = blockLightMask.copyOf(),
            emptySkyLightMask = emptySkyLightMask.copyOf(),
            emptyBlockLightMask = emptyBlockLightMask.copyOf(),
            skyLight = skyLight.map { it.copyOf() },
            blockLight = blockLight.map { it.copyOf() }
        )
    }

    private fun PersistedChunkLighting.sectionLightArraysBySectionY(
        worldKey: String,
        useSky: Boolean
    ): Map<Int, ByteArray> {
        val presentMask = if (useSky) skyLightMask else blockLightMask
        val emptyMask = if (useSky) emptySkyLightMask else emptyBlockLightMask
        val arrays = if (useSky) skyLight else blockLight
        if (presentMask.isEmpty() || arrays.isEmpty()) return emptyMap()

        val sectionCount = maxOf(
            presentMask.size * Long.SIZE_BITS,
            emptyMask.size * Long.SIZE_BITS
        )
        if (sectionCount <= 0) return emptyMap()
        val indexes = decodeLightSectionIndexes(presentMask, emptyMask, sectionCount)
        val minSectionY = minSectionYForWorld(worldKey)
        val out = HashMap<Int, ByteArray>()
        for (sectionY in MIN_SECTION_Y..MAX_SECTION_Y) {
            val lightSection = sectionY - minSectionY + 1
            if (lightSection !in indexes.indices) continue
            val arrayIndex = indexes[lightSection]
            if (arrayIndex !in arrays.indices) continue
            val bytes = arrays[arrayIndex]
            if (bytes.isEmpty()) continue
            out[sectionY] = bytes.copyOf()
        }
        return out
    }

    private fun sectionLightArraysToPersistedLighting(
        skyBySectionY: Map<Int, ByteArray>,
        blockBySectionY: Map<Int, ByteArray>
    ): PersistedChunkLighting? {
        val sectionCount = (MAX_SECTION_Y - MIN_SECTION_Y + 1) + 2
        if (sectionCount <= 0) return null
        val maskWords = (sectionCount + 63) ushr 6
        val skyPresent = LongArray(maskWords)
        val skyEmpty = LongArray(maskWords) { -1L }
        val blockPresent = LongArray(maskWords)
        val blockEmpty = LongArray(maskWords) { -1L }
        val skyArrays = ArrayList<ByteArray>()
        val blockArrays = ArrayList<ByteArray>()

        for (sectionY in MIN_SECTION_Y..MAX_SECTION_Y) {
            val sectionIndex = sectionY - MIN_SECTION_Y + 1
            val sky = skyBySectionY[sectionY]
            if (sky != null && sky.isNotEmpty()) {
                setMaskBit(skyPresent, sectionIndex)
                clearMaskBit(skyEmpty, sectionIndex)
                skyArrays.add(sky.copyOf())
            }
            val block = blockBySectionY[sectionY]
            if (block != null && block.isNotEmpty()) {
                setMaskBit(blockPresent, sectionIndex)
                clearMaskBit(blockEmpty, sectionIndex)
                blockArrays.add(block.copyOf())
            }
        }
        trimUnusedMaskBits(skyEmpty, sectionCount)
        trimUnusedMaskBits(blockEmpty, sectionCount)
        if (skyArrays.isEmpty() && blockArrays.isEmpty()) return null
        return PersistedChunkLighting(
            skyLightMask = skyPresent,
            blockLightMask = blockPresent,
            emptySkyLightMask = skyEmpty,
            emptyBlockLightMask = blockEmpty,
            skyLight = skyArrays,
            blockLight = blockArrays
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

    private fun minSectionYForWorld(worldKey: String): Int {
        return when (worldKey) {
            "minecraft:overworld" -> WORLD_MIN_Y shr 4
            else -> WORLD_MIN_Y shr 4
        }
    }

    private fun isMaskBitSet(mask: LongArray, bitIndex: Int): Boolean {
        if (bitIndex < 0) return false
        val word = bitIndex ushr 6
        if (word !in mask.indices) return false
        val bit = bitIndex and 63
        return ((mask[word] ushr bit) and 1L) != 0L
    }

    private fun setMaskBit(mask: LongArray, bitIndex: Int) {
        if (bitIndex < 0) return
        val word = bitIndex ushr 6
        if (word !in mask.indices) return
        val bit = bitIndex and 63
        mask[word] = mask[word] or (1L shl bit)
    }

    private fun clearMaskBit(mask: LongArray, bitIndex: Int) {
        if (bitIndex < 0) return
        val word = bitIndex ushr 6
        if (word !in mask.indices) return
        val bit = bitIndex and 63
        mask[word] = mask[word] and (1L shl bit).inv()
    }

    private fun trimUnusedMaskBits(mask: LongArray, sectionCount: Int) {
        if (mask.isEmpty()) return
        val usedBitsLastWord = sectionCount and 63
        if (usedBitsLastWord == 0) return
        val keepMask = (1L shl usedBitsLastWord) - 1L
        val lastIndex = mask.lastIndex
        mask[lastIndex] = mask[lastIndex] and keepMask
    }

    private fun cachePersistedChunkLighting(worldKey: String, chunkPos: ChunkPos, lighting: PersistedChunkLighting?) {
        if (lighting == null) {
            clearPersistedChunkLighting(worldKey, chunkPos)
            return
        }
        val worldMap = persistedChunkLightingByWorld.computeIfAbsent(worldKey) { ConcurrentHashMap() }
        worldMap[chunkPos] = lighting
    }

    private fun cachePersistedChunkHeightmaps(worldKey: String, chunkPos: ChunkPos, heightmaps: List<HeightmapData>) {
        if (heightmaps.isEmpty()) {
            clearPersistedChunkHeightmaps(worldKey, chunkPos)
            return
        }
        val worldMap = persistedChunkHeightmapsByWorld.computeIfAbsent(worldKey) { ConcurrentHashMap() }
        worldMap[chunkPos] = heightmaps.map { HeightmapData(typeId = it.typeId, values = it.values.copyOf()) }
    }

    private fun clearPersistedChunkLighting(worldKey: String, chunkPos: ChunkPos) {
        persistedChunkLightingByWorld[worldKey]?.remove(chunkPos)
    }

    private fun clearPersistedChunkHeightmaps(worldKey: String, chunkPos: ChunkPos) {
        persistedChunkHeightmapsByWorld[worldKey]?.remove(chunkPos)
    }

    private fun ceilLog2(value: Int): Int {
        if (value <= 1) return 0
        return 32 - Integer.numberOfLeadingZeros(value - 1)
    }

    private fun packValues(values: IntArray, bitsPerEntry: Int): LongArray {
        if (bitsPerEntry <= 0 || values.isEmpty()) return LongArray(0)
        val resultLength = ceil(values.size * bitsPerEntry / 64.0).toInt()
        val out = LongArray(resultLength)
        val mask = if (bitsPerEntry >= 32) -1L else (1L shl bitsPerEntry) - 1L

        var bitIndex = 0
        for (value in values) {
            val raw = value.toLong() and mask
            val longIndex = bitIndex ushr 6
            val startBit = bitIndex and 63
            out[longIndex] = out[longIndex] or (raw shl startBit)
            val endBit = startBit + bitsPerEntry
            if (endBit > 64) {
                out[longIndex + 1] = out[longIndex + 1] or (raw ushr (64 - startBit))
            }
            bitIndex += bitsPerEntry
        }
        return out
    }

    private enum class NbtType(val id: Int) {
        END(0),
        BYTE(1),
        SHORT(2),
        INT(3),
        LONG(4),
        FLOAT(5),
        DOUBLE(6),
        BYTE_ARRAY(7),
        STRING(8),
        LIST(9),
        COMPOUND(10),
        INT_ARRAY(11),
        LONG_ARRAY(12)
    }

    private class NbtOutput {
        private val buffer = ByteArrayOutputStream(8192)
        private val out = DataOutputStream(buffer)

        fun writeRootCompound(body: NbtCompoundWriter.() -> Unit) {
            out.writeByte(NbtType.COMPOUND.id)
            writeString(out, "")
            val writer = NbtCompoundWriter(out)
            writer.body()
            out.writeByte(NbtType.END.id)
        }

        fun toByteArray(): ByteArray = buffer.toByteArray()
    }

    private class NbtCompoundWriter(private val out: DataOutputStream) {
        fun byteTag(name: String, value: Int) {
            tagHeader(NbtType.BYTE, name)
            out.writeByte(value)
        }

        fun shortTag(name: String, value: Int) {
            tagHeader(NbtType.SHORT, name)
            out.writeShort(value)
        }

        fun intTag(name: String, value: Int) {
            tagHeader(NbtType.INT, name)
            out.writeInt(value)
        }

        fun longTag(name: String, value: Long) {
            tagHeader(NbtType.LONG, name)
            out.writeLong(value)
        }

        fun floatTag(name: String, value: Float) {
            tagHeader(NbtType.FLOAT, name)
            out.writeFloat(value)
        }

        fun doubleTag(name: String, value: Double) {
            tagHeader(NbtType.DOUBLE, name)
            out.writeDouble(value)
        }

        fun stringTag(name: String, value: String) {
            tagHeader(NbtType.STRING, name)
            writeString(out, value)
        }

        fun byteArrayTag(name: String, values: ByteArray) {
            tagHeader(NbtType.BYTE_ARRAY, name)
            out.writeInt(values.size)
            out.write(values)
        }

        fun longArrayTag(name: String, values: LongArray) {
            tagHeader(NbtType.LONG_ARRAY, name)
            out.writeInt(values.size)
            for (v in values) out.writeLong(v)
        }

        fun intArrayTag(name: String, values: IntArray) {
            tagHeader(NbtType.INT_ARRAY, name)
            out.writeInt(values.size)
            for (v in values) out.writeInt(v)
        }

        fun compoundTag(name: String, body: NbtCompoundWriter.() -> Unit) {
            tagHeader(NbtType.COMPOUND, name)
            val nested = NbtCompoundWriter(out)
            nested.body()
            out.writeByte(NbtType.END.id)
        }

        fun listTag(name: String, elementType: NbtType, body: NbtListWriter.() -> Unit) {
            tagHeader(NbtType.LIST, name)
            val listBuffer = ByteArrayOutputStream()
            val listOut = DataOutputStream(listBuffer)
            val writer = NbtListWriter(listOut, elementType)
            writer.body()
            out.writeByte(elementType.id)
            out.writeInt(writer.size)
            out.write(listBuffer.toByteArray())
        }

        private fun tagHeader(type: NbtType, name: String) {
            out.writeByte(type.id)
            writeString(out, name)
        }
    }

    private class NbtListWriter(
        private val out: DataOutputStream,
        private val elementType: NbtType
    ) {
        var size: Int = 0
            private set

        fun stringElement(value: String) {
            require(elementType == NbtType.STRING)
            writeString(out, value)
            size++
        }

        fun floatElement(value: Float) {
            require(elementType == NbtType.FLOAT)
            out.writeFloat(value)
            size++
        }

        fun doubleElement(value: Double) {
            require(elementType == NbtType.DOUBLE)
            out.writeDouble(value)
            size++
        }

        fun compoundElement(body: NbtCompoundWriter.() -> Unit) {
            require(elementType == NbtType.COMPOUND)
            val writer = NbtCompoundWriter(out)
            writer.body()
            out.writeByte(NbtType.END.id)
            size++
        }
    }

    private fun writeString(out: DataOutputStream, value: String) {
        val bytes = value.toByteArray(Charsets.UTF_8)
        out.writeShort(bytes.size)
        out.write(bytes)
    }

    private fun readRootCompound(input: DataInputStream): NbtCompoundTag? {
        val rootType = input.readUnsignedByte()
        if (rootType != TAG_COMPOUND) return null
        readNbtString(input)
        return readTagPayload(input, TAG_COMPOUND) as? NbtCompoundTag
    }

    private fun readTagPayload(input: DataInputStream, type: Int): NbtTag {
        return when (type) {
            TAG_BYTE -> NbtByteTag(input.readByte())
            TAG_SHORT -> NbtShortTag(input.readShort())
            TAG_INT -> NbtIntTag(input.readInt())
            TAG_LONG -> NbtLongTag(input.readLong())
            TAG_FLOAT -> NbtFloatTag(input.readFloat())
            TAG_DOUBLE -> NbtDoubleTag(input.readDouble())
            TAG_BYTE_ARRAY -> {
                val size = input.readInt().coerceAtLeast(0)
                val bytes = ByteArray(size)
                input.readFully(bytes)
                NbtByteArrayTag(bytes)
            }
            TAG_STRING -> NbtStringTag(readNbtString(input))
            TAG_LIST -> {
                val elementType = input.readUnsignedByte()
                val size = input.readInt().coerceAtLeast(0)
                val values = ArrayList<NbtTag>(size)
                repeat(size) {
                    values.add(readTagPayload(input, elementType))
                }
                NbtListTag(elementType, values)
            }
            TAG_COMPOUND -> {
                val entries = LinkedHashMap<String, NbtTag>()
                while (true) {
                    val childType = input.readUnsignedByte()
                    if (childType == TAG_END) break
                    val name = readNbtString(input)
                    entries[name] = readTagPayload(input, childType)
                }
                NbtCompoundTag(entries)
            }
            TAG_INT_ARRAY -> {
                val size = input.readInt().coerceAtLeast(0)
                val values = IntArray(size)
                for (i in 0 until size) {
                    values[i] = input.readInt()
                }
                NbtIntArrayTag(values)
            }
            TAG_LONG_ARRAY -> {
                val size = input.readInt().coerceAtLeast(0)
                val values = LongArray(size)
                for (i in 0 until size) {
                    values[i] = input.readLong()
                }
                NbtLongArrayTag(values)
            }
            else -> throw EOFException("Unsupported NBT tag type: $type")
        }
    }

    private fun readNbtString(input: DataInputStream): String {
        val length = input.readUnsignedShort()
        val bytes = ByteArray(length)
        input.readFully(bytes)
        return String(bytes, StandardCharsets.UTF_8)
    }

    private sealed class NbtTag
    private data class NbtByteTag(val value: Byte) : NbtTag()
    private data class NbtShortTag(val value: Short) : NbtTag()
    private data class NbtIntTag(val value: Int) : NbtTag()
    private data class NbtLongTag(val value: Long) : NbtTag()
    private data class NbtFloatTag(val value: Float) : NbtTag()
    private data class NbtDoubleTag(val value: Double) : NbtTag()
    private data class NbtByteArrayTag(val value: ByteArray) : NbtTag()
    private data class NbtStringTag(val value: String) : NbtTag()
    private data class NbtListTag(val elementType: Int, val elements: MutableList<NbtTag>) : NbtTag()
    private data class NbtCompoundTag(val entries: LinkedHashMap<String, NbtTag>) : NbtTag()
    private data class NbtIntArrayTag(val value: IntArray) : NbtTag()
    private data class NbtLongArrayTag(val value: LongArray) : NbtTag()

    private class AutosaveThreadFactory : ThreadFactory {
        override fun newThread(r: Runnable): Thread {
            return Thread(
                {
                    autosaveThreadRef.set(Thread.currentThread())
                    r.run()
                },
                "aerogel-world-autosave"
            ).apply { isDaemon = true }
        }
    }
}
