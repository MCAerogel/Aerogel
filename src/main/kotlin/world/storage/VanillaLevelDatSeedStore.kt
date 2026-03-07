package org.macaroon3145.world.storage

import org.macaroon3145.Aerogel
import org.macaroon3145.world.SpawnPoint
import org.slf4j.LoggerFactory
import java.io.BufferedInputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.EOFException
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.util.concurrent.atomic.AtomicReference
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream
import java.util.zip.InflaterInputStream

object VanillaLevelDatSeedStore {
    data class TimeWeatherMetadata(
        val worldAgeTicks: Long,
        val timeOfDayTicks: Long,
        val clearWeatherTime: Int,
        val rainTime: Int,
        val thunderTime: Int,
        val isRaining: Boolean,
        val isThundering: Boolean
    )

    private val logger = LoggerFactory.getLogger(VanillaLevelDatSeedStore::class.java)
    private val worldDir: Path = Path.of("world")
    private val levelDat: Path = worldDir.resolve("level.dat")
    private val levelDatOld: Path = worldDir.resolve("level.dat_old")
    private val levelDatTmp: Path = worldDir.resolve("level.dat.tmp")
    private data class SpawnPointCacheState(
        val token: Long,
        val spawnPoint: SpawnPoint?
    )
    private val spawnPointCache = AtomicReference(
        SpawnPointCacheState(
            token = SPAWN_POINT_CACHE_UNINITIALIZED_TOKEN,
            spawnPoint = null
        )
    )

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

    private val builtinWorldKeyToDimension = mapOf(
        "minecraft:overworld" to "minecraft:overworld",
        "minecraft:the_nether" to "minecraft:the_nether",
        "minecraft:the_end" to "minecraft:the_end"
    )
    private val builtinWorldKeyToGeneratorSettings = mapOf(
        "minecraft:overworld" to "minecraft:overworld",
        "minecraft:the_nether" to "minecraft:nether",
        "minecraft:the_end" to "minecraft:end"
    )
    // Minecraft 1.21.11 DataVersion (matches SharedConstants.WORLD_VERSION).
    private const val DEFAULT_DATA_VERSION = 4671
    // Vanilla level.dat storage format version (anvil).
    private const val DEFAULT_LEVEL_STORAGE_VERSION = 19133
    private const val DEFAULT_SPAWN_X = 0
    private const val DEFAULT_SPAWN_Y = 64
    private const val DEFAULT_SPAWN_Z = 0
    private const val SPAWN_POINT_CACHE_UNINITIALIZED_TOKEN = Long.MIN_VALUE
    private const val LEVEL_DAT_MISSING_TOKEN = Long.MIN_VALUE + 1

    private enum class CompressionFormat {
        GZIP,
        ZLIB,
        NONE
    }

    private data class LevelDatDocument(
        val rootName: String,
        val root: NbtCompound,
        val compression: CompressionFormat
    )

    fun load(): Map<String, Long> {
        val document = readLevelDatDocument() ?: return emptyMap()
        val data = document.root.entries["Data"] as? NbtCompound ?: document.root
        val out = LinkedHashMap<String, Long>()

        val worldGenSettings = data.entries["WorldGenSettings"] as? NbtCompound
        val dimensions = worldGenSettings?.entries?.get("dimensions") as? NbtCompound
        if (dimensions != null) {
            for ((worldKey, dimensionKey) in builtinWorldKeyToDimension) {
                val dim = dimensions.entries[dimensionKey] as? NbtCompound ?: continue
                val generator = dim.entries["generator"] as? NbtCompound ?: continue
                val seed = asLong(generator.entries["seed"]) ?: continue
                out[worldKey] = seed
            }
        }

        val fallbackSeed = asLong(worldGenSettings?.entries?.get("seed"))
            ?: asLong(data.entries["RandomSeed"])
        if (fallbackSeed != null) {
            out.putIfAbsent("minecraft:overworld", fallbackSeed)
            out.putIfAbsent("minecraft:the_nether", fallbackSeed)
            out.putIfAbsent("minecraft:the_end", fallbackSeed)
        }
        return out
    }

    fun loadTimeWeatherMetadata(): TimeWeatherMetadata? {
        val document = readLevelDatDocument() ?: return null
        val data = document.root.entries["Data"] as? NbtCompound ?: document.root
        val worldAge = asLong(data.entries["Time"])
            ?: asLong(data.entries["GameTime"])
            ?: return null
        val dayTime = asLong(data.entries["DayTime"]) ?: worldAge
        val clearWeatherTime = asInt(data.entries["clearWeatherTime"]) ?: 0
        val rainTime = asInt(data.entries["rainTime"]) ?: 0
        val thunderTime = asInt(data.entries["thunderTime"]) ?: 0
        val isRaining = asBoolean(data.entries["raining"]) ?: (rainTime > 0)
        val isThundering = asBoolean(data.entries["thundering"]) ?: (thunderTime > 0)
        return TimeWeatherMetadata(
            worldAgeTicks = worldAge,
            timeOfDayTicks = dayTime,
            clearWeatherTime = clearWeatherTime,
            rainTime = rainTime,
            thunderTime = thunderTime,
            isRaining = isRaining,
            isThundering = isThundering
        )
    }

    fun loadSpawnPoint(): SpawnPoint? {
        val token = currentLevelDatMtimeOrMissingToken()
        val cached = spawnPointCache.get()
        if (token == cached.token) {
            return cached.spawnPoint
        }
        while (true) {
            val current = spawnPointCache.get()
            if (token == current.token) {
                return current.spawnPoint
            }
            val loaded = if (token == LEVEL_DAT_MISSING_TOKEN) null else loadSpawnPointUncached()
            val updated = SpawnPointCacheState(token = token, spawnPoint = loaded)
            if (spawnPointCache.compareAndSet(current, updated)) {
                return loaded
            }
        }
    }

    private fun loadSpawnPointUncached(): SpawnPoint? {
        val document = readLevelDatDocument() ?: return null
        val data = document.root.entries["Data"] as? NbtCompound ?: document.root
        val spawnX = asInt(data.entries["SpawnX"]) ?: return null
        val spawnY = asInt(data.entries["SpawnY"]) ?: return null
        val spawnZ = asInt(data.entries["SpawnZ"]) ?: return null
        return SpawnPoint(
            x = spawnX + 0.5,
            y = spawnY.toDouble(),
            z = spawnZ + 0.5
        )
    }

    private fun currentLevelDatMtimeOrMissingToken(): Long {
        if (!Files.isRegularFile(levelDat)) return LEVEL_DAT_MISSING_TOKEN
        return runCatching { Files.getLastModifiedTime(levelDat).toMillis() }
            .getOrElse { LEVEL_DAT_MISSING_TOKEN }
    }

    fun save(seeds: Map<String, Long>, timeWeather: TimeWeatherMetadata? = null) {
        if (seeds.isEmpty()) return
        runCatching {
            Files.createDirectories(worldDir)
            val hadAnyLevelDat = Files.isRegularFile(levelDat) || Files.isRegularFile(levelDatOld)
            val existing = readLevelDatDocument()
            if (existing == null && hadAnyLevelDat) {
                logger.warn(
                    "Skipping level.dat write because existing metadata is unreadable: {}",
                    levelDat.toAbsolutePath()
                )
                return@runCatching
            }
            val effective = existing
                ?: runtimeTemplateLevelDatDocument()
            if (effective == null) {
                logger.info(
                    "Skipping level.dat write because source metadata is unavailable: {}",
                    levelDat.toAbsolutePath()
                )
                return@runCatching
            }
            val root = effective.root
            val rootName = effective.rootName
            val compression = effective.compression
            val data = ensureCompound(root, "Data")
            val preservedSpawnX = data.entries["SpawnX"]
            val preservedSpawnY = data.entries["SpawnY"]
            val preservedSpawnZ = data.entries["SpawnZ"]
            data.entries["version"] = NbtInt(DEFAULT_LEVEL_STORAGE_VERSION)
            data.entries["DataVersion"] = NbtInt(DEFAULT_DATA_VERSION)
            root.entries["DataVersion"] = NbtInt(DEFAULT_DATA_VERSION)
            val version = ensureCompound(data, "Version")
            version.entries["Id"] = NbtInt(DEFAULT_DATA_VERSION)
            version.entries["Name"] = NbtString(Aerogel.VERSION)
            version.entries["Series"] = NbtString("main")
            version.entries["Snapshot"] = NbtByte(0)
            data.entries["DataPacks"] = NbtCompound(
                linkedMapOf(
                    "Enabled" to NbtList(TAG_STRING, mutableListOf()),
                    "Disabled" to NbtList(TAG_STRING, mutableListOf())
                )
            )
            data.entries.remove("Bukkit.Version")
            data.entries.remove("ServerBrands")

            if (timeWeather != null) {
                data.entries["Time"] = NbtLong(timeWeather.worldAgeTicks)
                data.entries["GameTime"] = NbtLong(timeWeather.worldAgeTicks)
                data.entries["DayTime"] = NbtLong(timeWeather.timeOfDayTicks)
                data.entries["clearWeatherTime"] = NbtInt(timeWeather.clearWeatherTime)
                data.entries["rainTime"] = NbtInt(timeWeather.rainTime)
                data.entries["thunderTime"] = NbtInt(timeWeather.thunderTime)
                data.entries["raining"] = NbtByte(if (timeWeather.isRaining) 1 else 0)
                data.entries["thundering"] = NbtByte(if (timeWeather.isThundering) 1 else 0)
            } else {
                if (asLong(data.entries["Time"]) == null) data.entries["Time"] = NbtLong(0L)
                if (asLong(data.entries["GameTime"]) == null) data.entries["GameTime"] = NbtLong(0L)
                if (asLong(data.entries["DayTime"]) == null) data.entries["DayTime"] = NbtLong(0L)
            }
            data.entries["SpawnX"] = preservedSpawnX ?: NbtInt(DEFAULT_SPAWN_X)
            data.entries["SpawnY"] = preservedSpawnY ?: NbtInt(DEFAULT_SPAWN_Y)
            data.entries["SpawnZ"] = preservedSpawnZ ?: NbtInt(DEFAULT_SPAWN_Z)

            writeLevelDatDocument(LevelDatDocument(rootName = rootName, root = root, compression = compression))
        }.onFailure {
            logger.warn("Failed to write level.dat seed metadata: {}", levelDat.toAbsolutePath(), it)
            runCatching { Files.deleteIfExists(levelDatTmp) }
        }
    }

    fun saveSpawnPoint(spawn: SpawnPoint) {
        runCatching {
            val hadAnyLevelDat = Files.isRegularFile(levelDat) || Files.isRegularFile(levelDatOld)
            val existing = readLevelDatDocument()
            if (existing == null && hadAnyLevelDat) {
                logger.warn(
                    "Skipping level.dat spawn write because existing metadata is unreadable: {}",
                    levelDat.toAbsolutePath()
                )
                return@runCatching
            }
            val baseSeeds = normalizedSeeds(load())
            val effective = existing
                ?: runtimeTemplateLevelDatDocument()
            if (effective == null) {
                logger.info(
                    "Skipping level.dat spawn write because source metadata is unavailable: {}",
                    levelDat.toAbsolutePath()
                )
                return@runCatching
            }
            val root = effective.root
            val data = ensureCompound(root, "Data")
            val spawnX = kotlin.math.floor(spawn.x).toInt()
            val spawnY = spawn.y.toInt()
            val spawnZ = kotlin.math.floor(spawn.z).toInt()
            val currentX = asInt(data.entries["SpawnX"])
            val currentY = asInt(data.entries["SpawnY"])
            val currentZ = asInt(data.entries["SpawnZ"])
            if (currentX == spawnX && currentY == spawnY && currentZ == spawnZ) {
                return@runCatching
            }
            data.entries["SpawnX"] = NbtInt(spawnX)
            data.entries["SpawnY"] = NbtInt(spawnY)
            data.entries["SpawnZ"] = NbtInt(spawnZ)
            writeLevelDatDocument(
                LevelDatDocument(
                    rootName = effective.rootName,
                    root = root,
                    compression = effective.compression
                )
            )
            val updatedSpawnPoint = SpawnPoint(
                x = spawnX + 0.5,
                y = spawnY.toDouble(),
                z = spawnZ + 0.5
            )
            val updatedToken = currentLevelDatMtimeOrMissingToken()
            spawnPointCache.set(SpawnPointCacheState(token = updatedToken, spawnPoint = updatedSpawnPoint))
        }.onFailure {
            logger.warn("Failed to write level.dat spawn metadata: {}", levelDat.toAbsolutePath(), it)
        }
    }

    private fun createNewLevelDatDocument(
        seeds: Map<String, Long>,
        timeWeather: TimeWeatherMetadata?
    ): LevelDatDocument {
        val normalized = normalizedSeeds(seeds)
        val overworldSeed = normalized["minecraft:overworld"] ?: 0L

        val dataEntries = linkedMapOf<String, NbtTag>()
        dataEntries["version"] = NbtInt(DEFAULT_LEVEL_STORAGE_VERSION)
        dataEntries["DataVersion"] = NbtInt(DEFAULT_DATA_VERSION)
        dataEntries["RandomSeed"] = NbtLong(overworldSeed)
        dataEntries["SpawnX"] = NbtInt(DEFAULT_SPAWN_X)
        dataEntries["SpawnY"] = NbtInt(DEFAULT_SPAWN_Y)
        dataEntries["SpawnZ"] = NbtInt(DEFAULT_SPAWN_Z)
        dataEntries["Time"] = NbtLong(timeWeather?.worldAgeTicks ?: 0L)
        dataEntries["GameTime"] = NbtLong(timeWeather?.worldAgeTicks ?: 0L)
        dataEntries["DayTime"] = NbtLong(timeWeather?.timeOfDayTicks ?: 0L)
        dataEntries["clearWeatherTime"] = NbtInt(timeWeather?.clearWeatherTime ?: 0)
        dataEntries["rainTime"] = NbtInt(timeWeather?.rainTime ?: 0)
        dataEntries["thunderTime"] = NbtInt(timeWeather?.thunderTime ?: 0)
        dataEntries["raining"] = NbtByte(if (timeWeather?.isRaining == true) 1 else 0)
        dataEntries["thundering"] = NbtByte(if (timeWeather?.isThundering == true) 1 else 0)

        dataEntries["Version"] = NbtCompound(
            linkedMapOf(
                "Id" to NbtInt(DEFAULT_DATA_VERSION),
                "Name" to NbtString(Aerogel.VERSION),
                "Series" to NbtString("main"),
                "Snapshot" to NbtByte(0)
            )
        )

        val dimensionEntries = linkedMapOf<String, NbtTag>()
        for ((worldKey, dimensionKey) in builtinWorldKeyToDimension) {
            val seed = normalized[worldKey] ?: continue
            val settingKey = builtinWorldKeyToGeneratorSettings[worldKey] ?: "minecraft:overworld"
            val biomeSource: NbtCompound = when (worldKey) {
                "minecraft:the_end" -> NbtCompound(
                    linkedMapOf(
                        "type" to NbtString("minecraft:the_end"),
                        "seed" to NbtLong(seed)
                    )
                )
                "minecraft:the_nether" -> NbtCompound(
                    linkedMapOf(
                        "type" to NbtString("minecraft:multi_noise"),
                        "preset" to NbtString("minecraft:nether"),
                        "seed" to NbtLong(seed)
                    )
                )
                else -> NbtCompound(
                    linkedMapOf(
                        "type" to NbtString("minecraft:multi_noise"),
                        "preset" to NbtString("minecraft:overworld"),
                        "seed" to NbtLong(seed)
                    )
                )
            }
            dimensionEntries[dimensionKey] = NbtCompound(
                linkedMapOf(
                    "type" to NbtString(dimensionKey),
                    "generator" to NbtCompound(
                        linkedMapOf(
                            "type" to NbtString("minecraft:noise"),
                            "biome_source" to biomeSource,
                            "settings" to NbtString(settingKey),
                            "seed" to NbtLong(seed)
                        )
                    )
                )
            )
        }

        dataEntries["WorldGenSettings"] = NbtCompound(
            linkedMapOf(
                "seed" to NbtLong(overworldSeed),
                "generate_features" to NbtByte(1),
                "bonus_chest" to NbtByte(0),
                "dimensions" to NbtCompound(dimensionEntries)
            )
        )

        val rootEntries = linkedMapOf<String, NbtTag>(
            "DataVersion" to NbtInt(DEFAULT_DATA_VERSION),
            "Data" to NbtCompound(dataEntries)
        )
        return LevelDatDocument(
            rootName = "",
            root = NbtCompound(rootEntries),
            compression = CompressionFormat.GZIP
        )
    }

    private fun normalizedSeeds(seeds: Map<String, Long>): Map<String, Long> {
        val base = seeds["minecraft:overworld"] ?: seeds.values.firstOrNull() ?: 0L
        return linkedMapOf(
            "minecraft:overworld" to (seeds["minecraft:overworld"] ?: base),
            "minecraft:the_nether" to (seeds["minecraft:the_nether"] ?: base),
            "minecraft:the_end" to (seeds["minecraft:the_end"] ?: base)
        )
    }

    private fun runtimeTemplateLevelDatDocument(): LevelDatDocument? {
        val runtimeWorldDir = Path.of(".aerogel-cache").resolve("folia").resolve("runtime").resolve("world")
        val candidates = listOf(
            runtimeWorldDir.resolve("level.dat"),
            runtimeWorldDir.resolve("level.dat_old")
        )
        for (path in candidates) {
            val document = readLevelDatDocument(path, logAsPrimary = false)
            if (document != null) return document
        }
        return null
    }

    fun hasPositiveDataVersion(path: Path = levelDat): Boolean {
        val document = readLevelDatDocument(path, logAsPrimary = false) ?: return false
        val data = document.root.entries["Data"] as? NbtCompound ?: document.root
        val dataVersion = asLong(data.entries["DataVersion"]) ?: asLong(document.root.entries["DataVersion"]) ?: return false
        return dataVersion > 0L
    }

    private fun readLevelDatDocument(): LevelDatDocument? {
        readLevelDatDocument(levelDat, logAsPrimary = true)?.let { return it }
        return readLevelDatDocument(levelDatOld, logAsPrimary = false)
    }

    private fun readLevelDatDocument(path: Path, logAsPrimary: Boolean): LevelDatDocument? {
        if (!Files.isRegularFile(path)) return null
        return runCatching {
            BufferedInputStream(Files.newInputStream(path)).use { raw ->
                val compression = detectCompressionFormat(raw)
                val wrapped = when (compression) {
                    CompressionFormat.GZIP -> GZIPInputStream(raw)
                    CompressionFormat.ZLIB -> InflaterInputStream(raw)
                    CompressionFormat.NONE -> raw
                }
                DataInputStream(wrapped).use { input ->
                    val rootType = input.readUnsignedByte()
                    if (rootType != TAG_COMPOUND) {
                        throw IllegalStateException("Unsupported level.dat root tag type: $rootType")
                    }
                    val rootName = readString(input)
                    val root = readTagPayload(input, TAG_COMPOUND) as NbtCompound
                    LevelDatDocument(rootName = rootName, root = root, compression = compression)
                }
            }
        }.getOrElse {
            if (logAsPrimary) {
                logger.warn("Failed to read level.dat metadata: {}", path.toAbsolutePath(), it)
            } else {
                logger.info("Failed to read fallback level metadata: {}", path.toAbsolutePath())
            }
            null
        }
    }

    private fun detectCompressionFormat(input: BufferedInputStream): CompressionFormat {
        input.mark(2)
        val first = input.read()
        val second = input.read()
        input.reset()
        if (first == 0x1F && second == 0x8B) return CompressionFormat.GZIP
        if (first == 0x78) return CompressionFormat.ZLIB
        return CompressionFormat.NONE
    }

    private fun writeLevelDatDocument(document: LevelDatDocument) {
        DataOutputStream(
            when (document.compression) {
                CompressionFormat.GZIP -> GZIPOutputStream(Files.newOutputStream(levelDatTmp))
                CompressionFormat.ZLIB -> java.util.zip.DeflaterOutputStream(Files.newOutputStream(levelDatTmp))
                CompressionFormat.NONE -> Files.newOutputStream(levelDatTmp)
            }
        ).use { output ->
            output.writeByte(TAG_COMPOUND)
            writeString(output, document.rootName)
            writeTagPayload(output, TAG_COMPOUND, document.root)
        }
        runCatching {
            if (Files.isRegularFile(levelDat)) {
                Files.copy(levelDat, levelDatOld, StandardCopyOption.REPLACE_EXISTING)
            }
        }
        Files.move(
            levelDatTmp,
            levelDat,
            StandardCopyOption.REPLACE_EXISTING,
            StandardCopyOption.ATOMIC_MOVE
        )
    }

    private fun ensureCompound(parent: NbtCompound, name: String): NbtCompound {
        val existing = parent.entries[name] as? NbtCompound
        if (existing != null) return existing
        val created = NbtCompound(linkedMapOf())
        parent.entries[name] = created
        return created
    }

    private fun asLong(tag: NbtTag?): Long? {
        return when (tag) {
            is NbtLong -> tag.value
            is NbtInt -> tag.value.toLong()
            is NbtShort -> tag.value.toLong()
            is NbtByte -> tag.value.toLong()
            else -> null
        }
    }

    private fun asInt(tag: NbtTag?): Int? {
        return when (tag) {
            is NbtInt -> tag.value
            is NbtLong -> tag.value.toInt()
            is NbtShort -> tag.value.toInt()
            is NbtByte -> tag.value.toInt()
            else -> null
        }
    }

    private fun asBoolean(tag: NbtTag?): Boolean? {
        return when (tag) {
            is NbtByte -> tag.value.toInt() != 0
            is NbtShort -> tag.value.toInt() != 0
            is NbtInt -> tag.value != 0
            is NbtLong -> tag.value != 0L
            else -> null
        }
    }

    private fun readTagPayload(input: DataInputStream, type: Int): NbtTag {
        return when (type) {
            TAG_BYTE -> NbtByte(input.readByte())
            TAG_SHORT -> NbtShort(input.readShort())
            TAG_INT -> NbtInt(input.readInt())
            TAG_LONG -> NbtLong(input.readLong())
            TAG_FLOAT -> NbtFloat(input.readFloat())
            TAG_DOUBLE -> NbtDouble(input.readDouble())
            TAG_BYTE_ARRAY -> {
                val size = input.readInt().coerceAtLeast(0)
                val bytes = ByteArray(size)
                input.readFully(bytes)
                NbtByteArray(bytes)
            }
            TAG_STRING -> NbtString(readString(input))
            TAG_LIST -> {
                val elementType = input.readUnsignedByte()
                val size = input.readInt().coerceAtLeast(0)
                val elements = ArrayList<NbtTag>(size)
                repeat(size) {
                    elements.add(readTagPayload(input, elementType))
                }
                NbtList(elementType, elements)
            }
            TAG_COMPOUND -> {
                val entries = linkedMapOf<String, NbtTag>()
                while (true) {
                    val childType = input.readUnsignedByte()
                    if (childType == TAG_END) break
                    val name = readString(input)
                    entries[name] = readTagPayload(input, childType)
                }
                NbtCompound(entries)
            }
            TAG_INT_ARRAY -> {
                val size = input.readInt().coerceAtLeast(0)
                val values = IntArray(size)
                for (i in 0 until size) values[i] = input.readInt()
                NbtIntArray(values)
            }
            TAG_LONG_ARRAY -> {
                val size = input.readInt().coerceAtLeast(0)
                val values = LongArray(size)
                for (i in 0 until size) values[i] = input.readLong()
                NbtLongArray(values)
            }
            else -> throw EOFException("Unsupported NBT tag type: $type")
        }
    }

    private fun writeTagPayload(output: DataOutputStream, type: Int, tag: NbtTag) {
        when (type) {
            TAG_BYTE -> output.writeByte((tag as NbtByte).value.toInt())
            TAG_SHORT -> output.writeShort((tag as NbtShort).value.toInt())
            TAG_INT -> output.writeInt((tag as NbtInt).value)
            TAG_LONG -> output.writeLong((tag as NbtLong).value)
            TAG_FLOAT -> output.writeFloat((tag as NbtFloat).value)
            TAG_DOUBLE -> output.writeDouble((tag as NbtDouble).value)
            TAG_BYTE_ARRAY -> {
                val bytes = (tag as NbtByteArray).value
                output.writeInt(bytes.size)
                output.write(bytes)
            }
            TAG_STRING -> writeString(output, (tag as NbtString).value)
            TAG_LIST -> {
                val list = tag as NbtList
                output.writeByte(list.elementType)
                output.writeInt(list.elements.size)
                for (entry in list.elements) {
                    writeTagPayload(output, list.elementType, entry)
                }
            }
            TAG_COMPOUND -> {
                val compound = tag as NbtCompound
                for ((name, value) in compound.entries) {
                    output.writeByte(value.type)
                    writeString(output, name)
                    writeTagPayload(output, value.type, value)
                }
                output.writeByte(TAG_END)
            }
            TAG_INT_ARRAY -> {
                val values = (tag as NbtIntArray).value
                output.writeInt(values.size)
                for (value in values) output.writeInt(value)
            }
            TAG_LONG_ARRAY -> {
                val values = (tag as NbtLongArray).value
                output.writeInt(values.size)
                for (value in values) output.writeLong(value)
            }
            else -> throw IllegalStateException("Unsupported NBT tag type for write: $type")
        }
    }

    private fun readString(input: DataInputStream): String {
        val length = input.readUnsignedShort()
        val bytes = ByteArray(length)
        input.readFully(bytes)
        return String(bytes, StandardCharsets.UTF_8)
    }

    private fun writeString(output: DataOutputStream, value: String) {
        val bytes = value.toByteArray(StandardCharsets.UTF_8)
        output.writeShort(bytes.size)
        output.write(bytes)
    }

    private sealed class NbtTag(open val type: Int)
    private data class NbtByte(val value: Byte) : NbtTag(TAG_BYTE)
    private data class NbtShort(val value: Short) : NbtTag(TAG_SHORT)
    private data class NbtInt(val value: Int) : NbtTag(TAG_INT)
    private data class NbtLong(val value: Long) : NbtTag(TAG_LONG)
    private data class NbtFloat(val value: Float) : NbtTag(TAG_FLOAT)
    private data class NbtDouble(val value: Double) : NbtTag(TAG_DOUBLE)
    private data class NbtByteArray(val value: ByteArray) : NbtTag(TAG_BYTE_ARRAY)
    private data class NbtString(val value: String) : NbtTag(TAG_STRING)
    private data class NbtList(val elementType: Int, val elements: MutableList<NbtTag>) : NbtTag(TAG_LIST)
    private data class NbtCompound(val entries: LinkedHashMap<String, NbtTag>) : NbtTag(TAG_COMPOUND)
    private data class NbtIntArray(val value: IntArray) : NbtTag(TAG_INT_ARRAY)
    private data class NbtLongArray(val value: LongArray) : NbtTag(TAG_LONG_ARRAY)
}
