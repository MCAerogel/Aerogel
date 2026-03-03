package org.macaroon3145.ui

import com.formdev.flatlaf.FlatDarkLaf
import com.formdev.flatlaf.FlatLightLaf
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.macaroon3145.config.ServerConfig
import org.macaroon3145.i18n.ServerI18n
import org.macaroon3145.network.codec.BlockStateRegistry
import org.macaroon3145.network.handler.PlayerSessionManager
import org.macaroon3145.perf.PerformanceMonitor
import org.macaroon3145.world.WorldManager
import org.jetbrains.skia.Font as SkiaFont
import org.jetbrains.skia.FilterMipmap
import org.jetbrains.skia.FilterMode
import org.jetbrains.skia.Image as SkiaImage
import org.jetbrains.skia.ImageInfo
import org.jetbrains.skia.MipmapMode
import org.jetbrains.skia.Paint as SkiaPaint
import org.jetbrains.skia.PaintMode
import org.jetbrains.skia.Rect
import org.jetbrains.skia.SamplingMode
import org.jetbrains.skia.TextLine
import org.jetbrains.skia.ColorType
import org.jetbrains.skia.ColorAlphaType
import org.jetbrains.skiko.SkiaLayer
import org.jetbrains.skiko.SkiaLayerRenderDelegate
import org.jetbrains.skiko.SkikoRenderDelegate
import java.awt.BasicStroke
import java.awt.BorderLayout
import java.awt.CardLayout
import java.awt.Color
import java.awt.Cursor
import java.awt.Dimension
import java.awt.FlowLayout
import java.awt.Font
import java.awt.GradientPaint
import java.awt.Graphics
import java.awt.Graphics2D
import java.awt.GraphicsEnvironment
import java.awt.GridLayout
import java.awt.Point
import java.awt.RenderingHints
import java.awt.Rectangle
import java.awt.geom.Path2D
import java.awt.image.BufferedImage
import java.io.BufferedReader
import java.io.InputStreamReader
import java.lang.management.ManagementFactory
import java.net.URL
import java.time.Duration
import java.text.DecimalFormat
import java.util.Base64
import java.util.Locale
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import jdk.jfr.consumer.RecordingStream
import javax.imageio.ImageIO
import javax.swing.BorderFactory
import javax.swing.JFrame
import javax.swing.JLabel
import javax.swing.JPanel
import javax.swing.JScrollPane
import javax.swing.JTable
import javax.swing.SwingConstants
import javax.swing.SwingUtilities
import javax.swing.UIManager
import javax.swing.Timer
import javax.swing.table.AbstractTableModel
import kotlin.math.abs
import kotlin.math.ceil
import kotlin.math.exp
import kotlin.math.floor
import kotlin.math.ln
import kotlin.math.max
import kotlin.math.min
import kotlin.math.roundToInt
import kotlin.math.pow

object ServerDashboard {
    private const val GRAPH_HISTORY = 36_000 // 30 minutes at 20 TPS
    private const val MAX_CHUNK_LINES = 64
    private const val MIN_LISTED_MSPT = 1.0e-9
    private const val CHUNK_VIEW_LIST = "list"
    private const val CHUNK_VIEW_MAP = "map"
    private const val AIR_STATE_ID = 0
    private const val WORLD_MIN_Y = -64
    private const val WORLD_MAX_Y = 319
    private const val VANILLA_BRIGHTNESS_LOW = 180
    private const val VANILLA_BRIGHTNESS_NORMAL = 220
    private const val VANILLA_BRIGHTNESS_HIGH = 255
    private const val SKIN_PART_HAT_BIT = 0x40
    private const val PLAYER_HEAD_TEXTURE_SIZE = 8
    private const val MAX_TEXTURE_QUEUE_PER_TICK = 24

    private val started = AtomicBoolean(false)
    private var frame: JFrame? = null
    private val skinJson = Json { ignoreUnknownKeys = true }
    private val skinUrlByProperty = ConcurrentHashMap<String, String>()
    private val jfrLeakEvents = ConcurrentLinkedQueue<JfrAfterGcSample>()
    private val jfrLeakStreamStarted = AtomicBoolean(false)
    private val jfrLeakExecutor = Executors.newSingleThreadExecutor { runnable ->
        Thread(runnable, "dashboard-jfr-leak-stream").apply { isDaemon = true }
    }
    private val chunkTextureExecutor = Executors.newFixedThreadPool(1) { runnable ->
        Thread(runnable, "dashboard-chunk-texture-worker").apply { isDaemon = true }
    }
    private val pendingChunkTextureJobs = ConcurrentHashMap.newKeySet<ChunkTextureJobKey>()
    private val completedChunkTextureJobs = ConcurrentLinkedQueue<ChunkTextureJobResult>()
    private val pendingChunkTexture: BufferedImage by lazy { createPendingChunkTexture() }
    private val pendingChunkIdleTexture: BufferedImage by lazy { toGrayscaleTexture(pendingChunkTexture) }

    private lateinit var memoryLabel: JLabel
    private lateinit var tpsLabel: JLabel
    private lateinit var msptLabel: JLabel
    private lateinit var playersLabel: JLabel

    private lateinit var chunkTableModel: ChunkTableModel
    private lateinit var chunkMapPanel: SkiaSimulationChunkMapPanel
    private lateinit var chunkWorldSelector: javax.swing.JComboBox<String>
    private lateinit var chunkViewSelector: javax.swing.JComboBox<String>

    private lateinit var memoryGraph: MetricGraphPanel
    private lateinit var tpsGraph: MetricGraphPanel
    private lateinit var msptGraph: MetricGraphPanel
    private lateinit var playersGraph: MetricGraphPanel
    private lateinit var profilerPanel: ProfilerPanel
    private lateinit var headerTabs: javax.swing.JTabbedPane
    private var profilerRefreshTimer: Timer? = null
    private var chunkMapStateInitialized = false
    private var chunkMapDirty = true
    private val simulationChunkCellsByWorld = HashMap<String, MutableMap<Pair<Int, Int>, ChunkMapCell>>()
    private val blockStateColorCache = HashMap<Int, Color>()
    private var cachedChunkMapWorlds: List<ChunkMapWorld> = emptyList()
    private var lastSnapshot: TickSnapshot? = null
    private var updatingWorldSelector = false
    private var currentChunkView = CHUNK_VIEW_MAP
    private var leakBaselineBytes = -1L
    private var leakEstimateBytes = 0L
    private var leakRateBytesPerMin = 0.0
    private var lastPostGcLiveSetBytes = 0L
    private val postGcSamples = ArrayDeque<PostGcSample>()
    private val methodProfiler = MethodProfilerSampler()
    private val themeWatcherStarted = AtomicBoolean(false)
    private val uiThemeWatcher = Executors.newSingleThreadScheduledExecutor { runnable ->
        Thread(runnable, "dashboard-theme-watcher").apply { isDaemon = true }
    }
    @Volatile private var lastAppliedDarkMode = false

    fun start() {
        if (!started.compareAndSet(false, true)) return
        if (GraphicsEnvironment.isHeadless()) return
        startJfrLeakStream()
        methodProfiler.start()

        SwingUtilities.invokeLater {
            runCatching {
                installLookAndFeel(osPrefersDarkMode())
                buildUi()
                startThemeWatcher()
            }.onFailure {
                it.printStackTrace()
            }
        }
    }

    fun showOrFocus(): Boolean {
        if (GraphicsEnvironment.isHeadless()) return false
        start()
        SwingUtilities.invokeLater {
            val currentFrame = frame ?: return@invokeLater
            if (!currentFrame.isDisplayable) return@invokeLater
            if (!currentFrame.isVisible) {
                currentFrame.isVisible = true
            }
            currentFrame.toFront()
            currentFrame.requestFocus()
        }
        return true
    }

    fun onTick() {
        if (!started.get()) return
        if (GraphicsEnvironment.isHeadless()) return
        methodProfiler.onGameTick()

        val runtime = Runtime.getRuntime()
        val usedBytes = (runtime.totalMemory() - runtime.freeMemory()).coerceAtLeast(0L)
        val allocatedBytes = runtime.maxMemory().coerceAtLeast(1L)
        val tps = PerformanceMonitor.tps.coerceAtLeast(0.0)
        val mspt = PerformanceMonitor.mspt.coerceAtLeast(0.0)
        val onlinePlayers = PlayerSessionManager.onlineCount().coerceAtLeast(0)
        val chunkRows = collectTopChunks(MAX_CHUNK_LINES)
        refreshSimulationChunkMapState()
        val chunkMaps = cachedChunkMapWorlds
        val mapPlayers = collectMapPlayers()

        val snapshot = TickSnapshot(
            usedBytes = usedBytes,
            allocatedBytes = allocatedBytes,
            tps = tps,
            mspt = mspt,
            players = onlinePlayers,
            chunks = chunkRows,
            chunkMaps = chunkMaps,
            mapPlayers = mapPlayers
        )

        SwingUtilities.invokeLater {
            val currentFrame = frame
            if (currentFrame == null || !currentFrame.isDisplayable) return@invokeLater
            applySnapshot(snapshot)
        }
    }

    private fun collectTopChunks(limit: Int): List<ChunkRow> {
        if (limit <= 0) return emptyList()

        val rows = ArrayList<ChunkRow>(limit)
        for (world in WorldManager.allWorlds()) {
            val stats = world.topChunkStatsByEwmaMspt(limit = limit, minMspt = MIN_LISTED_MSPT)
            for (entry in stats) {
                if (world.isChunkIdle(entry.chunkPos.x, entry.chunkPos.z)) continue
                rows += ChunkRow(
                    world = displayWorldName(world.key),
                    chunkX = entry.chunkPos.x,
                    chunkZ = entry.chunkPos.z,
                    tps = entry.tps,
                    mspt = entry.mspt
                )
            }
        }

        if (rows.isEmpty()) return emptyList()

        rows.sortWith(
            compareByDescending<ChunkRow> { it.mspt }
                .thenByDescending { it.tps }
                .thenBy { it.world }
                .thenBy { it.chunkX }
                .thenBy { it.chunkZ }
        )

        return if (rows.size <= limit) rows else rows.subList(0, limit)
    }

    private fun displayWorldName(worldKey: String): String {
        return if (worldKey.startsWith("minecraft:")) worldKey.substringAfter(':') else worldKey
    }

    private fun collectMapPlayers(): List<MapPlayer> {
        val snapshots = PlayerSessionManager.dashboardPlayerSnapshots()
        if (snapshots.isEmpty()) return emptyList()
        val out = ArrayList<MapPlayer>(snapshots.size)
        for (snapshot in snapshots) {
            out += MapPlayer(
                world = displayWorldName(snapshot.worldKey),
                uuid = snapshot.uuid.toString(),
                name = snapshot.username,
                x = snapshot.x,
                z = snapshot.z,
                skinPartsMask = snapshot.skinPartsMask,
                texturesPropertyValue = snapshot.texturesPropertyValue
            )
        }
        return out
    }

    private fun decodeSkinUrl(texturesPropertyValue: String?): String? {
        val encoded = texturesPropertyValue?.trim().orEmpty()
        if (encoded.isEmpty()) return null
        val cached = skinUrlByProperty[encoded]
        if (cached != null) return cached.ifEmpty { null }
        val decoded = runCatching {
            String(Base64.getDecoder().decode(encoded), Charsets.UTF_8)
        }.getOrNull().orEmpty()
        if (decoded.isEmpty()) {
            skinUrlByProperty[encoded] = ""
            return null
        }
        val skinUrl = runCatching {
            val root = skinJson.parseToJsonElement(decoded).jsonObject
            root["textures"]
                ?.jsonObject
                ?.get("SKIN")
                ?.jsonObject
                ?.get("url")
                ?.jsonPrimitive
                ?.contentOrNull
                ?.takeIf { it.startsWith("http://") || it.startsWith("https://") }
        }.getOrNull().orEmpty()
        skinUrlByProperty[encoded] = skinUrl
        return skinUrl.ifEmpty { null }
    }

    private fun refreshSimulationChunkMapState() {
        if (drainCompletedChunkTextureJobs()) {
            chunkMapDirty = true
        }

        if (!chunkMapStateInitialized) {
            val initial = PlayerSessionManager.activeSimulationChunksByWorldSnapshot()
            for ((worldKey, activeChunks) in initial) {
                if (activeChunks.isEmpty()) continue
                val world = WorldManager.world(worldKey) ?: continue
                val cells = simulationChunkCellsByWorld.computeIfAbsent(worldKey) { HashMap() }
                for (chunkPos in activeChunks) {
                    cells[chunkPos.x to chunkPos.z] = ChunkMapCell(
                        tps = world.chunkEwmaTps(chunkPos.x, chunkPos.z),
                        mspt = world.chunkEwmaMspt(chunkPos.x, chunkPos.z),
                        idle = world.isChunkIdle(chunkPos.x, chunkPos.z),
                        texture = pendingChunkTexture,
                        idleTexture = pendingChunkIdleTexture,
                        textureReady = false
                    )
                    queueChunkTextureBuild(worldKey, chunkPos.x, chunkPos.z)
                }
            }
            chunkMapStateInitialized = true
            chunkMapDirty = true
        }

        val deltas = PlayerSessionManager.consumeActiveSimulationChunkDeltas()
        if (deltas.isNotEmpty()) {
            applySimulationChunkDeltas(deltas)
        }

        if (applyDirtyChunkMetricDeltas()) {
            chunkMapDirty = true
        }
        if (refreshChunkIdleStates()) {
            chunkMapDirty = true
        }
        if (applyDirtyTerrainTextureDeltas()) {
            chunkMapDirty = true
        }
        if (refreshPendingChunkTextures()) {
            chunkMapDirty = true
        }

        if (!chunkMapDirty) return
        cachedChunkMapWorlds = buildChunkMapWorlds()
        chunkMapDirty = false
    }

    private fun applySimulationChunkDeltas(deltas: Map<String, PlayerSessionManager.SimulationChunkDelta>) {
        for ((worldKey, delta) in deltas) {
            if (delta.added.isEmpty() && delta.removed.isEmpty()) continue
            val world = WorldManager.world(worldKey)
            val cells = simulationChunkCellsByWorld.computeIfAbsent(worldKey) { HashMap() }

            for (chunkPos in delta.removed) {
                cells.remove(chunkPos.x to chunkPos.z)
            }
            for (chunkPos in delta.added) {
                cells[chunkPos.x to chunkPos.z] = ChunkMapCell(
                    tps = world?.chunkEwmaTps(chunkPos.x, chunkPos.z) ?: 0.0,
                    mspt = world?.chunkEwmaMspt(chunkPos.x, chunkPos.z) ?: 0.0,
                    idle = world?.isChunkIdle(chunkPos.x, chunkPos.z) ?: true,
                    texture = pendingChunkTexture,
                    idleTexture = pendingChunkIdleTexture,
                    textureReady = false
                )
                queueChunkTextureBuild(worldKey, chunkPos.x, chunkPos.z)
            }
            if (cells.isEmpty()) {
                simulationChunkCellsByWorld.remove(worldKey)
            }
            chunkMapDirty = true
        }
    }

    private fun applyDirtyChunkMetricDeltas(): Boolean {
        var changed = false
        val worldKeys = simulationChunkCellsByWorld.keys.toList()
        for (worldKey in worldKeys) {
            val cells = simulationChunkCellsByWorld[worldKey] ?: continue
            if (cells.isEmpty()) continue
            val world = WorldManager.world(worldKey)
            if (world == null) {
                simulationChunkCellsByWorld.remove(worldKey)
                changed = true
                continue
            }
            val dirtyChunks = world.consumeDirtyChunkStats()
            if (dirtyChunks.isEmpty()) continue
            for (chunkPos in dirtyChunks) {
                val key = chunkPos.x to chunkPos.z
                val previous = cells[key] ?: continue
                val next = ChunkMapCell(
                    tps = world.chunkEwmaTps(chunkPos.x, chunkPos.z),
                    mspt = world.chunkEwmaMspt(chunkPos.x, chunkPos.z),
                    idle = world.isChunkIdle(chunkPos.x, chunkPos.z),
                    texture = previous.texture,
                    idleTexture = previous.idleTexture,
                    textureReady = previous.textureReady
                )
                if (
                    abs(previous.tps - next.tps) <= 1.0e-6 &&
                    abs(previous.mspt - next.mspt) <= 1.0e-6 &&
                    previous.idle == next.idle
                ) continue
                cells[key] = next
                changed = true
            }
        }
        return changed
    }

    private fun refreshChunkIdleStates(): Boolean {
        var changed = false
        val worldKeys = simulationChunkCellsByWorld.keys.toList()
        for (worldKey in worldKeys) {
            val world = WorldManager.world(worldKey) ?: continue
            val cells = simulationChunkCellsByWorld[worldKey] ?: continue
            if (cells.isEmpty()) continue
            val entries = cells.entries.toList()
            for ((coord, cell) in entries) {
                val idleNow = world.isChunkIdle(coord.first, coord.second)
                if (cell.idle == idleNow) continue
                cells[coord] = cell.copy(idle = idleNow)
                changed = true
            }
        }
        return changed
    }

    private fun applyDirtyTerrainTextureDeltas(): Boolean {
        var changed = false
        val worldKeys = simulationChunkCellsByWorld.keys.toList()
        for (worldKey in worldKeys) {
            val cells = simulationChunkCellsByWorld[worldKey] ?: continue
            if (cells.isEmpty()) continue
            val world = WorldManager.world(worldKey) ?: continue
            val dirtyChunks = world.consumeDirtyTerrainChunks()
            if (dirtyChunks.isEmpty()) continue
            for (chunkPos in dirtyChunks) {
                val key = chunkPos.x to chunkPos.z
                val previous = cells[key] ?: continue
                cells[key] = previous.copy(textureReady = false)
                queueChunkTextureBuild(worldKey, chunkPos.x, chunkPos.z)
                changed = true
            }
        }
        return changed
    }

    private fun refreshPendingChunkTextures(): Boolean {
        var queued = false
        var queuedCount = 0
        val worldKeys = simulationChunkCellsByWorld.keys.toList()
        for (worldKey in worldKeys) {
            if (queuedCount >= MAX_TEXTURE_QUEUE_PER_TICK) break
            val world = WorldManager.world(worldKey) ?: continue
            val cells = simulationChunkCellsByWorld[worldKey] ?: continue
            if (cells.isEmpty()) continue
            val entries = cells.entries.toList()
            for ((coord, cell) in entries) {
                if (queuedCount >= MAX_TEXTURE_QUEUE_PER_TICK) break
                if (cell.textureReady) continue
                if (!queueChunkTextureBuild(world.key, coord.first, coord.second)) continue
                queued = true
                queuedCount++
            }
        }
        return queued
    }

    private fun queueChunkTextureBuild(worldKey: String, chunkX: Int, chunkZ: Int): Boolean {
        val key = ChunkTextureJobKey(worldKey, chunkX, chunkZ)
        if (!pendingChunkTextureJobs.add(key)) return false
        chunkTextureExecutor.execute {
            try {
                val built = buildChunkTexture(worldKey, chunkX, chunkZ)
                val texture = built.image
                completedChunkTextureJobs.offer(
                    ChunkTextureJobResult(
                        key = key,
                        texture = texture,
                        idleTexture = toGrayscaleTexture(texture),
                        ready = built.ready
                    )
                )
            } catch (_: Throwable) {
                completedChunkTextureJobs.offer(
                    ChunkTextureJobResult(
                        key = key,
                        texture = pendingChunkTexture,
                        idleTexture = pendingChunkIdleTexture,
                        ready = false
                    )
                )
            } finally {
                pendingChunkTextureJobs.remove(key)
            }
        }
        return true
    }

    private fun drainCompletedChunkTextureJobs(): Boolean {
        var changed = false
        while (true) {
            val result = completedChunkTextureJobs.poll() ?: break
            val cells = simulationChunkCellsByWorld[result.key.worldKey] ?: continue
            val coord = result.key.chunkX to result.key.chunkZ
            val current = cells[coord] ?: continue
            cells[coord] = current.copy(
                texture = result.texture,
                idleTexture = result.idleTexture,
                textureReady = result.ready
            )
            changed = true
        }
        return changed
    }

    private fun buildChunkTexture(worldKey: String, chunkX: Int, chunkZ: Int): TextureBuildResult {
        val world = WorldManager.world(worldKey)
        val image = BufferedImage(CHUNK_TEXTURE_SIZE, CHUNK_TEXTURE_SIZE, BufferedImage.TYPE_INT_RGB)
        if (world == null) return TextureBuildResult(image, ready = false)
        val baseX = chunkX shl 4
        val baseZ = chunkZ shl 4
        val stateMap = Array(CHUNK_TEXTURE_SIZE) { IntArray(CHUNK_TEXTURE_SIZE) { AIR_STATE_ID } }
        val heightMap = Array(CHUNK_TEXTURE_SIZE) { IntArray(CHUNK_TEXTURE_SIZE) { WORLD_MIN_Y - 1 } }
        var missingSamples = 0

        for (lz in 0 until CHUNK_TEXTURE_SIZE) {
            for (lx in 0 until CHUNK_TEXTURE_SIZE) {
                val blockX = baseX + lx
                val blockZ = baseZ + lz
                val surface = sampleSurfaceIfCached(world, blockX, blockZ)
                if (surface != null) {
                    stateMap[lz][lx] = surface.stateId
                    heightMap[lz][lx] = surface.heightY
                } else {
                    missingSamples++
                }
            }
        }

        for (lz in 0 until CHUNK_TEXTURE_SIZE) {
            var previousHeight = heightMap[lz][0].toDouble()
            for (lx in 0 until CHUNK_TEXTURE_SIZE) {
                val stateId = stateMap[lz][lx]
                val baseColor = terrainColorForState(stateId)
                val center = heightMap[lz][lx]
                // Mirror vanilla filled-map shading thresholds/brightness levels.
                // FilledMapItem: shade = ((h - prevH) * 4 / (scale + 4)) + parityNoise
                val parityNoise = (((lx + lz) and 1).toDouble() - 0.5) * 0.4
                val shade = ((center - previousHeight) * 4.0 / 5.0) + parityNoise
                val brightness = when {
                    shade > 0.6 -> VANILLA_BRIGHTNESS_HIGH
                    shade < -0.6 -> VANILLA_BRIGHTNESS_LOW
                    else -> VANILLA_BRIGHTNESS_NORMAL
                }
                val shaded = applyVanillaBrightness(baseColor, brightness)
                image.setRGB(lx, lz, shaded.rgb)
                previousHeight = center.toDouble()
            }
        }
        return TextureBuildResult(image, ready = missingSamples == 0)
    }

    private fun createPendingChunkTexture(): BufferedImage {
        val image = BufferedImage(CHUNK_TEXTURE_SIZE, CHUNK_TEXTURE_SIZE, BufferedImage.TYPE_INT_RGB)
        val g2 = image.createGraphics()
        try {
            g2.color = Color(28, 32, 38)
            g2.fillRect(0, 0, CHUNK_TEXTURE_SIZE, CHUNK_TEXTURE_SIZE)
            g2.color = Color(44, 50, 58)
            g2.drawRect(0, 0, CHUNK_TEXTURE_SIZE - 1, CHUNK_TEXTURE_SIZE - 1)
            g2.color = Color(55, 63, 74)
            g2.drawLine(0, 0, CHUNK_TEXTURE_SIZE - 1, CHUNK_TEXTURE_SIZE - 1)
            g2.drawLine(CHUNK_TEXTURE_SIZE - 1, 0, 0, CHUNK_TEXTURE_SIZE - 1)
        } finally {
            g2.dispose()
        }
        return image
    }

    private fun sampleSurfaceIfCached(world: org.macaroon3145.world.World, blockX: Int, blockZ: Int): SurfaceSample? {
        var sawCached = false
        for (y in WORLD_MAX_Y downTo WORLD_MIN_Y) {
            val cached = world.blockStateAtIfCached(blockX, y, blockZ) ?: continue
            sawCached = true
            if (cached != AIR_STATE_ID) return SurfaceSample(cached, y)
        }
        return if (sawCached) SurfaceSample(AIR_STATE_ID, WORLD_MIN_Y - 1) else null
    }

    private fun applyVanillaBrightness(color: Color, brightness: Int): Color {
        val clamped = brightness.coerceIn(0, 255)
        val red = (color.red * clamped) / 255
        val green = (color.green * clamped) / 255
        val blue = (color.blue * clamped) / 255
        return Color(red, green, blue)
    }

    private fun toGrayscaleTexture(source: BufferedImage): BufferedImage {
        val width = source.width.coerceAtLeast(1)
        val height = source.height.coerceAtLeast(1)
        val out = BufferedImage(width, height, BufferedImage.TYPE_INT_RGB)
        for (y in 0 until height) {
            for (x in 0 until width) {
                val argb = source.getRGB(x, y)
                val r = (argb ushr 16) and 0xFF
                val g = (argb ushr 8) and 0xFF
                val b = argb and 0xFF
                val gray = (r * 299 + g * 587 + b * 114) / 1000
                val rgb = (gray shl 16) or (gray shl 8) or gray
                out.setRGB(x, y, rgb)
            }
        }
        return out
    }

    private fun toOverloadTexture(source: BufferedImage): BufferedImage {
        val width = source.width.coerceAtLeast(1)
        val height = source.height.coerceAtLeast(1)
        val out = BufferedImage(width, height, BufferedImage.TYPE_INT_RGB)
        for (y in 0 until height) {
            for (x in 0 until width) {
                val argb = source.getRGB(x, y)
                val r = (argb ushr 16) and 0xFF
                val g = (argb ushr 8) and 0xFF
                val b = argb and 0xFF
                val tintedR = ((r * 0.45) + (255 * 0.55)).roundToInt().coerceIn(0, 255)
                val tintedG = (g * 0.38).roundToInt().coerceIn(0, 255)
                val tintedB = (b * 0.38).roundToInt().coerceIn(0, 255)
                val rgb = (tintedR shl 16) or (tintedG shl 8) or tintedB
                out.setRGB(x, y, rgb)
            }
        }
        return out
    }

    private fun terrainColorForState(stateId: Int): Color {
        val key = stateId.coerceAtLeast(0)
        return blockStateColorCache.getOrPut(key) {
            val blockKey = BlockStateRegistry.parsedState(key)?.blockKey.orEmpty()
            when {
                blockKey.contains("water") -> Color(52, 114, 201)
                blockKey.contains("lava") -> Color(224, 98, 54)
                blockKey.contains("grass_block") -> Color(84, 148, 71)
                blockKey.contains("leaves") -> Color(69, 119, 62)
                blockKey.contains("log") || blockKey.contains("wood") -> Color(127, 96, 62)
                blockKey.contains("sand") || blockKey.contains("sandstone") -> Color(219, 204, 132)
                blockKey.contains("snow") || blockKey.contains("ice") -> Color(220, 235, 246)
                blockKey.contains("stone") || blockKey.contains("deepslate") || blockKey.contains("ore") -> Color(128, 128, 132)
                blockKey.contains("dirt") || blockKey.contains("mud") || blockKey.contains("clay") -> Color(130, 92, 66)
                blockKey.contains("air") || blockKey.isEmpty() -> Color(34, 39, 46)
                else -> {
                    val hash = blockKey.hashCode()
                    val red = 70 + ((hash ushr 16) and 0x3F)
                    val green = 70 + ((hash ushr 8) and 0x3F)
                    val blue = 70 + (hash and 0x3F)
                    Color(red, green, blue)
                }
            }
        }
    }

    private fun buildChunkMapWorlds(): List<ChunkMapWorld> {
        if (simulationChunkCellsByWorld.isEmpty()) return emptyList()
        val worlds = ArrayList<ChunkMapWorld>(simulationChunkCellsByWorld.size)
        for ((worldKey, cells) in simulationChunkCellsByWorld) {
            if (cells.isEmpty()) continue
            var minX = Int.MAX_VALUE
            var maxX = Int.MIN_VALUE
            var minZ = Int.MAX_VALUE
            var maxZ = Int.MIN_VALUE
            var sigSum = 0L
            var sigXor = 0L
            for ((coord, _) in cells) {
                minX = min(minX, coord.first)
                maxX = max(maxX, coord.first)
                minZ = min(minZ, coord.second)
                maxZ = max(maxZ, coord.second)
            }
            for ((coord, cell) in cells) {
                var h = 1469598103934665603L
                h = h * 1099511628211L + coord.first.toLong()
                h = h * 1099511628211L + coord.second.toLong()
                h = h * 1099511628211L + if (cell.idle) 1L else 0L
                h = h * 1099511628211L + if (cell.textureReady) 1L else 0L
                h = h * 1099511628211L + System.identityHashCode(cell.texture).toLong()
                h = h * 1099511628211L + System.identityHashCode(cell.idleTexture).toLong()
                sigSum += h
                sigXor = sigXor xor java.lang.Long.rotateLeft(h, ((coord.first xor coord.second) and 31))
            }
            val worldSig = sigSum * 31L + sigXor + cells.size.toLong()
            worlds += ChunkMapWorld(
                world = displayWorldName(worldKey),
                minChunkX = minX,
                maxChunkX = maxX,
                minChunkZ = minZ,
                maxChunkZ = maxZ,
                cells = HashMap(cells),
                signature = worldSig
            )
        }
        worlds.sortBy { it.world }
        return worlds
    }

    private fun applySnapshot(snapshot: TickSnapshot) {
        lastSnapshot = snapshot
        val leak = updateLeakEstimate()
        val leakMb = bytesToMb(leak.estimatedLeakBytes)

        memoryLabel.text = ServerI18n.tr(
            "aerogel.dashboard.card.memory.value",
            formatDouble(leakMb)
        )
        tpsLabel.text = ServerI18n.tr("aerogel.dashboard.card.tps.value", formatDouble(snapshot.tps))
        msptLabel.text = ServerI18n.tr("aerogel.dashboard.card.mspt.value", formatDouble(snapshot.mspt))
        playersLabel.text = ServerI18n.tr(
            "aerogel.dashboard.card.players.value",
            snapshot.players.toString(),
            ServerConfig.maxPlayers.toString()
        )

        updateWorldSelectorOptions(snapshot)
        applyChunkViewData(snapshot)

        memoryGraph.push(leakMb, max(bytesToMb(snapshot.allocatedBytes), 1.0))
        tpsGraph.push(snapshot.tps, max(targetTpsUpperBound(), 1.0))
        msptGraph.push(snapshot.mspt)
        playersGraph.push(snapshot.players.toDouble(), max(ServerConfig.maxPlayers.toDouble(), 1.0))
    }

    private fun updateLeakEstimate(): LeakEstimate {
        val memoryMx = ManagementFactory.getMemoryMXBean()
        var processedAnyJfrSample = false
        while (true) {
            val sample = jfrLeakEvents.poll() ?: break
            processedAnyJfrSample = true
            val postGcUsed = sample.heapUsedBytes.coerceAtLeast(0L)
            val now = sample.nowNanos
            lastPostGcLiveSetBytes = postGcUsed
            if (leakBaselineBytes < 0L) {
                leakBaselineBytes = postGcUsed
            }
            leakEstimateBytes = (postGcUsed - leakBaselineBytes).coerceAtLeast(0L).coerceAtMost(postGcUsed)
            postGcSamples.addLast(PostGcSample(nowNanos = now, usedBytes = leakEstimateBytes))
            while (postGcSamples.size > 48) postGcSamples.removeFirst()
        }
        if (processedAnyJfrSample) {
            leakRateBytesPerMin = estimateLeakRateBytesPerMin(postGcSamples).coerceAtLeast(0.0)
        }

        val liveSet = if (lastPostGcLiveSetBytes > 0L) {
            lastPostGcLiveSetBytes
        } else {
            memoryMx.heapMemoryUsage.used.coerceAtLeast(0L)
        }
        return LeakEstimate(
            estimatedLeakBytes = leakEstimateBytes.coerceIn(0L, liveSet),
            rateBytesPerMin = leakRateBytesPerMin,
            liveSetBytes = liveSet
        )
    }

    private fun estimateLeakRateBytesPerMin(samples: ArrayDeque<PostGcSample>): Double {
        if (samples.size < 2) return 0.0
        val firstTime = samples.first().nowNanos.toDouble()
        var sumX = 0.0
        var sumY = 0.0
        var sumXY = 0.0
        var sumXX = 0.0
        val n = samples.size.toDouble()
        for (sample in samples) {
            val xMin = (sample.nowNanos.toDouble() - firstTime) / 60_000_000_000.0
            val yMb = sample.usedBytes.toDouble()
            sumX += xMin
            sumY += yMb
            sumXY += xMin * yMb
            sumXX += xMin * xMin
        }
        val denominator = (n * sumXX) - (sumX * sumX)
        if (abs(denominator) <= 1.0e-9) return 0.0
        return ((n * sumXY) - (sumX * sumY)) / denominator
    }

    private fun startJfrLeakStream() {
        if (!jfrLeakStreamStarted.compareAndSet(false, true)) return
        jfrLeakExecutor.execute {
            runCatching {
                RecordingStream().use { stream ->
                    stream.enable("jdk.GCHeapSummary").withPeriod(Duration.ofMillis(50))
                    stream.onEvent("jdk.GCHeapSummary") { event ->
                        val whenValue = runCatching { event.getString("when") }.getOrNull().orEmpty()
                        if (!whenValue.equals("After GC", ignoreCase = true)) return@onEvent
                        val heapUsed = runCatching { event.getLong("heapUsed") }.getOrDefault(-1L)
                        if (heapUsed < 0L) return@onEvent
                        jfrLeakEvents.offer(JfrAfterGcSample(nowNanos = System.nanoTime(), heapUsedBytes = heapUsed))
                    }
                    stream.start()
                }
            }.onFailure {
                // Fallback path: keep existing values if JFR streaming is unavailable in runtime.
            }
        }
    }

    private fun updateWorldSelectorOptions(snapshot: TickSnapshot) {
        if (!::chunkWorldSelector.isInitialized) return
        val allWorldsLabel = ServerI18n.tr("aerogel.dashboard.world.all")
        val unavailableLabel = ServerI18n.tr("aerogel.dashboard.world.unavailable")
        val mapMode = currentChunkView == CHUNK_VIEW_MAP
        val worlds = LinkedHashSet<String>()
        if (mapMode) {
            // In map mode, selector must track active simulation worlds only.
            worlds.addAll(snapshot.chunkMaps.map { it.world })
        } else {
            // In list mode, only show worlds that actually have list rows (non-idle chunks).
            worlds.addAll(snapshot.chunks.map { it.world })
        }

        val selectedBefore = (chunkWorldSelector.selectedItem as? String).orEmpty()
        val nextItems = ArrayList<String>(worlds.size + if (mapMode) 0 else 1)
        if (!mapMode) {
            nextItems.add(allWorldsLabel)
        }
        nextItems.addAll(worlds.sorted())

        val currentItems = ArrayList<String>(chunkWorldSelector.itemCount)
        for (i in 0 until chunkWorldSelector.itemCount) {
            currentItems.add(chunkWorldSelector.getItemAt(i))
        }
        if (currentItems == nextItems) return

        updatingWorldSelector = true
        try {
            chunkWorldSelector.removeAllItems()
            if (nextItems.isEmpty()) {
                chunkWorldSelector.addItem(unavailableLabel)
                chunkWorldSelector.selectedIndex = 0
                chunkWorldSelector.isEnabled = false
            } else {
                chunkWorldSelector.isEnabled = true
                for (item in nextItems) chunkWorldSelector.addItem(item)
                val nextSelection = when {
                    selectedBefore in nextItems -> selectedBefore
                    !mapMode -> allWorldsLabel
                    else -> nextItems.first()
                }
                chunkWorldSelector.selectedItem = nextSelection
            }
        } finally {
            updatingWorldSelector = false
        }
    }

    private fun applyChunkViewData(snapshot: TickSnapshot) {
        updateMapRenderingState()
        if (!chunkWorldSelector.isEnabled) {
            chunkTableModel.setRows(emptyList())
            chunkMapPanel.setWorlds(emptyList())
            chunkMapPanel.setPlayers(emptyList())
            return
        }
        val allWorldsLabel = ServerI18n.tr("aerogel.dashboard.world.all")
        val mapMode = currentChunkView == CHUNK_VIEW_MAP
        val selectedWorld = (chunkWorldSelector.selectedItem as? String)
            ?.takeUnless { (!mapMode && it == allWorldsLabel) || it.isBlank() }
        val filteredRows = if (selectedWorld == null) snapshot.chunks else snapshot.chunks.filter { it.world == selectedWorld }
        val filteredMaps = when {
            mapMode && selectedWorld.isNullOrBlank() -> emptyList()
            selectedWorld == null -> snapshot.chunkMaps
            else -> snapshot.chunkMaps.filter { it.world == selectedWorld }
        }
        val filteredPlayers = when {
            mapMode && selectedWorld.isNullOrBlank() -> emptyList()
            selectedWorld == null -> snapshot.mapPlayers
            else -> snapshot.mapPlayers.filter { it.world == selectedWorld }
        }
        chunkTableModel.setRows(filteredRows)
        chunkMapPanel.setWorlds(filteredMaps)
        chunkMapPanel.setPlayers(filteredPlayers)
    }

    private fun updateMapRenderingState() {
        if (!::chunkMapPanel.isInitialized) return
        val mapMode = currentChunkView == CHUNK_VIEW_MAP
        val dashboardVisible = if (::headerTabs.isInitialized) headerTabs.selectedIndex == 0 else true
        chunkMapPanel.setRenderingEnabled(mapMode && dashboardVisible)
    }

    private fun targetTpsUpperBound(): Double {
        val configured = ServerConfig.maxTps
        return if (configured > 0.0) configured else 20.0
    }

    private fun targetMsptUpperBound(): Double {
        val targetTps = targetTpsUpperBound().coerceAtLeast(1.0)
        return 1000.0 / targetTps
    }

    private fun msptUsagePercentForTarget(mspt: Double): Int {
        if (!mspt.isFinite()) return 0
        val targetMspt = targetMsptUpperBound().coerceAtLeast(1.0e-9)
        return ((mspt.coerceAtLeast(0.0) / targetMspt) * 100.0).roundToInt().coerceAtLeast(0)
    }

    private fun installLookAndFeel(dark: Boolean) {
        lastAppliedDarkMode = dark
        runCatching {
            if (dark) {
                UIManager.setLookAndFeel(FlatDarkLaf())
            } else {
                UIManager.setLookAndFeel(FlatLightLaf())
            }
        }.onFailure {
            // Fallback to default Swing LAF if FlatLaf setup fails.
            runCatching { UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName()) }
        }
    }

    private fun startThemeWatcher() {
        if (!themeWatcherStarted.compareAndSet(false, true)) return
        uiThemeWatcher.scheduleAtFixedRate({
            val frameRef = frame
            if (frameRef == null || !frameRef.isDisplayable) return@scheduleAtFixedRate
            val dark = osPrefersDarkMode()
            if (dark == lastAppliedDarkMode) return@scheduleAtFixedRate
            SwingUtilities.invokeLater {
                val currentFrame = frame ?: return@invokeLater
                if (!currentFrame.isDisplayable) return@invokeLater
                installLookAndFeel(dark)
                SwingUtilities.updateComponentTreeUI(currentFrame)
                currentFrame.revalidate()
                currentFrame.repaint()
            }
        }, 2, 2, TimeUnit.SECONDS)
    }

    private fun buildUi() {
        val top = JPanel(GridLayout(1, 4, 8, 0)).apply {
            border = BorderFactory.createEmptyBorder(10, 10, 10, 10)
        }

        memoryLabel = metricCard(ServerI18n.tr("aerogel.dashboard.card.memory"))
        tpsLabel = metricCard(ServerI18n.tr("aerogel.dashboard.card.tps"))
        msptLabel = metricCard(ServerI18n.tr("aerogel.dashboard.card.mspt"))
        playersLabel = metricCard(ServerI18n.tr("aerogel.dashboard.card.players"))

        top.add(wrapCard(memoryLabel))
        top.add(wrapCard(tpsLabel))
        top.add(wrapCard(msptLabel))
        top.add(wrapCard(playersLabel))

        chunkTableModel = ChunkTableModel()
        val table = JTable(chunkTableModel).apply {
            rowHeight = 22
            autoCreateRowSorter = true
            fillsViewportHeight = true
        }
        val tablePane = JScrollPane(table).apply {
            border = BorderFactory.createTitledBorder(ServerI18n.tr("aerogel.dashboard.table.title"))
            preferredSize = Dimension(900, 300)
        }
        val tableWrap = JPanel(BorderLayout()).apply {
            border = BorderFactory.createEmptyBorder(0, 8, 0, 8)
            add(tablePane, BorderLayout.CENTER)
        }

        chunkMapPanel = SkiaSimulationChunkMapPanel()
        val mapPane = JScrollPane(chunkMapPanel).apply {
            border = BorderFactory.createTitledBorder(ServerI18n.tr("aerogel.dashboard.map.title"))
            preferredSize = Dimension(900, 300)
            horizontalScrollBarPolicy = JScrollPane.HORIZONTAL_SCROLLBAR_AS_NEEDED
            verticalScrollBarPolicy = JScrollPane.VERTICAL_SCROLLBAR_AS_NEEDED
        }
        val mapWrap = JPanel(BorderLayout()).apply {
            border = BorderFactory.createEmptyBorder(0, 8, 0, 8)
            add(mapPane, BorderLayout.CENTER)
        }

        val chunkViewCards = JPanel(CardLayout()).apply {
            add(tableWrap, CHUNK_VIEW_LIST)
            add(mapWrap, CHUNK_VIEW_MAP)
        }
        chunkViewSelector = javax.swing.JComboBox(
            arrayOf(
                ServerI18n.tr("aerogel.dashboard.view.list"),
                ServerI18n.tr("aerogel.dashboard.view.map")
            )
        ).apply {
            selectedIndex = 1
            addActionListener {
                currentChunkView = if (selectedIndex == 0) CHUNK_VIEW_LIST else CHUNK_VIEW_MAP
                val layout = chunkViewCards.layout as CardLayout
                if (selectedIndex == 0) {
                    layout.show(chunkViewCards, CHUNK_VIEW_LIST)
                } else {
                    layout.show(chunkViewCards, CHUNK_VIEW_MAP)
                }
                val snapshot = lastSnapshot ?: return@addActionListener
                updateWorldSelectorOptions(snapshot)
                applyChunkViewData(snapshot)
            }
        }
        chunkWorldSelector = javax.swing.JComboBox(arrayOf(ServerI18n.tr("aerogel.dashboard.world.all"))).apply {
            addActionListener {
                if (updatingWorldSelector) return@addActionListener
                val snapshot = lastSnapshot ?: return@addActionListener
                applyChunkViewData(snapshot)
            }
        }
        (chunkViewCards.layout as CardLayout).show(chunkViewCards, CHUNK_VIEW_MAP)
        val chunkViewHeader = JPanel(BorderLayout()).apply {
            border = BorderFactory.createEmptyBorder(0, 12, 6, 5)
            add(JLabel(ServerI18n.tr("aerogel.dashboard.view.label")), BorderLayout.WEST)
            add(
                JPanel(FlowLayout(FlowLayout.RIGHT, 6, 0)).apply {
                    isOpaque = false
                    add(chunkViewSelector)
                    add(chunkWorldSelector)
                },
                BorderLayout.EAST
            )
        }
        val chunkViews = JPanel(BorderLayout()).apply {
            border = BorderFactory.createEmptyBorder(0, 0, 8, 0)
            add(chunkViewHeader, BorderLayout.NORTH)
            add(chunkViewCards, BorderLayout.CENTER)
        }

        val graphGrid = JPanel(GridLayout(2, 2, 8, 8)).apply {
            border = BorderFactory.createEmptyBorder(10, 10, 10, 10)
        }

        memoryGraph = MetricGraphPanel(Color(70, 170, 255), baselineMin = 1.0, valueUnit = ServerI18n.tr("aerogel.dashboard.graph.unit.mb"))
        tpsGraph = MetricGraphPanel(Color(72, 201, 176), baselineMin = 20.0, valueUnit = ServerI18n.tr("aerogel.dashboard.graph.unit.tps"))
        msptGraph = MetricGraphPanel(Color(255, 159, 67), baselineMin = targetMsptUpperBound(), valueUnit = ServerI18n.tr("aerogel.dashboard.graph.unit.ms"))
        playersGraph = MetricGraphPanel(
            Color(245, 113, 193),
            baselineMin = 1.0,
            valueUnit = ServerI18n.tr("aerogel.dashboard.graph.unit.players"),
            integerDisplay = true
        )

        graphGrid.add(createGraphCard(ServerI18n.tr("aerogel.dashboard.graph.memory_leak"), memoryGraph))
        graphGrid.add(createGraphCard(ServerI18n.tr("aerogel.dashboard.graph.tps"), tpsGraph))
        graphGrid.add(createGraphCard(ServerI18n.tr("aerogel.dashboard.graph.mspt"), msptGraph))
        graphGrid.add(createGraphCard(ServerI18n.tr("aerogel.dashboard.graph.players"), playersGraph))

        val dashboardCenter = JPanel(BorderLayout()).apply {
            add(chunkViews, BorderLayout.CENTER)
            add(graphGrid, BorderLayout.SOUTH)
        }

        val dashboardPage = JPanel(BorderLayout()).apply {
            add(top, BorderLayout.NORTH)
            add(dashboardCenter, BorderLayout.CENTER)
        }

        profilerPanel = ProfilerPanel()
        val profilerPage = JPanel(BorderLayout()).apply {
            border = BorderFactory.createEmptyBorder(8, 8, 8, 8)
            add(profilerPanel, BorderLayout.CENTER)
        }

        headerTabs = javax.swing.JTabbedPane().apply {
            tabPlacement = javax.swing.JTabbedPane.TOP
            addTab(ServerI18n.tr("aerogel.dashboard.nav.dashboard"), dashboardPage)
            addTab(ServerI18n.tr("aerogel.dashboard.nav.profiler"), profilerPage)
            font = font.deriveFont(Font.BOLD, 13f)
            border = BorderFactory.createEmptyBorder(0, 0, 0, 0)
            addChangeListener { updateMapRenderingState() }
        }

        val dashboardFrame = JFrame(ServerI18n.tr("aerogel.dashboard.title")).apply {
            defaultCloseOperation = JFrame.HIDE_ON_CLOSE
            layout = BorderLayout()
            add(this@ServerDashboard.headerTabs, BorderLayout.CENTER)
            minimumSize = Dimension(1080, 1000)
            preferredSize = Dimension(1200, 1025)
            setLocationRelativeTo(null)
            isVisible = true
        }
        frame = dashboardFrame
        startProfilerRefreshTimer()
    }

    private fun startProfilerRefreshTimer() {
        profilerRefreshTimer?.stop()
        profilerRefreshTimer = Timer(displayRefreshPeriodMs()) {
            val currentFrame = frame
            if (currentFrame == null || !currentFrame.isDisplayable || !currentFrame.isVisible) return@Timer
            if (!::headerTabs.isInitialized || headerTabs.selectedIndex != 1) return@Timer
            if (!::profilerPanel.isInitialized) return@Timer
            val snapshot = lastSnapshot ?: return@Timer
            profilerPanel.update(snapshot, methodProfiler)
        }.apply {
            isRepeats = true
            setCoalesce(true)
            start()
        }
    }

    private fun displayRefreshPeriodMs(): Int {
        val hz = runCatching {
            val rate = GraphicsEnvironment.getLocalGraphicsEnvironment()
                .defaultScreenDevice
                .displayMode
                .refreshRate
            if (rate <= 0) 60 else rate
        }.getOrDefault(60)
        return (1000.0 / hz.toDouble()).roundToInt().coerceIn(4, 34)
    }

    private fun metricCard(title: String): JLabel {
        return JLabel("$title: --", SwingConstants.LEFT).apply {
            font = font.deriveFont(Font.BOLD, 14f)
            border = BorderFactory.createEmptyBorder(8, 10, 8, 10)
        }
    }

    private fun wrapCard(content: JLabel): JPanel {
        return JPanel(FlowLayout(FlowLayout.LEFT, 0, 0)).apply {
            border = BorderFactory.createCompoundBorder(
                BorderFactory.createLineBorder(UIManager.getColor("Component.borderColor") ?: Color.GRAY),
                BorderFactory.createEmptyBorder(6, 6, 6, 6)
            )
            add(content)
        }
    }

    private fun bytesToMb(bytes: Long): Double = bytes / (1024.0 * 1024.0)

    private fun formatDouble(value: Double): String = DECIMAL_2.format(value)

    private fun osPrefersDarkMode(): Boolean {
        val os = System.getProperty("os.name")?.lowercase(Locale.ROOT).orEmpty()
        return when {
            os.contains("win") -> windowsPrefersDarkMode()
            os.contains("mac") -> macPrefersDarkMode()
            else -> linuxPrefersDarkMode()
        }
    }

    private fun windowsPrefersDarkMode(): Boolean {
        val output = runCommand(
            "reg",
            "query",
            "HKCU\\Software\\Microsoft\\Windows\\CurrentVersion\\Themes\\Personalize",
            "/v",
            "AppsUseLightTheme"
        ) ?: return false
        return output.lines().any { line ->
            line.contains("AppsUseLightTheme", ignoreCase = true) &&
                line.trim().endsWith("0")
        }
    }

    private fun macPrefersDarkMode(): Boolean {
        val output = runCommand("defaults", "read", "-g", "AppleInterfaceStyle") ?: return false
        return output.contains("Dark", ignoreCase = true)
    }

    private fun linuxPrefersDarkMode(): Boolean {
        val gtkTheme = System.getenv("GTK_THEME")
        if (!gtkTheme.isNullOrBlank() && gtkTheme.lowercase(Locale.ROOT).contains("dark")) {
            return true
        }

        val colorScheme = runCommand("gsettings", "get", "org.gnome.desktop.interface", "color-scheme")
        if (!colorScheme.isNullOrBlank() && colorScheme.contains("prefer-dark", ignoreCase = true)) {
            return true
        }

        val gnomeTheme = runCommand("gsettings", "get", "org.gnome.desktop.interface", "gtk-theme")
        return !gnomeTheme.isNullOrBlank() && gnomeTheme.contains("dark", ignoreCase = true)
    }

    private fun runCommand(vararg command: String): String? {
        return runCatching {
            val process = ProcessBuilder(*command)
                .redirectErrorStream(true)
                .start()
            BufferedReader(InputStreamReader(process.inputStream)).use { reader ->
                val output = reader.readText().trim()
                val exit = process.waitFor()
                if (exit == 0) output else null
            }
        }.getOrNull()
    }

    private data class ChunkTextureJobKey(
        val worldKey: String,
        val chunkX: Int,
        val chunkZ: Int
    )

    private data class ChunkTextureJobResult(
        val key: ChunkTextureJobKey,
        val texture: BufferedImage,
        val idleTexture: BufferedImage,
        val ready: Boolean
    )

    private data class JfrAfterGcSample(
        val nowNanos: Long,
        val heapUsedBytes: Long
    )

    private data class PostGcSample(
        val nowNanos: Long,
        val usedBytes: Long
    )

    private data class LeakEstimate(
        val estimatedLeakBytes: Long,
        val rateBytesPerMin: Double,
        val liveSetBytes: Long
    )

    private data class TickSnapshot(
        val usedBytes: Long,
        val allocatedBytes: Long,
        val tps: Double,
        val mspt: Double,
        val players: Int,
        val chunks: List<ChunkRow>,
        val chunkMaps: List<ChunkMapWorld>,
        val mapPlayers: List<MapPlayer>
    )

    private data class ChunkRow(
        val world: String,
        val chunkX: Int,
        val chunkZ: Int,
        val tps: Double,
        val mspt: Double
    )

    private data class ChunkMapCell(
        val tps: Double,
        val mspt: Double,
        val idle: Boolean,
        val texture: BufferedImage,
        val idleTexture: BufferedImage,
        val textureReady: Boolean
    )

    private data class TextureBuildResult(
        val image: BufferedImage,
        val ready: Boolean
    )

    private data class SurfaceSample(
        val stateId: Int,
        val heightY: Int
    )

    private data class ChunkMapWorld(
        val world: String,
        val minChunkX: Int,
        val maxChunkX: Int,
        val minChunkZ: Int,
        val maxChunkZ: Int,
        val cells: Map<Pair<Int, Int>, ChunkMapCell>,
        val signature: Long
    )

    private data class MapPlayer(
        val world: String,
        val uuid: String,
        val name: String,
        val x: Double,
        val z: Double,
        val skinPartsMask: Int,
        val texturesPropertyValue: String?
    )

    private data class GraphWindowOption(
        val minutes: Int,
        private val label: String
    ) {
        override fun toString(): String = label
    }

    private data class ProfilerWindowOption(
        val seconds: Int,
        private val label: String
    ) {
        override fun toString(): String = label
    }

    private fun visibleWindowSamples(windowMinutes: Int): Int {
        val targetTps = targetTpsUpperBound().coerceAtLeast(1.0)
        val samplesPerSecond = targetTps.toInt().coerceAtLeast(1)
        return (windowMinutes * 60 * samplesPerSecond).coerceAtLeast(2)
    }

    private fun createGraphCard(title: String, graphPanel: MetricGraphPanel): JPanel {
        val combo = javax.swing.JComboBox(
            arrayOf(
                GraphWindowOption(3, ServerI18n.tr("aerogel.dashboard.graph.window.3m")),
                GraphWindowOption(5, ServerI18n.tr("aerogel.dashboard.graph.window.5m")),
                GraphWindowOption(10, ServerI18n.tr("aerogel.dashboard.graph.window.10m")),
                GraphWindowOption(30, ServerI18n.tr("aerogel.dashboard.graph.window.30m"))
            )
        ).apply {
            selectedIndex = 0
            addActionListener {
                val selected = selectedItem as? GraphWindowOption ?: return@addActionListener
                graphPanel.setWindowMinutes(selected.minutes)
            }
        }
        val titleLabel = JLabel(title).apply {
            font = font.deriveFont(Font.BOLD, 12f)
        }
        val controls = JPanel(BorderLayout()).apply {
            isOpaque = false
            border = BorderFactory.createEmptyBorder(4, 6, 0, 6)
            add(titleLabel, BorderLayout.WEST)
            add(combo, BorderLayout.EAST)
        }
        return JPanel(BorderLayout()).apply {
            border = BorderFactory.createCompoundBorder(
                BorderFactory.createLineBorder(UIManager.getColor("Component.borderColor") ?: Color.GRAY),
                BorderFactory.createEmptyBorder(4, 4, 4, 4)
            )
            add(controls, BorderLayout.NORTH)
            add(graphPanel, BorderLayout.CENTER)
        }
    }

    private class ChunkTableModel : AbstractTableModel() {
        private val columns = arrayOf(
            ServerI18n.tr("aerogel.dashboard.table.column.world"),
            ServerI18n.tr("aerogel.dashboard.table.column.chunk_x"),
            ServerI18n.tr("aerogel.dashboard.table.column.chunk_z"),
            ServerI18n.tr("aerogel.dashboard.table.column.tps"),
            ServerI18n.tr("aerogel.dashboard.table.column.mspt")
        )
        private var rows: List<ChunkRow> = emptyList()

        fun setRows(nextRows: List<ChunkRow>) {
            rows = nextRows
            fireTableDataChanged()
        }

        override fun getRowCount(): Int = rows.size

        override fun getColumnCount(): Int = columns.size

        override fun getColumnName(column: Int): String = columns[column]

        override fun getValueAt(rowIndex: Int, columnIndex: Int): Any {
            val row = rows[rowIndex]
            return when (columnIndex) {
                0 -> row.world
                1 -> row.chunkX
                2 -> row.chunkZ
                3 -> DECIMAL_2.format(row.tps)
                4 -> DECIMAL_2.format(row.mspt)
                else -> ""
            }
        }
    }

    private data class MethodProfileRow(
        val method: String,
        val cpuSharePercent: Double,
        val estimatedMspt: Double,
        val allocatedMb: Double,
        val executionCount: Long
    )

    private data class MethodProfileSnapshot(
        val totalSamples: Long,
        val rows: List<MethodProfileRow>,
        val callTreeRoot: MethodCallTreeNode
    )

    private data class MethodCallTreeNode(
        val method: String,
        val samples: Long,
        val allocatedBytes: Long,
        val children: List<MethodCallTreeNode>
    )

    private class MethodProfilerSampler {
        private data class SampleEvent(
            val atNanos: Long,
            val callPath: List<String>,
            val allocatedBytesDelta: Long
        )

        private data class AllocationEvent(
            val leafMethod: String,
            val bytes: Long
        )

        private data class AggNode(
            val method: String,
            var samples: Long = 0L,
            var allocatedBytes: Long = 0L,
            val children: MutableMap<String, AggNode> = HashMap()
        )

        private val started = AtomicBoolean(false)
        private val jfrStarted = AtomicBoolean(false)
        private val jfrExecutor = Executors.newSingleThreadExecutor { runnable ->
            Thread(runnable, "dashboard-method-profiler").apply { isDaemon = true }
        }
        private val pendingCallPaths = ConcurrentLinkedQueue<List<String>>()
        private val pendingAllocations = ConcurrentLinkedQueue<AllocationEvent>()
        private val samples = ArrayDeque<SampleEvent>()
        @Volatile private var lastHeapUsedBytes = ManagementFactory.getMemoryMXBean().heapMemoryUsage.used.coerceAtLeast(0L)
        private val maxWindowNanos = TimeUnit.MINUTES.toNanos(30)

        fun start() {
            if (!started.compareAndSet(false, true)) return
            startExecutionSampleStream()
        }

        fun onGameTick(nowNanos: Long = System.nanoTime()) {
            val drained = ArrayList<List<String>>(128)
            while (true) {
                val path = pendingCallPaths.poll() ?: break
                drained += path
                if (drained.size >= 4096) break
            }

            val allocByLeaf = HashMap<String, Long>()
            while (true) {
                val alloc = pendingAllocations.poll() ?: break
                if (alloc.bytes <= 0L) continue
                allocByLeaf[alloc.leafMethod] = (allocByLeaf[alloc.leafMethod] ?: 0L) + alloc.bytes
            }

            if (drained.isEmpty()) {
                synchronized(this) {
                    pruneOldSamples(nowNanos)
                }
                return
            }

            val countsByLeaf = HashMap<String, Int>()
            for (path in drained) {
                val leaf = path.lastOrNull() ?: continue
                countsByLeaf[leaf] = (countsByLeaf[leaf] ?: 0) + 1
            }

            var fallbackAllocPerPath = 0L
            if (allocByLeaf.isEmpty()) {
                val heapNow = ManagementFactory.getMemoryMXBean().heapMemoryUsage.used.coerceAtLeast(0L)
                val delta = (heapNow - lastHeapUsedBytes).coerceAtLeast(0L)
                lastHeapUsedBytes = heapNow
                fallbackAllocPerPath = if (delta > 0L) delta / drained.size.coerceAtLeast(1) else 0L
            }

            synchronized(this) {
                for (callPath in drained) {
                    val leaf = callPath.lastOrNull().orEmpty()
                    val allocPerPath = if (allocByLeaf.isNotEmpty()) {
                        val leafTotal = allocByLeaf[leaf] ?: 0L
                        val leafCount = countsByLeaf[leaf] ?: 1
                        if (leafCount > 0) leafTotal / leafCount else 0L
                    } else {
                        fallbackAllocPerPath
                    }
                    samples.addLast(
                        SampleEvent(
                            atNanos = nowNanos,
                            callPath = callPath,
                            allocatedBytesDelta = allocPerPath
                        )
                    )
                }
                pruneOldSamples(nowNanos)
            }
        }

        fun snapshot(currentMspt: Double, windowSeconds: Int): MethodProfileSnapshot {
            val nowNanos = System.nanoTime()
            val windowNanos = TimeUnit.SECONDS.toNanos(windowSeconds.coerceAtLeast(1).toLong())
            val minNanos = nowNanos - windowNanos
            var total = 0L
            val samplesByMethod = HashMap<String, Long>()
            val allocByMethod = HashMap<String, Long>()
            val treeRoot = AggNode("<root>")
            synchronized(this) {
                pruneOldSamples(nowNanos)
                for (event in samples) {
                    if (event.atNanos < minNanos) continue
                    if (event.callPath.isEmpty()) continue
                    total += 1
                    val leaf = event.callPath.last()
                    samplesByMethod[leaf] = (samplesByMethod[leaf] ?: 0L) + 1
                    if (event.allocatedBytesDelta > 0L) {
                        allocByMethod[leaf] = (allocByMethod[leaf] ?: 0L) + event.allocatedBytesDelta
                    }
                    addPathToTree(treeRoot, event.callPath, event.allocatedBytesDelta)
                }
            }
            val safeTotal = total.coerceAtLeast(1L)
            val rows = ArrayList<MethodProfileRow>(samplesByMethod.size)
            for ((method, sampleCountRaw) in samplesByMethod) {
                val sampleCount = sampleCountRaw.coerceAtLeast(0L)
                if (sampleCount == 0L) continue
                val share = sampleCount.toDouble() / safeTotal.toDouble()
                rows += MethodProfileRow(
                    method = method,
                    cpuSharePercent = share * 100.0,
                    estimatedMspt = currentMspt * share,
                    allocatedMb = (allocByMethod[method] ?: 0L).toDouble() / (1024.0 * 1024.0),
                    executionCount = sampleCount
                )
            }
            rows.sortByDescending { it.estimatedMspt }
            return MethodProfileSnapshot(
                totalSamples = safeTotal,
                rows = rows,
                callTreeRoot = toTreeNode(treeRoot)
            )
        }

        private fun startExecutionSampleStream() {
            if (!jfrStarted.compareAndSet(false, true)) return
            jfrExecutor.execute {
                runCatching {
                    RecordingStream().use { stream ->
                        stream.enable("jdk.ExecutionSample").withPeriod(Duration.ofMillis(25))
                        runCatching { stream.enable("jdk.ObjectAllocationSample") }
                        runCatching { stream.enable("jdk.ObjectAllocationInNewTLAB") }
                        runCatching { stream.enable("jdk.ObjectAllocationOutsideTLAB") }
                        stream.onEvent("jdk.ExecutionSample") { event ->
                            val threadName = sampledThreadName(event)
                            if (shouldIgnoreThread(threadName)) return@onEvent
                            val callPath = extractMethodPathFromJfr(event) ?: return@onEvent
                            pendingCallPaths.offer(callPath)
                        }
                        stream.onEvent("jdk.ObjectAllocationSample") { event ->
                            val threadName = sampledThreadName(event)
                            if (shouldIgnoreThread(threadName)) return@onEvent
                            val callPath = extractMethodPathFromJfr(event) ?: return@onEvent
                            val leaf = callPath.lastOrNull() ?: return@onEvent
                            val bytes = allocationBytes(event)
                            if (bytes > 0L) {
                                pendingAllocations.offer(AllocationEvent(leafMethod = leaf, bytes = bytes))
                            }
                        }
                        stream.onEvent("jdk.ObjectAllocationInNewTLAB") { event ->
                            val threadName = sampledThreadName(event)
                            if (shouldIgnoreThread(threadName)) return@onEvent
                            val callPath = extractMethodPathFromJfr(event) ?: return@onEvent
                            val leaf = callPath.lastOrNull() ?: return@onEvent
                            val bytes = allocationBytes(event)
                            if (bytes > 0L) {
                                pendingAllocations.offer(AllocationEvent(leafMethod = leaf, bytes = bytes))
                            }
                        }
                        stream.onEvent("jdk.ObjectAllocationOutsideTLAB") { event ->
                            val threadName = sampledThreadName(event)
                            if (shouldIgnoreThread(threadName)) return@onEvent
                            val callPath = extractMethodPathFromJfr(event) ?: return@onEvent
                            val leaf = callPath.lastOrNull() ?: return@onEvent
                            val bytes = allocationBytes(event)
                            if (bytes > 0L) {
                                pendingAllocations.offer(AllocationEvent(leafMethod = leaf, bytes = bytes))
                            }
                        }
                        stream.start()
                    }
                }.onFailure {
                    // If JFR execution samples are unavailable, keep profiler inactive.
                }
            }
        }

        private fun pruneOldSamples(nowNanos: Long) {
            val minNanos = nowNanos - maxWindowNanos
            while (samples.isNotEmpty() && samples.first().atNanos < minNanos) {
                samples.removeFirst()
            }
        }

        private fun extractMethodPathFromJfr(event: jdk.jfr.consumer.RecordedEvent): List<String>? {
            val stack = event.stackTrace ?: return null
            val frames = ArrayList<String>()
            for (frame in stack.frames) {
                val method = frame.method ?: continue
                val className = method.type.name
                if (className.startsWith("org.macaroon3145.ui.")) continue
                frames += "${className.substringAfterLast('.')}.${method.name}"
            }
            if (frames.isEmpty()) return null

            // JFR frame order differs by runtime; normalize to root -> leaf for app frames.
            val path = ArrayList(frames)
            val gameLoopIdx = path.indexOfLast { it.endsWith("GameLoop.runLoop") }
            val threadRunIdx = path.indexOfLast { it.endsWith("Thread.run") }
            if (gameLoopIdx >= 0 && threadRunIdx > gameLoopIdx) {
                path.reverse()
            } else {
                val first = path.firstOrNull().orEmpty()
                val last = path.lastOrNull().orEmpty()
                if (isThreadWrapperMethod(last) && !isThreadWrapperMethod(first)) {
                    path.reverse()
                }
            }

            while (path.size > 1 && isThreadWrapperMethod(path.last())) {
                path.removeLast()
            }
            return path
        }

        private fun sampledThreadName(event: jdk.jfr.consumer.RecordedEvent): String {
            val sampled = runCatching { event.getThread("sampledThread") }.getOrNull()
            if (sampled != null) return sampled.javaName.lowercase(Locale.ROOT)
            val eventThread = runCatching { event.getThread("eventThread") }.getOrNull()
            return eventThread?.javaName?.lowercase(Locale.ROOT).orEmpty()
        }

        private fun shouldIgnoreThread(threadName: String): Boolean {
            if (threadName.isBlank()) return false
            if (threadName.contains("dashboard-method-profiler")) return true
            if (threadName.contains("awt-eventqueue") || threadName.contains("javafx")) return true
            if (threadName.contains("signal dispatcher")) return true
            if (threadName.contains("reference handler")) return true
            if (threadName.contains("finalizer")) return true
            if (threadName.contains("common-cleaner")) return true
            return false
        }

        private fun allocationBytes(event: jdk.jfr.consumer.RecordedEvent): Long {
            val fields = arrayOf("allocationSize", "tlabSize", "objectSize", "weight")
            for (field in fields) {
                val value = runCatching { event.getLong(field) }.getOrNull()
                if (value != null && value > 0L) return value
            }
            return 0L
        }

        private fun isThreadWrapperMethod(methodName: String): Boolean {
            return methodName.endsWith(".run") ||
                methodName.endsWith(".exec") ||
                methodName.endsWith(".start0")
        }

        private fun addPathToTree(root: AggNode, path: List<String>, allocatedBytesDelta: Long) {
            root.samples += 1
            root.allocatedBytes += allocatedBytesDelta
            var node = root
            for (method in path) {
                val child = node.children.getOrPut(method) { AggNode(method = method) }
                child.samples += 1
                child.allocatedBytes += allocatedBytesDelta
                node = child
            }
        }

        private fun toTreeNode(node: AggNode): MethodCallTreeNode {
            val children = node.children.values
                .sortedByDescending { it.samples }
                .map { child -> toTreeNode(child) }
            return MethodCallTreeNode(
                method = node.method,
                samples = node.samples,
                allocatedBytes = node.allocatedBytes,
                children = children
            )
        }
    }

    private class ProfilerTableModel : AbstractTableModel() {
        private val columns = arrayOf(
            ServerI18n.tr("aerogel.dashboard.profiler.column.method"),
            ServerI18n.tr("aerogel.dashboard.profiler.column.cpu"),
            ServerI18n.tr("aerogel.dashboard.profiler.column.mspt"),
            ServerI18n.tr("aerogel.dashboard.profiler.column.alloc"),
            ServerI18n.tr("aerogel.dashboard.profiler.column.exec")
        )
        private var rows: List<MethodProfileRow> = emptyList()

        fun setRows(next: List<MethodProfileRow>) {
            rows = next
            fireTableDataChanged()
        }

        override fun getRowCount(): Int = rows.size
        override fun getColumnCount(): Int = columns.size
        override fun getColumnName(column: Int): String = columns[column]

        override fun getValueAt(rowIndex: Int, columnIndex: Int): Any {
            val row = rows[rowIndex]
            return when (columnIndex) {
                0 -> row.method
                1 -> DECIMAL_2.format(row.cpuSharePercent)
                2 -> DECIMAL_2.format(row.estimatedMspt)
                3 -> DECIMAL_2.format(row.allocatedMb)
                4 -> row.executionCount
                else -> ""
            }
        }
    }

    private class ProfilerPanel : JPanel(BorderLayout()) {
        private data class TreeEntry(
            val key: String,
            val method: String,
            val cpuSharePercent: Double,
            val estimatedMspt: Double,
            val allocatedMb: Double,
            val executionCount: Long,
            val isRoot: Boolean
        )

        private val treeModel = javax.swing.tree.DefaultTreeModel(
            javax.swing.tree.DefaultMutableTreeNode(
                TreeEntry(
                    key = "",
                    method = ServerI18n.tr("aerogel.dashboard.profiler.tree.root"),
                    cpuSharePercent = 0.0,
                    estimatedMspt = 0.0,
                    allocatedMb = 0.0,
                    executionCount = 0L,
                    isRoot = true
                )
            )
        )
        private val tree = object : javax.swing.JTree(treeModel) {
            override fun getScrollableTracksViewportWidth(): Boolean = true

            override fun paintComponent(g: java.awt.Graphics) {
                val g2 = g.create()
                try {
                    val clip = g2.clipBounds ?: return
                    val base = UIManager.getColor("Tree.textBackground") ?: Color.WHITE
                    val alt = stripeAlt(base)
                    val rh = rowHeight.coerceAtLeast(1)
                    val startRow = (clip.y / rh).coerceAtLeast(0)
                    val endRow = ((clip.y + clip.height) / rh) + 1
                    for (row in startRow..endRow) {
                        g2.color = if (row % 2 == 0) base else alt
                        val y = row * rh
                        g2.fillRect(clip.x, y, clip.width, rh)
                    }
                } finally {
                    g2.dispose()
                }
                super.paintComponent(g)
            }
        }
        private val treeMsptLabel = ServerI18n.tr("aerogel.dashboard.profiler.tree.label.mspt")
        private val treeMemLabel = ServerI18n.tr("aerogel.dashboard.profiler.tree.label.mem")
        private val treeCallsLabel = ServerI18n.tr("aerogel.dashboard.profiler.tree.label.calls")
        private val treeNoSamplesLabel = ServerI18n.tr("aerogel.dashboard.profiler.tree.no_samples")
        @Volatile private var selectedWindowSeconds = 60

        init {
            border = BorderFactory.createCompoundBorder(
                BorderFactory.createLineBorder(UIManager.getColor("Component.borderColor") ?: Color.GRAY),
                BorderFactory.createEmptyBorder(8, 8, 8, 5)
            )
            val windowCombo = javax.swing.JComboBox(
                arrayOf(
                    ProfilerWindowOption(1, ServerI18n.tr("aerogel.dashboard.profiler.window.1s")),
                    ProfilerWindowOption(10, ServerI18n.tr("aerogel.dashboard.profiler.window.10s")),
                    ProfilerWindowOption(30, ServerI18n.tr("aerogel.dashboard.profiler.window.30s")),
                    ProfilerWindowOption(60, ServerI18n.tr("aerogel.dashboard.profiler.window.1m")),
                    ProfilerWindowOption(180, ServerI18n.tr("aerogel.dashboard.profiler.window.3m")),
                    ProfilerWindowOption(300, ServerI18n.tr("aerogel.dashboard.profiler.window.5m")),
                    ProfilerWindowOption(600, ServerI18n.tr("aerogel.dashboard.profiler.window.10m")),
                    ProfilerWindowOption(1800, ServerI18n.tr("aerogel.dashboard.profiler.window.30m"))
                )
            ).apply {
                selectedIndex = 3
                addActionListener {
                    selectedWindowSeconds = (selectedItem as? ProfilerWindowOption)?.seconds ?: 60
                }
            }
            val footer = JPanel(BorderLayout()).apply {
                isOpaque = false
                border = BorderFactory.createEmptyBorder(8, 0, 0, 5)
                add(
                    JPanel(FlowLayout(FlowLayout.RIGHT, 8, 0)).apply {
                        isOpaque = false
                        border = BorderFactory.createEmptyBorder(0, 0, 0, 0)
                        add(JLabel(ServerI18n.tr("aerogel.dashboard.profiler.window.label")))
                        add(windowCombo)
                    },
                    BorderLayout.EAST,
                )
            }
            tree.showsRootHandles = true
            tree.isRootVisible = false
            tree.rowHeight = 44
            tree.font = java.awt.Font(java.awt.Font.MONOSPACED, java.awt.Font.PLAIN, 13)
            tree.isOpaque = false
            tree.cellRenderer = object : javax.swing.tree.TreeCellRenderer {
                private val panel = JPanel()
                private val methodLabel = JLabel()
                private val infoRow = JPanel()
                private val msptTitleLabel = JLabel("$treeMsptLabel ")
                private val msptValueLabel = JLabel()
                private val sep1 = JLabel(" | ")
                private val memTitleLabel = JLabel("$treeMemLabel ")
                private val memValueLabel = JLabel()
                private val sep2 = JLabel(" | ")
                private val callsTitleLabel = JLabel("$treeCallsLabel ")
                private val callsValueLabel = JLabel()

                init {
                    panel.layout = javax.swing.BoxLayout(panel, javax.swing.BoxLayout.Y_AXIS)
                    panel.border = BorderFactory.createEmptyBorder(0, 0, 0, 0)
                    methodLabel.font = java.awt.Font(java.awt.Font.MONOSPACED, java.awt.Font.PLAIN, 13)
                    methodLabel.horizontalAlignment = SwingConstants.LEFT
                    methodLabel.alignmentX = 0f
                    methodLabel.border = BorderFactory.createEmptyBorder(3, 0, 0, 0)
                    infoRow.layout = javax.swing.BoxLayout(infoRow, javax.swing.BoxLayout.X_AXIS)
                    infoRow.isOpaque = false
                    infoRow.alignmentX = 0f
                    infoRow.border = BorderFactory.createEmptyBorder(0, 0, 6, 0)
                    val infoFont = java.awt.Font(java.awt.Font.MONOSPACED, java.awt.Font.PLAIN, 12)
                    for (label in arrayOf(msptTitleLabel, msptValueLabel, sep1, memTitleLabel, memValueLabel, sep2, callsTitleLabel, callsValueLabel)) {
                        label.font = infoFont
                    }
                    panel.add(methodLabel)
                    panel.add(infoRow)
                    infoRow.add(msptTitleLabel)
                    infoRow.add(msptValueLabel)
                    infoRow.add(sep1)
                    infoRow.add(memTitleLabel)
                    infoRow.add(memValueLabel)
                    infoRow.add(sep2)
                    infoRow.add(callsTitleLabel)
                    infoRow.add(callsValueLabel)
                    methodLabel.font = java.awt.Font(java.awt.Font.MONOSPACED, java.awt.Font.BOLD, 13)
                }

                private fun baseRowX(tree: javax.swing.JTree?): Int {
                    return tree?.getRowBounds(0)?.x ?: 0
                }

                private fun cellWidthPx(tree: javax.swing.JTree?): Int {
                    val viewportWidth = tree?.parent?.width?.coerceAtLeast(280) ?: 280
                    return (viewportWidth - baseRowX(tree) - 14).coerceAtLeast(1)
                }

                private fun methodWidthPx(tree: javax.swing.JTree?, row: Int): Int {
                    val cellW = cellWidthPx(tree)
                    val rowX = if (tree != null) tree.getRowBounds(row)?.x ?: 0 else 0
                    val indentDelta = (rowX - baseRowX(tree)).coerceAtLeast(0)
                    return (cellW - 16 - indentDelta).coerceAtLeast(0)
                }

                override fun getTreeCellRendererComponent(
                    tree: javax.swing.JTree?,
                    value: Any?,
                    selected: Boolean,
                    expanded: Boolean,
                    leaf: Boolean,
                    row: Int,
                    hasFocus: Boolean
                ): java.awt.Component {
                    val node = value as? javax.swing.tree.DefaultMutableTreeNode
                    val entry = node?.userObject as? TreeEntry
                    if (entry == null || entry.isRoot) {
                        methodLabel.text = ""
                        msptValueLabel.text = ""
                        memValueLabel.text = ""
                        callsValueLabel.text = ""
                    } else {
                        methodLabel.text = clipWithDotsPx(
                            entry.method,
                            methodWidthPx(tree, row),
                            methodLabel.getFontMetrics(methodLabel.font)
                        )
                        msptValueLabel.text = String.format(Locale.US, "%6.2f ms", entry.estimatedMspt)
                        memValueLabel.text = String.format(Locale.US, "%7.2f MB", entry.allocatedMb)
                        callsValueLabel.text = String.format(Locale.US, "%,8d", entry.executionCount)
                    }
                    val bg = if (selected) {
                        UIManager.getColor("Tree.selectionBackground") ?: Color(59, 89, 152)
                    } else {
                        stripeColorForRow(tree, row)
                    }
                    val fg = if (selected) {
                        UIManager.getColor("Tree.selectionForeground") ?: Color.WHITE
                    } else {
                        UIManager.getColor("Tree.textForeground") ?: Color.BLACK
                    }
                    panel.background = bg
                    panel.isOpaque = true
                    methodLabel.foreground = fg
                    val titleColor = withAlpha(fg, 0.5)
                    msptTitleLabel.foreground = titleColor
                    sep1.foreground = titleColor
                    memTitleLabel.foreground = titleColor
                    sep2.foreground = titleColor
                    callsTitleLabel.foreground = titleColor
                    msptValueLabel.foreground = if (selected) fg else msptTierColor(entry?.estimatedMspt ?: 0.0, fg)
                    memValueLabel.foreground = fg
                    callsValueLabel.foreground = fg
                    val width = cellWidthPx(tree)
                    val height = (tree?.rowHeight ?: 22).coerceAtLeast(18)
                    panel.preferredSize = Dimension(width, height)
                    return panel
                }
            }
            val treeBody = JPanel(BorderLayout()).apply {
                isOpaque = false
                add(JScrollPane(tree), BorderLayout.CENTER)
            }
            add(treeBody, BorderLayout.CENTER)
            add(footer, BorderLayout.SOUTH)
        }

        private fun stripeAlt(base: Color): Color {
            return Color(
                (base.red * 0.94).toInt().coerceIn(0, 255),
                (base.green * 0.94).toInt().coerceIn(0, 255),
                (base.blue * 0.94).toInt().coerceIn(0, 255)
            )
        }

        private fun stripeColorForRow(tree: javax.swing.JTree?, row: Int): Color {
            val base = tree?.background ?: (UIManager.getColor("Tree.textBackground") ?: Color.WHITE)
            return if (row % 2 == 0) base else stripeAlt(base)
        }

        private fun msptTierColor(mspt: Double, normal: Color): Color {
            val usage = msptUsagePercentForTarget(mspt).toDouble()
            return when {
                usage <= 50.0 -> normal
                usage <= 80.0 -> Color(196, 160, 0)
                usage <= 100.0 -> Color(214, 120, 0)
                usage <= 140.0 -> Color(196, 64, 32)
                else -> Color(170, 24, 24)
            }
        }

        private fun withAlpha(color: Color, alphaRatio: Double): Color {
            val a = (255.0 * alphaRatio).toInt().coerceIn(0, 255)
            return Color(color.red, color.green, color.blue, a)
        }

        private fun clipWithDotsPx(text: String, maxWidthPx: Int, fm: java.awt.FontMetrics): String {
            if (fm.stringWidth(text) <= maxWidthPx) return text
            val ellipsis = "..."
            val ellipsisWidth = fm.stringWidth(ellipsis)
            val budget = (maxWidthPx - ellipsisWidth).coerceAtLeast(0)
            val out = StringBuilder()
            for (ch in text) {
                val next = out.toString() + ch
                if (fm.stringWidth(next) > budget) break
                out.append(ch)
            }
            return if (out.isEmpty()) ellipsis else out.toString() + ellipsis
        }

        fun update(snapshot: TickSnapshot, profiler: MethodProfilerSampler) {
            val methodSnapshot = profiler.snapshot(snapshot.mspt, selectedWindowSeconds)
            val expandedKeys = collectExpandedKeys(tree)
            val selectedKey = selectedKey(tree)
            val newRoot = buildTreeNode(
                node = methodSnapshot.callTreeRoot,
                currentMspt = snapshot.mspt,
                totalSamples = methodSnapshot.totalSamples,
                parentKey = ""
            )
            if (newRoot.childCount == 0) {
                newRoot.add(buildNoSamplesNode())
            }
            treeModel.setRoot(newRoot)
            restoreExpandedKeys(tree, expandedKeys)
            restoreSelectedKey(tree, selectedKey)
            val rootPath = javax.swing.tree.TreePath(newRoot.path)
            tree.expandPath(rootPath)
        }

        private fun buildNoSamplesNode(): javax.swing.tree.DefaultMutableTreeNode {
            return javax.swing.tree.DefaultMutableTreeNode(
                TreeEntry(
                    key = "<no-samples>",
                    method = treeNoSamplesLabel,
                    cpuSharePercent = 0.0,
                    estimatedMspt = 0.0,
                    allocatedMb = 0.0,
                    executionCount = 0L,
                    isRoot = false
                )
            )
        }

        private fun buildTreeNode(
            node: MethodCallTreeNode,
            currentMspt: Double,
            totalSamples: Long,
            parentKey: String
        ): javax.swing.tree.DefaultMutableTreeNode {
            val key = if (parentKey.isEmpty()) node.method else "$parentKey>${node.method}"
            val safeTotal = totalSamples.coerceAtLeast(1L).toDouble()
            val cpuShare = (node.samples.toDouble() / safeTotal) * 100.0
            val estMspt = currentMspt * (node.samples.toDouble() / safeTotal)
            val allocMb = node.allocatedBytes.toDouble() / (1024.0 * 1024.0)
            val treeNode = javax.swing.tree.DefaultMutableTreeNode(
                TreeEntry(
                    key = key,
                    method = if (node.method == "<root>") ServerI18n.tr("aerogel.dashboard.profiler.tree.root") else node.method,
                    cpuSharePercent = cpuShare,
                    estimatedMspt = estMspt,
                    allocatedMb = allocMb,
                    executionCount = node.samples,
                    isRoot = node.method == "<root>"
                )
            )
            for (child in node.children) {
                treeNode.add(buildTreeNode(child, currentMspt, totalSamples, key))
            }
            return treeNode
        }

        private fun collectExpandedKeys(tree: javax.swing.JTree): Set<String> {
            val out = HashSet<String>()
            val root = tree.model.root as? javax.swing.tree.DefaultMutableTreeNode ?: return out
            val e = root.depthFirstEnumeration()
            while (e.hasMoreElements()) {
                val node = e.nextElement() as? javax.swing.tree.DefaultMutableTreeNode ?: continue
                val path = javax.swing.tree.TreePath(node.path)
                if (!tree.isExpanded(path)) continue
                val entry = node.userObject as? TreeEntry ?: continue
                if (entry.key.isNotBlank()) {
                    out += entry.key
                }
            }
            return out
        }

        private fun restoreExpandedKeys(tree: javax.swing.JTree, keys: Set<String>) {
            if (keys.isEmpty()) return
            val root = tree.model.root as? javax.swing.tree.DefaultMutableTreeNode ?: return
            val e = root.depthFirstEnumeration()
            while (e.hasMoreElements()) {
                val node = e.nextElement() as? javax.swing.tree.DefaultMutableTreeNode ?: continue
                val entry = node.userObject as? TreeEntry ?: continue
                if (entry.key in keys) {
                    tree.expandPath(javax.swing.tree.TreePath(node.path))
                }
            }
        }

        private fun selectedKey(tree: javax.swing.JTree): String? {
            val node = tree.lastSelectedPathComponent as? javax.swing.tree.DefaultMutableTreeNode ?: return null
            val entry = node.userObject as? TreeEntry ?: return null
            return entry.key
        }

        private fun restoreSelectedKey(tree: javax.swing.JTree, key: String?) {
            if (key.isNullOrBlank()) return
            val root = tree.model.root as? javax.swing.tree.DefaultMutableTreeNode ?: return
            val e = root.depthFirstEnumeration()
            while (e.hasMoreElements()) {
                val node = e.nextElement() as? javax.swing.tree.DefaultMutableTreeNode ?: continue
                val entry = node.userObject as? TreeEntry ?: continue
                if (entry.key == key) {
                    tree.selectionPath = javax.swing.tree.TreePath(node.path)
                    return
                }
            }
        }
    }

    private class SkiaSimulationChunkMapPanel : JPanel(BorderLayout()) {
        private data class SkiaWorldRender(
            val world: String,
            val minChunkX: Int,
            val minChunkZ: Int,
            val width: Double,
            val height: Double,
            val cols: Int,
            val rows: Int,
            val cells: Array<ChunkMapCell?>
        )

        private val skiaLayer = SkiaLayer()
        private var worlds: List<ChunkMapWorld> = emptyList()
        private var players: List<MapPlayer> = emptyList()
        private var renders: List<SkiaWorldRender> = emptyList()
        private var renderingEnabled = true
        private var lastWorldsSignature: Long = Long.MIN_VALUE
        private var lastPlayersSignature: Long = Long.MIN_VALUE
        @Volatile private var pendingWorlds: List<ChunkMapWorld>? = null
        @Volatile private var pendingPlayers: List<MapPlayer>? = null
        @Volatile private var frameDirty = true
        private var zoom = 1.0
        private var targetZoom = 1.0
        private var minZoom = 1.0
        private var maxZoom = 20.0
        private val zoomLevels = 10
        private var targetZoomLevel = 0
        private var offsetX = 0.0
        private var offsetY = 0.0
        private var dragLastX = 0.0
        private var dragLastY = 0.0
        private var userViewportAdjusted = false
        private var initialMinZoomApplied = false
        private val padding = 10.0
        private val headerHeight = 0.0
        private val worldGap = 14.0
        private val usageTextMinCellPx = 36.0
        private val playerNameMinIconPx = 18.0
        private val skiaTextureCache = java.util.WeakHashMap<BufferedImage, SkiaImage>()
        private val playerHeadCache = ConcurrentHashMap<String, BufferedImage>()
        private val pendingPlayerHeadLoads = ConcurrentHashMap.newKeySet<String>()
        private val playerHeadExecutor = Executors.newFixedThreadPool(2) { runnable ->
            Thread(runnable, "dashboard-player-head-loader").apply { isDaemon = true }
        }
        private var overlayFontSizePx = 0f
        private var overlayFont = SkiaFont(null, 12f)
        private val overlayUsageLineCache = HashMap<Int, TextLine>()
        private var playerNameFontSizePx = 0f
        private var playerNameFont = SkiaFont(null, 12f)
        private val playerNameLineCache = object : LinkedHashMap<String, TextLine>(256, 0.75f, true) {
            override fun removeEldestEntry(eldest: MutableMap.MutableEntry<String, TextLine>?): Boolean {
                return size > 512
            }
        }
        private val linearMipmapSampling: SamplingMode = FilterMipmap(FilterMode.LINEAR, MipmapMode.LINEAR)
        private val nearestSampling: SamplingMode = FilterMipmap(FilterMode.NEAREST, MipmapMode.NONE)
        private val redrawTimer = Timer(refreshPeriodMs()) {
            if (!renderingEnabled) return@Timer
            syncPendingState()
            if (!frameDirty) return@Timer
            skiaLayer.needRedraw()
            frameDirty = false
        }.apply { isRepeats = true }
        private val defaultFont = SkiaFont(null, 12f)

        init {
            border = BorderFactory.createEmptyBorder(6, 6, 6, 6)
            preferredSize = Dimension(900, 300)
            skiaLayer.renderDelegate = SkiaLayerRenderDelegate(skiaLayer, object : SkikoRenderDelegate {
                override fun onRender(canvas: org.jetbrains.skia.Canvas, width: Int, height: Int, nanoTime: Long) {
                    renderSkia(canvas, width, height)
                }
            })
            skiaLayer.attachTo(this)
            installInputHandlers()
            redrawTimer.start()
        }

        fun setWorlds(next: List<ChunkMapWorld>) {
            worlds = next
            pendingWorlds = next
            frameDirty = true
        }

        fun setPlayers(next: List<MapPlayer>) {
            val nextSignature = playersSignature(next)
            if (nextSignature == lastPlayersSignature) return
            lastPlayersSignature = nextSignature
            players = next
            pendingPlayers = next
            frameDirty = true
        }

        fun setRenderingEnabled(enabled: Boolean) {
            if (renderingEnabled == enabled) return
            renderingEnabled = enabled
            if (enabled) {
                pendingWorlds = worlds
                pendingPlayers = players
                frameDirty = true
                redrawTimer.start()
            } else {
                redrawTimer.stop()
            }
        }

        private fun installInputHandlers() {
            skiaLayer.addMouseListener(object : java.awt.event.MouseAdapter() {
                override fun mousePressed(e: java.awt.event.MouseEvent) {
                    dragLastX = e.x.toDouble()
                    dragLastY = e.y.toDouble()
                }
            })
            skiaLayer.addMouseMotionListener(object : java.awt.event.MouseMotionAdapter() {
                override fun mouseDragged(e: java.awt.event.MouseEvent) {
                    val dx = e.x - dragLastX
                    val dy = e.y - dragLastY
                    offsetX += dx
                    offsetY += dy
                    userViewportAdjusted = true
                    dragLastX = e.x.toDouble()
                    dragLastY = e.y.toDouble()
                    clampOffsets(skiaLayer.width.toDouble(), skiaLayer.height.toDouble())
                    frameDirty = true
                }
            })
            skiaLayer.addMouseWheelListener { event ->
                if (event.preciseWheelRotation == 0.0) return@addMouseWheelListener
                recomputeZoomBounds(skiaLayer.width.toDouble(), skiaLayer.height.toDouble())
                val nextLevel = (targetZoomLevel + if (event.preciseWheelRotation < 0.0) 1 else -1).coerceIn(0, zoomLevels)
                if (nextLevel == targetZoomLevel) return@addMouseWheelListener
                val anchorX = event.x.toDouble()
                val anchorY = event.y.toDouble()
                val worldX = (anchorX - offsetX) / zoom
                val worldY = (anchorY - offsetY) / zoom
                targetZoomLevel = nextLevel
                targetZoom = zoomForLevel(targetZoomLevel)
                zoom = targetZoom
                offsetX = anchorX - worldX * zoom
                offsetY = anchorY - worldY * zoom
                userViewportAdjusted = true
                clampOffsets(skiaLayer.width.toDouble(), skiaLayer.height.toDouble())
                frameDirty = true
            }
            addComponentListener(object : java.awt.event.ComponentAdapter() {
                override fun componentResized(e: java.awt.event.ComponentEvent?) {
                    recomputeZoomBounds(skiaLayer.width.toDouble(), skiaLayer.height.toDouble())
                    if (!userViewportAdjusted || targetZoomLevel == 0) {
                        zoom = minZoom
                        targetZoom = zoom
                        targetZoomLevel = 0
                        centerContent(skiaLayer.width.toDouble(), skiaLayer.height.toDouble())
                    } else {
                        clampOffsets(skiaLayer.width.toDouble(), skiaLayer.height.toDouble())
                    }
                    frameDirty = true
                }
            })
        }

        private fun syncPendingState() {
            val nextWorlds = pendingWorlds.also { pendingWorlds = null }
            var worldsChanged = false
            if (nextWorlds != null) {
                val sig = worldsSignature(nextWorlds)
                val structureChanged = sig != lastWorldsSignature
                lastWorldsSignature = sig
                renders = buildWorldRenders(nextWorlds)
                worldsChanged = true
                if (structureChanged) {
                    if (renders.isEmpty()) {
                        initialMinZoomApplied = false
                        userViewportAdjusted = false
                    }
                    recomputeZoomBounds(skiaLayer.width.toDouble(), skiaLayer.height.toDouble())
                    if (!initialMinZoomApplied && renders.isNotEmpty()) {
                        zoom = minZoom
                        targetZoom = zoom
                        targetZoomLevel = 0
                        initialMinZoomApplied = true
                        centerContent(skiaLayer.width.toDouble(), skiaLayer.height.toDouble())
                    } else {
                        zoom = zoom.coerceIn(minZoom, maxZoom)
                        targetZoomLevel = zoomLevelFor(targetZoom)
                        targetZoom = zoomForLevel(targetZoomLevel)
                        clampOffsets(skiaLayer.width.toDouble(), skiaLayer.height.toDouble())
                    }
                }
            }
            val nextPlayers = pendingPlayers.also { pendingPlayers = null }
            if (worldsChanged || nextPlayers != null) {
                frameDirty = true
            }
        }

        private fun buildWorldRenders(input: List<ChunkMapWorld>): List<SkiaWorldRender> {
            if (input.isEmpty()) return emptyList()
            val out = ArrayList<SkiaWorldRender>(input.size)
            for (world in input) {
                val cols = (world.maxChunkX - world.minChunkX + 1).coerceAtLeast(1)
                val rows = (world.maxChunkZ - world.minChunkZ + 1).coerceAtLeast(1)
                val cells = arrayOfNulls<ChunkMapCell>(cols * rows)
                for ((coord, cell) in world.cells) {
                    val col = coord.first - world.minChunkX
                    val row = coord.second - world.minChunkZ
                    if (col !in 0 until cols || row !in 0 until rows) continue
                    cells[row * cols + col] = cell
                }
                out += SkiaWorldRender(
                    world = world.world,
                    minChunkX = world.minChunkX,
                    minChunkZ = world.minChunkZ,
                    width = (cols * CHUNK_TEXTURE_SIZE).toDouble(),
                    height = (rows * CHUNK_TEXTURE_SIZE).toDouble(),
                    cols = cols,
                    rows = rows,
                    cells = cells
                )
            }
            return out
        }

        private fun worldsSignature(input: List<ChunkMapWorld>): Long {
            var h = 1125899906842597L
            for (world in input) {
                h = h * 31 + world.world.hashCode().toLong()
                h = h * 31 + world.signature
            }
            return h
        }

        private fun playersSignature(input: List<MapPlayer>): Long {
            var h = 1469598103934665603L
            for (player in input) {
                h = h * 1099511628211L + player.uuid.hashCode().toLong()
                h = h * 1099511628211L + player.world.hashCode().toLong()
                val qx = (player.x * 4.0).roundToInt().toLong()
                val qz = (player.z * 4.0).roundToInt().toLong()
                h = h * 1099511628211L + qx
                h = h * 1099511628211L + qz
            }
            return h
        }

        private fun contentWidth(): Double {
            if (renders.isEmpty()) return 100.0 + padding * 2.0
            var maxWidth = 100.0 + padding * 2.0
            for (w in renders) {
                maxWidth = max(maxWidth, w.width + padding * 2.0)
            }
            return maxWidth
        }

        private fun contentHeight(): Double {
            if (renders.isEmpty()) return 70.0 + padding * 2.0
            var total = padding
            for (w in renders) {
                total += headerHeight + w.height + worldGap
            }
            return max(total, 70.0 + padding * 2.0)
        }

        private fun recomputeZoomBounds(canvasW: Double, canvasH: Double) {
            val contentW = contentWidth()
            val contentH = contentHeight()
            if (canvasW <= 0.0 || canvasH <= 0.0 || contentW <= 0.0 || contentH <= 0.0) {
                minZoom = 1.0
                maxZoom = 1.0
                return
            }
            val fitX = canvasW / contentW
            val fitY = canvasH / contentH
            minZoom = min(fitX, fitY).coerceAtMost(1.0).coerceAtLeast(0.02)
            maxZoom = 24.0
            targetZoomLevel = zoomLevelFor(targetZoom)
            targetZoom = zoomForLevel(targetZoomLevel)
        }

        private fun zoomForLevel(level: Int): Double {
            val clamped = level.coerceIn(0, zoomLevels)
            if (maxZoom <= minZoom + 1.0e-9) return minZoom
            val t = clamped.toDouble() / zoomLevels.toDouble()
            return minZoom * (maxZoom / minZoom).pow(t)
        }

        private fun zoomLevelFor(value: Double): Int {
            val v = value.coerceIn(minZoom, maxZoom)
            if (maxZoom <= minZoom + 1.0e-9) return 0
            val ratio = ln(v / minZoom) / ln(maxZoom / minZoom)
            return (ratio * zoomLevels.toDouble()).roundToInt().coerceIn(0, zoomLevels)
        }

        private fun centerContent(canvasW: Double, canvasH: Double) {
            val contentW = contentWidth() * zoom
            val contentH = contentHeight() * zoom
            offsetX = (canvasW - contentW) / 2.0
            offsetY = (canvasH - contentH) / 2.0
            clampOffsets(canvasW, canvasH)
        }

        private fun clampOffsets(canvasW: Double, canvasH: Double) {
            val scaledW = contentWidth() * zoom
            val scaledH = contentHeight() * zoom
            offsetX = if (scaledW <= canvasW) (canvasW - scaledW) / 2.0 else offsetX.coerceIn(canvasW - scaledW, 0.0)
            offsetY = if (scaledH <= canvasH) (canvasH - scaledH) / 2.0 else offsetY.coerceIn(canvasH - scaledH, 0.0)
            offsetX = offsetX.roundToInt().toDouble()
            offsetY = offsetY.roundToInt().toDouble()
        }

        private fun renderSkia(canvas: org.jetbrains.skia.Canvas, width: Int, height: Int) {
            val w = width.toDouble()
            val h = height.toDouble()
            val bg = UIManager.getColor("Panel.background") ?: Color(30, 33, 37)
            SkiaPaint().use { p ->
                p.color = toSkiaColor(bg)
                p.mode = PaintMode.FILL
                canvas.drawRect(Rect.makeXYWH(0f, 0f, width.toFloat(), height.toFloat()), p)
            }
            if (renders.isEmpty()) {
                drawText(canvas, ServerI18n.tr("aerogel.dashboard.map.empty"), 14f, 24f, Color(220, 220, 220), 12f)
                return
            }

            val worldByName = HashMap<String, SkiaWorldRender>(renders.size)
            for (wr in renders) worldByName[wr.world] = wr
            var y = padding
            for (wr in renders) {
                val imageTop = y + headerHeight
                val drawX = offsetX + padding * zoom
                val drawY = offsetY + imageTop * zoom
                val drawW = max(wr.width * zoom, 1.0)
                val drawH = max(wr.height * zoom, 1.0)
                drawWorldTiles(canvas, wr, drawX, drawY, drawW, drawH, w, h)
                drawPlayers(canvas, wr, worldByName, drawX, drawY, drawW, drawH)
                drawGridOverlay(canvas, wr, drawX, drawY, drawW, drawH, w, h)
                y = imageTop + wr.height + worldGap
            }
        }

        private fun drawWorldTiles(
            canvas: org.jetbrains.skia.Canvas,
            world: SkiaWorldRender,
            drawX: Double,
            drawY: Double,
            drawW: Double,
            drawH: Double,
            canvasW: Double,
            canvasH: Double
        ): Long {
            val cols = world.cols
            val rows = world.rows
            if (cols <= 0 || rows <= 0 || drawW <= 0.0 || drawH <= 0.0) return 0L
            val cellW = drawW / cols.toDouble()
            val cellH = drawH / rows.toDouble()
            val visibleColStart = floor(((0.0 - drawX) / cellW)).toInt().coerceIn(0, cols - 1)
            val visibleColEnd = ceil(((canvasW - drawX) / cellW)).toInt().coerceIn(0, cols - 1)
            val visibleRowStart = floor(((0.0 - drawY) / cellH)).toInt().coerceIn(0, rows - 1)
            val visibleRowEnd = ceil(((canvasH - drawY) / cellH)).toInt().coerceIn(0, rows - 1)
            if (visibleColStart > visibleColEnd || visibleRowStart > visibleRowEnd) return 0L

            var rendered = 0L
            SkiaPaint().use { p ->
                p.mode = PaintMode.FILL
                for (row in visibleRowStart..visibleRowEnd) {
                    val base = row * cols
                    for (col in visibleColStart..visibleColEnd) {
                        val cell = world.cells[base + col] ?: continue
                        val x0 = floor(drawX + col * cellW).toInt()
                        val y0 = floor(drawY + row * cellH).toInt()
                        val x1 = ceil(drawX + (col + 1) * cellW).toInt()
                        val y1 = ceil(drawY + (row + 1) * cellH).toInt()
                        val rw = (x1 - x0).coerceAtLeast(1).toDouble()
                        val rh = (y1 - y0).coerceAtLeast(1).toDouble()
                        val drawTexture = if (cell.idle) cell.idleTexture else cell.texture
                        val image = skiaImageFor(drawTexture)
                        if (image != null) {
                            drawChunkTexture(canvas, p, image, x0.toDouble(), y0.toDouble(), rw, rh)
                        } else {
                            p.color = toSkiaColor(if (cell.idle) Color(66, 71, 78) else cellColor(cell.mspt))
                            canvas.drawRect(Rect.makeXYWH(x0.toFloat(), y0.toFloat(), rw.toFloat(), rh.toFloat()), p)
                        }
                        rendered += (rw * rh).toLong().coerceAtLeast(0L)
                    }
                }
            }
            return rendered
        }

        private fun drawChunkTexture(
            canvas: org.jetbrains.skia.Canvas,
            paint: SkiaPaint,
            image: SkiaImage,
            x: Double,
            y: Double,
            w: Double,
            h: Double
        ) {
            val src = Rect.makeWH(image.width.toFloat(), image.height.toFloat())
            val dst = Rect.makeXYWH(x.toFloat(), y.toFloat(), w.toFloat(), h.toFloat())
            val sampling = if (w >= image.width.toDouble() && h >= image.height.toDouble()) {
                nearestSampling
            } else {
                linearMipmapSampling
            }
            canvas.drawImageRect(image, src, dst, sampling, paint, true)
        }

        private fun skiaImageFor(texture: BufferedImage): SkiaImage? {
            synchronized(skiaTextureCache) {
                val cached = skiaTextureCache[texture]
                if (cached != null) return cached
            }
            val width = texture.width.coerceAtLeast(1)
            val height = texture.height.coerceAtLeast(1)
            val pixels = ByteArray(width * height * 4)
            for (y in 0 until height) {
                for (x in 0 until width) {
                    val rgb = texture.getRGB(x, y)
                    val offset = (y * width + x) * 4
                    pixels[offset] = ((rgb ushr 16) and 0xFF).toByte()
                    pixels[offset + 1] = ((rgb ushr 8) and 0xFF).toByte()
                    pixels[offset + 2] = (rgb and 0xFF).toByte()
                    pixels[offset + 3] = 0xFF.toByte()
                }
            }
            val imageInfo = ImageInfo(width, height, ColorType.RGBA_8888, ColorAlphaType.OPAQUE)
            val image = runCatching { SkiaImage.makeRaster(imageInfo, pixels, width * 4) }.getOrNull() ?: return null
            synchronized(skiaTextureCache) {
                skiaTextureCache[texture] = image
            }
            return image
        }

        private fun drawGridOverlay(
            canvas: org.jetbrains.skia.Canvas,
            world: SkiaWorldRender,
            drawX: Double,
            drawY: Double,
            drawW: Double,
            drawH: Double,
            canvasW: Double,
            canvasH: Double
        ) {
            val cols = world.cols
            val rows = world.rows
            if (cols <= 0 || rows <= 0 || drawW <= 0.0 || drawH <= 0.0) return
            val cellW = drawW / cols.toDouble()
            val cellH = drawH / rows.toDouble()
            val visibleColStart = floor(((0.0 - drawX) / cellW)).toInt().coerceIn(0, cols - 1)
            val visibleColEnd = ceil(((canvasW - drawX) / cellW)).toInt().coerceIn(0, cols - 1)
            val visibleRowStart = floor(((0.0 - drawY) / cellH)).toInt().coerceIn(0, rows - 1)
            val visibleRowEnd = ceil(((canvasH - drawY) / cellH)).toInt().coerceIn(0, rows - 1)
            if (visibleColStart > visibleColEnd || visibleRowStart > visibleRowEnd) return

            val showUsageText = cellW >= usageTextMinCellPx && cellH >= usageTextMinCellPx
            if (!showUsageText) return

            SkiaPaint().use { stroke ->
                stroke.mode = PaintMode.STROKE
                stroke.strokeWidth = (min(cellW, cellH) * 0.04).coerceIn(1.0, 2.0).toFloat()
                stroke.color = toSkiaColor(Color(236, 236, 236, 180))
                val rowFont = overlayFontFor(cellW, cellH)
                SkiaPaint().use { textPaint ->
                    textPaint.mode = PaintMode.FILL
                    textPaint.color = toSkiaColor(Color(244, 244, 244))
                    for (row in visibleRowStart..visibleRowEnd) {
                        val base = row * cols
                        for (col in visibleColStart..visibleColEnd) {
                            val cell = world.cells[base + col] ?: continue
                            if (cell.idle) continue
                            val x = drawX + col * cellW
                            val y = drawY + row * cellH
                            val w = max(cellW - 1.0, 1.0)
                            val h = max(cellH - 1.0, 1.0)
                            canvas.drawRect(Rect.makeXYWH((x + 0.5).toFloat(), (y + 0.5).toFloat(), w.toFloat(), h.toFloat()), stroke)
                            val usage = msptUsagePercentForTarget(cell.mspt)
                            val line = overlayUsageLine(usage, rowFont)
                            val tx = (x + (cellW * 0.5) - (line.width / 2.0)).toFloat()
                            val ty = (y + (cellH * 0.5) + (line.height / 3.0)).toFloat()
                            canvas.drawTextLine(line, tx, ty, textPaint)
                        }
                    }
                }
            }
        }

        private fun drawPlayers(
            canvas: org.jetbrains.skia.Canvas,
            world: SkiaWorldRender,
            worldByName: Map<String, SkiaWorldRender>,
            drawX: Double,
            drawY: Double,
            drawW: Double,
            drawH: Double
        ) {
            val iconSize = (max(drawW / world.width, drawH / world.height) * PLAYER_HEAD_TEXTURE_SIZE).coerceIn(6.0, 24.0)
            val showName = iconSize >= playerNameMinIconPx
            SkiaPaint().use { p ->
                p.mode = PaintMode.FILL
                p.color = toSkiaColor(Color(233, 233, 233))
                val nameFont = playerNameFontFor(iconSize)
                for (player in players) {
                    val targetWorld = worldByName[player.world] ?: continue
                    if (targetWorld.world != world.world) continue
                    if (!player.x.isFinite() || !player.z.isFinite()) continue
                    val imageX = player.x - (world.minChunkX * CHUNK_TEXTURE_SIZE).toDouble()
                    val imageZ = player.z - (world.minChunkZ * CHUNK_TEXTURE_SIZE).toDouble()
                    if (imageX < -4.0 || imageZ < -4.0 || imageX > world.width + 4.0 || imageZ > world.height + 4.0) continue
                    val xRatio = (imageX / world.width).coerceIn(0.0, 1.0)
                    val zRatio = (imageZ / world.height).coerceIn(0.0, 1.0)
                    val px = drawX + xRatio * drawW
                    val py = drawY + zRatio * drawH
                    val left = px - (iconSize * 0.5)
                    val top = py - (iconSize * 0.5)
                    val head = playerHeadFor(player)
                    if (head != null) {
                        val image = skiaImageFor(head)
                        if (image != null) {
                            val src = Rect.makeWH(image.width.toFloat(), image.height.toFloat())
                            val dst = Rect.makeXYWH(left.toFloat(), top.toFloat(), iconSize.toFloat(), iconSize.toFloat())
                            canvas.drawImageRect(image, src, dst, nearestSampling, p, true)
                        } else {
                            canvas.drawCircle(px.toFloat(), py.toFloat(), (iconSize * 0.5).toFloat(), p)
                        }
                    } else {
                        canvas.drawCircle(px.toFloat(), py.toFloat(), (iconSize * 0.5).toFloat(), p)
                    }
                    if (showName && player.name.isNotBlank()) {
                        val label = playerNameLine(player.name, nameFont)
                        val tx = (px - (label.width * 0.5)).toFloat()
                        val ty = (py + iconSize * 0.9 + label.height * 0.35).toFloat()
                        canvas.drawTextLine(label, tx, ty, p.apply { color = toSkiaColor(Color(244, 244, 244)) })
                    }
                }
            }
        }

        private fun overlayFontFor(cellW: Double, cellH: Double): SkiaFont {
            val nextSize = (min(cellW, cellH) * 0.26).coerceIn(8.0, 42.0).toFloat()
            if (kotlin.math.abs(nextSize - overlayFontSizePx) > 0.25f) {
                overlayFontSizePx = nextSize
                overlayFont = SkiaFont(null, nextSize)
                overlayUsageLineCache.clear()
            }
            return overlayFont
        }

        private fun overlayUsageLine(usage: Int, font: SkiaFont): TextLine {
            return overlayUsageLineCache.getOrPut(usage) { TextLine.make("$usage%", font) }
        }

        private fun playerNameFontFor(iconSize: Double): SkiaFont {
            val nextSize = (iconSize * 0.5).toFloat()
            if (kotlin.math.abs(nextSize - playerNameFontSizePx) > 0.25f) {
                playerNameFontSizePx = nextSize
                playerNameFont = SkiaFont(null, nextSize)
                playerNameLineCache.clear()
            }
            return playerNameFont
        }

        private fun playerNameLine(name: String, font: SkiaFont): TextLine {
            return playerNameLineCache.getOrPut(name) { TextLine.make(name, font) }
        }

        private fun playerHeadFor(player: MapPlayer): BufferedImage? {
            val skinProp = player.texturesPropertyValue?.trim().orEmpty()
            if (skinProp.isEmpty()) return null
            val includeHat = (player.skinPartsMask and SKIN_PART_HAT_BIT) != 0
            val key = "$skinProp|${if (includeHat) 1 else 0}"
            val cached = playerHeadCache[key]
            if (cached != null) return cached
            if (!pendingPlayerHeadLoads.add(key)) return null
            playerHeadExecutor.execute {
                try {
                    val skinUrl = decodeSkinUrl(skinProp)
                    if (skinUrl.isNullOrBlank()) return@execute
                    val skin = runCatching { ImageIO.read(URL(skinUrl)) }.getOrNull() ?: return@execute
                    if (skin.width < 16 || skin.height < 16) return@execute
                    val head = buildPlayerHeadTexture(skin, includeHat)
                    playerHeadCache[key] = head
                    frameDirty = true
                } catch (_: Throwable) {
                    // Ignore invalid skins/network failures.
                } finally {
                    pendingPlayerHeadLoads.remove(key)
                }
            }
            return null
        }

        private fun buildPlayerHeadTexture(skin: BufferedImage, includeHat: Boolean): BufferedImage {
            val out = BufferedImage(PLAYER_HEAD_TEXTURE_SIZE, PLAYER_HEAD_TEXTURE_SIZE, BufferedImage.TYPE_INT_ARGB)
            val g2 = out.createGraphics()
            try {
                g2.drawImage(
                    skin,
                    0,
                    0,
                    PLAYER_HEAD_TEXTURE_SIZE,
                    PLAYER_HEAD_TEXTURE_SIZE,
                    8,
                    8,
                    16,
                    16,
                    null
                )
                if (includeHat && skin.width >= 64 && skin.height >= 16) {
                    g2.drawImage(
                        skin,
                        0,
                        0,
                        PLAYER_HEAD_TEXTURE_SIZE,
                        PLAYER_HEAD_TEXTURE_SIZE,
                        40,
                        8,
                        48,
                        16,
                        null
                    )
                }
            } finally {
                g2.dispose()
            }
            return out
        }

        private fun drawText(
            canvas: org.jetbrains.skia.Canvas,
            text: String,
            x: Float,
            y: Float,
            color: Color,
            size: Float
        ) {
            SkiaPaint().use { p ->
                p.mode = PaintMode.FILL
                p.color = toSkiaColor(color)
                val font = if (size == 12f) defaultFont else SkiaFont(null, size)
                val line = TextLine.make(text, font)
                canvas.drawTextLine(line, x, y, p)
            }
        }

        private fun cellColor(mspt: Double): Color {
            val value = if (mspt.isFinite()) mspt.coerceAtLeast(0.0) else 0.0
            if (value <= 0.0001) return Color(60, 68, 78)
            val ratio = value / targetMsptUpperBound().coerceAtLeast(1.0e-9)
            return when {
                ratio < 0.5 -> blend(Color(47, 158, 68), Color(247, 197, 72), ratio / 0.5)
                ratio < 1.0 -> blend(Color(247, 197, 72), Color(231, 76, 60), (ratio - 0.5) / 0.5)
                else -> {
                    val t = ((ratio - 1.0) / (ratio + 1.0)).coerceIn(0.0, 1.0)
                    blend(Color(231, 76, 60), Color(128, 24, 24), t)
                }
            }
        }

        private fun blend(from: Color, to: Color, t: Double): Color {
            val ratio = t.coerceIn(0.0, 1.0)
            val red = (from.red + (to.red - from.red) * ratio).toInt().coerceIn(0, 255)
            val green = (from.green + (to.green - from.green) * ratio).toInt().coerceIn(0, 255)
            val blue = (from.blue + (to.blue - from.blue) * ratio).toInt().coerceIn(0, 255)
            return Color(red, green, blue)
        }

        private fun toSkiaColor(color: Color): Int {
            return ((color.alpha and 0xFF) shl 24) or
                ((color.red and 0xFF) shl 16) or
                ((color.green and 0xFF) shl 8) or
                (color.blue and 0xFF)
        }

        private fun refreshPeriodMs(): Int {
            val hz = runCatching {
                val rate = GraphicsEnvironment.getLocalGraphicsEnvironment()
                    .defaultScreenDevice
                    .displayMode
                    .refreshRate
                if (rate <= 0) 60 else rate
            }.getOrDefault(60)
            return (1000.0 / hz.toDouble()).roundToInt().coerceIn(4, 34)
        }
    }

    private class SimulationChunkMapPanel : JPanel() {
        private var worlds: List<ChunkMapWorld> = emptyList()

        private val headerHeight = 22
        private val worldGap = 14
        private val baseCellSize = 56
        private val padding = 10
        private var zoomScale = 1.0
        private var targetZoomScale = 1.0
        private var minZoomScale = 0.2
        private var maxZoomScale = 8.0
        private var dragAnchorScreenX: Int? = null
        private var dragAnchorScreenY: Int? = null
        private var zoomAnchor: ZoomAnchor? = null
        private var initialMinZoomApplied = false
        private val zoomAnimationTimer = Timer(16) { event ->
            val next = zoomScale + (targetZoomScale - zoomScale) * 0.22
            val snapped = if (abs(targetZoomScale - next) <= 1.0e-4) targetZoomScale else next
            if (abs(snapped - zoomScale) <= 1.0e-6) {
                (event.source as? Timer)?.stop()
                return@Timer
            }
            val beforeContentWidth = scaledContentWidth()
            val beforeContentHeight = scaledContentHeight()
            zoomScale = snapped
            updatePreferredSize()
            applyZoomAnchor(beforeContentWidth, beforeContentHeight)
            repaint()
            if (snapped == targetZoomScale) {
                (event.source as? Timer)?.stop()
            }
        }

        init {
            border = BorderFactory.createEmptyBorder(6, 6, 6, 6)
            zoomAnimationTimer.isRepeats = true
            addMouseWheelListener { event ->
                if (event.wheelRotation == 0) return@addMouseWheelListener
                updateZoomBounds()
                zoomAnchor = captureZoomAnchor(event.point)
                val factor = exp(-event.wheelRotation * 0.12)
                targetZoomScale = (targetZoomScale * factor).coerceIn(minZoomScale, maxZoomScale)
                if (!zoomAnimationTimer.isRunning) {
                    zoomAnimationTimer.start()
                }
            }
            addMouseListener(object : java.awt.event.MouseAdapter() {
                override fun mousePressed(e: java.awt.event.MouseEvent) {
                    dragAnchorScreenX = e.xOnScreen
                    dragAnchorScreenY = e.yOnScreen
                    cursor = Cursor.getPredefinedCursor(Cursor.MOVE_CURSOR)
                }

                override fun mouseReleased(e: java.awt.event.MouseEvent) {
                    dragAnchorScreenX = null
                    dragAnchorScreenY = null
                    cursor = Cursor.getDefaultCursor()
                }
            })
            addMouseMotionListener(object : java.awt.event.MouseMotionAdapter() {
                override fun mouseDragged(e: java.awt.event.MouseEvent) {
                    val anchorX = dragAnchorScreenX ?: return
                    val anchorY = dragAnchorScreenY ?: return
                    val scroll = SwingUtilities.getAncestorOfClass(JScrollPane::class.java, this@SimulationChunkMapPanel) as? JScrollPane
                    val viewport = scroll?.viewport ?: return
                    val viewPos = viewport.viewPosition
                    val dx = e.xOnScreen - anchorX
                    val dy = e.yOnScreen - anchorY
                    val maxX = (width - viewport.width).coerceAtLeast(0)
                    val maxY = (height - viewport.height).coerceAtLeast(0)
                    val nextX = (viewPos.x - dx).coerceIn(0, maxX)
                    val nextY = (viewPos.y - dy).coerceIn(0, maxY)
                    viewport.viewPosition = Point(nextX, nextY)
                    dragAnchorScreenX = e.xOnScreen
                    dragAnchorScreenY = e.yOnScreen
                }
            })
            addHierarchyListener {
                if ((it.changeFlags and java.awt.event.HierarchyEvent.SHOWING_CHANGED.toLong()) != 0L) {
                    updateZoomBounds()
                    updatePreferredSize()
                    centerAtMinZoom()
                }
            }
        }

        fun setWorlds(next: List<ChunkMapWorld>) {
            if (worlds === next) return
            worlds = next
            if (worlds.isEmpty()) {
                initialMinZoomApplied = false
            }
            updateZoomBounds()
            if (!initialMinZoomApplied && worlds.isNotEmpty()) {
                zoomScale = minZoomScale
                targetZoomScale = minZoomScale
                initialMinZoomApplied = true
            } else if (zoomScale < minZoomScale || zoomScale > maxZoomScale) {
                zoomScale = zoomScale.coerceIn(minZoomScale, maxZoomScale)
            }
            targetZoomScale = targetZoomScale.coerceIn(minZoomScale, maxZoomScale)
            zoomAnchor = null
            updatePreferredSize()
            centerAtMinZoom()
            repaint()
        }

        private fun updatePreferredSize() {
            val contentWidth = scaledContentWidth()
            val contentHeight = scaledContentHeight()
            val scroll = SwingUtilities.getAncestorOfClass(JScrollPane::class.java, this) as? JScrollPane
            val extent = scroll?.viewport?.extentSize
            val preferredW = if (extent == null) max(contentWidth, 760) else max(contentWidth, extent.width)
            val preferredH = if (extent == null) max(contentHeight, 140) else max(contentHeight, extent.height)
            val old = preferredSize
            if (abs(old.width - preferredW) <= 1 && abs(old.height - preferredH) <= 1) return
            preferredSize = Dimension(preferredW, preferredH)
            revalidate()
        }

        private fun updateZoomBounds() {
            val unscaledWidth = unscaledContentWidth()
            val unscaledHeight = unscaledContentHeight()
            if (unscaledWidth <= 0 || unscaledHeight <= 0) {
                minZoomScale = 1.0
                maxZoomScale = 1.0
                zoomScale = 1.0
                return
            }

            val scroll = SwingUtilities.getAncestorOfClass(JScrollPane::class.java, this) as? JScrollPane
            val extent = scroll?.viewport?.extentSize
            if (extent == null || extent.width <= 0 || extent.height <= 0) {
                minZoomScale = 1.0
                maxZoomScale = 1.0
                zoomScale = 1.0
                return
            }

            val fitX = extent.width.toDouble() / unscaledWidth.toDouble()
            val fitY = extent.height.toDouble() / unscaledHeight.toDouble()
            minZoomScale = min(fitX, fitY).coerceAtMost(1.0).coerceAtLeast(0.02)
            val zoomOneChunk = min(extent.width, extent.height).toDouble() / baseCellSize.toDouble()
            maxZoomScale = zoomOneChunk.coerceAtLeast(minZoomScale)
            zoomScale = zoomScale.coerceIn(minZoomScale, maxZoomScale)
            targetZoomScale = targetZoomScale.coerceIn(minZoomScale, maxZoomScale)
        }

        private fun unscaledContentWidth(): Int {
            if (worlds.isEmpty()) return padding * 2 + 100
            var maxWidth = padding * 2 + baseCellSize
            for (world in worlds) {
                val widthInCells = world.maxChunkX - world.minChunkX + 1
                maxWidth = max(maxWidth, padding * 2 + widthInCells * baseCellSize)
            }
            return maxWidth
        }

        private fun unscaledContentHeight(): Int {
            if (worlds.isEmpty()) return padding * 2 + 70
            var total = padding
            for (world in worlds) {
                val heightInCells = world.maxChunkZ - world.minChunkZ + 1
                total += headerHeight + heightInCells * baseCellSize + worldGap
            }
            return max(total, padding * 2 + 70)
        }

        private fun scaledContentWidth(): Int {
            if (worlds.isEmpty()) return padding * 2 + 100
            var maxWidth = padding * 2 + (baseCellSize.toDouble() * zoomScale).roundToInt()
            for (world in worlds) {
                val widthInCells = world.maxChunkX - world.minChunkX + 1
                val scaledGridWidth = (widthInCells * baseCellSize.toDouble() * zoomScale).roundToInt()
                maxWidth = max(maxWidth, padding * 2 + scaledGridWidth)
            }
            return maxWidth
        }

        private fun scaledContentHeight(): Int {
            if (worlds.isEmpty()) return padding * 2 + 70
            var total = padding
            for (world in worlds) {
                val heightInCells = world.maxChunkZ - world.minChunkZ + 1
                val scaledGridHeight = (heightInCells * baseCellSize.toDouble() * zoomScale).roundToInt()
                total += headerHeight + scaledGridHeight + worldGap
            }
            return max(total, padding * 2 + 70)
        }

        private fun centerAtMinZoom() {
            if (zoomScale > minZoomScale + 1.0e-6) return
            val scroll = SwingUtilities.getAncestorOfClass(JScrollPane::class.java, this) as? JScrollPane ?: return
            val viewport = scroll.viewport
            val maxX = (width - viewport.width).coerceAtLeast(0)
            val maxY = (height - viewport.height).coerceAtLeast(0)
            viewport.viewPosition = Point(maxX / 2, maxY / 2)
        }

        private fun captureZoomAnchor(point: Point): ZoomAnchor? {
            val scroll = SwingUtilities.getAncestorOfClass(JScrollPane::class.java, this) as? JScrollPane ?: return null
            val viewport = scroll.viewport ?: return null
            if (viewport.width <= 0 || viewport.height <= 0) return null
            val viewX = point.x - viewport.viewPosition.x
            val viewY = point.y - viewport.viewPosition.y
            if (viewX !in 0..viewport.width || viewY !in 0..viewport.height) return null

            val contentWidth = scaledContentWidth()
            val contentHeight = scaledContentHeight()
            val baseX = ((width - contentWidth) / 2).coerceAtLeast(padding)
            val baseY = ((height - contentHeight) / 2).coerceAtLeast(padding)
            val relativeX = (point.x - baseX).toDouble()
            val relativeY = (point.y - baseY).toDouble()
            val normX = if (contentWidth <= 1) 0.5 else (relativeX / contentWidth.toDouble()).coerceIn(0.0, 1.0)
            val normY = if (contentHeight <= 1) 0.5 else (relativeY / contentHeight.toDouble()).coerceIn(0.0, 1.0)
            return ZoomAnchor(
                viewportX = viewX,
                viewportY = viewY,
                normX = normX,
                normY = normY
            )
        }

        private fun applyZoomAnchor(previousContentWidth: Int, previousContentHeight: Int) {
            val anchor = zoomAnchor
            val scroll = SwingUtilities.getAncestorOfClass(JScrollPane::class.java, this) as? JScrollPane
            val viewport = scroll?.viewport
            if (anchor == null || viewport == null || viewport.width <= 0 || viewport.height <= 0) {
                centerAtMinZoom()
                return
            }
            if (zoomScale <= minZoomScale + 1.0e-6 && abs(targetZoomScale - zoomScale) <= 1.0e-4) {
                zoomAnchor = null
                centerAtMinZoom()
                return
            }

            val contentWidth = scaledContentWidth()
            val contentHeight = scaledContentHeight()
            val baseX = ((width - contentWidth) / 2).coerceAtLeast(padding)
            val baseY = ((height - contentHeight) / 2).coerceAtLeast(padding)
            val targetPanelX = baseX + (anchor.normX * contentWidth).roundToInt()
            val targetPanelY = baseY + (anchor.normY * contentHeight).roundToInt()
            val maxX = (width - viewport.width).coerceAtLeast(0)
            val maxY = (height - viewport.height).coerceAtLeast(0)
            val nextX = (targetPanelX - anchor.viewportX).coerceIn(0, maxX)
            val nextY = (targetPanelY - anchor.viewportY).coerceIn(0, maxY)
            val current = viewport.viewPosition
            if (current.x != nextX || current.y != nextY) {
                viewport.viewPosition = Point(nextX, nextY)
            }

            if (abs(targetZoomScale - zoomScale) <= 1.0e-4 || (previousContentWidth == contentWidth && previousContentHeight == contentHeight)) {
                zoomAnchor = null
                if (zoomScale <= minZoomScale + 1.0e-6) {
                    centerAtMinZoom()
                }
            }
        }

        private data class ZoomAnchor(
            val viewportX: Int,
            val viewportY: Int,
            val normX: Double,
            val normY: Double
        )

        override fun paintComponent(g: Graphics) {
            super.paintComponent(g)
            val g2 = g.create() as Graphics2D
            try {
                g2.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON)
                g2.setRenderingHint(RenderingHints.KEY_INTERPOLATION, RenderingHints.VALUE_INTERPOLATION_NEAREST_NEIGHBOR)
                g2.setRenderingHint(RenderingHints.KEY_RENDERING, RenderingHints.VALUE_RENDER_SPEED)
                g2.color = UIManager.getColor("Panel.background") ?: background
                g2.fillRect(0, 0, width, height)

                if (worlds.isEmpty()) {
                    g2.color = UIManager.getColor("Label.foreground") ?: foreground
                    g2.font = g2.font.deriveFont(Font.PLAIN, 13f)
                    g2.drawString(ServerI18n.tr("aerogel.dashboard.map.empty"), padding, padding + 18)
                    return
                }

                val contentWidth = scaledContentWidth()
                val contentHeight = scaledContentHeight()
                val baseX = ((width - contentWidth) / 2).coerceAtLeast(padding)
                val baseY = ((height - contentHeight) / 2).coerceAtLeast(padding)
                var y = baseY
                for (world in worlds) {
                    val widthInCells = world.maxChunkX - world.minChunkX + 1
                    val heightInCells = world.maxChunkZ - world.minChunkZ + 1
                    val gridWidth = (widthInCells * baseCellSize.toDouble() * zoomScale).roundToInt()
                    val gridHeight = (heightInCells * baseCellSize.toDouble() * zoomScale).roundToInt()
                    val title = "${world.world} (${world.minChunkX}..${world.maxChunkX}, ${world.minChunkZ}..${world.maxChunkZ})"

                    g2.color = UIManager.getColor("Label.foreground") ?: foreground
                    g2.font = g2.font.deriveFont(Font.BOLD, 12f)
                    g2.drawString(title, baseX, y + 14)

                    val gridTop = y + headerHeight
                    drawWorldGrid(g2, world, Rectangle(baseX, gridTop, gridWidth, gridHeight))
                    y = gridTop + gridHeight + worldGap
                }
            } finally {
                g2.dispose()
            }
        }

        private fun drawWorldGrid(g2: Graphics2D, world: ChunkMapWorld, grid: Rectangle) {
            val originalTransform = g2.transform
            val clip = g2.clipBounds ?: Rectangle(grid.x, grid.y, grid.width, grid.height)
            val widthInCells = (world.maxChunkX - world.minChunkX + 1).coerceAtLeast(1)
            val heightInCells = (world.maxChunkZ - world.minChunkZ + 1).coerceAtLeast(1)
            val localMinX = ((clip.x - grid.x).toDouble() / zoomScale).coerceAtLeast(0.0)
            val localMaxX = ((clip.x + clip.width - grid.x).toDouble() / zoomScale).coerceAtLeast(0.0)
            val localMinY = ((clip.y - grid.y).toDouble() / zoomScale).coerceAtLeast(0.0)
            val localMaxY = ((clip.y + clip.height - grid.y).toDouble() / zoomScale).coerceAtLeast(0.0)
            val colStart = floor(localMinX / baseCellSize.toDouble()).toInt().coerceIn(0, widthInCells - 1)
            val colEnd = ceil(localMaxX / baseCellSize.toDouble()).toInt().coerceIn(0, widthInCells - 1)
            val rowStart = floor(localMinY / baseCellSize.toDouble()).toInt().coerceIn(0, heightInCells - 1)
            val rowEnd = ceil(localMaxY / baseCellSize.toDouble()).toInt().coerceIn(0, heightInCells - 1)

            g2.translate(grid.x.toDouble(), grid.y.toDouble())
            g2.scale(zoomScale, zoomScale)
            val border = UIManager.getColor("Component.borderColor") ?: Color.GRAY
            for (row in rowStart..rowEnd) {
                val chunkZ = world.minChunkZ + row
                for (col in colStart..colEnd) {
                    val chunkX = world.minChunkX + col
                    val x = col * baseCellSize
                    val y = row * baseCellSize
                    val cell = world.cells[chunkX to chunkZ]
                    if (cell != null) {
                        val drawTexture = if (cell.idle) cell.idleTexture else cell.texture
                        g2.drawImage(drawTexture, x, y, baseCellSize, baseCellSize, null)
                    } else {
                        g2.color = Color(40, 45, 52)
                        g2.fillRect(x, y, baseCellSize, baseCellSize)
                    }
                    g2.color = border
                    g2.drawRect(x, y, baseCellSize, baseCellSize)
                }
            }
            g2.transform = originalTransform
        }

        private fun cellColor(mspt: Double): Color {
            val value = if (mspt.isFinite()) mspt.coerceAtLeast(0.0) else 0.0
            if (value <= 0.0001) {
                return Color(60, 68, 78)
            }

            val ratio = value / targetMsptUpperBound().coerceAtLeast(1.0e-9)
            return when {
                ratio < 0.5 -> {
                    val t = ratio / 0.5
                    blend(Color(47, 158, 68), Color(247, 197, 72), t)
                }
                ratio < 1.0 -> {
                    val t = (ratio - 0.5) / 0.5
                    blend(Color(247, 197, 72), Color(231, 76, 60), t)
                }
                else -> {
                    val t = ((ratio - 1.0) / (ratio + 1.0)).coerceIn(0.0, 1.0)
                    blend(Color(231, 76, 60), Color(128, 24, 24), t)
                }
            }
        }

        private fun colorForCellText(background: Color): Color {
            val luminance = (0.2126 * background.red + 0.7152 * background.green + 0.0722 * background.blue) / 255.0
            return if (luminance < 0.45) Color(244, 244, 244) else Color(24, 24, 24)
        }

        private fun blend(from: Color, to: Color, t: Double): Color {
            val ratio = t.coerceIn(0.0, 1.0)
            val red = (from.red + (to.red - from.red) * ratio).toInt().coerceIn(0, 255)
            val green = (from.green + (to.green - from.green) * ratio).toInt().coerceIn(0, 255)
            val blue = (from.blue + (to.blue - from.blue) * ratio).toInt().coerceIn(0, 255)
            return Color(red, green, blue)
        }
    }

    private class MetricGraphPanel(
        private val lineColor: Color,
        private val baselineMin: Double = 1.0,
        private val valueUnit: String,
        private val integerDisplay: Boolean = false
    ) : JPanel() {
        private val history = ArrayDeque<Double>(GRAPH_HISTORY)
        private var fixedUpperBound: Double? = null
        @Volatile private var windowMinutes: Int = 3
        @Volatile private var hoveredIndex: Int? = null

        init {
            preferredSize = Dimension(420, 160)
            minimumSize = Dimension(300, 130)
            border = BorderFactory.createEmptyBorder(4, 4, 4, 4)
            addMouseMotionListener(object : java.awt.event.MouseMotionAdapter() {
                override fun mouseMoved(e: java.awt.event.MouseEvent) {
                    hoveredIndex = hoverIndexForEvent(e)
                    repaint()
                }
            })
            addMouseListener(object : java.awt.event.MouseAdapter() {
                override fun mouseExited(e: java.awt.event.MouseEvent) {
                    hoveredIndex = null
                    repaint()
                }
            })
        }

        fun push(value: Double, upperBound: Double? = null) {
            val safe = if (value.isFinite()) value else 0.0
            if (history.size >= GRAPH_HISTORY) {
                history.removeFirst()
            }
            history.addLast(safe)
            fixedUpperBound = upperBound
            repaint()
        }

        fun setWindowMinutes(minutes: Int) {
            windowMinutes = minutes.coerceAtLeast(1)
            repaint()
        }

        private fun formatSecondsAgo(seconds: Double): String {
            val total = seconds.coerceAtLeast(0.0).toInt()
            val min = total / 60
            val sec = total % 60
            return String.format("%d:%02d", min, sec)
        }

        private fun windowedSamples(): List<Double> {
            val windowSamples = visibleWindowSamples(windowMinutes)
            val source = if (history.size <= windowSamples) {
                history.toList()
            } else {
                history.drop(history.size - windowSamples)
            }
            return if (source.size < windowSamples) {
                val padded = ArrayList<Double>(windowSamples)
                repeat(windowSamples - source.size) { padded.add(0.0) }
                padded.addAll(source)
                padded
            } else {
                source
            }
        }

        override fun paintComponent(g: Graphics) {
            super.paintComponent(g)
            val g2 = g.create() as Graphics2D
            try {
                g2.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON)

                val insets = insets
                val x0 = insets.left
                val y0 = insets.top
                val w = width - insets.left - insets.right
                val h = height - insets.top - insets.bottom
                if (w <= 10 || h <= 10) return

                g2.color = UIManager.getColor("Panel.background") ?: background
                g2.fillRoundRect(x0, y0, w, h, 12, 12)

                if (history.isEmpty()) return

                val chartX = x0 + 8
                val chartY = y0 + 4
                val chartW = w - 16
                val chartH = h - 8
                if (chartW < 10 || chartH < 10) return

                val upper = (fixedUpperBound ?: history.maxOrNull() ?: baselineMin).coerceAtLeast(baselineMin)
                val lower = 0.0
                val range = (upper - lower).coerceAtLeast(1e-6)

                g2.color = (UIManager.getColor("Component.borderColor") ?: Color.GRAY)
                g2.stroke = BasicStroke(1f)
                g2.drawRoundRect(chartX, chartY, chartW, chartH, 8, 8)

                val list = windowedSamples()
                val points = list.size
                if (points < 2) return

                val path = Path2D.Double()
                var lastX = 0.0
                for (i in 0 until points) {
                    val value = list[i].coerceIn(lower, upper)
                    val x = chartX + (i.toDouble() / (points - 1).toDouble()) * chartW
                    val y = chartY + chartH - ((value - lower) / range) * chartH
                    if (i == 0) {
                        path.moveTo(x, y)
                    } else {
                        path.lineTo(x, y)
                    }
                    lastX = x
                }

                val area = Path2D.Double(path)
                area.lineTo(lastX, chartY + chartH.toDouble())
                area.lineTo(chartX.toDouble(), chartY + chartH.toDouble())
                area.closePath()

                val fill = GradientPaint(
                    chartX.toFloat(),
                    chartY.toFloat(),
                    Color(lineColor.red, lineColor.green, lineColor.blue, 80),
                    chartX.toFloat(),
                    (chartY + chartH).toFloat(),
                    Color(lineColor.red, lineColor.green, lineColor.blue, 8)
                )
                g2.paint = fill
                g2.fill(area)

                g2.color = lineColor
                g2.stroke = BasicStroke(2f)
                g2.draw(path)

                val hovered = hoveredIndex
                if (hovered != null && hovered in 0 until points) {
                    val hv = list[hovered].coerceIn(lower, upper)
                    val hx = chartX + (hovered.toDouble() / (points - 1).toDouble()) * chartW
                    val hy = chartY + chartH - ((hv - lower) / range) * chartH
                    g2.color = Color(lineColor.red, lineColor.green, lineColor.blue, 90)
                    g2.stroke = BasicStroke(1f)
                    g2.drawLine(hx.toInt(), chartY, hx.toInt(), chartY + chartH)
                    g2.color = lineColor
                    g2.fillOval((hx - 4.5).toInt(), (hy - 4.5).toInt(), 9, 9)
                    g2.color = UIManager.getColor("Panel.background") ?: background
                    g2.stroke = BasicStroke(1.4f)
                    g2.drawOval((hx - 4.5).toInt(), (hy - 4.5).toInt(), 9, 9)

                    val detail = detailTextForIndex(hovered, list)
                    if (detail.isNotEmpty()) {
                        g2.font = g2.font.deriveFont(Font.PLAIN, 11f)
                        val fm = g2.fontMetrics
                        val padX = 8
                        val padY = 5
                        val boxW = fm.stringWidth(detail) + (padX * 2)
                        val boxH = fm.height + (padY * 2)
                        var boxX = hx.toInt() + 10
                        var boxY = hy.toInt() - boxH - 8
                        if (boxX + boxW > chartX + chartW) boxX = hx.toInt() - boxW - 10
                        if (boxX < chartX) boxX = chartX
                        if (boxY < chartY) boxY = hy.toInt() + 10
                        if (boxY + boxH > chartY + chartH) boxY = (chartY + chartH - boxH).coerceAtLeast(chartY)
                        g2.color = Color(24, 24, 24, 225)
                        g2.fillRoundRect(boxX, boxY, boxW, boxH, 10, 10)
                        g2.color = Color(245, 245, 245, 235)
                        g2.drawRoundRect(boxX, boxY, boxW, boxH, 10, 10)
                        g2.color = Color(255, 255, 255, 245)
                        g2.drawString(detail, boxX + padX, boxY + padY + fm.ascent)
                    }
                }

                g2.color = UIManager.getColor("Label.foreground") ?: foreground
                g2.font = g2.font.deriveFont(Font.PLAIN, 11f)
                val latest = list.last()
                g2.drawString(
                    ServerI18n.tr("aerogel.dashboard.graph.now", formatSummaryValue(latest)),
                    chartX + 6,
                    chartY + 14
                )
                g2.drawString(
                    ServerI18n.tr("aerogel.dashboard.graph.max", formatSummaryValue(upper)),
                    chartX + chartW - 85,
                    chartY + 14
                )
            } finally {
                g2.dispose()
            }
        }

        private fun detailTextForIndex(index: Int, list: List<Double>): String {
            val points = list.size
            if (points < 2 || index !in 0 until points) return ""
            val value = list[index]
            val targetTps = targetTpsUpperBound().coerceAtLeast(1.0)
            val secondsAgo = ((points - 1 - index).coerceAtLeast(0)) / targetTps
            return ServerI18n.tr(
                "aerogel.dashboard.graph.tooltip",
                formatTooltipValue(value),
                valueUnit,
                formatSecondsAgo(secondsAgo)
            )
        }

        private fun formatSummaryValue(value: Double): String {
            return if (integerDisplay) value.roundToInt().toString() else DECIMAL_2.format(value)
        }

        private fun formatTooltipValue(value: Double): String {
            return if (integerDisplay) value.roundToInt().toString() else DECIMAL_4.format(value)
        }

        private fun hoverIndexForEvent(event: java.awt.event.MouseEvent): Int? {
            if (history.isEmpty()) return null
            val insets = insets
            val x0 = insets.left
            val y0 = insets.top
            val w = width - insets.left - insets.right
            val h = height - insets.top - insets.bottom
            if (w <= 10 || h <= 10) return null
            val chartX = x0 + 8
            val chartY = y0 + 4
            val chartW = w - 16
            val chartH = h - 8
            if (event.x < chartX || event.x > chartX + chartW || event.y < chartY || event.y > chartY + chartH) {
                return null
            }
            val list = windowedSamples()
            val points = list.size
            if (points < 2) return null
            val clamped = (event.x - chartX).coerceIn(0, chartW)
            return ((clamped.toDouble() / chartW.toDouble()) * (points - 1))
                .toInt()
                .coerceIn(0, points - 1)
        }
    }

    private val DECIMAL_2 = DecimalFormat("0.00")
    private val DECIMAL_4 = DecimalFormat("0.0000")
    private const val CHUNK_TEXTURE_SIZE = 16
    private val UNKNOWN_TERRAIN_COLOR = Color(28, 31, 37)
}
