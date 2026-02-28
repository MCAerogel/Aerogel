package org.macaroon3145.network.handler

import io.netty.buffer.Unpooled
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelId
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.macaroon3145.config.ServerConfig
import org.macaroon3145.DebugConsole
import org.macaroon3145.ServerLifecycle
import org.macaroon3145.i18n.ServerI18n
import org.macaroon3145.network.command.CommandContext
import org.macaroon3145.network.command.CommandDispatcher
import org.macaroon3145.network.command.EntitySelectorCompletions
import org.macaroon3145.network.NetworkUtils
import org.macaroon3145.network.codec.BlockStateRegistry
import org.macaroon3145.network.codec.BlockEntityTypeRegistry
import org.macaroon3145.network.codec.ItemBlockStateRegistry
import org.macaroon3145.network.codec.RegistryCodec
import org.macaroon3145.network.packet.PlayerSample
import org.macaroon3145.world.BlockPos
import org.macaroon3145.world.ChunkPos
import org.macaroon3145.world.DroppedItemSnapshot
import org.macaroon3145.world.FallingBlockSnapshot
import org.macaroon3145.world.FoliaSidecarSpawnPointProvider
import org.macaroon3145.world.VanillaMiningRules
import org.macaroon3145.world.WorldManager
import org.macaroon3145.world.generators.FoliaSharedMemoryWorldGenerator
import java.util.ArrayList
import java.util.HashSet
import java.io.ByteArrayOutputStream
import java.nio.file.Files
import java.nio.file.Path
import java.util.Locale
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.LockSupport
import kotlin.math.abs
import kotlin.math.ceil
import kotlin.math.cos
import kotlin.math.sin
import kotlin.math.roundToLong
import org.slf4j.LoggerFactory

data class DroppedItemTrackerState(
    var encodedX4096: Long,
    var encodedY4096: Long,
    var encodedZ4096: Long,
    var lastVx: Double,
    var lastVy: Double,
    var lastVz: Double,
    var lastOnGround: Boolean,
    var secondsSinceHardSync: Double,
    var relativeMoveAccumulatorSeconds: Double
)

data class PlayerSession(
    val channelId: ChannelId,
    val profile: ConnectionProfile,
    @Volatile var locale: String,
    val worldKey: String,
    val entityId: Int,
    val skinPartsMask: Int,
    @Volatile var pingMs: Int,
    @Volatile var x: Double,
    @Volatile var y: Double,
    @Volatile var z: Double,
    @Volatile var velocityX: Double,
    @Volatile var velocityY: Double,
    @Volatile var velocityZ: Double,
    @Volatile var lastHorizontalMovementSq: Double,
    @Volatile var encodedX4096: Long,
    @Volatile var encodedY4096: Long,
    @Volatile var encodedZ4096: Long,
    @Volatile var yaw: Float,
    @Volatile var pitch: Float,
    @Volatile var onGround: Boolean,
    @Volatile var health: Float,
    @Volatile var food: Int,
    @Volatile var saturation: Float,
    @Volatile var fallDistance: Double,
    @Volatile var dead: Boolean,
    @Volatile var sneaking: Boolean,
    @Volatile var sprinting: Boolean,
    @Volatile var swimming: Boolean,
    @Volatile var selectedBlockStateId: Int,
    @Volatile var selectedHotbarSlot: Int,
    @Volatile var pendingPickedBlockStateId: Int,
    val hotbarItemIds: IntArray,
    val hotbarItemCounts: IntArray,
    val mainInventoryItemIds: IntArray,
    val mainInventoryItemCounts: IntArray,
    @Volatile var offhandItemId: Int,
    @Volatile var offhandItemCount: Int,
    val armorItemIds: IntArray,
    val armorItemCounts: IntArray,
    val hotbarBlockStateIds: IntArray,
    val hotbarBlockEntityTypeIds: IntArray,
    val hotbarBlockEntityNbtPayloads: Array<ByteArray?>,
    @Volatile var inventoryStateId: Int,
    @Volatile var gameMode: Int,
    @Volatile var requestedViewDistance: Int,
    @Volatile var chunkRadius: Int,
    @Volatile var centerChunkX: Int,
    @Volatile var centerChunkZ: Int,
    val targetChunks: MutableSet<ChunkPos>,
    val loadedChunks: MutableSet<ChunkPos>,
    val visibleDroppedItemEntityIds: MutableSet<Int>,
    val droppedItemTrackerStates: MutableMap<Int, DroppedItemTrackerState>,
    val visibleFallingBlockEntityIds: MutableSet<Int>,
    val fallingBlockTrackerStates: MutableMap<Int, DroppedItemTrackerState>,
    val generatingChunks: MutableSet<ChunkPos>,
    val chunkStreamVersion: AtomicInteger,
    val nextTeleportId: AtomicInteger,
    val chunkStreamInFlight: AtomicBoolean,
    val pendingChunkTarget: AtomicReference<ChunkStreamTarget?>,
    @Volatile var lastChunkMsptActionBarAtNanos: Long
)

data class ChunkStreamTarget(
    val streamId: Int,
    val centerChunkX: Int,
    val centerChunkZ: Int,
    val radius: Int
)

private data class WorldTimeState(
    var worldAgeTicks: Double,
    var timeOfDayTicks: Double,
    var broadcastAccumulatorSeconds: Double
)

data class CommandSuggestionWindow(
    val start: Int,
    val length: Int,
    val suggestions: List<String>
)

object PlayerSessionManager {
    private val logger = LoggerFactory.getLogger(PlayerSessionManager::class.java)
    private val gamemodeCompletionCandidates = listOf("survival", "creative", "adventure", "spectator")
    private val nextEntityId = AtomicInteger(1)
    private val sessions = ConcurrentHashMap<ChannelId, PlayerSession>()
    private val contexts = ConcurrentHashMap<ChannelId, ChannelHandlerContext>()
    private val worldTimes = ConcurrentHashMap<String, WorldTimeState>()
    private val droppedItemDispatchRunning = AtomicBoolean(false)
    private val commandExecutor = Executors.newFixedThreadPool(2) { runnable ->
        Thread(runnable, "aerogel-command-worker").apply {
            isDaemon = true
            priority = Thread.NORM_PRIORITY - 1
        }
    }
    private val operatorFilePath: Path = Path.of("op.txt")
    private val operatorFileLock = Any()
    private val persistedOperatorUuids = ConcurrentHashMap.newKeySet<UUID>()
    private val operatorUuids = ConcurrentHashMap.newKeySet<java.util.UUID>()
    private val commandDispatcher = CommandDispatcher.default()
    private val commandContext = object : CommandContext {
        override fun sendConsoleMessage(message: String) {
            ServerI18n.log("aerogel.log.console.raw", message)
        }
        override fun sendMessage(target: PlayerSession, message: String) = PlayerSessionManager.sendSystemMessage(target, message)
        override fun sendTranslation(target: PlayerSession, key: String, vararg args: PlayPackets.ChatComponent) =
            PlayerSessionManager.sendSystemTranslation(target, key, *args)
        override fun sendErrorTranslation(target: PlayerSession, key: String, vararg args: PlayPackets.ChatComponent) =
            PlayerSessionManager.sendSystemTranslation(target, key, color = "red", *args)
        override fun sendUnknownCommandWithContext(target: PlayerSession?, input: String) =
            PlayerSessionManager.sendUnknownCommandWithContext(target, input)
        override fun sendSourceTranslationWithContext(
            target: PlayerSession?,
            key: String,
            input: String,
            errorStart: Int,
            vararg args: PlayPackets.ChatComponent
        ) = PlayerSessionManager.sendSourceTranslationWithContext(target, key, input, errorStart, *args)
        override fun sendSourceTranslation(target: PlayerSession?, key: String, vararg args: PlayPackets.ChatComponent) {
            if (target == null) {
                val renderedArgs = args.map { ServerI18n.componentToText(it) }
                ServerI18n.log("aerogel.log.console.prefixed", ServerI18n.translate(key, renderedArgs))
            } else {
                PlayerSessionManager.sendSystemTranslation(target, key, *args)
            }
        }
        override fun sendSourceErrorTranslation(target: PlayerSession?, key: String, vararg args: PlayPackets.ChatComponent) {
            if (target == null) {
                val renderedArgs = args.map { ServerI18n.componentToText(it) }
                ServerI18n.log("aerogel.log.console.prefixed_error", ServerI18n.translate(key, renderedArgs))
            } else {
                PlayerSessionManager.sendSystemTranslation(target, key, color = "red", *args)
            }
        }
        override fun sendSourceWarnTranslation(target: PlayerSession?, key: String, vararg args: PlayPackets.ChatComponent) {
            if (target == null) {
                val renderedArgs = args.map { ServerI18n.componentToText(it) }
                ServerI18n.log("aerogel.log.console.prefixed_warn", ServerI18n.translate(key, renderedArgs))
            } else {
                PlayerSessionManager.sendSystemTranslation(target, key, color = "yellow", *args)
            }
        }
        override fun sendTranslationFromTerminal(target: PlayerSession, key: String, vararg args: PlayPackets.ChatComponent) {
            PlayerSessionManager.sendTerminalSourcedTranslation(target, key, *args)
        }
        override fun sendAdminTranslation(
            target: PlayerSession,
            source: PlayPackets.ChatComponent,
            key: String,
            vararg args: PlayPackets.ChatComponent
        ) {
            PlayerSessionManager.sendAdminSourcedTranslation(target, source, key, *args)
        }
        override fun sourceWorldKey(target: PlayerSession?): String = target?.worldKey ?: (WorldManager.defaultWorld().key)
        override fun sourceX(target: PlayerSession?): Double = target?.x ?: 0.0
        override fun sourceY(target: PlayerSession?): Double = target?.y ?: 64.0
        override fun sourceZ(target: PlayerSession?): Double = target?.z ?: 0.0
        override fun onlinePlayers(): List<PlayerSession> = sessions.values.toList()
        override fun onlinePlayersInWorld(worldKey: String): List<PlayerSession> = PlayerSessionManager.onlinePlayersInWorld(worldKey)
        override fun findOnlinePlayer(name: String): PlayerSession? = PlayerSessionManager.findOnlinePlayer(name)
        override fun findOnlinePlayer(uuid: UUID): PlayerSession? = PlayerSessionManager.findOnlinePlayer(uuid)
        override fun isOperator(session: PlayerSession?): Boolean = PlayerSessionManager.isOperator(session)
        override fun canUseOpCommand(sender: PlayerSession?): Boolean = PlayerSessionManager.canRunOpCommand(sender)
        override fun grantOperator(target: PlayerSession): Boolean = PlayerSessionManager.grantOperatorAndRefreshCommands(target)
        override fun revokeOperator(uuid: UUID): Boolean = PlayerSessionManager.revokeOperatorAndRefreshCommands(uuid)
        override fun playerNameComponent(target: PlayerSession): PlayPackets.ChatComponent = playerNameComponent(target.profile)
        override fun teleport(target: PlayerSession, x: Double, y: Double, z: Double, yaw: Float?, pitch: Float?) =
            PlayerSessionManager.teleportPlayer(target, x, y, z, yaw, pitch)
        override fun setGamemode(target: PlayerSession, mode: Int) = PlayerSessionManager.setPlayerGamemode(target, mode)
        override fun worldKeys(): List<String> = WorldManager.allWorlds().map { it.key }.sortedWith(String.CASE_INSENSITIVE_ORDER)
        override fun worldTimeSnapshot(worldKey: String): Pair<Long, Long>? = PlayerSessionManager.worldTimeSnapshotOrNull(worldKey)
        override fun setWorldTime(worldKey: String, timeOfDayTicks: Long): Pair<Long, Long>? =
            PlayerSessionManager.setWorldTimeAndBroadcast(worldKey, timeOfDayTicks)
        override fun addWorldTime(worldKey: String, deltaTicks: Long): Pair<Long, Long>? =
            PlayerSessionManager.addWorldTimeAndBroadcast(worldKey, deltaTicks)
        override fun stopServer(): Boolean = ServerLifecycle.stopServer()
    }
    @Volatile
    private var droppedItemDispatchThread: Thread? = null
    private val itemKeyById: List<String> by lazy {
        val resource = PlayerSessionManager::class.java.classLoader
            .getResourceAsStream("item-id-map-1.21.11.json")
            ?: return@lazy emptyList()
        val root = resource.bufferedReader().use { json.parseToJsonElement(it.readText()).jsonObject }
        val entries = root["entries"]?.jsonObject ?: return@lazy emptyList()
        val maxId = entries.values.maxOfOrNull { it.jsonPrimitive.intOrNull ?: -1 } ?: -1
        if (maxId < 0) return@lazy emptyList()
        val out = MutableList(maxId + 1) { "" }
        for ((itemKey, idElement) in entries) {
            val itemId = idElement.jsonPrimitive.intOrNull ?: continue
            if (itemId !in out.indices) continue
            out[itemId] = itemKey
        }
        out
    }
    private val creativeNoBreakItemIds: Set<Int> by lazy {
        val itemRegistry = RegistryCodec.allRegistries().firstOrNull { it.id == "minecraft:item" } ?: return@lazy emptySet()
        itemRegistry.entries.withIndex()
            .filter { (_, entry) -> entry.key.endsWith("_sword") }
            .mapTo(HashSet()) { it.index }
    }

    private data class PickedBlockData(
        val stateId: Int,
        val itemId: Int,
        val blockEntityTypeId: Int,
        val blockEntityNbtPayload: ByteArray?
    )

    init {
        loadPersistedOperators()
    }

    data class JoinResult(
        val session: PlayerSession,
        val existing: List<PlayerSession>
    )

    private data class DroppedItemPickupCandidate(
        val snapshot: DroppedItemSnapshot,
        val collector: PlayerSession,
        val collectorOrder: Int,
        val distanceSq: Double
    )

    private data class DroppedItemPickupResult(
        val entityId: Int,
        val collectorEntityId: Int,
        val pickupItemCount: Int,
        val x: Double,
        val y: Double,
        val z: Double
    )

    private fun allocateEntityId(): Int = nextEntityId.getAndIncrement()

    fun prepareJoin(
        ctx: ChannelHandlerContext,
        profile: ConnectionProfile,
        locale: String,
        worldKey: String,
        skinPartsMask: Int,
        requestedViewDistance: Int,
        spawnX: Double,
        spawnY: Double,
        spawnZ: Double
    ): JoinResult {
        val spawnChunkX = ChunkStreamingService.chunkXFromBlockX(spawnX)
        val spawnChunkZ = ChunkStreamingService.chunkZFromBlockZ(spawnZ)
        val effectiveRadius = effectiveChunkRadius(requestedViewDistance)
        val session = PlayerSession(
            channelId = ctx.channel().id(),
            profile = profile,
            locale = locale,
            worldKey = worldKey,
            entityId = allocateEntityId(),
            skinPartsMask = skinPartsMask,
            pingMs = 0,
            x = spawnX,
            y = spawnY,
            z = spawnZ,
            velocityX = 0.0,
            velocityY = 0.0,
            velocityZ = 0.0,
            lastHorizontalMovementSq = 0.0,
            encodedX4096 = encodeRelativePosition4096(spawnX),
            encodedY4096 = encodeRelativePosition4096(spawnY),
            encodedZ4096 = encodeRelativePosition4096(spawnZ),
            yaw = 0f,
            pitch = 0f,
            onGround = true,
            health = MAX_PLAYER_HEALTH,
            food = DEFAULT_PLAYER_FOOD,
            saturation = DEFAULT_PLAYER_SATURATION,
            fallDistance = 0.0,
            dead = false,
            sneaking = false,
            sprinting = false,
            swimming = false,
            selectedBlockStateId = 0,
            selectedHotbarSlot = 0,
            pendingPickedBlockStateId = 0,
            hotbarItemIds = IntArray(9) { -1 },
            hotbarItemCounts = IntArray(9) { 0 },
            mainInventoryItemIds = IntArray(27) { -1 },
            mainInventoryItemCounts = IntArray(27) { 0 },
            offhandItemId = -1,
            offhandItemCount = 0,
            armorItemIds = IntArray(4) { -1 },
            armorItemCounts = IntArray(4) { 0 },
            hotbarBlockStateIds = IntArray(9) { 0 },
            hotbarBlockEntityTypeIds = IntArray(9) { -1 },
            hotbarBlockEntityNbtPayloads = arrayOfNulls(9),
            inventoryStateId = 0,
            gameMode = ServerConfig.defaultGameMode.coerceIn(0, 3),
            requestedViewDistance = requestedViewDistance.coerceAtLeast(0),
            chunkRadius = effectiveRadius,
            centerChunkX = spawnChunkX,
            centerChunkZ = spawnChunkZ,
            targetChunks = ConcurrentHashMap.newKeySet(),
            loadedChunks = ConcurrentHashMap.newKeySet(),
            visibleDroppedItemEntityIds = ConcurrentHashMap.newKeySet(),
            droppedItemTrackerStates = ConcurrentHashMap(),
            visibleFallingBlockEntityIds = ConcurrentHashMap.newKeySet(),
            fallingBlockTrackerStates = ConcurrentHashMap(),
            generatingChunks = ConcurrentHashMap.newKeySet(),
            chunkStreamVersion = AtomicInteger(0),
            nextTeleportId = AtomicInteger(2),
            chunkStreamInFlight = AtomicBoolean(false),
            pendingChunkTarget = AtomicReference(null),
            lastChunkMsptActionBarAtNanos = 0L
        )
        if (persistedOperatorUuids.contains(session.profile.uuid)) {
            operatorUuids.add(profile.uuid)
        }

        val existing = sessions.values.filter { it.worldKey == worldKey && it.channelId != session.channelId }
        sessions[session.channelId] = session
        contexts[session.channelId] = ctx
        ensureDroppedItemDispatchLoop()
        return JoinResult(session, existing)
    }

    fun finishJoin(join: JoinResult) {
        val session = join.session
        val existing = join.existing
        // Send already-connected players to newcomer.
        for (other in existing) {
            val ctx = contexts[session.channelId] ?: continue
            ctx.write(PlayPackets.playerInfoPacket(other.profile, other.gameMode, other.pingMs))
            ctx.write(
                PlayPackets.addPlayerEntityPacket(
                    entityId = other.entityId,
                    uuid = other.profile.uuid,
                    x = other.x,
                    y = other.y,
                    z = other.z,
                    yaw = other.yaw,
                    pitch = other.pitch
                )
            )
            ctx.write(PlayPackets.playerSkinPartsMetadataPacket(other.entityId, other.skinPartsMask))
            ctx.write(
                PlayPackets.playerSharedFlagsMetadataPacket(
                    other.entityId,
                    other.sneaking,
                    other.sprinting,
                    other.swimming
                )
            )
            sendHeldItemEquipment(other, ctx, flush = false)
        }
        contexts[session.channelId]?.flush()

        // Broadcast newcomer to already-connected players.
        for (other in existing) {
            val otherCtx = contexts[other.channelId] ?: continue
            if (!otherCtx.channel().isActive) continue
            otherCtx.write(PlayPackets.playerInfoPacket(session.profile, session.gameMode, session.pingMs))
            otherCtx.write(
                PlayPackets.addPlayerEntityPacket(
                    entityId = session.entityId,
                    uuid = session.profile.uuid,
                    x = session.x,
                    y = session.y,
                    z = session.z,
                    yaw = session.yaw,
                    pitch = session.pitch
                )
            )
            otherCtx.write(PlayPackets.playerSkinPartsMetadataPacket(session.entityId, session.skinPartsMask))
            otherCtx.write(
                PlayPackets.playerSharedFlagsMetadataPacket(
                    session.entityId,
                    session.sneaking,
                    session.sprinting,
                    session.swimming
                )
            )
            sendHeldItemEquipment(session, otherCtx, flush = false)
            otherCtx.flush()
        }

        broadcastSystemMessage(
            session.worldKey,
            PlayPackets.ChatComponent.Translate(
                key = "multiplayer.player.joined",
                args = listOf(playerNameComponent(session.profile)),
                color = "yellow"
            )
        )
        ServerI18n.log("aerogel.log.info.player.joined", session.profile.username)
    }

    fun byChannel(channelId: ChannelId): PlayerSession? = sessions[channelId]

    fun onlineCount(): Int = sessions.size

    fun statusPlayerSamples(limit: Int = 12): List<PlayerSample> {
        if (limit <= 0) return emptyList()
        return sessions.values
            .asSequence()
            .map { PlayerSample(name = it.profile.username, id = it.profile.uuid.toString()) }
            .sortedBy { it.name.lowercase() }
            .take(limit)
            .toList()
    }

    fun tick(deltaSeconds: Double) {
        if (deltaSeconds <= 0.0) return
        advanceAndBroadcastWorldTime(deltaSeconds)
    }

    fun worldTimeSnapshot(worldKey: String): Pair<Long, Long> {
        val state = worldTimes.computeIfAbsent(worldKey) {
            WorldTimeState(
                worldAgeTicks = INITIAL_WORLD_TIME_TICKS,
                timeOfDayTicks = INITIAL_WORLD_TIME_TICKS,
                broadcastAccumulatorSeconds = 0.0
            )
        }
        synchronized(state) {
            return state.worldAgeTicks.toLong() to state.timeOfDayTicks.toLong()
        }
    }

    private fun worldTimeSnapshotOrNull(worldKey: String): Pair<Long, Long>? {
        if (WorldManager.world(worldKey) == null) return null
        val state = worldTimes.computeIfAbsent(worldKey) {
            WorldTimeState(
                worldAgeTicks = INITIAL_WORLD_TIME_TICKS,
                timeOfDayTicks = INITIAL_WORLD_TIME_TICKS,
                broadcastAccumulatorSeconds = 0.0
            )
        }
        synchronized(state) {
            return state.worldAgeTicks.toLong() to state.timeOfDayTicks.toLong()
        }
    }

    private fun setWorldTimeAndBroadcast(worldKey: String, timeOfDayTicks: Long): Pair<Long, Long>? {
        if (WorldManager.world(worldKey) == null) return null
        val state = worldTimes.computeIfAbsent(worldKey) {
            WorldTimeState(
                worldAgeTicks = INITIAL_WORLD_TIME_TICKS,
                timeOfDayTicks = INITIAL_WORLD_TIME_TICKS,
                broadcastAccumulatorSeconds = 0.0
            )
        }
        val worldAge: Long
        val timeOfDay: Long
        synchronized(state) {
            state.timeOfDayTicks = normalizeTimeOfDayTicks(timeOfDayTicks.toDouble())
            state.broadcastAccumulatorSeconds = 0.0
            worldAge = state.worldAgeTicks.toLong()
            timeOfDay = state.timeOfDayTicks.toLong()
        }
        broadcastWorldTimePacket(worldKey, worldAge, timeOfDay)
        return worldAge to timeOfDay
    }

    private fun addWorldTimeAndBroadcast(worldKey: String, deltaTicks: Long): Pair<Long, Long>? {
        if (WorldManager.world(worldKey) == null) return null
        val state = worldTimes.computeIfAbsent(worldKey) {
            WorldTimeState(
                worldAgeTicks = INITIAL_WORLD_TIME_TICKS,
                timeOfDayTicks = INITIAL_WORLD_TIME_TICKS,
                broadcastAccumulatorSeconds = 0.0
            )
        }
        val worldAge: Long
        val timeOfDay: Long
        synchronized(state) {
            state.worldAgeTicks = (state.worldAgeTicks + deltaTicks).coerceAtLeast(0.0)
            state.timeOfDayTicks = normalizeTimeOfDayTicks(state.timeOfDayTicks + deltaTicks)
            state.broadcastAccumulatorSeconds = 0.0
            worldAge = state.worldAgeTicks.toLong()
            timeOfDay = state.timeOfDayTicks.toLong()
        }
        broadcastWorldTimePacket(worldKey, worldAge, timeOfDay)
        return worldAge to timeOfDay
    }

    private fun normalizeTimeOfDayTicks(rawTicks: Double): Double {
        var normalized = rawTicks % MINECRAFT_DAY_TICKS
        if (normalized < 0.0) {
            normalized += MINECRAFT_DAY_TICKS
        }
        return normalized
    }

    private fun broadcastWorldTimePacket(worldKey: String, worldAge: Long, timeOfDay: Long) {
        val packet = PlayPackets.timeUpdatePacket(worldAge = worldAge, timeOfDay = timeOfDay, tickDayTime = true)
        for ((id, session) in sessions) {
            if (session.worldKey != worldKey) continue
            val ctx = contexts[id] ?: continue
            if (!ctx.channel().isActive) continue
            ctx.writeAndFlush(packet)
        }
    }

    private fun drainDroppedItemEvents(deltaSeconds: Double) {
        if (deltaSeconds <= 0.0) return
        val timeScale = ServerConfig.timeScale
        if (timeScale <= 0.0) return
        val physicsDeltaSeconds = (deltaSeconds * timeScale)
            .coerceAtMost(MAX_DROPPED_ITEM_DELTA_SECONDS)
        if (physicsDeltaSeconds <= 0.0) return
        if (sessions.isEmpty()) return
        val worlds = WorldManager.allWorlds()
        if (worlds.isEmpty()) return

        val sessionsByWorld = HashMap<String, MutableList<PlayerSession>>()
        for (session in sessions.values) {
            sessionsByWorld
                .computeIfAbsent(session.worldKey) { ArrayList() }
                .add(session)
        }
        updateRetainedBaseWorldChunks(sessionsByWorld)

        data class WorldPhysicsEvents(
            val world: org.macaroon3145.world.World,
            val droppedItems: org.macaroon3145.world.DroppedItemTickEvents,
            val fallingBlocks: org.macaroon3145.world.FallingBlockTickEvents
        )
        val ready = ArrayList<WorldPhysicsEvents>()
        for (world in worlds) {
            val worldSessions = sessionsByWorld[world.key]
            if (worldSessions.isNullOrEmpty()) continue
            val activeSimulationChunks = collectActiveSimulationChunks(worldSessions)
            if (activeSimulationChunks.isEmpty()) continue
            val chunkProcessingFrame = world.beginChunkProcessingFrame()
            val droppedEvents = if (world.hasDroppedItems()) {
                world.tickDroppedItems(
                    physicsDeltaSeconds,
                    activeSimulationChunks
                ) { chunkPos, elapsedNanos ->
                    world.recordChunkProcessingNanos(chunkProcessingFrame, chunkPos, elapsedNanos)
                }
            } else {
                org.macaroon3145.world.DroppedItemTickEvents(emptyList(), emptyList(), emptyList())
            }
            val fallingEvents = if (world.hasFallingBlocks()) {
                world.tickFallingBlocks(
                    physicsDeltaSeconds,
                    activeSimulationChunks
                ) { chunkPos, elapsedNanos ->
                    world.recordChunkProcessingNanos(chunkProcessingFrame, chunkPos, elapsedNanos)
                }
            } else {
                org.macaroon3145.world.FallingBlockTickEvents(emptyList(), emptyList(), emptyList(), emptyList())
            }
            world.finishChunkProcessingFrame(chunkProcessingFrame, activeSimulationChunks)
            ready.add(WorldPhysicsEvents(world, droppedEvents, fallingEvents))
        }
        for (physics in ready) {
            val world = physics.world
            val droppedEvents = physics.droppedItems
            val fallingEvents = physics.fallingBlocks
            val worldSessions = sessionsByWorld[world.key] ?: continue
            if (worldSessions.isEmpty()) continue

            val outboundByContext = HashMap<ChannelHandlerContext, MutableList<ByteArray>>()
            val pickedRemovals = collectDroppedItemPickups(world, worldSessions, outboundByContext)

            for (removed in droppedEvents.removed) {
                val packet = PlayPackets.removeEntitiesPacket(intArrayOf(removed.entityId))
                for (session in worldSessions) {
                    if (!session.visibleDroppedItemEntityIds.remove(removed.entityId)) continue
                    session.droppedItemTrackerStates.remove(removed.entityId)
                    val ctx = contexts[session.channelId] ?: continue
                    if (!ctx.channel().isActive) continue
                    enqueueDroppedItemPacket(outboundByContext, ctx, packet)
                }
            }
            for (pickup in pickedRemovals) {
                val takePacket = PlayPackets.takeItemEntityPacket(
                    collectedEntityId = pickup.entityId,
                    collectorEntityId = pickup.collectorEntityId,
                    pickupItemCount = pickup.pickupItemCount
                )
                val removePacket = PlayPackets.removeEntitiesPacket(intArrayOf(pickup.entityId))
                for (session in worldSessions) {
                    val isCollector = session.entityId == pickup.collectorEntityId
                    val wasVisible = session.visibleDroppedItemEntityIds.remove(pickup.entityId)
                    if (!isCollector && !wasVisible) continue
                    if (!isCollector && !isWithinPickupBroadcastRange(session, pickup)) continue
                    session.droppedItemTrackerStates.remove(pickup.entityId)
                    val ctx = contexts[session.channelId] ?: continue
                    if (!ctx.channel().isActive) continue
                    enqueueDroppedItemPacket(outboundByContext, ctx, takePacket)
                    enqueueDroppedItemPacket(outboundByContext, ctx, removePacket)
                }
            }

            for (snapshot in droppedEvents.spawned) {
                val current = world.droppedItemSnapshot(snapshot.entityId) ?: continue
                syncDroppedItemSnapshot(
                    snapshot = current,
                    worldSessions = worldSessions,
                    outboundByContext = outboundByContext,
                    sendPositionUpdate = false,
                    deltaSeconds = deltaSeconds
                )
            }
            for (snapshot in droppedEvents.updated) {
                val current = world.droppedItemSnapshot(snapshot.entityId) ?: continue
                syncDroppedItemSnapshot(
                    snapshot = current,
                    worldSessions = worldSessions,
                    outboundByContext = outboundByContext,
                    sendPositionUpdate = true,
                    deltaSeconds = deltaSeconds
                )
            }
            for (removed in fallingEvents.removed) {
                val packet = PlayPackets.removeEntitiesPacket(intArrayOf(removed.entityId))
                for (session in worldSessions) {
                    if (!session.visibleFallingBlockEntityIds.remove(removed.entityId)) continue
                    session.fallingBlockTrackerStates.remove(removed.entityId)
                    val ctx = contexts[session.channelId] ?: continue
                    if (!ctx.channel().isActive) continue
                    enqueueDroppedItemPacket(outboundByContext, ctx, packet)
                }
            }
            for (landed in fallingEvents.landed) {
                val packet = PlayPackets.blockChangePacket(landed.blockX, landed.blockY, landed.blockZ, landed.blockStateId)
                for (session in worldSessions) {
                    if (!session.loadedChunks.contains(landed.chunkPos)) continue
                    val ctx = contexts[session.channelId] ?: continue
                    if (!ctx.channel().isActive) continue
                    enqueueDroppedItemPacket(outboundByContext, ctx, packet)
                }
            }
            for (snapshot in fallingEvents.spawned) {
                val current = world.fallingBlockSnapshot(snapshot.entityId) ?: continue
                syncFallingBlockSnapshot(
                    snapshot = current,
                    worldSessions = worldSessions,
                    outboundByContext = outboundByContext,
                    sendPositionUpdate = false,
                    deltaSeconds = deltaSeconds
                )
            }
            for (snapshot in fallingEvents.updated) {
                val current = world.fallingBlockSnapshot(snapshot.entityId) ?: continue
                syncFallingBlockSnapshot(
                    snapshot = current,
                    worldSessions = worldSessions,
                    outboundByContext = outboundByContext,
                    sendPositionUpdate = true,
                    deltaSeconds = deltaSeconds
                )
            }

            for ((ctx, packets) in outboundByContext) {
                if (packets.isEmpty()) continue
                val packetBatch = packets.toTypedArray()
                ctx.executor().execute {
                    if (!ctx.channel().isActive) return@execute
                    for (packet in packetBatch) {
                        ctx.write(packet)
                    }
                    ctx.flush()
                }
            }
        }
    }

    private fun updateRetainedBaseWorldChunks(sessionsByWorld: Map<String, List<PlayerSession>>) {
        for ((worldKey, worldSessions) in sessionsByWorld) {
            val retained = HashSet<ChunkPos>()
            for (session in worldSessions) {
                retained.addAll(session.targetChunks)
                retained.addAll(session.loadedChunks)
            }
            FoliaSharedMemoryWorldGenerator.retainLoadedChunks(worldKey, retained)
        }
    }

    private fun updateRetainedBaseWorldChunksForWorld(worldKey: String) {
        val retained = HashSet<ChunkPos>()
        for (session in sessions.values) {
            if (session.worldKey != worldKey) continue
            retained.addAll(session.targetChunks)
            retained.addAll(session.loadedChunks)
        }
        FoliaSharedMemoryWorldGenerator.retainLoadedChunks(worldKey, retained)
    }

    private fun collectActiveSimulationChunks(worldSessions: List<PlayerSession>): Set<ChunkPos> {
        val maxSimulation = ServerConfig.maxSimulationDistanceChunks.coerceIn(2, 32)
        val active = HashSet<ChunkPos>(worldSessions.size * 128)
        for (session in worldSessions) {
            val simulationRadius = session.chunkRadius.coerceAtMost(maxSimulation)
            val radiusSq = simulationRadius * simulationRadius
            for (dz in -simulationRadius..simulationRadius) {
                for (dx in -simulationRadius..simulationRadius) {
                    if (dx * dx + dz * dz <= radiusSq) {
                        active.add(ChunkPos(session.centerChunkX + dx, session.centerChunkZ + dz))
                    }
                }
            }
        }
        return active
    }

    private fun ensureDroppedItemDispatchLoop() {
        if (droppedItemDispatchRunning.get()) return
        if (!droppedItemDispatchRunning.compareAndSet(false, true)) return
        droppedItemDispatchThread = Thread(::runDroppedItemDispatchLoop, "aerogel-dropped-item-dispatch").apply {
            isDaemon = true
            start()
        }
    }

    private fun runDroppedItemDispatchLoop() {
        var lastConfiguredMaxTps = Double.NaN
        var tickIntervalNanos = 50_000_000L
        var uncapped = false
        var nextDeadline = System.nanoTime()
        var lastTickNanos = System.nanoTime()
        while (droppedItemDispatchRunning.get()) {
            val configuredMaxTps = ServerConfig.maxTps
            if (configuredMaxTps != lastConfiguredMaxTps) {
                lastConfiguredMaxTps = configuredMaxTps
                if (configuredMaxTps == -1.0) {
                    uncapped = true
                } else {
                    uncapped = false
                    tickIntervalNanos = (1_000_000_000.0 / configuredMaxTps)
                        .roundToLong()
                        .coerceAtLeast(1L)
                    nextDeadline = System.nanoTime() + tickIntervalNanos
                }
            }

            if (!uncapped) {
                var now = System.nanoTime()
                var remaining = nextDeadline - now
                if (remaining > DROPPED_ITEM_COARSE_PARK_THRESHOLD_NANOS) {
                    val parkNanos = (remaining - DROPPED_ITEM_COARSE_PARK_GUARD_NANOS).coerceAtLeast(1L)
                    LockSupport.parkNanos(parkNanos)
                    now = System.nanoTime()
                    remaining = nextDeadline - now
                }
                while (remaining > 0L) {
                    Thread.onSpinWait()
                    now = System.nanoTime()
                    remaining = nextDeadline - now
                }
            }

            val now = System.nanoTime()
            val deltaSeconds = ((now - lastTickNanos).coerceAtLeast(0L) / 1_000_000_000.0)
                .coerceAtMost(MAX_DROPPED_ITEM_DELTA_SECONDS)
            lastTickNanos = now
            try {
                // Keep physics packet cadence stable: avoid bursty back-to-back substep dispatch
                // that can amplify client-side arrival jitter under catch-up.
                drainDroppedItemEvents(deltaSeconds)
                maybeSendChunkMsptActionBar(now)
            } catch (_: Throwable) {
                // Keep dispatcher alive even if one cycle fails.
            }
            if (!uncapped) {
                nextDeadline += tickIntervalNanos
                val lateBy = System.nanoTime() - nextDeadline
                if (lateBy > tickIntervalNanos * MAX_DROPPED_ITEM_LAG_TICKS_BEFORE_RESYNC) {
                    nextDeadline = System.nanoTime() + tickIntervalNanos
                }
            }
        }
    }

    private fun advanceAndBroadcastWorldTime(deltaSeconds: Double) {
        val worlds = WorldManager.allWorlds()
        if (worlds.isEmpty()) return
        val tickDelta = deltaSeconds * MINECRAFT_TICKS_PER_SECOND
        for (world in worlds) {
            val state = worldTimes.computeIfAbsent(world.key) {
                WorldTimeState(
                    worldAgeTicks = INITIAL_WORLD_TIME_TICKS,
                    timeOfDayTicks = INITIAL_WORLD_TIME_TICKS,
                    broadcastAccumulatorSeconds = 0.0
                )
            }
            var shouldBroadcast = false
            var worldAge = 0L
            var timeOfDay = 0L
            synchronized(state) {
                state.worldAgeTicks += tickDelta
                state.timeOfDayTicks = normalizeTimeOfDayTicks(state.timeOfDayTicks + tickDelta)
                state.broadcastAccumulatorSeconds += deltaSeconds
                if (state.broadcastAccumulatorSeconds >= WORLD_TIME_BROADCAST_INTERVAL_SECONDS) {
                    state.broadcastAccumulatorSeconds -= WORLD_TIME_BROADCAST_INTERVAL_SECONDS
                    shouldBroadcast = true
                    worldAge = state.worldAgeTicks.toLong()
                    timeOfDay = state.timeOfDayTicks.toLong()
                }
            }
            if (!shouldBroadcast) continue
            broadcastWorldTimePacket(world.key, worldAge, timeOfDay)
        }
    }

    fun triggerChunkStream(channelId: ChannelId) {
        val session = sessions[channelId] ?: return
        val ctx = contexts[channelId] ?: return
        val world = WorldManager.world(session.worldKey) ?: return
        requestChunkStream(session, ctx, world, session.centerChunkX, session.centerChunkZ, session.chunkRadius)
    }

    fun updateViewDistance(channelId: ChannelId, requestedViewDistance: Int) {
        val session = sessions[channelId] ?: return
        session.requestedViewDistance = requestedViewDistance.coerceAtLeast(0)
        val newRadius = effectiveChunkRadius(session.requestedViewDistance)
        if (newRadius == session.chunkRadius) return
        session.chunkRadius = newRadius

        val ctx = contexts[channelId] ?: return
        val world = WorldManager.world(session.worldKey) ?: return
        requestChunkStream(session, ctx, world, session.centerChunkX, session.centerChunkZ, session.chunkRadius)
    }

    fun updateAndBroadcastMovement(
        channelId: ChannelId,
        x: Double,
        y: Double,
        z: Double,
        yaw: Float,
        pitch: Float,
        onGround: Boolean
    ) {
        val session = sessions[channelId] ?: return
        val ctx = contexts[channelId]
        val world = WorldManager.world(session.worldKey)
        val previousX = session.x
        val previousY = session.y
        val previousZ = session.z
        val wasOnGround = session.onGround
        val deltaY = y - previousY
        val deltaX = x - previousX
        val deltaZ = z - previousZ
        session.velocityX = deltaX
        session.velocityY = deltaY
        session.velocityZ = deltaZ
        session.lastHorizontalMovementSq = deltaX * deltaX + deltaZ * deltaZ
        session.x = x
        session.y = y
        session.z = z
        session.yaw = yaw
        session.pitch = pitch
        session.onGround = onGround
        updateFallDistance(session, wasOnGround, onGround, deltaY)

        val encodedX = encodeRelativePosition4096(x)
        val encodedY = encodeRelativePosition4096(y)
        val encodedZ = encodeRelativePosition4096(z)
        val dx = encodedX - session.encodedX4096
        val dy = encodedY - session.encodedY4096
        val dz = encodedZ - session.encodedZ4096
        val requiresTeleportSync =
            dx < Short.MIN_VALUE.toLong() || dx > Short.MAX_VALUE.toLong() ||
                dy < Short.MIN_VALUE.toLong() || dy > Short.MAX_VALUE.toLong() ||
                dz < Short.MIN_VALUE.toLong() || dz > Short.MAX_VALUE.toLong()

        // Do not infer swimming from jump/fall alone.
        // Require an intentional low-profile input combination to avoid false swim pose on jump.
        val inferredSwimming = session.sprinting && session.sneaking && !onGround
        val swimmingChanged = session.swimming != inferredSwimming
        session.swimming = inferredSwimming

        val packet = if (requiresTeleportSync) {
            PlayPackets.entityPositionSyncPacket(
                entityId = session.entityId,
                x = x,
                y = y,
                z = z,
                yaw = yaw,
                pitch = pitch,
                onGround = onGround
            )
        } else {
            PlayPackets.entityRelativeMoveLookPacket(
                entityId = session.entityId,
                deltaX = dx.toInt(),
                deltaY = dy.toInt(),
                deltaZ = dz.toInt(),
                yaw = yaw,
                pitch = pitch,
                onGround = onGround
            )
        }
        val headLookPacket = PlayPackets.entityHeadLookPacket(
            entityId = session.entityId,
            headYaw = yaw
        )
        val statePacket = if (swimmingChanged) {
            PlayPackets.playerSharedFlagsMetadataPacket(
                entityId = session.entityId,
                sneaking = session.sneaking,
                sprinting = session.sprinting,
                swimming = session.swimming
            )
        } else null

        for ((id, other) in sessions) {
            if (id == channelId || other.worldKey != session.worldKey) continue
            val otherCtx = contexts[id] ?: continue
            if (otherCtx.channel().isActive) {
                otherCtx.write(packet)
                otherCtx.write(headLookPacket)
                if (statePacket != null) otherCtx.write(statePacket)
                otherCtx.flush()
            }
        }
        session.encodedX4096 = encodedX
        session.encodedY4096 = encodedY
        session.encodedZ4096 = encodedZ

        val currentChunkX = ChunkStreamingService.chunkXFromBlockX(x)
        val currentChunkZ = ChunkStreamingService.chunkZFromBlockZ(z)
        if (currentChunkX != session.centerChunkX || currentChunkZ != session.centerChunkZ) {
            session.centerChunkX = currentChunkX
            session.centerChunkZ = currentChunkZ
            if (ctx != null && world != null) {
                requestChunkStream(session, ctx, world, currentChunkX, currentChunkZ, session.chunkRadius)
            }
        }

        if (!session.dead && session.gameMode == GAME_MODE_SURVIVAL && wasOnGround != onGround && onGround) {
            applyLandingDamage(session)
        }
    }

    private fun updateFallDistance(session: PlayerSession, wasOnGround: Boolean, onGround: Boolean, deltaY: Double) {
        if (session.gameMode != GAME_MODE_SURVIVAL || session.dead) {
            session.fallDistance = 0.0
            return
        }
        if (onGround) {
            if (wasOnGround) {
                session.fallDistance = 0.0
            }
            return
        }
        when {
            deltaY < 0.0 -> session.fallDistance += -deltaY
            deltaY > 0.0 -> session.fallDistance = 0.0
            wasOnGround -> session.fallDistance = 0.0
        }
    }

    private fun applyLandingDamage(session: PlayerSession) {
        val accumulatedFallDistance = session.fallDistance
        session.fallDistance = 0.0
        val damage = ceil(accumulatedFallDistance - SAFE_FALL_DISTANCE_BLOCKS).toInt()
        if (damage > 0) {
            damagePlayer(
                session = session,
                amount = damage.toFloat(),
                damageTypeId = FALL_DAMAGE_TYPE_ID,
                hurtSoundId = PLAYER_SMALL_FALL_SOUND_ID,
                bigHurtSoundId = PLAYER_BIG_FALL_SOUND_ID,
                deathMessage = PlayPackets.ChatComponent.Translate(
                    key = "death.fell.accident.generic",
                    args = listOf(playerNameComponent(session.profile))
                )
            )
        }
    }

    fun handlePlayerAttackEntity(attackerChannelId: ChannelId, targetEntityId: Int) {
        val attacker = sessions[attackerChannelId] ?: return
        if (attacker.dead) return
        if (attacker.gameMode == GAME_MODE_SPECTATOR) return

        val target = sessions.values.firstOrNull { it.entityId == targetEntityId } ?: return
        if (target.channelId == attackerChannelId) return
        if (target.worldKey != attacker.worldKey) return
        if (target.dead) return
        if (target.gameMode == GAME_MODE_SPECTATOR) return
        if (target.gameMode == GAME_MODE_CREATIVE) return

        val dx = attacker.x - target.x
        val dy = attacker.y - target.y
        val dz = attacker.z - target.z
        val distanceSq = dx * dx + dy * dy + dz * dz
        if (distanceSq > MAX_PLAYER_MELEE_REACH_SQ) return

        val attackStrengthScale = 1.0f
        val strongAttack = attackStrengthScale > VANILLA_STRONG_ATTACK_THRESHOLD
        val knockbackAttack = attacker.sprinting && strongAttack
        val criticalAttack = strongAttack && canCriticalAttack(attacker, target)
        val sweepAttack = canSweepAttack(attacker, strongAttack, criticalAttack, knockbackAttack)
        var damage = meleeAttackDamage(attacker).coerceAtLeast(1.0f)
        if (criticalAttack) {
            damage *= VANILLA_CRITICAL_DAMAGE_MULTIPLIER
        }

        damagePlayer(
            session = target,
            amount = damage,
            damageTypeId = PLAYER_ATTACK_DAMAGE_TYPE_ID,
            hurtSoundId = PLAYER_HURT_SOUND_ID,
            bigHurtSoundId = PLAYER_HURT_SOUND_ID,
            deathMessage = PlayPackets.ChatComponent.Translate(
                key = "death.attack.player",
                args = listOf(
                    playerNameComponent(target.profile),
                    playerNameComponent(attacker.profile)
                )
            )
        )

        val knockbackStrength = BASE_PLAYER_KNOCKBACK + if (knockbackAttack) SPRINT_KNOCKBACK_BONUS else 0.0
        applyVanillaKnockback(attacker, target, knockbackStrength)
        if (knockbackAttack) {
            attacker.sprinting = false
            updateAndBroadcastPlayerState(attacker.channelId, sprinting = false)
            broadcastAttackSound(attacker, PLAYER_ATTACK_KNOCKBACK_SOUND_ID)
        } else if (criticalAttack) {
            broadcastAttackSound(attacker, PLAYER_ATTACK_CRIT_SOUND_ID)
        } else if (sweepAttack) {
            // Vanilla does not play strong/weak when sweep triggers.
        } else if (strongAttack) {
            broadcastAttackSound(attacker, PLAYER_ATTACK_STRONG_SOUND_ID)
        } else {
            broadcastAttackSound(attacker, PLAYER_ATTACK_WEAK_SOUND_ID)
        }

        if (sweepAttack) {
            broadcastAttackSound(attacker, PLAYER_ATTACK_SWEEP_SOUND_ID)
            applySweepAttack(attacker, target, damageTypeId = PLAYER_ATTACK_DAMAGE_TYPE_ID)
        }
    }

    private fun meleeAttackDamage(attacker: PlayerSession): Float {
        val heldItemId = attacker.hotbarItemIds[attacker.selectedHotbarSlot.coerceIn(0, 8)]
        val heldItemKey = heldItemKey(heldItemId) ?: return BASE_UNARMED_DAMAGE
        return vanillaAttackDamageByItemKey(heldItemKey)
    }

    private fun heldItemKey(itemId: Int): String? {
        if (itemId < 0) return null
        val key = itemKeyById.getOrNull(itemId) ?: return null
        return key.ifEmpty { null }
    }

    private fun vanillaAttackDamageByItemKey(itemKey: String): Float {
        val key = itemKey.substringAfter("minecraft:")
        return when {
            key.endsWith("_sword") -> when {
                key.startsWith("wooden_") || key.startsWith("golden_") -> 4.0f
                key.startsWith("stone_") -> 5.0f
                key.startsWith("iron_") -> 6.0f
                key.startsWith("diamond_") -> 7.0f
                key.startsWith("netherite_") -> 8.0f
                else -> 4.0f
            }
            key.endsWith("_axe") -> when {
                key.startsWith("wooden_") || key.startsWith("golden_") -> 7.0f
                key.startsWith("stone_") -> 9.0f
                key.startsWith("iron_") -> 9.0f
                key.startsWith("diamond_") -> 9.0f
                key.startsWith("netherite_") -> 10.0f
                else -> 7.0f
            }
            key.endsWith("_pickaxe") -> when {
                key.startsWith("wooden_") || key.startsWith("golden_") -> 2.0f
                key.startsWith("stone_") -> 3.0f
                key.startsWith("iron_") -> 4.0f
                key.startsWith("diamond_") -> 5.0f
                key.startsWith("netherite_") -> 6.0f
                else -> 2.0f
            }
            key.endsWith("_shovel") -> when {
                key.startsWith("wooden_") || key.startsWith("golden_") -> 2.5f
                key.startsWith("stone_") -> 3.5f
                key.startsWith("iron_") -> 4.5f
                key.startsWith("diamond_") -> 5.5f
                key.startsWith("netherite_") -> 6.5f
                else -> 2.5f
            }
            key.endsWith("_hoe") -> when {
                key.startsWith("wooden_") || key.startsWith("golden_") -> 1.0f
                key.startsWith("stone_") -> 1.0f
                key.startsWith("iron_") -> 1.0f
                key.startsWith("diamond_") -> 1.0f
                key.startsWith("netherite_") -> 1.0f
                else -> 1.0f
            }
            key == "trident" -> 9.0f
            else -> BASE_UNARMED_DAMAGE
        }
    }

    private fun canCriticalAttack(attacker: PlayerSession, target: PlayerSession): Boolean {
        return attacker.fallDistance > 0.0 &&
            !attacker.onGround &&
            !attacker.sprinting &&
            !attacker.dead &&
            !target.dead
    }

    private fun canSweepAttack(
        attacker: PlayerSession,
        strongAttack: Boolean,
        criticalAttack: Boolean,
        knockbackAttack: Boolean
    ): Boolean {
        if (!strongAttack || criticalAttack || knockbackAttack) return false
        if (!attacker.onGround) return false
        if (attacker.lastHorizontalMovementSq >= (PLAYER_SWEEP_SPEED_THRESHOLD * PLAYER_SWEEP_SPEED_THRESHOLD)) return false
        val heldItemId = attacker.hotbarItemIds[attacker.selectedHotbarSlot.coerceIn(0, 8)]
        val heldItemKey = heldItemKey(heldItemId) ?: return false
        return heldItemKey.endsWith("_sword")
    }

    private fun applyVanillaKnockback(attacker: PlayerSession, target: PlayerSession, strength: Double) {
        if (strength <= 0.0) return

        // Do not use packet-to-packet position delta as knockback base velocity.
        // It over-amplifies knockback under jitter and low packet rates.
        val horizontalBaseX = 0.0
        val horizontalBaseY = 0.0
        val horizontalBaseZ = 0.0
        val yawRad = Math.toRadians(attacker.yaw.toDouble())
        val knockDirX = sin(yawRad)
        val knockDirZ = -cos(yawRad)
        val norm = kotlin.math.sqrt((knockDirX * knockDirX) + (knockDirZ * knockDirZ))
        if (norm <= 1.0e-6) return

        val normalizedX = knockDirX / norm
        val normalizedZ = knockDirZ / norm
        val nextVx = (horizontalBaseX * 0.5) - (normalizedX * strength)
        val nextVz = (horizontalBaseZ * 0.5) - (normalizedZ * strength)
        val nextVy = if (target.onGround) {
            minOf(0.4, (horizontalBaseY * 0.5) + strength)
        } else {
            horizontalBaseY
        }

        target.velocityX = nextVx
        target.velocityY = nextVy
        target.velocityZ = nextVz
        broadcastEntityVelocity(target, nextVx, nextVy, nextVz)
    }

    private fun applySweepAttack(attacker: PlayerSession, primaryTarget: PlayerSession, damageTypeId: Int) {
        val sweepDamage = VANILLA_SWEEP_BASE_DAMAGE
        if (sweepDamage <= 0f) return
        for (other in sessions.values) {
            if (other.channelId == attacker.channelId || other.channelId == primaryTarget.channelId) continue
            if (other.worldKey != attacker.worldKey) continue
            if (other.dead) continue
            if (other.gameMode == GAME_MODE_SPECTATOR || other.gameMode == GAME_MODE_CREATIVE) continue

            val dx = other.x - primaryTarget.x
            val dy = other.y - primaryTarget.y
            val dz = other.z - primaryTarget.z
            if (dx * dx + dy * dy + dz * dz > VANILLA_SWEEP_MAX_DISTANCE_SQ) continue

            damagePlayer(
                session = other,
                amount = sweepDamage,
                damageTypeId = damageTypeId,
                hurtSoundId = PLAYER_HURT_SOUND_ID,
                bigHurtSoundId = PLAYER_HURT_SOUND_ID,
                deathMessage = PlayPackets.ChatComponent.Translate(
                    key = "death.attack.player",
                    args = listOf(
                        playerNameComponent(other.profile),
                        playerNameComponent(attacker.profile)
                    )
                )
            )
            applyVanillaKnockback(attacker, other, VANILLA_SWEEP_KNOCKBACK)
        }
    }

    private fun broadcastEntityVelocity(session: PlayerSession, vx: Double, vy: Double, vz: Double) {
        val velocityPacket = PlayPackets.entityVelocityPacket(
            entityId = session.entityId,
            vx = vx,
            vy = vy,
            vz = vz
        )
        for ((id, other) in sessions) {
            if (other.worldKey != session.worldKey) continue
            val ctx = contexts[id] ?: continue
            if (!ctx.channel().isActive) continue
            ctx.write(velocityPacket)
            ctx.flush()
        }
    }

    private fun broadcastAttackSound(attacker: PlayerSession, soundEventId: Int?) {
        if (soundEventId == null) return
        val packet = PlayPackets.soundEntityPacket(
            soundEventId = soundEventId,
            soundSourceId = PLAYERS_SOUND_SOURCE_ID,
            entityId = attacker.entityId,
            volume = 1.0f,
            pitch = 1.0f,
            seed = ThreadLocalRandom.current().nextLong()
        )
        for ((id, other) in sessions) {
            if (other.worldKey != attacker.worldKey) continue
            val ctx = contexts[id] ?: continue
            if (!ctx.channel().isActive) continue
            ctx.write(packet)
            ctx.flush()
        }
    }

    private fun damagePlayer(
        session: PlayerSession,
        amount: Float,
        damageTypeId: Int,
        hurtSoundId: Int?,
        bigHurtSoundId: Int?,
        deathMessage: PlayPackets.ChatComponent
    ) {
        if (session.dead || amount <= 0f) return
        if (session.gameMode == GAME_MODE_SPECTATOR || session.gameMode == GAME_MODE_CREATIVE) return
        val nextHealth = (session.health - amount).coerceAtLeast(0f)
        if (nextHealth == session.health) return
        session.health = nextHealth
        broadcastDamageFeedback(session, amount, damageTypeId, hurtSoundId, bigHurtSoundId)
        sendHealthPacket(session)
        if (nextHealth <= 0f) {
            handlePlayerDeath(session, deathMessage)
        }
    }

    private fun broadcastDamageFeedback(
        session: PlayerSession,
        amount: Float,
        damageTypeId: Int,
        hurtSoundId: Int?,
        bigHurtSoundId: Int?
    ) {
        val hurtAnimationPacket = PlayPackets.hurtAnimationPacket(session.entityId, session.yaw)
        val damageEventPacket = PlayPackets.damageEventPacket(session.entityId, damageTypeId)
        val fallSoundId = if (amount >= BIG_FALL_SOUND_DAMAGE_THRESHOLD) {
            bigHurtSoundId ?: PLAYER_HURT_SOUND_ID
        } else {
            hurtSoundId ?: PLAYER_HURT_SOUND_ID
        }
        val fallSoundPacket = fallSoundId?.let { soundEventId ->
            PlayPackets.soundEntityPacket(
                soundEventId = soundEventId,
                soundSourceId = PLAYERS_SOUND_SOURCE_ID,
                entityId = session.entityId,
                volume = 1.0f,
                pitch = 1.0f,
                seed = ThreadLocalRandom.current().nextLong()
            )
        }
        for ((id, other) in sessions) {
            if (other.worldKey != session.worldKey) continue
            val ctx = contexts[id] ?: continue
            if (!ctx.channel().isActive) continue
            ctx.write(hurtAnimationPacket)
            ctx.write(damageEventPacket)
            if (fallSoundPacket != null) {
                ctx.write(fallSoundPacket)
            }
            ctx.flush()
        }
    }

    private fun handlePlayerDeath(session: PlayerSession, deathMessage: PlayPackets.ChatComponent) {
        if (session.dead) return
        session.dead = true
        session.fallDistance = 0.0
        broadcastDeathAnimation(session)
        broadcastSystemMessage(session.worldKey, deathMessage)
    }

    private fun broadcastDeathAnimation(session: PlayerSession) {
        val deathAnimationPacket = PlayPackets.entityEventPacket(
            entityId = session.entityId,
            eventId = ENTITY_EVENT_DEATH_ANIMATION
        )
        for ((id, other) in sessions) {
            if (other.worldKey != session.worldKey) continue
            val ctx = contexts[id] ?: continue
            if (!ctx.channel().isActive) continue
            ctx.write(deathAnimationPacket)
            ctx.flush()
        }
    }

    private fun respawnPlayer(session: PlayerSession) {
        val world = WorldManager.world(session.worldKey) ?: WorldManager.defaultWorld()
        val spawn = FoliaSidecarSpawnPointProvider.spawnPointFor(world.key)
            ?: world.spawnPointForPlayer(session.profile.uuid)
        val ctx = contexts[session.channelId]
        session.dead = false
        session.health = MAX_PLAYER_HEALTH
        session.food = DEFAULT_PLAYER_FOOD
        session.saturation = DEFAULT_PLAYER_SATURATION
        session.fallDistance = 0.0
        session.sneaking = false
        session.sprinting = false
        session.swimming = false
        if (ctx != null && ctx.channel().isActive) {
            ctx.write(
                PlayPackets.respawnPacket(
                    worldKey = session.worldKey,
                    gameMode = session.gameMode,
                    previousGameMode = session.gameMode,
                    seaLevel = 63,
                    copyDataFlags = 0
                )
            )
            ctx.write(PlayPackets.gameStateStartLoadingPacket())
            resetRespawningSessionChunkView(session, ctx)
            val spawnChunkX = ChunkStreamingService.chunkXFromBlockX(spawn.x)
            val spawnChunkZ = ChunkStreamingService.chunkZFromBlockZ(spawn.z)
            session.centerChunkX = spawnChunkX
            session.centerChunkZ = spawnChunkZ
            requestChunkStream(session, ctx, world, spawnChunkX, spawnChunkZ, session.chunkRadius)
        }
        teleportPlayer(session, spawn.x, spawn.y, spawn.z, 0f, 0f)
        if (ctx != null && ctx.channel().isActive) {
            ctx.write(PlayPackets.spawnPositionPacket(session.worldKey, spawn.x, spawn.y, spawn.z))
            ctx.flush()
        }
        sendHealthPacket(session)
        updateAndBroadcastPlayerState(session.channelId, sneaking = false, sprinting = false, swimming = false)
    }

    private fun resetRespawningSessionChunkView(session: PlayerSession, ctx: ChannelHandlerContext) {
        session.pendingChunkTarget.set(null)
        session.targetChunks.clear()
        session.generatingChunks.clear()
        session.visibleDroppedItemEntityIds.clear()
        session.droppedItemTrackerStates.clear()
        session.visibleFallingBlockEntityIds.clear()
        session.fallingBlockTrackerStates.clear()
        val toUnload = ArrayList<ChunkPos>(session.loadedChunks)
        session.loadedChunks.clear()
        for (pos in toUnload) {
            ctx.write(PlayPackets.unloadChunkPacket(pos.x, pos.z))
        }
        updateRetainedBaseWorldChunksForWorld(session.worldKey)
    }

    fun respawnIfDead(channelId: ChannelId) {
        val session = sessions[channelId] ?: return
        if (!session.dead) return
        respawnPlayer(session)
    }

    private fun nextTeleportId(session: PlayerSession): Int {
        val next = session.nextTeleportId.getAndIncrement()
        return if (next > 0) next else session.nextTeleportId.updateAndGet { current ->
            if (current <= 0) 2 else current
        } - 1
    }

    private fun sendHealthPacket(session: PlayerSession) {
        val ctx = contexts[session.channelId] ?: return
        if (!ctx.channel().isActive) return
        ctx.executor().execute {
            if (!ctx.channel().isActive) return@execute
            ctx.writeAndFlush(PlayPackets.setHealthPacket(session.health, session.food, session.saturation))
        }
    }

    fun updateAndBroadcastPlayerState(
        channelId: ChannelId,
        sneaking: Boolean? = null,
        sprinting: Boolean? = null,
        swimming: Boolean? = null
    ) {
        val session = sessions[channelId] ?: return
        if (sneaking != null) session.sneaking = sneaking
        if (sprinting != null) session.sprinting = sprinting
        if (swimming != null) session.swimming = swimming

        val metadataPacket = PlayPackets.playerSharedFlagsMetadataPacket(
            entityId = session.entityId,
            sneaking = session.sneaking,
            sprinting = session.sprinting,
            swimming = session.swimming
        )

        for ((id, other) in sessions) {
            if (id == channelId || other.worldKey != session.worldKey) continue
            val otherCtx = contexts[id] ?: continue
            if (!otherCtx.channel().isActive) continue
            otherCtx.writeAndFlush(metadataPacket)
        }
    }

    fun broadcastSwingAnimation(channelId: ChannelId, offHand: Boolean) {
        val session = sessions[channelId] ?: return
        val packet = PlayPackets.entityAnimationPacket(
            entityId = session.entityId,
            animationType = if (offHand) 3 else 0
        )

        for ((id, other) in sessions) {
            if (id == channelId || other.worldKey != session.worldKey) continue
            val otherCtx = contexts[id] ?: continue
            if (!otherCtx.channel().isActive) continue
            otherCtx.writeAndFlush(packet)
        }
    }

    fun leave(channelId: ChannelId) {
        val removed = sessions.remove(channelId) ?: return
        contexts.remove(channelId)

        val removeEntitiesPacket = PlayPackets.removeEntitiesPacket(intArrayOf(removed.entityId))
        val removeInfoPacket = PlayPackets.playerInfoRemovePacket(listOf(removed.profile.uuid))
        for ((id, other) in sessions) {
            if (other.worldKey != removed.worldKey) continue
            val otherCtx = contexts[id] ?: continue
            if (!otherCtx.channel().isActive) continue
            otherCtx.write(removeEntitiesPacket)
            otherCtx.write(removeInfoPacket)
            otherCtx.flush()
        }

        broadcastSystemMessage(
            removed.worldKey,
            PlayPackets.ChatComponent.Translate(
                key = "multiplayer.player.left",
                args = listOf(playerNameComponent(removed.profile)),
                color = "yellow"
            )
        )
        ServerI18n.log("aerogel.log.info.player.left", removed.profile.username)
    }

    fun shutdown() {
        droppedItemDispatchRunning.set(false)
        droppedItemDispatchThread?.interrupt()
        droppedItemDispatchThread = null
        for ((_, ctx) in contexts) {
            runCatching { ctx.close() }
        }
        contexts.clear()
        sessions.clear()
    }

    fun broadcastChat(channelId: ChannelId, message: String) {
        val sender = sessions[channelId] ?: return
        val clean = message.trim()
        if (clean.isEmpty()) return
        if (clean.startsWith("/")) {
            dispatchCommandAsync(sender, clean)
            return
        }
        val packet = PlayPackets.systemChatPacket(
            PlayPackets.ChatComponent.Text(
                text = "<",
                extra = listOf(
                    playerNameComponent(sender.profile),
                    PlayPackets.ChatComponent.Text("> $clean")
                )
            )
        )
        for ((id, other) in sessions) {
            if (other.worldKey != sender.worldKey) continue
            val ctx = contexts[id] ?: continue
            if (!ctx.channel().isActive) continue
            ctx.writeAndFlush(packet)
        }
    }

    fun submitCommand(channelId: ChannelId, command: String) {
        val sender = sessions[channelId] ?: return
        val clean = command.trim()
        if (clean.isEmpty()) return
        dispatchCommandAsync(sender, if (clean.startsWith("/")) clean else "/$clean")
    }

    fun submitConsoleCommand(command: String) {
        val clean = command.trim()
        if (clean.isEmpty()) return
        dispatchCommandAsync(null, if (clean.startsWith("/")) clean else "/$clean")
    }

    fun sendCommandSuggestions(channelId: ChannelId, requestId: Int, input: String) {
        val sender = sessions[channelId] ?: return
        val ctx = contexts[channelId] ?: return
        if (!ctx.channel().isActive) return
        val window = inGameCommandSuggestions(sender, input, includeOperatorCommands = isOperator(sender))
        val packet = PlayPackets.commandSuggestionsPacket(
            requestId = requestId,
            start = window.start,
            length = window.length,
            suggestions = window.suggestions
        )
        ctx.writeAndFlush(packet)
    }

    fun updateLocale(channelId: ChannelId, locale: String) {
        val session = sessions[channelId] ?: return
        session.locale = locale
    }

    fun inGameCommandSuggestions(sender: PlayerSession?, input: String, includeOperatorCommands: Boolean): CommandSuggestionWindow {
        val commandNames = if (includeOperatorCommands) {
            commandDispatcher.commandNames().sortedWith(String.CASE_INSENSITIVE_ORDER)
        } else {
            emptyList()
        }
        val hasSlash = input.startsWith("/")
        val normalized = if (hasSlash) input.substring(1) else input

        if (normalized.isEmpty()) {
            return CommandSuggestionWindow(
                start = if (hasSlash) 1 else 0,
                length = 0,
                suggestions = commandNames
            )
        }

        val trailingWhitespace = normalized.lastOrNull()?.isWhitespace() == true
        val tokens = normalized.trim().split(Regex("\\s+")).filter { it.isNotEmpty() }
        if (tokens.isEmpty()) {
            return CommandSuggestionWindow(
                start = if (hasSlash) 1 else 0,
                length = 0,
                suggestions = commandNames
            )
        }

        if (tokens.size == 1 && !trailingWhitespace) {
            val prefix = tokens[0]
            val matched = commandNames
                .asSequence()
                .filter { it.startsWith(prefix, ignoreCase = true) }
                .toList()
            return CommandSuggestionWindow(
                start = if (hasSlash) 1 else 0,
                length = prefix.length,
                suggestions = matched
            )
        }

        val commandName = tokens[0].lowercase()
        val providedArgs = if (tokens.size > 1) tokens.drop(1) else emptyList()
        val activeArgIndex = if (trailingWhitespace) providedArgs.size else providedArgs.size - 1
        val activeArgPrefix = if (trailingWhitespace) "" else providedArgs.lastOrNull().orEmpty()
        val activeArgStart = if (trailingWhitespace) {
            normalized.length
        } else {
            normalized.lastIndexOfAny(charArrayOf(' ', '\t')) + 1
        }
        val selectorFragment = inGameSelectorSuggestions(commandName, activeArgPrefix, activeArgStart)
        if (selectorFragment != null) {
            return CommandSuggestionWindow(
                start = selectorFragment.start + if (hasSlash) 1 else 0,
                length = selectorFragment.length,
                suggestions = selectorFragment.suggestions
            )
        }
        return CommandSuggestionWindow(
            start = (activeArgStart + if (hasSlash) 1 else 0) +
                if (shouldAppendTimeSetUnitSuffix(commandName, providedArgs, activeArgIndex, activeArgPrefix)) activeArgPrefix.length else 0,
            length = if (shouldAppendTimeSetUnitSuffix(commandName, providedArgs, activeArgIndex, activeArgPrefix)) {
                0
            } else {
                if (trailingWhitespace) 0 else normalized.length - activeArgStart
            },
            suggestions = commandArgumentCompletions(sender, commandName, providedArgs, activeArgIndex, activeArgPrefix)
        )
    }

    private fun shouldAppendTimeSetUnitSuffix(
        commandName: String,
        providedArgs: List<String>,
        activeArgIndex: Int,
        activeArgPrefix: String
    ): Boolean {
        if (commandName != "time") return false
        if (activeArgIndex != 1) return false
        if (providedArgs.getOrNull(0)?.lowercase() != "set") return false
        return activeArgPrefix.matches(Regex("^\\d+$"))
    }

    fun consoleCommandCompletions(input: String): List<String> {
        val line = input.trimStart()
        val commandNames = commandDispatcher.commandNames().sorted()
        if (line.isEmpty()) return commandNames

        val hasSlash = line.startsWith("/")
        val normalized = if (hasSlash) line.substring(1) else line
        if (normalized.isEmpty()) {
            return commandNames.map { "/$it" }
        }

        val trailingWhitespace = line.lastOrNull()?.isWhitespace() == true
        val tokens = normalized.trim().split(Regex("\\s+")).filter { it.isNotEmpty() }
        if (tokens.isEmpty()) return if (hasSlash) commandNames.map { "/$it" } else commandNames

        if (tokens.size == 1 && !trailingWhitespace) {
            val prefix = tokens[0]
            val matched = commandNames
                .asSequence()
                .filter { it.startsWith(prefix, ignoreCase = true) }
                .toList()
            return if (hasSlash) matched.map { "/$it" } else matched
        }

        val commandName = tokens[0].lowercase()
        val providedArgs = if (tokens.size > 1) tokens.drop(1) else emptyList()
        val activeArgIndex = if (trailingWhitespace) providedArgs.size else providedArgs.size - 1
        val activeArgPrefix = if (trailingWhitespace) "" else providedArgs.lastOrNull().orEmpty()
        return commandArgumentCompletions(null, commandName, providedArgs, activeArgIndex, activeArgPrefix)
    }

    private fun commandArgumentCompletions(
        sender: PlayerSession?,
        commandName: String,
        providedArgs: List<String>,
        activeArgIndex: Int,
        activeArgPrefix: String
    ): List<String> {
        return when (commandName) {
            "op" -> if (activeArgIndex == 0) {
                sessions.values.asSequence()
                    .map { it.profile.username }
                    .distinct()
                    .filter { it.startsWith(activeArgPrefix, ignoreCase = true) }
                    .sortedWith(String.CASE_INSENSITIVE_ORDER)
                    .toList()
            } else {
                emptyList()
            }
            "deop" -> if (activeArgIndex == 0) {
                val onlineOperators = sessions.values.asSequence()
                    .filter { isOperator(it) }
                    .toList()
                val onlineCandidateUuids = onlineOperators.asSequence()
                    .map { it.profile.uuid }
                    .toSet()
                val onlineCandidates = onlineOperators.asSequence()
                    .map { it.profile.username }
                    .filter { it.startsWith(activeArgPrefix, ignoreCase = true) }
                    .distinct()
                    .sortedWith(String.CASE_INSENSITIVE_ORDER)
                    .toList()
                val offlineUuidCandidates = persistedOperatorUuids.asSequence()
                    .filterNot { it in onlineCandidateUuids }
                    .map(UUID::toString)
                    .filter { it.startsWith(activeArgPrefix, ignoreCase = true) }
                    .sortedWith(String.CASE_INSENSITIVE_ORDER)
                    .toList()
                (onlineCandidates + offlineUuidCandidates).distinct()
            } else {
                emptyList()
            }
            "gamemode" -> gamemodeCommandCompletions(activeArgIndex, activeArgPrefix)
            "tp", "teleport" -> tpCommandCompletions(sender, providedArgs, activeArgIndex, activeArgPrefix)
            "time" -> timeCommandCompletions(providedArgs, activeArgIndex, activeArgPrefix)
            else -> emptyList()
        }
    }

    private fun timeCommandCompletions(
        providedArgs: List<String>,
        activeArgIndex: Int,
        activeArgPrefix: String
    ): List<String> {
        val subcommands = listOf("add", "set", "query")
        val queryKinds = listOf("daytime", "gametime", "day")
        val setValueAliases = listOf("day", "night", "midnight", "noon")
        val worldKeys = WorldManager.allWorlds()
            .asSequence()
            .map { it.key.substringAfter(':', it.key) }
            .sortedWith(String.CASE_INSENSITIVE_ORDER)
            .toList()
        val subcommand = providedArgs.getOrNull(0)?.lowercase()
        val raw = when (activeArgIndex) {
            0 -> subcommands
            1 -> when (subcommand) {
                "set" -> timeSetValueCompletions(activeArgPrefix, setValueAliases)
                "add" -> listOf("1000", "6000", "12000")
                "query" -> queryKinds
                else -> emptyList()
            }
            2 -> if (subcommand in subcommands) worldKeys else emptyList()
            else -> emptyList()
        }
        val appendUnitSuffix = shouldAppendTimeSetUnitSuffix(
            "time",
            providedArgs,
            activeArgIndex,
            activeArgPrefix
        )
        val filtered = if (appendUnitSuffix) {
            // Suffix suggestions (d/s/t) are appended at token end; do not prefix-filter with numeric input.
            raw
        } else {
            raw.asSequence()
                .filter { it.startsWith(activeArgPrefix, ignoreCase = true) }
                .toList()
        }
        return filtered.asSequence().distinct().toList()
    }

    private fun timeSetValueCompletions(activeArgPrefix: String, aliases: List<String>): List<String> {
        val trimmed = activeArgPrefix.trim()
        if (trimmed.matches(Regex("^\\d+$"))) {
            return listOf("d", "s", "t")
        }
        val base = ArrayList<String>(aliases.size)
        base.addAll(aliases)
        return base
    }

    private fun gamemodeCommandCompletions(
        activeArgIndex: Int,
        activeArgPrefix: String
    ): List<String> {
        return when (activeArgIndex) {
            0 -> gamemodeCompletionCandidates.asSequence()
                .filter { it.startsWith(activeArgPrefix, ignoreCase = true) }
                .toList()
            1 -> sessions.values.asSequence()
                .map { it.profile.username }
                .distinct()
                .filter { it.startsWith(activeArgPrefix, ignoreCase = true) }
                .sortedWith(String.CASE_INSENSITIVE_ORDER)
                .toList()
            else -> emptyList()
        }
    }

    private fun tpCommandCompletions(
        sender: PlayerSession?,
        providedArgs: List<String>,
        activeArgIndex: Int,
        activeArgPrefix: String
    ): List<String> {
        val playerCandidates = sessions.values.asSequence()
            .map { it.profile.username }
            .distinct()
            .sortedWith(String.CASE_INSENSITIVE_ORDER)
            .toList()
        val selectorCandidates = selectorCandidates(activeArgPrefix)
        val targetCandidates = (playerCandidates + selectorCandidates)
            .distinct()
            .sortedWith(String.CASE_INSENSITIVE_ORDER)
        val coordinateCandidates = tpCoordinateCandidates(sender, providedArgs, activeArgIndex, activeArgPrefix)

        val raw = when (activeArgIndex) {
            0 -> coordinateCandidates + targetCandidates
            1 -> {
                if (providedArgs.firstOrNull()?.let(::looksLikeCoordinateToken) == true) {
                    coordinateCandidates
                } else {
                    coordinateCandidates + targetCandidates
                }
            }
            2 -> coordinateCandidates
            else -> emptyList()
        }
        return raw.asSequence()
            .filter { it.startsWith(activeArgPrefix, ignoreCase = true) }
            .distinct()
            .toList()
    }

    private fun tpCoordinateCandidates(
        sender: PlayerSession?,
        providedArgs: List<String>,
        activeArgIndex: Int,
        activeArgPrefix: String
    ): List<String> {
        val x = formatTpCompletionCoord(sender?.x ?: 0.0)
        val y = formatTpCompletionCoord(sender?.y ?: 64.0)
        val z = formatTpCompletionCoord(sender?.z ?: 0.0)
        val coordinateStartArgIndex = when {
            providedArgs.isEmpty() -> 0
            providedArgs.firstOrNull()?.let(::looksLikeCoordinateToken) == true -> 0
            else -> 1
        }
        val coordinateComponentIndex = activeArgIndex - coordinateStartArgIndex
        if (coordinateComponentIndex !in 0..2) return emptyList()
        val coordinateTokens = providedArgs.drop(coordinateStartArgIndex)
        val currentRelativeToken = activeArgPrefix.takeIf { it.startsWith("~") } ?: "~"
        val shouldSuggestRelative = activeArgPrefix.startsWith("~") || coordinateTokens.any { it.startsWith("~") }

        return if (shouldSuggestRelative) {
            listOf(currentRelativeToken)
        } else {
            when (coordinateComponentIndex) {
                0 -> listOf(x, "$x $y", "$x $y $z")
                1 -> listOf(y, "$y $z")
                else -> listOf(z)
            }
        }
    }

    private fun looksLikeCoordinateToken(token: String): Boolean {
        if (token.isEmpty()) return false
        if (token == "~") return true
        if (token.startsWith("~")) return token.substring(1).toDoubleOrNull() != null
        if (token.startsWith("^")) return token.substring(1).isEmpty() || token.substring(1).toDoubleOrNull() != null
        return token.toDoubleOrNull() != null
    }

    private fun formatTpCompletionCoord(value: Double): String {
        val rounded = kotlin.math.round(value * 1000.0) / 1000.0
        val whole = rounded.toLong().toDouble() == rounded
        return if (whole) {
            rounded.toLong().toString()
        } else {
            java.lang.Double.toString(rounded)
        }
    }

    private fun selectorCandidates(prefix: String): List<String> {
        return EntitySelectorCompletions.suggest(prefix)
    }

    private fun inGameSelectorSuggestions(
        commandName: String,
        activeArgPrefix: String,
        activeArgStart: Int
    ): CommandSuggestionWindow? {
        if (commandName != "tp" && commandName != "teleport") return null
        val fragment = EntitySelectorCompletions.suggestFragment(activeArgPrefix) ?: return null
        return CommandSuggestionWindow(
            start = activeArgStart + fragment.relativeStart,
            length = fragment.replaceLength,
            suggestions = fragment.suggestions
        )
    }

    private fun dispatchCommandAsync(sender: PlayerSession?, rawCommand: String) {
        commandExecutor.execute {
            commandDispatcher.dispatch(commandContext, sender, rawCommand)
        }
    }

    private fun canRunOpCommand(sender: PlayerSession?): Boolean {
        if (sender == null) return true
        if (operatorUuids.isEmpty()) return true
        return isOperator(sender)
    }

    fun isOperatorSession(session: PlayerSession): Boolean = isOperator(session)

    private fun grantOperatorAndRefreshCommands(target: PlayerSession): Boolean {
        val granted = operatorUuids.add(target.profile.uuid)
        if (granted) {
            addPersistedOperator(target.profile.uuid)
            val ctx = contexts[target.channelId]
            if (ctx != null && ctx.channel().isActive) {
                ctx.executor().execute {
                    if (ctx.channel().isActive) {
                        ctx.writeAndFlush(PlayPackets.commandsPacket(includeOperatorCommands = true))
                    }
                }
            }
        }
        return granted
    }

    private fun revokeOperatorAndRefreshCommands(uuid: UUID): Boolean {
        val target = sessions.values.firstOrNull { it.profile.uuid == uuid }
        val removedPersisted = removePersistedOperator(uuid)
        val removedActive = operatorUuids.remove(uuid)
        if (removedActive && target != null) {
            val ctx = contexts[target.channelId]
            if (ctx != null && ctx.channel().isActive) {
                ctx.executor().execute {
                    if (ctx.channel().isActive) {
                        ctx.writeAndFlush(PlayPackets.commandsPacket(includeOperatorCommands = false))
                    }
                }
            }
        }
        return removedPersisted || removedActive
    }

    private fun isOperator(session: PlayerSession?): Boolean {
        if (session == null) return true
        return operatorUuids.contains(session.profile.uuid)
    }

    private fun loadPersistedOperators() {
        synchronized(operatorFileLock) {
            persistedOperatorUuids.clear()
            if (!Files.exists(operatorFilePath)) return
            try {
                Files.readAllLines(operatorFilePath).asSequence()
                    .map { it.trim() }
                    .filter { it.isNotEmpty() }
                    .mapNotNull { runCatching { UUID.fromString(it) }.getOrNull() }
                    .forEach { persistedOperatorUuids.add(it) }
            } catch (t: Throwable) {
                logger.warn("Failed to load persisted operators from {}", operatorFilePath.toAbsolutePath(), t)
            }
        }
    }

    private fun addPersistedOperator(uuid: UUID) {
        if (persistedOperatorUuids.add(uuid)) {
            savePersistedOperators()
        }
    }

    private fun removePersistedOperator(uuid: UUID): Boolean {
        val removed = persistedOperatorUuids.remove(uuid)
        if (removed) {
            savePersistedOperators()
        }
        return removed
    }

    private fun savePersistedOperators() {
        synchronized(operatorFileLock) {
            try {
                val uuidLines = persistedOperatorUuids.asSequence()
                    .map(UUID::toString)
                    .sortedWith(String.CASE_INSENSITIVE_ORDER)
                    .toList()
                Files.write(operatorFilePath, uuidLines, Charsets.UTF_8)
            } catch (t: Throwable) {
                logger.warn("Failed to save persisted operators to {}", operatorFilePath.toAbsolutePath(), t)
            }
        }
    }

    private fun findOnlinePlayer(name: String): PlayerSession? {
        return sessions.values.firstOrNull { it.profile.username.equals(name, ignoreCase = true) }
    }

    private fun findOnlinePlayer(uuid: UUID): PlayerSession? {
        return sessions.values.firstOrNull { it.profile.uuid == uuid }
    }

    private fun onlinePlayersInWorld(worldKey: String): List<PlayerSession> {
        return sessions.values.filter { it.worldKey == worldKey }
    }

    private fun setPlayerGamemode(target: PlayerSession, mode: Int) {
        val clamped = mode.coerceIn(0, 3)
        target.gameMode = clamped
        val selfCtx = contexts[target.channelId]
        if (selfCtx != null && selfCtx.channel().isActive) {
            selfCtx.executor().execute {
                if (!selfCtx.channel().isActive) return@execute
                selfCtx.write(PlayPackets.gameStateGameModePacket(clamped))
                selfCtx.writeAndFlush(PlayPackets.playerInfoGameModeUpdatePacket(target.profile.uuid, clamped))
            }
        }
        val updatePacket = PlayPackets.playerInfoGameModeUpdatePacket(target.profile.uuid, clamped)
        for ((id, other) in sessions) {
            if (id == target.channelId) continue
            if (other.worldKey != target.worldKey) continue
            val ctx = contexts[id] ?: continue
            if (!ctx.channel().isActive) continue
            ctx.executor().execute {
                if (ctx.channel().isActive) {
                    ctx.writeAndFlush(updatePacket)
                }
            }
        }
    }

    private fun teleportPlayer(
        target: PlayerSession,
        x: Double,
        y: Double,
        z: Double,
        yaw: Float?,
        pitch: Float?
    ): Boolean {
        if (y < -64.0 || y > 320.0) return false
        val clampedY = y
        val hasRotation = yaw != null && pitch != null
        val nextYaw = yaw ?: target.yaw
        val nextPitch = pitch ?: target.pitch
        val selfCtx = contexts[target.channelId]
        if (selfCtx != null && selfCtx.channel().isActive) {
            selfCtx.executor().execute {
                if (selfCtx.channel().isActive) {
                    val teleportId = nextTeleportId(target)
                    val packet = if (hasRotation) {
                        PlayPackets.playerPositionPacket(
                            teleportId = teleportId,
                            x = x,
                            y = clampedY,
                            z = z,
                            yaw = nextYaw,
                            pitch = nextPitch,
                            relativeFlags = 0
                        )
                    } else {
                        // Keep client yaw/pitch unchanged by using relative-rotation flags with zero deltas.
                        PlayPackets.playerPositionPacket(
                            teleportId = teleportId,
                            x = x,
                            y = clampedY,
                            z = z,
                            yaw = 0f,
                            pitch = 0f,
                            relativeFlags = 0x18
                        )
                    }
                    selfCtx.writeAndFlush(packet)
                }
            }
        }
        updateAndBroadcastMovement(
            channelId = target.channelId,
            x = x,
            y = clampedY,
            z = z,
            yaw = nextYaw,
            pitch = nextPitch,
            onGround = false
        )
        return true
    }

    private fun sendSystemMessage(session: PlayerSession, message: String) {
        val ctx = contexts[session.channelId] ?: return
        if (!ctx.channel().isActive) return
        val packet = PlayPackets.systemChatPacket(message)
        ctx.executor().execute {
            if (ctx.channel().isActive) {
                ctx.writeAndFlush(packet)
            }
        }
    }

    private fun maybeSendChunkMsptActionBar(nowNanos: Long) {
        for (session in sessions.values) {
            if (nowNanos - session.lastChunkMsptActionBarAtNanos < CHUNK_MSPT_ACTION_BAR_INTERVAL_NANOS) continue
            val ctx = contexts[session.channelId] ?: continue
            if (!ctx.channel().isActive) continue
            val world = WorldManager.world(session.worldKey) ?: continue
            val mspt = world.chunkMspt(session.centerChunkX, session.centerChunkZ)
            val packet = PlayPackets.systemChatPacket(
                PlayPackets.ChatComponent.Text(
                    String.format(
                        Locale.ROOT,
                        "Chunk (%d, %d) MSPT: %.3f ms",
                        session.centerChunkX,
                        session.centerChunkZ,
                        mspt
                    )
                ),
                overlay = true
            )
            session.lastChunkMsptActionBarAtNanos = nowNanos
            ctx.executor().execute {
                if (ctx.channel().isActive) {
                    ctx.writeAndFlush(packet)
                }
            }
        }
    }

    private fun sendSystemComponent(session: PlayerSession, component: PlayPackets.ChatComponent) {
        val ctx = contexts[session.channelId] ?: return
        if (!ctx.channel().isActive) return
        val packet = PlayPackets.systemChatPacket(component)
        ctx.executor().execute {
            if (ctx.channel().isActive) {
                ctx.writeAndFlush(packet)
            }
        }
    }

    private fun sendSystemTranslation(session: PlayerSession, key: String, vararg args: PlayPackets.ChatComponent) {
        sendSystemTranslation(session, key, color = null, *args)
    }

    private fun sendSystemTranslation(
        session: PlayerSession,
        key: String,
        color: String?,
        vararg args: PlayPackets.ChatComponent
    ) {
        val ctx = contexts[session.channelId] ?: return
        if (!ctx.channel().isActive) return
        val packet = PlayPackets.systemChatPacket(
            PlayPackets.ChatComponent.Translate(
                key = key,
                args = args.toList(),
                color = color
            )
        )
        ctx.executor().execute {
            if (ctx.channel().isActive) {
                ctx.writeAndFlush(packet)
            }
        }
    }

    private fun sendTerminalSourcedTranslation(
        session: PlayerSession,
        key: String,
        vararg args: PlayPackets.ChatComponent
    ) {
        val source = PlayPackets.ChatComponent.Text(
            ServerI18n.trFor(session.locale, "aerogel.console.source.terminal"),
            italic = true
        )
        sendAdminSourcedTranslation(session, source, key, *args)
    }

    private fun sendAdminSourcedTranslation(
        session: PlayerSession,
        source: PlayPackets.ChatComponent,
        key: String,
        vararg args: PlayPackets.ChatComponent
    ) {
        val message = PlayPackets.ChatComponent.Translate(key = key, args = args.toList(), italic = true)
        val packetComponent = PlayPackets.ChatComponent.Translate(
            key = "chat.type.admin",
            args = listOf(source, message),
            color = "gray",
            italic = true
        )
        sendSystemComponent(session, packetComponent)
    }

    private fun sendUnknownCommandWithContext(session: PlayerSession?, input: String) {
        if (session == null) {
            ServerI18n.log(
                "aerogel.log.console.prefixed_error",
                ServerI18n.translate("aerogel.console.command.unknown.single_line", listOf(input))
            )
            return
        }
        sendSystemTranslation(session, "command.unknown.command", color = "red")
        val pointer = PlayPackets.ChatComponent.Text(
            text = input,
            color = "red",
            underlined = true,
            extra = listOf(
                PlayPackets.ChatComponent.Translate(
                    key = "command.context.here",
                    color = "red",
                    underlined = false
                )
            )
        )
        val ctx = contexts[session.channelId] ?: return
        if (!ctx.channel().isActive) return
        val packet = PlayPackets.systemChatPacket(pointer)
        ctx.executor().execute {
            if (ctx.channel().isActive) {
                ctx.writeAndFlush(packet)
            }
        }
    }

    private fun sendSourceTranslationWithContext(
        session: PlayerSession?,
        key: String,
        input: String,
        errorStart: Int,
        vararg args: PlayPackets.ChatComponent
    ) {
        if (session == null) {
            if (key == "command.unknown.command") {
                ServerI18n.log(
                    "aerogel.log.console.prefixed_error",
                    ServerI18n.translate("aerogel.console.command.unknown_or_incomplete")
                )
                return
            }
            val renderedArgs = args.map { ServerI18n.componentToText(it) }
            ServerI18n.log("aerogel.log.console.prefixed_error", ServerI18n.translate(key, renderedArgs))
            return
        }

        sendSystemTranslation(session, key, color = "red", *args)

        val safeStart = errorStart.coerceIn(0, input.length)
        val validPrefix = input.substring(0, safeStart)
        val invalidSuffix = input.substring(safeStart)
        val pointer = PlayPackets.ChatComponent.Text(
            text = validPrefix,
            color = if (validPrefix.isEmpty()) null else "gray",
            extra = listOf(
                PlayPackets.ChatComponent.Text(
                    text = invalidSuffix,
                    color = "red",
                    underlined = true,
                    extra = listOf(
                        PlayPackets.ChatComponent.Translate(
                            key = "command.context.here",
                            color = "red",
                            underlined = false
                        )
                    )
                )
            )
        )
        val ctx = contexts[session.channelId] ?: return
        if (!ctx.channel().isActive) return
        val packet = PlayPackets.systemChatPacket(pointer)
        ctx.executor().execute {
            if (ctx.channel().isActive) {
                ctx.writeAndFlush(packet)
            }
        }
    }

    private fun broadcastSystemMessage(worldKey: String, message: String) {
        broadcastSystemMessage(worldKey, PlayPackets.ChatComponent.Text(message))
    }

    private fun broadcastSystemMessage(worldKey: String, component: PlayPackets.ChatComponent) {
        val packet = PlayPackets.systemChatPacket(component)
        for ((id, other) in sessions) {
            if (other.worldKey != worldKey) continue
            val ctx = contexts[id] ?: continue
            if (!ctx.channel().isActive) continue
            ctx.writeAndFlush(packet)
        }
    }

    private fun playerNameComponent(profile: ConnectionProfile): PlayPackets.ChatComponent.Text {
        return PlayPackets.ChatComponent.Text(
            text = profile.username,
            hoverEntity = PlayPackets.HoverEntity(
                entityType = "minecraft:player",
                uuid = profile.uuid,
                name = profile.username
            )
        )
    }


    fun setSelectedBlockFromWorld(channelId: ChannelId, x: Int, y: Int, z: Int) {
        val session = sessions[channelId] ?: return
        if (session.gameMode != 1) return
        val world = WorldManager.world(session.worldKey) ?: return
        val ctx = contexts[channelId] ?: return
        // Pick-block pending state must only live for the next matching creative slot update.
        session.pendingPickedBlockStateId = 0

        val cached = resolvePickedBlockData(world, x, y, z, cachedOnly = true)
        if (cached == null) return
        applyPickedBlockData(session, ctx, cached)
    }

    private fun resolvePickedBlockData(
        world: org.macaroon3145.world.World,
        x: Int,
        y: Int,
        z: Int,
        cachedOnly: Boolean
    ): PickedBlockData? {
        val stateId = if (cachedOnly) {
            world.blockStateAtIfCached(x, y, z) ?: return null
        } else {
            world.blockStateAt(x, y, z)
        }
        if (stateId == 0) return null
        val pickedStateId = normalizePickedBlockStateId(stateId)
        val pickedItemId = itemIdForState(pickedStateId)
        if (pickedItemId < 0) return null

        val blockEntity = world.blockEntityAt(x, y, z)
        val pickedTypeId = blockEntity?.typeId ?: -1
        val pickedNbt = blockEntity?.nbtPayload
        return PickedBlockData(
            stateId = pickedStateId,
            itemId = pickedItemId,
            blockEntityTypeId = pickedTypeId,
            blockEntityNbtPayload = pickedNbt
        )
    }

    private fun applyPickedBlockData(session: PlayerSession, ctx: ChannelHandlerContext, picked: PickedBlockData) {
        val existingSlot = findHotbarSlotForPick(
            session = session,
            stateId = picked.stateId,
            itemId = picked.itemId,
            blockEntityTypeId = picked.blockEntityTypeId,
            blockEntityNbtPayload = picked.blockEntityNbtPayload
        )

        if (existingSlot >= 0) {
            session.selectedHotbarSlot = existingSlot
            session.selectedBlockStateId = picked.stateId
            if (ctx.channel().isActive) {
                ctx.writeAndFlush(PlayPackets.setHeldSlotPacket(existingSlot))
            }
            broadcastHeldItemEquipment(session)
            return
        }

        val previousSelected = session.selectedHotbarSlot.coerceIn(0, 8)
        val targetSlot = findPickInsertHotbarSlot(session, previousSelected)
        session.selectedHotbarSlot = targetSlot
        session.pendingPickedBlockStateId = picked.stateId
        session.hotbarBlockStateIds[targetSlot] = picked.stateId
        session.selectedBlockStateId = picked.stateId
        session.hotbarBlockEntityTypeIds[targetSlot] = picked.blockEntityTypeId
        session.hotbarBlockEntityNbtPayloads[targetSlot] = picked.blockEntityNbtPayload
        session.hotbarItemIds[targetSlot] = picked.itemId
        session.hotbarItemCounts[targetSlot] = 1

        if (ctx.channel().isActive && picked.itemId >= 0) {
            if (targetSlot != previousSelected) {
                ctx.write(PlayPackets.setHeldSlotPacket(targetSlot))
            }
            val inventoryStateId = nextInventoryStateId(session)
            ctx.writeAndFlush(
                PlayPackets.containerSetSlotPacket(
                    containerId = 0,
                    stateId = inventoryStateId,
                    slot = 36 + targetSlot,
                    encodedItemStack = encodedItemStack(picked.itemId)
                )
            )
        }
        broadcastHeldItemEquipment(session)
    }

    fun updateSelectedHotbarSlot(channelId: ChannelId, slot: Int) {
        val session = sessions[channelId] ?: return
        val clamped = slot.coerceIn(0, 8)
        session.selectedHotbarSlot = clamped
        session.selectedBlockStateId = if (session.hotbarItemCounts[clamped] > 0) {
            session.hotbarBlockStateIds[clamped]
        } else {
            0
        }
        broadcastHeldItemEquipment(session)
    }

    fun handleBlockDiggingAction(channelId: ChannelId, action: Int, x: Int, y: Int, z: Int) {
        val session = sessions[channelId] ?: return
        val world = WorldManager.world(session.worldKey)
        val shouldBreak = when (session.gameMode) {
            GAME_MODE_CREATIVE -> action == 0 || action == 2
            GAME_MODE_SURVIVAL -> {
                action == 2 || (action == 0 && world != null && isInstantBreakBlock(world.blockStateAt(x, y, z)))
            }
            else -> false
        }
        if (!shouldBreak) return
        breakBlock(channelId, x, y, z)
    }

    fun applyCreativeSlot(channelId: ChannelId, slot: Int, encodedItemStack: ByteArray) {
        val session = sessions[channelId] ?: return
        if (session.gameMode != 1) return
        val itemId = decodeItemNetworkId(encodedItemStack)
        val clampedSlot = slot.coerceAtLeast(-1)
        if (clampedSlot == -1) {
            val count = decodeItemCount(encodedItemStack).coerceAtMost(MAX_HOTBAR_STACK_SIZE)
            if (itemId >= 0 && count > 0) {
                spawnDroppedItemFromPlayer(
                    session = session,
                    itemId = itemId,
                    itemCount = count,
                    dropStackMotion = count > 1
                )
            }
            session.pendingPickedBlockStateId = 0
            return
        }
        if (clampedSlot == 45) {
            val count = decodeItemCount(encodedItemStack)
            if (itemId >= 0 && count > 0) {
                session.offhandItemId = itemId
                session.offhandItemCount = count
            } else {
                session.offhandItemId = -1
                session.offhandItemCount = 0
            }
            session.pendingPickedBlockStateId = 0
            broadcastHeldItemEquipment(session)
            return
        }
        val mainInventoryIndex = mainInventoryIndexForInventorySlot(clampedSlot)
        if (mainInventoryIndex >= 0) {
            val count = decodeItemCount(encodedItemStack)
            if (itemId >= 0 && count > 0) {
                session.mainInventoryItemIds[mainInventoryIndex] = itemId
                session.mainInventoryItemCounts[mainInventoryIndex] = count
            } else {
                session.mainInventoryItemIds[mainInventoryIndex] = -1
                session.mainInventoryItemCounts[mainInventoryIndex] = 0
            }
            session.pendingPickedBlockStateId = 0
            return
        }
        val armorIndex = armorIndexForInventorySlot(clampedSlot)
        if (armorIndex >= 0) {
            val count = decodeItemCount(encodedItemStack)
            if (itemId >= 0 && count > 0) {
                session.armorItemIds[armorIndex] = itemId
                session.armorItemCounts[armorIndex] = count
            } else {
                session.armorItemIds[armorIndex] = -1
                session.armorItemCounts[armorIndex] = 0
            }
            session.pendingPickedBlockStateId = 0
            broadcastHeldItemEquipment(session)
            return
        }
        if (clampedSlot !in 36..44) {
            session.pendingPickedBlockStateId = 0
            return
        }
        val hotbar = clampedSlot - 36
        session.hotbarItemIds[hotbar] = itemId
        session.hotbarItemCounts[hotbar] = decodeItemCount(encodedItemStack)
        val pendingSlotState = consumePendingPickedBlockState(session, hotbar, itemId)
        val blockKey = BlockStateRegistry.blockForItem(itemId)
        val stateTagProps = extractBlockStateTagProperties(encodedItemStack)
        val slotState = when {
            itemId < 0 -> 0
            pendingSlotState != null -> pendingSlotState
            blockKey != null && stateTagProps != null -> {
                BlockStateRegistry.stateId(blockKey, stateTagProps)
                    ?: BlockStateRegistry.defaultStateId(blockKey)
                    ?: ItemBlockStateRegistry.blockStateIdForItem(itemId)
                    ?: 0
            }
            blockKey != null -> {
                BlockStateRegistry.defaultStateId(blockKey)
                    ?: ItemBlockStateRegistry.blockStateIdForItem(itemId)
                    ?: 0
            }
            else -> ItemBlockStateRegistry.blockStateIdForItem(itemId) ?: 0
        }
        session.hotbarBlockStateIds[hotbar] = slotState
        // Default to no block-entity payload for generic items unless explicitly inferred/set later.
        session.hotbarBlockEntityTypeIds[hotbar] = -1
        session.hotbarBlockEntityNbtPayloads[hotbar] = null
        val commandFromItem = extractCommandFromEncodedItemStack(encodedItemStack)
        if (commandFromItem != null) {
            val typeId = BlockEntityTypeRegistry.idOf("minecraft:command_block")
            if (typeId != null) {
                session.hotbarBlockEntityTypeIds[hotbar] = typeId
                session.hotbarBlockEntityNbtPayloads[hotbar] = commandBlockNbtPayload(
                    x = 0,
                    y = 0,
                    z = 0,
                    command = commandFromItem,
                    trackOutput = true,
                    conditional = false,
                    automatic = false
                )
            }
        }
        if (session.selectedHotbarSlot == hotbar) {
            session.selectedBlockStateId = if (session.hotbarItemCounts[hotbar] > 0) slotState else 0
            broadcastHeldItemEquipment(session)
        }
        session.pendingPickedBlockStateId = 0
    }

    private fun consumePendingPickedBlockState(session: PlayerSession, hotbar: Int, itemId: Int): Int? {
        if (itemId < 0) return null
        val pending = session.pendingPickedBlockStateId
        if (pending <= 0) return null
        if (hotbar != session.selectedHotbarSlot) return null

        val normalizedPending = normalizePickedBlockStateId(pending)
        val expectedItemId = itemIdForState(normalizedPending)
        if (expectedItemId >= 0) {
            if (expectedItemId != itemId) {
                session.pendingPickedBlockStateId = 0
                return null
            }
            ItemBlockStateRegistry.learn(itemId, normalizedPending)
            return normalizedPending
        }

        val pendingBlockKey = BlockStateRegistry.parsedState(normalizedPending)?.blockKey
        val itemBlockKey = BlockStateRegistry.blockForItem(itemId)
        if (pendingBlockKey == null || itemBlockKey == null || pendingBlockKey != itemBlockKey) {
            session.pendingPickedBlockStateId = 0
            return null
        }
        ItemBlockStateRegistry.learn(itemId, normalizedPending)
        return normalizedPending
    }

    fun placeSelectedBlock(
        channelId: ChannelId,
        x: Int,
        y: Int,
        z: Int,
        hand: Int,
        faceId: Int,
        cursorX: Float,
        cursorY: Float,
        cursorZ: Float
    ) {
        val session = sessions[channelId] ?: return
        if (!canPlaceBlocks(session)) {
            if (PLACEMENT_DEBUG_LOG_ENABLED) {
                logger.info("Placement drop: player={} reason=game_mode mode={}", session.profile.username, session.gameMode)
            }
            return
        }
        val world = WorldManager.world(session.worldKey) ?: return
        if (!isChunkLoadedForSession(session, x, z)) {
            if (PLACEMENT_DEBUG_LOG_ENABLED) {
                logger.info(
                    "Placement drop: player={} reason=chunk_not_loaded pos=({}, {}, {})",
                    session.profile.username, x, y, z
                )
            }
            resyncBlockToPlayer(session, x, y, z, world.blockStateAt(x, y, z))
            return
        }
        // Reject placement into occupied space to keep server-authoritative collision rules sane.
        // (simple rule for now: only place into air)
        if (world.blockStateAt(x, y, z) != 0) {
            if (PLACEMENT_DEBUG_LOG_ENABLED) {
                logger.info(
                    "Placement drop: player={} reason=target_not_air pos=({}, {}, {}) stateId={}",
                    session.profile.username, x, y, z, world.blockStateAt(x, y, z)
                )
            }
            return
        }
        // Do not allow placing into the player's own collision box.
        if (isPlacementBlockedByPlayer(session, x, y, z)) {
            if (PLACEMENT_DEBUG_LOG_ENABLED) {
                logger.info(
                    "Placement drop: player={} reason=player_collision pos=({}, {}, {})",
                    session.profile.username, x, y, z
                )
            }
            return
        }

        val offhand = hand == 1
        val slot = session.selectedHotbarSlot.coerceIn(0, 8)
        val selectedItemId = if (offhand) session.offhandItemId else session.hotbarItemIds[slot]
        val selectedItemCount = if (offhand) session.offhandItemCount else session.hotbarItemCounts[slot]
        if (selectedItemCount <= 0 || selectedItemId < 0) {
            if (PLACEMENT_DEBUG_LOG_ENABLED) {
                logger.info(
                    "Placement drop: player={} reason=empty_hand offhand={} slot={} itemId={} count={}",
                    session.profile.username, offhand, slot, selectedItemId, selectedItemCount
                )
            }
            return
        }
        val baseStateId = if (offhand) {
            itemBlockStateId(selectedItemId)
        } else {
            val fromItem = itemBlockStateId(selectedItemId)
            if (fromItem > 0) fromItem else session.hotbarBlockStateIds[slot]
        }
        if (baseStateId <= 0) {
            if (PLACEMENT_DEBUG_LOG_ENABLED) {
                logger.info(
                    "Placement drop: player={} reason=no_block_state itemId={} offhand={} slot={}",
                    session.profile.username, selectedItemId, offhand, slot
                )
            }
            return
        }
        val blockStateId = normalizePrimaryPlacementStateId(resolvePlacementState(
            baseStateId = baseStateId,
            faceId = faceId,
            cursorX = cursorX,
            cursorY = cursorY,
            cursorZ = cursorZ,
            playerYaw = session.yaw
        ))
        val pairedPlacement = pairedPlacementFor(world, x, y, z, blockStateId)
        val logPlacementDebug = shouldLogPlacementDebug(blockStateId)
        if (logPlacementDebug) {
            logger.info(
                "Placement debug pre: player={} itemId={} offhand={} pos=({}, {}, {}) baseStateId={} resolvedStateId={} paired={}",
                session.profile.username,
                selectedItemId,
                offhand,
                x,
                y,
                z,
                baseStateId,
                blockStateId,
                pairedPlacement?.let { "(${it.x},${it.y},${it.z}) stateId=${it.stateId}" } ?: "none"
            )
        }
        if (pairedPlacement != null) {
            val pairedCurrentState = world.blockStateAt(pairedPlacement.x, pairedPlacement.y, pairedPlacement.z)
            if (pairedCurrentState != 0) {
                if (PLACEMENT_DEBUG_LOG_ENABLED) {
                    logger.info(
                        "Placement drop: player={} reason=paired_not_air paired=({}, {}, {}) stateId={}",
                        session.profile.username,
                        pairedPlacement.x,
                        pairedPlacement.y,
                        pairedPlacement.z,
                        pairedCurrentState
                    )
                }
                return
            }
        }
        setBlockAndBroadcast(
            session = session,
            x = x,
            y = y,
            z = z,
            stateId = blockStateId,
            blockEntityTypeId = if (offhand) -1 else session.hotbarBlockEntityTypeIds[slot],
            blockEntityNbtPayload = if (offhand) null else session.hotbarBlockEntityNbtPayloads[slot]
        )
        if (pairedPlacement != null) {
            setBlockAndBroadcast(
                session = session,
                x = pairedPlacement.x,
                y = pairedPlacement.y,
                z = pairedPlacement.z,
                stateId = pairedPlacement.stateId
            )
        }
        if (logPlacementDebug) {
            val lowerAfter = world.blockStateAt(x, y, z)
            val upperAfter = pairedPlacement?.let { world.blockStateAt(it.x, it.y, it.z) }
            logger.info(
                "Placement debug post: player={} pos=({}, {}, {}) lowerAfter={} upperAfter={} paired={}",
                session.profile.username,
                x,
                y,
                z,
                lowerAfter,
                upperAfter?.toString() ?: "n/a",
                pairedPlacement?.let { "(${it.x},${it.y},${it.z})" } ?: "none"
            )
        }
        if (session.gameMode != GAME_MODE_CREATIVE) {
            consumePlacedItem(session, hand)
        }
    }

    fun dropSelectedItem(channelId: ChannelId, dropStack: Boolean) {
        val session = sessions[channelId] ?: return
        val slot = session.selectedHotbarSlot.coerceIn(0, 8)
        val itemId = session.hotbarItemIds[slot]
        val count = session.hotbarItemCounts[slot]
        if (itemId < 0 || count <= 0) return

        val dropCount = if (dropStack) count else 1
        if (dropCount <= 0) return
        val spawned = spawnDroppedItemFromPlayer(
            session = session,
            itemId = itemId,
            itemCount = dropCount,
            dropStackMotion = dropStack
        )
        if (!spawned) return

        val remaining = count - dropCount
        if (remaining > 0) {
            session.hotbarItemCounts[slot] = remaining
        } else {
            session.hotbarItemIds[slot] = -1
            session.hotbarItemCounts[slot] = 0
            session.hotbarBlockStateIds[slot] = 0
            session.hotbarBlockEntityTypeIds[slot] = -1
            session.hotbarBlockEntityNbtPayloads[slot] = null
        }
        session.selectedBlockStateId = if (session.hotbarItemCounts[slot] > 0) session.hotbarBlockStateIds[slot] else 0

        val ctx = contexts[channelId]
        if (ctx != null && ctx.channel().isActive) {
            val inventoryStateId = nextInventoryStateId(session)
            val encoded = if (session.hotbarItemIds[slot] >= 0 && session.hotbarItemCounts[slot] > 0) {
                encodedItemStack(session.hotbarItemIds[slot], session.hotbarItemCounts[slot])
            } else {
                emptyItemStack()
            }
            ctx.writeAndFlush(
                PlayPackets.containerSetSlotPacket(
                    containerId = 0,
                    stateId = inventoryStateId,
                    slot = 36 + slot,
                    encodedItemStack = encoded
                )
            )
        }
        broadcastHeldItemEquipment(session)
    }

    private fun spawnDroppedItemFromPlayer(
        session: PlayerSession,
        itemId: Int,
        itemCount: Int,
        dropStackMotion: Boolean
    ): Boolean {
        if (itemId < 0 || itemCount <= 0) return false
        val world = WorldManager.world(session.worldKey) ?: return false

        val yawRad = Math.toRadians(session.yaw.toDouble())
        val pitchRad = Math.toRadians(session.pitch.toDouble())
        val cosPitch = cos(pitchRad)
        val dirX = -sin(yawRad) * cosPitch
        val dirY = -sin(pitchRad)
        val dirZ = cos(yawRad) * cosPitch

        val spawnX = session.x + (dirX * 0.4)
        val spawnY = session.y + 1.35
        val spawnZ = session.z + (dirZ * 0.4)
        val launchSpeed = if (dropStackMotion) 0.24 else 0.30
        val vx = dirX * launchSpeed
        val vy = dirY * launchSpeed + 0.16
        val vz = dirZ * launchSpeed

        return world.spawnDroppedItem(
            entityId = allocateEntityId(),
            itemId = itemId,
            itemCount = itemCount,
            x = spawnX,
            y = spawnY,
            z = spawnZ,
            vx = vx,
            vy = vy,
            vz = vz,
            pickupDelaySeconds = PLAYER_DROPPED_ITEM_PICKUP_DELAY_SECONDS
        )
    }

    private fun collectDroppedItemPickups(
        world: org.macaroon3145.world.World,
        worldSessions: List<PlayerSession>,
        outboundByContext: MutableMap<ChannelHandlerContext, MutableList<ByteArray>>
    ): List<DroppedItemPickupResult> {
        if (worldSessions.isEmpty()) return emptyList()
        val collectors = worldSessions
            .filter { it.gameMode != GAME_MODE_SPECTATOR }
            .sortedBy { it.entityId }
        if (collectors.isEmpty()) return emptyList()

        val candidatesByEntity = HashMap<Int, DroppedItemPickupCandidate>()

        for ((collectorOrder, collector) in collectors.withIndex()) {
            val playerChunkX = ChunkStreamingService.chunkXFromBlockX(collector.x)
            val playerChunkZ = ChunkStreamingService.chunkZFromBlockZ(collector.z)

            for (chunkX in (playerChunkX - 1)..(playerChunkX + 1)) {
                for (chunkZ in (playerChunkZ - 1)..(playerChunkZ + 1)) {
                    val items = world.droppedItemsInChunk(chunkX, chunkZ)
                    if (items.isEmpty()) continue
                    for (snapshot in items) {
                        if (snapshot.pickupDelaySeconds > DROPPED_ITEM_PICKUP_DELAY_EPSILON) continue
                        if (!canPlayerPickDroppedItem(collector, snapshot)) continue
                        if (!canAbsorbDroppedItem(collector, snapshot.itemId, snapshot.itemCount)) continue
                        val distanceSq = pickupDistanceSquared(collector, snapshot)
                        val current = candidatesByEntity[snapshot.entityId]
                        val shouldReplace = current == null ||
                            distanceSq < current.distanceSq - DROPPED_ITEM_PICKUP_DISTANCE_EPSILON ||
                            (
                                abs(distanceSq - current.distanceSq) <= DROPPED_ITEM_PICKUP_DISTANCE_EPSILON &&
                                    collectorOrder < current.collectorOrder
                                )
                        if (!shouldReplace) continue
                        candidatesByEntity[snapshot.entityId] = DroppedItemPickupCandidate(
                            snapshot = snapshot,
                            collector = collector,
                            collectorOrder = collectorOrder,
                            distanceSq = distanceSq
                        )
                    }
                }
            }
        }

        if (candidatesByEntity.isEmpty()) return emptyList()

        val removed = ArrayList<DroppedItemPickupResult>(candidatesByEntity.size)
        val orderedCandidates = candidatesByEntity.values
            .sortedWith(
                compareBy<DroppedItemPickupCandidate> { it.distanceSq }
                    .thenBy { it.collectorOrder }
                    .thenBy { it.snapshot.entityId }
            )

        for (candidate in orderedCandidates) {
            val snapshot = world.droppedItemSnapshot(candidate.snapshot.entityId) ?: continue
            val collector = candidate.collector
            if (snapshot.pickupDelaySeconds > DROPPED_ITEM_PICKUP_DELAY_EPSILON) continue
            if (!canPlayerPickDroppedItem(collector, snapshot)) continue
            if (!canAbsorbDroppedItem(collector, snapshot.itemId, snapshot.itemCount)) continue

            val removedSnapshot = world.removeDroppedItem(snapshot.entityId) ?: continue
            if (!absorbDroppedItemIntoHotbar(
                    session = collector,
                    itemId = removedSnapshot.itemId,
                    itemCount = removedSnapshot.itemCount,
                    outboundByContext = outboundByContext
                )
            ) {
                // Keep items safe if inventory changed between can-absorb check and apply.
                world.spawnDroppedItem(
                    entityId = allocateEntityId(),
                    itemId = removedSnapshot.itemId,
                    itemCount = removedSnapshot.itemCount,
                    x = removedSnapshot.x,
                    y = removedSnapshot.y,
                    z = removedSnapshot.z,
                    vx = removedSnapshot.vx,
                    vy = removedSnapshot.vy,
                    vz = removedSnapshot.vz,
                    pickupDelaySeconds = 0.0
                )
                continue
            }

            removed.add(
                DroppedItemPickupResult(
                    entityId = removedSnapshot.entityId,
                    collectorEntityId = collector.entityId,
                    pickupItemCount = removedSnapshot.itemCount,
                    x = removedSnapshot.x,
                    y = removedSnapshot.y,
                    z = removedSnapshot.z
                )
            )
        }
        return removed
    }

    private fun pickupDistanceSquared(session: PlayerSession, snapshot: DroppedItemSnapshot): Double {
        val playerCenterY = when {
            session.swimming -> session.y + 0.3
            session.sneaking -> session.y + 0.75
            else -> session.y + 0.9
        }
        val itemCenterY = snapshot.y + (DROPPED_ITEM_HEIGHT * 0.5)
        val dx = session.x - snapshot.x
        val dy = playerCenterY - itemCenterY
        val dz = session.z - snapshot.z
        return (dx * dx) + (dy * dy) + (dz * dz)
    }

    private fun isWithinPickupBroadcastRange(session: PlayerSession, pickup: DroppedItemPickupResult): Boolean {
        val dx = session.x - pickup.x
        val dy = session.y - pickup.y
        val dz = session.z - pickup.z
        return (dx * dx) + (dy * dy) + (dz * dz) <= DROPPED_ITEM_PICKUP_BROADCAST_DISTANCE_SQ
    }

    private fun canPlayerPickDroppedItem(session: PlayerSession, snapshot: DroppedItemSnapshot): Boolean {
        val playerHeight = when {
            session.swimming -> 0.6
            session.sneaking -> 1.5
            else -> 1.8
        }

        val playerMinX = session.x - PLAYER_HITBOX_HALF_WIDTH - DROPPED_ITEM_PICKUP_EXPAND_HORIZONTAL
        val playerMaxX = session.x + PLAYER_HITBOX_HALF_WIDTH + DROPPED_ITEM_PICKUP_EXPAND_HORIZONTAL
        val playerMinY = session.y - DROPPED_ITEM_PICKUP_EXPAND_DOWN
        val playerMaxY = session.y + playerHeight + DROPPED_ITEM_PICKUP_EXPAND_UP
        val playerMinZ = session.z - PLAYER_HITBOX_HALF_WIDTH - DROPPED_ITEM_PICKUP_EXPAND_HORIZONTAL
        val playerMaxZ = session.z + PLAYER_HITBOX_HALF_WIDTH + DROPPED_ITEM_PICKUP_EXPAND_HORIZONTAL

        val itemMinX = snapshot.x - DROPPED_ITEM_HALF_WIDTH
        val itemMaxX = snapshot.x + DROPPED_ITEM_HALF_WIDTH
        val itemMinY = snapshot.y
        val itemMaxY = snapshot.y + DROPPED_ITEM_HEIGHT
        val itemMinZ = snapshot.z - DROPPED_ITEM_HALF_WIDTH
        val itemMaxZ = snapshot.z + DROPPED_ITEM_HALF_WIDTH

        // Vanilla AABB intersection is strict: touching only at the border does not count.
        return playerMaxX > itemMinX &&
            playerMinX < itemMaxX &&
            playerMaxY > itemMinY &&
            playerMinY < itemMaxY &&
            playerMaxZ > itemMinZ &&
            playerMinZ < itemMaxZ
    }

    private fun canAbsorbDroppedItem(session: PlayerSession, itemId: Int, itemCount: Int): Boolean {
        if (itemId < 0 || itemCount <= 0) return false
        var remaining = itemCount

        for (slot in 0..8) {
            if (session.hotbarItemCounts[slot] <= 0 || session.hotbarItemIds[slot] != itemId) continue
            val free = MAX_HOTBAR_STACK_SIZE - session.hotbarItemCounts[slot]
            if (free <= 0) continue
            remaining -= free
            if (remaining <= 0) return true
        }
        for (slot in 0..8) {
            if (session.hotbarItemCounts[slot] > 0 && session.hotbarItemIds[slot] >= 0) continue
            remaining -= MAX_HOTBAR_STACK_SIZE
            if (remaining <= 0) return true
        }

        for (index in session.mainInventoryItemIds.indices) {
            if (session.mainInventoryItemCounts[index] <= 0 || session.mainInventoryItemIds[index] != itemId) continue
            val free = MAX_HOTBAR_STACK_SIZE - session.mainInventoryItemCounts[index]
            if (free <= 0) continue
            remaining -= free
            if (remaining <= 0) return true
        }
        for (index in session.mainInventoryItemIds.indices) {
            if (session.mainInventoryItemCounts[index] > 0 && session.mainInventoryItemIds[index] >= 0) continue
            remaining -= MAX_HOTBAR_STACK_SIZE
            if (remaining <= 0) return true
        }
        return false
    }

    private fun absorbDroppedItemIntoHotbar(
        session: PlayerSession,
        itemId: Int,
        itemCount: Int,
        outboundByContext: MutableMap<ChannelHandlerContext, MutableList<ByteArray>>
    ): Boolean {
        if (itemId < 0 || itemCount <= 0) return false
        var remaining = itemCount
        val changedHotbarSlots = ArrayList<Int>(3)
        val changedMainInventorySlots = ArrayList<Int>(3)
        val selectedSlot = session.selectedHotbarSlot.coerceIn(0, 8)
        val selectedBeforeItemId = session.hotbarItemIds[selectedSlot]
        val selectedBeforeCount = session.hotbarItemCounts[selectedSlot]

        for (slot in 0..8) {
            if (remaining <= 0) break
            if (session.hotbarItemCounts[slot] <= 0 || session.hotbarItemIds[slot] != itemId) continue
            val free = MAX_HOTBAR_STACK_SIZE - session.hotbarItemCounts[slot]
            if (free <= 0) continue
            val added = minOf(free, remaining)
            session.hotbarItemCounts[slot] += added
            remaining -= added
            changedHotbarSlots.add(slot)
        }

        for (slot in 0..8) {
            if (remaining <= 0) break
            if (session.hotbarItemCounts[slot] > 0 && session.hotbarItemIds[slot] >= 0) continue
            val added = minOf(MAX_HOTBAR_STACK_SIZE, remaining)
            session.hotbarItemIds[slot] = itemId
            session.hotbarItemCounts[slot] = added
            session.hotbarBlockStateIds[slot] = itemBlockStateId(itemId)
            session.hotbarBlockEntityTypeIds[slot] = -1
            session.hotbarBlockEntityNbtPayloads[slot] = null
            remaining -= added
            changedHotbarSlots.add(slot)
        }

        for (index in session.mainInventoryItemIds.indices) {
            if (remaining <= 0) break
            if (session.mainInventoryItemCounts[index] <= 0 || session.mainInventoryItemIds[index] != itemId) continue
            val free = MAX_HOTBAR_STACK_SIZE - session.mainInventoryItemCounts[index]
            if (free <= 0) continue
            val added = minOf(free, remaining)
            session.mainInventoryItemCounts[index] += added
            remaining -= added
            changedMainInventorySlots.add(index)
        }

        for (index in session.mainInventoryItemIds.indices) {
            if (remaining <= 0) break
            if (session.mainInventoryItemCounts[index] > 0 && session.mainInventoryItemIds[index] >= 0) continue
            val added = minOf(MAX_HOTBAR_STACK_SIZE, remaining)
            session.mainInventoryItemIds[index] = itemId
            session.mainInventoryItemCounts[index] = added
            remaining -= added
            changedMainInventorySlots.add(index)
        }

        if (remaining > 0) return false

        val ctx = contexts[session.channelId]
        if (ctx != null && ctx.channel().isActive) {
            for (slot in changedHotbarSlots) {
                val encoded = if (session.hotbarItemIds[slot] >= 0 && session.hotbarItemCounts[slot] > 0) {
                    encodedItemStack(session.hotbarItemIds[slot], session.hotbarItemCounts[slot])
                } else {
                    emptyItemStack()
                }
                enqueueDroppedItemPacket(
                    outboundByContext,
                    ctx,
                    PlayPackets.containerSetSlotPacket(
                        containerId = 0,
                        stateId = nextInventoryStateId(session),
                        slot = 36 + slot,
                        encodedItemStack = encoded
                    )
                )
            }
            for (index in changedMainInventorySlots) {
                val inventorySlot = inventorySlotForMainInventoryIndex(index)
                val encoded = if (session.mainInventoryItemIds[index] >= 0 && session.mainInventoryItemCounts[index] > 0) {
                    encodedItemStack(session.mainInventoryItemIds[index], session.mainInventoryItemCounts[index])
                } else {
                    emptyItemStack()
                }
                enqueueDroppedItemPacket(
                    outboundByContext,
                    ctx,
                    PlayPackets.containerSetSlotPacket(
                        containerId = 0,
                        stateId = nextInventoryStateId(session),
                        slot = inventorySlot,
                        encodedItemStack = encoded
                    )
                )
            }
        }

        session.selectedBlockStateId = if (session.hotbarItemCounts[selectedSlot] > 0) {
            session.hotbarBlockStateIds[selectedSlot]
        } else {
            0
        }
        val selectedAfterItemId = session.hotbarItemIds[selectedSlot]
        val selectedAfterCount = session.hotbarItemCounts[selectedSlot]
        if (selectedBeforeItemId != selectedAfterItemId || selectedBeforeCount != selectedAfterCount) {
            broadcastHeldItemEquipment(session)
        }
        return true
    }

    fun swapMainHandWithOffhand(channelId: ChannelId) {
        val session = sessions[channelId] ?: return
        val slot = session.selectedHotbarSlot.coerceIn(0, 8)

        val mainItemId = session.hotbarItemIds[slot]
        val mainItemCount = session.hotbarItemCounts[slot]
        val offItemId = session.offhandItemId
        val offItemCount = session.offhandItemCount

        session.hotbarItemIds[slot] = offItemId
        session.hotbarItemCounts[slot] = offItemCount
        session.offhandItemId = mainItemId
        session.offhandItemCount = mainItemCount

        if (offItemId >= 0 && offItemCount > 0) {
            session.hotbarBlockStateIds[slot] = itemBlockStateId(offItemId)
        } else {
            session.hotbarBlockStateIds[slot] = 0
        }
        session.hotbarBlockEntityTypeIds[slot] = -1
        session.hotbarBlockEntityNbtPayloads[slot] = null
        session.selectedBlockStateId = if (session.hotbarItemCounts[slot] > 0) {
            session.hotbarBlockStateIds[slot]
        } else {
            0
        }

        val ctx = contexts[channelId]
        if (ctx != null && ctx.channel().isActive) {
            val stateId = nextInventoryStateId(session)
            val mainEncoded = if (session.hotbarItemIds[slot] >= 0 && session.hotbarItemCounts[slot] > 0) {
                encodedItemStack(session.hotbarItemIds[slot], session.hotbarItemCounts[slot])
            } else {
                emptyItemStack()
            }
            val offEncoded = if (session.offhandItemId >= 0 && session.offhandItemCount > 0) {
                encodedItemStack(session.offhandItemId, session.offhandItemCount)
            } else {
                emptyItemStack()
            }
            ctx.write(
                PlayPackets.containerSetSlotPacket(
                    containerId = 0,
                    stateId = stateId,
                    slot = 36 + slot,
                    encodedItemStack = mainEncoded
                )
            )
            ctx.writeAndFlush(
                PlayPackets.containerSetSlotPacket(
                    containerId = 0,
                    stateId = stateId,
                    slot = 45,
                    encodedItemStack = offEncoded
                )
            )
        }

        broadcastHeldItemEquipment(session)
    }

    private fun itemBlockStateId(itemId: Int): Int {
        if (itemId < 0) return 0
        return ItemBlockStateRegistry.blockStateIdForItem(itemId) ?: 0
    }

    private fun armorIndexForInventorySlot(slot: Int): Int {
        return when (slot) {
            8 -> 0 // boots
            7 -> 1 // leggings
            6 -> 2 // chestplate
            5 -> 3 // helmet
            else -> -1
        }
    }

    private fun mainInventoryIndexForInventorySlot(slot: Int): Int {
        if (slot !in 9..35) return -1
        return slot - 9
    }

    private fun inventorySlotForMainInventoryIndex(index: Int): Int {
        return 9 + index.coerceIn(0, 26)
    }

    private fun selectedHotbarEncodedItemStack(session: PlayerSession): ByteArray {
        val slot = session.selectedHotbarSlot.coerceIn(0, 8)
        val itemId = session.hotbarItemIds[slot]
        val count = session.hotbarItemCounts[slot]
        return if (itemId >= 0 && count > 0) {
            encodedItemStack(itemId, count)
        } else {
            emptyItemStack()
        }
    }

    private fun armorEncodedItemStack(session: PlayerSession, armorIndex: Int): ByteArray {
        if (armorIndex !in 0..3) return emptyItemStack()
        val itemId = session.armorItemIds[armorIndex]
        val count = session.armorItemCounts[armorIndex]
        return if (itemId >= 0 && count > 0) {
            encodedItemStack(itemId, count)
        } else {
            emptyItemStack()
        }
    }

    private fun offhandEncodedItemStack(session: PlayerSession): ByteArray {
        val itemId = session.offhandItemId
        val count = session.offhandItemCount
        return if (itemId >= 0 && count > 0) {
            encodedItemStack(itemId, count)
        } else {
            emptyItemStack()
        }
    }

    private fun equipmentEntries(session: PlayerSession): List<PlayPackets.EquipmentEntry> {
        return listOf(
            PlayPackets.EquipmentEntry(slot = 0, encodedItemStack = selectedHotbarEncodedItemStack(session)),
            PlayPackets.EquipmentEntry(slot = 1, encodedItemStack = offhandEncodedItemStack(session)),
            PlayPackets.EquipmentEntry(slot = 2, encodedItemStack = armorEncodedItemStack(session, 0)),
            PlayPackets.EquipmentEntry(slot = 3, encodedItemStack = armorEncodedItemStack(session, 1)),
            PlayPackets.EquipmentEntry(slot = 4, encodedItemStack = armorEncodedItemStack(session, 2)),
            PlayPackets.EquipmentEntry(slot = 5, encodedItemStack = armorEncodedItemStack(session, 3))
        )
    }

    private fun sendHeldItemEquipment(session: PlayerSession, ctx: ChannelHandlerContext, flush: Boolean = true) {
        val packet = PlayPackets.entityEquipmentPacket(session.entityId, equipmentEntries(session))
        if (flush) {
            ctx.writeAndFlush(packet)
        } else {
            ctx.write(packet)
        }
    }

    private fun broadcastHeldItemEquipment(session: PlayerSession) {
        val packet = PlayPackets.entityEquipmentPacket(session.entityId, equipmentEntries(session))
        for ((id, other) in sessions) {
            if (id == session.channelId) continue
            if (other.worldKey != session.worldKey) continue
            val ctx = contexts[id] ?: continue
            if (!ctx.channel().isActive) continue
            ctx.writeAndFlush(packet)
        }
    }

    private fun syncDroppedItemSnapshot(
        snapshot: DroppedItemSnapshot,
        worldSessions: List<PlayerSession>,
        outboundByContext: MutableMap<ChannelHandlerContext, MutableList<ByteArray>>,
        sendPositionUpdate: Boolean,
        deltaSeconds: Double
    ) {
        val chunkPos = snapshot.chunkPos
        var removePacket: ByteArray? = null
        val (syncVx, syncVy, syncVz) = droppedItemNetworkVelocity(snapshot)

        for (session in worldSessions) {
            val shouldBeVisible = session.loadedChunks.contains(chunkPos)
            val ctx = contexts[session.channelId] ?: continue
            if (!ctx.channel().isActive) continue

            if (shouldBeVisible) {
                if (session.visibleDroppedItemEntityIds.add(snapshot.entityId)) {
                    queueDroppedItemSpawnPackets(
                        outboundByContext = outboundByContext,
                        ctx = ctx,
                        session = session,
                        snapshot = snapshot,
                        syncVx = syncVx,
                        syncVy = syncVy,
                        syncVz = syncVz
                    )
                    continue
                }
                if (sendPositionUpdate) {
                    val movementPackets = droppedItemMovementPackets(
                        session = session,
                        snapshot = snapshot,
                        syncVx = syncVx,
                        syncVy = syncVy,
                        syncVz = syncVz,
                        deltaSeconds = deltaSeconds
                    )
                    if (movementPackets.isNotEmpty()) {
                        for (packet in movementPackets) {
                            enqueueDroppedItemPacket(outboundByContext, ctx, packet)
                        }
                    }
                }
                continue
            }

            if (session.visibleDroppedItemEntityIds.remove(snapshot.entityId)) {
                session.droppedItemTrackerStates.remove(snapshot.entityId)
                val packet = removePacket ?: PlayPackets.removeEntitiesPacket(intArrayOf(snapshot.entityId)).also {
                    removePacket = it
                }
                enqueueDroppedItemPacket(outboundByContext, ctx, packet)
            }
        }
    }

    private fun enqueueDroppedItemPacket(
        outboundByContext: MutableMap<ChannelHandlerContext, MutableList<ByteArray>>,
        ctx: ChannelHandlerContext,
        packet: ByteArray
    ) {
        outboundByContext
            .computeIfAbsent(ctx) { ArrayList(4) }
            .add(packet)
    }

    private fun queueDroppedItemSpawnPackets(
        outboundByContext: MutableMap<ChannelHandlerContext, MutableList<ByteArray>>,
        ctx: ChannelHandlerContext,
        session: PlayerSession,
        snapshot: DroppedItemSnapshot,
        syncVx: Double,
        syncVy: Double,
        syncVz: Double
    ) {
        enqueueDroppedItemPacket(
            outboundByContext,
            ctx,
            PlayPackets.addEntityPacket(
                entityId = snapshot.entityId,
                uuid = snapshot.uuid,
                entityTypeId = PlayPackets.itemEntityTypeId(),
                x = snapshot.x,
                y = snapshot.y,
                z = snapshot.z,
                yaw = 0f,
                pitch = 0f,
                objectData = 1
            )
        )
        enqueueDroppedItemPacket(
            outboundByContext,
            ctx,
            PlayPackets.itemEntityMetadataPacket(
                entityId = snapshot.entityId,
                encodedItemStack = PlayPackets.encodeItemStack(snapshot.itemId, snapshot.itemCount)
            )
        )
        enqueueDroppedItemPacket(
            outboundByContext,
            ctx,
            PlayPackets.entityVelocityPacket(
                entityId = snapshot.entityId,
                vx = syncVx,
                vy = syncVy,
                vz = syncVz
            )
        )
        session.droppedItemTrackerStates[snapshot.entityId] = newDroppedItemTrackerState(snapshot, syncVx, syncVy, syncVz)
    }

    private fun writeDroppedItemSpawnPackets(session: PlayerSession, ctx: ChannelHandlerContext, snapshot: DroppedItemSnapshot) {
        val (syncVx, syncVy, syncVz) = droppedItemNetworkVelocity(snapshot)
        ctx.write(
            PlayPackets.addEntityPacket(
                entityId = snapshot.entityId,
                uuid = snapshot.uuid,
                entityTypeId = PlayPackets.itemEntityTypeId(),
                x = snapshot.x,
                y = snapshot.y,
                z = snapshot.z,
                yaw = 0f,
                pitch = 0f,
                objectData = 1
            )
        )
        ctx.write(
            PlayPackets.itemEntityMetadataPacket(
                entityId = snapshot.entityId,
                encodedItemStack = PlayPackets.encodeItemStack(snapshot.itemId, snapshot.itemCount)
            )
        )
        ctx.write(
            PlayPackets.entityVelocityPacket(
                entityId = snapshot.entityId,
                vx = syncVx,
                vy = syncVy,
                vz = syncVz
            )
        )
        session.droppedItemTrackerStates[snapshot.entityId] = newDroppedItemTrackerState(snapshot, syncVx, syncVy, syncVz)
    }

    private fun droppedItemMovementPackets(
        session: PlayerSession,
        snapshot: DroppedItemSnapshot,
        syncVx: Double,
        syncVy: Double,
        syncVz: Double,
        deltaSeconds: Double
    ): List<ByteArray> {
        val state = session.droppedItemTrackerStates.computeIfAbsent(snapshot.entityId) {
            newDroppedItemTrackerState(snapshot, syncVx, syncVy, syncVz)
        }
        state.secondsSinceHardSync = (state.secondsSinceHardSync + deltaSeconds).coerceAtMost(120.0)
        val playerTimeScale = ServerConfig.playerTimeScale

        val encodedX = encodeDroppedItemPosition4096(snapshot.x)
        val encodedY = encodeDroppedItemPosition4096(snapshot.y)
        val encodedZ = encodeDroppedItemPosition4096(snapshot.z)

        val dx = encodedX - state.encodedX4096
        val dy = encodedY - state.encodedY4096
        val dz = encodedZ - state.encodedZ4096
        val hasPositionDelta = dx != 0L || dy != 0L || dz != 0L
        val onGroundChanged = state.lastOnGround != snapshot.onGround

        val packets = ArrayList<ByteArray>(2)
        state.relativeMoveAccumulatorSeconds =
            (state.relativeMoveAccumulatorSeconds + deltaSeconds).coerceAtMost(MAX_DROPPED_ITEM_RELATIVE_MOVE_ACCUMULATOR_SECONDS)
        val relativeMoveIntervalSeconds = effectiveDroppedItemRelativeMoveIntervalSeconds(playerTimeScale)
        val relativeMoveDue = state.relativeMoveAccumulatorSeconds >= relativeMoveIntervalSeconds
        val shouldEmitMovement = hasPositionDelta || onGroundChanged
        var movementPacketSent = false
        var packetVxForMovement = syncVx
        var packetVyForMovement = syncVy
        var packetVzForMovement = syncVz

        if (shouldEmitMovement) {
            val requiresHardSync =
                dx < Short.MIN_VALUE.toLong() || dx > Short.MAX_VALUE.toLong() ||
                    dy < Short.MIN_VALUE.toLong() || dy > Short.MAX_VALUE.toLong() ||
                    dz < Short.MIN_VALUE.toLong() || dz > Short.MAX_VALUE.toLong() ||
                    state.secondsSinceHardSync >= MAX_DROPPED_ITEM_RELATIVE_SECONDS_BEFORE_HARD_SYNC

            if (requiresHardSync || relativeMoveDue || onGroundChanged) {
                if (requiresHardSync) {
                    packets.add(
                        PlayPackets.entityPositionSyncPacket(
                            entityId = snapshot.entityId,
                            x = snapshot.x,
                            y = snapshot.y,
                            z = snapshot.z,
                            vx = packetVxForMovement,
                            vy = packetVyForMovement,
                            vz = packetVzForMovement,
                            yaw = 0f,
                            pitch = 0f,
                            onGround = snapshot.onGround
                        )
                    )
                    state.secondsSinceHardSync = 0.0
                } else {
                    packets.add(
                        PlayPackets.entityRelativeMovePacket(
                            entityId = snapshot.entityId,
                            deltaX = dx.toInt(),
                            deltaY = dy.toInt(),
                            deltaZ = dz.toInt(),
                            onGround = snapshot.onGround
                        )
                    )
                }
                state.encodedX4096 = encodedX
                state.encodedY4096 = encodedY
                state.encodedZ4096 = encodedZ
                state.lastOnGround = snapshot.onGround
                state.relativeMoveAccumulatorSeconds = if (relativeMoveDue) {
                    (state.relativeMoveAccumulatorSeconds - relativeMoveIntervalSeconds).coerceAtLeast(0.0)
                } else {
                    state.relativeMoveAccumulatorSeconds
                }
                movementPacketSent = true
            }
        }

        val hadVelocity =
            abs(state.lastVx) > DROPPED_ITEM_VELOCITY_EPSILON ||
                abs(state.lastVy) > DROPPED_ITEM_VELOCITY_EPSILON ||
                abs(state.lastVz) > DROPPED_ITEM_VELOCITY_EPSILON
        val shouldSendVelocity = movementPacketSent || onGroundChanged
        if (shouldSendVelocity) {
            val hasVelocity =
                abs(packetVxForMovement) > DROPPED_ITEM_VELOCITY_EPSILON ||
                    abs(packetVyForMovement) > DROPPED_ITEM_VELOCITY_EPSILON ||
                    abs(packetVzForMovement) > DROPPED_ITEM_VELOCITY_EPSILON
            if (hasVelocity) {
                packets.add(
                    PlayPackets.entityVelocityPacket(
                        snapshot.entityId,
                        packetVxForMovement,
                        packetVyForMovement,
                        packetVzForMovement
                    )
                )
                state.lastVx = packetVxForMovement
                state.lastVy = packetVyForMovement
                state.lastVz = packetVzForMovement
            } else if (hadVelocity) {
                packets.add(PlayPackets.entityVelocityPacket(snapshot.entityId, 0.0, 0.0, 0.0))
                state.lastVx = 0.0
                state.lastVy = 0.0
                state.lastVz = 0.0
            }
        }
        return packets
    }

    private fun newDroppedItemTrackerState(
        snapshot: DroppedItemSnapshot,
        syncVx: Double,
        syncVy: Double,
        syncVz: Double
    ): DroppedItemTrackerState {
        return DroppedItemTrackerState(
            encodedX4096 = encodeDroppedItemPosition4096(snapshot.x),
            encodedY4096 = encodeDroppedItemPosition4096(snapshot.y),
            encodedZ4096 = encodeDroppedItemPosition4096(snapshot.z),
            lastVx = syncVx,
            lastVy = syncVy,
            lastVz = syncVz,
            lastOnGround = snapshot.onGround,
            secondsSinceHardSync = 0.0,
            relativeMoveAccumulatorSeconds = 0.0
        )
    }

    private fun droppedItemNetworkVelocity(snapshot: DroppedItemSnapshot): Triple<Double, Double, Double> {
        val effectiveTickScale = droppedItemNetworkVelocityTimeScale(ServerConfig.timeScale)
        val vx = sanitizeDroppedItemNetworkVelocity(snapshot.vx * effectiveTickScale)
        val vy = if (snapshot.onGround) {
            0.0
        } else {
            sanitizeDroppedItemNetworkVelocity(snapshot.vy * effectiveTickScale)
        }
        val vz = sanitizeDroppedItemNetworkVelocity(snapshot.vz * effectiveTickScale)
        return Triple(vx, vy, vz)
    }

    private fun droppedItemNetworkVelocityTimeScale(timeScale: Double): Double {
        if (timeScale <= 0.0 || timeScale.isNaN() || timeScale.isInfinite()) return 0.0
        return timeScale
    }

    private fun effectiveDroppedItemRelativeMoveIntervalSeconds(playerTimeScale: Double): Double {
        if (playerTimeScale <= 0.0) return DROPPED_ITEM_RELATIVE_MOVE_INTERVAL_SECONDS
        return DROPPED_ITEM_RELATIVE_MOVE_INTERVAL_SECONDS / playerTimeScale
    }

    private fun sanitizeDroppedItemNetworkVelocity(value: Double): Double {
        if (value.isNaN() || value.isInfinite()) return 0.0
        if (abs(value) < DROPPED_ITEM_VELOCITY_EPSILON) return 0.0
        return value
    }

    private fun encodeDroppedItemPosition4096(value: Double): Long {
        return (value * DROPPED_ITEM_RELATIVE_MOVE_SCALE).roundToLong()
    }

    private fun encodeRelativePosition4096(value: Double): Long {
        return (value * PLAYER_RELATIVE_MOVE_SCALE).roundToLong()
    }

    private fun sendDroppedItemsForChunk(
        session: PlayerSession,
        ctx: ChannelHandlerContext,
        snapshots: List<DroppedItemSnapshot>
    ) {
        if (snapshots.isEmpty()) return
        for (snapshot in snapshots) {
            if (!session.visibleDroppedItemEntityIds.add(snapshot.entityId)) continue
            writeDroppedItemSpawnPackets(session, ctx, snapshot)
        }
    }

    private fun hideDroppedItemsForChunk(
        session: PlayerSession,
        ctx: ChannelHandlerContext,
        chunkPos: ChunkPos,
        itemEntityIds: IntArray
    ) {
        if (itemEntityIds.isEmpty()) return
        val toRemove = ArrayList<Int>(itemEntityIds.size)
        for (id in itemEntityIds) {
            if (session.visibleDroppedItemEntityIds.remove(id)) {
                session.droppedItemTrackerStates.remove(id)
                toRemove.add(id)
            }
        }
        if (toRemove.isNotEmpty()) {
            ctx.write(PlayPackets.removeEntitiesPacket(toRemove.toIntArray()))
        }
    }

    private fun syncFallingBlockSnapshot(
        snapshot: FallingBlockSnapshot,
        worldSessions: List<PlayerSession>,
        outboundByContext: MutableMap<ChannelHandlerContext, MutableList<ByteArray>>,
        sendPositionUpdate: Boolean,
        deltaSeconds: Double
    ) {
        val chunkPos = snapshot.chunkPos
        var removePacket: ByteArray? = null
        val (syncVx, syncVy, syncVz) = fallingBlockNetworkVelocity(snapshot)
        for (session in worldSessions) {
            val shouldBeVisible = session.loadedChunks.contains(chunkPos)
            val ctx = contexts[session.channelId] ?: continue
            if (!ctx.channel().isActive) continue

            if (shouldBeVisible) {
                if (session.visibleFallingBlockEntityIds.add(snapshot.entityId)) {
                    queueFallingBlockSpawnPackets(
                        outboundByContext = outboundByContext,
                        ctx = ctx,
                        session = session,
                        snapshot = snapshot,
                        syncVx = syncVx,
                        syncVy = syncVy,
                        syncVz = syncVz
                    )
                    continue
                }
                if (sendPositionUpdate) {
                    val movementPackets = fallingBlockMovementPackets(
                        session = session,
                        snapshot = snapshot,
                        syncVx = syncVx,
                        syncVy = syncVy,
                        syncVz = syncVz,
                        deltaSeconds = deltaSeconds
                    )
                    for (packet in movementPackets) {
                        enqueueDroppedItemPacket(outboundByContext, ctx, packet)
                    }
                }
                continue
            }

            if (session.visibleFallingBlockEntityIds.remove(snapshot.entityId)) {
                session.fallingBlockTrackerStates.remove(snapshot.entityId)
                val packet = removePacket ?: PlayPackets.removeEntitiesPacket(intArrayOf(snapshot.entityId)).also {
                    removePacket = it
                }
                enqueueDroppedItemPacket(outboundByContext, ctx, packet)
            }
        }
    }

    private fun queueFallingBlockSpawnPackets(
        outboundByContext: MutableMap<ChannelHandlerContext, MutableList<ByteArray>>,
        ctx: ChannelHandlerContext,
        session: PlayerSession,
        snapshot: FallingBlockSnapshot,
        syncVx: Double,
        syncVy: Double,
        syncVz: Double
    ) {
        enqueueDroppedItemPacket(
            outboundByContext,
            ctx,
            PlayPackets.addEntityPacket(
                entityId = snapshot.entityId,
                uuid = snapshot.uuid,
                entityTypeId = PlayPackets.fallingBlockEntityTypeId(),
                x = snapshot.x,
                y = snapshot.y,
                z = snapshot.z,
                yaw = 0f,
                pitch = 0f,
                objectData = snapshot.blockStateId
            )
        )
        enqueueDroppedItemPacket(
            outboundByContext,
            ctx,
            PlayPackets.entityVelocityPacket(
                entityId = snapshot.entityId,
                vx = syncVx,
                vy = syncVy,
                vz = syncVz
            )
        )
        session.fallingBlockTrackerStates[snapshot.entityId] = newDroppedItemTrackerState(
            snapshot = DroppedItemSnapshot(
                entityId = snapshot.entityId,
                uuid = snapshot.uuid,
                itemId = 0,
                itemCount = 1,
                x = snapshot.x,
                y = snapshot.y,
                z = snapshot.z,
                vx = syncVx,
                vy = syncVy,
                vz = syncVz,
                pickupDelaySeconds = 0.0,
                onGround = snapshot.onGround,
                chunkPos = snapshot.chunkPos
            ),
            syncVx = syncVx,
            syncVy = syncVy,
            syncVz = syncVz
        )
    }

    private fun fallingBlockMovementPackets(
        session: PlayerSession,
        snapshot: FallingBlockSnapshot,
        syncVx: Double,
        syncVy: Double,
        syncVz: Double,
        deltaSeconds: Double
    ): List<ByteArray> {
        val state = session.fallingBlockTrackerStates.computeIfAbsent(snapshot.entityId) {
            newDroppedItemTrackerState(
                snapshot = DroppedItemSnapshot(
                    entityId = snapshot.entityId,
                    uuid = snapshot.uuid,
                    itemId = 0,
                    itemCount = 1,
                    x = snapshot.x,
                    y = snapshot.y,
                    z = snapshot.z,
                    vx = syncVx,
                    vy = syncVy,
                    vz = syncVz,
                    pickupDelaySeconds = 0.0,
                    onGround = snapshot.onGround,
                    chunkPos = snapshot.chunkPos
                ),
                syncVx = syncVx,
                syncVy = syncVy,
                syncVz = syncVz
            )
        }
        state.secondsSinceHardSync = (state.secondsSinceHardSync + deltaSeconds).coerceAtMost(120.0)
        val playerTimeScale = ServerConfig.playerTimeScale
        val encodedX = encodeDroppedItemPosition4096(snapshot.x)
        val encodedY = encodeDroppedItemPosition4096(snapshot.y)
        val encodedZ = encodeDroppedItemPosition4096(snapshot.z)
        val dx = encodedX - state.encodedX4096
        val dy = encodedY - state.encodedY4096
        val dz = encodedZ - state.encodedZ4096
        val hasPositionDelta = dx != 0L || dy != 0L || dz != 0L
        val onGroundChanged = state.lastOnGround != snapshot.onGround

        val packets = ArrayList<ByteArray>(2)
        state.relativeMoveAccumulatorSeconds =
            (state.relativeMoveAccumulatorSeconds + deltaSeconds).coerceAtMost(MAX_DROPPED_ITEM_RELATIVE_MOVE_ACCUMULATOR_SECONDS)
        val relativeMoveIntervalSeconds = effectiveDroppedItemRelativeMoveIntervalSeconds(playerTimeScale)
        val relativeMoveDue = state.relativeMoveAccumulatorSeconds >= relativeMoveIntervalSeconds
        val shouldEmitMovement = hasPositionDelta || onGroundChanged
        var movementPacketSent = false

        if (shouldEmitMovement) {
            val requiresHardSync =
                dx < Short.MIN_VALUE.toLong() || dx > Short.MAX_VALUE.toLong() ||
                    dy < Short.MIN_VALUE.toLong() || dy > Short.MAX_VALUE.toLong() ||
                    dz < Short.MIN_VALUE.toLong() || dz > Short.MAX_VALUE.toLong() ||
                    state.secondsSinceHardSync >= MAX_DROPPED_ITEM_RELATIVE_SECONDS_BEFORE_HARD_SYNC
            if (requiresHardSync || relativeMoveDue || onGroundChanged) {
                if (requiresHardSync) {
                    packets.add(
                        PlayPackets.entityPositionSyncPacket(
                            entityId = snapshot.entityId,
                            x = snapshot.x,
                            y = snapshot.y,
                            z = snapshot.z,
                            vx = syncVx,
                            vy = syncVy,
                            vz = syncVz,
                            yaw = 0f,
                            pitch = 0f,
                            onGround = snapshot.onGround
                        )
                    )
                    state.secondsSinceHardSync = 0.0
                } else {
                    packets.add(
                        PlayPackets.entityRelativeMovePacket(
                            entityId = snapshot.entityId,
                            deltaX = dx.toInt(),
                            deltaY = dy.toInt(),
                            deltaZ = dz.toInt(),
                            onGround = snapshot.onGround
                        )
                    )
                }
                state.encodedX4096 = encodedX
                state.encodedY4096 = encodedY
                state.encodedZ4096 = encodedZ
                state.lastOnGround = snapshot.onGround
                state.relativeMoveAccumulatorSeconds = if (relativeMoveDue) {
                    (state.relativeMoveAccumulatorSeconds - relativeMoveIntervalSeconds).coerceAtLeast(0.0)
                } else {
                    state.relativeMoveAccumulatorSeconds
                }
                movementPacketSent = true
            }
        }

        val hadVelocity =
            abs(state.lastVx) > DROPPED_ITEM_VELOCITY_EPSILON ||
                abs(state.lastVy) > DROPPED_ITEM_VELOCITY_EPSILON ||
                abs(state.lastVz) > DROPPED_ITEM_VELOCITY_EPSILON
        if (movementPacketSent || onGroundChanged) {
            val hasVelocity =
                abs(syncVx) > DROPPED_ITEM_VELOCITY_EPSILON ||
                    abs(syncVy) > DROPPED_ITEM_VELOCITY_EPSILON ||
                    abs(syncVz) > DROPPED_ITEM_VELOCITY_EPSILON
            if (hasVelocity) {
                packets.add(PlayPackets.entityVelocityPacket(snapshot.entityId, syncVx, syncVy, syncVz))
                state.lastVx = syncVx
                state.lastVy = syncVy
                state.lastVz = syncVz
            } else if (hadVelocity) {
                packets.add(PlayPackets.entityVelocityPacket(snapshot.entityId, 0.0, 0.0, 0.0))
                state.lastVx = 0.0
                state.lastVy = 0.0
                state.lastVz = 0.0
            }
        }
        return packets
    }

    private fun fallingBlockNetworkVelocity(snapshot: FallingBlockSnapshot): Triple<Double, Double, Double> {
        val effectiveTickScale = droppedItemNetworkVelocityTimeScale(ServerConfig.timeScale)
        val vx = sanitizeDroppedItemNetworkVelocity(snapshot.vx * effectiveTickScale)
        val vy = if (snapshot.onGround) 0.0 else sanitizeDroppedItemNetworkVelocity(snapshot.vy * effectiveTickScale)
        val vz = sanitizeDroppedItemNetworkVelocity(snapshot.vz * effectiveTickScale)
        return Triple(vx, vy, vz)
    }

    private fun writeFallingBlockSpawnPackets(session: PlayerSession, ctx: ChannelHandlerContext, snapshot: FallingBlockSnapshot) {
        val (syncVx, syncVy, syncVz) = fallingBlockNetworkVelocity(snapshot)
        ctx.write(
            PlayPackets.addEntityPacket(
                entityId = snapshot.entityId,
                uuid = snapshot.uuid,
                entityTypeId = PlayPackets.fallingBlockEntityTypeId(),
                x = snapshot.x,
                y = snapshot.y,
                z = snapshot.z,
                yaw = 0f,
                pitch = 0f,
                objectData = snapshot.blockStateId
            )
        )
        ctx.write(PlayPackets.entityVelocityPacket(snapshot.entityId, syncVx, syncVy, syncVz))
        session.fallingBlockTrackerStates[snapshot.entityId] = newDroppedItemTrackerState(
            snapshot = DroppedItemSnapshot(
                entityId = snapshot.entityId,
                uuid = snapshot.uuid,
                itemId = 0,
                itemCount = 1,
                x = snapshot.x,
                y = snapshot.y,
                z = snapshot.z,
                vx = syncVx,
                vy = syncVy,
                vz = syncVz,
                pickupDelaySeconds = 0.0,
                onGround = snapshot.onGround,
                chunkPos = snapshot.chunkPos
            ),
            syncVx = syncVx,
            syncVy = syncVy,
            syncVz = syncVz
        )
    }

    private fun sendFallingBlocksForChunk(
        session: PlayerSession,
        ctx: ChannelHandlerContext,
        snapshots: List<FallingBlockSnapshot>
    ) {
        if (snapshots.isEmpty()) return
        for (snapshot in snapshots) {
            if (!session.visibleFallingBlockEntityIds.add(snapshot.entityId)) continue
            writeFallingBlockSpawnPackets(session, ctx, snapshot)
        }
    }

    private fun hideFallingBlocksForChunk(
        session: PlayerSession,
        ctx: ChannelHandlerContext,
        chunkPos: ChunkPos,
        entityIds: IntArray
    ) {
        if (entityIds.isEmpty()) return
        val toRemove = ArrayList<Int>(entityIds.size)
        for (id in entityIds) {
            if (session.visibleFallingBlockEntityIds.remove(id)) {
                session.fallingBlockTrackerStates.remove(id)
                toRemove.add(id)
            }
        }
        if (toRemove.isNotEmpty()) {
            ctx.write(PlayPackets.removeEntitiesPacket(toRemove.toIntArray()))
        }
    }

    private fun resolvePlacementState(
        baseStateId: Int,
        faceId: Int,
        cursorX: Float,
        cursorY: Float,
        cursorZ: Float,
        playerYaw: Float
    ): Int {
        val parsed = BlockStateRegistry.parsedState(baseStateId) ?: return baseStateId
        if (parsed.properties.isEmpty()) return baseStateId

        val props = HashMap(parsed.properties)
        val blockKey = parsed.blockKey

        if (props.containsKey("axis")) {
            props["axis"] = when (faceId) {
                0, 1 -> "y"
                2, 3 -> "z"
                4, 5 -> "x"
                else -> props["axis"] ?: "y"
            }
        }

        if (props.containsKey("horizontal_facing")) {
            props["horizontal_facing"] = yawToHorizontalFacing(playerYaw)
        }

        if (props.containsKey("facing")) {
            val values = BlockStateRegistry.propertyValues(blockKey, "facing")
            val face = when (faceId) {
                0 -> "down"
                1 -> "up"
                2 -> "north"
                3 -> "south"
                4 -> "west"
                5 -> "east"
                else -> null
            }
            val horizontal = yawToHorizontalFacing(playerYaw)
            props["facing"] = when {
                face != null && values.contains(face) -> face
                values.contains(horizontal) -> horizontal
                else -> props["facing"] ?: horizontal
            }
        }

        if (props.containsKey("face")) {
            props["face"] = when (faceId) {
                0 -> "ceiling"
                1 -> "floor"
                else -> "wall"
            }
        }

        if (props.containsKey("half")) {
            val values = BlockStateRegistry.propertyValues(blockKey, "half")
            if (values.contains("top") && values.contains("bottom")) {
                val top = when (faceId) {
                    0 -> true
                    1 -> false
                    else -> cursorY > 0.5f
                }
                props["half"] = if (top) "top" else "bottom"
            }
        }

        if (props.containsKey("type")) {
            val values = BlockStateRegistry.propertyValues(blockKey, "type")
            if (values.contains("top") && values.contains("bottom")) {
                val top = when (faceId) {
                    0 -> true
                    1 -> false
                    else -> cursorY > 0.5f
                }
                props["type"] = if (top) "top" else "bottom"
            }
        }

        return BlockStateRegistry.stateId(blockKey, props) ?: baseStateId
    }

    private fun yawToHorizontalFacing(yaw: Float): String {
        val normalized = ((yaw % 360f) + 360f) % 360f
        val quadrant = kotlin.math.floor((normalized / 90f) + 0.5f).toInt() and 3
        return when (quadrant) {
            0 -> "south"
            1 -> "west"
            2 -> "north"
            else -> "east"
        }
    }

    fun breakBlock(channelId: ChannelId, x: Int, y: Int, z: Int) {
        val session = sessions[channelId] ?: return
        val world = WorldManager.world(session.worldKey) ?: return
        if (!isChunkLoadedForSession(session, x, z)) {
            resyncBlockToPlayer(session, x, y, z, world.blockStateAt(x, y, z))
            return
        }
        if (session.gameMode == GAME_MODE_CREATIVE) {
            val slot = session.selectedHotbarSlot.coerceIn(0, 8)
            val itemId = session.hotbarItemIds[slot]
            if (creativeNoBreakItemIds.contains(itemId)) {
                resyncBlockToPlayer(session, x, y, z, world.blockStateAt(x, y, z))
                return
            }
        }
        val stateId = world.blockStateAt(x, y, z)
        if (stateId == 0) return
        val directPairRemovals = LinkedHashSet<BlockPos>(2)
        collectDirectVerticalPairRemovals(world, x, y, z, stateId, directPairRemovals)
        val heldItemId = session.hotbarItemIds[session.selectedHotbarSlot.coerceIn(0, 8)]
        val drops = if (session.gameMode == GAME_MODE_SURVIVAL) {
            resolveBlockDrops(world, stateId, heldItemId, x, y, z)
        } else {
            emptyList()
        }
        val removedPositions = LinkedHashSet<BlockPos>(directPairRemovals.size + 4)
        for (pos in directPairRemovals) {
            setBlockAndBroadcast(session, pos.x, pos.y, pos.z, 0)
            removedPositions.add(pos)
        }
        val additionallyRemoved = LinkedHashSet<BlockPos>()
        breakImmediateOrphanVerticalPairs(session, world, removedPositions, heldItemId, additionallyRemoved)
        breakDependentBlocks(session, world, removedPositions, heldItemId, additionallyRemoved)
        if (additionallyRemoved.isNotEmpty()) {
            removedPositions.addAll(additionallyRemoved)
        }
        resyncRemovedBlocksToBreaker(session, world, removedPositions)
        if (drops.isNotEmpty()) {
            spawnBrokenBlockDrops(session, x, y, z, drops)
        }
    }

    private fun resolveBlockDrops(
        world: org.macaroon3145.world.World,
        stateId: Int,
        heldItemId: Int,
        x: Int,
        y: Int,
        z: Int
    ): List<org.macaroon3145.world.VanillaDrop> {
        return runCatching {
            VanillaMiningRules.resolveDrops(world, stateId, heldItemId, x, y, z)
        }.onFailure { throwable ->
            logger.error(
                "Failed to resolve survival block drops for block at {} {} {} in {} (stateId={}, heldItemId={})",
                x,
                y,
                z,
                world.key,
                stateId,
                heldItemId,
                throwable
            )
        }.getOrDefault(emptyList())
    }

    fun updateCommandBlock(
        channelId: ChannelId,
        x: Int,
        y: Int,
        z: Int,
        command: String,
        trackOutput: Boolean,
        conditional: Boolean,
        automatic: Boolean
    ) {
        val session = sessions[channelId] ?: return
        if (!isChunkLoadedForSession(session, x, z)) return
        val typeId = BlockEntityTypeRegistry.idOf("minecraft:command_block") ?: return
        val payload = commandBlockNbtPayload(
            x = x,
            y = y,
            z = z,
            command = command,
            trackOutput = trackOutput,
            conditional = conditional,
            automatic = automatic
        )
        setBlockAndBroadcast(
            session = session,
            x = x,
            y = y,
            z = z,
            stateId = WorldManager.world(session.worldKey)?.blockStateAt(x, y, z) ?: return,
            blockEntityTypeId = typeId,
            blockEntityNbtPayload = payload
        )
    }

    private fun isChunkLoadedForSession(session: PlayerSession, blockX: Int, blockZ: Int): Boolean {
        return session.loadedChunks.contains(ChunkPos(blockX shr 4, blockZ shr 4))
    }

    private fun resyncBlockToPlayer(session: PlayerSession, x: Int, y: Int, z: Int, stateId: Int) {
        val ctx = contexts[session.channelId] ?: return
        if (!ctx.channel().isActive) return
        ctx.writeAndFlush(PlayPackets.blockChangePacket(x, y, z, stateId))
    }

    fun acknowledgeBlockChangedSequence(channelId: ChannelId, sequence: Int) {
        if (sequence < 0) return
        val ctx = contexts[channelId] ?: return
        if (!ctx.channel().isActive) return
        val packet = PlayPackets.acknowledgeBlockChangedPacket(sequence)
        ctx.executor().execute {
            if (ctx.channel().isActive) {
                ctx.writeAndFlush(packet)
            }
        }
    }

    private fun resyncRemovedBlocksToBreaker(
        session: PlayerSession,
        world: org.macaroon3145.world.World,
        removedPositions: Collection<BlockPos>
    ) {
        if (removedPositions.isEmpty()) return
        val ctx = contexts[session.channelId] ?: return
        if (!ctx.channel().isActive) return
        ctx.executor().execute {
            if (!ctx.channel().isActive) return@execute
            for (pos in removedPositions) {
                ctx.write(PlayPackets.blockChangePacket(pos.x, pos.y, pos.z, world.blockStateAt(pos.x, pos.y, pos.z)))
            }
            ctx.flush()
        }
        ctx.executor().schedule({
            if (!ctx.channel().isActive) return@schedule
            for (pos in removedPositions) {
                ctx.write(PlayPackets.blockChangePacket(pos.x, pos.y, pos.z, world.blockStateAt(pos.x, pos.y, pos.z)))
            }
            ctx.flush()
        }, 50L, TimeUnit.MILLISECONDS)
        ctx.executor().schedule({
            if (!ctx.channel().isActive) return@schedule
            for (pos in removedPositions) {
                ctx.write(PlayPackets.blockChangePacket(pos.x, pos.y, pos.z, world.blockStateAt(pos.x, pos.y, pos.z)))
            }
            ctx.flush()
        }, 250L, TimeUnit.MILLISECONDS)
        ctx.executor().schedule({
            if (!ctx.channel().isActive) return@schedule
            for (pos in removedPositions) {
                ctx.write(PlayPackets.blockChangePacket(pos.x, pos.y, pos.z, world.blockStateAt(pos.x, pos.y, pos.z)))
            }
            ctx.flush()
        }, 1000L, TimeUnit.MILLISECONDS)
    }

    private fun setBlockAndBroadcast(
        session: PlayerSession,
        x: Int,
        y: Int,
        z: Int,
        stateId: Int,
        blockEntityTypeId: Int = -1,
        blockEntityNbtPayload: ByteArray? = null
    ) {
        if (y !in -64..319) return
        val world = WorldManager.world(session.worldKey) ?: return
        val oldStateId = world.blockStateAt(x, y, z)
        world.setBlockState(x, y, z, stateId)
        if (stateId != 0 && stateId != oldStateId) {
            world.requestDroppedItemUnstuckAtBlock(x, y, z)
        }
        if (blockEntityTypeId >= 0 && blockEntityNbtPayload != null && blockEntityNbtPayload.isNotEmpty()) {
            world.setBlockEntity(
                x,
                y,
                z,
                org.macaroon3145.world.World.BlockEntityData(blockEntityTypeId, blockEntityNbtPayload.copyOf())
            )
        } else {
            world.setBlockEntity(x, y, z, null)
        }
        val chunk = ChunkPos(x shr 4, z shr 4)
        val packet = PlayPackets.blockChangePacket(x, y, z, stateId)
        val breakEffectPacket =
            if (stateId == 0 && oldStateId != 0) {
                // 2001: block break effect (sound + particles), data=broken block state id.
                PlayPackets.levelEventPacket(
                    eventId = 2001,
                    x = x,
                    y = y,
                    z = z,
                    data = oldStateId,
                    global = false
                )
            } else null
        val blockEntityPacket =
            if (blockEntityTypeId >= 0 && blockEntityNbtPayload != null && blockEntityNbtPayload.isNotEmpty()) {
                PlayPackets.blockEntityDataPacket(x, y, z, blockEntityTypeId, blockEntityNbtPayload)
            } else {
                null
            }
        for ((id, other) in sessions) {
            if (other.worldKey != session.worldKey) continue
            if (!other.loadedChunks.contains(chunk)) continue
            val ctx = contexts[id] ?: continue
            if (!ctx.channel().isActive) continue
            ctx.write(packet)
            if (breakEffectPacket != null && id != session.channelId) {
                ctx.write(breakEffectPacket)
            }
            if (blockEntityPacket != null) {
                ctx.write(blockEntityPacket)
            }
            ctx.flush()
        }
        triggerFallingBlocksNear(session, world, x, y, z)
    }

    private fun triggerFallingBlocksNear(session: PlayerSession, world: org.macaroon3145.world.World, x: Int, y: Int, z: Int) {
        triggerFallingBlockColumnFrom(session, world, x, y, z)
        if (y + 1 <= 319) {
            triggerFallingBlockColumnFrom(session, world, x, y + 1, z)
        }
    }

    private fun triggerFallingBlockColumnFrom(
        session: PlayerSession,
        world: org.macaroon3145.world.World,
        x: Int,
        startY: Int,
        z: Int
    ) {
        val maxY = (startY + MAX_FALLING_BLOCK_COLUMN_SCAN).coerceAtMost(319)
        var currentY = startY.coerceAtLeast(-64)
        while (currentY <= maxY) {
            val stateId = world.blockStateAt(x, currentY, z)
            if (stateId == 0) {
                currentY++
                continue
            }
            if (!isFallingGravityBlockState(stateId)) break
            if (!canFallingBlockPassThrough(world.blockStateAt(x, currentY - 1, z))) break
            if (!startFallingBlockEntity(session, world, x, currentY, z, stateId)) break
            currentY++
        }
    }

    private fun startFallingBlockEntity(
        session: PlayerSession,
        world: org.macaroon3145.world.World,
        x: Int,
        y: Int,
        z: Int,
        stateId: Int
    ): Boolean {
        val entityId = allocateEntityId()
        world.setBlockState(x, y, z, 0)
        world.setBlockEntity(x, y, z, null)
        val spawned = world.spawnFallingBlock(
            entityId = entityId,
            blockStateId = stateId,
            x = x + 0.5,
            y = y.toDouble(),
            z = z + 0.5
        )
        if (!spawned) {
            world.setBlockState(x, y, z, stateId)
            return false
        }
        val chunk = ChunkPos(x shr 4, z shr 4)
        val clearPacket = PlayPackets.blockChangePacket(x, y, z, 0)
        val snapshot = world.fallingBlockSnapshot(entityId) ?: return false
        for ((id, other) in sessions) {
            if (other.worldKey != session.worldKey) continue
            if (!other.loadedChunks.contains(chunk)) continue
            val ctx = contexts[id] ?: continue
            if (!ctx.channel().isActive) continue
            ctx.write(clearPacket)
            if (other.visibleFallingBlockEntityIds.add(entityId)) {
                writeFallingBlockSpawnPackets(other, ctx, snapshot)
            }
            ctx.flush()
        }
        return true
    }

    fun broadcastGameMode(gameMode: Int) {
        setAllPlayersGameMode(gameMode)
    }

    fun updateAndBroadcastPing(channelId: ChannelId, pingMs: Int) {
        val session = sessions[channelId] ?: return
        val clamped = pingMs.coerceIn(0, 60_000)
        if (session.pingMs == clamped) return
        session.pingMs = clamped

        val packet = PlayPackets.playerInfoLatencyUpdatePacket(session.profile.uuid, clamped)
        for ((id, other) in sessions) {
            if (other.worldKey != session.worldKey) continue
            val ctx = contexts[id] ?: continue
            if (!ctx.channel().isActive) continue
            ctx.writeAndFlush(packet)
        }
    }

    fun setPlayerGameMode(channelId: ChannelId, gameMode: Int) {
        val session = sessions[channelId] ?: return
        val ctx = contexts[channelId] ?: return
        val clamped = gameMode.coerceIn(0, 3)
        if (session.gameMode == clamped) return
        session.gameMode = clamped

        val ownPacket = PlayPackets.gameStateGameModePacket(clamped)
        val tabPacket = PlayPackets.playerInfoGameModeUpdatePacket(session.profile.uuid, clamped)
        if (ctx.channel().isActive) {
            ctx.writeAndFlush(ownPacket)
        }
        for ((id, other) in sessions) {
            if (other.worldKey != session.worldKey) continue
            val otherCtx = contexts[id] ?: continue
            if (!otherCtx.channel().isActive) continue
            otherCtx.writeAndFlush(tabPacket)
        }
    }

    fun setAllPlayersGameMode(gameMode: Int) {
        val clamped = gameMode.coerceIn(0, 3)
        val ownPacket = PlayPackets.gameStateGameModePacket(clamped)
        for ((id, session) in sessions) {
            session.gameMode = clamped
            val ctx = contexts[id] ?: continue
            if (!ctx.channel().isActive) continue
            ctx.executor().execute {
                if (ctx.channel().isActive) {
                    ctx.writeAndFlush(ownPacket)
                }
            }
        }
        val byWorld = sessions.values.groupBy { it.worldKey }
        for ((_, worldSessions) in byWorld) {
            val uuids = worldSessions.map { it.profile.uuid }
            for (uuid in uuids) {
                val tabPacket = PlayPackets.playerInfoGameModeUpdatePacket(uuid, clamped)
                for (target in worldSessions) {
                    val targetCtx = contexts[target.channelId] ?: continue
                    if (!targetCtx.channel().isActive) continue
                    targetCtx.write(tabPacket)
                }
            }
            for (target in worldSessions) {
                val targetCtx = contexts[target.channelId] ?: continue
                if (!targetCtx.channel().isActive) continue
                targetCtx.flush()
            }
        }
    }

    private fun decodeItemNetworkId(encodedItemStack: ByteArray): Int {
        if (encodedItemStack.isEmpty()) return -1
        val buf = Unpooled.wrappedBuffer(encodedItemStack)
        return try {
            val count = NetworkUtils.readVarInt(buf)
            if (count <= 0) return -1
            val rawItemId = NetworkUtils.readVarInt(buf)
            if (rawItemId < 0) -1 else rawItemId
        } catch (_: Throwable) {
            -1
        } finally {
            buf.release()
        }
    }

    private fun decodeItemCount(encodedItemStack: ByteArray): Int {
        if (encodedItemStack.isEmpty()) return 0
        val buf = Unpooled.wrappedBuffer(encodedItemStack)
        return try {
            NetworkUtils.readVarInt(buf).coerceAtLeast(0)
        } catch (_: Throwable) {
            0
        } finally {
            buf.release()
        }
    }

    private fun findHotbarSlotForPick(
        session: PlayerSession,
        stateId: Int,
        itemId: Int,
        blockEntityTypeId: Int,
        blockEntityNbtPayload: ByteArray?
    ): Int {
        val normalizedStateId = normalizePickedBlockStateId(stateId)
        var itemOnlyMatch = -1
        for (slot in 0..8) {
            if (session.hotbarItemCounts[slot] <= 0 || session.hotbarItemIds[slot] < 0) continue
            if (session.hotbarItemIds[slot] != itemId) continue
            if (blockEntityTypeId >= 0 && session.hotbarBlockEntityTypeIds[slot] != blockEntityTypeId) continue

            val slotStateId = normalizePickedBlockStateId(session.hotbarBlockStateIds[slot])
            if (slotStateId == normalizedStateId) {
                if (blockEntityTypeId < 0) return slot
                val existingNbt = session.hotbarBlockEntityNbtPayloads[slot]
                if (existingNbt == null && blockEntityNbtPayload == null) return slot
                if (existingNbt != null && blockEntityNbtPayload != null && existingNbt.contentEquals(blockEntityNbtPayload)) {
                    return slot
                }
                continue
            }

            if (itemOnlyMatch < 0) {
                if (blockEntityTypeId < 0) {
                    itemOnlyMatch = slot
                } else {
                    val existingNbt = session.hotbarBlockEntityNbtPayloads[slot]
                    if (existingNbt == null && blockEntityNbtPayload == null) {
                        itemOnlyMatch = slot
                    } else if (existingNbt != null && blockEntityNbtPayload != null && existingNbt.contentEquals(blockEntityNbtPayload)) {
                        itemOnlyMatch = slot
                    }
                }
            }
        }
        return itemOnlyMatch
    }

    private fun findPickInsertHotbarSlot(session: PlayerSession, selectedSlot: Int): Int {
        val selected = selectedSlot.coerceIn(0, 8)
        if (session.hotbarItemCounts[selected] <= 0 || session.hotbarItemIds[selected] < 0) {
            return selected
        }
        for (offset in 1..8) {
            val slot = (selected + offset) % 9
            if (session.hotbarItemCounts[slot] <= 0 || session.hotbarItemIds[slot] < 0) {
                return slot
            }
        }
        return selected
    }

    private fun itemIdForState(stateId: Int): Int {
        ItemBlockStateRegistry.itemIdForBlockState(stateId)?.let { return it }
        lowerHalfStateId(stateId)?.let { normalized ->
            ItemBlockStateRegistry.itemIdForBlockState(normalized)?.let { return it }
        }
        val parsed = BlockStateRegistry.parsedState(stateId) ?: return -1
        PICK_ITEM_FALLBACK_BLOCK_BY_BLOCK[parsed.blockKey]?.let { fallbackBlock ->
            BlockStateRegistry.itemIdForBlock(fallbackBlock)?.let { return it }
            RegistryCodec.entryIndex("minecraft:item", fallbackBlock)?.let { return it }
        }
        return BlockStateRegistry.itemIdForBlock(parsed.blockKey)
            ?: RegistryCodec.entryIndex("minecraft:item", parsed.blockKey)
            ?: -1
    }

    private fun canPlaceBlocks(session: PlayerSession): Boolean {
        return session.gameMode == GAME_MODE_SURVIVAL || session.gameMode == GAME_MODE_CREATIVE
    }

    private fun consumePlacedItem(session: PlayerSession, hand: Int) {
        val ctx = contexts[session.channelId]
        if (hand == 1) {
            if (session.offhandItemCount <= 0 || session.offhandItemId < 0) return
            session.offhandItemCount -= 1
            if (session.offhandItemCount <= 0) {
                session.offhandItemId = -1
                session.offhandItemCount = 0
            }
            if (ctx != null && ctx.channel().isActive) {
                val encoded = if (session.offhandItemId >= 0 && session.offhandItemCount > 0) {
                    encodedItemStack(session.offhandItemId, session.offhandItemCount)
                } else {
                    emptyItemStack()
                }
                ctx.writeAndFlush(
                    PlayPackets.containerSetSlotPacket(
                        containerId = 0,
                        stateId = nextInventoryStateId(session),
                        slot = 45,
                        encodedItemStack = encoded
                    )
                )
            }
            broadcastHeldItemEquipment(session)
            return
        }

        val slot = session.selectedHotbarSlot.coerceIn(0, 8)
        if (session.hotbarItemCounts[slot] <= 0 || session.hotbarItemIds[slot] < 0) return
        session.hotbarItemCounts[slot] -= 1
        if (session.hotbarItemCounts[slot] <= 0) {
            session.hotbarItemIds[slot] = -1
            session.hotbarItemCounts[slot] = 0
            session.hotbarBlockStateIds[slot] = 0
            session.hotbarBlockEntityTypeIds[slot] = -1
            session.hotbarBlockEntityNbtPayloads[slot] = null
        }
        session.selectedBlockStateId = if (session.hotbarItemCounts[slot] > 0) {
            session.hotbarBlockStateIds[slot]
        } else {
            0
        }

        if (ctx != null && ctx.channel().isActive) {
            val encoded = if (session.hotbarItemIds[slot] >= 0 && session.hotbarItemCounts[slot] > 0) {
                encodedItemStack(session.hotbarItemIds[slot], session.hotbarItemCounts[slot])
            } else {
                emptyItemStack()
            }
            ctx.writeAndFlush(
                PlayPackets.containerSetSlotPacket(
                    containerId = 0,
                    stateId = nextInventoryStateId(session),
                    slot = 36 + slot,
                    encodedItemStack = encoded
                )
            )
        }
        broadcastHeldItemEquipment(session)
    }

    private fun spawnBrokenBlockDrops(session: PlayerSession, x: Int, y: Int, z: Int, drops: List<org.macaroon3145.world.VanillaDrop>) {
        val world = WorldManager.world(session.worldKey) ?: return
        for (drop in drops) {
            if (drop.itemId < 0 || drop.count <= 0) continue
            val random = ThreadLocalRandom.current()
            world.spawnDroppedItem(
                entityId = allocateEntityId(),
                itemId = drop.itemId,
                itemCount = drop.count,
                x = x + 0.5,
                y = y + 0.375,
                z = z + 0.5,
                vx = random.nextDouble(BLOCK_DROP_HORIZONTAL_SPEED_MIN, BLOCK_DROP_HORIZONTAL_SPEED_MAX),
                vy = BLOCK_DROP_VERTICAL_SPEED,
                vz = random.nextDouble(BLOCK_DROP_HORIZONTAL_SPEED_MIN, BLOCK_DROP_HORIZONTAL_SPEED_MAX),
                pickupDelaySeconds = BLOCK_DROP_PICKUP_DELAY_SECONDS
            )
        }
    }

    private fun twoBlockUpperStateId(stateId: Int): Int {
        val parsed = BlockStateRegistry.parsedState(stateId) ?: return 0
        if (parsed.properties["half"] != "lower") return 0
        return verticalHalfCounterpartStateId(stateId) ?: 0
    }

    private fun lowerHalfStateId(stateId: Int): Int? {
        val parsed = BlockStateRegistry.parsedState(stateId) ?: return null
        if (parsed.properties["half"] != "upper") return null
        return verticalHalfCounterpartStateId(stateId)
    }

    private fun verticalHalfCounterpartStateId(stateId: Int): Int? {
        BlockStateRegistry.verticalHalfCounterpartStateId(stateId)?.let { return it }
        val parsed = BlockStateRegistry.parsedState(stateId) ?: return null
        val half = parsed.properties["half"] ?: return null
        val opposite = when (half) {
            "lower" -> "upper"
            "upper" -> "lower"
            else -> return null
        }
        val props = HashMap(parsed.properties)
        props["half"] = opposite
        return BlockStateRegistry.stateId(parsed.blockKey, props)
    }

    private fun normalizePickedBlockStateId(stateId: Int): Int {
        if (stateId <= 0) return stateId
        return lowerHalfStateId(stateId) ?: stateId
    }

    private fun shouldLogPlacementDebug(stateId: Int): Boolean {
        if (!PLACEMENT_DEBUG_LOG_ENABLED) return false
        val parsed = BlockStateRegistry.parsedState(stateId) ?: return false
        val half = parsed.properties["half"]
        val part = parsed.properties["part"]
        return half == "lower" || half == "upper" || part == "foot" || part == "head"
    }

    private fun normalizePrimaryPlacementStateId(stateId: Int): Int {
        if (stateId <= 0) return stateId
        val parsed = BlockStateRegistry.parsedState(stateId) ?: return stateId
        val props = HashMap(parsed.properties)
        var changed = false

        if (props["half"] == "upper") {
            val lowerProps = HashMap(props)
            lowerProps["half"] = "lower"
            val lowerStateId = BlockStateRegistry.stateId(parsed.blockKey, lowerProps)
            if (lowerStateId != null && verticalHalfCounterpartStateId(lowerStateId) != null) {
                props["half"] = "lower"
                changed = true
            }
        }

        if (parsed.blockKey in BED_BLOCK_KEYS && props["part"] != "foot") {
            props["part"] = "foot"
            changed = true
        }

        if (!changed) return stateId
        return BlockStateRegistry.stateId(parsed.blockKey, props) ?: stateId
    }

    private fun twoBlockPairedPos(
        world: org.macaroon3145.world.World,
        x: Int,
        y: Int,
        z: Int,
        stateId: Int
    ): BlockPos? {
        return verticalPair(world, x, y, z, stateId)?.secondary
    }

    private fun verticallyPairedBlockPos(
        world: org.macaroon3145.world.World,
        x: Int,
        y: Int,
        z: Int,
        stateId: Int
    ): BlockPos? {
        val counterpartStateId = verticalHalfCounterpartStateId(stateId) ?: return null
        val parsed = BlockStateRegistry.parsedState(stateId) ?: return null
        val half = parsed.properties["half"] ?: return null
        if (half != "lower" && half != "upper") return null
        val pairedY = if (half == "lower") y + 1 else y - 1
        if (pairedY !in -64..319) return null
        val pairedStateId = world.blockStateAt(x, pairedY, z)
        if (pairedStateId != counterpartStateId) return null
        return BlockPos(x, pairedY, z)
    }

    private fun collectDirectVerticalPairRemovals(
        world: org.macaroon3145.world.World,
        x: Int,
        y: Int,
        z: Int,
        stateId: Int,
        out: MutableCollection<BlockPos>
    ) {
        bedPair(world, x, y, z, stateId)?.let {
            out.add(it.primary)
            out.add(it.secondary)
            return
        }
        verticalPair(world, x, y, z, stateId)?.let {
            out.add(it.primary)
            out.add(it.secondary)
            return
        }
        out.add(BlockPos(x, y, z))
    }

    private fun bedPairedPos(
        world: org.macaroon3145.world.World,
        x: Int,
        y: Int,
        z: Int,
        stateId: Int
    ): BlockPos? {
        val parsed = BlockStateRegistry.parsedState(stateId) ?: return null
        if (parsed.blockKey !in BED_BLOCK_KEYS) return null
        val part = parsed.properties["part"] ?: return null
        val facing = parsed.properties["facing"] ?: return null
        val (dx, dz) = horizontalFacingOffset(facing) ?: return null
        val pairedX: Int
        val pairedZ: Int
        val expectedPart: String
        when (part) {
            "foot" -> {
                pairedX = x + dx
                pairedZ = z + dz
                expectedPart = "head"
            }
            "head" -> {
                pairedX = x - dx
                pairedZ = z - dz
                expectedPart = "foot"
            }
            else -> return null
        }
        val pairedStateId = world.blockStateAt(pairedX, y, pairedZ)
        val paired = BlockStateRegistry.parsedState(pairedStateId) ?: return null
        if (paired.blockKey != parsed.blockKey) return null
        if (paired.properties["part"] != expectedPart) return null
        return BlockPos(pairedX, y, pairedZ)
    }

    private data class BlockPair(
        val primary: BlockPos,
        val secondary: BlockPos
    )

    private fun verticalPair(
        world: org.macaroon3145.world.World,
        x: Int,
        y: Int,
        z: Int,
        stateId: Int
    ): BlockPair? {
        val parsed = BlockStateRegistry.parsedState(stateId) ?: return null
        val half = parsed.properties["half"] ?: return null
        val lowerY = when (half) {
            "lower" -> y
            "upper" -> y - 1
            else -> return null
        }
        val upperY = lowerY + 1
        if (lowerY !in -64..319 || upperY !in -64..319) return null
        val lowerStateId = world.blockStateAt(x, lowerY, z)
        val upperStateId = world.blockStateAt(x, upperY, z)
        val lower = BlockStateRegistry.parsedState(lowerStateId) ?: return null
        val upper = BlockStateRegistry.parsedState(upperStateId) ?: return null
        if (lower.blockKey != parsed.blockKey || upper.blockKey != parsed.blockKey) return null
        if (lower.properties["half"] != "lower" || upper.properties["half"] != "upper") return null
        return BlockPair(BlockPos(x, lowerY, z), BlockPos(x, upperY, z))
    }

    private fun bedPair(
        world: org.macaroon3145.world.World,
        x: Int,
        y: Int,
        z: Int,
        stateId: Int
    ): BlockPair? {
        val parsed = BlockStateRegistry.parsedState(stateId) ?: return null
        if (parsed.blockKey !in BED_BLOCK_KEYS) return null
        val part = parsed.properties["part"] ?: return null
        val facing = parsed.properties["facing"] ?: return null
        val (dx, dz) = horizontalFacingOffset(facing) ?: return null
        val footX: Int
        val footZ: Int
        val headX: Int
        val headZ: Int
        when (part) {
            "foot" -> {
                footX = x
                footZ = z
                headX = x + dx
                headZ = z + dz
            }
            "head" -> {
                footX = x - dx
                footZ = z - dz
                headX = x
                headZ = z
            }
            else -> return null
        }
        val footStateId = world.blockStateAt(footX, y, footZ)
        val headStateId = world.blockStateAt(headX, y, headZ)
        val foot = BlockStateRegistry.parsedState(footStateId) ?: return null
        val head = BlockStateRegistry.parsedState(headStateId) ?: return null
        if (foot.blockKey != parsed.blockKey || head.blockKey != parsed.blockKey) return null
        if (foot.properties["part"] != "foot" || head.properties["part"] != "head") return null
        return BlockPair(BlockPos(footX, y, footZ), BlockPos(headX, y, headZ))
    }

    private data class PairedPlacement(
        val x: Int,
        val y: Int,
        val z: Int,
        val stateId: Int
    )

    private fun pairedPlacementFor(
        world: org.macaroon3145.world.World,
        x: Int,
        y: Int,
        z: Int,
        stateId: Int
    ): PairedPlacement? {
        val parsed = BlockStateRegistry.parsedState(stateId) ?: return null
        if (parsed.properties["half"] == "lower") {
            val upperY = y + 1
            if (upperY !in -64..319) return null
            val upperStateId = twoBlockUpperStateId(stateId)
            if (upperStateId > 0) {
                return PairedPlacement(x, upperY, z, upperStateId)
            }
        }

        if (parsed.blockKey in BED_BLOCK_KEYS && parsed.properties["part"] == "foot") {
            val facing = parsed.properties["facing"] ?: return null
            val (dx, dz) = horizontalFacingOffset(facing) ?: return null
            val props = HashMap(parsed.properties)
            props["part"] = "head"
            val headStateId = BlockStateRegistry.stateId(parsed.blockKey, props) ?: return null
            return PairedPlacement(x + dx, y, z + dz, headStateId)
        }

        return null
    }

    private fun breakDependentBlocks(
        session: PlayerSession,
        world: org.macaroon3145.world.World,
        removedPositions: Collection<BlockPos>,
        heldItemId: Int,
        additionallyRemoved: MutableSet<BlockPos>
    ) {
        if (removedPositions.isEmpty()) return
        val pending = ArrayDeque<BlockPos>()
        val enqueued = HashSet<BlockPos>()
        val dependents = ArrayList<BlockPos>(2)
        for (removed in removedPositions) {
            enqueueDependentChecks(removed, pending, enqueued)
        }
        while (pending.isNotEmpty()) {
            val pos = pending.removeFirst()
            val stateId = world.blockStateAt(pos.x, pos.y, pos.z)
            if (stateId == 0) continue
            dependents.clear()
            appendDependentBlocksToBreak(world, pos.x, pos.y, pos.z, stateId, dependents)
            if (dependents.isEmpty()) continue
            for (dependent in dependents) {
                val dependentStateId = world.blockStateAt(dependent.x, dependent.y, dependent.z)
                if (dependentStateId == 0) continue
                val drops = if (session.gameMode == GAME_MODE_SURVIVAL || session.gameMode == GAME_MODE_CREATIVE) {
                    runCatching {
                        VanillaMiningRules.resolveDrops(
                            world,
                            dependentStateId,
                            heldItemId,
                            dependent.x,
                            dependent.y,
                            dependent.z
                        )
                    }.onFailure { throwable ->
                        logger.error(
                            "Failed to resolve dependent block drops for block at {} {} {} in {} (stateId={}, heldItemId={})",
                            dependent.x,
                            dependent.y,
                            dependent.z,
                            session.worldKey,
                            dependentStateId,
                            heldItemId,
                            throwable
                        )
                    }.getOrDefault(emptyList())
                } else {
                    emptyList()
                }
                setBlockAndBroadcast(session, dependent.x, dependent.y, dependent.z, 0)
                additionallyRemoved.add(dependent)
                if (drops.isNotEmpty()) {
                    spawnBrokenBlockDrops(session, dependent.x, dependent.y, dependent.z, drops)
                }
                enqueueDependentChecks(dependent, pending, enqueued)
            }
        }
    }

    private fun breakImmediateOrphanVerticalPairs(
        session: PlayerSession,
        world: org.macaroon3145.world.World,
        removedPositions: Collection<BlockPos>,
        heldItemId: Int,
        additionallyRemoved: MutableSet<BlockPos>
    ) {
        for (removed in removedPositions) {
            breakOrphanVerticalPairAt(session, world, BlockPos(removed.x, removed.y + 1, removed.z), heldItemId, additionallyRemoved)
            breakOrphanVerticalPairAt(session, world, BlockPos(removed.x, removed.y - 1, removed.z), heldItemId, additionallyRemoved)
        }
    }

    private fun breakOrphanVerticalPairAt(
        session: PlayerSession,
        world: org.macaroon3145.world.World,
        pos: BlockPos,
        heldItemId: Int,
        additionallyRemoved: MutableSet<BlockPos>
    ) {
        if (pos.y !in -64..319) return
        val stateId = world.blockStateAt(pos.x, pos.y, pos.z)
        if (stateId == 0) return
        if (!isBrokenVerticalPairBlock(world, pos.x, pos.y, pos.z, stateId)) return
        val drops = if (session.gameMode == GAME_MODE_SURVIVAL || session.gameMode == GAME_MODE_CREATIVE) {
            runCatching {
                VanillaMiningRules.resolveDrops(world, stateId, heldItemId, pos.x, pos.y, pos.z)
            }.onFailure { throwable ->
                logger.error(
                    "Failed to resolve orphan vertical pair drops for block at {} {} {} in {} (stateId={}, heldItemId={})",
                    pos.x,
                    pos.y,
                    pos.z,
                    session.worldKey,
                    stateId,
                    heldItemId,
                    throwable
                )
            }.getOrDefault(emptyList())
        } else {
            emptyList()
        }
        setBlockAndBroadcast(session, pos.x, pos.y, pos.z, 0)
        additionallyRemoved.add(pos)
        if (drops.isNotEmpty()) {
            spawnBrokenBlockDrops(session, pos.x, pos.y, pos.z, drops)
        }
    }

    private fun isBrokenVerticalPairBlock(
        world: org.macaroon3145.world.World,
        x: Int,
        y: Int,
        z: Int,
        stateId: Int
    ): Boolean {
        val parsed = BlockStateRegistry.parsedState(stateId) ?: return false
        val half = parsed.properties["half"] ?: return false
        if (half != "lower" && half != "upper") return false
        return verticallyPairedBlockPos(world, x, y, z, stateId) == null
    }

    private fun enqueueDependentChecks(
        center: BlockPos,
        pending: ArrayDeque<BlockPos>,
        enqueued: MutableSet<BlockPos>
    ) {
        enqueueDependentCheck(center.x, center.y + 1, center.z, pending, enqueued)
        enqueueDependentCheck(center.x, center.y - 1, center.z, pending, enqueued)
        enqueueDependentCheck(center.x + 1, center.y, center.z, pending, enqueued)
        enqueueDependentCheck(center.x - 1, center.y, center.z, pending, enqueued)
        enqueueDependentCheck(center.x, center.y, center.z + 1, pending, enqueued)
        enqueueDependentCheck(center.x, center.y, center.z - 1, pending, enqueued)
    }

    private fun enqueueDependentCheck(
        x: Int,
        y: Int,
        z: Int,
        pending: ArrayDeque<BlockPos>,
        enqueued: MutableSet<BlockPos>
    ) {
        if (y !in -64..319) return
        val candidate = BlockPos(x, y, z)
        if (enqueued.add(candidate)) {
            pending.addLast(candidate)
        }
    }

    private fun appendDependentBlocksToBreak(
        world: org.macaroon3145.world.World,
        x: Int,
        y: Int,
        z: Int,
        stateId: Int,
        out: MutableList<BlockPos>
    ) {
        if (!shouldBreakForMissingSupport(world, x, y, z, stateId)) return
        val primary = BlockPos(x, y, z)
        out.add(primary)
        val pair = verticallyPairedBlockPos(world, x, y, z, stateId)
        if (pair != null && pair != primary) {
            out.add(pair)
        }
    }

    private fun shouldBreakForMissingSupport(
        world: org.macaroon3145.world.World,
        x: Int,
        y: Int,
        z: Int,
        stateId: Int
    ): Boolean {
        val parsed = BlockStateRegistry.parsedState(stateId) ?: return false
        val blockKey = parsed.blockKey
        val half = parsed.properties["half"]
        if ((half == "lower" || half == "upper") &&
            verticalHalfCounterpartStateId(stateId) != null &&
            verticallyPairedBlockPos(world, x, y, z, stateId) == null
        ) {
            return true
        }

        if (half != "upper" && requiresSupportBelow(blockKey)) {
            val belowStateId = world.blockStateAt(x, y - 1, z)
            if (!isValidBelowSupportFor(blockKey, belowStateId)) {
                return true
            }
        }

        val face = parsed.properties["face"]
        val facing = parsed.properties["facing"]
        if (face != null && facing != null) {
            val attached = when (face) {
                "floor" -> BlockPos(x, y - 1, z)
                "ceiling" -> BlockPos(x, y + 1, z)
                "wall" -> attachedSupportPos(x, y, z, facing) ?: return true
                else -> null
            }
            if (attached != null && !isSupportBlock(world.blockStateAt(attached.x, attached.y, attached.z))) {
                return true
            }
        }

        if (blockKey in WALL_MOUNTED_BLOCK_KEYS) {
            val attached = attachedSupportPos(x, y, z, facing ?: return true) ?: return true
            if (!isSupportBlock(world.blockStateAt(attached.x, attached.y, attached.z))) {
                return true
            }
        }

        if (blockKey == "minecraft:vine") {
            if (!vineHasAnySupport(world, x, y, z, parsed.properties)) {
                return true
            }
        }

        return false
    }

    private fun requiresSupportBelow(blockKey: String): Boolean {
        return blockKey in BELOW_SUPPORT_BREAK_KEYS ||
            blockKey.endsWith("_sapling") ||
            blockKey.endsWith("_tulip") ||
            blockKey.endsWith("_orchid") ||
            blockKey.endsWith("_daisy") ||
            blockKey.endsWith("_bluet") ||
            blockKey.endsWith("_mushroom") ||
            blockKey.endsWith("_bush") ||
            blockKey.endsWith("_fungus")
    }

    private fun isValidBelowSupportFor(blockKey: String, belowStateId: Int): Boolean {
        val below = BlockStateRegistry.parsedState(belowStateId)
        val belowBlockKey = below?.blockKey
        if (blockKey in CROPS_REQUIRING_FARMLAND) {
            return belowBlockKey == "minecraft:farmland"
        }
        if (blockKey in GRASS_DECORATION_SUPPORT_KEYS) {
            return belowBlockKey in GRASS_LIKE_SUPPORT_KEYS
        }
        return isSupportBlock(belowStateId)
    }

    private fun isSupportBlock(stateId: Int): Boolean {
        if (stateId == 0) return false
        val parsed = BlockStateRegistry.parsedState(stateId) ?: return true
        return parsed.blockKey !in NON_SUPPORT_BLOCK_KEYS
    }

    private fun isInstantBreakBlock(stateId: Int): Boolean {
        if (stateId <= 0) return false
        val parsed = BlockStateRegistry.parsedState(stateId) ?: return false
        return parsed.blockKey in INSTANT_BREAK_BLOCK_KEYS
    }

    private fun isFallingGravityBlockState(stateId: Int): Boolean {
        if (stateId <= 0) return false
        return fallingGravityStateCache.computeIfAbsent(stateId) { id ->
            val blockKey = BlockStateRegistry.parsedState(id)?.blockKey ?: return@computeIfAbsent false
            blockKey in FALLING_BLOCK_KEYS
        }
    }

    private fun canFallingBlockPassThrough(stateId: Int): Boolean {
        if (stateId == 0) return true
        return fallingPassThroughStateCache.computeIfAbsent(stateId) { id ->
            val blockKey = BlockStateRegistry.parsedState(id)?.blockKey ?: return@computeIfAbsent false
            blockKey in FALLING_BLOCK_PASS_THROUGH_KEYS
        }
    }

    private fun attachedSupportPos(x: Int, y: Int, z: Int, facing: String): BlockPos? {
        return when (facing) {
            "north" -> BlockPos(x, y, z + 1)
            "south" -> BlockPos(x, y, z - 1)
            "east" -> BlockPos(x - 1, y, z)
            "west" -> BlockPos(x + 1, y, z)
            "up" -> BlockPos(x, y - 1, z)
            "down" -> BlockPos(x, y + 1, z)
            else -> null
        }
    }

    private fun horizontalFacingOffset(facing: String): Pair<Int, Int>? {
        return when (facing) {
            "north" -> 0 to -1
            "south" -> 0 to 1
            "east" -> 1 to 0
            "west" -> -1 to 0
            else -> null
        }
    }

    private fun vineHasAnySupport(
        world: org.macaroon3145.world.World,
        x: Int,
        y: Int,
        z: Int,
        properties: Map<String, String>
    ): Boolean {
        if (properties["up"] == "true" && isSupportBlock(world.blockStateAt(x, y + 1, z))) {
            return true
        }
        if (properties["north"] == "true" && isSupportBlock(world.blockStateAt(x, y, z - 1))) {
            return true
        }
        if (properties["south"] == "true" && isSupportBlock(world.blockStateAt(x, y, z + 1))) {
            return true
        }
        if (properties["east"] == "true" && isSupportBlock(world.blockStateAt(x + 1, y, z))) {
            return true
        }
        if (properties["west"] == "true" && isSupportBlock(world.blockStateAt(x - 1, y, z))) {
            return true
        }
        return false
    }

    private fun encodedItemStack(itemId: Int, count: Int = 1): ByteArray {
        val out = ByteArrayOutputStream(8)
        // 1.20.5+ Slot shape (minimal):
        // [count VarInt][itemId VarInt][added_components VarInt=0][removed_components VarInt=0]
        NetworkUtils.writeVarInt(out, count.coerceAtLeast(1))
        NetworkUtils.writeVarInt(out, itemId)
        NetworkUtils.writeVarInt(out, 0)
        NetworkUtils.writeVarInt(out, 0)
        return out.toByteArray()
    }

    private fun emptyItemStack(): ByteArray {
        val out = ByteArrayOutputStream(1)
        NetworkUtils.writeVarInt(out, 0)
        return out.toByteArray()
    }

    private fun nextInventoryStateId(session: PlayerSession): Int {
        val next = session.inventoryStateId + 1
        session.inventoryStateId = next
        return next
    }

    private fun isPlacementBlockedByPlayer(session: PlayerSession, blockX: Int, blockY: Int, blockZ: Int): Boolean {
        val playerHeight = when {
            session.swimming -> 0.6
            session.sneaking -> 1.5
            else -> 1.8
        }

        val playerMinX = session.x - PLAYER_HITBOX_HALF_WIDTH
        val playerMaxX = session.x + PLAYER_HITBOX_HALF_WIDTH
        val playerMinY = session.y
        val playerMaxY = session.y + playerHeight
        val playerMinZ = session.z - PLAYER_HITBOX_HALF_WIDTH
        val playerMaxZ = session.z + PLAYER_HITBOX_HALF_WIDTH

        val blockMinX = blockX.toDouble()
        val blockMaxX = blockMinX + 1.0
        val blockMinY = blockY.toDouble()
        val blockMaxY = blockMinY + 1.0
        val blockMinZ = blockZ.toDouble()
        val blockMaxZ = blockMinZ + 1.0

        return playerMaxX > blockMinX && playerMinX < blockMaxX &&
            playerMaxY > blockMinY && playerMinY < blockMaxY &&
            playerMaxZ > blockMinZ && playerMinZ < blockMaxZ
    }

    private fun commandBlockNbtPayload(
        x: Int,
        y: Int,
        z: Int,
        command: String,
        trackOutput: Boolean,
        conditional: Boolean,
        automatic: Boolean
    ): ByteArray {
        val out = java.io.ByteArrayOutputStream()
        val data = java.io.DataOutputStream(out)

        fun writeName(name: String) {
            val bytes = name.toByteArray(Charsets.UTF_8)
            data.writeShort(bytes.size)
            data.write(bytes)
        }

        fun writeStringTag(name: String, value: String) {
            data.writeByte(8)
            writeName(name)
            val bytes = value.toByteArray(Charsets.UTF_8)
            data.writeShort(bytes.size)
            data.write(bytes)
        }

        fun writeIntTag(name: String, value: Int) {
            data.writeByte(3)
            writeName(name)
            data.writeInt(value)
        }

        fun writeByteTag(name: String, value: Boolean) {
            data.writeByte(1)
            writeName(name)
            data.writeByte(if (value) 1 else 0)
        }

        data.writeByte(10) // unnamed root compound
        writeStringTag("id", "minecraft:command_block")
        writeIntTag("x", x)
        writeIntTag("y", y)
        writeIntTag("z", z)
        writeStringTag("Command", command)
        writeByteTag("TrackOutput", trackOutput)
        writeByteTag("conditionMet", conditional)
        writeByteTag("auto", automatic)
        data.writeByte(0) // end
        return out.toByteArray()
    }

    private fun extractCommandFromEncodedItemStack(encoded: ByteArray): String? {
        val name = "Command".toByteArray(Charsets.UTF_8)
        var i = 0
        while (i + 12 < encoded.size) {
            if (encoded[i].toInt() == 8 &&
                encoded[i + 1].toInt() == 0 &&
                encoded[i + 2].toInt() == name.size
            ) {
                var match = true
                for (k in name.indices) {
                    if (encoded[i + 3 + k] != name[k]) {
                        match = false
                        break
                    }
                }
                if (match) {
                    val lenIndex = i + 3 + name.size
                    if (lenIndex + 1 < encoded.size) {
                        val commandLen =
                            ((encoded[lenIndex].toInt() and 0xFF) shl 8) or (encoded[lenIndex + 1].toInt() and 0xFF)
                        val valueStart = lenIndex + 2
                        val valueEnd = valueStart + commandLen
                        if (commandLen >= 0 && valueEnd <= encoded.size) {
                            return try {
                                String(encoded, valueStart, commandLen, Charsets.UTF_8)
                            } catch (_: Throwable) {
                                null
                            }
                        }
                    }
                }
            }
            i++
        }
        return null
    }

    private fun extractBlockStateTagProperties(encoded: ByteArray): Map<String, String>? {
        val targetName = "BlockStateTag".toByteArray(Charsets.UTF_8)
        var i = 0
        while (i + 3 + targetName.size < encoded.size) {
            // NBT compound tag entry: type(10) + name length + name bytes + payload
            if ((encoded[i].toInt() and 0xFF) == 10) {
                val nameLen = readUShort(encoded, i + 1)
                if (nameLen == targetName.size && i + 3 + nameLen <= encoded.size) {
                    var matched = true
                    for (k in targetName.indices) {
                        if (encoded[i + 3 + k] != targetName[k]) {
                            matched = false
                            break
                        }
                    }
                    if (matched) {
                        val start = i + 3 + nameLen
                        val parsed = parseSimpleStringCompound(encoded, start)
                        if (parsed != null && parsed.first.isNotEmpty()) {
                            return parsed.first
                        }
                    }
                }
            }
            i++
        }
        return null
    }

    private fun parseSimpleStringCompound(bytes: ByteArray, start: Int): Pair<Map<String, String>, Int>? {
        val out = HashMap<String, String>()
        var idx = start
        while (idx < bytes.size) {
            val tagType = bytes[idx].toInt() and 0xFF
            idx++
            if (tagType == 0) {
                return out to idx
            }
            if (idx + 2 > bytes.size) return null
            val nameLen = readUShort(bytes, idx)
            idx += 2
            if (idx + nameLen > bytes.size) return null
            val name = try {
                String(bytes, idx, nameLen, Charsets.UTF_8)
            } catch (_: Throwable) {
                return null
            }
            idx += nameLen
            if (tagType != 8) {
                // BlockStateTag values are string tags; ignore unsupported payloads.
                return null
            }
            if (idx + 2 > bytes.size) return null
            val valueLen = readUShort(bytes, idx)
            idx += 2
            if (idx + valueLen > bytes.size) return null
            val value = try {
                String(bytes, idx, valueLen, Charsets.UTF_8)
            } catch (_: Throwable) {
                return null
            }
            idx += valueLen
            out[name] = value
        }
        return null
    }

    private fun readUShort(bytes: ByteArray, index: Int): Int {
        return ((bytes[index].toInt() and 0xFF) shl 8) or (bytes[index + 1].toInt() and 0xFF)
    }

    private fun effectiveChunkRadius(requestedViewDistance: Int): Int {
        val requested = requestedViewDistance.coerceAtLeast(0)
        return requested.coerceAtMost(ServerConfig.maxViewDistanceChunks.coerceAtLeast(2))
    }

    private const val MAX_DROPPED_ITEM_DELTA_SECONDS = 0.25
    private const val MAX_DROPPED_ITEM_CATCH_UP_TICKS = 5
    private const val MAX_DROPPED_ITEM_LAG_TICKS_BEFORE_RESYNC = 10L
    private const val DROPPED_ITEM_COARSE_PARK_THRESHOLD_NANOS = 2_000_000L
    private const val DROPPED_ITEM_COARSE_PARK_GUARD_NANOS = 100_000L
    private const val CHUNK_MSPT_ACTION_BAR_INTERVAL_NANOS = 50_000_000L
    private const val GAME_MODE_SURVIVAL = 0
    private const val GAME_MODE_CREATIVE = 1
    private const val GAME_MODE_SPECTATOR = 3
    private const val MAX_PLAYER_HEALTH = 20f
    private const val DEFAULT_PLAYER_FOOD = 20
    private const val DEFAULT_PLAYER_SATURATION = 5f
    private const val MINECRAFT_TICKS_PER_SECOND = 20.0
    private const val MINECRAFT_DAY_TICKS = 24_000.0
    private const val INITIAL_WORLD_TIME_TICKS = 0.0
    private const val WORLD_TIME_BROADCAST_INTERVAL_SECONDS = 1.0
    private const val MAX_FALLING_BLOCK_COLUMN_SCAN = 32
    private const val SAFE_FALL_DISTANCE_BLOCKS = 3.0
    private const val MAX_HOTBAR_STACK_SIZE = 64
    private const val PLAYER_DROPPED_ITEM_PICKUP_DELAY_SECONDS = 2.0
    private const val BLOCK_DROP_PICKUP_DELAY_SECONDS = 0.5
    private const val BLOCK_DROP_HORIZONTAL_SPEED_MIN = -0.1
    private const val BLOCK_DROP_HORIZONTAL_SPEED_MAX = 0.1
    private const val BLOCK_DROP_VERTICAL_SPEED = 0.2
    private const val PLAYERS_SOUND_SOURCE_ID = 7
    private const val BIG_FALL_SOUND_DAMAGE_THRESHOLD = 5.0f
    private const val FALL_DAMAGE_TYPE_ID_FALLBACK = 17
    private val PLACEMENT_DEBUG_LOG_ENABLED: Boolean =
        System.getProperty("aerogel.debug.placement")?.toBooleanStrictOrNull() ?: false
    private const val PLAYER_HITBOX_HALF_WIDTH = 0.3
    private const val DROPPED_ITEM_PICKUP_EXPAND_HORIZONTAL = 1.0
    private const val DROPPED_ITEM_PICKUP_EXPAND_UP = 0.5
    private const val DROPPED_ITEM_PICKUP_EXPAND_DOWN = 0.5
    private const val DROPPED_ITEM_HALF_WIDTH = 0.125
    private const val DROPPED_ITEM_HEIGHT = 0.25
    private const val DROPPED_ITEM_PICKUP_DELAY_EPSILON = 1.0e-9
    private const val DROPPED_ITEM_PICKUP_DISTANCE_EPSILON = 1.0e-9
    private const val DROPPED_ITEM_PICKUP_BROADCAST_DISTANCE_SQ = 32.0 * 32.0
    private const val PLAYER_RELATIVE_MOVE_SCALE = 4096.0
    private const val DROPPED_ITEM_RELATIVE_MOVE_SCALE = 4096.0
    private const val DROPPED_ITEM_RELATIVE_MOVE_INTERVAL_SECONDS = 0.05
    private const val MAX_DROPPED_ITEM_RELATIVE_MOVE_ACCUMULATOR_SECONDS = 2.0
    private const val MAX_DROPPED_ITEM_RELATIVE_SECONDS_BEFORE_HARD_SYNC = 20.0
    private const val DROPPED_ITEM_VELOCITY_EPSILON = 1.0e-8
    private val json = Json { ignoreUnknownKeys = true }
    private val PICK_ITEM_FALLBACK_BLOCK_BY_BLOCK = hashMapOf(
        "minecraft:tall_grass" to "minecraft:short_grass",
        "minecraft:large_fern" to "minecraft:fern"
    )
    private val BED_BLOCK_KEYS = loadBlockTag("beds")
    private val INSTANT_BREAK_BLOCK_KEYS = hashSetOf(
        "minecraft:air",
        "minecraft:cave_air",
        "minecraft:void_air",
        "minecraft:fire",
        "minecraft:soul_fire",
        "minecraft:redstone_wire",
        "minecraft:tripwire",
        "minecraft:nether_portal",
        "minecraft:end_portal",
        "minecraft:end_gateway"
    ).apply {
        addAll(loadBlockTag("replaceable"))
        addAll(loadBlockTag("flowers"))
        addAll(loadBlockTag("small_flowers"))
        addAll(loadBlockTag("crops"))
        addAll(loadBlockTag("bee_growables"))
    }
    private val GRASS_LIKE_SUPPORT_KEYS = setOf(
        "minecraft:grass_block",
        "minecraft:podzol",
        "minecraft:mycelium"
    )
    private val GRASS_DECORATION_SUPPORT_KEYS = setOf(
        "minecraft:short_grass",
        "minecraft:fern",
        "minecraft:bush",
        "minecraft:firefly_bush",
        "minecraft:pink_petals",
        "minecraft:wildflowers",
        "minecraft:leaf_litter"
    )
    private val CROPS_REQUIRING_FARMLAND = setOf(
        "minecraft:wheat",
        "minecraft:carrots",
        "minecraft:potatoes",
        "minecraft:beetroots",
        "minecraft:torchflower_crop",
        "minecraft:pitcher_crop"
    )
    private val BELOW_SUPPORT_BREAK_KEYS = setOf(
        "minecraft:short_grass",
        "minecraft:fern",
        "minecraft:dead_bush",
        "minecraft:dandelion",
        "minecraft:poppy",
        "minecraft:blue_orchid",
        "minecraft:allium",
        "minecraft:azure_bluet",
        "minecraft:red_tulip",
        "minecraft:orange_tulip",
        "minecraft:white_tulip",
        "minecraft:pink_tulip",
        "minecraft:oxeye_daisy",
        "minecraft:cornflower",
        "minecraft:lily_of_the_valley",
        "minecraft:wither_rose",
        "minecraft:torchflower",
        "minecraft:bush",
        "minecraft:firefly_bush",
        "minecraft:pink_petals",
        "minecraft:wildflowers",
        "minecraft:leaf_litter",
        "minecraft:closed_eyeblossom",
        "minecraft:open_eyeblossom",
        "minecraft:oak_sapling",
        "minecraft:spruce_sapling",
        "minecraft:birch_sapling",
        "minecraft:jungle_sapling",
        "minecraft:acacia_sapling",
        "minecraft:dark_oak_sapling",
        "minecraft:mangrove_propagule",
        "minecraft:cherry_sapling",
        "minecraft:brown_mushroom",
        "minecraft:red_mushroom",
        "minecraft:bamboo_sapling",
        "minecraft:bamboo",
        "minecraft:sweet_berry_bush",
        "minecraft:torch",
        "minecraft:fire",
        "minecraft:soul_fire",
        "minecraft:tall_grass",
        "minecraft:large_fern",
        "minecraft:sunflower",
        "minecraft:lilac",
        "minecraft:rose_bush",
        "minecraft:peony",
        "minecraft:pitcher_plant",
        "minecraft:wheat",
        "minecraft:carrots",
        "minecraft:potatoes",
        "minecraft:beetroots",
        "minecraft:torchflower_crop",
        "minecraft:pitcher_crop"
    )
    private val WALL_MOUNTED_BLOCK_KEYS = setOf(
        "minecraft:wall_torch",
        "minecraft:redstone_wall_torch",
        "minecraft:soul_wall_torch"
    )
    private val NON_SUPPORT_BLOCK_KEYS = setOf(
        "minecraft:air",
        "minecraft:cave_air",
        "minecraft:void_air",
        "minecraft:water",
        "minecraft:lava",
        "minecraft:short_grass",
        "minecraft:tall_grass",
        "minecraft:fern",
        "minecraft:large_fern",
        "minecraft:dead_bush",
        "minecraft:vine",
        "minecraft:glow_lichen",
        "minecraft:torch",
        "minecraft:wall_torch",
        "minecraft:redstone_torch",
        "minecraft:redstone_wall_torch",
        "minecraft:soul_torch",
        "minecraft:soul_wall_torch",
        "minecraft:fire",
        "minecraft:soul_fire",
        "minecraft:snow"
    )
    private val fallingGravityStateCache = ConcurrentHashMap<Int, Boolean>()
    private val fallingPassThroughStateCache = ConcurrentHashMap<Int, Boolean>()
    private val FALLING_BLOCK_KEYS = loadBlockTag("falling_blocks")
    private val FALLING_BLOCK_PASS_THROUGH_KEYS = loadBlockTag("falling_block_pass_through")
    private fun loadBlockTag(tagName: String): Set<String> {
        return resolveBlockTag(tagName, HashSet())
    }

    private fun resolveBlockTag(tagName: String, visited: MutableSet<String>): Set<String> {
        if (!visited.add(tagName)) return emptySet()
        val resourcePath = "/data/minecraft/tags/block/$tagName.json"
        val stream = PlayerSessionManager::class.java.getResourceAsStream(resourcePath) ?: return emptySet()
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
    private val PLAYER_SMALL_FALL_SOUND_ID: Int? by lazy {
        RegistryCodec.entryIndex("minecraft:sound_event", "minecraft:entity.player.small_fall")
            ?: RegistryCodec.entryIndex("sound_event", "minecraft:entity.player.small_fall")
            ?: PLAYER_SMALL_FALL_SOUND_ID_FALLBACK
    }
    private val PLAYER_BIG_FALL_SOUND_ID: Int? by lazy {
        RegistryCodec.entryIndex("minecraft:sound_event", "minecraft:entity.player.big_fall")
            ?: RegistryCodec.entryIndex("sound_event", "minecraft:entity.player.big_fall")
            ?: PLAYER_BIG_FALL_SOUND_ID_FALLBACK
    }
    private val PLAYER_HURT_SOUND_ID: Int? by lazy {
        RegistryCodec.entryIndex("minecraft:sound_event", "minecraft:entity.player.hurt")
            ?: RegistryCodec.entryIndex("sound_event", "minecraft:entity.player.hurt")
            ?: PLAYER_HURT_SOUND_ID_FALLBACK
    }
    private val PLAYER_ATTACK_STRONG_SOUND_ID: Int? by lazy {
        RegistryCodec.entryIndex("minecraft:sound_event", "minecraft:entity.player.attack.strong")
            ?: RegistryCodec.entryIndex("sound_event", "minecraft:entity.player.attack.strong")
            ?: PLAYER_ATTACK_STRONG_SOUND_ID_FALLBACK
    }
    private val PLAYER_ATTACK_KNOCKBACK_SOUND_ID: Int? by lazy {
        RegistryCodec.entryIndex("minecraft:sound_event", "minecraft:entity.player.attack.knockback")
            ?: RegistryCodec.entryIndex("sound_event", "minecraft:entity.player.attack.knockback")
            ?: PLAYER_ATTACK_KNOCKBACK_SOUND_ID_FALLBACK
    }
    private val PLAYER_ATTACK_CRIT_SOUND_ID: Int? by lazy {
        RegistryCodec.entryIndex("minecraft:sound_event", "minecraft:entity.player.attack.crit")
            ?: RegistryCodec.entryIndex("sound_event", "minecraft:entity.player.attack.crit")
            ?: PLAYER_ATTACK_CRIT_SOUND_ID_FALLBACK
    }
    private val PLAYER_ATTACK_SWEEP_SOUND_ID: Int? by lazy {
        RegistryCodec.entryIndex("minecraft:sound_event", "minecraft:entity.player.attack.sweep")
            ?: RegistryCodec.entryIndex("sound_event", "minecraft:entity.player.attack.sweep")
            ?: PLAYER_ATTACK_SWEEP_SOUND_ID_FALLBACK
    }
    private val PLAYER_ATTACK_WEAK_SOUND_ID: Int? by lazy {
        RegistryCodec.entryIndex("minecraft:sound_event", "minecraft:entity.player.attack.weak")
            ?: RegistryCodec.entryIndex("sound_event", "minecraft:entity.player.attack.weak")
            ?: PLAYER_ATTACK_WEAK_SOUND_ID_FALLBACK
    }
    private val FALL_DAMAGE_TYPE_ID: Int by lazy {
        RegistryCodec.entryIndex("minecraft:damage_type", "minecraft:fall")
            ?: FALL_DAMAGE_TYPE_ID_FALLBACK
    }
    private val PLAYER_ATTACK_DAMAGE_TYPE_ID: Int by lazy {
        RegistryCodec.entryIndex("minecraft:damage_type", "minecraft:player_attack")
            ?: FALL_DAMAGE_TYPE_ID
    }
    private const val BASE_UNARMED_DAMAGE = 1.0f
    private const val MAX_PLAYER_MELEE_REACH_SQ = 9.0
    private const val VANILLA_CRITICAL_DAMAGE_MULTIPLIER = 1.5f
    private const val VANILLA_STRONG_ATTACK_THRESHOLD = 0.9f
    private const val BASE_PLAYER_KNOCKBACK = 0.4
    private const val SPRINT_KNOCKBACK_BONUS = 0.5
    private const val VANILLA_KNOCKBACK_UPWARD = 0.1
    private const val VANILLA_SWEEP_BASE_DAMAGE = 1.0f
    private const val VANILLA_SWEEP_KNOCKBACK = 0.4
    private const val VANILLA_SWEEP_MAX_DISTANCE_SQ = 9.0
    private const val PLAYER_SWEEP_SPEED_THRESHOLD = 0.25
    private const val PLAYER_SMALL_FALL_SOUND_ID_FALLBACK = 1257
    private const val PLAYER_BIG_FALL_SOUND_ID_FALLBACK = 1247
    private const val PLAYER_HURT_SOUND_ID_FALLBACK = 1251
    private const val PLAYER_ATTACK_KNOCKBACK_SOUND_ID_FALLBACK = 1242
    private const val PLAYER_ATTACK_CRIT_SOUND_ID_FALLBACK = 1241
    private const val PLAYER_ATTACK_STRONG_SOUND_ID_FALLBACK = 1244
    private const val PLAYER_ATTACK_SWEEP_SOUND_ID_FALLBACK = 1245
    private const val PLAYER_ATTACK_WEAK_SOUND_ID_FALLBACK = 1246
    private const val ENTITY_EVENT_DEATH_ANIMATION = 3

    private fun chunkStreamBatchWorkers(): Int = ChunkStreamingService.maxWorkerCount().coerceAtLeast(1)

    private fun requestChunkStream(
        session: PlayerSession,
        ctx: ChannelHandlerContext,
        world: org.macaroon3145.world.World,
        centerChunkX: Int,
        centerChunkZ: Int,
        radius: Int
    ) {
        // Deduplicate identical targets while a stream is in flight.
        if (session.chunkStreamInFlight.get()) {
            val pending = session.pendingChunkTarget.get()
            if (pending != null &&
                pending.centerChunkX == centerChunkX &&
                pending.centerChunkZ == centerChunkZ &&
                pending.radius == radius
            ) {
                return
            }
        }

        val streamId = session.chunkStreamVersion.incrementAndGet()
        val target = ChunkStreamTarget(streamId, centerChunkX, centerChunkZ, radius)
        session.pendingChunkTarget.set(target)
        if (!session.chunkStreamInFlight.compareAndSet(false, true)) {
            return
        }

        fun drain() {
            val next = session.pendingChunkTarget.getAndSet(null)
            if (next == null || !ctx.channel().isActive) {
                session.chunkStreamInFlight.set(false)
                val race = session.pendingChunkTarget.get()
                if (race != null && session.chunkStreamInFlight.compareAndSet(false, true)) {
                    ctx.executor().execute { drain() }
                }
                return
            }

            // Drop stale targets captured while player kept moving.
            if (next.centerChunkX != session.centerChunkX ||
                next.centerChunkZ != session.centerChunkZ ||
                next.radius != session.chunkRadius ||
                next.streamId != session.chunkStreamVersion.get()
            ) {
                ctx.executor().execute { drain() }
                return
            }

            // Keep client chunk center/limit synchronized with server-side cap.
            ctx.write(PlayPackets.updateViewDistancePacket(next.radius))
            ctx.write(PlayPackets.updateViewPositionPacket(next.centerChunkX, next.centerChunkZ))
            ctx.write(PlayPackets.chunkBatchStartPacket())

            val targetCoords = ChunkStreamingService.buildSquareCoords(next.centerChunkX, next.centerChunkZ, next.radius)
            val targetKeys = HashSet<ChunkPos>(targetCoords.size)
            val toLoadAll = ArrayList<ChunkPos>()
            for (coord in targetCoords) {
                targetKeys.add(coord)
                if (!session.loadedChunks.contains(coord) && !session.generatingChunks.contains(coord)) {
                    toLoadAll.add(coord)
                }
            }
            session.targetChunks.clear()
            session.targetChunks.addAll(targetKeys)
            updateRetainedBaseWorldChunksForWorld(session.worldKey)
            val toLoad = toLoadAll

            val shouldSendCurrentTarget = {
                next.streamId == session.chunkStreamVersion.get() &&
                    next.centerChunkX == session.centerChunkX &&
                    next.centerChunkZ == session.centerChunkZ &&
                    next.radius == session.chunkRadius
            }

            fun finishStream(totalSentCount: Int) {
                ctx.executor().execute {
                    if (ctx.channel().isActive) {
                        if (next.streamId != session.chunkStreamVersion.get()) {
                            drain()
                            return@execute
                        }
                        // Load-first, unload-after to avoid transparent border holes.
                        val toUnload = ArrayList<ChunkPos>()
                        for (pos in session.loadedChunks) {
                            if (!targetKeys.contains(pos)) {
                                toUnload.add(pos)
                            }
                        }
                        val unloadIdsByChunk = HashMap<ChunkPos, CompletableFuture<IntArray>>(toUnload.size)
                        for (pos in toUnload) {
                            unloadIdsByChunk[pos] = ChunkStreamingService.fetchDroppedItemEntityIdsAsync(world, pos)
                        }

                        val finishUnloadAndFlush = {
                            for (pos in toUnload) {
                                val ids = unloadIdsByChunk[pos]?.getNow(IntArray(0)) ?: IntArray(0)
                                hideDroppedItemsForChunk(session, ctx, pos, ids)
                                hideFallingBlocksForChunk(session, ctx, pos, world.fallingBlockEntityIdsInChunk(pos.x, pos.z))
                                session.loadedChunks.remove(pos)
                                ctx.write(PlayPackets.unloadChunkPacket(pos.x, pos.z))
                            }
                            updateRetainedBaseWorldChunksForWorld(session.worldKey)
                            ctx.writeAndFlush(PlayPackets.chunkBatchFinishedPacket(totalSentCount))
                            drain()
                        }

                        if (unloadIdsByChunk.isEmpty()) {
                            finishUnloadAndFlush()
                            return@execute
                        }

                        val futuresArray = unloadIdsByChunk.values.toTypedArray()
                        CompletableFuture.allOf(*futuresArray).whenComplete { _, _ ->
                            ctx.executor().execute {
                                if (!ctx.channel().isActive) {
                                    drain()
                                    return@execute
                                }
                                if (next.streamId != session.chunkStreamVersion.get()) {
                                    drain()
                                    return@execute
                                }
                                finishUnloadAndFlush()
                            }
                        }
                        return@execute
                    }
                    drain()
                }
            }

            ChunkStreamingService.streamCoords(
                ctx = ctx,
                world = world,
                coordsInput = toLoad,
                loadedChunks = session.loadedChunks,
                generatingChunks = session.generatingChunks,
                shouldSend = shouldSendCurrentTarget,
                onChunkSent = { chunkPos, droppedItems ->
                    sendDroppedItemsForChunk(session, ctx, droppedItems)
                    sendFallingBlocksForChunk(session, ctx, world.fallingBlocksInChunk(chunkPos.x, chunkPos.z))
                },
                workerLimit = chunkStreamBatchWorkers()
            ).whenComplete { sentCount, error ->
                ctx.executor().execute {
                    if (!ctx.channel().isActive || !shouldSendCurrentTarget()) {
                        drain()
                        return@execute
                    }
                    if (error != null) {
                        logger.warn(
                            "Chunk stream failed for player={} at center=({}, {}) radius={}; closing loading state",
                            session.profile.username,
                            next.centerChunkX,
                            next.centerChunkZ,
                            next.radius,
                            error
                        )
                        // Avoid "Loading terrain..." stall on persistent generator failures.
                        // Do not requeue the same failing target; allow future movement/view changes
                        // to request a fresh stream.
                        ctx.writeAndFlush(PlayPackets.chunkBatchFinishedPacket(0))
                        drain()
                        return@execute
                    }
                    finishStream(sentCount ?: 0)
                }
            }
        }

        // Do not block stream start on prewarm; overlapping avoids front-loaded worker idle time.
        ChunkStreamingService.prewarmAsync(world)
        ctx.executor().execute { drain() }
    }

}
