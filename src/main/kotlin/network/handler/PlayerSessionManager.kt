package org.macaroon3145.network.handler

import io.netty.buffer.Unpooled
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelId
import org.macaroon3145.config.ServerConfig
import org.macaroon3145.DebugConsole
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
import org.macaroon3145.world.WorldManager
import java.util.ArrayList
import java.util.HashSet
import java.io.ByteArrayOutputStream
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.LockSupport
import kotlin.math.abs
import kotlin.math.cos
import kotlin.math.sin
import kotlin.math.roundToLong

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
    val worldKey: String,
    val entityId: Int,
    val skinPartsMask: Int,
    @Volatile var pingMs: Int,
    @Volatile var x: Double,
    @Volatile var y: Double,
    @Volatile var z: Double,
    @Volatile var encodedX4096: Long,
    @Volatile var encodedY4096: Long,
    @Volatile var encodedZ4096: Long,
    @Volatile var yaw: Float,
    @Volatile var pitch: Float,
    @Volatile var onGround: Boolean,
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
    val loadedChunks: MutableSet<ChunkPos>,
    val visibleDroppedItemEntityIds: MutableSet<Int>,
    val droppedItemTrackerStates: MutableMap<Int, DroppedItemTrackerState>,
    val generatingChunks: MutableSet<ChunkPos>,
    val chunkStreamVersion: AtomicInteger,
    val chunkStreamInFlight: AtomicBoolean,
    val pendingChunkTarget: AtomicReference<ChunkStreamTarget?>
)

data class ChunkStreamTarget(
    val streamId: Int,
    val centerChunkX: Int,
    val centerChunkZ: Int,
    val radius: Int
)

data class CommandSuggestionWindow(
    val start: Int,
    val length: Int,
    val suggestions: List<String>
)

object PlayerSessionManager {
    private val nextEntityId = AtomicInteger(1)
    private val sessions = ConcurrentHashMap<ChannelId, PlayerSession>()
    private val contexts = ConcurrentHashMap<ChannelId, ChannelHandlerContext>()
    private val droppedItemDispatchRunning = AtomicBoolean(false)
    private val commandExecutor = Executors.newFixedThreadPool(2) { runnable ->
        Thread(runnable, "aerogel-command-worker").apply {
            isDaemon = true
            priority = Thread.NORM_PRIORITY - 1
        }
    }
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
        override fun sourceWorldKey(target: PlayerSession?): String = target?.worldKey ?: (WorldManager.defaultWorld().key)
        override fun sourceX(target: PlayerSession?): Double = target?.x ?: 0.0
        override fun sourceY(target: PlayerSession?): Double = target?.y ?: 64.0
        override fun sourceZ(target: PlayerSession?): Double = target?.z ?: 0.0
        override fun onlinePlayers(): List<PlayerSession> = sessions.values.toList()
        override fun onlinePlayersInWorld(worldKey: String): List<PlayerSession> = PlayerSessionManager.onlinePlayersInWorld(worldKey)
        override fun findOnlinePlayer(name: String): PlayerSession? = PlayerSessionManager.findOnlinePlayer(name)
        override fun isOperator(session: PlayerSession?): Boolean = PlayerSessionManager.isOperator(session)
        override fun canUseOpCommand(sender: PlayerSession?): Boolean = PlayerSessionManager.canRunOpCommand(sender)
        override fun grantOperator(target: PlayerSession): Boolean = PlayerSessionManager.grantOperatorAndRefreshCommands(target)
        override fun teleport(target: PlayerSession, x: Double, y: Double, z: Double, yaw: Float?, pitch: Float?) =
            PlayerSessionManager.teleportPlayer(target, x, y, z, yaw, pitch)
        override fun setGamemode(target: PlayerSession, mode: Int) = PlayerSessionManager.setPlayerGamemode(target, mode)
    }
    @Volatile
    private var droppedItemDispatchThread: Thread? = null
    private val creativeNoBreakItemIds: Set<Int> by lazy {
        val itemRegistry = RegistryCodec.allRegistries().firstOrNull { it.id == "minecraft:item" } ?: return@lazy emptySet()
        itemRegistry.entries.withIndex()
            .filter { (_, entry) -> entry.key.endsWith("_sword") }
            .mapTo(HashSet()) { it.index }
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
            worldKey = worldKey,
            entityId = allocateEntityId(),
            skinPartsMask = skinPartsMask,
            pingMs = 0,
            x = spawnX,
            y = spawnY,
            z = spawnZ,
            encodedX4096 = encodeRelativePosition4096(spawnX),
            encodedY4096 = encodeRelativePosition4096(spawnY),
            encodedZ4096 = encodeRelativePosition4096(spawnZ),
            yaw = 0f,
            pitch = 0f,
            onGround = true,
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
            loadedChunks = ConcurrentHashMap.newKeySet(),
            visibleDroppedItemEntityIds = ConcurrentHashMap.newKeySet(),
            droppedItemTrackerStates = ConcurrentHashMap(),
            generatingChunks = ConcurrentHashMap.newKeySet(),
            chunkStreamVersion = AtomicInteger(0),
            chunkStreamInFlight = AtomicBoolean(false),
            pendingChunkTarget = AtomicReference(null)
        )

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
        // Dropped-item processing runs in its own async dispatch thread.
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

        val ready = ArrayList<Pair<org.macaroon3145.world.World, org.macaroon3145.world.DroppedItemTickEvents>>()
        for (world in worlds) {
            if (!world.hasDroppedItems()) continue
            val worldSessions = sessionsByWorld[world.key]
            if (worldSessions.isNullOrEmpty()) continue
            val activeSimulationChunks = collectActiveSimulationChunks(worldSessions)
            if (activeSimulationChunks.isEmpty()) continue
            val events = world.tickDroppedItems(physicsDeltaSeconds, activeSimulationChunks)
            ready.add(world to events)
        }
        if (ready.isEmpty()) return

        for ((world, events) in ready) {
            val worldSessions = sessionsByWorld[world.key] ?: continue
            if (worldSessions.isEmpty()) continue

            val outboundByContext = HashMap<ChannelHandlerContext, MutableList<ByteArray>>()
            val pickedRemovals = collectDroppedItemPickups(world, worldSessions, outboundByContext)

            for (removed in events.removed) {
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

            for (snapshot in events.spawned) {
                val current = world.droppedItemSnapshot(snapshot.entityId) ?: continue
                syncDroppedItemSnapshot(
                    snapshot = current,
                    worldSessions = worldSessions,
                    outboundByContext = outboundByContext,
                    sendPositionUpdate = false,
                    deltaSeconds = deltaSeconds
                )
            }
            for (snapshot in events.updated) {
                val current = world.droppedItemSnapshot(snapshot.entityId) ?: continue
                syncDroppedItemSnapshot(
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
        var targetDeltaSeconds = tickIntervalNanos / 1_000_000_000.0
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
                    targetDeltaSeconds = tickIntervalNanos / 1_000_000_000.0
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
                val steps = if (uncapped) {
                    1
                } else {
                    kotlin.math.ceil(deltaSeconds / targetDeltaSeconds)
                        .toInt()
                        .coerceAtLeast(1)
                        .coerceAtMost(MAX_DROPPED_ITEM_CATCH_UP_TICKS)
                }
                val stepDelta = (deltaSeconds / steps).coerceAtMost(MAX_DROPPED_ITEM_DELTA_SECONDS)
                repeat(steps) {
                    drainDroppedItemEvents(stepDelta)
                }
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
        session.x = x
        session.y = y
        session.z = z
        session.yaw = yaw
        session.pitch = pitch
        session.onGround = onGround

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
            if (ctx != null && world != null) {
                requestChunkStream(session, ctx, world, currentChunkX, currentChunkZ, session.chunkRadius)
            }
            session.centerChunkX = currentChunkX
            session.centerChunkZ = currentChunkZ
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
        val window = inGameCommandSuggestions(input, includeOperatorCommands = isOperator(sender))
        val packet = PlayPackets.commandSuggestionsPacket(
            requestId = requestId,
            start = window.start,
            length = window.length,
            suggestions = window.suggestions
        )
        ctx.writeAndFlush(packet)
    }

    fun inGameCommandSuggestions(input: String, includeOperatorCommands: Boolean): CommandSuggestionWindow {
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
            start = activeArgStart + if (hasSlash) 1 else 0,
            length = if (trailingWhitespace) 0 else normalized.length - activeArgStart,
            suggestions = commandArgumentCompletions(commandName, providedArgs, activeArgIndex, activeArgPrefix)
        )
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
        return commandArgumentCompletions(commandName, providedArgs, activeArgIndex, activeArgPrefix)
    }

    private fun commandArgumentCompletions(
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
            "tp", "teleport" -> tpCommandCompletions(providedArgs, activeArgIndex, activeArgPrefix)
            else -> emptyList()
        }
    }

    private fun tpCommandCompletions(
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
        val coordinateCandidates = listOf("-100", "0", "64", "100")

        val raw = when (activeArgIndex) {
            0 -> targetCandidates
            1 -> (targetCandidates + coordinateCandidates)
            2, 3 -> coordinateCandidates
            else -> emptyList()
        }
        return raw.asSequence()
            .filter { it.startsWith(activeArgPrefix, ignoreCase = true) }
            .distinct()
            .toList()
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

    private fun isOperator(session: PlayerSession?): Boolean {
        if (session == null) return true
        return operatorUuids.contains(session.profile.uuid)
    }

    private fun findOnlinePlayer(name: String): PlayerSession? {
        return sessions.values.firstOrNull { it.profile.username.equals(name, ignoreCase = true) }
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
                    val packet = if (hasRotation) {
                        PlayPackets.playerPositionPacket(x, clampedY, z, yaw = nextYaw, pitch = nextPitch, relativeFlags = 0)
                    } else {
                        // Keep client yaw/pitch unchanged by using relative-rotation flags with zero deltas.
                        PlayPackets.playerPositionPacket(x, clampedY, z, yaw = 0f, pitch = 0f, relativeFlags = 0x18)
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
        val source = PlayPackets.ChatComponent.Text(ServerI18n.tr("aerogel.console.source.terminal"), italic = true)
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
        val stateId = world.blockStateAt(x, y, z)
        if (stateId == 0) return
        val pickedStateId = normalizePickedBlockStateId(stateId)
        val pickedItemId = itemIdForState(pickedStateId)
        if (pickedItemId < 0) return

        val blockEntity = world.blockEntityAt(x, y, z)
        val pickedTypeId = blockEntity?.typeId ?: -1
        val pickedNbt = blockEntity?.nbtPayload?.copyOf()

        // Creative pick-block behavior:
        // 1) If same block state (+block entity payload) already exists in hotbar, just switch held slot.
        // 2) Otherwise place item into currently selected hotbar slot and sync that slot to client.
        val existingSlot = findHotbarSlotForPick(
            session = session,
            stateId = pickedStateId,
            itemId = pickedItemId,
            blockEntityTypeId = pickedTypeId,
            blockEntityNbtPayload = pickedNbt
        )

        if (existingSlot >= 0) {
            // Vanilla creative pick-block behavior:
            // If same block already exists in hotbar, switch held slot to it.
            session.selectedHotbarSlot = existingSlot
            session.selectedBlockStateId = pickedStateId
            if (ctx.channel().isActive) {
                ctx.writeAndFlush(PlayPackets.setHeldSlotPacket(existingSlot))
            }
            broadcastHeldItemEquipment(session)
            return
        }
        val previousSelected = session.selectedHotbarSlot.coerceIn(0, 8)
        val targetSlot = findPickInsertHotbarSlot(session, previousSelected)
        session.selectedHotbarSlot = targetSlot
        session.pendingPickedBlockStateId = pickedStateId
        session.hotbarBlockStateIds[targetSlot] = pickedStateId
        session.selectedBlockStateId = pickedStateId
        session.hotbarBlockEntityTypeIds[targetSlot] = pickedTypeId
        session.hotbarBlockEntityNbtPayloads[targetSlot] = pickedNbt
        session.hotbarItemIds[targetSlot] = pickedItemId
        session.hotbarItemCounts[targetSlot] = 1

        val itemId = session.hotbarItemIds[targetSlot]
        if (ctx.channel().isActive && itemId >= 0) {
            if (targetSlot != previousSelected) {
                ctx.write(PlayPackets.setHeldSlotPacket(targetSlot))
            }
            val inventoryStateId = nextInventoryStateId(session)
            // Creative inventory indexing: hotbar 0..8 => slots 36..44.
            ctx.writeAndFlush(
                PlayPackets.containerSetSlotPacket(
                    containerId = 0,
                    stateId = inventoryStateId,
                    slot = 36 + targetSlot,
                    encodedItemStack = encodedItemStack(itemId)
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
        if (session.gameMode != 1) return
        val world = WorldManager.world(session.worldKey) ?: return
        if (!isChunkLoadedForSession(session, x, z)) {
            resyncBlockToPlayer(session, x, y, z, world.blockStateAt(x, y, z))
            return
        }
        // Reject placement into occupied space to keep server-authoritative collision rules sane.
        // (simple rule for now: only place into air)
        if (world.blockStateAt(x, y, z) != 0) return
        // Do not allow placing into the player's own collision box.
        if (isPlacementBlockedByPlayer(session, x, y, z)) return

        val offhand = hand == 1
        val slot = session.selectedHotbarSlot.coerceIn(0, 8)
        val selectedItemId = if (offhand) session.offhandItemId else session.hotbarItemIds[slot]
        val selectedItemCount = if (offhand) session.offhandItemCount else session.hotbarItemCounts[slot]
        if (selectedItemCount <= 0 || selectedItemId < 0) return
        val baseStateId = if (offhand) {
            itemBlockStateId(selectedItemId)
        } else {
            session.hotbarBlockStateIds[slot]
        }
        if (baseStateId <= 0) return
        val blockStateId = resolvePlacementState(
            baseStateId = baseStateId,
            faceId = faceId,
            cursorX = cursorX,
            cursorY = cursorY,
            cursorZ = cursorZ,
            playerYaw = session.yaw
        )
        val upperStateId = twoBlockUpperStateId(blockStateId)
        if (upperStateId > 0) {
            val upperY = y + 1
            if (upperY > 319) return
            if (world.blockStateAt(x, upperY, z) != 0) return
            if (isPlacementBlockedByPlayer(session, x, upperY, z)) return
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
        if (upperStateId > 0) {
            setBlockAndBroadcast(
                session = session,
                x = x,
                y = y + 1,
                z = z,
                stateId = upperStateId
            )
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
        if (session.gameMode == 1) {
            val slot = session.selectedHotbarSlot.coerceIn(0, 8)
            val itemId = session.hotbarItemIds[slot]
            if (creativeNoBreakItemIds.contains(itemId)) {
                resyncBlockToPlayer(session, x, y, z, world.blockStateAt(x, y, z))
                return
            }
        }
        val stateId = world.blockStateAt(x, y, z)
        val paired = twoBlockPairedPos(world, x, y, z, stateId)
        setBlockAndBroadcast(session, x, y, z, 0)
        if (paired != null) {
            setBlockAndBroadcast(session, paired.x, paired.y, paired.z, 0)
        }
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

    private fun twoBlockUpperStateId(stateId: Int): Int {
        val parsed = BlockStateRegistry.parsedState(stateId) ?: return 0
        if (parsed.blockKey !in TWO_BLOCK_PLANT_KEYS) return 0
        if (parsed.properties["half"] != "lower") return 0
        val props = HashMap(parsed.properties)
        props["half"] = "upper"
        return BlockStateRegistry.stateId(parsed.blockKey, props) ?: 0
    }

    private fun lowerHalfStateId(stateId: Int): Int? {
        val parsed = BlockStateRegistry.parsedState(stateId) ?: return null
        if (parsed.blockKey !in TWO_BLOCK_PLANT_KEYS) return null
        if (parsed.properties["half"] != "upper") return null
        val props = HashMap(parsed.properties)
        props["half"] = "lower"
        return BlockStateRegistry.stateId(parsed.blockKey, props)
    }

    private fun normalizePickedBlockStateId(stateId: Int): Int {
        if (stateId <= 0) return stateId
        return lowerHalfStateId(stateId) ?: stateId
    }

    private fun twoBlockPairedPos(
        world: org.macaroon3145.world.World,
        x: Int,
        y: Int,
        z: Int,
        stateId: Int
    ): BlockPos? {
        if (stateId <= 0) return null
        val parsed = BlockStateRegistry.parsedState(stateId) ?: return null
        if (parsed.blockKey !in TWO_BLOCK_PLANT_KEYS) return null
        val half = parsed.properties["half"] ?: return null
        val pairedY = when (half) {
            "lower" -> y + 1
            "upper" -> y - 1
            else -> return null
        }
        if (pairedY !in -64..319) return null
        val pairedStateId = world.blockStateAt(x, pairedY, z)
        val paired = BlockStateRegistry.parsedState(pairedStateId) ?: return null
        if (paired.blockKey != parsed.blockKey) return null
        val expectedHalf = if (half == "lower") "upper" else "lower"
        if (paired.properties["half"] != expectedHalf) return null
        return BlockPos(x, pairedY, z)
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
    private const val GAME_MODE_SPECTATOR = 3
    private const val MAX_HOTBAR_STACK_SIZE = 64
    private const val PLAYER_DROPPED_ITEM_PICKUP_DELAY_SECONDS = 2.0
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
    private val TWO_BLOCK_PLANT_KEYS = hashSetOf(
        "minecraft:tall_grass",
        "minecraft:large_fern"
    )
    private val PICK_ITEM_FALLBACK_BLOCK_BY_BLOCK = hashMapOf(
        "minecraft:tall_grass" to "minecraft:short_grass",
        "minecraft:large_fern" to "minecraft:fern"
    )

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
            if (pending == null &&
                session.centerChunkX == centerChunkX &&
                session.centerChunkZ == centerChunkZ &&
                session.chunkRadius == radius
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
                                session.loadedChunks.remove(pos)
                                ctx.write(PlayPackets.unloadChunkPacket(pos.x, pos.z))
                            }
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
                onChunkSent = { _, droppedItems ->
                    sendDroppedItemsForChunk(session, ctx, droppedItems)
                },
                workerLimit = chunkStreamBatchWorkers()
            ).whenComplete { sentCount, error ->
                ctx.executor().execute {
                    if (!ctx.channel().isActive || !shouldSendCurrentTarget()) {
                        drain()
                        return@execute
                    }
                    if (error != null) {
                        session.pendingChunkTarget.set(next)
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
