package org.macaroon3145.plugin

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import io.netty.channel.ChannelId
import org.macaroon3145.DebugConsole
import org.macaroon3145.api.Server
import org.macaroon3145.api.command.CommandInvocation
import org.macaroon3145.api.command.CommandHandler
import org.macaroon3145.api.command.CommandExecutor
import org.macaroon3145.api.command.CommandRegistrar
import org.macaroon3145.api.command.CommandSender
import org.macaroon3145.api.command.CommandSpec
import org.macaroon3145.api.command.FunctionalCommandSpec
import org.macaroon3145.api.command.Command as ApiCommand
import org.macaroon3145.api.command.CommandContext as ApiCommandContext
import org.macaroon3145.api.command.ConsoleOnly
import org.macaroon3145.api.command.Permission as ApiPermission
import org.macaroon3145.api.command.PlayerOnly
import org.macaroon3145.api.command.RegisteredCommand
import org.macaroon3145.api.data.PluginDataStore
import org.macaroon3145.api.event.CancellableEvent
import org.macaroon3145.api.event.CommandDispatchEvent
import org.macaroon3145.api.event.Event
import org.macaroon3145.api.event.EventBus
import org.macaroon3145.api.event.EventFilter
import org.macaroon3145.api.event.EventPriority
import org.macaroon3145.api.event.EventSubscription
import org.macaroon3145.api.event.BlockBreakEvent
import org.macaroon3145.api.event.BlockChangeEvent
import org.macaroon3145.api.event.BlockChangeReason
import org.macaroon3145.api.event.BlockSnapshot
import org.macaroon3145.api.event.BlockPlaceEvent
import org.macaroon3145.api.event.EntityMoveEvent
import org.macaroon3145.api.event.MoveMedium
import org.macaroon3145.api.event.MoveReason
import org.macaroon3145.api.event.PlayerInteractAction
import org.macaroon3145.api.event.PlayerInteractEvent
import org.macaroon3145.api.event.PlayerJumpEvent
import org.macaroon3145.api.event.PlayerMoveEvent
import org.macaroon3145.api.event.PluginStateChangeEvent
import org.macaroon3145.api.event.PlayerJoinEvent
import org.macaroon3145.api.event.PlayerQuitEvent
import org.macaroon3145.api.entity.ArmorSlot
import org.macaroon3145.api.entity.Bot
import org.macaroon3145.api.entity.ConnectedPlayer
import org.macaroon3145.api.entity.DroppedItem
import org.macaroon3145.api.entity.Egg
import org.macaroon3145.api.entity.Entity
import org.macaroon3145.api.entity.EnderPearl
import org.macaroon3145.api.entity.GameMode
import org.macaroon3145.api.entity.Hand
import org.macaroon3145.api.entity.Item
import org.macaroon3145.api.entity.InventorySlotView
import org.macaroon3145.api.entity.Player
import org.macaroon3145.api.entity.PlayerInventory
import org.macaroon3145.api.entity.PlayerRegistry
import org.macaroon3145.api.entity.Snowball
import org.macaroon3145.api.entity.EntityType
import org.macaroon3145.api.entity.Thrown
import org.macaroon3145.api.world.Block
import org.macaroon3145.api.world.Chunk
import org.macaroon3145.api.world.ChunkState
import org.macaroon3145.api.world.BlockState
import org.macaroon3145.api.world.BlockFace
import org.macaroon3145.api.world.Location
import org.macaroon3145.api.world.World
import org.macaroon3145.api.world.WorldRegistry
import org.macaroon3145.api.event.Subscribe
import org.macaroon3145.api.packet.PacketEnvelope
import org.macaroon3145.api.packet.PacketInterceptor
import org.macaroon3145.api.packet.PacketInterceptorHandle
import org.macaroon3145.api.packet.PacketInterceptorRegistry
import org.macaroon3145.api.permission.PermissionResult
import org.macaroon3145.api.permission.PermissionService
import org.macaroon3145.api.plugin.AerogelPlugin
import org.macaroon3145.api.plugin.PluginContext
import org.macaroon3145.api.plugin.PluginDisableReason
import org.macaroon3145.api.plugin.PluginLogger
import org.macaroon3145.api.plugin.PluginMetadata
import org.macaroon3145.api.plugin.PluginI18n
import org.macaroon3145.api.plugin.PluginRegistry
import org.macaroon3145.api.plugin.PluginReloadReason
import org.macaroon3145.api.plugin.PluginRuntime
import org.macaroon3145.api.plugin.PluginState
import org.macaroon3145.api.scheduler.ScheduledTask
import org.macaroon3145.api.scheduler.TaskScheduler
import org.macaroon3145.api.scheduler.TickScheduler
import org.macaroon3145.api.scheduler.TickTask
import org.macaroon3145.api.service.ServiceHandle
import org.macaroon3145.api.service.ServiceRegistry
import org.macaroon3145.api.type.BlockType
import org.macaroon3145.api.type.ItemType
import org.macaroon3145.api.type.TypeRegistry
import org.macaroon3145.i18n.ServerI18n
import org.macaroon3145.network.command.Command
import org.macaroon3145.network.command.CommandCompletionEncoding
import org.macaroon3145.network.command.CommandContext
import org.macaroon3145.network.codec.BlockStateRegistry
import org.macaroon3145.network.codec.RegistryCodec
import org.macaroon3145.network.handler.ConnectionProfile
import org.macaroon3145.network.handler.PlayPackets
import org.macaroon3145.network.handler.PlayerSession
import org.macaroon3145.network.handler.PlayerSessionManager
import org.macaroon3145.plugin.mixin.MixinWeavingEngine
import org.macaroon3145.world.BlockCollisionRegistry
import org.macaroon3145.world.ChunkPos
import org.macaroon3145.world.ThrownItemKind
import org.macaroon3145.world.WorldManager
import org.macaroon3145.world.generators.FoliaSharedMemoryWorldGenerator
import org.slf4j.LoggerFactory
import java.lang.invoke.MethodHandles
import java.lang.reflect.Modifier
import java.net.URL
import java.net.URLClassLoader
import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.abs
import kotlin.math.floor
import kotlin.time.Duration

object PluginSystem {
    private val logger = LoggerFactory.getLogger(PluginSystem::class.java)
    private val json = Json { ignoreUnknownKeys = true }
    private val initialized = AtomicBoolean(false)

    private val eventBus = FastEventBus()
    private val permissionService = StandardPermissionService()
    private val serviceRegistry = GenerationServiceRegistry()
    private val packetInterceptors = SimplePacketInterceptorRegistry()
    private val scheduler = SimpleTaskScheduler()
    private val tickScheduler = SimpleTickScheduler()
    private val pluginJarHotReloadLoop = PluginJarHotReloadLoop(::handlePluginJarChanged)
    private val activePluginOwner = ThreadLocal<String?>()
    private val activePluginContext = ThreadLocal<PluginContext?>()

    private val pluginsById = ConcurrentHashMap<String, LoadedPlugin>()
    private val pluginsInLoadOrder = CopyOnWriteArrayList<LoadedPlugin>()
    private val pluginStateLock = Any()

    private enum class PluginLoadReason {
        STARTUP,
        RELOAD,
        DEPENDENCY_RELOAD,
        DEV_HOT_RELOAD
    }

    private enum class PluginLoadOutcome {
        ENABLED,
        SKIPPED_ALREADY_LOADED,
        SKIPPED_MISSING_DEPENDENCIES
    }

    data class BlockBreakDecision(
        val proceed: Boolean,
        val modifiedByPlugin: Boolean
    )

    data class MoveDecision(
        val cancelPosition: Boolean,
        val cancelRotation: Boolean
    ) {
        val cancelAll: Boolean
            get() = cancelPosition && cancelRotation
    }

    fun initialize() {
        if (!initialized.compareAndSet(false, true)) return
        PluginRuntime.bind(
            ownerProvider = ::currentPluginOwner,
            currentContextProvider = ::currentPluginContext,
            contextProvider = ::pluginContext
        )
        Server.bindWorldRegistry(runtimeWorldRegistry)
        Server.bindMainTickScheduler(tickScheduler.global())
        Server.bindTaskScheduler(scheduler)
        Server.bindPlayerRegistry(runtimePlayerRegistry)
        synchronized(pluginStateLock) {
            loadAllFromDisk()
        }
        pluginJarHotReloadLoop.start()
    }

    fun shutdown() {
        pluginJarHotReloadLoop.stop()
        synchronized(pluginStateLock) {
            disableAll()
            scheduler.shutdownAll()
            tickScheduler.shutdownAll()
            initialized.set(false)
        }
        PluginRuntime.clearBindings()
        Server.clearBindings()
    }

    fun onGameTick(gameTick: Long) {
        tickScheduler.pulse(gameTick)
        dispatchRuntimeBotTicks()
        syncRuntimeBotNetwork()
    }

    fun runtimeBotSnapshot(worldKey: String, entityId: Int): RuntimeBotSnapshot? {
        val botUuid = runtimeBotUuidByEntityId[entityId] ?: return null
        val state = runtimeBotsByUuid[botUuid] ?: return null
        synchronized(state) {
            if (state.worldKey != worldKey) return null
            if (state.health <= 0f) return null
            return RuntimeBotSnapshot(
                entityId = state.entityId,
                x = state.x,
                y = state.y,
                z = state.z
            )
        }
    }

    fun damageRuntimeBot(
        worldKey: String,
        entityId: Int,
        amount: Float,
        attackerX: Double,
        attackerZ: Double,
        knockbackStrength: Double
    ): RuntimeBotDamageResult? {
        if (amount <= 0f) return null
        val botUuid = runtimeBotUuidByEntityId[entityId] ?: return null
        val state = runtimeBotsByUuid[botUuid] ?: return null
        val result = synchronized(state) {
            if (state.worldKey != worldKey) return null
            if (state.health <= 0f) return null
            if (!state.attackable || state.invulnerable) return null
            if (state.damageDelayRemainingSeconds > 1.0e-6) return null

            var knockbackApplied = false
            if (knockbackStrength > 0.0) {
                val dx = state.x - attackerX
                val dz = state.z - attackerZ
                val distSq = (dx * dx) + (dz * dz)
                if (distSq > 1.0e-9) {
                    val dist = kotlin.math.sqrt(distSq)
                    val nx = dx / dist
                    val nz = dz / dist
                    val impulse = knockbackStrength * 0.4
                    state.speedX += nx * impulse * 20.0
                    state.speedY += 0.15 * 20.0
                    state.speedZ += nz * impulse * 20.0
                    knockbackApplied = true
                }
            }

            state.health = (state.health - amount).coerceAtLeast(0f)
            state.damageDelayRemainingSeconds = state.damageDelaySeconds.coerceAtLeast(0.0)
            RuntimeBotDamageResult(
                x = state.x,
                y = state.y,
                z = state.z,
                knockbackApplied = knockbackApplied,
                died = state.health <= 0f
            )
        } ?: return null

        broadcastRuntimeBotDamage(worldKey, entityId, result.x, result.y, result.z)
        if (result.died) {
            removeRuntimeBot(botUuid)
        }
        return result
    }

    fun resolveTranslationForLocale(localeTag: String?, key: String, args: List<String> = emptyList()): String? {
        val normalizedKey = key.trim()
        if (normalizedKey.isEmpty()) return null
        synchronized(pluginStateLock) {
            for (loaded in pluginsInLoadOrder) {
                val translated = loaded.context.i18n.trFor(localeTag, normalizedKey, *args.toTypedArray())
                if (translated != normalizedKey) return translated
            }
        }
        return null
    }

    fun chunkTickScheduler(worldKey: String, chunkX: Int, chunkZ: Int): TickScheduler {
        return tickScheduler.chunkGlobal(worldKey, chunkX, chunkZ)
    }

    fun pluginIds(): List<String> {
        synchronized(pluginStateLock) {
            return pluginsInLoadOrder.map { it.metadata.id }
        }
    }

    fun pluginCompletionCandidates(): List<String> {
        synchronized(pluginStateLock) {
            val out = LinkedHashSet<String>()
            for (loaded in pluginsInLoadOrder) {
                val displayName = loaded.metadata.name.trim()
                if (displayName.isNotEmpty()) {
                    out += displayName
                } else {
                    out += loaded.metadata.id
                }
            }
            return out.toList()
        }
    }

    fun isPluginLoaded(pluginId: String): Boolean {
        synchronized(pluginStateLock) {
            return pluginsById.containsKey(pluginId.lowercase())
        }
    }

    fun pluginMetadata(pluginId: String): PluginMetadata? {
        synchronized(pluginStateLock) {
            return pluginsById[pluginId.lowercase()]?.metadata
        }
    }

    fun pluginContext(pluginId: String): PluginContext? {
        synchronized(pluginStateLock) {
            return pluginsById[pluginId.lowercase()]?.context
        }
    }

    fun reloadTargetNames(): List<String> {
        synchronized(pluginStateLock) {
            return pluginsInLoadOrder.asSequence()
                .map { plugin ->
                    plugin.metadata.name.takeIf { it.isNotBlank() } ?: plugin.metadata.id
                }
                .map { it.trim() }
                .filter { it.isNotEmpty() }
                .distinct()
                .sortedWith(String.CASE_INSENSITIVE_ORDER)
                .toList()
        }
    }

    fun reloadAll(): Int {
        synchronized(pluginStateLock) {
            val snapshot = pluginsInLoadOrder.map { it.metadata.id }
            var reloaded = 0
            for (pluginId in snapshot) {
                if (reloadPlugin(pluginId)) {
                    reloaded++
                }
            }
            return reloaded
        }
    }

    fun reloadPlugin(pluginId: String): Boolean {
        synchronized(pluginStateLock) {
            val normalized = pluginId.lowercase()
            val byId = pluginsById[normalized]
            if (byId != null) {
                return reloadPluginWithJar(byId.metadata.id, byId.jarPath)
            }
            val byNameMatches = pluginsInLoadOrder.filter {
                it.metadata.name.equals(pluginId, ignoreCase = true)
            }
            if (byNameMatches.size != 1) {
                return false
            }
            val existing = byNameMatches.first()
            return reloadPluginWithJar(existing.metadata.id, existing.jarPath)
        }
    }

    fun beforeCommandDispatch(sender: PlayerSession?, rawCommand: String): Boolean {
        val trimmed = rawCommand.removePrefix("/").trim()
        if (trimmed.isEmpty()) return true
        val parts = trimmed.split(Regex("\\s+"))
        if (parts.isEmpty()) return true
        val event = CommandDispatchEvent(
            sender = RuntimeCommandSender.fromSession(sender),
            commandLine = trimmed,
            commandName = parts[0].lowercase(),
            args = if (parts.size > 1) parts.subList(1, parts.size) else emptyList()
        )
        eventBus.post(event)
        return !event.cancelled
    }

    fun interceptPacket(packet: PacketEnvelope): PacketEnvelope? {
        return packetInterceptors.intercept(packet)
    }

    fun onPlayerJoin(session: PlayerSession, defaultMessage: String): String? {
        val event = PlayerJoinEvent(
            player = playerRef(session),
            message = defaultMessage
        )
        eventBus.post(event)
        return if (event.message == defaultMessage) null else event.message
    }

    fun onPlayerQuit(session: PlayerSession, defaultMessage: String): String? {
        val event = PlayerQuitEvent(
            player = playerRef(session),
            message = defaultMessage
        )
        eventBus.post(event)
        return if (event.message == defaultMessage) null else event.message
    }

    internal fun <T> withPluginOwner(owner: String, block: () -> T): T {
        val previous = activePluginOwner.get()
        activePluginOwner.set(owner)
        return try {
            block()
        } finally {
            if (previous == null) {
                activePluginOwner.remove()
            } else {
                activePluginOwner.set(previous)
            }
        }
    }

    internal fun <T> withPluginContext(owner: String, context: PluginContext, block: () -> T): T {
        val previousOwner = activePluginOwner.get()
        val previousContext = activePluginContext.get()
        activePluginOwner.set(owner)
        activePluginContext.set(context)
        return try {
            block()
        } finally {
            if (previousOwner == null) {
                activePluginOwner.remove()
            } else {
                activePluginOwner.set(previousOwner)
            }
            if (previousContext == null) {
                activePluginContext.remove()
            } else {
                activePluginContext.set(previousContext)
            }
        }
    }

    internal fun currentPluginOwner(): String? = activePluginOwner.get()

    internal fun currentPluginContext(): PluginContext? = activePluginContext.get()

    internal fun postPluginStateChange(
        context: PluginContext,
        previousState: PluginState,
        currentState: PluginState
    ) {
        eventBus.post(
            PluginStateChangeEvent(
                context = context,
                previousState = previousState,
                currentState = currentState
            )
        )
    }

    fun beforeBlockBreak(session: PlayerSession, x: Int, y: Int, z: Int): BlockBreakDecision {
        if (!beforePlayerInteract(
                session = session,
                action = PlayerInteractAction.LEFT_CLICK_BLOCK,
                hand = Hand.MAIN_HAND,
                clickedX = x,
                clickedY = y,
                clickedZ = z,
                faceId = null
            )
        ) {
            return BlockBreakDecision(
                proceed = false,
                modifiedByPlugin = false
            )
        }
        val world = runtimeWorldFor(session.worldKey)
        val blockRef = world.blockAt(x, y, z)
        val event = BlockBreakEvent(
            player = playerRef(session),
            block = blockRef
        )
        eventBus.post(event)
        val modifiedByPlugin = (blockRef as? RuntimeBlock)?.wasMutatedByPlugin() == true
        return BlockBreakDecision(
            proceed = !event.cancelled,
            modifiedByPlugin = modifiedByPlugin
        )
    }

    fun beforeBlockPlace(
        session: PlayerSession,
        x: Int,
        y: Int,
        z: Int,
        clickedX: Int,
        clickedY: Int,
        clickedZ: Int,
        hand: Int,
        faceId: Int
    ): Boolean {
        if (!beforePlayerInteract(
                session = session,
                action = PlayerInteractAction.RIGHT_CLICK_BLOCK,
                hand = Hand.fromId(hand),
                clickedX = clickedX,
                clickedY = clickedY,
                clickedZ = clickedZ,
                faceId = faceId
            )
        ) {
            return false
        }
        val world = runtimeWorldFor(session.worldKey)
        val event = BlockPlaceEvent(
            player = playerRef(session),
            block = world.blockAt(x, y, z),
            clickedBlock = world.blockAt(clickedX, clickedY, clickedZ),
            hand = Hand.fromId(hand),
            face = BlockFace.fromId(faceId)
        )
        eventBus.post(event)
        return !event.cancelled
    }

    fun beforePlayerInteract(
        session: PlayerSession,
        action: PlayerInteractAction,
        hand: Hand,
        clickedX: Int?,
        clickedY: Int?,
        clickedZ: Int?,
        faceId: Int?
    ): Boolean {
        val world = runtimeWorldFor(session.worldKey)
        val clickedBlock = if (clickedX != null && clickedY != null && clickedZ != null) {
            world.blockAt(clickedX, clickedY, clickedZ)
        } else {
            null
        }
        val face = faceId?.let(BlockFace::fromId)
        val event = PlayerInteractEvent(
            player = playerRef(session),
            hand = hand,
            item = handItem(session, hand),
            isRightClick = action.isRightClick,
            isLeftClick = action.isLeftClick,
            isBlockInteract = action.isBlock,
            isAirInteract = action.isAir,
            clickedBlock = clickedBlock,
            face = face
        )
        eventBus.post(event)
        return !event.cancelled
    }

    fun onPlayerJump(
        session: PlayerSession,
        x: Double,
        y: Double,
        z: Double,
        yaw: Float,
        pitch: Float,
        fromGround: Boolean,
        airJumpAttemptCount: Int
    ) {
        val world = runtimeWorldFor(session.worldKey)
        eventBus.post(
            PlayerJumpEvent(
                player = playerRef(session),
                location = Location(
                    world = world,
                    x = x,
                    y = y,
                    z = z,
                    yaw = yaw,
                    pitch = pitch
                ),
                fromGround = fromGround,
                airJumpAttemptCount = airJumpAttemptCount
            )
        )
    }

    fun beforePlayerMove(
        session: PlayerSession,
        toX: Double,
        toY: Double,
        toZ: Double,
        toYaw: Float,
        toPitch: Float,
        reason: MoveReason
    ): MoveDecision {
        val world = runtimeWorldFor(session.worldKey)
        val player = playerRef(session)
        val from = Location(
            world = world,
            x = session.x,
            y = session.y,
            z = session.z,
            yaw = session.yaw,
            pitch = session.pitch
        )
        val to = Location(
            world = world,
            x = toX,
            y = toY,
            z = toZ,
            yaw = toYaw,
            pitch = toPitch
        )
        val fromMedium = resolveMoveMedium(player, from)
        val toMedium = resolveMoveMedium(player, to)
        val sameTransform = from.world.key == to.world.key &&
            from.x == to.x &&
            from.y == to.y &&
            from.z == to.z &&
            from.yaw == to.yaw &&
            from.pitch == to.pitch
        if (sameTransform && fromMedium == toMedium) {
            return MoveDecision(cancelPosition = false, cancelRotation = false)
        }
        val entityDecision = beforeEntityMove(player, from, to, reason)
        if (entityDecision.cancelAll) {
            return entityDecision
        }
        val isRotationOnly = from.world.key == to.world.key &&
            from.x == to.x &&
            from.y == to.y &&
            from.z == to.z &&
            (from.yaw != to.yaw || from.pitch != to.pitch)
        val deltaX = to.x - from.x
        val deltaY = to.y - from.y
        val deltaZ = to.z - from.z
        val rawDeltaYaw = to.yaw - from.yaw
        val deltaYaw = wrapDegrees(rawDeltaYaw)
        val deltaPitch = (to.pitch - from.pitch)
        val event = PlayerMoveEvent(
            player = player,
            from = from,
            to = to,
            reason = reason,
            fromMedium = fromMedium,
            toMedium = toMedium,
            isRotationOnly = isRotationOnly,
            deltaX = deltaX,
            deltaY = deltaY,
            deltaZ = deltaZ,
            rawDeltaYaw = rawDeltaYaw,
            deltaYaw = deltaYaw,
            deltaPitch = deltaPitch
        )
        eventBus.post(event)
        return MoveDecision(
            cancelPosition = event.cancelled || event.cancelPosition || entityDecision.cancelPosition,
            cancelRotation = event.cancelled || event.cancelRotation || entityDecision.cancelRotation
        )
    }

    fun beforeEntityMove(
        entity: Entity,
        from: Location,
        to: Location,
        reason: MoveReason
    ): MoveDecision {
        val sameTransform = from.world.key == to.world.key &&
            from.x == to.x &&
            from.y == to.y &&
            from.z == to.z &&
            from.yaw == to.yaw &&
            from.pitch == to.pitch
        val fromMedium = resolveMoveMedium(entity, from)
        val toMedium = resolveMoveMedium(entity, to)
        if (sameTransform && fromMedium == toMedium) {
            return MoveDecision(cancelPosition = false, cancelRotation = false)
        }
        val isRotationOnly = from.world.key == to.world.key &&
            from.x == to.x &&
            from.y == to.y &&
            from.z == to.z &&
            (from.yaw != to.yaw || from.pitch != to.pitch)
        val deltaX = to.x - from.x
        val deltaY = to.y - from.y
        val deltaZ = to.z - from.z
        val rawDeltaYaw = to.yaw - from.yaw
        val deltaYaw = wrapDegrees(rawDeltaYaw)
        val deltaPitch = (to.pitch - from.pitch)
        val event = EntityMoveEvent(
            entity = entity,
            from = from,
            to = to,
            reason = reason,
            fromMedium = fromMedium,
            toMedium = toMedium,
            isRotationOnly = isRotationOnly,
            deltaX = deltaX,
            deltaY = deltaY,
            deltaZ = deltaZ,
            rawDeltaYaw = rawDeltaYaw,
            deltaYaw = deltaYaw,
            deltaPitch = deltaPitch
        )
        eventBus.post(event)
        return MoveDecision(
            cancelPosition = event.cancelled || event.cancelPosition,
            cancelRotation = event.cancelled || event.cancelRotation
        )
    }

    fun beforeBlockChange(
        session: PlayerSession?,
        worldKey: String,
        x: Int,
        y: Int,
        z: Int,
        previousStateId: Int,
        changedStateId: Int,
        reason: BlockChangeReason
    ): Boolean {
        if (previousStateId == changedStateId) return true
        val world = runtimeWorldFor(worldKey)
        val location = Location(
            world = world,
            x = x.toDouble(),
            y = y.toDouble(),
            z = z.toDouble(),
            yaw = 0f,
            pitch = 0f
        )
        val event = BlockChangeEvent(
            player = session?.let(::playerRef),
            world = world,
            location = location,
            block = world.blockAt(x, y, z),
            reason = reason,
            previous = blockSnapshot(previousStateId),
            changed = blockSnapshot(changedStateId)
        )
        eventBus.post(event)
        return !event.cancelled
    }

    private fun handItem(session: PlayerSession, hand: Hand): Item? {
        val (itemId, amount) = when (hand) {
            Hand.MAIN_HAND -> {
                val slot = session.selectedHotbarSlot.coerceIn(0, 8)
                session.hotbarItemIds[slot] to session.hotbarItemCounts[slot]
            }
            Hand.OFF_HAND -> session.offhandItemId to session.offhandItemCount
        }
        if (itemId < 0 || amount <= 0) return null
        return Item(
            id = itemId,
            type = ItemType.fromId(itemId),
            amount = amount
        )
    }

    private fun playerRef(session: PlayerSession): ConnectedPlayer {
        return RuntimeConnectedPlayer.fromSession(session)
    }

    private fun blockSnapshot(stateId: Int): BlockSnapshot {
        val parsed = BlockStateRegistry.parsedState(stateId)
        val blockKey = parsed?.blockKey ?: "minecraft:air"
        val properties = parsed?.properties ?: emptyMap()
        val blockType = runtimeTypeRegistry.blockByKey(blockKey) ?: runtimeFallbackAirBlockType
        return BlockSnapshot(
            type = blockType,
            state = RuntimeBlockState(
                id = stateId,
                key = buildStateKey(blockKey, properties),
                properties = properties
            )
        )
    }

    private fun resolveMoveMedium(entity: Entity, location: Location): MoveMedium {
        if (!location.x.isFinite() || !location.y.isFinite() || !location.z.isFinite()) return MoveMedium.UNKNOWN
        val playerSession = if (entity is Player) PlayerSessionManager.byUuid(entity.uniqueId) else null
        if (playerSession != null) {
            if (playerSession.ridingAnimalEntityId >= 0) return MoveMedium.PLAYER_RIDING
            if (playerSession.flying) return MoveMedium.PLAYER_FLYING
            if (playerSession.swimming) return MoveMedium.PLAYER_SWIMMING
        }
        val blockState = runCatching { location.world.blockAt(location).state }.getOrNull() ?: return MoveMedium.UNKNOWN
        val blockKey = blockState.key.substringBefore('[')
        if (blockKey.startsWith("minecraft:water") || blockState.properties["waterlogged"] == "true") {
            return MoveMedium.WATER
        }
        if (blockKey.startsWith("minecraft:lava")) {
            return MoveMedium.LAVA
        }
        if (blockKey in CLIMBABLE_MEDIUM_BLOCK_KEYS) {
            return MoveMedium.CLIMBING
        }
        return if (entity.onGround) MoveMedium.GROUND else MoveMedium.AIR
    }

    private fun loadAllFromDisk() {
        val pluginsDir = Path.of("plugins")
        runCatching { Files.createDirectories(pluginsDir) }
        val jarPaths = runCatching {
            Files.list(pluginsDir).use { stream ->
                stream.filter { Files.isRegularFile(it) && it.fileName.toString().endsWith(".jar", ignoreCase = true) }
                    .sorted()
                    .toList()
            }
        }.getOrElse {
            logger.error("Failed to list plugin jars", it)
            emptyList()
        }
        for (jarPath in jarPaths) {
            val descriptorPreview = runCatching { readDescriptor(jarPath) }.getOrNull()
            val pluginDisplayName = descriptorPreview?.name?.takeIf { it.isNotBlank() }
                ?: descriptorPreview?.id?.lowercase()
                ?: jarPath.fileName.toString()
            val pluginIdLabel = descriptorPreview?.id?.lowercase() ?: jarPath.fileName.toString()
            val progressMessage = ServerI18n.tr("aerogel.log.plugin.load.progress", pluginDisplayName)
            runCatching {
                DebugConsole.withSpinnerResult(
                    progressMessage = progressMessage,
                    doneMessage = { outcome ->
                        when (outcome) {
                            PluginLoadOutcome.ENABLED -> if (descriptorPreview == null) {
                                ServerI18n.tr("aerogel.log.plugin.load.done.unknown", jarPath.fileName.toString())
                            } else {
                                ServerI18n.tr(
                                    "aerogel.log.plugin.load.done",
                                    pluginDisplayName
                                )
                            }

                            PluginLoadOutcome.SKIPPED_ALREADY_LOADED -> {
                                ServerI18n.tr("aerogel.log.plugin.load.skipped.already_loaded", pluginIdLabel)
                            }

                            PluginLoadOutcome.SKIPPED_MISSING_DEPENDENCIES -> {
                                ServerI18n.tr("aerogel.log.plugin.load.skipped.missing_dependencies", pluginIdLabel)
                            }
                        }
                    },
                    doneMark = { outcome ->
                        when (outcome) {
                            PluginLoadOutcome.ENABLED -> "✓"
                            PluginLoadOutcome.SKIPPED_ALREADY_LOADED -> "!"
                            PluginLoadOutcome.SKIPPED_MISSING_DEPENDENCIES -> "✗"
                        }
                    }
                ) {
                    loadPluginJar(jarPath, PluginLoadReason.STARTUP)
                }
            }
                .onFailure { logger.error("Failed to load plugin jar: {}", jarPath, it) }
        }
    }

    private fun reloadPluginWithJar(pluginId: String, jarPath: Path): Boolean {
        val existing = pluginsById[pluginId] ?: return false
        val dependents = dependentPluginsOf(pluginId)
        for (dependent in dependents.asReversed()) {
            disablePlugin(dependent, PluginDisableReason.DEPENDENCY_RELOAD)
        }
        disablePlugin(existing, PluginDisableReason.RELOAD)
        val primaryReloaded = runCatching { loadPluginJar(jarPath, PluginLoadReason.RELOAD) == PluginLoadOutcome.ENABLED }
            .onFailure { logger.error("Failed to reload plugin '{}' from {}", pluginId, jarPath, it) }
            .getOrDefault(false)
        if (!primaryReloaded) {
            return false
        }
        for (dependent in dependents) {
            runCatching { loadPluginJar(dependent.jarPath, PluginLoadReason.DEPENDENCY_RELOAD) == PluginLoadOutcome.ENABLED }
                .onFailure { logger.error("Failed to reload dependent plugin '{}'", dependent.metadata.id, it) }
        }
        return true
    }

    private fun handlePluginJarChanged(jarPath: Path): Boolean {
        synchronized(pluginStateLock) {
            val descriptor = runCatching { readDescriptor(jarPath) }
                .onFailure { logger.error("Failed to parse changed plugin descriptor: {}", jarPath, it) }
                .getOrNull() ?: return false
            val pluginId = descriptor.id.lowercase()
            val loaded = pluginsById[pluginId] ?: return false
            val reloaded = runCatching { reloadPluginWithJar(pluginId, jarPath) }
                .onFailure { logger.error("Automatic plugin hot-reload failed for '{}'", pluginId, it) }
                .getOrDefault(false)
            val pluginName = loaded.metadata.name.ifBlank { loaded.metadata.id }
            if (reloaded) {
                ServerI18n.logCustom(
                    ServerI18n.style("[시스템] ", ServerI18n.Color.GREEN),
                    ServerI18n.style(ServerI18n.tr("aerogel.log.plugin.hotreload.applied", pluginName), ServerI18n.Color.GREEN)
                )
            } else {
                ServerI18n.logCustom(
                    ServerI18n.style("[시스템] ", ServerI18n.Color.RED),
                    ServerI18n.style(ServerI18n.tr("aerogel.log.plugin.hotreload.failed", pluginName), ServerI18n.Color.RED)
                )
            }
            return reloaded
        }
    }

    private fun loadPluginJar(jarPath: Path, loadReason: PluginLoadReason): PluginLoadOutcome {
        val descriptor = readDescriptor(jarPath)
        val pluginId = descriptor.id.lowercase()
        if (pluginsById.containsKey(pluginId)) {
            logger.warn("Plugin '{}' is already loaded. Skipping {}", pluginId, jarPath)
            return PluginLoadOutcome.SKIPPED_ALREADY_LOADED
        }

        val missingDeps = descriptor.dependencies.filter { dep -> !pluginsById.containsKey(dep.lowercase()) }
        if (missingDeps.isNotEmpty()) {
            logger.warn("Plugin '{}' skipped due to missing dependencies: {}", pluginId, missingDeps.joinToString(","))
            return PluginLoadOutcome.SKIPPED_MISSING_DEPENDENCIES
        }

        val classLoader = PluginClassLoader(
            urls = arrayOf(jarPath.toUri().toURL()),
            parent = PluginSystem::class.java.classLoader
        )
        try {
            MixinWeavingEngine.registerPluginMixins(
                owner = pluginId,
                classLoader = classLoader,
                pluginJar = jarPath
            )

            val pluginClass = classLoader.loadClass(descriptor.mainClass)
            if (!AerogelPlugin::class.java.isAssignableFrom(pluginClass)) {
                throw IllegalStateException("Main class ${descriptor.mainClass} does not implement AerogelPlugin")
            }

            val plugin = pluginClass.getDeclaredConstructor().newInstance() as AerogelPlugin
            val metadata = PluginMetadata(
                id = pluginId,
                name = descriptor.name.ifBlank { pluginId },
                version = descriptor.version,
                apiVersion = descriptor.apiVersion,
                dependencies = descriptor.dependencies,
                softDependencies = descriptor.softDependencies
            )

            val loggerFacade = Slf4jPluginLogger(metadata)
            val dataStore = AsyncPluginDataStore(pluginId)
            val i18n = RuntimePluginI18n(
                catalogs = loadPluginLangCatalogs(jarPath),
                localeResolver = { player ->
                    PlayerSessionManager.byUuid(player.uniqueId)?.locale
                }
            )
            val context = RuntimePluginContext(
                metadata = metadata,
                state = PluginState.LOADING,
                logger = loggerFacade,
                events = eventBus,
                commands = RuntimeCommandRegistrar(pluginId, permissionService),
                scheduler = scheduler.ownerScoped(pluginId),
                tickScheduler = tickScheduler.ownerScoped(pluginId),
                services = serviceRegistry,
                permissions = permissionService,
                dataStore = dataStore,
                packets = packetInterceptors,
                types = runtimeTypeRegistry,
                worlds = runtimeWorldRegistry,
                plugins = runtimePluginRegistry,
                i18n = i18n
            )

            val loaded = LoadedPlugin(
                metadata = metadata,
                plugin = plugin,
                context = context,
                classLoader = classLoader,
                jarPath = jarPath
            )

            try {
                context.updateState(PluginState.LOADING)
                withPluginContext(pluginId, context) { plugin.onLoad(context) }
                context.updateState(PluginState.LOADED)
                val commandRegistrar = context.commands as RuntimeCommandRegistrar
                eventBus.registerAnnotated(pluginId, plugin)
                commandRegistrar.registerAnnotated(pluginId, plugin)
                val registeredListenerClasses = HashSet<String>()
                registeredListenerClasses.add(plugin.javaClass.name)
                for (listener in plugin.listeners()) {
                    eventBus.registerAnnotated(pluginId, listener)
                    commandRegistrar.registerAnnotated(pluginId, listener)
                    registeredListenerClasses.add(listener.javaClass.name)
                }
                val autoListeners = discoverAutoListeners(
                    classLoader = classLoader,
                    pluginJar = jarPath,
                    mainClassName = descriptor.mainClass,
                    excludedClassNames = registeredListenerClasses
                )
                for (listener in autoListeners) {
                    eventBus.registerAnnotated(pluginId, listener)
                    commandRegistrar.registerAnnotated(pluginId, listener)
                }
                val registeredCommandHandlerClasses = HashSet<String>()
                registeredCommandHandlerClasses.add(plugin.javaClass.name)
                registeredCommandHandlerClasses.addAll(registeredListenerClasses)
                val autoCommandHandlers = discoverAutoCommandHandlers(
                    classLoader = classLoader,
                    pluginJar = jarPath,
                    mainClassName = descriptor.mainClass,
                    excludedClassNames = registeredCommandHandlerClasses
                )
                for (handler in autoCommandHandlers) {
                    commandRegistrar.registerAnnotated(pluginId, handler)
                }
                context.updateState(PluginState.ENABLING)
                withPluginContext(pluginId, context) { plugin.onEnable(context) }
                context.updateState(PluginState.ENABLED)
                if (loadReason != PluginLoadReason.STARTUP) {
                    runCatching { withPluginContext(pluginId, context) { plugin.onReload(context, loadReason.toPluginReloadReason()) } }
                        .onFailure { logger.error("Plugin '{}' onReload failed", pluginId, it) }
                }
                pluginsById[pluginId] = loaded
                pluginsInLoadOrder.add(loaded)
                return PluginLoadOutcome.ENABLED
            } catch (error: Throwable) {
                context.updateState(PluginState.FAILED)
                runCatching { withPluginContext(pluginId, context) { plugin.onDisable(context, PluginDisableReason.LOAD_FAILURE) } }
                cleanupPluginResources(loaded)
                throw error
            }
        } catch (error: Throwable) {
            MixinWeavingEngine.unregisterOwner(pluginId)
            runCatching { classLoader.close() }
            throw error
        }
    }

    private fun discoverAutoListeners(
        classLoader: ClassLoader,
        pluginJar: Path,
        mainClassName: String,
        excludedClassNames: Set<String>
    ): List<Any> {
        val out = ArrayList<Any>()
        val classNames = runCatching {
            FileSystems.newFileSystem(pluginJar).use { fs ->
                Files.walk(fs.getPath("/")).use { stream ->
                    stream
                        .filter { Files.isRegularFile(it) && it.toString().endsWith(".class") }
                        .map { path ->
                            val normalized = path.toString().removePrefix("/").removeSuffix(".class")
                            normalized.replace('/', '.')
                        }
                        .filter { name -> name.isNotBlank() && !name.contains("module-info") }
                        .toList()
                }
            }
        }.getOrElse { error ->
            logger.warn("Failed to scan plugin classes for auto listeners: {}", pluginJar, error)
            return emptyList()
        }

        for (className in classNames) {
            if (className == mainClassName) continue
            if (excludedClassNames.contains(className)) continue

            val clazz = runCatching { Class.forName(className, false, classLoader) }.getOrNull() ?: continue
            if (clazz.isInterface || clazz.isAnnotation || clazz.isEnum || Modifier.isAbstract(clazz.modifiers)) continue

            val hasSubscribeMethod = runCatching {
                clazz.methods.any { method -> method.getAnnotation(Subscribe::class.java) != null }
            }.getOrDefault(false)
            if (!hasSubscribeMethod) continue

            val instance = createListenerInstance(clazz) ?: continue
            out.add(instance)
        }
        return out
    }

    private fun discoverAutoCommandHandlers(
        classLoader: ClassLoader,
        pluginJar: Path,
        mainClassName: String,
        excludedClassNames: Set<String>
    ): List<Any> {
        val out = ArrayList<Any>()
        val classNames = runCatching {
            FileSystems.newFileSystem(pluginJar).use { fs ->
                Files.walk(fs.getPath("/")).use { stream ->
                    stream
                        .filter { Files.isRegularFile(it) && it.toString().endsWith(".class") }
                        .map { path ->
                            val normalized = path.toString().removePrefix("/").removeSuffix(".class")
                            normalized.replace('/', '.')
                        }
                        .filter { name -> name.isNotBlank() && !name.contains("module-info") }
                        .toList()
                }
            }
        }.getOrElse { error ->
            logger.warn("Failed to scan plugin classes for auto commands: {}", pluginJar, error)
            return emptyList()
        }

        for (className in classNames) {
            if (className == mainClassName) continue
            if (excludedClassNames.contains(className)) continue

            val clazz = runCatching { Class.forName(className, false, classLoader) }.getOrNull() ?: continue
            if (clazz.isInterface || clazz.isAnnotation || clazz.isEnum || Modifier.isAbstract(clazz.modifiers)) continue

            val hasCommandMethod = runCatching {
                clazz.methods.any { method -> method.getAnnotation(ApiCommand::class.java) != null }
            }.getOrDefault(false)
            if (!hasCommandMethod) continue

            val instance = createListenerInstance(clazz) ?: continue
            out.add(instance)
        }
        return out
    }

    private fun createListenerInstance(clazz: Class<*>): Any? {
        // Kotlin object singleton support.
        runCatching {
            val instanceField = clazz.getDeclaredField("INSTANCE")
            if (Modifier.isStatic(instanceField.modifiers)) {
                instanceField.isAccessible = true
                return instanceField.get(null)
            }
        }

        if (clazz.enclosingClass != null && !Modifier.isStatic(clazz.modifiers)) {
            // Non-static inner classes require outer instance; skip.
            return null
        }

        return runCatching {
            val ctor = clazz.getDeclaredConstructor()
            ctor.isAccessible = true
            ctor.newInstance()
        }.getOrNull()
    }

    private fun disableAll() {
        val reverse = pluginsInLoadOrder.toList().asReversed()
        for (plugin in reverse) {
            disablePlugin(plugin, PluginDisableReason.SHUTDOWN)
        }
    }

    private fun PluginLoadReason.toPluginReloadReason(): PluginReloadReason {
        return when (this) {
            PluginLoadReason.STARTUP -> PluginReloadReason.COMMAND_RELOAD
            PluginLoadReason.RELOAD -> PluginReloadReason.COMMAND_RELOAD
            PluginLoadReason.DEPENDENCY_RELOAD -> PluginReloadReason.DEPENDENCY_RELOAD
            PluginLoadReason.DEV_HOT_RELOAD -> PluginReloadReason.DEV_HOT_RELOAD
        }
    }

    private fun dependentPluginsOf(pluginId: String): List<LoadedPlugin> {
        val result = LinkedHashSet<LoadedPlugin>()

        fun visit(currentId: String) {
            for (candidate in pluginsInLoadOrder) {
                if (candidate.metadata.id == currentId) continue
                if (candidate.metadata.dependencies.none { it.equals(currentId, ignoreCase = true) }) continue
                if (result.add(candidate)) {
                    visit(candidate.metadata.id)
                }
            }
        }

        visit(pluginId)
        return result.toList()
    }

    private fun disablePlugin(plugin: LoadedPlugin, reason: PluginDisableReason) {
        pluginsById.remove(plugin.metadata.id)
        pluginsInLoadOrder.remove(plugin)

        plugin.context.updateState(PluginState.DISABLING)
        runCatching { withPluginContext(plugin.metadata.id, plugin.context) { plugin.plugin.onDisable(plugin.context, reason) } }
            .onFailure { logger.error("Plugin '{}' onDisable failed", plugin.metadata.id, it) }
        plugin.context.updateState(PluginState.DISABLED)

        cleanupPluginResources(plugin)
    }

    private fun cleanupPluginResources(plugin: LoadedPlugin) {
        val pluginId = plugin.metadata.id
        eventBus.unregisterOwner(pluginId)
        packetInterceptors.unregisterOwner(pluginId)
        serviceRegistry.invalidateOwner(pluginId)
        MixinWeavingEngine.unregisterOwner(pluginId)
        permissionService.clearOwner(pluginId)
        scheduler.shutdownOwner(pluginId)
        tickScheduler.shutdownOwner(pluginId)
        (plugin.context.commands as RuntimeCommandRegistrar).close()
        (plugin.context.dataStore as? AsyncPluginDataStore)?.shutdown()
        runCatching { plugin.classLoader.close() }
    }

    private fun readDescriptor(jarPath: Path): PluginJarDescriptor {
        val fs = FileSystems.newFileSystem(jarPath)
        fs.use {
            val descriptorPath = it.getPath("/aerogel-plugin.json")
            if (!Files.exists(descriptorPath)) {
                throw IllegalStateException("Missing aerogel-plugin.json in $jarPath")
            }
            val text = Files.readString(descriptorPath)
            val descriptor = json.decodeFromString<PluginJarDescriptor>(text)
            if (descriptor.id.isBlank()) throw IllegalStateException("Plugin id is blank in $jarPath")
            if (descriptor.mainClass.isBlank()) throw IllegalStateException("Plugin mainClass is blank in $jarPath")
            return descriptor
        }
    }

    @Serializable
    private data class PluginJarDescriptor(
        val id: String,
        val name: String = "",
        val version: String,
        val apiVersion: String,
        val mainClass: String,
        val dependencies: List<String> = emptyList(),
        val softDependencies: List<String> = emptyList()
    )

    private data class LoadedPlugin(
        val metadata: PluginMetadata,
        val plugin: AerogelPlugin,
        val context: RuntimePluginContext,
        val classLoader: PluginClassLoader,
        val jarPath: Path
    )
}

private class RuntimePluginContext(
    override val metadata: PluginMetadata,
    state: PluginState,
    override val logger: PluginLogger,
    override val events: EventBus,
    override val commands: CommandRegistrar,
    override val scheduler: TaskScheduler,
    override val taskScheduler: TaskScheduler = scheduler,
    override val tickScheduler: TickScheduler,
    override val services: ServiceRegistry,
    override val permissions: PermissionService,
    override val dataStore: PluginDataStore,
    override val packets: PacketInterceptorRegistry,
    override val types: TypeRegistry,
    override val worlds: WorldRegistry,
    override val plugins: PluginRegistry,
    override val i18n: PluginI18n
) : PluginContext {
    @Volatile
    private var currentState: PluginState = state

    override val state: PluginState
        get() = currentState

    fun updateState(state: PluginState) {
        val previous = currentState
        if (previous == state) return
        currentState = state
        PluginSystem.postPluginStateChange(this, previous, state)
    }
}

private fun loadPluginLangCatalogs(pluginJar: Path): Map<String, Map<String, String>> {
    val out = LinkedHashMap<String, Map<String, String>>()
    val parser = Json { ignoreUnknownKeys = true }
    runCatching {
        FileSystems.newFileSystem(pluginJar).use { fs ->
            val langRoot = fs.getPath("/lang")
            if (!Files.exists(langRoot)) return@use
            Files.walk(langRoot, 1).use { stream ->
                stream.filter { path ->
                    path != langRoot &&
                        Files.isRegularFile(path) &&
                        path.fileName.toString().endsWith(".json", ignoreCase = true)
                }.forEach { file ->
                    val fileName = file.fileName.toString()
                    val locale = fileName.removeSuffix(".json").lowercase()
                    if (locale.isBlank()) return@forEach
                    val jsonText = Files.readString(file)
                    val root = runCatching { parser.parseToJsonElement(jsonText).jsonObject }.getOrNull() ?: return@forEach
                    val catalog = LinkedHashMap<String, String>(root.size)
                    for ((key, value) in root) {
                        val primitive = value.jsonPrimitive
                        if (!primitive.isString) continue
                        catalog[key] = primitive.content
                    }
                    out[locale] = catalog
                }
            }
        }
    }
    return out
}

private class RuntimePluginI18n(
    private val catalogs: Map<String, Map<String, String>>,
    private val localeResolver: (Player) -> String?
) : PluginI18n {
    override fun tr(key: String, vararg args: String): String {
        return trFor(localeTag = null, key = key, *args)
    }

    override fun trFor(localeTag: String?, key: String, vararg args: String): String {
        val template = resolveTemplate(localeTag, key)
        return runCatching { String.format(java.util.Locale.ROOT, template, *args) }
            .getOrElse { if (args.isEmpty()) template else "$template ${args.joinToString(" ")}" }
    }

    override fun tr(player: Player, key: String, vararg args: String): String {
        return trFor(localeResolver(player), key, *args)
    }

    private fun resolveTemplate(localeTag: String?, key: String): String {
        val locale = normalizeLocale(localeTag)
        val primary = locale?.let { catalogs[it] }
        if (primary != null && key in primary) return primary.getValue(key)
        val lang = locale?.substringBefore('_')
        val byLang = lang?.let { catalogs[it] }
        if (byLang != null && key in byLang) return byLang.getValue(key)
        val english = catalogs["en_us"] ?: catalogs["en"]
        if (english != null && key in english) return english.getValue(key)
        return key
    }

    private fun normalizeLocale(localeTag: String?): String? {
        if (localeTag == null) return null
        val normalized = localeTag.trim().lowercase()
            .replace('-', '_')
        return normalized.takeIf { it.isNotBlank() }
    }
}

private val runtimeTypeRegistry: TypeRegistry by lazy { RuntimeTypeRegistry() }
private val runtimeWorldRegistry: WorldRegistry by lazy { RuntimeWorldRegistry() }
private val runtimePlayerRegistry: PlayerRegistry by lazy { RuntimePlayerRegistry() }
private val runtimePluginRegistry: PluginRegistry by lazy { RuntimePluginRegistry() }
private val runtimeFallbackAirBlockType: BlockType by lazy {
    BlockType.fromKey("minecraft:air") ?: BlockType.entries.first()
}
private val runtimeBotsByUuid = ConcurrentHashMap<UUID, RuntimeBotState>()
private val runtimeBotUuidByEntityId = ConcurrentHashMap<Int, UUID>()
private val runtimeBotViewerStatesByChannelId = ConcurrentHashMap<ChannelId, MutableMap<UUID, RuntimeBotViewerState>>()

private data class RuntimeBotState(
    val uuid: UUID,
    val entityId: Int,
    var worldKey: String,
    var x: Double,
    var y: Double,
    var z: Double,
    var yaw: Float,
    var pitch: Float,
    var onGround: Boolean,
    var speedX: Double,
    var speedY: Double,
    var speedZ: Double,
    var name: String,
    var tabList: Boolean,
    var pushable: Boolean,
    var attackable: Boolean,
    var gravity: Boolean,
    var collision: Boolean,
    var invulnerable: Boolean,
    var fallDamage: Boolean,
    var damageDelaySeconds: Double,
    var damageDelayRemainingSeconds: Double,
    var maxHealth: Float,
    var health: Float,
    var accelerationX: Double,
    var accelerationY: Double,
    var accelerationZ: Double
)

private data class RuntimeBotViewerState(
    val entityId: Int,
    var listed: Boolean,
    var name: String,
    var x: Double,
    var y: Double,
    var z: Double,
    var yaw: Float,
    var pitch: Float,
    var onGround: Boolean
)

data class RuntimeBotSnapshot(
    val entityId: Int,
    val x: Double,
    val y: Double,
    val z: Double
)

data class RuntimeBotDamageResult(
    val x: Double,
    val y: Double,
    val z: Double,
    val knockbackApplied: Boolean,
    val died: Boolean
)

private class RuntimeWorldRegistry : WorldRegistry {
    override fun defaultWorld(): World {
        return runtimeWorldFor(WorldManager.defaultWorld().key)
    }

    override fun world(key: String): World? {
        if (WorldManager.world(key) == null) return null
        return runtimeWorldFor(key)
    }

    override fun allWorlds(): List<World> {
        return WorldManager.allWorlds()
            .asSequence()
            .map { runtimeWorldFor(it.key) }
            .toList()
    }
}

private class RuntimePlayerRegistry : PlayerRegistry {
    override fun player(uuid: UUID): Player {
        val session = PlayerSessionManager.byUuid(uuid)
        return if (session != null) {
            RuntimeConnectedPlayer.fromSession(session)
        } else {
            RuntimePlayerReference.offlineReference(
                playerUuid = uuid,
                fallbackName = null
            )
        }
    }

    override fun player(name: String): Player? {
        val session = PlayerSessionManager.byName(name)
        return if (session != null) {
            RuntimeConnectedPlayer.fromSession(session)
        } else {
            null
        }
    }
}

private class RuntimePluginRegistry : PluginRegistry {
    override fun isLoaded(pluginId: String): Boolean {
        return PluginSystem.isPluginLoaded(pluginId)
    }

    override fun metadata(pluginId: String): PluginMetadata? {
        return PluginSystem.pluginMetadata(pluginId)
    }

    override fun context(pluginId: String): PluginContext? {
        return PluginSystem.pluginContext(pluginId)
    }

    override fun loadedPluginIds(): List<String> {
        return PluginSystem.pluginIds()
    }
}

private class RuntimeTypeRegistry : TypeRegistry {
    override fun itemById(id: Int): ItemType? = ItemType.fromId(id)

    override fun itemByKey(key: String): ItemType? = ItemType.fromKey(key)

    override fun blockById(id: Int): BlockType? = BlockType.fromId(id)

    override fun blockByKey(key: String): BlockType? = BlockType.fromKey(key)

    override fun allItems(): List<ItemType> = ItemType.entries

    override fun allBlocks(): List<BlockType> = BlockType.entries
}

private class Slf4jPluginLogger(
    private val metadata: PluginMetadata
) : PluginLogger {
    private val displayName = metadata.name.ifBlank { metadata.id }

    override fun info(message: String) {
        logLine(
            levelLabel = ServerI18n.tr("aerogel.log.level.info"),
            message = message
        )
    }

    override fun warn(message: String) {
        logLine(
            levelLabel = ServerI18n.tr("aerogel.log.level.warn"),
            message = message
        )
    }

    override fun error(message: String, error: Throwable?) {
        logLine(
            levelLabel = ServerI18n.tr("aerogel.log.level.error"),
            message = message
        )
        if (error != null) {
            DebugConsole.errThrowable(
                prefix = "[${ServerI18n.tr("aerogel.log.level.error")}/$displayName]",
                throwable = error
            )
        }
    }

    private fun logLine(levelLabel: String, message: String) {
        ServerI18n.logCustom(
            ServerI18n.style("["),
            ServerI18n.style(levelLabel),
            ServerI18n.style("/"),
            ServerI18n.style(displayName),
            ServerI18n.style("] "),
            ServerI18n.style(message)
        )
    }
}

private interface PermissionAwareSender {
    fun permissionSubject(): String
    fun baseHasPermission(node: String): Boolean
}

private class RuntimeConnectedPlayer(
    private val playerUuid: UUID
) : ConnectedPlayer(playerUuid) {

    private fun liveSession(): PlayerSession {
        return checkNotNull(PlayerSessionManager.byUuid(playerUuid)) { "Player session is no longer connected: $playerUuid" }
    }

    override val name: String
        get() = liveSession().profile.username

    override val world: World
        get() = runtimeWorldFor(liveSession().worldKey)

    override val online: Boolean
        get() = true

    override val inventory: PlayerInventory
        get() {
            val session = liveSession()
            return PlayerInventory(
                selectedHotbarSlot = session.selectedHotbarSlot.coerceIn(0, 8),
                hotbar = InventorySlotView(
                    size = session.hotbarItemIds.size,
                    getter = { slot ->
                        val live = liveSession()
                        inventoryStackOf(live.hotbarItemIds[slot], live.hotbarItemCounts[slot])
                    },
                    setter = { slot, item ->
                        setHotbarItem(slot, item)
                    }
                ),
                main = InventorySlotView(
                    size = session.mainInventoryItemIds.size,
                    getter = { index ->
                        val live = liveSession()
                        inventoryStackOf(live.mainInventoryItemIds[index], live.mainInventoryItemCounts[index])
                    },
                    setter = { slot, item ->
                        setMainItem(slot, item)
                    }
                ),
                armor = InventorySlotView(
                    size = session.armorItemIds.size,
                    getter = { armor ->
                        val live = liveSession()
                        inventoryStackOf(live.armorItemIds[armor], live.armorItemCounts[armor])
                    },
                    setter = { armorIndex, item ->
                        val slot = ArmorSlot.fromArmorIndex(armorIndex) ?: return@InventorySlotView false
                        setArmorItem(slot, item)
                    }
                ),
                offhand = inventoryStackOf(session.offhandItemId, session.offhandItemCount)
            )
        }

    override val settings: ConnectedPlayer.PlayerSettings
        get() {
            val session = liveSession()
            return ConnectedPlayer.PlayerSettings(
                locale = session.locale,
                viewDistance = session.requestedViewDistance
            )
        }

    override val op: Boolean
        get() = PlayerSessionManager.isOperatorSession(liveSession())

    override val gameMode: GameMode
        get() = GameMode.fromId(liveSession().gameMode)

    override val sprinting: Boolean
        get() = liveSession().sprinting

    override val swimming: Boolean
        get() = liveSession().swimming

    override val flying: Boolean
        get() = liveSession().flying

    override val sneaking: Boolean
        get() = liveSession().sneaking

    override val pingMs: Double
        get() = liveSession().pingMsExact

    override val fallDistance: Double
        get() = liveSession().fallDistance

    override val dead: Boolean
        get() = liveSession().dead

    override var location: Location
        get() {
            val session = liveSession()
            val world = runtimeWorldFor(session.worldKey)
            return Location(
                world = world,
                x = session.x,
                y = session.y,
                z = session.z,
                yaw = session.yaw,
                pitch = session.pitch
            )
        }
        set(value) {
            val session = liveSession()
            check(
                PlayerSessionManager.setPlayerLocation(
                    uuid = playerUuid,
                    worldKey = value.world.key,
                    x = value.x,
                    y = value.y,
                    z = value.z,
                    yaw = value.yaw,
                    pitch = value.pitch
                )
            ) {
                "Failed to set player location: ${value.world.key} (${value.x}, ${value.y}, ${value.z})"
            }
        }

    override var onGround: Boolean
        get() = liveSession().onGround
        set(value) {
            val session = liveSession()
            PlayerSessionManager.updateAndBroadcastMovement(
                channelId = session.channelId,
                x = session.x,
                y = session.y,
                z = session.z,
                yaw = session.yaw,
                pitch = session.pitch,
                onGround = value,
                reason = MoveReason.API
            )
        }

    override fun accelerate(x: Double, y: Double, z: Double): Boolean {
        return PlayerSessionManager.acceleratePlayer(playerUuid, x, y, z)
    }

    override fun jump(heightBlocks: Double): Boolean {
        return PlayerSessionManager.jumpPlayer(playerUuid, heightBlocks)
    }

    override var speedX: Double
        get() = liveSession().velocityX * 20.0
        set(value) {
            val current = liveSession()
            PlayerSessionManager.setPlayerSpeedMetersPerSecond(
                uuid = playerUuid,
                x = value,
                y = current.velocityY * 20.0,
                z = current.velocityZ * 20.0
            )
        }

    override var speedY: Double
        get() = liveSession().velocityY * 20.0
        set(value) {
            val current = liveSession()
            PlayerSessionManager.setPlayerSpeedMetersPerSecond(
                uuid = playerUuid,
                x = current.velocityX * 20.0,
                y = value,
                z = current.velocityZ * 20.0
            )
        }

    override var speedZ: Double
        get() = liveSession().velocityZ * 20.0
        set(value) {
            val current = liveSession()
            PlayerSessionManager.setPlayerSpeedMetersPerSecond(
                uuid = playerUuid,
                x = current.velocityX * 20.0,
                y = current.velocityY * 20.0,
                z = value
            )
        }

    override var accelerationX: Double
        get() = liveSession().accelerationX
        set(value) {
            val current = liveSession()
            PlayerSessionManager.setPlayerAccelerationMetersPerSecondSquared(
                uuid = playerUuid,
                x = value,
                y = current.accelerationY,
                z = current.accelerationZ
            )
        }

    override var accelerationY: Double
        get() = liveSession().accelerationY
        set(value) {
            val current = liveSession()
            PlayerSessionManager.setPlayerAccelerationMetersPerSecondSquared(
                uuid = playerUuid,
                x = current.accelerationX,
                y = value,
                z = current.accelerationZ
            )
        }

    override var accelerationZ: Double
        get() = liveSession().accelerationZ
        set(value) {
            val current = liveSession()
            PlayerSessionManager.setPlayerAccelerationMetersPerSecondSquared(
                uuid = playerUuid,
                x = current.accelerationX,
                y = current.accelerationY,
                z = value
            )
        }

    override val eyeLocation: Location
        get() {
            val session = liveSession()
            val world = runtimeWorldFor(session.worldKey)
            return Location(
                world = world,
                x = session.clientEyeX,
                y = session.clientEyeY,
                z = session.clientEyeZ,
                yaw = session.yaw,
                pitch = session.pitch
            )
        }

    override val viewChunk: Chunk
        get() {
            val session = liveSession()
            return runtimeWorldFor(session.worldKey).chunkAt(session.centerChunkX, session.centerChunkZ)
        }

    override val viewChunkRadius: Int
        get() = liveSession().chunkRadius

    override val loadedViewChunks: List<Chunk>
        get() {
            val session = liveSession()
            val runtimeWorld = runtimeWorldFor(session.worldKey)
            return session.loadedChunks.asSequence()
                .map { runtimeWorld.chunkAt(it.x, it.z) }
                .toList()
        }

    override val ridingEntityId: UUID?
        get() {
            val session = liveSession()
            val animalEntityId = session.ridingAnimalEntityId
            if (animalEntityId < 0) return null
            return WorldManager.world(session.worldKey)?.animalSnapshot(animalEntityId)?.uuid
        }

    override var health: Float?
        get() = liveSession().health
        set(value) {
            val next = value ?: return
            PlayerSessionManager.setPlayerHealth(playerUuid, next)
        }

    override val food: Int
        get() = liveSession().food

    override val saturation: Float
        get() = liveSession().saturation

    override fun setHotbarItem(slot: Int, item: Item?): Boolean {
        val translatedName = item?.translatedName()
        val translatedLore = item?.translatedLore()?.map { it.key to it.args }.orEmpty()
        return PlayerSessionManager.setHotbarItem(
            uuid = playerUuid,
            slot = slot,
            itemId = item?.id ?: -1,
            amount = item?.amount ?: 0,
            name = item?.name,
            lore = item?.lore.orEmpty(),
            translatedNameKey = translatedName?.key,
            translatedNameArgs = translatedName?.args.orEmpty(),
            translatedLore = translatedLore
        )
    }

    override fun setMainItem(slot: Int, item: Item?): Boolean {
        val translatedName = item?.translatedName()
        val translatedLore = item?.translatedLore()?.map { it.key to it.args }.orEmpty()
        return PlayerSessionManager.setMainInventoryItem(
            uuid = playerUuid,
            index = slot,
            itemId = item?.id ?: -1,
            amount = item?.amount ?: 0,
            name = item?.name,
            lore = item?.lore.orEmpty(),
            translatedNameKey = translatedName?.key,
            translatedNameArgs = translatedName?.args.orEmpty(),
            translatedLore = translatedLore
        )
    }

    override fun setArmorItem(slot: ArmorSlot, item: Item?): Boolean {
        val translatedName = item?.translatedName()
        val translatedLore = item?.translatedLore()?.map { it.key to it.args }.orEmpty()
        return PlayerSessionManager.setArmorItem(
            uuid = playerUuid,
            slot = slot,
            itemId = item?.id ?: -1,
            amount = item?.amount ?: 0,
            name = item?.name,
            lore = item?.lore.orEmpty(),
            translatedNameKey = translatedName?.key,
            translatedNameArgs = translatedName?.args.orEmpty(),
            translatedLore = translatedLore
        )
    }

    override fun setOffhandItem(item: Item?): Boolean {
        val translatedName = item?.translatedName()
        val translatedLore = item?.translatedLore()?.map { it.key to it.args }.orEmpty()
        return PlayerSessionManager.setOffhandItem(
            uuid = playerUuid,
            itemId = item?.id ?: -1,
            amount = item?.amount ?: 0,
            name = item?.name,
            lore = item?.lore.orEmpty(),
            translatedNameKey = translatedName?.key,
            translatedNameArgs = translatedName?.args.orEmpty(),
            translatedLore = translatedLore
        )
    }

    override var displayName: String?
        get() = liveSession().displayName
        set(value) {
            PlayerSessionManager.setDisplayName(playerUuid, value)
        }

    override fun tr(key: String, vararg args: String): String {
        return ServerI18n.trFor(liveSession().locale, key, *args)
    }

    override fun sendMessage(message: String) {
        val session = liveSession()
        PlayerSessionManager.sendPluginMessage(session.channelId, message)
    }

    override fun respawn(): Boolean {
        val session = liveSession()
        val wasDead = session.dead
        if (!wasDead) return false
        PlayerSessionManager.respawnIfDead(session.channelId)
        return true
    }

    private fun inventoryStackOf(itemId: Int, count: Int): Item? {
        if (itemId < 0 || count <= 0) return null
        return Item(
            id = itemId,
            type = ItemType.fromId(itemId),
            amount = count
        )
    }

    companion object {
        fun fromSession(session: PlayerSession): RuntimeConnectedPlayer {
            return RuntimeConnectedPlayer(session.profile.uuid)
        }
    }
}

private class RuntimePlayerReference(
    private val playerUuid: UUID,
    private val fallbackName: String?,
    private var fallbackDisplayName: String?
) : Player(playerUuid) {

    private fun liveSession(): PlayerSession? = PlayerSessionManager.byUuid(playerUuid)

    override val online: Boolean
        get() = liveSession() != null

    override val op: Boolean
        get() = liveSession()?.let { PlayerSessionManager.isOperatorSession(it) } ?: false

    override var location: Location
        get() {
            val live = liveSession()
            if (live != null) {
                val world = runtimeWorldFor(live.worldKey)
                return Location(
                    world = world,
                    x = live.x,
                    y = live.y,
                    z = live.z,
                    yaw = live.yaw,
                    pitch = live.pitch
                )
            }
            val fallbackWorld = WorldManager.defaultWorld()
            val spawn = fallbackWorld.spawnPointForPlayer(playerUuid)
            return Location(
                world = runtimeWorldFor(fallbackWorld.key),
                x = spawn.x,
                y = spawn.y,
                z = spawn.z,
                yaw = 0f,
                pitch = 0f
            )
        }
        set(value) {
            val live = liveSession()
                ?: throw IllegalStateException("Cannot set location of offline player: $playerUuid")
            check(
                PlayerSessionManager.setPlayerLocation(
                    uuid = live.profile.uuid,
                    worldKey = value.world.key,
                    x = value.x,
                    y = value.y,
                    z = value.z,
                    yaw = value.yaw,
                    pitch = value.pitch
                )
            ) {
                "Failed to set player location: ${value.world.key} (${value.x}, ${value.y}, ${value.z})"
            }
        }

    override var onGround: Boolean
        get() = liveSession()?.onGround ?: false
        set(value) {
            val live = liveSession() ?: return
            PlayerSessionManager.updateAndBroadcastMovement(
                channelId = live.channelId,
                x = live.x,
                y = live.y,
                z = live.z,
                yaw = live.yaw,
                pitch = live.pitch,
                onGround = value,
                reason = MoveReason.API
            )
        }

    override fun accelerate(x: Double, y: Double, z: Double): Boolean {
        val live = liveSession() ?: return false
        return PlayerSessionManager.acceleratePlayer(live.profile.uuid, x, y, z)
    }

    override fun jump(heightBlocks: Double): Boolean {
        val live = liveSession() ?: return false
        return PlayerSessionManager.jumpPlayer(live.profile.uuid, heightBlocks)
    }

    override var speedX: Double
        get() = liveSession()?.let { it.velocityX * 20.0 } ?: 0.0
        set(value) {
            val live = liveSession() ?: return
            PlayerSessionManager.setPlayerSpeedMetersPerSecond(
                uuid = live.profile.uuid,
                x = value,
                y = live.velocityY * 20.0,
                z = live.velocityZ * 20.0
            )
        }

    override var speedY: Double
        get() = liveSession()?.let { it.velocityY * 20.0 } ?: 0.0
        set(value) {
            val live = liveSession() ?: return
            PlayerSessionManager.setPlayerSpeedMetersPerSecond(
                uuid = live.profile.uuid,
                x = live.velocityX * 20.0,
                y = value,
                z = live.velocityZ * 20.0
            )
        }

    override var speedZ: Double
        get() = liveSession()?.let { it.velocityZ * 20.0 } ?: 0.0
        set(value) {
            val live = liveSession() ?: return
            PlayerSessionManager.setPlayerSpeedMetersPerSecond(
                uuid = live.profile.uuid,
                x = live.velocityX * 20.0,
                y = live.velocityY * 20.0,
                z = value
            )
        }

    override var accelerationX: Double
        get() = liveSession()?.accelerationX ?: 0.0
        set(value) {
            val live = liveSession() ?: return
            PlayerSessionManager.setPlayerAccelerationMetersPerSecondSquared(
                uuid = live.profile.uuid,
                x = value,
                y = live.accelerationY,
                z = live.accelerationZ
            )
        }

    override var accelerationY: Double
        get() = liveSession()?.accelerationY ?: 0.0
        set(value) {
            val live = liveSession() ?: return
            PlayerSessionManager.setPlayerAccelerationMetersPerSecondSquared(
                uuid = live.profile.uuid,
                x = live.accelerationX,
                y = value,
                z = live.accelerationZ
            )
        }

    override var accelerationZ: Double
        get() = liveSession()?.accelerationZ ?: 0.0
        set(value) {
            val live = liveSession() ?: return
            PlayerSessionManager.setPlayerAccelerationMetersPerSecondSquared(
                uuid = live.profile.uuid,
                x = live.accelerationX,
                y = live.accelerationY,
                z = value
            )
        }

    override var health: Float?
        get() = liveSession()?.health
        set(value) {
            val live = liveSession() ?: return
            val next = value ?: return
            PlayerSessionManager.setPlayerHealth(live.profile.uuid, next)
        }

    companion object {
        fun offlineReference(playerUuid: UUID, fallbackName: String?): RuntimePlayerReference {
            return RuntimePlayerReference(
                playerUuid = playerUuid,
                fallbackName = fallbackName,
                fallbackDisplayName = null
            )
        }
    }
}

private class RuntimeBot(
    private val botUuid: UUID
) : Bot(botUuid) {

    private fun liveState(): RuntimeBotState {
        return checkNotNull(runtimeBotsByUuid[botUuid]) { "Bot no longer exists: $botUuid" }
    }

    override var name: String
        get() = liveState().name
        set(value) {
            liveState().name = value
        }

    override var tabList: Boolean
        get() = liveState().tabList
        set(value) {
            liveState().tabList = value
        }

    override var pushable: Boolean
        get() = liveState().pushable
        set(value) {
            liveState().pushable = value
        }

    override var attackable: Boolean
        get() = liveState().attackable
        set(value) {
            liveState().attackable = value
        }

    override var gravity: Boolean
        get() = liveState().gravity
        set(value) {
            liveState().gravity = value
        }

    override var collision: Boolean
        get() = liveState().collision
        set(value) {
            liveState().collision = value
        }

    override var invulnerable: Boolean
        get() = liveState().invulnerable
        set(value) {
            liveState().invulnerable = value
        }

    override var fallDamage: Boolean
        get() = liveState().fallDamage
        set(value) {
            liveState().fallDamage = value
        }

    override var damageDelaySeconds: Double
        get() = liveState().damageDelaySeconds
        set(value) {
            liveState().damageDelaySeconds = value.coerceAtLeast(0.0)
        }

    override var damageDelayRemainingSeconds: Double
        get() = liveState().damageDelayRemainingSeconds
        set(value) {
            liveState().damageDelayRemainingSeconds = value.coerceAtLeast(0.0)
        }

    override var maxHealth: Float
        get() = liveState().maxHealth
        set(value) {
            val state = liveState()
            state.maxHealth = value.coerceAtLeast(0.0f)
            state.health = state.health.coerceAtMost(state.maxHealth)
        }

    override var health: Float?
        get() = liveState().health
        set(value) {
            val state = liveState()
            state.health = (value ?: return).coerceIn(0f, state.maxHealth)
        }

    override var location: Location
        get() {
            val state = liveState()
            return Location(
                world = runtimeWorldFor(state.worldKey),
                x = state.x,
                y = state.y,
                z = state.z,
                yaw = state.yaw,
                pitch = state.pitch
            )
        }
        set(value) {
            val state = liveState()
            state.worldKey = value.world.key
            state.x = value.x
            state.y = value.y
            state.z = value.z
            state.yaw = value.yaw
            state.pitch = value.pitch
        }

    override var onGround: Boolean
        get() = liveState().onGround
        set(value) {
            liveState().onGround = value
        }

    override var speedX: Double
        get() = liveState().speedX
        set(value) {
            liveState().speedX = value
        }

    override var speedY: Double
        get() = liveState().speedY
        set(value) {
            liveState().speedY = value
        }

    override var speedZ: Double
        get() = liveState().speedZ
        set(value) {
            liveState().speedZ = value
        }

    override var accelerationX: Double
        get() = liveState().accelerationX
        set(value) {
            liveState().accelerationX = value
        }

    override var accelerationY: Double
        get() = liveState().accelerationY
        set(value) {
            liveState().accelerationY = value
        }

    override var accelerationZ: Double
        get() = liveState().accelerationZ
        set(value) {
            liveState().accelerationZ = value
        }
}

private class RuntimeThrown(
    protected var worldKey: String,
    private val entityId: Int,
    uniqueId: UUID
) : Thrown(uniqueId) {
    override val type: EntityType
        get() = when (liveSnapshotOrNull()?.kind ?: ThrownItemKind.SNOWBALL) {
            ThrownItemKind.SNOWBALL -> EntityType.SNOWBALL
            ThrownItemKind.EGG -> EntityType.EGG
            ThrownItemKind.BLUE_EGG -> EntityType.BLUE_EGG
            ThrownItemKind.BROWN_EGG -> EntityType.BROWN_EGG
            ThrownItemKind.ENDER_PEARL -> EntityType.ENDER_PEARL
        }

    private fun liveSnapshot() = checkNotNull(PlayerSessionManager.thrownItemSnapshot(worldKey, entityId)) {
        "Thrown entity no longer exists: $worldKey#$entityId"
    }

    private fun liveSnapshotOrNull() = PlayerSessionManager.thrownItemSnapshot(worldKey, entityId)

    override var location: Location
        get() {
            val snapshot = liveSnapshot()
            return Location(
                world = runtimeWorldFor(worldKey),
                x = snapshot.x,
                y = snapshot.y,
                z = snapshot.z,
                yaw = 0f,
                pitch = 0f
            )
        }
        set(value) {
            val current = liveSnapshotOrNull() ?: return
            val from = Location(
                world = runtimeWorldFor(worldKey),
                x = current.x,
                y = current.y,
                z = current.z,
                yaw = 0f,
                pitch = 0f
            )
            val decision = PluginSystem.beforeEntityMove(this, from, value, MoveReason.API)
            if (decision.cancelAll) return
            val target = Location(
                world = value.world,
                x = if (decision.cancelPosition) from.x else value.x,
                y = if (decision.cancelPosition) from.y else value.y,
                z = if (decision.cancelPosition) from.z else value.z,
                yaw = if (decision.cancelRotation) from.yaw else value.yaw,
                pitch = if (decision.cancelRotation) from.pitch else value.pitch
            )
            if (target.world.key == worldKey) {
                check(PlayerSessionManager.setThrownItemPosition(worldKey, entityId, target.x, target.y, target.z)) {
                    "Failed to set thrown location: ${target.world.key} (${target.x}, ${target.y}, ${target.z})"
                }
                return
            }
            val sourceWorld = WorldManager.world(worldKey)
                ?: throw IllegalStateException("Thrown source world not found: $worldKey")
            val snapshot = sourceWorld.thrownItemSnapshot(entityId)
                ?: throw IllegalStateException("Thrown entity no longer exists: $worldKey#$entityId")
            val targetWorldKey = target.world.key
            val targetWorld = WorldManager.world(targetWorldKey)
                ?: throw IllegalStateException("Thrown target world not found: $targetWorldKey")
            sourceWorld.removeThrownItem(entityId, hit = false)
                ?: throw IllegalStateException("Failed to remove thrown entity from source world: $worldKey#$entityId")
            val spawned = targetWorld.spawnThrownItem(
                entityId = entityId,
                ownerEntityId = snapshot.ownerEntityId,
                kind = snapshot.kind,
                x = target.x,
                y = target.y,
                z = target.z,
                vx = snapshot.vx,
                vy = snapshot.vy,
                vz = snapshot.vz,
                uuid = snapshot.uuid
            )
            check(spawned) {
                "Failed to spawn thrown entity in target world: $targetWorldKey#$entityId"
            }
            targetWorld.setThrownItemAcceleration(
                entityId = entityId,
                ax = snapshot.accelerationX,
                ay = snapshot.accelerationY,
                az = snapshot.accelerationZ
            )
            targetWorld.setThrownItemOnGround(entityId, snapshot.onGround)
            worldKey = targetWorldKey
        }

    override var ownerUniqueId: UUID?
        get() {
            val ownerEntityId = liveSnapshotOrNull()?.ownerEntityId ?: -1
            if (ownerEntityId < 0) return null
            val player = PlayerSessionManager.playersInWorld(worldKey).firstOrNull { it.entityId == ownerEntityId }
            if (player != null) return player.profile.uuid
            return WorldManager.world(worldKey)?.animalSnapshot(ownerEntityId)?.uuid
        }
        set(value) {
            val ownerEntityId = if (value == null) {
                -1
            } else {
                val playerEntityId = PlayerSessionManager.playersInWorld(worldKey)
                    .firstOrNull { it.profile.uuid == value }
                    ?.entityId
                if (playerEntityId != null) {
                    playerEntityId
                } else {
                    WorldManager.world(worldKey)
                        ?.animalSnapshots()
                        ?.firstOrNull { it.uuid == value }
                        ?.entityId
                        ?: -1
                }
            }
            check(PlayerSessionManager.setThrownItemOwnerEntityId(worldKey, entityId, ownerEntityId)) {
                "Failed to set thrown owner: $worldKey#$entityId owner=$value"
            }
        }

    override var x: Double
        get() = liveSnapshotOrNull()?.x ?: 0.0
        set(value) {
            val current = liveSnapshotOrNull() ?: return
            location = Location(runtimeWorldFor(worldKey), value, current.y, current.z, 0f, 0f)
        }

    override var y: Double
        get() = liveSnapshotOrNull()?.y ?: 0.0
        set(value) {
            val current = liveSnapshotOrNull() ?: return
            location = Location(runtimeWorldFor(worldKey), current.x, value, current.z, 0f, 0f)
        }

    override var z: Double
        get() = liveSnapshotOrNull()?.z ?: 0.0
        set(value) {
            val current = liveSnapshotOrNull() ?: return
            location = Location(runtimeWorldFor(worldKey), current.x, current.y, value, 0f, 0f)
        }

    override val prevX: Double
        get() = liveSnapshotOrNull()?.prevX ?: 0.0

    override val prevY: Double
        get() = liveSnapshotOrNull()?.prevY ?: 0.0

    override val prevZ: Double
        get() = liveSnapshotOrNull()?.prevZ ?: 0.0

    override var onGround: Boolean
        get() = liveSnapshotOrNull()?.onGround ?: false
        set(value) {
            PlayerSessionManager.setThrownItemOnGround(worldKey, entityId, value)
        }

    override var speedX: Double
        get() = (liveSnapshotOrNull()?.vx ?: 0.0) * 20.0
        set(value) {
            val current = liveSnapshotOrNull() ?: return
            PlayerSessionManager.setThrownItemSpeedMetersPerSecond(
                worldKey = worldKey,
                entityId = entityId,
                x = value,
                y = current.vy * 20.0,
                z = current.vz * 20.0
            )
        }

    override var speedY: Double
        get() = (liveSnapshotOrNull()?.vy ?: 0.0) * 20.0
        set(value) {
            val current = liveSnapshotOrNull() ?: return
            PlayerSessionManager.setThrownItemSpeedMetersPerSecond(
                worldKey = worldKey,
                entityId = entityId,
                x = current.vx * 20.0,
                y = value,
                z = current.vz * 20.0
            )
        }

    override var speedZ: Double
        get() = (liveSnapshotOrNull()?.vz ?: 0.0) * 20.0
        set(value) {
            val current = liveSnapshotOrNull() ?: return
            PlayerSessionManager.setThrownItemSpeedMetersPerSecond(
                worldKey = worldKey,
                entityId = entityId,
                x = current.vx * 20.0,
                y = current.vy * 20.0,
                z = value
            )
        }

    override var accelerationX: Double
        get() = liveSnapshotOrNull()?.accelerationX ?: 0.0
        set(value) {
            val current = liveSnapshotOrNull() ?: return
            PlayerSessionManager.setThrownItemAccelerationMetersPerSecondSquared(
                worldKey = worldKey,
                entityId = entityId,
                x = value,
                y = current.accelerationY,
                z = current.accelerationZ
            )
        }

    override var accelerationY: Double
        get() = liveSnapshotOrNull()?.accelerationY ?: 0.0
        set(value) {
            val current = liveSnapshotOrNull() ?: return
            PlayerSessionManager.setThrownItemAccelerationMetersPerSecondSquared(
                worldKey = worldKey,
                entityId = entityId,
                x = current.accelerationX,
                y = value,
                z = current.accelerationZ
            )
        }

    override var accelerationZ: Double
        get() = liveSnapshotOrNull()?.accelerationZ ?: 0.0
        set(value) {
            val current = liveSnapshotOrNull() ?: return
            PlayerSessionManager.setThrownItemAccelerationMetersPerSecondSquared(
                worldKey = worldKey,
                entityId = entityId,
                x = current.accelerationX,
                y = current.accelerationY,
                z = value
            )
        }

    override fun setSpeed(x: Double, y: Double, z: Double) {
        PlayerSessionManager.setThrownItemSpeedMetersPerSecond(worldKey, entityId, x, y, z)
    }

    override fun setAcceleration(x: Double, y: Double, z: Double) {
        PlayerSessionManager.setThrownItemAccelerationMetersPerSecondSquared(worldKey, entityId, x, y, z)
    }
}

private data class DroppedItemRotation(
    val yaw: Float,
    val pitch: Float
)

private class RuntimeDroppedItem(
    private var worldKey: String,
    private val entityId: Int,
    uniqueId: UUID
) : DroppedItem(uniqueId) {
    private fun rotationKey(): String = "$worldKey:$entityId"

    private fun rotationFromVelocity(snapshotVx: Double, snapshotVy: Double, snapshotVz: Double): DroppedItemRotation {
        val horizontal = kotlin.math.sqrt((snapshotVx * snapshotVx) + (snapshotVz * snapshotVz))
        if (horizontal <= 1.0e-9 && kotlin.math.abs(snapshotVy) <= 1.0e-9) {
            return DroppedItemRotation(0f, 0f)
        }
        val yaw = Math.toDegrees(kotlin.math.atan2(-snapshotVx, snapshotVz)).toFloat()
        val pitch = Math.toDegrees(kotlin.math.atan2(-snapshotVy, horizontal)).toFloat()
        return DroppedItemRotation(yaw = yaw, pitch = pitch)
    }

    private fun currentRotation(snapshotVx: Double, snapshotVy: Double, snapshotVz: Double): DroppedItemRotation {
        val key = rotationKey()
        val cached = droppedItemRotationByKey[key]
        if (cached != null) return cached
        val inferred = rotationFromVelocity(snapshotVx, snapshotVy, snapshotVz)
        droppedItemRotationByKey[key] = inferred
        return inferred
    }

    private fun liveSnapshot() = checkNotNull(PlayerSessionManager.droppedItemSnapshot(worldKey, entityId)) {
        "Dropped item no longer exists: $worldKey#$entityId"
    }

    private fun liveSnapshotOrNull() = PlayerSessionManager.droppedItemSnapshot(worldKey, entityId)

    override var location: Location
        get() {
            val snapshot = liveSnapshot()
            val rotation = currentRotation(snapshot.vx, snapshot.vy, snapshot.vz)
            return Location(
                world = runtimeWorldFor(worldKey),
                x = snapshot.x,
                y = snapshot.y,
                z = snapshot.z,
                yaw = rotation.yaw,
                pitch = rotation.pitch
            )
        }
        set(value) {
            val currentSnapshot = liveSnapshotOrNull() ?: return
            val rotation = currentRotation(currentSnapshot.vx, currentSnapshot.vy, currentSnapshot.vz)
            val from = Location(
                world = runtimeWorldFor(worldKey),
                x = currentSnapshot.x,
                y = currentSnapshot.y,
                z = currentSnapshot.z,
                yaw = rotation.yaw,
                pitch = rotation.pitch
            )
            val decision = PluginSystem.beforeEntityMove(this, from, value, MoveReason.API)
            if (decision.cancelAll) {
                return
            }
            val target = Location(
                world = value.world,
                x = if (decision.cancelPosition) from.x else value.x,
                y = if (decision.cancelPosition) from.y else value.y,
                z = if (decision.cancelPosition) from.z else value.z,
                yaw = if (decision.cancelRotation) from.yaw else value.yaw,
                pitch = if (decision.cancelRotation) from.pitch else value.pitch
            )
            if (target.world.key == worldKey) {
                check(
                    PlayerSessionManager.setDroppedItemPosition(
                        worldKey = worldKey,
                        entityId = entityId,
                        x = target.x,
                        y = target.y,
                        z = target.z
                    )
                ) {
                    "Failed to set dropped-item location: ${target.world.key} (${target.x}, ${target.y}, ${target.z})"
                }
                droppedItemRotationByKey[rotationKey()] = DroppedItemRotation(target.yaw, target.pitch)
                return
            }
            val sourceWorld = WorldManager.world(worldKey)
                ?: throw IllegalStateException("Dropped-item source world not found: $worldKey")
            val snapshot = sourceWorld.droppedItemSnapshot(entityId)
                ?: throw IllegalStateException("Dropped item no longer exists: $worldKey#$entityId")
            val targetWorldKey = target.world.key
            val targetWorld = WorldManager.world(targetWorldKey)
                ?: throw IllegalStateException("Dropped-item target world not found: $targetWorldKey")
            sourceWorld.removeDroppedItemIfUuidMatches(entityId, snapshot.uuid)
                ?: sourceWorld.removeDroppedItem(entityId)
                ?: throw IllegalStateException("Failed to remove dropped item from source world: $worldKey#$entityId")
            val spawned = targetWorld.spawnDroppedItem(
                entityId = entityId,
                itemId = snapshot.itemId,
                itemCount = snapshot.itemCount,
                x = target.x,
                y = target.y,
                z = target.z,
                vx = snapshot.vx,
                vy = snapshot.vy,
                vz = snapshot.vz,
                uuid = snapshot.uuid,
                pickupDelaySeconds = snapshot.pickupDelaySeconds
            )
            check(spawned) {
                "Failed to spawn dropped item in target world: $targetWorldKey#$entityId"
            }
            targetWorld.setDroppedItemAcceleration(
                entityId = entityId,
                ax = snapshot.accelerationX,
                ay = snapshot.accelerationY,
                az = snapshot.accelerationZ
            )
            targetWorld.setDroppedItemOnGround(entityId, snapshot.onGround)
            val oldKey = "$worldKey:$entityId"
            worldKey = targetWorldKey
            droppedItemRotationByKey.remove(oldKey)
            droppedItemRotationByKey[rotationKey()] = DroppedItemRotation(target.yaw, target.pitch)
        }

    override var x: Double
        get() = liveSnapshotOrNull()?.x ?: 0.0
        set(value) {
            val current = liveSnapshotOrNull() ?: return
            val rotation = currentRotation(current.vx, current.vy, current.vz)
            location = Location(
                world = runtimeWorldFor(worldKey),
                x = value,
                y = current.y,
                z = current.z,
                yaw = rotation.yaw,
                pitch = rotation.pitch
            )
        }

    override var y: Double
        get() = liveSnapshotOrNull()?.y ?: 0.0
        set(value) {
            val current = liveSnapshotOrNull() ?: return
            val rotation = currentRotation(current.vx, current.vy, current.vz)
            location = Location(
                world = runtimeWorldFor(worldKey),
                x = current.x,
                y = value,
                z = current.z,
                yaw = rotation.yaw,
                pitch = rotation.pitch
            )
        }

    override var z: Double
        get() = liveSnapshotOrNull()?.z ?: 0.0
        set(value) {
            val current = liveSnapshotOrNull() ?: return
            val rotation = currentRotation(current.vx, current.vy, current.vz)
            location = Location(
                world = runtimeWorldFor(worldKey),
                x = current.x,
                y = current.y,
                z = value,
                yaw = rotation.yaw,
                pitch = rotation.pitch
            )
        }

    override var speedX: Double
        get() = (liveSnapshotOrNull()?.vx ?: 0.0) * 20.0
        set(value) {
            val current = liveSnapshotOrNull() ?: return
            PlayerSessionManager.setDroppedItemSpeedMetersPerSecond(
                worldKey = worldKey,
                entityId = entityId,
                x = value,
                y = current.vy * 20.0,
                z = current.vz * 20.0
            )
        }

    override var speedY: Double
        get() = (liveSnapshotOrNull()?.vy ?: 0.0) * 20.0
        set(value) {
            val current = liveSnapshotOrNull() ?: return
            PlayerSessionManager.setDroppedItemSpeedMetersPerSecond(
                worldKey = worldKey,
                entityId = entityId,
                x = current.vx * 20.0,
                y = value,
                z = current.vz * 20.0
            )
        }

    override var speedZ: Double
        get() = (liveSnapshotOrNull()?.vz ?: 0.0) * 20.0
        set(value) {
            val current = liveSnapshotOrNull() ?: return
            PlayerSessionManager.setDroppedItemSpeedMetersPerSecond(
                worldKey = worldKey,
                entityId = entityId,
                x = current.vx * 20.0,
                y = current.vy * 20.0,
                z = value
            )
        }

    override var accelerationX: Double
        get() = liveSnapshotOrNull()?.accelerationX ?: 0.0
        set(value) {
            val current = liveSnapshotOrNull() ?: return
            PlayerSessionManager.setDroppedItemAccelerationMetersPerSecondSquared(
                worldKey = worldKey,
                entityId = entityId,
                x = value,
                y = current.accelerationY,
                z = current.accelerationZ
            )
        }

    override var accelerationY: Double
        get() = liveSnapshotOrNull()?.accelerationY ?: 0.0
        set(value) {
            val current = liveSnapshotOrNull() ?: return
            PlayerSessionManager.setDroppedItemAccelerationMetersPerSecondSquared(
                worldKey = worldKey,
                entityId = entityId,
                x = current.accelerationX,
                y = value,
                z = current.accelerationZ
            )
        }

    override var accelerationZ: Double
        get() = liveSnapshotOrNull()?.accelerationZ ?: 0.0
        set(value) {
            val current = liveSnapshotOrNull() ?: return
            PlayerSessionManager.setDroppedItemAccelerationMetersPerSecondSquared(
                worldKey = worldKey,
                entityId = entityId,
                x = current.accelerationX,
                y = current.accelerationY,
                z = value
            )
        }

    override var item: Item
        get() {
            val snapshot = liveSnapshot()
            return Item(
                id = snapshot.itemId,
                type = ItemType.fromId(snapshot.itemId),
                amount = snapshot.itemCount
            )
        }
        set(value) {
            PlayerSessionManager.setDroppedItemStack(
                worldKey = worldKey,
                entityId = entityId,
                itemId = value.id,
                amount = value.amount
            )
        }

    override var pickupDelaySeconds: Double
        get() = liveSnapshotOrNull()?.pickupDelaySeconds ?: 0.0
        set(value) {
            PlayerSessionManager.setDroppedItemPickupDelaySeconds(worldKey, entityId, value)
        }

    override var onGround: Boolean
        get() = liveSnapshotOrNull()?.onGround ?: false
        set(value) {
            PlayerSessionManager.setDroppedItemOnGround(worldKey, entityId, value)
        }

    override fun setSpeed(x: Double, y: Double, z: Double) {
        PlayerSessionManager.setDroppedItemSpeedMetersPerSecond(worldKey, entityId, x, y, z)
    }

    override fun setAcceleration(x: Double, y: Double, z: Double) {
        PlayerSessionManager.setDroppedItemAccelerationMetersPerSecondSquared(worldKey, entityId, x, y, z)
    }
}

private val droppedItemRotationByKey = ConcurrentHashMap<String, DroppedItemRotation>()
private val runtimeWorldCache = ConcurrentHashMap<String, RuntimeWorld>()
private val CLIMBABLE_MEDIUM_BLOCK_KEYS = setOf(
    "minecraft:ladder",
    "minecraft:vine",
    "minecraft:weeping_vines",
    "minecraft:weeping_vines_plant",
    "minecraft:twisting_vines",
    "minecraft:twisting_vines_plant",
    "minecraft:scaffolding"
)
private val BOT_NON_COLLIDING_BLOCK_KEYS = setOf(
    "minecraft:air",
    "minecraft:cave_air",
    "minecraft:void_air",
    "minecraft:water",
    "minecraft:lava",
    "minecraft:bubble_column",
    "minecraft:fire",
    "minecraft:soul_fire"
)
private const val BOT_HALF_WIDTH = 0.3
private const val BOT_HEIGHT = 1.8
private const val BOT_GRAVITY_PER_TICK = 0.08
private const val BOT_AIR_DRAG_PER_TICK = 0.91
private const val BOT_VERTICAL_DRAG_PER_TICK = 0.98
private const val BOT_VELOCITY_EPSILON = 1.0e-6
private const val PLAYER_HALF_WIDTH = 0.3
private const val BOT_PUSH_STRENGTH = 0.08
private const val BOT_NETWORK_POSITION_EPSILON = 1.0e-4
private const val BOT_HURT_SOUND_KEY = "minecraft:entity.player.hurt"
private val BOT_PLAYER_ATTACK_DAMAGE_TYPE_ID: Int by lazy {
    RegistryCodec.entryIndex("minecraft:damage_type", "minecraft:player_attack") ?: 0
}

private fun runtimeWorldFor(worldKey: String): RuntimeWorld {
    return runtimeWorldCache.computeIfAbsent(worldKey) { RuntimeWorld(it) }
}

private fun runtimeBotProfile(state: RuntimeBotState): ConnectionProfile {
    val username = runtimeBotWireName(state)
    return ConnectionProfile(
        protocolVersion = 0,
        username = username,
        uuid = state.uuid
    )
}

private fun runtimeBotWireName(state: RuntimeBotState): String {
    return state.name.trim().ifEmpty { "Bot-${state.uuid.toString().take(8)}" }.take(16)
}

private fun syncRuntimeBotNetwork() {
    if (runtimeBotsByUuid.isEmpty()) {
        if (runtimeBotViewerStatesByChannelId.isEmpty()) return
            for ((channelId, viewers) in runtimeBotViewerStatesByChannelId) {
                if (viewers.isEmpty()) continue
                val ctx = PlayerSessionManager.channelContext(channelId) ?: continue
                if (!ctx.channel().isActive) continue
            for ((botUuid, viewerState) in viewers) {
                ctx.write(PlayPackets.removeEntitiesPacket(intArrayOf(viewerState.entityId)))
                ctx.write(PlayPackets.playerInfoRemovePacket(listOf(botUuid)))
            }
            ctx.flush()
        }
        runtimeBotViewerStatesByChannelId.clear()
        return
    }

    val activeSessionIds = PlayerSessionManager.players().map { it.channelId }.toHashSet()
    runtimeBotViewerStatesByChannelId.keys.removeIf { it !in activeSessionIds }

    val botStates = runtimeBotsByUuid.values.toList()
    val botSetByWorld = botStates.groupBy { it.worldKey }

    for (session in PlayerSessionManager.players()) {
        val ctx = PlayerSessionManager.channelContext(session.channelId) ?: continue
        if (!ctx.channel().isActive) continue
        val worldBots = botSetByWorld[session.worldKey].orEmpty()
        val viewerStates = runtimeBotViewerStatesByChannelId.computeIfAbsent(session.channelId) { ConcurrentHashMap() }
        val seenBots = HashSet<UUID>(worldBots.size)

        for (state in worldBots) {
            val snapshot = synchronized(state) {
                RuntimeBotViewerState(
                    entityId = state.entityId,
                    listed = state.tabList,
                    name = state.name,
                    x = state.x,
                    y = state.y,
                    z = state.z,
                    yaw = state.yaw,
                    pitch = state.pitch,
                    onGround = state.onGround
                )
            }
            seenBots.add(state.uuid)
            val botChunk = ChunkPos(floor(snapshot.x / 16.0).toInt(), floor(snapshot.z / 16.0).toInt())
            val visible = session.loadedChunks.contains(botChunk)
            val previous = viewerStates[state.uuid]

            if (!visible) {
                if (previous != null) {
                    ctx.write(PlayPackets.removeEntitiesPacket(intArrayOf(state.entityId)))
                    ctx.write(PlayPackets.playerInfoRemovePacket(listOf(state.uuid)))
                    viewerStates.remove(state.uuid)
                }
                continue
            }

            val profile = runtimeBotProfile(state)
            val wireName = runtimeBotWireName(state)
            if (previous == null) {
                ctx.write(
                    PlayPackets.playerInfoPacket(
                        profile = profile,
                        displayName = wireName,
                        gameMode = 0,
                        latencyMs = 0,
                        listed = snapshot.listed
                    )
                )
                ctx.write(
                    PlayPackets.addPlayerEntityPacket(
                        entityId = state.entityId,
                        uuid = state.uuid,
                        x = snapshot.x,
                        y = snapshot.y,
                        z = snapshot.z,
                        yaw = snapshot.yaw,
                        pitch = snapshot.pitch
                    )
                )
                ctx.write(PlayPackets.entityHeadLookPacket(state.entityId, snapshot.yaw))
                viewerStates[state.uuid] = snapshot
                continue
            }

            if (previous.listed != snapshot.listed || previous.name != snapshot.name) {
                ctx.write(
                    PlayPackets.playerInfoPacket(
                        profile = profile,
                        displayName = wireName,
                        gameMode = 0,
                        latencyMs = 0,
                        listed = snapshot.listed
                    )
                )
                previous.listed = snapshot.listed
                previous.name = snapshot.name
            }

            val moved =
                abs(previous.x - snapshot.x) > BOT_NETWORK_POSITION_EPSILON ||
                    abs(previous.y - snapshot.y) > BOT_NETWORK_POSITION_EPSILON ||
                    abs(previous.z - snapshot.z) > BOT_NETWORK_POSITION_EPSILON ||
                    previous.yaw != snapshot.yaw ||
                    previous.pitch != snapshot.pitch ||
                    previous.onGround != snapshot.onGround
            if (moved) {
                ctx.write(
                    PlayPackets.entityPositionSyncPacket(
                        entityId = state.entityId,
                        x = snapshot.x,
                        y = snapshot.y,
                        z = snapshot.z,
                        vx = 0.0,
                        vy = 0.0,
                        vz = 0.0,
                        yaw = snapshot.yaw,
                        pitch = snapshot.pitch,
                        onGround = snapshot.onGround
                    )
                )
                ctx.write(PlayPackets.entityHeadLookPacket(state.entityId, snapshot.yaw))
                previous.x = snapshot.x
                previous.y = snapshot.y
                previous.z = snapshot.z
                previous.yaw = snapshot.yaw
                previous.pitch = snapshot.pitch
                previous.onGround = snapshot.onGround
            }
        }

        val removeUuids = viewerStates.keys.filter { it !in seenBots }
        for (uuid in removeUuids) {
            val state = runtimeBotsByUuid[uuid]
            if (state != null) {
                ctx.write(PlayPackets.removeEntitiesPacket(intArrayOf(state.entityId)))
            }
            ctx.write(PlayPackets.playerInfoRemovePacket(listOf(uuid)))
            viewerStates.remove(uuid)
        }
        ctx.flush()
    }
}

private fun broadcastRuntimeBotDamage(worldKey: String, entityId: Int, x: Double, y: Double, z: Double) {
    val chunkPos = ChunkPos(floor(x / 16.0).toInt(), floor(z / 16.0).toInt())
    val hurtPacket = PlayPackets.hurtAnimationPacket(entityId, 0f)
    val damagePacket = PlayPackets.damageEventPacket(entityId, BOT_PLAYER_ATTACK_DAMAGE_TYPE_ID)
    val soundPacket = PlayPackets.soundPacketByKey(
        soundKey = BOT_HURT_SOUND_KEY,
        soundSourceId = 7,
        x = x,
        y = y + 0.9,
        z = z,
        volume = 1.0f,
        pitch = 1.0f,
        seed = 0L
    )
    for (session in PlayerSessionManager.playersInWorld(worldKey)) {
        if (!session.loadedChunks.contains(chunkPos)) continue
        val ctx = PlayerSessionManager.channelContext(session.channelId) ?: continue
        if (!ctx.channel().isActive) continue
        ctx.write(hurtPacket)
        ctx.write(damagePacket)
        ctx.write(soundPacket)
        ctx.flush()
    }
}

private fun removeRuntimeBot(botUuid: UUID) {
    val removed = runtimeBotsByUuid.remove(botUuid) ?: return
    runtimeBotUuidByEntityId.remove(removed.entityId, botUuid)
    for ((channelId, viewerStates) in runtimeBotViewerStatesByChannelId) {
        if (viewerStates.remove(botUuid) == null) continue
        val ctx = PlayerSessionManager.channelContext(channelId) ?: continue
        if (!ctx.channel().isActive) continue
        ctx.write(PlayPackets.removeEntitiesPacket(intArrayOf(removed.entityId)))
        ctx.write(PlayPackets.playerInfoRemovePacket(listOf(botUuid)))
        ctx.flush()
    }
}

private fun dispatchRuntimeBotTicks() {
    if (runtimeBotsByUuid.isEmpty()) return
    for ((_, state) in runtimeBotsByUuid) {
        val world = WorldManager.world(state.worldKey) ?: continue
        val chunkPos = ChunkPos(floor(state.x / 16.0).toInt(), floor(state.z / 16.0).toInt())
        world.submitOnChunkActor(chunkPos) {
            tickRuntimeBotOnChunkActor(world, state)
        }
    }
}

private fun tickRuntimeBotOnChunkActor(
    world: org.macaroon3145.world.World,
    state: RuntimeBotState
) {
    synchronized(state) {
        if (state.damageDelayRemainingSeconds > 0.0) {
            state.damageDelayRemainingSeconds = (state.damageDelayRemainingSeconds - (1.0 / 20.0)).coerceAtLeast(0.0)
        }
        val nextSpeedXMps = state.speedX + (state.accelerationX / 20.0)
        val nextSpeedYMps = state.speedY + (state.accelerationY / 20.0)
        val nextSpeedZMps = state.speedZ + (state.accelerationZ / 20.0)
        var vx = nextSpeedXMps / 20.0
        var vy = nextSpeedYMps / 20.0
        var vz = nextSpeedZMps / 20.0

        if (state.pushable) {
            val pushRadius = BOT_HALF_WIDTH + PLAYER_HALF_WIDTH
            val players = PlayerSessionManager.playersInWorld(state.worldKey)
            for (player in players) {
                if (player.dead) continue
                val dx = state.x - player.x
                val dz = state.z - player.z
                val distSq = (dx * dx) + (dz * dz)
                if (distSq <= 1.0e-9 || distSq >= (pushRadius * pushRadius)) continue
                val dist = kotlin.math.sqrt(distSq)
                val overlap = pushRadius - dist
                if (overlap <= 0.0) continue
                val nx = dx / dist
                val nz = dz / dist
                val impulse = overlap * BOT_PUSH_STRENGTH
                vx += nx * impulse
                vz += nz * impulse
            }
        }

        if (state.gravity) {
            vy -= BOT_GRAVITY_PER_TICK
        }

        var onGround = false
        var nextY = state.y + vy
        if (state.collision && botCollidesAt(world, state.x, nextY, state.z)) {
            if (vy < 0.0) onGround = true
            vy = 0.0
            nextY = state.y
        }
        state.y = nextY

        var nextX = state.x + vx
        if (state.collision && botCollidesAt(world, nextX, state.y, state.z)) {
            vx = 0.0
            nextX = state.x
        }
        state.x = nextX

        var nextZ = state.z + vz
        if (state.collision && botCollidesAt(world, state.x, state.y, nextZ)) {
            vz = 0.0
            nextZ = state.z
        }
        state.z = nextZ

        state.onGround = onGround
        val drag = if (state.onGround) BOT_AIR_DRAG_PER_TICK else BOT_AIR_DRAG_PER_TICK
        vx *= drag
        vy *= BOT_VERTICAL_DRAG_PER_TICK
        vz *= drag
        if (abs(vx) < BOT_VELOCITY_EPSILON) vx = 0.0
        if (abs(vy) < BOT_VELOCITY_EPSILON) vy = 0.0
        if (abs(vz) < BOT_VELOCITY_EPSILON) vz = 0.0

        state.speedX = vx * 20.0
        state.speedY = vy * 20.0
        state.speedZ = vz * 20.0
    }
}

private fun botCollidesAt(world: org.macaroon3145.world.World, x: Double, y: Double, z: Double): Boolean {
    val minX = x - BOT_HALF_WIDTH
    val maxX = x + BOT_HALF_WIDTH
    val minY = y
    val maxY = y + BOT_HEIGHT
    val minZ = z - BOT_HALF_WIDTH
    val maxZ = z + BOT_HALF_WIDTH

    val startX = floor(minX).toInt()
    val endX = floor(maxX - 1.0e-7).toInt()
    val startY = floor(minY).toInt()
    val endY = floor(maxY - 1.0e-7).toInt()
    val startZ = floor(minZ).toInt()
    val endZ = floor(maxZ - 1.0e-7).toInt()

    for (bx in startX..endX) {
        for (by in startY..endY) {
            for (bz in startZ..endZ) {
                val stateId = world.blockStateAt(bx, by, bz)
                if (stateId <= 0) continue
                val parsed = BlockStateRegistry.parsedState(stateId) ?: continue
                if (parsed.blockKey in BOT_NON_COLLIDING_BLOCK_KEYS) continue
                val resolved = BlockCollisionRegistry.boxesForStateId(stateId, parsed)
                if (resolved == null) {
                    if (aabbOverlap(minX, minY, minZ, maxX, maxY, maxZ, bx.toDouble(), by.toDouble(), bz.toDouble(), bx + 1.0, by + 1.0, bz + 1.0)) {
                        return true
                    }
                    continue
                }
                for (collisionBox in resolved) {
                    if (aabbOverlap(
                            minX,
                            minY,
                            minZ,
                            maxX,
                            maxY,
                            maxZ,
                            bx + collisionBox.minX,
                            by + collisionBox.minY,
                            bz + collisionBox.minZ,
                            bx + collisionBox.maxX,
                            by + collisionBox.maxY,
                            bz + collisionBox.maxZ
                        )
                    ) {
                        return true
                    }
                }
            }
        }
    }
    return false
}

private fun aabbOverlap(
    aMinX: Double,
    aMinY: Double,
    aMinZ: Double,
    aMaxX: Double,
    aMaxY: Double,
    aMaxZ: Double,
    bMinX: Double,
    bMinY: Double,
    bMinZ: Double,
    bMaxX: Double,
    bMaxY: Double,
    bMaxZ: Double
): Boolean {
    return aMaxX > bMinX && aMinX < bMaxX &&
        aMaxY > bMinY && aMinY < bMaxY &&
        aMaxZ > bMinZ && aMinZ < bMaxZ
}

private class RuntimeWorld(
    override val key: String
) : World() {
    override val elapsedTicks: Long
        get() = PlayerSessionManager.worldElapsedTicks(key) ?: 0L

    override var timeOfDayTicks: Long
        get() = PlayerSessionManager.worldTimeOfDayTicks(key) ?: 0L
        set(value) {
            PlayerSessionManager.setWorldTimeOfDayTicks(key, value)
        }

    override fun addTime(deltaTicks: Long): Boolean {
        return PlayerSessionManager.addWorldTimeTicks(key, deltaTicks)
    }

    override fun pluginInternalDropItem(location: Location, item: Item, impulse: Boolean): DroppedItem? {
        val snapshot = PlayerSessionManager.spawnDroppedItemEntityAt(
            worldKey = key,
            itemId = item.id,
            itemCount = item.amount,
            x = location.x,
            y = location.y,
            z = location.z,
            impulse = impulse
        ) ?: return null
        return RuntimeDroppedItem(
            worldKey = key,
            entityId = snapshot.entityId,
            uniqueId = snapshot.uuid
        )
    }

    override fun pluginInternalSpawnEntity(location: Location, entityType: EntityType): Entity? {
        val kind = when (entityType) {
            EntityType.SNOWBALL -> ThrownItemKind.SNOWBALL
            EntityType.EGG -> ThrownItemKind.EGG
            EntityType.BLUE_EGG -> ThrownItemKind.BLUE_EGG
            EntityType.BROWN_EGG -> ThrownItemKind.BROWN_EGG
            EntityType.ENDER_PEARL -> ThrownItemKind.ENDER_PEARL
            EntityType.DROPPED_ITEM -> {
                val dropped = pluginInternalDropItem(
                    location = location,
                    item = Item(type = ItemType.STONE, amount = 1),
                    impulse = false
                ) ?: return null
                return dropped
            }
            EntityType.PLAYER -> return null
            EntityType.BOT -> {
                val botUuid = UUID.randomUUID()
                val entityId = PlayerSessionManager.allocateRuntimeEntityId()
                runtimeBotsByUuid[botUuid] = RuntimeBotState(
                    uuid = botUuid,
                    entityId = entityId,
                    worldKey = key,
                    x = location.x,
                    y = location.y,
                    z = location.z,
                    yaw = location.yaw,
                    pitch = location.pitch,
                    onGround = false,
                    speedX = 0.0,
                    speedY = 0.0,
                    speedZ = 0.0,
                    name = "Bot-${botUuid.toString().take(8)}",
                    tabList = false,
                    pushable = true,
                    attackable = true,
                    gravity = true,
                    collision = true,
                    invulnerable = false,
                    fallDamage = true,
                    damageDelaySeconds = 0.5,
                    damageDelayRemainingSeconds = 0.0,
                    maxHealth = 20f,
                    health = 20f,
                    accelerationX = 0.0,
                    accelerationY = 0.0,
                    accelerationZ = 0.0
                )
                runtimeBotUuidByEntityId[entityId] = botUuid
                return RuntimeBot(botUuid)
            }
        }
        val spawned = PlayerSessionManager.spawnThrownItemEntityAt(
            worldKey = key,
            kind = kind,
            x = location.x,
            y = location.y,
            z = location.z
        ) ?: return null
        return RuntimeThrown(
            worldKey = key,
            entityId = spawned.entityId,
            uniqueId = spawned.uuid
        )
    }

    override fun pluginInternalEntitiesByType(type: EntityType): List<Entity> {
        return when (type) {
            EntityType.PLAYER -> PlayerSessionManager.playersInWorld(key).map { RuntimeConnectedPlayer.fromSession(it) }
            EntityType.BOT -> runtimeBotsByUuid.entries.asSequence()
                .filter { (_, state) -> state.worldKey == key }
                .map { (uuid, _) -> RuntimeBot(uuid) }
                .toList()
            EntityType.DROPPED_ITEM -> PlayerSessionManager.droppedItemSnapshots(key).map { snapshot ->
                RuntimeDroppedItem(
                    worldKey = key,
                    entityId = snapshot.entityId,
                    uniqueId = snapshot.uuid
                )
            }
            EntityType.SNOWBALL -> PlayerSessionManager.thrownItemSnapshots(key)
                .asSequence()
                .filter { it.kind == ThrownItemKind.SNOWBALL }
                .map { snapshot ->
                    RuntimeThrown(
                        worldKey = key,
                        entityId = snapshot.entityId,
                        uniqueId = snapshot.uuid
                    )
                }
                .toList()
            EntityType.EGG -> PlayerSessionManager.thrownItemSnapshots(key)
                .asSequence()
                .filter { it.kind == ThrownItemKind.EGG }
                .map { snapshot ->
                    RuntimeThrown(
                        worldKey = key,
                        entityId = snapshot.entityId,
                        uniqueId = snapshot.uuid
                    )
                }
                .toList()
            EntityType.BLUE_EGG -> PlayerSessionManager.thrownItemSnapshots(key)
                .asSequence()
                .filter { it.kind == ThrownItemKind.BLUE_EGG }
                .map { snapshot ->
                    RuntimeThrown(
                        worldKey = key,
                        entityId = snapshot.entityId,
                        uniqueId = snapshot.uuid
                    )
                }
                .toList()
            EntityType.BROWN_EGG -> PlayerSessionManager.thrownItemSnapshots(key)
                .asSequence()
                .filter { it.kind == ThrownItemKind.BROWN_EGG }
                .map { snapshot ->
                    RuntimeThrown(
                        worldKey = key,
                        entityId = snapshot.entityId,
                        uniqueId = snapshot.uuid
                    )
                }
                .toList()
            EntityType.ENDER_PEARL -> PlayerSessionManager.thrownItemSnapshots(key)
                .asSequence()
                .filter { it.kind == ThrownItemKind.ENDER_PEARL }
                .map { snapshot ->
                    RuntimeThrown(
                        worldKey = key,
                        entityId = snapshot.entityId,
                        uniqueId = snapshot.uuid
                    )
                }
                .toList()
        }
    }

    override fun pluginInternalEntityById(entityId: UUID): Entity? {
        val botState = runtimeBotsByUuid[entityId]
        if (botState != null && botState.worldKey == key) {
            return RuntimeBot(entityId)
        }
        val player = PlayerSessionManager.byUuid(entityId)
        if (player != null && player.worldKey == key) {
            return RuntimeConnectedPlayer.fromSession(player)
        }
        val dropped = PlayerSessionManager.droppedItemSnapshotByUuid(key, entityId)
        if (dropped != null) {
            return RuntimeDroppedItem(
                worldKey = key,
                entityId = dropped.entityId,
                uniqueId = dropped.uuid
            )
        }
        val thrown = PlayerSessionManager.thrownItemSnapshotByUuid(key, entityId)
        if (thrown != null) {
            return RuntimeThrown(
                worldKey = key,
                entityId = thrown.entityId,
                uniqueId = thrown.uuid
            )
        }
        return null
    }

    override fun chunkAt(chunkX: Int, chunkZ: Int): Chunk {
        return RuntimeChunk(
            world = this,
            chunkX = chunkX,
            chunkZ = chunkZ
        )
    }

    override fun blockAt(x: Int, y: Int, z: Int): Block {
        val world = WorldManager.world(key)
        val stateId = world?.blockStateAt(x, y, z) ?: 0
        val parsed = BlockStateRegistry.parsedState(stateId)
        val blockKey = parsed?.blockKey ?: "minecraft:air"
        val props = parsed?.properties ?: emptyMap()
        val blockType = runtimeTypeRegistry.blockByKey(blockKey) ?: runtimeFallbackAirBlockType
        val location = Location(
            world = this,
            x = x.toDouble(),
            y = y.toDouble(),
            z = z.toDouble(),
            yaw = 0f,
            pitch = 0f
        )
        val stateKey = buildStateKey(blockKey, props)
        val state = RuntimeBlockState(
            id = stateId,
            key = stateKey,
            properties = props
        )
        return RuntimeBlock(
            location = location,
            initialType = blockType,
            initialState = state
        )
    }
}

private class RuntimeChunk(
    override val world: World,
    private val chunkX: Int,
    private val chunkZ: Int
) : Chunk() {
    override val x: Int
        get() = chunkX

    override val z: Int
        get() = chunkZ

    override val state: ChunkState
        get() = FoliaSharedMemoryWorldGenerator.chunkState(world.key, chunkX, chunkZ)

    override val simulationChunk: Boolean
        get() = PlayerSessionManager.activeSimulationChunksByWorldSnapshot()[world.key]?.contains(ChunkPos(chunkX, chunkZ)) == true

    override val tps: Double
        get() = WorldManager.world(world.key)?.chunkEwmaTps(chunkX, chunkZ) ?: 0.0

    override val mspt: Double
        get() = WorldManager.world(world.key)?.chunkEwmaMspt(chunkX, chunkZ) ?: 0.0

    override val virtualThread: Thread?
        get() = WorldManager.world(world.key)?.chunkActorVirtualThread(chunkX, chunkZ)

    override val tickScheduler: TickScheduler
        get() = PluginSystem.chunkTickScheduler(world.key, chunkX, chunkZ)

    override fun load(): Boolean {
        return FoliaSharedMemoryWorldGenerator.loadChunk(world.key, chunkX, chunkZ)
    }

    override fun unload(): Boolean {
        return FoliaSharedMemoryWorldGenerator.unloadChunk(world.key, chunkX, chunkZ)
    }

    override fun pluginInternalEntitiesByType(type: EntityType): List<Entity> {
        return world.getEntityByType(type).filter { entity ->
            val location = entity.location
            location.chunkX == chunkX && location.chunkZ == chunkZ
        }
    }

    override fun pluginInternalEntityById(entityId: UUID): Entity? {
        val entity = world.getEntityById(entityId) ?: return null
        val location = entity.location
        return if (location.chunkX == chunkX && location.chunkZ == chunkZ) entity else null
    }
}

private class RuntimeBlock(
    override val location: Location,
    private val initialType: BlockType,
    private val initialState: BlockState
) : Block() {
    @Volatile
    private var mutatedByPlugin: Boolean = false

    override var type: BlockType
        get() {
            val runtimeState = currentRuntimeState() ?: return initialType
            return runtimeTypeRegistry.blockByKey(runtimeState.blockKey) ?: initialType
        }
        set(value) {
            mutatedByPlugin = true
            val stateId = BlockStateRegistry.defaultStateId(value.key)
                ?: throw IllegalStateException("Failed to set block type: no default state for ${value.key}.")
            val applied = PlayerSessionManager.pluginSetBlockStateAndBroadcast(
                worldKey = location.world.key,
                x = location.blockX,
                y = location.blockY,
                z = location.blockZ,
                stateId = stateId
            )
            if (!applied) {
                throw IllegalStateException("Failed to set block type at ${location.world.key}:${location.blockX},${location.blockY},${location.blockZ}")
            }
        }

    override var state: BlockState
        get() {
            val runtimeState = currentRuntimeState() ?: return initialState
            return RuntimeBlockState(
                id = runtimeState.stateId,
                key = buildStateKey(runtimeState.blockKey, runtimeState.properties),
                properties = runtimeState.properties
            )
        }
        set(value) {
            mutatedByPlugin = true
            val parsed = BlockStateRegistry.parsedState(value.id)
                ?: throw IllegalStateException("Failed to set block state: unknown state id ${value.id}.")
            val targetStateId = BlockStateRegistry.stateId(parsed.blockKey, parsed.properties)
                ?: BlockStateRegistry.defaultStateId(parsed.blockKey)
                ?: throw IllegalStateException("Failed to set block state: unresolved state for ${parsed.blockKey}.")
            val applied = PlayerSessionManager.pluginSetBlockStateAndBroadcast(
                worldKey = location.world.key,
                x = location.blockX,
                y = location.blockY,
                z = location.blockZ,
                stateId = targetStateId
            )
            if (!applied) {
                throw IllegalStateException("Failed to set block state at ${location.world.key}:${location.blockX},${location.blockY},${location.blockZ}")
            }
        }

    fun wasMutatedByPlugin(): Boolean = mutatedByPlugin

    private data class RuntimeStateSnapshot(
        val stateId: Int,
        val blockKey: String,
        val properties: Map<String, String>
    )

    private fun currentRuntimeState(): RuntimeStateSnapshot? {
        val world = WorldManager.world(location.world.key) ?: return null
        val stateId = world.blockStateAt(location.blockX, location.blockY, location.blockZ)
        val parsed = BlockStateRegistry.parsedState(stateId) ?: return null
        return RuntimeStateSnapshot(stateId, parsed.blockKey, parsed.properties)
    }
}

private class RuntimeBlockState(
    override val id: Int,
    override val key: String,
    override val properties: Map<String, String>
) : BlockState()

private fun buildStateKey(blockKey: String, properties: Map<String, String>): String {
    if (properties.isEmpty()) return blockKey
    val canonicalProps = properties.entries
        .sortedBy { it.key }
        .joinToString(",") { "${it.key}=${it.value}" }
    return "$blockKey[$canonicalProps]"
}

private fun wrapDegrees(value: Float): Float {
    var wrapped = value % 360f
    if (wrapped <= -180f) wrapped += 360f
    if (wrapped > 180f) wrapped -= 360f
    return wrapped
}

private class RuntimeCommandSender private constructor(
    override val name: String,
    private val subjectName: String,
    private val send: (String) -> Unit,
    private val hasPermissionImpl: (String) -> Boolean
) : CommandSender, PermissionAwareSender {
    override fun sendMessage(message: String) {
        send(message)
    }

    override fun hasPermission(node: String): Boolean {
        return hasPermissionImpl(node)
    }

    override fun baseHasPermission(node: String): Boolean {
        return hasPermissionImpl(node)
    }

    override fun permissionSubject(): String {
        return subjectName
    }

    companion object {
        fun fromSession(session: PlayerSession?): CommandSender {
            return if (session == null) {
                RuntimeCommandSender(
                    name = "CONSOLE",
                    subjectName = "CONSOLE",
                    send = { message -> DebugConsole.err("[Plugin] $message") },
                    hasPermissionImpl = { true }
                )
            } else {
                RuntimeConnectedPlayer.fromSession(session)
            }
        }
    }
}

private class RuntimeCommandRegistrar(
    private val owner: String,
    private val permissions: StandardPermissionService
) : CommandRegistrar {
    private val logger = LoggerFactory.getLogger(RuntimeCommandRegistrar::class.java)
    private val specsByPrimaryName = ConcurrentHashMap<String, CommandSpec>()
    private val aliasesByPrimaryName = ConcurrentHashMap<String, List<String>>()

    override fun register(owner: String, spec: CommandSpec): RegisteredCommand {
        if (owner != this.owner) {
            throw IllegalArgumentException("Owner mismatch: expected '${this.owner}', got '$owner'")
        }
        val primary = spec.name.lowercase()
        val aliases = spec.aliases.map { it.lowercase() }.filter { it.isNotBlank() && it != primary }
        val wrapped = object : Command {
            override fun visibleTo(sender: PlayerSession?): Boolean {
                val permission = spec.permission ?: return true
                val commandSender = RuntimeCommandSender.fromSession(sender)
                return permissions.has(commandSender, permission)
            }

            override fun execute(context: CommandContext, sender: PlayerSession?, args: List<String>) {
                val commandSender = RuntimeCommandSender.fromSession(sender)
                val permission = spec.permission
                if (permission != null && !permissions.has(commandSender, permission)) {
                    commandSender.sendMessage("No permission: $permission")
                    return
                }
                val invocation = CommandInvocation(name = primary, args = args, sender = commandSender)
                PluginSystem.withPluginOwner(this@RuntimeCommandRegistrar.owner) {
                    spec.executor.execute(invocation)
                }
            }

            override fun complete(
                context: CommandContext,
                sender: PlayerSession?,
                providedArgs: List<String>,
                activeArgIndex: Int,
                activeArgPrefix: String
            ): List<String> {
                val completer = spec.completer ?: return emptyList()
                val commandSender = RuntimeCommandSender.fromSession(sender)
                val completionArgs = providedArgs.toMutableList()
                if (activeArgIndex >= 0) {
                    while (completionArgs.size <= activeArgIndex) {
                        completionArgs += ""
                    }
                    completionArgs[activeArgIndex] = activeArgPrefix
                }
                val invocation = CommandInvocation(name = primary, args = completionArgs, sender = commandSender)
                return PluginSystem.withPluginOwner(this@RuntimeCommandRegistrar.owner) {
                    completer.complete(invocation)
                }.asSequence()
                    .filter { it.startsWith(activeArgPrefix, ignoreCase = true) }
                    .distinct()
                    .sortedWith(String.CASE_INSENSITIVE_ORDER)
                    .toList()
            }
        }

        PlayerSessionManager.registerDynamicCommand(primary, aliases, wrapped)
        specsByPrimaryName[primary] = spec
        aliasesByPrimaryName[primary] = aliases
        return object : RegisteredCommand {
            override val name: String = primary

            override fun unregister() {
                val removedAliases = aliasesByPrimaryName.remove(primary).orEmpty()
                specsByPrimaryName.remove(primary)
                PlayerSessionManager.unregisterDynamicCommand(primary, removedAliases)
            }
        }
    }

    override fun register(owner: String, spec: FunctionalCommandSpec): RegisteredCommand {
        if (owner != this.owner) {
            throw IllegalArgumentException("Owner mismatch: expected '${this.owner}', got '$owner'")
        }
        require(!spec.playerOnly || !spec.consoleOnly) {
            "Cannot use both playerOnly and consoleOnly: ${spec.name}"
        }

        val commandName = spec.name.trim().lowercase()
        require(commandName.isNotEmpty()) { "Command name must not be blank." }
        val aliases = spec.aliases
            .map { it.trim() }
            .filter { it.isNotEmpty() && !it.equals(commandName, ignoreCase = true) }
            .map { it.lowercase() }
            .filter { it.isNotBlank() && it != commandName }
        val permission = spec.permission?.trim()?.takeIf { it.isNotEmpty() }

        val wrappedCommand = object : Command {
            override fun visibleTo(sender: PlayerSession?): Boolean {
                if (spec.playerOnly) return sender != null
                if (spec.consoleOnly) return sender == null
                val permissionNode = permission
                if (permissionNode != null) {
                    val commandSender = RuntimeCommandSender.fromSession(sender)
                    if (!permissions.has(commandSender, permissionNode)) {
                        return false
                    }
                }
                return true
            }

            override fun execute(context: CommandContext, sender: PlayerSession?, args: List<String>) {
                val commandSender = RuntimeCommandSender.fromSession(sender)
                val invocation = CommandInvocation(name = commandName, args = args, sender = commandSender)
                PluginSystem.withPluginOwner(this@RuntimeCommandRegistrar.owner) {
                    val permissionNode = permission
                    if (permissionNode != null && !permissions.has(commandSender, permissionNode)) {
                        commandSender.sendMessage("No permission: $permissionNode")
                        return@withPluginOwner
                    }

                    val executeContext = buildApiCommandContext(
                        commandName = invocation.name,
                        args = invocation.args,
                        sender = invocation.sender,
                        activeArgIndex = invocation.args.size,
                        activeArgPrefix = ""
                    )
                    val built = runCatching { spec.handler(executeContext) }
                        .onFailure { throwable ->
                            logger.error(
                                "Functional command handler failed (owner={}, command={})",
                                this@RuntimeCommandRegistrar.owner,
                                invocation.name,
                                throwable
                            )
                        }
                        .getOrNull() ?: return@withPluginOwner

                    if (built.strict && built.complete != null && args.isNotEmpty()) {
                        val invalidArgIndex = args.indices.firstOrNull { argIndex ->
                            val current = args[argIndex]
                            val completion = resolveCompletions(
                                built = built,
                                commandSender = commandSender,
                                providedArgs = args,
                                activeArgIndex = argIndex,
                                activeArgPrefix = current
                            )
                            val suggestions = completion.first
                            val appendMode = completion.second
                            if (suggestions.isEmpty()) {
                                true
                            } else if (appendMode) {
                                suggestions.any { suffix ->
                                    current.endsWith(suffix, ignoreCase = true)
                                }.not()
                            } else {
                                suggestions.any { suggestion ->
                                    suggestion.equals(current, ignoreCase = true)
                                }.not()
                            }
                        }
                        if (invalidArgIndex != null) {
                            val rawInput = if (args.isEmpty()) {
                                commandName
                            } else {
                                "$commandName ${args.joinToString(" ")}"
                            }
                            var errorStart = commandName.length
                            if (args.isNotEmpty()) {
                                errorStart += 1
                                for (i in 0 until invalidArgIndex) {
                                    errorStart += args[i].length + 1
                                }
                            }
                            context.sendSourceTranslationWithContext(
                                sender,
                                "command.unknown.command",
                                rawInput,
                                errorStart
                            )
                            return@withPluginOwner
                        }
                    }

                    runCatching {
                        built.execute?.invoke()
                    }.onFailure { throwable ->
                        logger.error(
                            "Functional command execution failed (owner={}, command={})",
                            this@RuntimeCommandRegistrar.owner,
                            invocation.name,
                            throwable
                        )
                        invocation.sender.sendMessage("Command failed: ${throwable.message ?: throwable.javaClass.simpleName}")
                    }
                }
            }

            override fun complete(
                context: CommandContext,
                sender: PlayerSession?,
                providedArgs: List<String>,
                activeArgIndex: Int,
                activeArgPrefix: String
            ): List<String> {
                val commandSender = RuntimeCommandSender.fromSession(sender)
                val completion = resolveCompletions(
                    built = null,
                    commandSender = commandSender,
                    providedArgs = providedArgs,
                    activeArgIndex = activeArgIndex,
                    activeArgPrefix = activeArgPrefix
                )
                val suggestions = completion.first
                val appendMode = completion.second
                if (!appendMode) return suggestions
                return suggestions.map(CommandCompletionEncoding::encodeAppend)
            }

            private fun resolveCompletions(
                built: CommandHandler?,
                commandSender: CommandSender,
                providedArgs: List<String>,
                activeArgIndex: Int,
                activeArgPrefix: String
            ): Pair<List<String>, Boolean> {
                val completionArgs = providedArgs.toMutableList()
                if (activeArgIndex >= 0) {
                    while (completionArgs.size <= activeArgIndex) {
                        completionArgs += ""
                    }
                    completionArgs[activeArgIndex] = activeArgPrefix
                }
                val invocation = CommandInvocation(name = commandName, args = completionArgs, sender = commandSender)
                val permissionNode = permission
                if (permissionNode != null && !permissions.has(commandSender, permissionNode)) {
                    return emptyList<String>() to false
                }
                val apiContext = buildApiCommandContext(
                    commandName = invocation.name,
                    args = invocation.args,
                    sender = invocation.sender,
                    activeArgIndex = activeArgIndex,
                    activeArgPrefix = activeArgPrefix
                )
                val resolved = built ?: runCatching { spec.handler(apiContext) }
                    .onFailure { throwable ->
                        logger.error(
                            "Functional command completer failed (owner={}, command={})",
                            this@RuntimeCommandRegistrar.owner,
                            invocation.name,
                            throwable
                        )
                    }
                    .getOrNull()
                    ?: return emptyList<String>() to false
                val completions = resolved.complete?.invoke().orEmpty()
                val appendMode = apiContext.appendCompletionsToInput
                val normalized = if (appendMode) {
                    completions.asSequence()
                        .map(String::trim)
                        .filter { it.isNotEmpty() }
                        .distinct()
                        .toList()
                } else {
                    completions.asSequence()
                        .filter { it.startsWith(activeArgPrefix, ignoreCase = true) }
                        .distinct()
                        .sortedWith(String.CASE_INSENSITIVE_ORDER)
                        .toList()
                }
                return normalized to appendMode
            }
        }

        PlayerSessionManager.registerDynamicCommand(commandName, aliases, wrappedCommand)
        specsByPrimaryName[commandName] = CommandSpec(
            name = commandName,
            aliases = aliases,
            permission = permission,
            executor = CommandExecutor { },
            completer = null
        )
        aliasesByPrimaryName[commandName] = aliases
        return object : RegisteredCommand {
            override val name: String = commandName

            override fun unregister() {
                val removedAliases = aliasesByPrimaryName.remove(commandName).orEmpty()
                specsByPrimaryName.remove(commandName)
                PlayerSessionManager.unregisterDynamicCommand(commandName, removedAliases)
            }
        }
    }

    override fun registerAnnotated(owner: String, handler: Any): List<RegisteredCommand> {
        if (owner != this.owner) {
            throw IllegalArgumentException("Owner mismatch: expected '${this.owner}', got '$owner'")
        }
        val out = ArrayList<RegisteredCommand>()
        for (method in handler.javaClass.methods) {
            val command = method.getAnnotation(ApiCommand::class.java) ?: continue
            if (method.parameterCount != 1 || method.parameterTypes[0] != ApiCommandContext::class.java) {
                throw IllegalArgumentException(
                    "Invalid @Command method signature: ${handler.javaClass.name}#${method.name}. " +
                        "Expected: fun method(context: org.macaroon3145.api.command.CommandContext): CommandHandler"
                )
            }
            if (!CommandHandler::class.java.isAssignableFrom(method.returnType)) {
                throw IllegalArgumentException(
                    "Invalid @Command return type: ${handler.javaClass.name}#${method.name}. " +
                        "Expected: org.macaroon3145.api.command.CommandHandler"
                )
            }
            val commandName = command.value.trim().lowercase()
            require(commandName.isNotEmpty()) {
                "@Command value must not be blank: ${handler.javaClass.name}#${method.name}"
            }
            val aliases = command.aliases.map { it.trim() }.filter { it.isNotEmpty() && !it.equals(commandName, ignoreCase = true) }
            val permission = method.getAnnotation(ApiPermission::class.java)?.value?.trim()?.takeIf { it.isNotEmpty() }
            val playerOnly = method.isAnnotationPresent(PlayerOnly::class.java)
            val consoleOnly = method.isAnnotationPresent(ConsoleOnly::class.java)
            if (playerOnly && consoleOnly) {
                throw IllegalArgumentException(
                    "Cannot use both @PlayerOnly and @ConsoleOnly: ${handler.javaClass.name}#${method.name}"
                )
            }

            method.isAccessible = true
            val bound = MethodHandles.lookup().unreflect(method).bindTo(handler)

            val wrappedCommand = object : Command {
                override fun visibleTo(sender: PlayerSession?): Boolean {
                    if (playerOnly) return sender != null
                    if (consoleOnly) return sender == null
                    val permissionNode = permission
                    if (permissionNode != null) {
                        val commandSender = RuntimeCommandSender.fromSession(sender)
                        if (!permissions.has(commandSender, permissionNode)) {
                            return false
                        }
                    }
                    return true
                }

                override fun execute(context: CommandContext, sender: PlayerSession?, args: List<String>) {
                    val commandSender = RuntimeCommandSender.fromSession(sender)
                    val invocation = CommandInvocation(name = commandName, args = args, sender = commandSender)
                    PluginSystem.withPluginOwner(this@RuntimeCommandRegistrar.owner) {
                        val permissionNode = permission
                        if (permissionNode != null && !permissions.has(commandSender, permissionNode)) {
                            commandSender.sendMessage("No permission: $permissionNode")
                            return@withPluginOwner
                        }
                        val executeContext = buildApiCommandContext(
                            commandName = invocation.name,
                            args = invocation.args,
                            sender = invocation.sender,
                            activeArgIndex = invocation.args.size,
                            activeArgPrefix = ""
                        )
                        val built = resolveHandler(executeContext, invocation.name) ?: return@withPluginOwner
                        if (built.strict && built.complete != null && args.isNotEmpty()) {
                            val invalidArgIndex = args.indices.firstOrNull { argIndex ->
                                val current = args[argIndex]
                                val completion = resolveCompletions(
                                    built = built,
                                    commandSender = commandSender,
                                    providedArgs = args,
                                    activeArgIndex = argIndex,
                                    activeArgPrefix = current
                                )
                                val suggestions = completion.first
                                val appendMode = completion.second
                                if (suggestions.isEmpty()) {
                                    true
                                } else if (appendMode) {
                                    suggestions.any { suffix ->
                                        current.endsWith(suffix, ignoreCase = true)
                                    }.not()
                                } else {
                                    suggestions.any { suggestion ->
                                        suggestion.equals(current, ignoreCase = true)
                                    }.not()
                                }
                            }
                            if (invalidArgIndex != null) {
                                val rawInput = if (args.isEmpty()) {
                                    commandName
                                } else {
                                    "$commandName ${args.joinToString(" ")}"
                                }
                                var errorStart = commandName.length
                                if (args.isNotEmpty()) {
                                    errorStart += 1 // space after command name
                                    for (i in 0 until invalidArgIndex) {
                                        errorStart += args[i].length + 1 // arg + separating space
                                    }
                                }
                                context.sendSourceTranslationWithContext(
                                    sender,
                                    "command.unknown.command",
                                    rawInput,
                                    errorStart
                                )
                                return@withPluginOwner
                            }
                        }
                        runCatching {
                            built.execute?.invoke()
                        }.onFailure { throwable ->
                            logger.error(
                                "Annotated command execution failed (owner={}, handler={}, method={}, command={})",
                                this@RuntimeCommandRegistrar.owner,
                                handler.javaClass.name,
                                method.name,
                                invocation.name,
                                throwable
                            )
                            invocation.sender.sendMessage("Command failed: ${throwable.message ?: throwable.javaClass.simpleName}")
                        }
                    }
                }

                override fun complete(
                    context: CommandContext,
                    sender: PlayerSession?,
                    providedArgs: List<String>,
                    activeArgIndex: Int,
                    activeArgPrefix: String
                ): List<String> {
                    val commandSender = RuntimeCommandSender.fromSession(sender)
                    val completion = resolveCompletions(
                        built = null,
                        commandSender = commandSender,
                        providedArgs = providedArgs,
                        activeArgIndex = activeArgIndex,
                        activeArgPrefix = activeArgPrefix
                    )
                    val suggestions = completion.first
                    val appendMode = completion.second
                    if (!appendMode) return suggestions
                    return suggestions.map(CommandCompletionEncoding::encodeAppend)
                }

                private fun resolveCompletions(
                    built: CommandHandler?,
                    commandSender: CommandSender,
                    providedArgs: List<String>,
                    activeArgIndex: Int,
                    activeArgPrefix: String
                ): Pair<List<String>, Boolean> {
                    val completionArgs = providedArgs.toMutableList()
                    if (activeArgIndex >= 0) {
                        while (completionArgs.size <= activeArgIndex) {
                            completionArgs += ""
                        }
                        completionArgs[activeArgIndex] = activeArgPrefix
                    }
                    val invocation = CommandInvocation(name = commandName, args = completionArgs, sender = commandSender)
                    val permissionNode = permission
                    if (permissionNode != null && !permissions.has(commandSender, permissionNode)) {
                        return emptyList<String>() to false
                    }
                    val apiContext = buildApiCommandContext(
                        commandName = invocation.name,
                        args = invocation.args,
                        sender = invocation.sender,
                        activeArgIndex = activeArgIndex,
                        activeArgPrefix = activeArgPrefix
                    )
                    val resolved = built ?: resolveHandler(apiContext, invocation.name) ?: return emptyList<String>() to false
                    val completions = resolved.complete?.invoke().orEmpty()

                    val appendMode = apiContext.appendCompletionsToInput
                    val normalized = if (appendMode) {
                        completions.asSequence()
                            .map(String::trim)
                            .filter { it.isNotEmpty() }
                            .distinct()
                            .toList()
                    } else {
                        completions.asSequence()
                            .filter { it.startsWith(activeArgPrefix, ignoreCase = true) }
                            .distinct()
                            .sortedWith(String.CASE_INSENSITIVE_ORDER)
                            .toList()
                    }
                    return normalized to appendMode
                }

                private fun resolveHandler(apiContext: ApiCommandContext, command: String): CommandHandler? {
                    return runCatching {
                        PluginSystem.withPluginOwner(this@RuntimeCommandRegistrar.owner) {
                            bound.invoke(apiContext) as CommandHandler
                        }
                    }.onFailure { throwable ->
                        logger.error(
                            "Annotated command handler failed (owner={}, handler={}, method={}, command={})",
                            this@RuntimeCommandRegistrar.owner,
                            handler.javaClass.name,
                            method.name,
                            command,
                            throwable
                        )
                    }.getOrNull()
                }
            }
            val wrappedAliases = aliases.map { it.lowercase() }.filter { it.isNotBlank() && it != commandName }
            PlayerSessionManager.registerDynamicCommand(commandName, wrappedAliases, wrappedCommand)
            specsByPrimaryName[commandName] = CommandSpec(
                name = commandName,
                aliases = wrappedAliases,
                permission = permission,
                executor = CommandExecutor { },
                completer = null
            )
            aliasesByPrimaryName[commandName] = wrappedAliases
            out += object : RegisteredCommand {
                override val name: String = commandName

                override fun unregister() {
                    val removedAliases = aliasesByPrimaryName.remove(commandName).orEmpty()
                    specsByPrimaryName.remove(commandName)
                    PlayerSessionManager.unregisterDynamicCommand(commandName, removedAliases)
                }
            }
        }
        return out
    }

    override fun unregisterOwner(owner: String) {
        if (owner != this.owner) return
        close()
    }

    override fun commandNames(): Set<String> {
        return PlayerSessionManager.dynamicAndBuiltinCommandNames()
    }

    fun close() {
        val snapshot = aliasesByPrimaryName.entries.toList()
        aliasesByPrimaryName.clear()
        specsByPrimaryName.clear()
        for ((primary, aliases) in snapshot) {
            PlayerSessionManager.unregisterDynamicCommand(primary, aliases)
        }
    }

    private fun buildApiCommandContext(
        commandName: String,
        args: List<String>,
        sender: CommandSender,
        activeArgIndex: Int,
        activeArgPrefix: String
    ): ApiCommandContext {
        val inputs = ArrayList<String>(args.size + 1)
        inputs += commandName
        if (args.isNotEmpty()) {
            inputs += args
        }
        if (activeArgIndex >= 0) {
            val inputIndex = activeArgIndex + 1
            while (inputs.size <= inputIndex) {
                inputs += ""
            }
            inputs[inputIndex] = activeArgPrefix
        }
        return ApiCommandContext(
            name = commandName,
            inputs = inputs,
            sender = sender,
            activeInputPrefix = activeArgPrefix,
            onlinePlayerNamesProvider = {
                PlayerSessionManager.players()
                    .asSequence()
                    .map { it.profile.username }
                    .distinct()
                    .sortedWith(String.CASE_INSENSITIVE_ORDER)
                    .toList()
            },
            worldKeysProvider = {
                WorldManager.allWorlds()
                    .asSequence()
                    .map { it.key.substringAfter(':', it.key) }
                    .distinct()
                    .sortedWith(String.CASE_INSENSITIVE_ORDER)
                    .toList()
            },
            pluginIdsProvider = { PluginSystem.pluginCompletionCandidates() },
            commandNamesProvider = { PlayerSessionManager.dynamicAndBuiltinCommandNames() },
            itemKeysProvider = {
                runtimeTypeRegistry.allItems()
                    .asSequence()
                    .map { it.key }
                    .distinct()
                    .sortedWith(String.CASE_INSENSITIVE_ORDER)
                    .toList()
            },
            blockKeysProvider = {
                runtimeTypeRegistry.allBlocks()
                    .asSequence()
                    .map { it.key }
                    .distinct()
                    .sortedWith(String.CASE_INSENSITIVE_ORDER)
                    .toList()
            }
        )
    }
}

private class StandardPermissionService : PermissionService {
    private data class RuleKey(
        val owner: String,
        val subject: String,
        val node: String
    )

    private val rules = ConcurrentHashMap<RuleKey, PermissionResult>()

    override fun check(sender: CommandSender?, node: String): PermissionResult {
        if (sender == null) return PermissionResult.ALLOW
        val subject = ((sender as? PermissionAwareSender)?.permissionSubject() ?: sender.name).lowercase()
        val matching = rules.entries.firstOrNull { entry ->
            entry.key.subject == subject && entry.key.node == node.lowercase()
        }
        if (matching != null) {
            return matching.value
        }
        val fallback = (sender as? PermissionAwareSender)?.baseHasPermission(node) ?: sender.hasPermission(node)
        return if (fallback) PermissionResult.ALLOW else PermissionResult.UNSET
    }

    fun clearOwner(owner: String) {
        val it = rules.entries.iterator()
        while (it.hasNext()) {
            val entry = it.next()
            if (entry.key.owner == owner) {
                it.remove()
            }
        }
    }
}

private class FastEventBus : EventBus {
    private data class RegisteredHandler(
        val owner: String,
        val eventType: Class<out Event>,
        val priority: EventPriority,
        val receiveCancelled: Boolean,
        val invoke: (Event) -> Unit,
        val filter: ((Event) -> Boolean)?
    )

    private inner class SubscriptionHandle(
        private val owner: String,
        private val handler: RegisteredHandler
    ) : EventSubscription {
        private val removed = AtomicBoolean(false)

        override val active: Boolean
            get() = !removed.get()

        override fun unregister() {
            if (!removed.compareAndSet(false, true)) return
            handlersByType[handler.eventType]?.remove(handler)
            subscriptionsByOwner[owner]?.remove(this)
            dispatchCache.clear()
        }
    }

    private val handlersByType = ConcurrentHashMap<Class<out Event>, CopyOnWriteArrayList<RegisteredHandler>>()
    private val subscriptionsByOwner = ConcurrentHashMap<String, CopyOnWriteArrayList<EventSubscription>>()
    private val dispatchCache = ConcurrentHashMap<Class<out Event>, List<RegisteredHandler>>()

    override fun <E : Event> listen(
        owner: String,
        eventType: Class<E>,
        priority: EventPriority,
        receiveCancelled: Boolean,
        filter: EventFilter<E>?,
        handler: (E) -> Unit
    ): EventSubscription {
        val castedFilter: ((Event) -> Boolean)? = if (filter == null) {
            null
        } else {
            { event -> filter.test(event as E) }
        }
        val entry = RegisteredHandler(
            owner = owner,
            eventType = eventType,
            priority = priority,
            receiveCancelled = receiveCancelled,
            invoke = { event -> handler(event as E) },
            filter = castedFilter
        )
        val bucket = handlersByType.computeIfAbsent(eventType) { CopyOnWriteArrayList() }
        bucket.add(entry)
        bucket.sortWith(compareBy<RegisteredHandler> { it.priority.ordinal })

        val subscription = SubscriptionHandle(owner, entry)
        subscriptionsByOwner.computeIfAbsent(owner) { CopyOnWriteArrayList() }.add(subscription)
        dispatchCache.clear()
        return subscription
    }

    override fun registerAnnotated(owner: String, listener: Any): List<EventSubscription> {
        val out = ArrayList<EventSubscription>()
        for (method in listener.javaClass.methods) {
            val subscribe = method.getAnnotation(Subscribe::class.java) ?: continue
            if (method.parameterCount != 1) continue
            val parameterType = method.parameterTypes[0]
            if (!Event::class.java.isAssignableFrom(parameterType)) continue
            val eventType = parameterType as Class<out Event>
            method.isAccessible = true
            val handle = MethodHandles.lookup().unreflect(method).bindTo(listener)
            val sub = listen(
                owner = owner,
                eventType = eventType,
                priority = subscribe.priority,
                receiveCancelled = subscribe.receiveCancelled,
                filter = null,
                handler = { event -> handle.invoke(event) }
            )
            out += sub
        }
        return out
    }

    override fun unregisterOwner(owner: String) {
        val subs = subscriptionsByOwner.remove(owner).orEmpty()
        for (sub in subs) {
            sub.unregister()
        }
    }

    override fun <E : Event> post(event: E): E {
        val handlers = dispatchHandlersFor(event.javaClass)
        for (handler in handlers) {
            if (event is CancellableEvent && event.cancelled && !handler.receiveCancelled) {
                continue
            }
            if (handler.filter?.invoke(event) == false) {
                continue
            }
            try {
                PluginSystem.withPluginOwner(handler.owner) {
                    handler.invoke(event)
                }
            } catch (error: Throwable) {
                LoggerFactory.getLogger(FastEventBus::class.java)
                    .error("Plugin event handler failed (owner={}, event={})", handler.owner, event.javaClass.name, error)
            }
        }
        return event
    }

    override fun hasListeners(eventType: Class<out Event>): Boolean {
        return dispatchHandlersFor(eventType).isNotEmpty()
    }

    private fun dispatchHandlersFor(eventClass: Class<out Event>): List<RegisteredHandler> {
        return dispatchCache.computeIfAbsent(eventClass) {
            handlersByType.entries.asSequence()
                .filter { entry -> entry.key.isAssignableFrom(eventClass) }
                .flatMap { entry -> entry.value.asSequence() }
                .sortedBy { it.priority.ordinal }
                .toList()
        }
    }
}

private class GenerationServiceRegistry : ServiceRegistry {
    private data class ServiceEntry(
        val owner: String,
        val generation: Long,
        val service: Any
    )

    private val servicesByType = ConcurrentHashMap<Class<*>, ServiceEntry>()
    private val generationByOwner = ConcurrentHashMap<String, AtomicLong>()

    override fun <T : Any> publish(owner: String, key: Class<T>, service: T): ServiceHandle<T> {
        val generation = generationByOwner.computeIfAbsent(owner) { AtomicLong(0L) }.incrementAndGet()
        val entry = ServiceEntry(owner = owner, generation = generation, service = service)
        servicesByType[key] = entry
        return object : ServiceHandle<T> {
            override val owner: String = owner
            override val generation: Long = generation

            override fun isValid(): Boolean {
                val active = servicesByType[key] ?: return false
                return active.owner == owner && active.generation == generation
            }

            override fun getOrNull(): T? {
                val active = servicesByType[key] ?: return null
                if (active.owner != owner || active.generation != generation) return null
                @Suppress("UNCHECKED_CAST")
                return active.service as T
            }
        }
    }

    override fun <T : Any> resolve(key: Class<T>): ServiceHandle<T>? {
        val entry = servicesByType[key] ?: return null
        @Suppress("UNCHECKED_CAST")
        return publishSnapshot(key, entry as ServiceEntry)
    }

    private fun <T : Any> publishSnapshot(key: Class<T>, entry: ServiceEntry): ServiceHandle<T> {
        return object : ServiceHandle<T> {
            override val owner: String = entry.owner
            override val generation: Long = entry.generation

            override fun isValid(): Boolean {
                val active = servicesByType[key] ?: return false
                return active.owner == entry.owner && active.generation == entry.generation
            }

            override fun getOrNull(): T? {
                val active = servicesByType[key] ?: return null
                if (active.owner != entry.owner || active.generation != entry.generation) return null
                @Suppress("UNCHECKED_CAST")
                return active.service as T
            }
        }
    }

    override fun invalidateOwner(owner: String) {
        generationByOwner.computeIfAbsent(owner) { AtomicLong(0L) }.incrementAndGet()
        val it = servicesByType.entries.iterator()
        while (it.hasNext()) {
            val entry = it.next()
            if (entry.value.owner == owner) {
                it.remove()
            }
        }
    }
}

private class AsyncPluginDataStore(pluginId: String) : PluginDataStore {
    override val dataDirectory: Path = Path.of("plugins", pluginId, "data")
    private val ioExecutor: ExecutorService = Executors.newSingleThreadExecutor { runnable ->
        Thread(runnable, "aerogel-plugin-data-$pluginId").apply { isDaemon = true }
    }

    init {
        runCatching { Files.createDirectories(dataDirectory) }
    }

    override fun resolve(key: String): Path {
        val normalized = key.trim().removePrefix("/").replace("..", "_")
        return dataDirectory.resolve(normalized)
    }

    override fun saveBytes(key: String, bytes: ByteArray): CompletableFuture<Unit> {
        return CompletableFuture.supplyAsync({
            val target = resolve(key)
            val parent = target.parent
            if (parent != null) {
                Files.createDirectories(parent)
            }
            val temp = target.resolveSibling("${target.fileName}.tmp")
            Files.write(temp, bytes)
            Files.move(temp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
            Unit
        }, ioExecutor)
    }

    override fun loadBytes(key: String): CompletableFuture<ByteArray?> {
        return CompletableFuture.supplyAsync({
            val path = resolve(key)
            if (!Files.exists(path)) return@supplyAsync null
            Files.readAllBytes(path)
        }, ioExecutor)
    }

    override fun delete(key: String): CompletableFuture<Boolean> {
        return CompletableFuture.supplyAsync({
            val path = resolve(key)
            Files.deleteIfExists(path)
        }, ioExecutor)
    }

    fun shutdown() {
        ioExecutor.shutdownNow()
    }
}

private class SimpleTaskScheduler : TaskScheduler {
    private val perOwnerExecutors = ConcurrentHashMap<String, ScheduledExecutorService>()
    private val generationByOwner = ConcurrentHashMap<String, AtomicLong>()

    override fun runSync(task: () -> Unit) {
        task()
    }

    override fun <T> supplyAsync(task: () -> T): CompletableFuture<T> {
        return CompletableFuture.supplyAsync(task)
    }

    override fun runAsync(task: () -> Unit): ScheduledTask {
        val future = CompletableFuture.runAsync(task)
        return CompletableFutureScheduledTask(future)
    }

    override fun runAsyncLater(delay: Duration, task: () -> Unit): ScheduledTask {
        val ownerExecutor = ownerExecutorFor(GLOBAL_OWNER)
        val future = ownerExecutor.schedule(task, delay.toDelayNanos(), TimeUnit.NANOSECONDS)
        return ScheduledFutureTask(future)
    }

    override fun runAsyncRepeating(initialDelay: Duration, period: Duration, task: () -> Unit): ScheduledTask {
        val ownerExecutor = ownerExecutorFor(GLOBAL_OWNER)
        val repeatingNanos = period.toPeriodNanos()
        val future = ownerExecutor.scheduleAtFixedRate(
            task,
            initialDelay.toDelayNanos(),
            repeatingNanos,
            TimeUnit.NANOSECONDS
        )
        return ScheduledFutureTask(future)
    }

    override fun shutdownOwner(owner: String) {
        generationByOwner.computeIfAbsent(owner) { AtomicLong(0L) }.incrementAndGet()
        perOwnerExecutors.remove(owner)?.shutdownNow()
    }

    fun ownerScoped(owner: String): TaskScheduler {
        val ownerExecutor = ownerExecutorFor(owner)
        val generation = generationByOwner.computeIfAbsent(owner) { AtomicLong(0L) }.get()
        return object : TaskScheduler {
            override fun runSync(task: () -> Unit) {
                if (!isGenerationActive(owner, generation)) return
                PluginSystem.withPluginOwner(owner) {
                    task()
                }
            }

            override fun <T> supplyAsync(task: () -> T): CompletableFuture<T> {
                return CompletableFuture.supplyAsync({
                    if (!isGenerationActive(owner, generation)) {
                        throw IllegalStateException("Task owner '$owner' is no longer active.")
                    }
                    PluginSystem.withPluginOwner(owner) {
                        task()
                    }
                }, ownerExecutor)
            }

            override fun runAsync(task: () -> Unit): ScheduledTask {
                val wrapped = Runnable {
                    if (!isGenerationActive(owner, generation)) return@Runnable
                    PluginSystem.withPluginOwner(owner) {
                        task()
                    }
                }
                val future = ownerExecutor.schedule(wrapped, 0L, TimeUnit.NANOSECONDS)
                return ScheduledFutureTask(future)
            }

            override fun runAsyncLater(delay: Duration, task: () -> Unit): ScheduledTask {
                val wrapped = Runnable {
                    if (!isGenerationActive(owner, generation)) return@Runnable
                    PluginSystem.withPluginOwner(owner) {
                        task()
                    }
                }
                val future = ownerExecutor.schedule(wrapped, delay.toDelayNanos(), TimeUnit.NANOSECONDS)
                return ScheduledFutureTask(future)
            }

            override fun runAsyncRepeating(initialDelay: Duration, period: Duration, task: () -> Unit): ScheduledTask {
                val repeatingNanos = period.toPeriodNanos()
                val wrapped = Runnable {
                    if (!isGenerationActive(owner, generation)) return@Runnable
                    PluginSystem.withPluginOwner(owner) {
                        task()
                    }
                }
                val future = ownerExecutor.scheduleAtFixedRate(
                    wrapped,
                    initialDelay.toDelayNanos(),
                    repeatingNanos,
                    TimeUnit.NANOSECONDS
                )
                return ScheduledFutureTask(future)
            }

            override fun shutdownOwner(owner: String) {
                this@SimpleTaskScheduler.shutdownOwner(owner)
            }
        }
    }

    fun shutdownAll() {
        for (generation in generationByOwner.values) {
            generation.incrementAndGet()
        }
        for (executor in perOwnerExecutors.values) {
            runCatching { executor.shutdownNow() }
        }
        perOwnerExecutors.clear()
    }

    private fun isGenerationActive(owner: String, generation: Long): Boolean {
        return generationByOwner.computeIfAbsent(owner) { AtomicLong(0L) }.get() == generation
    }

    private fun ownerExecutorFor(owner: String): ScheduledExecutorService {
        return perOwnerExecutors.computeIfAbsent(owner) { ownerKey ->
            Executors.newSingleThreadScheduledExecutor { runnable ->
                Thread(runnable, "aerogel-plugin-async-$ownerKey").apply { isDaemon = true }
            }
        }
    }

    private class ScheduledFutureTask(
        private val delegate: ScheduledFuture<*>
    ) : ScheduledTask {
        override val isCancelled: Boolean
            get() = delegate.isCancelled
        override val isDone: Boolean
            get() = delegate.isDone

        override fun cancel(mayInterruptIfRunning: Boolean): Boolean {
            return delegate.cancel(mayInterruptIfRunning)
        }
    }

    private class CompletableFutureScheduledTask(
        private val delegate: CompletableFuture<*>
    ) : ScheduledTask {
        override val isCancelled: Boolean
            get() = delegate.isCancelled
        override val isDone: Boolean
            get() = delegate.isDone

        override fun cancel(mayInterruptIfRunning: Boolean): Boolean {
            return delegate.cancel(mayInterruptIfRunning)
        }
    }

    private fun Duration.toDelayNanos(): Long {
        if (isInfinite()) {
            throw IllegalArgumentException("Delay must be finite.")
        }
        return inWholeNanoseconds.coerceAtLeast(0L)
    }

    private fun Duration.toPeriodNanos(): Long {
        if (isInfinite()) {
            throw IllegalArgumentException("Period must be finite.")
        }
        val nanos = inWholeNanoseconds
        require(nanos > 0L) { "Period must be greater than zero." }
        return nanos
    }

    private companion object {
        const val GLOBAL_OWNER = "__global__"
    }
}

private class SimpleTickScheduler : TickScheduler {
    private data class TickTaskEntry(
        val id: Long,
        val owner: String,
        val task: () -> Unit,
        val mode: ExecutionMode,
        val worldKey: String?,
        val chunkX: Int?,
        val chunkZ: Int?,
        val periodTicks: Long?,
        var nextRunTick: Long,
        var cancelled: Boolean = false,
        var done: Boolean = false
    )

    private enum class ExecutionMode {
        GAME,
        CHUNK
    }

    private val logger = LoggerFactory.getLogger(SimpleTickScheduler::class.java)
    private val lock = Any()
    private val nextTaskId = AtomicLong(1L)
    private val currentTickRef = AtomicLong(0L)
    private val tasksById = LinkedHashMap<Long, TickTaskEntry>()

    override val currentTick: Long
        get() = currentTickRef.get()

    override fun runLater(
        delayTicks: Long,
        task: () -> Unit
    ): TickTask {
        return scheduleGame(owner = GLOBAL_OWNER, delayTicks = delayTicks, periodTicks = null, task = task)
    }

    override fun runRepeating(
        initialDelayTicks: Long,
        periodTicks: Long,
        task: () -> Unit
    ): TickTask {
        require(periodTicks > 0L) { "periodTicks must be greater than zero." }
        return scheduleGame(owner = GLOBAL_OWNER, delayTicks = initialDelayTicks, periodTicks = periodTicks, task = task)
    }

    fun global(): TickScheduler {
        return ownerScoped(GLOBAL_OWNER)
    }

    fun chunkGlobal(worldKey: String, chunkX: Int, chunkZ: Int): TickScheduler {
        return chunkScoped(
            owner = GLOBAL_OWNER,
            worldKey = worldKey,
            chunkX = chunkX,
            chunkZ = chunkZ
        )
    }

    fun ownerScoped(owner: String): TickScheduler {
        return object : TickScheduler {
            override val currentTick: Long
                get() = this@SimpleTickScheduler.currentTick

            override fun runLater(
                delayTicks: Long,
                task: () -> Unit
            ): TickTask {
                return scheduleGame(owner, delayTicks, null, task)
            }

            override fun runRepeating(
                initialDelayTicks: Long,
                periodTicks: Long,
                task: () -> Unit
            ): TickTask {
                require(periodTicks > 0L) { "periodTicks must be greater than zero." }
                return scheduleGame(owner, initialDelayTicks, periodTicks, task)
            }
        }
    }

    fun chunkScoped(owner: String, worldKey: String, chunkX: Int, chunkZ: Int): TickScheduler {
        return object : TickScheduler {
            override val currentTick: Long
                get() = this@SimpleTickScheduler.currentTick

            override fun runLater(delayTicks: Long, task: () -> Unit): TickTask {
                return scheduleChunk(owner, worldKey, chunkX, chunkZ, delayTicks, null, task)
            }

            override fun runRepeating(initialDelayTicks: Long, periodTicks: Long, task: () -> Unit): TickTask {
                require(periodTicks > 0L) { "periodTicks must be greater than zero." }
                return scheduleChunk(owner, worldKey, chunkX, chunkZ, initialDelayTicks, periodTicks, task)
            }
        }
    }

    fun pulse(gameTick: Long) {
        currentTickRef.set(gameTick)
        val dueTasks = ArrayList<TickTaskEntry>()
        synchronized(lock) {
            val iter = tasksById.values.iterator()
            while (iter.hasNext()) {
                val entry = iter.next()
                if (entry.cancelled) {
                    entry.done = true
                    iter.remove()
                    continue
                }
                if (entry.nextRunTick <= gameTick) {
                    dueTasks.add(entry)
                }
            }
        }
        for (entry in dueTasks) {
            if (entry.cancelled || entry.done) continue
            dispatch(entry)
            synchronized(lock) {
                val current = tasksById[entry.id] ?: continue
                if (current.cancelled) {
                    current.done = true
                    tasksById.remove(current.id)
                    continue
                }
                val period = current.periodTicks
                if (period == null) {
                    current.done = true
                    tasksById.remove(current.id)
                    continue
                }
                var next = current.nextRunTick + period
                if (next <= gameTick) {
                    val missed = ((gameTick - next) / period) + 1L
                    next += missed * period
                }
                current.nextRunTick = next
            }
        }
    }

    fun shutdownOwner(owner: String) {
        synchronized(lock) {
            val iter = tasksById.values.iterator()
            while (iter.hasNext()) {
                val entry = iter.next()
                if (entry.owner != owner) continue
                entry.cancelled = true
                entry.done = true
                iter.remove()
            }
        }
    }

    fun shutdownAll() {
        synchronized(lock) {
            for (entry in tasksById.values) {
                entry.cancelled = true
                entry.done = true
            }
            tasksById.clear()
        }
    }

    private fun scheduleGame(
        owner: String,
        delayTicks: Long,
        periodTicks: Long?,
        task: () -> Unit
    ): TickTask {
        return schedule(
            owner = owner,
            delayTicks = delayTicks,
            periodTicks = periodTicks,
            mode = ExecutionMode.GAME,
            worldKey = null,
            chunkX = null,
            chunkZ = null,
            task = task
        )
    }

    private fun scheduleChunk(
        owner: String,
        worldKey: String,
        chunkX: Int,
        chunkZ: Int,
        delayTicks: Long,
        periodTicks: Long?,
        task: () -> Unit
    ): TickTask {
        return schedule(
            owner = owner,
            delayTicks = delayTicks,
            periodTicks = periodTicks,
            mode = ExecutionMode.CHUNK,
            worldKey = worldKey,
            chunkX = chunkX,
            chunkZ = chunkZ,
            task = task
        )
    }

    private fun schedule(
        owner: String,
        delayTicks: Long,
        periodTicks: Long?,
        mode: ExecutionMode,
        worldKey: String?,
        chunkX: Int?,
        chunkZ: Int?,
        task: () -> Unit
    ): TickTask {
        val now = currentTickRef.get()
        val safeDelay = delayTicks.coerceAtLeast(0L)
        val entry = TickTaskEntry(
            id = nextTaskId.getAndIncrement(),
            owner = owner,
            task = task,
            mode = mode,
            worldKey = worldKey,
            chunkX = chunkX,
            chunkZ = chunkZ,
            periodTicks = periodTicks,
            nextRunTick = now + safeDelay
        )
        synchronized(lock) {
            tasksById[entry.id] = entry
        }
        return object : TickTask {
            override val isCancelled: Boolean
                get() = synchronized(lock) { tasksById[entry.id]?.cancelled ?: true }

            override val isDone: Boolean
                get() = synchronized(lock) { tasksById[entry.id]?.done ?: true }

            override fun cancel(): Boolean {
                synchronized(lock) {
                    val current = tasksById[entry.id] ?: return false
                    if (current.done || current.cancelled) return false
                    current.cancelled = true
                    current.done = true
                    tasksById.remove(current.id)
                    return true
                }
            }
        }
    }

    private fun dispatch(entry: TickTaskEntry) {
        when (entry.mode) {
            ExecutionMode.GAME -> {
                runCatching {
                    PluginSystem.withPluginOwner(entry.owner) {
                        entry.task.invoke()
                    }
                }
                    .onFailure { throwable -> logger.error("Tick task failed (owner={})", entry.owner, throwable) }
            }

            ExecutionMode.CHUNK -> {
                val worldKey = entry.worldKey ?: return
                val chunkX = entry.chunkX ?: return
                val chunkZ = entry.chunkZ ?: return
                val world = WorldManager.world(worldKey) ?: return
                world.submitOnChunkActor(ChunkPos(chunkX, chunkZ)) {
                    runCatching {
                        PluginSystem.withPluginOwner(entry.owner) {
                            entry.task.invoke()
                        }
                    }
                        .onFailure { throwable -> logger.error("Chunk tick task failed (owner={})", entry.owner, throwable) }
                }
            }
        }
    }

    private companion object {
        const val GLOBAL_OWNER = "__global__"
    }
}

private class SimplePacketInterceptorRegistry : PacketInterceptorRegistry {
    private data class Entry(
        val owner: String,
        val interceptor: PacketInterceptor,
        val active: AtomicBoolean = AtomicBoolean(true)
    )

    private val interceptors = CopyOnWriteArrayList<Entry>()

    override fun register(owner: String, interceptor: PacketInterceptor): PacketInterceptorHandle {
        val entry = Entry(owner = owner, interceptor = interceptor)
        interceptors += entry
        return object : PacketInterceptorHandle {
            override val active: Boolean
                get() = entry.active.get()

            override fun unregister() {
                if (!entry.active.compareAndSet(true, false)) return
                interceptors.remove(entry)
            }
        }
    }

    override fun unregisterOwner(owner: String) {
        val it = interceptors.iterator()
        while (it.hasNext()) {
            val entry = it.next()
            if (entry.owner == owner) {
                entry.active.set(false)
                interceptors.remove(entry)
            }
        }
    }

    override fun intercept(packet: PacketEnvelope): PacketEnvelope? {
        var current: PacketEnvelope? = packet
        for (entry in interceptors) {
            if (!entry.active.get()) continue
            current = current?.let { entry.interceptor.intercept(it) }
            if (current == null) {
                return null
            }
        }
        return current
    }
}

private class PluginClassLoader(
    urls: Array<URL>,
    parent: ClassLoader
) : URLClassLoader(urls, parent) {
    override fun loadClass(name: String, resolve: Boolean): Class<*> {
        if (name.startsWith("org.macaroon3145.api.")) {
            return super.loadClass(name, resolve)
        }
        synchronized(getClassLoadingLock(name)) {
            findLoadedClass(name)?.let { return it }
            try {
                val found = findClass(name)
                if (resolve) resolveClass(found)
                return found
            } catch (_: ClassNotFoundException) {
                return super.loadClass(name, resolve)
            }
        }
    }
}
