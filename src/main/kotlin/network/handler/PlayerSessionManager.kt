package org.macaroon3145.network.handler

import io.netty.buffer.Unpooled
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelId
import io.netty.channel.ChannelFuture
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.macaroon3145.config.ServerConfig
import org.macaroon3145.DebugConsole
import org.macaroon3145.ServerLifecycle
import org.macaroon3145.i18n.ServerI18n
import org.macaroon3145.ui.ServerDashboard
import org.macaroon3145.network.command.CommandContext
import org.macaroon3145.network.command.CommandDispatcher
import org.macaroon3145.network.command.Command
import org.macaroon3145.network.command.EntitySelectorCompletions
import org.macaroon3145.network.NetworkUtils
import org.macaroon3145.network.codec.BlockStateRegistry
import org.macaroon3145.network.codec.BlockEntityTypeRegistry
import org.macaroon3145.network.codec.ItemBlockStateRegistry
import org.macaroon3145.network.codec.RegistryCodec
import org.macaroon3145.network.item.FurnaceFuelCache
import org.macaroon3145.network.item.FoodItemCache
import org.macaroon3145.network.recipe.CraftingRecipeCache
import org.macaroon3145.network.recipe.SmeltingRecipeCache
import org.macaroon3145.network.packet.PlayerSample
import org.macaroon3145.api.entity.ArmorSlot
import org.macaroon3145.api.event.BlockChangeReason
import org.macaroon3145.api.event.MoveReason
import org.macaroon3145.world.BlockPos
import org.macaroon3145.world.ChunkPos
import org.macaroon3145.world.DroppedItemSnapshot
import org.macaroon3145.world.FallingBlockSnapshot
import org.macaroon3145.world.FoliaSidecarSpawnPointProvider
import org.macaroon3145.world.AnimalDamageCause
import org.macaroon3145.world.AnimalKind
import org.macaroon3145.world.AnimalRideControl
import org.macaroon3145.world.AnimalTickEvents
import org.macaroon3145.world.EntityLootDropCache
import org.macaroon3145.world.AnimalSnapshot
import org.macaroon3145.world.AnimalTemptSource
import org.macaroon3145.world.AnimalLookSource
import org.macaroon3145.world.BlockCollisionRegistry
import org.macaroon3145.world.EntityHitboxRegistry
import org.macaroon3145.world.ThrownItemKind
import org.macaroon3145.world.ThrownItemSnapshot
import org.macaroon3145.world.VanillaMiningRules
import org.macaroon3145.world.WorldManager
import org.macaroon3145.world.generators.FoliaSharedMemoryWorldGenerator
import java.util.ArrayList
import java.util.HashSet
import java.io.ByteArrayOutputStream
import java.io.DataOutputStream
import java.nio.file.Files
import java.nio.file.Path
import java.util.Locale
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.atomic.DoubleAdder
import java.util.concurrent.locks.LockSupport
import kotlin.math.abs
import kotlin.math.ceil
import kotlin.math.cos
import kotlin.math.floor
import kotlin.math.min
import kotlin.math.max
import kotlin.math.sin
import kotlin.math.sqrt
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
    var lastYaw: Float = 0f,
    var lastPitch: Float = 0f,
    var lastHeadYaw: Float = 0f
)

data class PlayerSession(
    val channelId: ChannelId,
    val profile: ConnectionProfile,
    @Volatile var locale: String,
    @Volatile var displayName: String?,
    @Volatile var worldKey: String,
    val entityId: Int,
    val skinPartsMask: Int,
    @Volatile var pingMs: Int,
    @Volatile var x: Double,
    @Volatile var y: Double,
    @Volatile var z: Double,
    @Volatile var clientEyeX: Double,
    @Volatile var clientEyeY: Double,
    @Volatile var clientEyeZ: Double,
    @Volatile var velocityX: Double,
    @Volatile var velocityY: Double,
    @Volatile var velocityZ: Double,
    @Volatile var accelerationX: Double,
    @Volatile var accelerationY: Double,
    @Volatile var accelerationZ: Double,
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
    @Volatile var foodExhaustion: Float,
    @Volatile var regenAccumulatorSeconds: Double,
    @Volatile var usingItemHand: Int,
    @Volatile var usingItemId: Int,
    @Volatile var usingItemRemainingSeconds: Double,
    @Volatile var usingItemSoundKey: String?,
    @Volatile var usingItemSoundDelaySeconds: Double,
    @Volatile var fallDistance: Double,
    @Volatile var attackStrengthTickerTicks: Double,
    @Volatile var hurtInvulnerableSeconds: Double,
    @Volatile var lastHurtAmount: Float,
    @Volatile var dead: Boolean,
    @Volatile var sneaking: Boolean,
    @Volatile var sprinting: Boolean,
    @Volatile var swimming: Boolean,
    @Volatile var inputForward: Boolean,
    @Volatile var inputBackward: Boolean,
    @Volatile var inputLeft: Boolean,
    @Volatile var inputRight: Boolean,
    @Volatile var inputJump: Boolean,
    @Volatile var previousInputJump: Boolean,
    @Volatile var airJumpAttemptCount: Int,
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
    @Volatile var openContainerId: Int,
    @Volatile var openContainerType: Int,
    @Volatile var openFurnaceX: Int,
    @Volatile var openFurnaceY: Int,
    @Volatile var openFurnaceZ: Int,
    @Volatile var nextContainerId: Int,
    @Volatile var cursorItemId: Int,
    @Volatile var cursorItemCount: Int,
    @Volatile var quickCraftActive: Boolean,
    @Volatile var quickCraftContainerId: Int,
    @Volatile var quickCraftMouseButton: Int,
    val quickCraftSlots: MutableSet<Int>,
    val playerCraftItemIds: IntArray,
    val playerCraftItemCounts: IntArray,
    val tableCraftItemIds: IntArray,
    val tableCraftItemCounts: IntArray,
    @Volatile var gameMode: Int,
    @Volatile var flying: Boolean,
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
    val visibleThrownItemEntityIds: MutableSet<Int>,
    val thrownItemTrackerStates: MutableMap<Int, DroppedItemTrackerState>,
    val visibleAnimalEntityIds: MutableSet<Int>,
    val animalTrackerStates: MutableMap<Int, DroppedItemTrackerState>,
    val generatingChunks: MutableSet<ChunkPos>,
    val chunkStreamVersion: AtomicInteger,
    val nextTeleportId: AtomicInteger,
    val chunkStreamInFlight: AtomicBoolean,
    val pendingChunkTarget: AtomicReference<ChunkStreamTarget?>,
    @Volatile var lastChunkMsptActionBarAtNanos: Long,
    @Volatile var ridingAnimalEntityId: Int
    ,
    @Volatile var lastClientVehicleX: Double,
    @Volatile var lastClientVehicleY: Double,
    @Volatile var lastClientVehicleZ: Double,
    @Volatile var lastClientVehicleYaw: Float,
    @Volatile var lastClientVehiclePitch: Float,
    @Volatile var lastClientVehicleOnGround: Boolean,
    @Volatile var lastClientVehiclePacketAtNanos: Long,
    @Volatile var suppressNextMoveEvent: Boolean,
    @Volatile var suppressMoveEchoX: Double,
    @Volatile var suppressMoveEchoY: Double,
    @Volatile var suppressMoveEchoZ: Double,
    @Volatile var suppressMoveEchoYaw: Float,
    @Volatile var suppressMoveEchoPitch: Float
)

data class ChunkStreamTarget(
    val streamId: Int,
    val centerChunkX: Int,
    val centerChunkZ: Int,
    val radius: Int
)

data class DashboardPlayerSnapshot(
    val worldKey: String,
    val uuid: UUID,
    val username: String,
    val x: Double,
    val z: Double,
    val skinPartsMask: Int,
    val texturesPropertyValue: String?
)

data class SpawnedEntityRef(
    val entityId: Int,
    val uuid: UUID
)

private data class WorldTimeState(
    var worldAgeTicks: Double,
    var timeOfDayTicks: Double,
    var broadcastAccumulatorSeconds: Double
)

private data class FurnaceKey(
    val worldKey: String,
    val x: Int,
    val y: Int,
    val z: Int
)

private data class WorldChunkKey(
    val worldKey: String,
    val chunkPos: ChunkPos
)

private data class PushingContactKey(
    val worldKey: String,
    val firstEntityId: Int,
    val secondEntityId: Int
)

private data class PushingContactState(
    val ownerChunk: ChunkPos,
    val remainingTicks: Int
)

private data class FurnaceState(
    var inputItemId: Int,
    var inputCount: Int,
    var fuelItemId: Int,
    var fuelCount: Int,
    var resultItemId: Int,
    var resultCount: Int,
    var burnRemainingSeconds: Double,
    var burnTotalSeconds: Double,
    var cookProgressSeconds: Double,
    var cookTotalSeconds: Double,
    var dirty: Boolean
)

data class CommandSuggestionWindow(
    val start: Int,
    val length: Int,
    val suggestions: List<String>
)

object PlayerSessionManager {
    private data class TickSimulationContext(
        val sessionsByWorld: Map<String, List<PlayerSession>>,
        val activeChunksByWorld: Map<String, Set<ChunkPos>>
    )

    private data class WorldPhysicsEvents(
        val droppedItems: org.macaroon3145.world.DroppedItemTickEvents,
        val fallingBlocks: org.macaroon3145.world.FallingBlockTickEvents,
        val thrownItems: org.macaroon3145.world.ThrownItemTickEvents,
        val animals: org.macaroon3145.world.AnimalTickEvents
    )

    private data class ChannelFlushState(
        val scheduled: AtomicBoolean = AtomicBoolean(false),
        val pendingPackets: ConcurrentLinkedQueue<ByteArray> = ConcurrentLinkedQueue()
    )

private data class PendingAnimalRemoval(
    val worldKey: String,
    val entityId: Int,
    val dueAtNanos: Long
)

private data class PigBoostState(
    var elapsedSeconds: Double,
    var durationSeconds: Double
)

private data class PlayerSimulationStamp(
    val worldKey: String,
    val centerChunkX: Int,
    val centerChunkZ: Int,
    val radius: Int
)

data class SimulationChunkDelta(
    val added: Set<ChunkPos>,
    val removed: Set<ChunkPos>
)

private data class ItemTextMeta(
    val expectedItemId: Int,
    val name: String?,
    val lore: List<String>
)

private data class PlayerItemTextMeta(
    val hotbar: Array<ItemTextMeta?> = arrayOfNulls(9),
    val main: Array<ItemTextMeta?> = arrayOfNulls(27),
    val armor: Array<ItemTextMeta?> = arrayOfNulls(4),
    var offhand: ItemTextMeta? = null
)

    private val logger = LoggerFactory.getLogger(PlayerSessionManager::class.java)
    private val gamemodeCompletionCandidates = listOf("survival", "creative", "adventure", "spectator")
    private val nextEntityId = AtomicInteger(1)
    private val sessions = ConcurrentHashMap<ChannelId, PlayerSession>()
    private val itemTextMetaByPlayerUuid = ConcurrentHashMap<UUID, PlayerItemTextMeta>()
    private val contexts = ConcurrentHashMap<ChannelId, ChannelHandlerContext>()
    private val channelFlushStates = ConcurrentHashMap<ChannelId, ChannelFlushState>()
    private val pendingAnimalRemovals = ConcurrentLinkedQueue<PendingAnimalRemoval>()
    private val pendingAnimalRemovalKeys = ConcurrentHashMap.newKeySet<String>()
    private val saddledAnimalEntityIds = ConcurrentHashMap.newKeySet<Int>()
    private val animalRiderEntityIdByAnimalEntityId = ConcurrentHashMap<Int, Int>()
    private val animalEntityIdByRiderEntityId = ConcurrentHashMap<Int, Int>()
    private val pigBoostStatesByAnimalEntityId = ConcurrentHashMap<Int, PigBoostState>()
    private val animalSourceDirtyWorlds = ConcurrentHashMap.newKeySet<String>()
    private val animalRideControlsActiveWorlds = ConcurrentHashMap.newKeySet<String>()
    private val attackCooldownActiveChannelIds = ConcurrentHashMap.newKeySet<ChannelId>()
    private val worldTimes = ConcurrentHashMap<String, WorldTimeState>()
    private val furnaceStates = ConcurrentHashMap<FurnaceKey, FurnaceState>()
    private val activeFurnaceKeys = ConcurrentHashMap.newKeySet<FurnaceKey>()
    private val activeFurnaceKeysByWorld = ConcurrentHashMap<String, MutableSet<FurnaceKey>>()
    private val pendingFurnaceResyncKeys = ConcurrentLinkedQueue<FurnaceKey>()
    private val pendingFurnaceResyncSet = ConcurrentHashMap.newKeySet<FurnaceKey>()
    private val furnaceChunkLastTickNanos = ConcurrentHashMap<WorldChunkKey, Long>()
    private val pushingChunkLastTickNanos = ConcurrentHashMap<WorldChunkKey, Long>()
    private val activeSimulationChunksCacheByWorld = ConcurrentHashMap<String, Set<ChunkPos>>()
    private val activeSimulationRefCountByWorld = ConcurrentHashMap<String, ConcurrentHashMap<ChunkPos, Int>>()
    private val activeSimulationStampByPlayerEntityId = ConcurrentHashMap<Int, PlayerSimulationStamp>()
    private val activeSimulationChunksByPlayerEntityId = ConcurrentHashMap<Int, Set<ChunkPos>>()
    private val activeSimulationDeltaLock = Any()
    private val activeSimulationDeltaByWorld = HashMap<String, SimulationChunkDelta>()
    @Volatile private var activeSimulationLastMaxDistanceChunks = -1
    private val retainedBaseChunksCacheByWorld = ConcurrentHashMap<String, Set<ChunkPos>>()
    private val retainedBaseRefCountByWorld = ConcurrentHashMap<String, ConcurrentHashMap<ChunkPos, Int>>()
    private val retainedBaseChunksByPlayerEntityId = ConcurrentHashMap<Int, Set<ChunkPos>>()
    private val retainedBaseWorldKeyByPlayerEntityId = ConcurrentHashMap<Int, String>()
    private val pushingWakeChunksByWorld = ConcurrentHashMap<String, MutableSet<ChunkPos>>()
    private val pushingContactStates = ConcurrentHashMap<PushingContactKey, PushingContactState>()
    private val lastPushingChunkByPlayerEntityId = ConcurrentHashMap<Int, ChunkPos>()
    private val furnaceChunkInFlight = ConcurrentHashMap<WorldChunkKey, AtomicBoolean>()
    private val pushingChunkInFlight = ConcurrentHashMap<WorldChunkKey, AtomicBoolean>()
    private val enderPearlCooldownEndNanosByPlayerEntityId = ConcurrentHashMap<Int, Long>()
    private val chorusFruitCooldownEndNanosByPlayerEntityId = ConcurrentHashMap<Int, Long>()
    private val lastUseItemSequenceByPlayerEntityId = ConcurrentHashMap<Int, Int>()
    private val chunkProcessingTickSequence = java.util.concurrent.atomic.AtomicLong(0L)
    private val commandExecutor = Executors.newFixedThreadPool(2) { runnable ->
        Thread(runnable, "aerogel-command-worker").apply {
            isDaemon = true
            priority = Thread.NORM_PRIORITY - 1
        }
    }
    @Volatile
    private var nextChunkMsptActionBarSweepAtNanos: Long = 0L
    private val operatorFilePath: Path = Path.of("op.txt")
        private val persistedOperatorUuids = ConcurrentHashMap.newKeySet<UUID>()
    private val operatorUuids = ConcurrentHashMap.newKeySet<java.util.UUID>()
    private val fluidDependentDropCache = ConcurrentHashMap<Int, List<org.macaroon3145.world.VanillaDrop>>()
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
        override fun sendSourceSuccessTranslation(target: PlayerSession?, key: String, vararg args: PlayPackets.ChatComponent) {
            if (target == null) {
                val renderedArgs = args.map { ServerI18n.componentToText(it) }
                ServerI18n.log("aerogel.log.console.prefixed", ServerI18n.translate(key, renderedArgs))
            } else {
                PlayerSessionManager.sendSystemTranslation(target, key, color = "green", *args)
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
        override fun playerNameComponent(target: PlayerSession): PlayPackets.ChatComponent = PlayerSessionManager.playerNameComponent(target)
        override fun teleport(target: PlayerSession, x: Double, y: Double, z: Double, yaw: Float?, pitch: Float?) =
            PlayerSessionManager.teleportPlayer(target, x, y, z, yaw, pitch, reason = MoveReason.COMMAND)
        override fun setGamemode(target: PlayerSession, mode: Int) = PlayerSessionManager.setPlayerGamemode(target, mode)
        override fun worldKeys(): List<String> = WorldManager.allWorlds().map { it.key }.sortedWith(String.CASE_INSENSITIVE_ORDER)
        override fun worldTimeSnapshot(worldKey: String): Pair<Long, Long>? = PlayerSessionManager.worldTimeSnapshotOrNull(worldKey)
        override fun setWorldTime(worldKey: String, timeOfDayTicks: Long): Pair<Long, Long>? =
            PlayerSessionManager.setWorldTimeAndBroadcast(worldKey, timeOfDayTicks)
        override fun addWorldTime(worldKey: String, deltaTicks: Long): Pair<Long, Long>? =
            PlayerSessionManager.addWorldTimeAndBroadcast(worldKey, deltaTicks)
        override fun showDashboard(): Boolean = ServerDashboard.showOrFocus()
        override fun stopServer(): Boolean = ServerLifecycle.stopServer()
        override fun reloadAllPlugins(): Int = org.macaroon3145.plugin.PluginSystem.reloadAll()
        override fun reloadPlugin(pluginId: String): Boolean = org.macaroon3145.plugin.PluginSystem.reloadPlugin(pluginId)
        override fun pluginIds(): List<String> = org.macaroon3145.plugin.PluginSystem.pluginIds()
    }
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
    private val itemIdByKey: Map<String, Int> by lazy {
        val out = HashMap<String, Int>(itemKeyById.size)
        for ((id, key) in itemKeyById.withIndex()) {
            if (key.isEmpty()) continue
            out[key] = id
        }
        out
    }
    private val itemMaxStackSizeByItemId: IntArray by lazy {
        val out = IntArray(itemKeyById.size) { MAX_HOTBAR_STACK_SIZE }
        val resource = PlayerSessionManager::class.java.classLoader
            .getResourceAsStream("data/minecraft/item/max_stack_size.json")
            ?: return@lazy out
        val root = runCatching {
            resource.bufferedReader().use { json.parseToJsonElement(it.readText()).jsonObject }
        }.getOrNull() ?: return@lazy out
        val defaultSize = root["default"]?.jsonPrimitive?.intOrNull
            ?.coerceIn(1, MAX_HOTBAR_STACK_SIZE)
            ?: MAX_HOTBAR_STACK_SIZE
        for (i in out.indices) {
            out[i] = defaultSize
        }
        val entries = root["entries"]?.jsonObject ?: return@lazy out
        for ((itemKey, maxElement) in entries) {
            val itemId = itemIdByKey[itemKey] ?: continue
            if (itemId !in out.indices) continue
            val maxStack = maxElement.jsonPrimitive.intOrNull
                ?.coerceIn(1, MAX_HOTBAR_STACK_SIZE)
                ?: continue
            out[itemId] = maxStack
        }
        out
    }
    private val armorEquipSoundKeyByItemKey: Map<String, String> by lazy {
        buildVanillaArmorEquipSoundMap()
    }
    private val creativeNoBreakItemIds: Set<Int> by lazy {
        val fromSwordTag = loadItemTag("swords")
            .mapNotNullTo(HashSet()) { itemIdByKey[it] }
        if (fromSwordTag.isNotEmpty()) return@lazy fromSwordTag

        // Fallback for environments where item tags are unavailable.
        val itemRegistry = RegistryCodec.allRegistries().firstOrNull { it.id == "minecraft:item" } ?: return@lazy emptySet()
        itemRegistry.entries.asSequence()
            .map { it.key }
            .filter { it.endsWith("_sword") }
            .mapNotNullTo(HashSet()) { key ->
                itemIdByKey[key] ?: itemIdByKey["minecraft:$key"]
            }
    }
    private val snowballItemId: Int by lazy { itemIdByKey["minecraft:snowball"] ?: -1 }
    private val eggItemId: Int by lazy { itemIdByKey["minecraft:egg"] ?: -1 }
    private val blueEggItemId: Int by lazy { itemIdByKey["minecraft:blue_egg"] ?: -1 }
    private val brownEggItemId: Int by lazy { itemIdByKey["minecraft:brown_egg"] ?: -1 }
    private val enderPearlItemId: Int by lazy { itemIdByKey["minecraft:ender_pearl"] ?: -1 }
    private val spyglassItemId: Int by lazy { itemIdByKey["minecraft:spyglass"] ?: -1 }
    private val chorusFruitItemId: Int by lazy { itemIdByKey["minecraft:chorus_fruit"] ?: -1 }
    private val shieldItemId: Int by lazy { itemIdByKey["minecraft:shield"] ?: -1 }
    private val pigSpawnEggItemId: Int by lazy { itemIdByKey["minecraft:pig_spawn_egg"] ?: -1 }
    private val saddleItemId: Int by lazy { itemIdByKey["minecraft:saddle"] ?: -1 }
    private val carrotOnAStickItemId: Int by lazy { itemIdByKey["minecraft:carrot_on_a_stick"] ?: -1 }
    private val pigFoodItemIds: Set<Int> by lazy {
        loadItemTag("pig_food")
            .mapNotNullTo(HashSet()) { itemIdByKey[it] }
    }
    private val waterBucketItemId: Int by lazy { itemIdByKey["minecraft:water_bucket"] ?: -1 }
    private val emptyBucketItemId: Int by lazy { itemIdByKey["minecraft:bucket"] ?: -1 }
    private val pigTemptItemIds: Set<Int> by lazy {
        val tagged = loadItemTag("pig_food")
            .mapNotNullTo(HashSet()) { itemIdByKey[it] }
        itemIdByKey["minecraft:carrot_on_a_stick"]?.let { tagged.add(it) }
        tagged
    }
    private val waterSourceStateId: Int by lazy {
        BlockStateRegistry.stateId("minecraft:water", mapOf("level" to "0"))
            ?: BlockStateRegistry.defaultStateId("minecraft:water")
            ?: 0
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

    fun prewarm() {
        // Shift lazy lookups to startup.
        itemKeyById.size
        itemIdByKey.size
        itemMaxStackSizeByItemId.size
        creativeNoBreakItemIds.size
        waterBucketItemId
        emptyBucketItemId
        snowballItemId
        eggItemId
        blueEggItemId
        brownEggItemId
        enderPearlItemId
        spyglassItemId
        chorusFruitItemId
        shieldItemId
        pigSpawnEggItemId
        pigTemptItemIds.size
        waterSourceStateId
        CraftingRecipeCache.prewarm(itemIdByKey)
        SmeltingRecipeCache.prewarm(itemIdByKey)
        FoodItemCache.prewarm(itemIdByKey)
        FurnaceFuelCache.prewarm(itemIdByKey)
        EntityLootDropCache.prewarm(itemIdByKey, AnimalKind.entries.map { it.entityTypeKey })
        WATER_BUCKET_REPLACEABLE_BLOCK_KEYS.size
        PLAYER_SMALL_FALL_SOUND_ID
        PLAYER_BIG_FALL_SOUND_ID
        PLAYER_HURT_SOUND_ID
        PLAYER_ATTACK_STRONG_SOUND_ID
        PLAYER_ATTACK_KNOCKBACK_SOUND_ID
        PLAYER_ATTACK_CRIT_SOUND_ID
        PLAYER_ATTACK_SWEEP_SOUND_ID
        PLAYER_ATTACK_WEAK_SOUND_ID
        SNOWBALL_THROW_SOUND_ID
        EGG_THROW_SOUND_ID
        FALL_DAMAGE_TYPE_ID
        PLAYER_ATTACK_DAMAGE_TYPE_ID
    }

    fun prewarmFluidDependentDropCache() {
        val world = runCatching { WorldManager.defaultWorld() }.getOrNull() ?: return
        for (stateId in BlockStateRegistry.allStateIds()) {
            if (stateId <= 0) continue
            val drops = runCatching {
                VanillaMiningRules.resolveDrops(world, stateId, -1, 0, 64, 0)
            }.getOrDefault(emptyList())
            fluidDependentDropCache.putIfAbsent(stateId, drops)
        }
    }

    fun prewarmFluidDependentRuntime() {
        val world = runCatching { WorldManager.defaultWorld() }.getOrNull() ?: return
        val seed = BlockPos(0, 64, 0)
        runCatching {
            breakDependentBlocksForFluidByChunk(world, listOf(seed))
        }
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
            displayName = null,
            worldKey = worldKey,
            entityId = allocateEntityId(),
            skinPartsMask = skinPartsMask,
            pingMs = 0,
            x = spawnX,
            y = spawnY,
            z = spawnZ,
            clientEyeX = spawnX,
            clientEyeY = spawnY,
            clientEyeZ = spawnZ,
            velocityX = 0.0,
            velocityY = 0.0,
            velocityZ = 0.0,
            accelerationX = 0.0,
            accelerationY = 0.0,
            accelerationZ = 0.0,
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
            foodExhaustion = 0f,
            regenAccumulatorSeconds = 0.0,
            usingItemHand = -1,
            usingItemId = -1,
            usingItemRemainingSeconds = 0.0,
            usingItemSoundKey = null,
            usingItemSoundDelaySeconds = 0.0,
            fallDistance = 0.0,
            attackStrengthTickerTicks = INITIAL_ATTACK_STRENGTH_TICKS,
            hurtInvulnerableSeconds = 0.0,
            lastHurtAmount = 0f,
            dead = false,
            sneaking = false,
            sprinting = false,
            swimming = false,
            inputForward = false,
            inputBackward = false,
            inputLeft = false,
            inputRight = false,
            inputJump = false,
            previousInputJump = false,
            airJumpAttemptCount = 0,
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
            openContainerId = 0,
            openContainerType = CONTAINER_TYPE_PLAYER_INVENTORY,
            openFurnaceX = 0,
            openFurnaceY = 0,
            openFurnaceZ = 0,
            nextContainerId = 1,
            cursorItemId = -1,
            cursorItemCount = 0,
            quickCraftActive = false,
            quickCraftContainerId = -1,
            quickCraftMouseButton = 0,
            quickCraftSlots = ConcurrentHashMap.newKeySet(),
            playerCraftItemIds = IntArray(4) { -1 },
            playerCraftItemCounts = IntArray(4) { 0 },
            tableCraftItemIds = IntArray(9) { -1 },
            tableCraftItemCounts = IntArray(9) { 0 },
            gameMode = ServerConfig.defaultGameMode.coerceIn(0, 3),
            flying = false,
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
            visibleThrownItemEntityIds = ConcurrentHashMap.newKeySet(),
            thrownItemTrackerStates = ConcurrentHashMap(),
            visibleAnimalEntityIds = ConcurrentHashMap.newKeySet(),
            animalTrackerStates = ConcurrentHashMap(),
            generatingChunks = ConcurrentHashMap.newKeySet(),
            chunkStreamVersion = AtomicInteger(0),
            nextTeleportId = AtomicInteger(2),
            chunkStreamInFlight = AtomicBoolean(false),
            pendingChunkTarget = AtomicReference(null),
            lastChunkMsptActionBarAtNanos = 0L,
            ridingAnimalEntityId = -1,
            lastClientVehicleX = spawnX,
            lastClientVehicleY = spawnY,
            lastClientVehicleZ = spawnZ,
            lastClientVehicleYaw = 0f,
            lastClientVehiclePitch = 0f,
            lastClientVehicleOnGround = true,
            lastClientVehiclePacketAtNanos = 0L,
            suppressNextMoveEvent = false,
            suppressMoveEchoX = spawnX,
            suppressMoveEchoY = spawnY,
            suppressMoveEchoZ = spawnZ,
            suppressMoveEchoYaw = 0f,
            suppressMoveEchoPitch = 0f
        )
        if (persistedOperatorUuids.contains(session.profile.uuid)) {
            operatorUuids.add(profile.uuid)
        }

        val existing = sessions.values.filter { it.worldKey == worldKey && it.channelId != session.channelId }
        sessions[session.channelId] = session
        contexts[session.channelId] = ctx
        animalSourceDirtyWorlds.add(session.worldKey)
        val spawnChunk = ChunkPos(spawnChunkX, spawnChunkZ)
        lastPushingChunkByPlayerEntityId[session.entityId] = spawnChunk
        markPushingChunksAwake(session.worldKey, spawnChunk)
        return JoinResult(session, existing)
    }

    fun finishJoin(join: JoinResult) {
        val session = join.session
        val existing = join.existing
        // Send already-connected players to newcomer.
        for (other in existing) {
            val ctx = contexts[session.channelId] ?: continue
            ctx.write(PlayPackets.playerInfoPacket(other.profile, effectiveDisplayName(other), other.gameMode, other.pingMs))
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
                    other.swimming,
                    other.usingItemHand
                )
            )
            sendHeldItemEquipment(other, ctx, flush = false)
        }
        contexts[session.channelId]?.flush()

        // Broadcast newcomer to already-connected players.
        for (other in existing) {
            val otherCtx = contexts[other.channelId] ?: continue
            if (!otherCtx.channel().isActive) continue
            otherCtx.write(PlayPackets.playerInfoPacket(session.profile, effectiveDisplayName(session), session.gameMode, session.pingMs))
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
                    session.swimming,
                    session.usingItemHand
                )
            )
            sendHeldItemEquipment(session, otherCtx, flush = false)
            otherCtx.flush()
        }

        val defaultJoinMessage = ServerI18n.translate(
            "multiplayer.player.joined",
            listOf(ServerI18n.componentToText(playerNameComponent(session)))
        )
        val joinMessage = org.macaroon3145.plugin.PluginSystem.onPlayerJoin(session, defaultJoinMessage)
        if (joinMessage == null) {
            broadcastSystemMessage(
                session.worldKey,
                PlayPackets.ChatComponent.Translate(
                    key = "multiplayer.player.joined",
                    args = listOf(playerNameComponent(session)),
                    color = "yellow"
                )
            )
        } else {
            broadcastSystemMessage(
                session.worldKey,
                PlayPackets.ChatComponent.Text(
                    text = joinMessage,
                    color = "yellow"
                )
            )
        }
        ServerI18n.log("aerogel.log.info.player.joined", session.profile.username)
    }

    fun byChannel(channelId: ChannelId): PlayerSession? = sessions[channelId]

    fun byName(name: String): PlayerSession? = sessions.values.firstOrNull { it.profile.username.equals(name, ignoreCase = true) }

    fun byUuid(uuid: UUID): PlayerSession? = sessions.values.firstOrNull { it.profile.uuid == uuid }

    fun playersInWorld(worldKey: String): List<PlayerSession> {
        return sessions.values.filter { it.worldKey == worldKey }
    }

    fun setPlayerHealth(uuid: UUID, health: Float): Boolean {
        val session = sessions.values.firstOrNull { it.profile.uuid == uuid } ?: return false
        session.health = health.coerceAtLeast(0f)
        sendHealthPacket(session)
        return true
    }

    fun setPlayerLocation(
        uuid: UUID,
        x: Double,
        y: Double,
        z: Double,
        yaw: Float? = null,
        pitch: Float? = null
    ): Boolean {
        if (!x.isFinite() || !y.isFinite() || !z.isFinite()) return false
        val session = sessions.values.firstOrNull { it.profile.uuid == uuid } ?: return false
        val resolvedYaw = yaw ?: session.yaw
        val resolvedPitch = pitch ?: session.pitch
        return setPlayerLocation(uuid, session.worldKey, x, y, z, resolvedYaw, resolvedPitch)
    }

    fun setPlayerLocation(
        uuid: UUID,
        worldKey: String,
        x: Double,
        y: Double,
        z: Double,
        yaw: Float? = null,
        pitch: Float? = null
    ): Boolean {
        if (!x.isFinite() || !y.isFinite() || !z.isFinite()) return false
        val session = sessions.values.firstOrNull { it.profile.uuid == uuid } ?: return false
        val resolvedYaw = yaw ?: session.yaw
        val resolvedPitch = pitch ?: session.pitch
        return if (session.worldKey == worldKey) {
            teleportPlayer(session, x, y, z, resolvedYaw, resolvedPitch, reason = MoveReason.API)
        } else {
            transferPlayerToWorld(session, worldKey, x, y, z, resolvedYaw, resolvedPitch)
        }
    }

    private fun transferPlayerToWorld(
        session: PlayerSession,
        targetWorldKey: String,
        x: Double,
        y: Double,
        z: Double,
        yaw: Float,
        pitch: Float
    ): Boolean {
        if (y < -64.0 || y > 320.0) return false
        val targetWorld = WorldManager.world(targetWorldKey) ?: return false
        val ctx = contexts[session.channelId] ?: return false
        if (!ctx.channel().isActive) return false
        val oldWorldKey = session.worldKey
        if (oldWorldKey == targetWorldKey) {
            return teleportPlayer(session, x, y, z, yaw, pitch, reason = MoveReason.WORLD_CHANGE)
        }

        dismountPlayerFromAnimal(session)
        val oldViewers = sessions.values.filter { it.worldKey == oldWorldKey && it.channelId != session.channelId }
        val newViewers = sessions.values.filter { it.worldKey == targetWorldKey && it.channelId != session.channelId }

        val removeEntityPacket = PlayPackets.removeEntitiesPacket(intArrayOf(session.entityId))
        for (viewer in oldViewers) {
            val viewerCtx = contexts[viewer.channelId] ?: continue
            if (!viewerCtx.channel().isActive) continue
            viewerCtx.write(removeEntityPacket)
            viewerCtx.flush()
        }

        session.worldKey = targetWorldKey
        val targetChunkX = ChunkStreamingService.chunkXFromBlockX(x)
        val targetChunkZ = ChunkStreamingService.chunkZFromBlockZ(z)
        session.centerChunkX = targetChunkX
        session.centerChunkZ = targetChunkZ
        lastPushingChunkByPlayerEntityId[session.entityId] = ChunkPos(targetChunkX, targetChunkZ)
        markPushingChunksAwake(targetWorldKey, ChunkPos(targetChunkX, targetChunkZ))
        animalSourceDirtyWorlds.add(oldWorldKey)
        animalSourceDirtyWorlds.add(targetWorldKey)

        ctx.executor().execute {
            if (!ctx.channel().isActive) return@execute
            ctx.write(
                PlayPackets.respawnPacket(
                    worldKey = targetWorldKey,
                    gameMode = session.gameMode,
                    previousGameMode = session.gameMode,
                    seaLevel = 63,
                    copyDataFlags = 0
                )
            )
            ctx.write(PlayPackets.gameStateStartLoadingPacket())
            resetRespawningSessionChunkView(session, ctx)
            requestChunkStream(session, ctx, targetWorld, targetChunkX, targetChunkZ, session.chunkRadius)

            for (other in newViewers) {
                ctx.write(PlayPackets.playerInfoPacket(other.profile, effectiveDisplayName(other), other.gameMode, other.pingMs))
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
                        other.swimming,
                        other.usingItemHand
                    )
                )
                sendHeldItemEquipment(other, ctx, flush = false)
            }
            ctx.write(PlayPackets.spawnPositionPacket(targetWorldKey, x, y, z))
            ctx.flush()
        }

        for (other in newViewers) {
            val otherCtx = contexts[other.channelId] ?: continue
            if (!otherCtx.channel().isActive) continue
            otherCtx.write(PlayPackets.playerInfoPacket(session.profile, effectiveDisplayName(session), session.gameMode, session.pingMs))
            otherCtx.write(
                PlayPackets.addPlayerEntityPacket(
                    entityId = session.entityId,
                    uuid = session.profile.uuid,
                    x = x,
                    y = y,
                    z = z,
                    yaw = yaw,
                    pitch = pitch
                )
            )
            otherCtx.write(PlayPackets.playerSkinPartsMetadataPacket(session.entityId, session.skinPartsMask))
            otherCtx.write(
                PlayPackets.playerSharedFlagsMetadataPacket(
                    session.entityId,
                    session.sneaking,
                    session.sprinting,
                    session.swimming,
                    session.usingItemHand
                )
            )
            sendHeldItemEquipment(session, otherCtx, flush = false)
            otherCtx.flush()
        }

        return teleportPlayer(session, x, y, z, yaw, pitch, reason = MoveReason.WORLD_CHANGE)
    }

    fun acceleratePlayer(uuid: UUID, x: Double, y: Double, z: Double): Boolean {
        if (!x.isFinite() || !y.isFinite() || !z.isFinite()) return false
        val session = sessions.values.firstOrNull { it.profile.uuid == uuid } ?: return false
        val tickSpeedX = x / MINECRAFT_TICKS_PER_SECOND
        val tickSpeedY = y / MINECRAFT_TICKS_PER_SECOND
        val tickSpeedZ = z / MINECRAFT_TICKS_PER_SECOND
        session.velocityX = (session.velocityX + tickSpeedX).coerceIn(-ENTITY_PUSH_MAX_PLAYER_VELOCITY, ENTITY_PUSH_MAX_PLAYER_VELOCITY)
        session.velocityY += tickSpeedY
        session.velocityZ = (session.velocityZ + tickSpeedZ).coerceIn(-ENTITY_PUSH_MAX_PLAYER_VELOCITY, ENTITY_PUSH_MAX_PLAYER_VELOCITY)
        broadcastEntityVelocity(session, session.velocityX, session.velocityY, session.velocityZ)
        return true
    }

    fun playerSpeedMetersPerSecond(uuid: UUID): Triple<Double, Double, Double>? {
        val session = sessions.values.firstOrNull { it.profile.uuid == uuid } ?: return null
        return Triple(
            session.velocityX * MINECRAFT_TICKS_PER_SECOND,
            session.velocityY * MINECRAFT_TICKS_PER_SECOND,
            session.velocityZ * MINECRAFT_TICKS_PER_SECOND
        )
    }

    fun setPlayerSpeedMetersPerSecond(uuid: UUID, x: Double, y: Double, z: Double): Boolean {
        if (!x.isFinite() || !y.isFinite() || !z.isFinite()) return false
        val session = sessions.values.firstOrNull { it.profile.uuid == uuid } ?: return false
        session.velocityX = (x / MINECRAFT_TICKS_PER_SECOND).coerceIn(-ENTITY_PUSH_MAX_PLAYER_VELOCITY, ENTITY_PUSH_MAX_PLAYER_VELOCITY)
        session.velocityY = (y / MINECRAFT_TICKS_PER_SECOND)
        session.velocityZ = (z / MINECRAFT_TICKS_PER_SECOND).coerceIn(-ENTITY_PUSH_MAX_PLAYER_VELOCITY, ENTITY_PUSH_MAX_PLAYER_VELOCITY)
        broadcastEntityVelocity(session, session.velocityX, session.velocityY, session.velocityZ)
        return true
    }

    fun playerAccelerationMetersPerSecondSquared(uuid: UUID): Triple<Double, Double, Double>? {
        val session = sessions.values.firstOrNull { it.profile.uuid == uuid } ?: return null
        return Triple(session.accelerationX, session.accelerationY, session.accelerationZ)
    }

    fun setPlayerAccelerationMetersPerSecondSquared(uuid: UUID, x: Double, y: Double, z: Double): Boolean {
        if (!x.isFinite() || !y.isFinite() || !z.isFinite()) return false
        val session = sessions.values.firstOrNull { it.profile.uuid == uuid } ?: return false
        session.accelerationX = x
        session.accelerationY = y
        session.accelerationZ = z
        return true
    }

    fun setDisplayName(channelId: ChannelId, displayName: String?): Boolean {
        val session = sessions[channelId] ?: return false
        val nextDisplayName = displayName?.trim()?.ifEmpty { null }
        if (session.displayName == nextDisplayName) return true
        session.displayName = nextDisplayName
        rebroadcastPlayerDisplayName(session)
        return true
    }

    fun setDisplayName(uuid: UUID, displayName: String?): Boolean {
        val session = sessions.values.firstOrNull { it.profile.uuid == uuid } ?: return false
        return setDisplayName(session.channelId, displayName)
    }

    fun setHotbarItem(
        uuid: UUID,
        slot: Int,
        itemId: Int,
        amount: Int,
        name: String? = null,
        lore: List<String> = emptyList()
    ): Boolean {
        val session = sessions.values.firstOrNull { it.profile.uuid == uuid } ?: return false
        return setHotbarItem(session.channelId, slot, itemId, amount, name, lore)
    }

    fun setHotbarItem(
        channelId: ChannelId,
        slot: Int,
        itemId: Int,
        amount: Int,
        name: String? = null,
        lore: List<String> = emptyList()
    ): Boolean {
        val session = sessions[channelId] ?: return false
        if (slot !in 0..8) return false
        val (normalizedId, normalizedCount) = normalizePluginInventoryStack(itemId, amount)
        setHotbarSlotWithMeta(session, slot, normalizedId, normalizedCount, inheritedMeta = null)
        setHotbarTextMeta(
            session = session,
            hotbarSlot = slot,
            itemId = normalizedId,
            name = name,
            lore = lore
        )
        session.pendingPickedBlockStateId = 0
        updateSelectedBlockStateAndEquipmentAfterHotbarMutation(session, changedHotbarA = slot, changedHotbarB = null)
        animalSourceDirtyWorlds.add(session.worldKey)
        resyncPlayerInventoryViewsForSession(session)
        return true
    }

    fun setMainInventoryItem(
        uuid: UUID,
        index: Int,
        itemId: Int,
        amount: Int,
        name: String? = null,
        lore: List<String> = emptyList()
    ): Boolean {
        val session = sessions.values.firstOrNull { it.profile.uuid == uuid } ?: return false
        return setMainInventoryItem(session.channelId, index, itemId, amount, name, lore)
    }

    fun setMainInventoryItem(
        channelId: ChannelId,
        index: Int,
        itemId: Int,
        amount: Int,
        name: String? = null,
        lore: List<String> = emptyList()
    ): Boolean {
        val session = sessions[channelId] ?: return false
        if (index !in 0..26) return false
        val (normalizedId, normalizedCount) = normalizePluginInventoryStack(itemId, amount)
        session.mainInventoryItemIds[index] = normalizedId
        session.mainInventoryItemCounts[index] = normalizedCount
        setMainTextMeta(
            session = session,
            mainIndex = index,
            itemId = normalizedId,
            name = name,
            lore = lore
        )
        session.pendingPickedBlockStateId = 0
        resyncPlayerInventoryViewsForSession(session)
        return true
    }

    fun setArmorItem(
        uuid: UUID,
        slot: ArmorSlot,
        itemId: Int,
        amount: Int,
        name: String? = null,
        lore: List<String> = emptyList()
    ): Boolean {
        val session = sessions.values.firstOrNull { it.profile.uuid == uuid } ?: return false
        return setArmorItem(session.channelId, slot, itemId, amount, name, lore)
    }

    fun setArmorItem(
        channelId: ChannelId,
        slot: ArmorSlot,
        itemId: Int,
        amount: Int,
        name: String? = null,
        lore: List<String> = emptyList()
    ): Boolean {
        val session = sessions[channelId] ?: return false
        val armorIndex = when (slot) {
            ArmorSlot.HELMET -> 3
            ArmorSlot.CHESTPLATE -> 2
            ArmorSlot.LEGGINGS -> 1
            ArmorSlot.BOOTS -> 0
        }
        val previousItemId = session.armorItemIds[armorIndex]
        val previousCount = session.armorItemCounts[armorIndex]
        val (normalizedId, normalizedCount) = normalizePluginInventoryStack(itemId, amount)
        session.armorItemIds[armorIndex] = normalizedId
        session.armorItemCounts[armorIndex] = normalizedCount
        setArmorTextMeta(
            session = session,
            armorIndex = armorIndex,
            itemId = normalizedId,
            name = name,
            lore = lore
        )
        session.pendingPickedBlockStateId = 0
        broadcastHeldItemEquipment(session)
        maybeBroadcastArmorSlotChangeSound(
            session = session,
            previousItemId = previousItemId,
            previousCount = previousCount,
            nextItemId = normalizedId,
            nextCount = normalizedCount
        )
        resyncPlayerInventoryViewsForSession(session)
        return true
    }

    fun setOffhandItem(
        uuid: UUID,
        itemId: Int,
        amount: Int,
        name: String? = null,
        lore: List<String> = emptyList()
    ): Boolean {
        val session = sessions.values.firstOrNull { it.profile.uuid == uuid } ?: return false
        return setOffhandItem(session.channelId, itemId, amount, name, lore)
    }

    fun setOffhandItem(
        channelId: ChannelId,
        itemId: Int,
        amount: Int,
        name: String? = null,
        lore: List<String> = emptyList()
    ): Boolean {
        val session = sessions[channelId] ?: return false
        val previousItemId = session.offhandItemId
        val previousCount = session.offhandItemCount
        val (normalizedId, normalizedCount) = normalizePluginInventoryStack(itemId, amount)
        session.offhandItemId = normalizedId
        session.offhandItemCount = normalizedCount
        setOffhandTextMeta(
            session = session,
            itemId = normalizedId,
            name = name,
            lore = lore
        )
        session.pendingPickedBlockStateId = 0
        broadcastHeldItemEquipment(session)
        maybeBroadcastOffhandSlotChangeSound(
            session = session,
            previousItemId = previousItemId,
            previousCount = previousCount,
            nextItemId = normalizedId,
            nextCount = normalizedCount
        )
        animalSourceDirtyWorlds.add(session.worldKey)
        resyncPlayerInventoryViewsForSession(session)
        return true
    }

    fun displayNameOrUsername(session: PlayerSession): String = effectiveDisplayName(session)

    fun displayNameOrUsername(uuid: UUID): String? {
        val session = sessions.values.firstOrNull { it.profile.uuid == uuid } ?: return null
        return effectiveDisplayName(session)
    }

    private fun rebroadcastPlayerDisplayName(session: PlayerSession) {
        val removeInfoPacket = PlayPackets.playerInfoRemovePacket(listOf(session.profile.uuid))
        val addInfoPacket = PlayPackets.playerInfoPacket(
            profile = session.profile,
            displayName = effectiveDisplayName(session),
            gameMode = session.gameMode,
            latencyMs = session.pingMs
        )
        for ((id, other) in sessions) {
            if (other.worldKey != session.worldKey) continue
            val ctx = contexts[id] ?: continue
            if (!ctx.channel().isActive) continue
            ctx.write(removeInfoPacket)
            ctx.write(addInfoPacket)
            ctx.flush()
        }
    }

    fun onlineCount(): Int = sessions.size

    private fun normalizePluginInventoryStack(itemId: Int, amount: Int): Pair<Int, Int> {
        if (itemId < 0 || amount <= 0) return -1 to 0
        return itemId to amount.coerceAtMost(itemMaxStackSize(itemId))
    }

    private fun itemMaxStackSize(itemId: Int): Int {
        if (itemId < 0 || itemId >= itemMaxStackSizeByItemId.size) return MAX_HOTBAR_STACK_SIZE
        return itemMaxStackSizeByItemId[itemId].coerceIn(1, MAX_HOTBAR_STACK_SIZE)
    }

    private fun setHotbarTextMeta(session: PlayerSession, hotbarSlot: Int, itemId: Int, name: String?, lore: List<String>) {
        val meta = itemTextMetaByPlayerUuid.computeIfAbsent(session.profile.uuid) { PlayerItemTextMeta() }
        meta.hotbar[hotbarSlot] = buildItemTextMeta(itemId, name, lore)
    }

    private fun setMainTextMeta(session: PlayerSession, mainIndex: Int, itemId: Int, name: String?, lore: List<String>) {
        val meta = itemTextMetaByPlayerUuid.computeIfAbsent(session.profile.uuid) { PlayerItemTextMeta() }
        meta.main[mainIndex] = buildItemTextMeta(itemId, name, lore)
    }

    private fun setArmorTextMeta(session: PlayerSession, armorIndex: Int, itemId: Int, name: String?, lore: List<String>) {
        val meta = itemTextMetaByPlayerUuid.computeIfAbsent(session.profile.uuid) { PlayerItemTextMeta() }
        meta.armor[armorIndex] = buildItemTextMeta(itemId, name, lore)
    }

    private fun setOffhandTextMeta(session: PlayerSession, itemId: Int, name: String?, lore: List<String>) {
        val meta = itemTextMetaByPlayerUuid.computeIfAbsent(session.profile.uuid) { PlayerItemTextMeta() }
        meta.offhand = buildItemTextMeta(itemId, name, lore)
    }

    private fun buildItemTextMeta(itemId: Int, name: String?, lore: List<String>): ItemTextMeta? {
        if (itemId < 0) return null
        val normalizedName = name?.trim()?.takeIf { it.isNotEmpty() }
        val normalizedLore = lore.asSequence()
            .map { it.trim() }
            .filter { it.isNotEmpty() }
            .toList()
        if (normalizedName == null && normalizedLore.isEmpty()) return null
        return ItemTextMeta(expectedItemId = itemId, name = normalizedName, lore = normalizedLore)
    }

    private fun resyncPlayerInventoryViewsForSession(session: PlayerSession) {
        resyncContainerForSession(session, PLAYER_INVENTORY_CONTAINER_ID)
        if (session.openContainerId != PLAYER_INVENTORY_CONTAINER_ID) {
            resyncContainerForSession(session, session.openContainerId)
        }
    }

    fun dashboardPlayerSnapshots(): List<DashboardPlayerSnapshot> {
        if (sessions.isEmpty()) return emptyList()
        return sessions.values.map { session ->
            DashboardPlayerSnapshot(
                worldKey = session.worldKey,
                uuid = session.profile.uuid,
                username = effectiveDisplayName(session),
                x = session.x,
                z = session.z,
                skinPartsMask = session.skinPartsMask,
                texturesPropertyValue = session.profile.properties.firstOrNull { it.name.equals("textures", ignoreCase = true) }?.value
            )
        }
    }

    fun activeSimulationChunksByWorldSnapshot(): Map<String, Set<ChunkPos>> {
        if (activeSimulationChunksCacheByWorld.isEmpty()) return emptyMap()
        val out = HashMap<String, Set<ChunkPos>>(activeSimulationChunksCacheByWorld.size)
        for ((worldKey, chunks) in activeSimulationChunksCacheByWorld) {
            if (chunks.isEmpty()) continue
            out[worldKey] = HashSet(chunks)
        }
        return out
    }

    fun consumeActiveSimulationChunkDeltas(): Map<String, SimulationChunkDelta> {
        synchronized(activeSimulationDeltaLock) {
            if (activeSimulationDeltaByWorld.isEmpty()) return emptyMap()
            val out = HashMap<String, SimulationChunkDelta>(activeSimulationDeltaByWorld.size)
            for ((worldKey, delta) in activeSimulationDeltaByWorld) {
                out[worldKey] = SimulationChunkDelta(
                    added = HashSet(delta.added),
                    removed = HashSet(delta.removed)
                )
            }
            activeSimulationDeltaByWorld.clear()
            return out
        }
    }

    fun statusPlayerSamples(limit: Int = 12): List<PlayerSample> {
        if (limit <= 0) return emptyList()
        return sessions.values
            .asSequence()
            .map { PlayerSample(name = effectiveDisplayName(it), id = it.profile.uuid.toString()) }
            .sortedBy { it.name.lowercase() }
            .take(limit)
            .toList()
    }

    fun tick(deltaSeconds: Double) {
        if (deltaSeconds <= 0.0) return
        val tickSequence = chunkProcessingTickSequence.incrementAndGet()
        org.macaroon3145.plugin.PluginSystem.onGameTick(tickSequence)
        val gameplayDeltaSeconds = computePhysicsDeltaSeconds(deltaSeconds)
        tickPerPlayerState(gameplayDeltaSeconds, deltaSeconds)
        advanceAndBroadcastWorldTime(deltaSeconds)
        val simulationContext = buildTickSimulationContext()
        val physicsDeltaSeconds = gameplayDeltaSeconds
        if (simulationContext != null && physicsDeltaSeconds > 0.0) {
            updateRetainedBaseWorldChunks(simulationContext.sessionsByWorld)
            scheduleDroppedItemEvents(
                tickSequence = tickSequence,
                deltaSeconds = deltaSeconds,
                physicsDeltaSeconds = physicsDeltaSeconds,
                context = simulationContext
            )
            scheduleFluidEvents(
                tickSequence = tickSequence,
                physicsDeltaSeconds = physicsDeltaSeconds,
                context = simulationContext
            )
            tickFurnaces(tickSequence, physicsDeltaSeconds, simulationContext)
            scheduleEntityPushing(tickSequence, simulationContext, physicsDeltaSeconds)
        }
        flushPendingAnimalRemovals()
        drainPendingFurnaceResyncs()
        maybeSendChunkMsptActionBar(System.nanoTime())
    }

    private fun tickPerPlayerState(gameplayDeltaSeconds: Double, deltaSeconds: Double) {
        if (sessions.isEmpty()) return
        val hasGameplayDelta = gameplayDeltaSeconds.isFinite() && gameplayDeltaSeconds > 0.0
        val hasDelta = deltaSeconds.isFinite() && deltaSeconds > 0.0
        if (!hasGameplayDelta && !hasDelta) return
        val tickDelta = if (hasGameplayDelta) {
            (gameplayDeltaSeconds * MINECRAFT_TICKS_PER_SECOND).takeIf { it.isFinite() && it > 0.0 } ?: 0.0
        } else {
            0.0
        }
        if (tickDelta > 0.0) {
            tickAttackStrengthCooldownsIncremental(tickDelta)
        }

        for (session in sessions.values) {
            if (hasGameplayDelta) {
                if (session.accelerationX != 0.0 || session.accelerationY != 0.0 || session.accelerationZ != 0.0) {
                    val nextSpeedXMps = (session.velocityX * MINECRAFT_TICKS_PER_SECOND) + (session.accelerationX * gameplayDeltaSeconds)
                    val nextSpeedYMps = (session.velocityY * MINECRAFT_TICKS_PER_SECOND) + (session.accelerationY * gameplayDeltaSeconds)
                    val nextSpeedZMps = (session.velocityZ * MINECRAFT_TICKS_PER_SECOND) + (session.accelerationZ * gameplayDeltaSeconds)
                    session.velocityX = (nextSpeedXMps / MINECRAFT_TICKS_PER_SECOND).coerceIn(-ENTITY_PUSH_MAX_PLAYER_VELOCITY, ENTITY_PUSH_MAX_PLAYER_VELOCITY)
                    session.velocityY = (nextSpeedYMps / MINECRAFT_TICKS_PER_SECOND)
                    session.velocityZ = (nextSpeedZMps / MINECRAFT_TICKS_PER_SECOND).coerceIn(-ENTITY_PUSH_MAX_PLAYER_VELOCITY, ENTITY_PUSH_MAX_PLAYER_VELOCITY)
                    broadcastEntityVelocity(session, session.velocityX, session.velocityY, session.velocityZ)
                }
                if (session.hurtInvulnerableSeconds > 0.0) {
                    session.hurtInvulnerableSeconds =
                        (session.hurtInvulnerableSeconds - gameplayDeltaSeconds).coerceAtLeast(0.0)
                    if (session.hurtInvulnerableSeconds <= 0.0) {
                        session.lastHurtAmount = 0f
                    }
                }
                tickUsingItemForSession(session, gameplayDeltaSeconds)
            }

            if (hasDelta) {
                tickNaturalRegenerationForSession(session, deltaSeconds)
            }
        }
    }

    private fun tickAttackStrengthCooldownsIncremental(tickDelta: Double) {
        if (attackCooldownActiveChannelIds.isEmpty()) return
        val toRemove = ArrayList<ChannelId>()
        for (channelId in attackCooldownActiveChannelIds) {
            val session = sessions[channelId]
            if (session == null) {
                toRemove.add(channelId)
                continue
            }
            val current = session.attackStrengthTickerTicks
            if (current >= MAX_ATTACK_STRENGTH_TICKER) {
                toRemove.add(channelId)
                continue
            }
            val next = (current + tickDelta).coerceAtMost(MAX_ATTACK_STRENGTH_TICKER)
            session.attackStrengthTickerTicks = next
            if (next >= MAX_ATTACK_STRENGTH_TICKER) {
                toRemove.add(channelId)
            }
        }
        for (channelId in toRemove) {
            attackCooldownActiveChannelIds.remove(channelId)
        }
    }

    private fun tickAttackStrengthCooldowns(deltaSeconds: Double) {
        if (!deltaSeconds.isFinite() || deltaSeconds <= 0.0) return
        val tickDelta = deltaSeconds * MINECRAFT_TICKS_PER_SECOND
        if (!tickDelta.isFinite() || tickDelta <= 0.0) return
        for (session in sessions.values) {
            val next = (session.attackStrengthTickerTicks + tickDelta).coerceAtMost(MAX_ATTACK_STRENGTH_TICKER)
            session.attackStrengthTickerTicks = next
        }
    }

    private fun tickPlayerHurtInvulnerability(deltaSeconds: Double) {
        if (!deltaSeconds.isFinite() || deltaSeconds <= 0.0) return
        for (session in sessions.values) {
            if (session.hurtInvulnerableSeconds <= 0.0) continue
            session.hurtInvulnerableSeconds = (session.hurtInvulnerableSeconds - deltaSeconds).coerceAtLeast(0.0)
            if (session.hurtInvulnerableSeconds <= 0.0) {
                session.lastHurtAmount = 0f
            }
        }
    }

    private fun computePhysicsDeltaSeconds(deltaSeconds: Double): Double {
        val timeScale = ServerConfig.timeScale
        if (timeScale <= 0.0) return 0.0
        val scaled = deltaSeconds * timeScale
        if (scaled.isNaN() || scaled.isInfinite()) return 0.0
        return scaled.coerceAtLeast(0.0)
    }

    private fun tickUsingItems(deltaSeconds: Double) {
        if (!deltaSeconds.isFinite() || deltaSeconds <= 0.0) return
        for (session in sessions.values) {
            tickUsingItemForSession(session, deltaSeconds)
        }
    }

    private fun tickUsingItemForSession(session: PlayerSession, deltaSeconds: Double) {
        if (session.usingItemRemainingSeconds <= 0.0) return
        if (session.dead || session.gameMode == GAME_MODE_SPECTATOR) {
            stopUsingItem(session)
            return
        }
        session.usingItemSoundDelaySeconds -= deltaSeconds
        var soundLoops = 0
        while (session.usingItemSoundDelaySeconds <= 0.0 &&
            session.usingItemRemainingSeconds > 0.0 &&
            soundLoops < MAX_ITEM_USE_SOUND_LOOPS_PER_TICK
        ) {
            val soundKey = session.usingItemSoundKey
            if (!soundKey.isNullOrEmpty()) {
                broadcastFoodUseProgressSound(session, soundKey)
            }
            session.usingItemSoundDelaySeconds += FOOD_USE_SOUND_INTERVAL_SECONDS
            soundLoops++
        }
        session.usingItemRemainingSeconds -= deltaSeconds
        if (session.usingItemRemainingSeconds > 0.0) return

        val hand = session.usingItemHand
        val expectedItemId = session.usingItemId
        stopUsingItem(session)
        if (hand !in 0..1 || expectedItemId < 0) return
        finishConsumingFoodItem(session, hand, expectedItemId)
    }

    private fun tickFurnaces(tickSequence: Long, deltaSeconds: Double, context: TickSimulationContext?) {
        if (!deltaSeconds.isFinite() || deltaSeconds <= 0.0) return
        val simulation = context ?: return
        val activeByWorld = HashMap<String, MutableList<FurnaceKey>>()
        for (key in activeFurnaceKeys) {
            activeByWorld.computeIfAbsent(key.worldKey) { ArrayList() }.add(key)
        }
        if (activeByWorld.isEmpty()) {
            furnaceChunkLastTickNanos.clear()
            return
        }
        pruneInactiveWorldChunkTimingState(activeByWorld.keys, furnaceChunkLastTickNanos)
        for ((worldKey, worldActiveKeys) in activeByWorld) {
            val activeChunks = simulation.activeChunksByWorld[worldKey] ?: continue
            if (activeChunks.isEmpty()) {
                pruneIdleWorldChunkTimingState(worldKey, emptySet(), furnaceChunkLastTickNanos)
                continue
            }
            val world = WorldManager.world(worldKey) ?: continue
            val byChunk = HashMap<ChunkPos, MutableList<BlockPos>>()
            for (key in worldActiveKeys) {
                val chunkPos = ChunkPos(
                    ChunkStreamingService.chunkXFromBlockX(key.x.toDouble()),
                    ChunkStreamingService.chunkZFromBlockZ(key.z.toDouble())
                )
                if (chunkPos !in activeChunks) continue
                byChunk.computeIfAbsent(chunkPos) { ArrayList() }.add(BlockPos(key.x, key.y, key.z))
            }
            if (byChunk.isEmpty()) {
                pruneIdleWorldChunkTimingState(worldKey, emptySet(), furnaceChunkLastTickNanos)
                continue
            }
            pruneIdleWorldChunkTimingState(worldKey, byChunk.keys, furnaceChunkLastTickNanos)
            for ((chunkPos, positions) in byChunk) {
                val positionSnapshot = positions.toList()
                val worldChunkKey = WorldChunkKey(worldKey, chunkPos)
                if (!tryAcquireWorldChunkInFlight(worldChunkKey, furnaceChunkInFlight)) {
                    continue
                }
                world.submitOnChunkActor(chunkPos) {
                    try {
                        val frame = world.beginChunkProcessingFrame(tickSequence)
                        val startedAt = System.nanoTime()
                        val chunkDeltaSeconds = consumeChunkElapsedDeltaSeconds(
                            worldChunkKey = worldChunkKey,
                            fallbackSeconds = deltaSeconds,
                            lastTickMap = furnaceChunkLastTickNanos
                        )
                        if (chunkDeltaSeconds <= 0.0) {
                            world.recordChunkProcessingNanos(
                                frame = frame,
                                chunkPos = chunkPos,
                                elapsedNanos = System.nanoTime() - startedAt,
                                category = "furnace"
                            )
                            world.finishChunkProcessingFrame(
                                frame = frame,
                                activeChunks = setOf(chunkPos),
                                includeZeroForInactive = false,
                                accumulateIntoLast = true
                            )
                            return@submitOnChunkActor
                        }
                        for (pos in positionSnapshot) {
                            val key = FurnaceKey(worldKey, pos.x, pos.y, pos.z)
                            val furnace = furnaceStates[key]
                            if (furnace == null) {
                                activeFurnaceKeys.remove(key)
                                continue
                            }
                            val wasBurning = furnace.burnRemainingSeconds > 0.0
                            if (tickSingleFurnaceState(furnace, chunkDeltaSeconds)) {
                                enqueueFurnaceResync(key)
                            }
                            val isBurning = furnace.burnRemainingSeconds > 0.0
                            if (wasBurning != isBurning) {
                                syncFurnaceLitBlockState(world, key, isBurning)
                            }
                            refreshFurnaceActivity(key, furnace)
                        }
                        world.recordChunkProcessingNanos(
                            frame = frame,
                            chunkPos = chunkPos,
                            elapsedNanos = System.nanoTime() - startedAt,
                            category = "furnace"
                        )
                        world.finishChunkProcessingFrame(
                            frame = frame,
                            activeChunks = setOf(chunkPos),
                            includeZeroForInactive = false,
                            accumulateIntoLast = true
                        )
                    } finally {
                        releaseWorldChunkInFlight(worldChunkKey, furnaceChunkInFlight)
                    }
                }
            }
        }
    }

    private fun consumeChunkElapsedDeltaSeconds(
        worldChunkKey: WorldChunkKey,
        fallbackSeconds: Double,
        lastTickMap: ConcurrentHashMap<WorldChunkKey, Long>
    ): Double {
        val nowNanos = System.nanoTime()
        val previous = lastTickMap.put(worldChunkKey, nowNanos)
        if (previous == null) {
            return if (fallbackSeconds.isFinite() && fallbackSeconds > 0.0) fallbackSeconds else 0.0
        }
        val elapsedSeconds = ((nowNanos - previous).coerceAtLeast(0L)) / 1_000_000_000.0
        if (!elapsedSeconds.isFinite() || elapsedSeconds <= 0.0) return 0.0
        return elapsedSeconds
    }

    private fun tryAcquireWorldChunkInFlight(
        key: WorldChunkKey,
        inFlight: ConcurrentHashMap<WorldChunkKey, AtomicBoolean>
    ): Boolean {
        return inFlight.computeIfAbsent(key) { AtomicBoolean(false) }.compareAndSet(false, true)
    }

    private fun releaseWorldChunkInFlight(
        key: WorldChunkKey,
        inFlight: ConcurrentHashMap<WorldChunkKey, AtomicBoolean>
    ) {
        inFlight[key]?.set(false)
    }

    private fun pruneIdleWorldChunkTimingState(
        worldKey: String,
        activeChunks: Collection<ChunkPos>,
        lastTickMap: ConcurrentHashMap<WorldChunkKey, Long>
    ) {
        if (lastTickMap.isEmpty()) return
        val active = if (activeChunks is Set<ChunkPos>) activeChunks else HashSet(activeChunks)
        val iterator = lastTickMap.entries.iterator()
        while (iterator.hasNext()) {
            val entry = iterator.next()
            if (entry.key.worldKey != worldKey) continue
            if (entry.key.chunkPos !in active) {
                iterator.remove()
            }
        }
    }

    private fun pruneInactiveWorldChunkTimingState(
        activeWorldKeys: Set<String>,
        lastTickMap: ConcurrentHashMap<WorldChunkKey, Long>
    ) {
        if (lastTickMap.isEmpty()) return
        if (activeWorldKeys.isEmpty()) {
            lastTickMap.clear()
            return
        }
        val iterator = lastTickMap.entries.iterator()
        while (iterator.hasNext()) {
            val entry = iterator.next()
            if (entry.key.worldKey !in activeWorldKeys) {
                iterator.remove()
            }
        }
    }

    private fun enqueueFurnaceResync(key: FurnaceKey) {
        if (!pendingFurnaceResyncSet.add(key)) return
        pendingFurnaceResyncKeys.add(key)
    }

    private fun drainPendingFurnaceResyncs() {
        while (true) {
            val key = pendingFurnaceResyncKeys.poll() ?: break
            pendingFurnaceResyncSet.remove(key)
            resyncOpenFurnaceViewers(key)
        }
    }

    private fun tickSingleFurnaceState(furnace: FurnaceState, deltaSeconds: Double): Boolean {
        run {
            val recipe = currentSmeltingRecipe(furnace)
            val canSmelt = canSmeltNow(furnace, recipe)
            var dirty = false

            if (furnace.burnRemainingSeconds > 0.0) {
                furnace.burnRemainingSeconds = (furnace.burnRemainingSeconds - deltaSeconds).coerceAtLeast(0.0)
                dirty = true
            }

            if (canSmelt && furnace.burnRemainingSeconds <= 0.0) {
                val fuelBurnSeconds = fuelBurnSeconds(furnace.fuelItemId)
                if (fuelBurnSeconds > 0.0 && furnace.fuelCount > 0) {
                    furnace.fuelCount -= 1
                    if (furnace.fuelCount <= 0) {
                        furnace.fuelItemId = -1
                        furnace.fuelCount = 0
                    }
                    furnace.burnRemainingSeconds = fuelBurnSeconds
                    furnace.burnTotalSeconds = fuelBurnSeconds
                    dirty = true
                }
            }

            if (canSmelt && furnace.burnRemainingSeconds > 0.0 && recipe != null) {
                if (furnace.cookTotalSeconds <= 0.0) {
                    furnace.cookTotalSeconds = recipe.cookSeconds.coerceAtLeast(MIN_FURNACE_COOK_SECONDS)
                }
                furnace.cookProgressSeconds += deltaSeconds
                dirty = true
                while (furnace.cookProgressSeconds + 1.0e-9 >= furnace.cookTotalSeconds) {
                    if (!canSmeltNow(furnace, recipe)) {
                        furnace.cookProgressSeconds = 0.0
                        break
                    }
                    smeltOneItem(furnace, recipe)
                    furnace.cookProgressSeconds -= furnace.cookTotalSeconds
                    dirty = true
                }
            } else if (furnace.cookProgressSeconds > 0.0) {
                furnace.cookProgressSeconds = 0.0
                dirty = true
            }

            return dirty
        }
    }

    private fun refreshFurnaceActivity(key: FurnaceKey, furnace: FurnaceState) {
        val active = run {
            shouldTickFurnaceLocked(furnace)
        }
        if (active) {
            activeFurnaceKeys.add(key)
            activeFurnaceKeysByWorld.computeIfAbsent(key.worldKey) { ConcurrentHashMap.newKeySet() }.add(key)
        } else {
            activeFurnaceKeys.remove(key)
            activeFurnaceKeysByWorld[key.worldKey]?.let { set ->
                set.remove(key)
                if (set.isEmpty()) {
                    activeFurnaceKeysByWorld.remove(key.worldKey, set)
                }
            }
        }
    }

    private fun shouldTickFurnaceLocked(furnace: FurnaceState): Boolean {
        if (furnace.burnRemainingSeconds > 0.0) return true
        val recipe = currentSmeltingRecipe(furnace) ?: return false
        if (!canSmeltNow(furnace, recipe)) return false
        return furnace.fuelCount > 0 && fuelBurnSeconds(furnace.fuelItemId) > 0.0
    }

    private fun syncFurnaceLitBlockState(
        world: org.macaroon3145.world.World,
        key: FurnaceKey,
        lit: Boolean
    ) {
        val currentStateId = world.blockStateAt(key.x, key.y, key.z)
        if (currentStateId <= 0) return
        val parsed = BlockStateRegistry.parsedState(currentStateId) ?: return
        if (parsed.blockKey != "minecraft:furnace") return
        val currentLit = parsed.properties["lit"] ?: return
        val targetLit = if (lit) "true" else "false"
        if (currentLit == targetLit) return
        val props = HashMap(parsed.properties)
        props["lit"] = targetLit
        val targetStateId = BlockStateRegistry.stateId(parsed.blockKey, props) ?: return
        if (targetStateId == currentStateId) return
        if (!org.macaroon3145.plugin.PluginSystem.beforeBlockChange(
                session = null,
                worldKey = key.worldKey,
                x = key.x,
                y = key.y,
                z = key.z,
                previousStateId = currentStateId,
                changedStateId = targetStateId,
                reason = BlockChangeReason.FURNACE_LIT_UPDATE
            )
        ) {
            return
        }
        world.setBlockState(key.x, key.y, key.z, targetStateId)
        val chunkPos = ChunkPos(key.x shr 4, key.z shr 4)
        broadcastBlockChangeToLoadedPlayers(
            worldKey = key.worldKey,
            chunkPos = chunkPos,
            x = key.x,
            y = key.y,
            z = key.z,
            stateId = targetStateId
        )
    }

    private fun tickNaturalRegeneration(deltaSeconds: Double) {
        if (!deltaSeconds.isFinite() || deltaSeconds <= 0.0) return
        for (session in sessions.values) {
            tickNaturalRegenerationForSession(session, deltaSeconds)
        }
    }

    private fun tickNaturalRegenerationForSession(session: PlayerSession, deltaSeconds: Double) {
        if (session.dead) return
        if (session.gameMode == GAME_MODE_SPECTATOR) return
        if (session.health >= MAX_PLAYER_HEALTH) {
            session.regenAccumulatorSeconds = 0.0
            return
        }

        session.regenAccumulatorSeconds += deltaSeconds
        if (!session.regenAccumulatorSeconds.isFinite()) {
            session.regenAccumulatorSeconds = 0.0
            return
        }

        var changed = false
        var loops = 0
        while (loops < MAX_PLAYER_REGEN_LOOPS_PER_TICK) {
            val canSaturatedRegen = session.food >= SATURATED_REGEN_FOOD_THRESHOLD && session.saturation > 0f
            val canNormalRegen = session.food >= NORMAL_REGEN_FOOD_THRESHOLD
            val intervalSeconds = when {
                canSaturatedRegen -> SATURATED_REGEN_INTERVAL_SECONDS
                canNormalRegen -> NORMAL_REGEN_INTERVAL_SECONDS
                else -> {
                    session.regenAccumulatorSeconds = 0.0
                    break
                }
            }
            if (session.regenAccumulatorSeconds + REGEN_TIME_EPSILON < intervalSeconds) break
            session.regenAccumulatorSeconds -= intervalSeconds

            val healAmount = if (canSaturatedRegen) {
                min(session.saturation, SATURATED_REGEN_MAX_EXHAUSTION) / SATURATED_REGEN_MAX_EXHAUSTION
            } else {
                NORMAL_REGEN_HEAL_AMOUNT
            }
            if (healAmount <= 0f) {
                loops++
                continue
            }

            val previousHealth = session.health
            val nextHealth = (previousHealth + healAmount).coerceAtMost(MAX_PLAYER_HEALTH)
            if (nextHealth <= previousHealth) {
                loops++
                continue
            }
            session.health = nextHealth
            applyFoodExhaustion(
                session = session,
                exhaustion = if (canSaturatedRegen) healAmount * SATURATED_REGEN_MAX_EXHAUSTION else NORMAL_REGEN_EXHAUSTION
            )
            changed = true
            if (session.health >= MAX_PLAYER_HEALTH) {
                session.regenAccumulatorSeconds = 0.0
                break
            }
            loops++
        }

        if (changed) {
            sendHealthPacket(session)
        }
    }

    private fun applyFoodExhaustion(session: PlayerSession, exhaustion: Float) {
        if (exhaustion <= 0f || !exhaustion.isFinite()) return
        session.foodExhaustion += exhaustion
        while (session.foodExhaustion >= FOOD_EXHAUSTION_PER_LEVEL) {
            session.foodExhaustion -= FOOD_EXHAUSTION_PER_LEVEL
            if (session.saturation > 0f) {
                session.saturation = (session.saturation - 1f).coerceAtLeast(0f)
            } else if (session.food > 0) {
                session.food -= 1
            }
        }
        if (session.food <= 0) {
            session.food = 0
            session.saturation = 0f
        }
    }

    private fun buildTickSimulationContext(): TickSimulationContext? {
        if (sessions.isEmpty()) return null
        if (WorldManager.allWorlds().isEmpty()) return null

        val sessionsByWorld = HashMap<String, MutableList<PlayerSession>>()
        for (session in sessions.values) {
            if (WorldManager.world(session.worldKey) == null) continue
            sessionsByWorld
                .computeIfAbsent(session.worldKey) { ArrayList() }
                .add(session)
        }
        if (sessionsByWorld.isEmpty()) return null

        ensureActiveSimulationChunkIndex(sessionsByWorld)
        pruneRetainedBaseChunkCaches(sessionsByWorld.keys)
        val activeByWorld = HashMap<String, Set<ChunkPos>>(sessionsByWorld.size)
        for ((worldKey, worldSessions) in sessionsByWorld) {
            val activeChunks = activeSimulationChunksCacheByWorld[worldKey] ?: emptySet()
            if (activeChunks.isEmpty()) continue
            activeByWorld[worldKey] = activeChunks
        }
        if (activeByWorld.isEmpty()) return null

        val immutableSessionsByWorld = HashMap<String, List<PlayerSession>>(sessionsByWorld.size)
        for ((worldKey, worldSessions) in sessionsByWorld) {
            immutableSessionsByWorld[worldKey] = worldSessions
        }

        return TickSimulationContext(
            sessionsByWorld = immutableSessionsByWorld,
            activeChunksByWorld = activeByWorld
        )
    }

    private fun isOpenSpawnSpace(stateId: Int): Boolean {
        if (stateId <= 0) return true
        val parsed = BlockStateRegistry.parsedState(stateId) ?: return true
        if (parsed.blockKey == "minecraft:water" || parsed.blockKey == "minecraft:lava") return false
        return parsed.blockKey in NON_SUPPORT_BLOCK_KEYS
    }

    private fun applyPlayerPlayerPushing(worldSessions: List<PlayerSession>, deltaSeconds: Double) {
        for (i in 0 until worldSessions.size) {
            val a = worldSessions[i]
            if (a.dead || a.gameMode == GAME_MODE_SPECTATOR) continue
            val aMinY = a.y
            val aMaxY = a.y + playerCollisionHeight(a)
            for (j in (i + 1) until worldSessions.size) {
                val b = worldSessions[j]
                if (b.dead || b.gameMode == GAME_MODE_SPECTATOR) continue
                val bMinY = b.y
                val bMaxY = b.y + playerCollisionHeight(b)
                if (aMaxY <= bMinY || aMinY >= bMaxY) continue
                applyHorizontalPushPair(
                    ax = a.x,
                    az = a.z,
                    ar = PLAYER_HITBOX_HALF_WIDTH,
                    bx = b.x,
                    bz = b.z,
                    br = PLAYER_HITBOX_HALF_WIDTH,
                    deltaSeconds = deltaSeconds
                ) { pushAX, pushAZ, pushBX, pushBZ ->
                    applyPlayerPushImpulse(a, pushAX, pushAZ)
                    applyPlayerPushImpulse(b, pushBX, pushBZ)
                }
            }
        }
    }

    private fun applyPlayerAnimalPushing(
        world: org.macaroon3145.world.World,
        worldSessions: List<PlayerSession>,
        animals: List<AnimalSnapshot>,
        deltaSeconds: Double
    ) {
        for (player in worldSessions) {
            if (player.dead || player.gameMode == GAME_MODE_SPECTATOR) continue
            val playerMinY = player.y
            val playerMaxY = player.y + playerCollisionHeight(player)
            for (animal in animals) {
                val animalMinY = animal.y
                val animalMaxY = animal.y + animal.hitboxHeight
                if (playerMaxY <= animalMinY || playerMinY >= animalMaxY) continue
                applyHorizontalPushPair(
                    ax = player.x,
                    az = player.z,
                    ar = PLAYER_HITBOX_HALF_WIDTH,
                    bx = animal.x,
                    bz = animal.z,
                    br = animal.hitboxWidth * 0.5,
                    deltaSeconds = deltaSeconds
                ) { _, _, pushAX, pushAZ ->
                    world.addAnimalHorizontalImpulse(
                        animal.entityId,
                        pushAX,
                        pushAZ
                    )
                }
            }
        }
    }

    private fun playerCollisionHeight(session: PlayerSession): Double {
        return playerPoseHeight(session)
    }

    private data class Aabb(
        val minX: Double,
        val minY: Double,
        val minZ: Double,
        val maxX: Double,
        val maxY: Double,
        val maxZ: Double
    ) {
        fun toDoubleArray(): DoubleArray = doubleArrayOf(minX, maxX, minY, maxY, minZ, maxZ)
    }

    private fun playerPoseHeight(session: PlayerSession): Double {
        return when {
            session.swimming -> 0.6
            session.sneaking -> 1.5
            else -> 1.8
        }
    }

    private fun playerAabb(
        session: PlayerSession,
        expandHorizontal: Double = 0.0,
        expandDown: Double = 0.0,
        expandUp: Double = 0.0
    ): Aabb {
        return Aabb(
            minX = session.x - PLAYER_HITBOX_HALF_WIDTH - expandHorizontal,
            minY = session.y - expandDown,
            minZ = session.z - PLAYER_HITBOX_HALF_WIDTH - expandHorizontal,
            maxX = session.x + PLAYER_HITBOX_HALF_WIDTH + expandHorizontal,
            maxY = session.y + playerPoseHeight(session) + expandUp,
            maxZ = session.z + PLAYER_HITBOX_HALF_WIDTH + expandHorizontal
        )
    }

    private fun animalAabb(
        target: AnimalSnapshot,
        expandHorizontal: Double = 0.0,
        expandDown: Double = 0.0,
        expandUp: Double = 0.0
    ): Aabb {
        val halfWidth = target.hitboxWidth * 0.5
        return Aabb(
            minX = target.x - halfWidth - expandHorizontal,
            minY = target.y - expandDown,
            minZ = target.z - halfWidth - expandHorizontal,
            maxX = target.x + halfWidth + expandHorizontal,
            maxY = target.y + target.hitboxHeight + expandUp,
            maxZ = target.z + halfWidth + expandHorizontal
        )
    }

    private fun playerAabbAt(
        x: Double,
        y: Double,
        z: Double,
        halfWidth: Double,
        height: Double
    ): Aabb {
        return Aabb(
            minX = x - halfWidth,
            minY = y,
            minZ = z - halfWidth,
            maxX = x + halfWidth,
            maxY = y + height,
            maxZ = z + halfWidth
        )
    }

    private fun droppedItemAabb(snapshot: DroppedItemSnapshot): Aabb {
        return Aabb(
            minX = snapshot.x - DROPPED_ITEM_HALF_WIDTH,
            minY = snapshot.y,
            minZ = snapshot.z - DROPPED_ITEM_HALF_WIDTH,
            maxX = snapshot.x + DROPPED_ITEM_HALF_WIDTH,
            maxY = snapshot.y + DROPPED_ITEM_HEIGHT,
            maxZ = snapshot.z + DROPPED_ITEM_HALF_WIDTH
        )
    }

    private fun applyAnimalAnimalPushing(
        world: org.macaroon3145.world.World,
        animals: List<AnimalSnapshot>,
        deltaSeconds: Double
    ) {
        for (i in 0 until animals.size) {
            val a = animals[i]
            val aMinY = a.y
            val aMaxY = a.y + a.hitboxHeight
            for (j in (i + 1) until animals.size) {
                val b = animals[j]
                val bMinY = b.y
                val bMaxY = b.y + b.hitboxHeight
                if (aMaxY <= bMinY || aMinY >= bMaxY) continue
                applyHorizontalPushPair(
                    ax = a.x,
                    az = a.z,
                    ar = a.hitboxWidth * 0.5,
                    bx = b.x,
                    bz = b.z,
                    br = b.hitboxWidth * 0.5,
                    deltaSeconds = deltaSeconds
                ) { pushAX, pushAZ, pushBX, pushBZ ->
                    world.addAnimalHorizontalImpulse(a.entityId, pushAX, pushAZ)
                    world.addAnimalHorizontalImpulse(b.entityId, pushBX, pushBZ)
                }
            }
        }
    }

    private fun collectAnimalTemptSources(worldSessions: List<PlayerSession>): List<AnimalTemptSource> {
        if (worldSessions.isEmpty() || pigTemptItemIds.isEmpty()) return emptyList()
        val out = ArrayList<AnimalTemptSource>(worldSessions.size)
        for (session in worldSessions) {
            if (session.dead || session.gameMode == GAME_MODE_SPECTATOR) continue
            val selectedSlot = session.selectedHotbarSlot.coerceIn(0, 8)
            val mainHandItemId = session.hotbarItemIds[selectedSlot]
            val mainHandCount = session.hotbarItemCounts[selectedSlot]
            val holdingPigFoodMain = mainHandCount > 0 && mainHandItemId in pigTemptItemIds
            val holdingPigFoodOffhand = session.offhandItemCount > 0 && session.offhandItemId in pigTemptItemIds
            if (!holdingPigFoodMain && !holdingPigFoodOffhand) continue
            out.add(
                AnimalTemptSource(
                    x = session.x,
                    y = session.clientEyeY,
                    z = session.z,
                    range = PIG_TEMPT_RANGE_BLOCKS
                )
            )
        }
        return out
    }

    private fun collectAnimalLookSources(worldSessions: List<PlayerSession>): List<AnimalLookSource> {
        if (worldSessions.isEmpty()) return emptyList()
        val out = ArrayList<AnimalLookSource>(worldSessions.size)
        for (session in worldSessions) {
            if (session.dead || session.gameMode == GAME_MODE_SPECTATOR) continue
            out.add(
                AnimalLookSource(
                    x = session.x,
                    y = session.clientEyeY,
                    z = session.z
                )
            )
        }
        return out
    }

    private fun collectAnimalRideControls(
        world: org.macaroon3145.world.World,
        worldSessions: List<PlayerSession>,
        deltaSeconds: Double
    ): List<AnimalRideControl> {
        if (worldSessions.isEmpty()) return emptyList()
        val out = ArrayList<AnimalRideControl>(worldSessions.size)
        for (session in worldSessions) {
            val animalEntityId = session.ridingAnimalEntityId
            if (animalEntityId < 0) continue
            if (session.dead || session.gameMode == GAME_MODE_SPECTATOR) {
                dismountPlayerFromAnimal(session)
                continue
            }
            if (animalEntityId !in saddledAnimalEntityIds) {
                dismountPlayerFromAnimal(session)
                continue
            }
            val animal = world.animalSnapshot(animalEntityId)
            if (animal == null || animal.kind != AnimalKind.PIG) {
                dismountPlayerFromAnimal(session)
                continue
            }
            val mappedAnimalEntityId = animalEntityIdByRiderEntityId[session.entityId]
            val mappedRiderEntityId = animalRiderEntityIdByAnimalEntityId[animalEntityId]
            if (mappedAnimalEntityId != animalEntityId || mappedRiderEntityId != session.entityId) {
                dismountPlayerFromAnimal(session)
                continue
            }
            val canControlPig = isHoldingPigControlItem(session)
            if (!canControlPig) {
                continue
            }
            val forwardInput = 1.0
            val boost = currentPigBoostMultiplier(animalEntityId, deltaSeconds)
            val hasClientVehiclePose = hasFreshClientVehiclePose(session)
            out.add(
                AnimalRideControl(
                    entityId = animalEntityId,
                    riderYaw = session.yaw,
                    riderPitch = session.pitch,
                    forwardInput = forwardInput,
                    boostMultiplier = boost,
                    hasClientVehiclePose = hasClientVehiclePose,
                    clientVehicleX = session.lastClientVehicleX,
                    clientVehicleY = session.lastClientVehicleY,
                    clientVehicleZ = session.lastClientVehicleZ,
                    clientVehicleYaw = session.lastClientVehicleYaw,
                    clientVehiclePitch = session.lastClientVehiclePitch,
                    clientVehicleOnGround = session.lastClientVehicleOnGround
                )
            )
        }
        return out
    }

    private fun hasFreshClientVehiclePose(session: PlayerSession): Boolean {
        val stampedAt = session.lastClientVehiclePacketAtNanos
        if (stampedAt <= 0L) return false
        val age = System.nanoTime() - stampedAt
        return age in 0..MAX_CLIENT_VEHICLE_POSE_AGE_NANOS
    }

    private fun isHoldingPigControlItem(session: PlayerSession): Boolean {
        if (carrotOnAStickItemId < 0) return false
        val selectedSlot = session.selectedHotbarSlot.coerceIn(0, 8)
        if (session.hotbarItemCounts[selectedSlot] > 0 && session.hotbarItemIds[selectedSlot] == carrotOnAStickItemId) {
            return true
        }
        return session.offhandItemCount > 0 && session.offhandItemId == carrotOnAStickItemId
    }

    private fun currentPigBoostMultiplier(animalEntityId: Int, deltaSeconds: Double): Double {
        val state = pigBoostStatesByAnimalEntityId[animalEntityId] ?: return 1.0
        val duration = state.durationSeconds
        if (duration <= 0.0 || !duration.isFinite()) {
            pigBoostStatesByAnimalEntityId.remove(animalEntityId, state)
            return 1.0
        }
        val nextElapsed = (state.elapsedSeconds + deltaSeconds.coerceAtLeast(0.0))
        if (!nextElapsed.isFinite() || nextElapsed >= duration) {
            pigBoostStatesByAnimalEntityId.remove(animalEntityId, state)
            return 1.0
        }
        state.elapsedSeconds = nextElapsed
        val progress = (nextElapsed / duration).coerceIn(0.0, 1.0)
        return 1.0 + (1.15 * sin(Math.PI * progress))
    }

    private inline fun applyHorizontalPushPair(
        ax: Double,
        az: Double,
        ar: Double,
        bx: Double,
        bz: Double,
        br: Double,
        deltaSeconds: Double,
        apply: (Double, Double, Double, Double) -> Unit
    ) {
        val dx = bx - ax
        val dz = bz - az
        val minDist = ar + br
        val distSq = dx * dx + dz * dz
        if (distSq >= minDist * minDist || minDist <= 0.0) return
        // Vanilla Entity.push(Entity):
        // absMax(dx,dz) -> sqrt -> normalize -> scale by min(1, 1/dist) -> *0.05 per tick.
        var absMax = maxOf(abs(dx), abs(dz))
        if (absMax < VANILLA_ENTITY_PUSH_MIN_DISTANCE) return
        absMax = sqrt(absMax)
        if (!absMax.isFinite() || absMax <= 1.0e-9) return
        var nx = dx / absMax
        var nz = dz / absMax
        var scalar = 1.0 / absMax
        if (scalar > 1.0) scalar = 1.0
        nx *= scalar
        nz *= scalar
        val tickScale = (deltaSeconds * 20.0).coerceAtLeast(0.0)
        val pushX = nx * VANILLA_ENTITY_PUSH_STRENGTH_PER_TICK * tickScale
        val pushZ = nz * VANILLA_ENTITY_PUSH_STRENGTH_PER_TICK * tickScale
        apply(-pushX, -pushZ, pushX, pushZ)
    }

    private fun applyPlayerPushImpulse(session: PlayerSession, impulseX: Double, impulseZ: Double) {
        if (impulseX == 0.0 && impulseZ == 0.0) return
        session.velocityX = (session.velocityX + impulseX).coerceIn(-ENTITY_PUSH_MAX_PLAYER_VELOCITY, ENTITY_PUSH_MAX_PLAYER_VELOCITY)
        session.velocityZ = (session.velocityZ + impulseZ).coerceIn(-ENTITY_PUSH_MAX_PLAYER_VELOCITY, ENTITY_PUSH_MAX_PLAYER_VELOCITY)
    }


    fun worldTimeSnapshot(worldKey: String): Pair<Long, Long> {
        val state = worldTimes.computeIfAbsent(worldKey) {
            WorldTimeState(
                worldAgeTicks = INITIAL_WORLD_TIME_TICKS,
                timeOfDayTicks = INITIAL_WORLD_TIME_TICKS,
                broadcastAccumulatorSeconds = 0.0
            )
        }
        run {
            return state.worldAgeTicks.toLong() to state.timeOfDayTicks.toLong()
        }
    }

    fun worldElapsedTicks(worldKey: String): Long? {
        return worldTimeSnapshotOrNull(worldKey)?.first
    }

    fun worldTimeOfDayTicks(worldKey: String): Long? {
        return worldTimeSnapshotOrNull(worldKey)?.second
    }

    fun setWorldTimeOfDayTicks(worldKey: String, timeOfDayTicks: Long): Boolean {
        return setWorldTimeAndBroadcast(worldKey, timeOfDayTicks) != null
    }

    fun addWorldTimeTicks(worldKey: String, deltaTicks: Long): Boolean {
        return addWorldTimeAndBroadcast(worldKey, deltaTicks) != null
    }

    fun spawnDroppedItemAt(
        worldKey: String,
        itemId: Int,
        itemCount: Int,
        x: Double,
        y: Double,
        z: Double,
        impulse: Boolean = false
    ): Boolean {
        if (itemId < 0 || itemCount <= 0) return false
        val world = WorldManager.world(worldKey) ?: return false
        val (vx, vy, vz) = if (!impulse) {
            Triple(0.0, 0.0, 0.0)
        } else {
            val random = ThreadLocalRandom.current()
            Triple(
                (random.nextDouble() - 0.5) * 0.16,
                0.2 + random.nextDouble() * 0.05,
                (random.nextDouble() - 0.5) * 0.16
            )
        }
        return world.spawnDroppedItem(
            entityId = allocateEntityId(),
            itemId = itemId,
            itemCount = itemCount,
            x = x,
            y = y,
            z = z,
            vx = vx,
            vy = vy,
            vz = vz,
            pickupDelaySeconds = PLAYER_DROPPED_ITEM_PICKUP_DELAY_SECONDS
        )
    }

    fun spawnDroppedItemEntityAt(
        worldKey: String,
        itemId: Int,
        itemCount: Int,
        x: Double,
        y: Double,
        z: Double,
        impulse: Boolean = false
    ): DroppedItemSnapshot? {
        if (itemId < 0 || itemCount <= 0) return null
        val world = WorldManager.world(worldKey) ?: return null
        val (vx, vy, vz) = if (!impulse) {
            Triple(0.0, 0.0, 0.0)
        } else {
            val random = ThreadLocalRandom.current()
            Triple(
                (random.nextDouble() - 0.5) * 0.16,
                0.2 + random.nextDouble() * 0.05,
                (random.nextDouble() - 0.5) * 0.16
            )
        }
        val entityId = allocateEntityId()
        val spawned = world.spawnDroppedItem(
            entityId = entityId,
            itemId = itemId,
            itemCount = itemCount,
            x = x,
            y = y,
            z = z,
            vx = vx,
            vy = vy,
            vz = vz,
            pickupDelaySeconds = PLAYER_DROPPED_ITEM_PICKUP_DELAY_SECONDS
        )
        if (!spawned) return null
        return world.droppedItemSnapshot(entityId)
    }

    fun spawnThrownItemEntityAt(
        worldKey: String,
        kind: ThrownItemKind,
        x: Double,
        y: Double,
        z: Double,
        ownerEntityId: Int = -1,
        vx: Double = 0.0,
        vy: Double = 0.0,
        vz: Double = 0.0
    ): SpawnedEntityRef? {
        if (!x.isFinite() || !y.isFinite() || !z.isFinite()) {
            logger.warn("spawnThrownItemEntityAt rejected non-finite position: world={}, kind={}, x={}, y={}, z={}", worldKey, kind, x, y, z)
            return null
        }
        if (!vx.isFinite() || !vy.isFinite() || !vz.isFinite()) {
            logger.warn("spawnThrownItemEntityAt rejected non-finite velocity: world={}, kind={}, vx={}, vy={}, vz={}", worldKey, kind, vx, vy, vz)
            return null
        }
        val world = WorldManager.world(worldKey)
        if (world == null) {
            logger.warn("spawnThrownItemEntityAt failed: world not found: world={}, kind={}", worldKey, kind)
            return null
        }
        val entityId = allocateEntityId()
        val uuid = UUID.randomUUID()
        val spawned = world.spawnThrownItem(
            entityId = entityId,
            ownerEntityId = ownerEntityId,
            kind = kind,
            x = x,
            y = y,
            z = z,
            vx = vx,
            vy = vy,
            vz = vz,
            uuid = uuid
        )
        if (!spawned) {
            logger.warn(
                "spawnThrownItemEntityAt failed: world.spawnThrownItem=false (world={}, kind={}, entityId={}, owner={}, x={}, y={}, z={}, vx={}, vy={}, vz={})",
                worldKey,
                kind,
                entityId,
                ownerEntityId,
                x,
                y,
                z,
                vx,
                vy,
                vz
            )
            return null
        }
        return SpawnedEntityRef(entityId = entityId, uuid = uuid)
    }

    fun droppedItemSnapshot(worldKey: String, entityId: Int): DroppedItemSnapshot? {
        return WorldManager.world(worldKey)?.droppedItemSnapshot(entityId)
    }

    fun thrownItemSnapshot(worldKey: String, entityId: Int): ThrownItemSnapshot? {
        return WorldManager.world(worldKey)?.thrownItemSnapshot(entityId)
    }

    fun droppedItemSnapshots(worldKey: String): List<DroppedItemSnapshot> {
        return WorldManager.world(worldKey)?.droppedItemSnapshots() ?: emptyList()
    }

    fun thrownItemSnapshots(worldKey: String): List<ThrownItemSnapshot> {
        return WorldManager.world(worldKey)?.thrownItemSnapshots() ?: emptyList()
    }

    fun droppedItemSnapshotByUuid(worldKey: String, uuid: UUID): DroppedItemSnapshot? {
        return WorldManager.world(worldKey)?.droppedItemSnapshotByUuid(uuid)
    }

    fun thrownItemSnapshotByUuid(worldKey: String, uuid: UUID): ThrownItemSnapshot? {
        return WorldManager.world(worldKey)?.thrownItemSnapshotByUuid(uuid)
    }

    fun setDroppedItemStack(worldKey: String, entityId: Int, itemId: Int, amount: Int): Boolean {
        if (itemId < 0 || amount <= 0) return false
        val world = WorldManager.world(worldKey) ?: return false
        return world.setDroppedItemStack(entityId, itemId, amount) != null
    }

    fun setDroppedItemPosition(worldKey: String, entityId: Int, x: Double, y: Double, z: Double): Boolean {
        if (!x.isFinite() || !y.isFinite() || !z.isFinite()) return false
        val world = WorldManager.world(worldKey) ?: return false
        return world.setDroppedItemPosition(entityId, x, y, z) != null
    }

    fun setDroppedItemSpeedMetersPerSecond(worldKey: String, entityId: Int, x: Double, y: Double, z: Double): Boolean {
        if (!x.isFinite() || !y.isFinite() || !z.isFinite()) return false
        val world = WorldManager.world(worldKey) ?: return false
        return world.setDroppedItemVelocity(
            entityId = entityId,
            vx = x / MINECRAFT_TICKS_PER_SECOND,
            vy = y / MINECRAFT_TICKS_PER_SECOND,
            vz = z / MINECRAFT_TICKS_PER_SECOND
        ) != null
    }

    fun setDroppedItemAccelerationMetersPerSecondSquared(
        worldKey: String,
        entityId: Int,
        x: Double,
        y: Double,
        z: Double
    ): Boolean {
        if (!x.isFinite() || !y.isFinite() || !z.isFinite()) return false
        val world = WorldManager.world(worldKey) ?: return false
        return world.setDroppedItemAcceleration(entityId, x, y, z) != null
    }

    fun setDroppedItemPickupDelaySeconds(worldKey: String, entityId: Int, pickupDelaySeconds: Double): Boolean {
        if (!pickupDelaySeconds.isFinite()) return false
        val world = WorldManager.world(worldKey) ?: return false
        return world.setDroppedItemPickupDelay(entityId, pickupDelaySeconds) != null
    }

    fun setDroppedItemOnGround(worldKey: String, entityId: Int, onGround: Boolean): Boolean {
        val world = WorldManager.world(worldKey) ?: return false
        return world.setDroppedItemOnGround(entityId, onGround) != null
    }

    fun setThrownItemPosition(worldKey: String, entityId: Int, x: Double, y: Double, z: Double): Boolean {
        if (!x.isFinite() || !y.isFinite() || !z.isFinite()) return false
        val world = WorldManager.world(worldKey) ?: return false
        return world.setThrownItemPosition(entityId, x, y, z) != null
    }

    fun setThrownItemSpeedMetersPerSecond(worldKey: String, entityId: Int, x: Double, y: Double, z: Double): Boolean {
        if (!x.isFinite() || !y.isFinite() || !z.isFinite()) return false
        val world = WorldManager.world(worldKey) ?: return false
        return world.setThrownItemVelocity(
            entityId = entityId,
            vx = x / MINECRAFT_TICKS_PER_SECOND,
            vy = y / MINECRAFT_TICKS_PER_SECOND,
            vz = z / MINECRAFT_TICKS_PER_SECOND
        ) != null
    }

    fun setThrownItemPreviousPosition(worldKey: String, entityId: Int, x: Double, y: Double, z: Double): Boolean {
        if (!x.isFinite() || !y.isFinite() || !z.isFinite()) return false
        val world = WorldManager.world(worldKey) ?: return false
        return world.setThrownItemPreviousPosition(entityId, x, y, z) != null
    }

    fun setThrownItemAccelerationMetersPerSecondSquared(
        worldKey: String,
        entityId: Int,
        x: Double,
        y: Double,
        z: Double
    ): Boolean {
        if (!x.isFinite() || !y.isFinite() || !z.isFinite()) return false
        val world = WorldManager.world(worldKey) ?: return false
        return world.setThrownItemAcceleration(entityId, x, y, z) != null
    }

    fun setThrownItemOwnerEntityId(worldKey: String, entityId: Int, ownerEntityId: Int): Boolean {
        val world = WorldManager.world(worldKey) ?: return false
        return world.setThrownItemOwnerEntityId(entityId, ownerEntityId) != null
    }

    fun setThrownItemKind(worldKey: String, entityId: Int, kind: ThrownItemKind): Boolean {
        val world = WorldManager.world(worldKey) ?: return false
        return world.setThrownItemKind(entityId, kind) != null
    }

    fun setThrownItemOnGround(worldKey: String, entityId: Int, onGround: Boolean): Boolean {
        val world = WorldManager.world(worldKey) ?: return false
        return world.setThrownItemOnGround(entityId, onGround) != null
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
        run {
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
        run {
            state.timeOfDayTicks = timeOfDayTicks.toDouble().coerceAtLeast(0.0)
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
        run {
            state.worldAgeTicks = (state.worldAgeTicks + deltaTicks).coerceAtLeast(0.0)
            state.timeOfDayTicks = (state.timeOfDayTicks + deltaTicks).coerceAtLeast(0.0)
            state.broadcastAccumulatorSeconds = 0.0
            worldAge = state.worldAgeTicks.toLong()
            timeOfDay = state.timeOfDayTicks.toLong()
        }
        broadcastWorldTimePacket(worldKey, worldAge, timeOfDay)
        return worldAge to timeOfDay
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

    private fun scheduleDroppedItemEvents(
        tickSequence: Long,
        deltaSeconds: Double,
        physicsDeltaSeconds: Double,
        context: TickSimulationContext
    ) {
        for ((worldKey, activeSimulationChunks) in context.activeChunksByWorld) {
            val world = WorldManager.world(worldKey)
            if (world == null) continue
            val worldSessions = context.sessionsByWorld[world.key]
            if (worldSessions == null) continue
            if (worldSessions.isEmpty()) continue
            val sessionsSnapshot = worldSessions.toList()
            val pendingAsync = AtomicInteger(1)
            val done = {
                pendingAsync.decrementAndGet()
            }
            try {
                val hasAnimals = world.hasAnimals()
                if (hasAnimals) {
                    if (animalSourceDirtyWorlds.remove(world.key)) {
                        world.setAnimalTemptSources(collectAnimalTemptSources(sessionsSnapshot))
                        world.setAnimalLookSources(collectAnimalLookSources(sessionsSnapshot))
                    }
                    if (animalEntityIdByRiderEntityId.isNotEmpty()) {
                        world.setAnimalRideControls(
                            collectAnimalRideControls(
                                world = world,
                                worldSessions = sessionsSnapshot,
                                deltaSeconds = physicsDeltaSeconds
                            )
                        )
                        animalRideControlsActiveWorlds.add(world.key)
                    } else if (animalRideControlsActiveWorlds.remove(world.key)) {
                        world.setAnimalRideControls(emptyList())
                    }
                } else {
                    animalSourceDirtyWorlds.remove(world.key)
                    if (animalRideControlsActiveWorlds.remove(world.key)) {
                        world.setAnimalRideControls(emptyList())
                    }
                }
                val chunkProcessingFrame = world.beginChunkProcessingFrame(tickSequence)
                pendingAsync.incrementAndGet()
                val droppedEvents = world.tickDroppedItems(
                    physicsDeltaSeconds,
                    activeSimulationChunks,
                    onChunkEvents = { chunkEvents ->
                        dispatchWorldPhysicsEvents(
                            world = world,
                            worldSessions = sessionsSnapshot,
                            events = WorldPhysicsEvents(
                                droppedItems = chunkEvents,
                                fallingBlocks = org.macaroon3145.world.FallingBlockTickEvents(emptyList(), emptyList(), emptyList(), emptyList()),
                                thrownItems = org.macaroon3145.world.ThrownItemTickEvents(emptyList(), emptyList(), emptyList()),
                                animals = org.macaroon3145.world.AnimalTickEvents(emptyList(), emptyList(), emptyList(), emptyList(), emptyList())
                            ),
                            deltaSeconds = deltaSeconds
                        )
                    },
                    onDispatchComplete = { done() }
                ) { chunkPos, elapsedNanos ->
                    world.recordChunkProcessingNanos(chunkProcessingFrame, chunkPos, elapsedNanos, category = "dropped")
                }
                val fallingEvents = world.tickFallingBlocks(
                    physicsDeltaSeconds,
                    activeSimulationChunks
                ) { chunkPos, elapsedNanos ->
                    world.recordChunkProcessingNanos(chunkProcessingFrame, chunkPos, elapsedNanos, category = "falling")
                }
                val thrownEvents = world.tickThrownItems(
                    physicsDeltaSeconds,
                    activeSimulationChunks
                ) { chunkPos, elapsedNanos ->
                    world.recordChunkProcessingNanos(chunkProcessingFrame, chunkPos, elapsedNanos, category = "thrown")
                }
                val animalEvents = if (hasAnimals) {
                    pendingAsync.incrementAndGet()
                    world.tickAnimals(
                        physicsDeltaSeconds,
                        activeSimulationChunks,
                        onChunkEvents = { chunkEvents ->
                            markPushingWakeFromAnimalEvents(world.key, chunkEvents)
                            dispatchWorldPhysicsEvents(
                                world = world,
                                worldSessions = sessionsSnapshot,
                                events = WorldPhysicsEvents(
                                    droppedItems = org.macaroon3145.world.DroppedItemTickEvents(emptyList(), emptyList(), emptyList()),
                                    fallingBlocks = org.macaroon3145.world.FallingBlockTickEvents(emptyList(), emptyList(), emptyList(), emptyList()),
                                    thrownItems = org.macaroon3145.world.ThrownItemTickEvents(emptyList(), emptyList(), emptyList()),
                                    animals = chunkEvents
                                ),
                                deltaSeconds = deltaSeconds
                            )
                        },
                        onDispatchComplete = { done() }
                    ) { chunkPos, elapsedNanos ->
                        world.recordChunkProcessingNanos(chunkProcessingFrame, chunkPos, elapsedNanos, category = "animal")
                    }
                } else {
                    org.macaroon3145.world.AnimalTickEvents(emptyList(), emptyList(), emptyList(), emptyList(), emptyList())
                }
                world.finishChunkProcessingFrame(
                    frame = chunkProcessingFrame,
                    activeChunks = activeSimulationChunks,
                    includeZeroForInactive = false,
                    accumulateIntoLast = true
                )
                markPushingWakeFromAnimalEvents(world.key, animalEvents)
                dispatchWorldPhysicsEvents(
                    world = world,
                    worldSessions = sessionsSnapshot,
                    events = WorldPhysicsEvents(
                        droppedItems = droppedEvents,
                        fallingBlocks = fallingEvents,
                        thrownItems = thrownEvents,
                        animals = animalEvents
                    ),
                    deltaSeconds = deltaSeconds
                )
            } catch (_: Throwable) {}
            done()
        }
    }

    private fun dispatchWorldPhysicsEvents(
        world: org.macaroon3145.world.World,
        worldSessions: List<PlayerSession>,
        events: WorldPhysicsEvents,
        deltaSeconds: Double
    ) {
        val droppedEvents = events.droppedItems
        val fallingEvents = events.fallingBlocks
        val thrownEvents = events.thrownItems
        val animalEvents = events.animals

        val outboundByContext = HashMap<ChannelHandlerContext, MutableList<ByteArray>>()
        val pickedRemovals = collectDroppedItemPickups(world, worldSessions, outboundByContext)
        val thrownRemoved = ArrayList(thrownEvents.removed)
        val thrownHitIds = HashSet<Int>()
        collectThrownItemEntityHits(world, worldSessions, thrownEvents.spawned, thrownRemoved, thrownHitIds)
        collectThrownItemEntityHits(world, worldSessions, thrownEvents.updated, thrownRemoved, thrownHitIds)
        processEnderPearlImpacts(world.key, thrownRemoved)
        val thrownRemovedIds = thrownRemoved.asSequence().map { it.entityId }.toHashSet()
        val pickedEntityIds = pickedRemovals.asSequence()
            .map { it.entityId }
            .toHashSet()
        val droppedRemovedByChunk = droppedEvents.removed
            .asSequence()
            .filterNot { it.entityId in pickedEntityIds }
            .groupBy { it.chunkPos }
        val pickupsByChunk = pickedRemovals.groupBy {
            ChunkPos(ChunkStreamingService.chunkXFromBlockX(it.x), ChunkStreamingService.chunkZFromBlockZ(it.z))
        }
        fun liveDroppedSnapshotOrNull(snapshot: DroppedItemSnapshot): DroppedItemSnapshot? {
            val live = world.droppedItemSnapshot(snapshot.entityId) ?: return null
            if (live.uuid != snapshot.uuid) return null
            return live
        }
        val droppedSpawnedByChunk = droppedEvents.spawned
            .asSequence()
            .filterNot { it.entityId in pickedEntityIds }
            .mapNotNull { liveDroppedSnapshotOrNull(it) }
            .groupBy { it.chunkPos }
        val droppedUpdatedByChunk = droppedEvents.updated
            .asSequence()
            .filterNot { it.entityId in pickedEntityIds }
            .mapNotNull { liveDroppedSnapshotOrNull(it) }
            .groupBy { it.chunkPos }
        val fallingRemovedByChunk = fallingEvents.removed.groupBy { it.chunkPos }
        val fallingLandedByChunk = fallingEvents.landed.groupBy { it.chunkPos }
        val fallingSpawnedByChunk = fallingEvents.spawned.groupBy { it.chunkPos }
        val fallingUpdatedByChunk = fallingEvents.updated.groupBy { it.chunkPos }
        val thrownRemovedByChunk = thrownRemoved.groupBy { it.chunkPos }
        val thrownSpawnedByChunk = thrownEvents.spawned
            .asSequence()
            .filterNot { it.entityId in thrownRemovedIds }
            .filterNot { it.entityId in thrownHitIds }
            .groupBy { it.chunkPos }
        val thrownUpdatedByChunk = thrownEvents.updated
            .asSequence()
            .filterNot { it.entityId in thrownRemovedIds }
            .filterNot { it.entityId in thrownHitIds }
            .groupBy { it.chunkPos }
        val animalRemovedByChunk = animalEvents.removed.groupBy { it.chunkPos }
        val animalSpawnedByChunk = animalEvents.spawned.groupBy { it.chunkPos }
        val animalUpdatedByChunk = animalEvents.updated.groupBy { it.chunkPos }
        val animalDamagedByChunk = animalEvents.damaged.groupBy { it.chunkPos }
        val animalAmbientByChunk = animalEvents.ambient.groupBy { it.chunkPos }

        val chunksInOrder = LinkedHashSet<ChunkPos>()
        chunksInOrder.addAll(pickupsByChunk.keys)
        chunksInOrder.addAll(droppedRemovedByChunk.keys)
        chunksInOrder.addAll(droppedSpawnedByChunk.keys)
        chunksInOrder.addAll(droppedUpdatedByChunk.keys)
        chunksInOrder.addAll(fallingRemovedByChunk.keys)
        chunksInOrder.addAll(fallingLandedByChunk.keys)
        chunksInOrder.addAll(fallingSpawnedByChunk.keys)
        chunksInOrder.addAll(fallingUpdatedByChunk.keys)
        chunksInOrder.addAll(thrownRemovedByChunk.keys)
        chunksInOrder.addAll(thrownSpawnedByChunk.keys)
        chunksInOrder.addAll(thrownUpdatedByChunk.keys)
        chunksInOrder.addAll(animalRemovedByChunk.keys)
        chunksInOrder.addAll(animalSpawnedByChunk.keys)
        chunksInOrder.addAll(animalUpdatedByChunk.keys)
        chunksInOrder.addAll(animalDamagedByChunk.keys)
        chunksInOrder.addAll(animalAmbientByChunk.keys)

        for (chunkPos in chunksInOrder) {
            for (pickup in pickupsByChunk[chunkPos].orEmpty()) {
                val takePacket = PlayPackets.takeItemEntityPacket(
                    collectedEntityId = pickup.entityId,
                    collectorEntityId = pickup.collectorEntityId,
                    pickupItemCount = pickup.pickupItemCount
                )
                val removePacket = PlayPackets.removeEntitiesPacket(intArrayOf(pickup.entityId))
                for (session in worldSessions) {
                    val isCollector = session.entityId == pickup.collectorEntityId
                    val wasVisible = session.visibleDroppedItemEntityIds.contains(pickup.entityId)
                    val inLoadedChunk = session.loadedChunks.contains(chunkPos)
                    if (!isCollector && !wasVisible && !inLoadedChunk) continue
                    session.visibleDroppedItemEntityIds.remove(pickup.entityId)
                    session.droppedItemTrackerStates.remove(pickup.entityId)
                    val ctx = contexts[session.channelId] ?: continue
                    if (!ctx.channel().isActive) continue
                    if (isCollector || wasVisible || inLoadedChunk) {
                        enqueueDroppedItemPacket(outboundByContext, ctx, takePacket)
                    }
                    enqueueDroppedItemPacket(outboundByContext, ctx, removePacket)
                }
            }

            for (removed in droppedRemovedByChunk[chunkPos].orEmpty()) {
                val packet = PlayPackets.removeEntitiesPacket(intArrayOf(removed.entityId))
                for (session in worldSessions) {
                    val wasVisible = session.visibleDroppedItemEntityIds.remove(removed.entityId)
                    val inLoadedChunk = session.loadedChunks.contains(chunkPos)
                    if (!wasVisible && !inLoadedChunk) continue
                    session.droppedItemTrackerStates.remove(removed.entityId)
                    val ctx = contexts[session.channelId] ?: continue
                    if (!ctx.channel().isActive) continue
                    enqueueDroppedItemPacket(outboundByContext, ctx, packet)
                }
            }
            for (snapshot in droppedSpawnedByChunk[chunkPos].orEmpty()) {
                syncDroppedItemSnapshot(
                    snapshot = snapshot,
                    worldSessions = worldSessions,
                    outboundByContext = outboundByContext,
                    sendPositionUpdate = false,
                    deltaSeconds = deltaSeconds
                )
            }
            for (snapshot in droppedUpdatedByChunk[chunkPos].orEmpty()) {
                syncDroppedItemSnapshot(
                    snapshot = snapshot,
                    worldSessions = worldSessions,
                    outboundByContext = outboundByContext,
                    sendPositionUpdate = true,
                    deltaSeconds = deltaSeconds
                )
            }

            for (removed in fallingRemovedByChunk[chunkPos].orEmpty()) {
                val packet = PlayPackets.removeEntitiesPacket(intArrayOf(removed.entityId))
                for (session in worldSessions) {
                    val wasVisible = session.visibleFallingBlockEntityIds.remove(removed.entityId)
                    val inLoadedChunk = session.loadedChunks.contains(chunkPos)
                    if (!wasVisible && !inLoadedChunk) continue
                    session.fallingBlockTrackerStates.remove(removed.entityId)
                    val ctx = contexts[session.channelId] ?: continue
                    if (!ctx.channel().isActive) continue
                    enqueueDroppedItemPacket(outboundByContext, ctx, packet)
                }
            }
            for (landed in fallingLandedByChunk[chunkPos].orEmpty()) {
                val packet = PlayPackets.blockChangePacket(landed.blockX, landed.blockY, landed.blockZ, landed.blockStateId)
                for (session in worldSessions) {
                    if (!session.loadedChunks.contains(landed.chunkPos)) continue
                    val ctx = contexts[session.channelId] ?: continue
                    if (!ctx.channel().isActive) continue
                    enqueueDroppedItemPacket(outboundByContext, ctx, packet)
                }
            }
            for (snapshot in fallingSpawnedByChunk[chunkPos].orEmpty()) {
                syncFallingBlockSnapshot(
                    snapshot = snapshot,
                    worldSessions = worldSessions,
                    outboundByContext = outboundByContext,
                    sendPositionUpdate = false,
                    deltaSeconds = deltaSeconds
                )
            }
            for (snapshot in fallingUpdatedByChunk[chunkPos].orEmpty()) {
                syncFallingBlockSnapshot(
                    snapshot = snapshot,
                    worldSessions = worldSessions,
                    outboundByContext = outboundByContext,
                    sendPositionUpdate = true,
                    deltaSeconds = deltaSeconds
                )
            }

            for (removed in thrownRemovedByChunk[chunkPos].orEmpty()) {
                val impactPacket = if (removed.hit) {
                    PlayPackets.entityPositionSyncPacket(
                        entityId = removed.entityId,
                        x = removed.x,
                        y = removed.y,
                        z = removed.z,
                        yaw = 0f,
                        pitch = 0f,
                        onGround = false
                    )
                } else {
                    null
                }
                val breakParticlesPacket = if (removed.hit) {
                    PlayPackets.entityEventPacket(
                        entityId = removed.entityId,
                        eventId = ENTITY_EVENT_THROWN_ITEM_BREAK_PARTICLES
                    )
                } else {
                    null
                }
                val packet = PlayPackets.removeEntitiesPacket(intArrayOf(removed.entityId))
                for (session in worldSessions) {
                    val wasVisible = session.visibleThrownItemEntityIds.remove(removed.entityId)
                    val inLoadedChunk = session.loadedChunks.contains(chunkPos)
                    if (!wasVisible && !inLoadedChunk) continue
                    session.thrownItemTrackerStates.remove(removed.entityId)
                    val ctx = contexts[session.channelId] ?: continue
                    if (!ctx.channel().isActive) continue
                    if (impactPacket != null) {
                        enqueueDroppedItemPacket(outboundByContext, ctx, impactPacket)
                    }
                    if (breakParticlesPacket != null) {
                        enqueueDroppedItemPacket(outboundByContext, ctx, breakParticlesPacket)
                    }
                    enqueueDroppedItemPacket(outboundByContext, ctx, packet)
                }
            }
            for (snapshot in thrownSpawnedByChunk[chunkPos].orEmpty()) {
                syncThrownItemSnapshot(
                    snapshot = snapshot,
                    worldSessions = worldSessions,
                    outboundByContext = outboundByContext,
                    sendPositionUpdate = false,
                    deltaSeconds = deltaSeconds
                )
                if (snapshot.ownerEntityId >= 0 && worldSessions.any { it.entityId == snapshot.ownerEntityId }) {
                    val soundPacket = thrownItemLaunchSoundPacket(
                        kind = snapshot.kind,
                        x = snapshot.x,
                        y = snapshot.y,
                        z = snapshot.z
                    )
                    for (session in worldSessions) {
                        val ctx = contexts[session.channelId] ?: continue
                        if (!ctx.channel().isActive) continue
                        enqueueDroppedItemPacket(outboundByContext, ctx, soundPacket)
                    }
                }
            }
            for (snapshot in thrownUpdatedByChunk[chunkPos].orEmpty()) {
                syncThrownItemSnapshot(
                    snapshot = snapshot,
                    worldSessions = worldSessions,
                    outboundByContext = outboundByContext,
                    sendPositionUpdate = true,
                    deltaSeconds = deltaSeconds
                )
            }

            for (removed in animalRemovedByChunk[chunkPos].orEmpty()) {
                dismountAnimalRider(world.key, removed.entityId)
                val wasSaddled = saddledAnimalEntityIds.remove(removed.entityId)
                pigBoostStatesByAnimalEntityId.remove(removed.entityId)
                if (removed.died) {
                    spawnAnimalDeathDrops(world, removed, null, dropSaddle = wasSaddled)
                }
                val packet = PlayPackets.removeEntitiesPacket(intArrayOf(removed.entityId))
                for (session in worldSessions) {
                    if (!session.visibleAnimalEntityIds.contains(removed.entityId)) continue
                    val ctx = contexts[session.channelId] ?: continue
                    if (!ctx.channel().isActive) continue
                    if (removed.died) {
                        enqueueDroppedItemPacket(
                            outboundByContext,
                            ctx,
                            PlayPackets.entityEventPacket(removed.entityId, ENTITY_EVENT_DEATH_ANIMATION)
                        )
                        enqueueDroppedItemPacket(
                            outboundByContext,
                            ctx,
                            PlayPackets.soundPacketByKey(
                                soundKey = removed.kind.deathSoundKey,
                                soundSourceId = NEUTRAL_SOUND_SOURCE_ID,
                                x = removed.x,
                                y = removed.y + 0.5,
                                z = removed.z,
                                volume = 1.0f,
                                pitch = (0.9f + ThreadLocalRandom.current().nextFloat() * 0.2f),
                                seed = ThreadLocalRandom.current().nextLong()
                            )
                        )
                        scheduleAnimalRemoval(world.key, removed.entityId)
                        continue
                    }
                    session.visibleAnimalEntityIds.remove(removed.entityId)
                    session.animalTrackerStates.remove(removed.entityId)
                    enqueueDroppedItemPacket(outboundByContext, ctx, packet)
                }
            }
            for (snapshot in animalSpawnedByChunk[chunkPos].orEmpty()) {
                syncAnimalSnapshot(
                    snapshot = snapshot,
                    worldSessions = worldSessions,
                    outboundByContext = outboundByContext,
                    sendPositionUpdate = false,
                    deltaSeconds = deltaSeconds
                )
            }
            for (snapshot in animalUpdatedByChunk[chunkPos].orEmpty()) {
                syncAnimalSnapshot(
                    snapshot = snapshot,
                    worldSessions = worldSessions,
                    outboundByContext = outboundByContext,
                    sendPositionUpdate = true,
                    deltaSeconds = deltaSeconds
                )
            }
            for (damage in animalDamagedByChunk[chunkPos].orEmpty()) {
                val damageTypeId = when (damage.cause) {
                    AnimalDamageCause.FALL -> FALL_DAMAGE_TYPE_ID
                    AnimalDamageCause.PLAYER_ATTACK -> PLAYER_ATTACK_DAMAGE_TYPE_ID
                    AnimalDamageCause.GENERIC -> PLAYER_ATTACK_DAMAGE_TYPE_ID
                }
                val hurtAnimation = PlayPackets.hurtAnimationPacket(damage.entityId, 0f)
                val damageEvent = PlayPackets.damageEventPacket(damage.entityId, damageTypeId)
                val soundSnapshot = if (damage.died) null else world.animalSnapshot(damage.entityId)
                val damageSoundPackets = if (soundSnapshot != null) {
                    val packets = ArrayList<ByteArray>(2)
                    val soundKeys = animalDamageSoundKeys(damage)
                    for (soundKey in soundKeys) {
                        packets.add(
                            PlayPackets.soundPacketByKey(
                                soundKey = soundKey,
                                soundSourceId = NEUTRAL_SOUND_SOURCE_ID,
                                x = soundSnapshot.x,
                                y = soundSnapshot.y + soundSnapshot.hitboxHeight * 0.5,
                                z = soundSnapshot.z,
                                volume = 1.0f,
                                pitch = (0.9f + ThreadLocalRandom.current().nextFloat() * 0.2f),
                                seed = ThreadLocalRandom.current().nextLong()
                            )
                        )
                    }
                    packets
                } else {
                    emptyList()
                }
                for (session in worldSessions) {
                    if (!session.visibleAnimalEntityIds.contains(damage.entityId)) continue
                    val ctx = contexts[session.channelId] ?: continue
                    if (!ctx.channel().isActive) continue
                    enqueueDroppedItemPacket(outboundByContext, ctx, hurtAnimation)
                    enqueueDroppedItemPacket(outboundByContext, ctx, damageEvent)
                    for (soundPacket in damageSoundPackets) {
                        enqueueDroppedItemPacket(outboundByContext, ctx, soundPacket)
                    }
                }
            }
            for (ambient in animalAmbientByChunk[chunkPos].orEmpty()) {
                val soundPacket = PlayPackets.soundPacketByKey(
                    soundKey = ambient.kind.ambientSoundKey,
                    soundSourceId = NEUTRAL_SOUND_SOURCE_ID,
                    x = ambient.x,
                    y = ambient.y + ambient.hitboxHeight * 0.5,
                    z = ambient.z,
                    volume = 1.0f,
                    pitch = (0.9f + ThreadLocalRandom.current().nextFloat() * 0.2f),
                    seed = ThreadLocalRandom.current().nextLong()
                )
                for (session in worldSessions) {
                    if (!session.visibleAnimalEntityIds.contains(ambient.entityId)) continue
                    val ctx = contexts[session.channelId] ?: continue
                    if (!ctx.channel().isActive) continue
                    enqueueDroppedItemPacket(outboundByContext, ctx, soundPacket)
                }
            }
            flushQueuedPackets(outboundByContext)
        }
    }

    private fun flushQueuedPackets(outboundByContext: MutableMap<ChannelHandlerContext, MutableList<ByteArray>>) {
        if (outboundByContext.isEmpty()) return
        for ((ctx, packets) in outboundByContext) {
            if (packets.isEmpty()) continue
            for (packet in packets) {
                enqueueChannelFlushPacket(ctx, packet)
            }
        }
        outboundByContext.clear()
    }

    private fun enqueueChannelFlushPacket(ctx: ChannelHandlerContext, packet: ByteArray) {
        val channelId = ctx.channel().id()
        val state = channelFlushStates.computeIfAbsent(channelId) { ChannelFlushState() }
        state.pendingPackets.add(packet)
        scheduleChannelFlushDrain(ctx, state)
    }

    private fun scheduleChannelFlushDrain(ctx: ChannelHandlerContext, state: ChannelFlushState) {
        if (!state.scheduled.compareAndSet(false, true)) return
        ctx.executor().execute {
            try {
                if (!ctx.channel().isActive) {
                    state.pendingPackets.clear()
                    channelFlushStates.remove(ctx.channel().id(), state)
                    return@execute
                }
                while (true) {
                    var wroteAny = false
                    while (true) {
                        val packet = state.pendingPackets.poll() ?: break
                        ctx.write(packet)
                        wroteAny = true
                    }
                    if (wroteAny) {
                        ctx.flush()
                    }
                    state.scheduled.set(false)
                    if (!ctx.channel().isActive) {
                        state.pendingPackets.clear()
                        channelFlushStates.remove(ctx.channel().id(), state)
                        return@execute
                    }
                    if (state.pendingPackets.isEmpty()) {
                        return@execute
                    }
                    if (!state.scheduled.compareAndSet(false, true)) {
                        return@execute
                    }
                }
            } catch (_: Throwable) {
                state.scheduled.set(false)
            }
        }
    }

    private fun scheduleFluidEvents(
        tickSequence: Long,
        physicsDeltaSeconds: Double,
        context: TickSimulationContext
    ) {
        for ((worldKey, activeSimulationChunks) in context.activeChunksByWorld) {
            val world = WorldManager.world(worldKey)
            if (world == null) continue
            try {
                val chunkProcessingFrame = world.beginChunkProcessingFrame(tickSequence)
                val fluidEvents = world.tickFluids(
                    physicsDeltaSeconds,
                    activeSimulationChunks,
                    onChunkChanged = { changedList ->
                        if (changedList.isEmpty()) return@tickFluids
                        val fluidBreakSeeds = applyAndBroadcastFluidChanges(world, changedList)
                        if (fluidBreakSeeds.isEmpty()) return@tickFluids
                        scheduleFluidDependentBreakRounds(
                            world = world,
                            removedCenters = fluidBreakSeeds
                        ) {}
                    },
                    onDispatchComplete = {}
                ) { chunkPos, elapsedNanos ->
                    world.recordChunkProcessingNanos(chunkProcessingFrame, chunkPos, elapsedNanos, category = "fluid")
                }
                world.finishChunkProcessingFrame(
                    frame = chunkProcessingFrame,
                    activeChunks = activeSimulationChunks,
                    includeZeroForInactive = false,
                    accumulateIntoLast = true
                )
                if (fluidEvents.changed.isEmpty()) {
                    continue
                }
                val fluidBreakSeeds = applyAndBroadcastFluidChanges(world, fluidEvents.changed)
                if (fluidBreakSeeds.isEmpty()) {
                    continue
                }
                scheduleFluidDependentBreakRounds(
                    world = world,
                    removedCenters = fluidBreakSeeds
                ) {
                    // Completion for already-drained events is handled by tickFluids onDispatchComplete.
                }
            } catch (_: Throwable) {}
        }
    }

    private fun applyAndBroadcastFluidChanges(
        world: org.macaroon3145.world.World,
        changes: List<org.macaroon3145.world.FluidBlockChange>
    ): List<BlockPos> {
        if (changes.isEmpty()) return emptyList()
        val appliedSeeds = ArrayList<BlockPos>(changes.size)
        for (change in changes) {
            val reason = classifyFluidChangeReason(change.previousStateId, change.stateId)
            val proceed = org.macaroon3145.plugin.PluginSystem.beforeBlockChange(
                session = null,
                worldKey = world.key,
                x = change.x,
                y = change.y,
                z = change.z,
                previousStateId = change.previousStateId,
                changedStateId = change.stateId,
                reason = reason
            )
            val finalState = if (proceed) {
                maybeSpawnFluidBrokenBlockDrops(world, change)
                appliedSeeds.add(BlockPos(change.x, change.y, change.z))
                change.stateId
            } else {
                world.setBlockStateWithoutFluidUpdates(change.x, change.y, change.z, change.previousStateId)
                change.previousStateId
            }
            broadcastBlockChangeToLoadedPlayers(
                worldKey = world.key,
                chunkPos = change.chunkPos,
                x = change.x,
                y = change.y,
                z = change.z,
                stateId = finalState
            )
        }
        return appliedSeeds
    }

    private fun broadcastAppliedFluidChanges(worldKey: String, changes: List<org.macaroon3145.world.FluidBlockChange>) {
        if (changes.isEmpty()) return
        for (change in changes) {
            broadcastBlockChangeToLoadedPlayers(
                worldKey = worldKey,
                chunkPos = change.chunkPos,
                x = change.x,
                y = change.y,
                z = change.z,
                stateId = change.stateId
            )
        }
    }

    private fun markPushingWakeFromPlayerMovement(
        session: PlayerSession,
        previousX: Double,
        previousZ: Double,
        nextX: Double,
        nextZ: Double,
        horizontalMovementSq: Double
    ) {
        val previousChunk = ChunkPos(
            ChunkStreamingService.chunkXFromBlockX(previousX),
            ChunkStreamingService.chunkZFromBlockZ(previousZ)
        )
        val nextChunk = ChunkPos(
            ChunkStreamingService.chunkXFromBlockX(nextX),
            ChunkStreamingService.chunkZFromBlockZ(nextZ)
        )
        val knownChunk = lastPushingChunkByPlayerEntityId.put(session.entityId, nextChunk)
        val changedChunk = previousChunk != nextChunk || (knownChunk != null && knownChunk != nextChunk)
        val movedEnough = horizontalMovementSq >= PUSHING_WAKE_MIN_HORIZONTAL_MOVEMENT_SQ
        if (movedEnough) {
            animalSourceDirtyWorlds.add(session.worldKey)
        }
        if (!changedChunk && !movedEnough) return
        // Wake only the current owner chunk to avoid mirrored pushing work on adjacent chunks.
        markPushingChunksAwake(session.worldKey, nextChunk)
    }

    private fun markPushingChunksAwake(
        worldKey: String,
        centerChunk: ChunkPos,
    ) {
        val bucket = pushingWakeChunksByWorld.computeIfAbsent(worldKey) { ConcurrentHashMap.newKeySet() }
        bucket.add(centerChunk)
    }

    private fun markPushingWakeFromAnimalEvents(worldKey: String, events: AnimalTickEvents) {
        if (events.isEmpty()) return
        for (snapshot in events.spawned) {
            markPushingChunksAwake(worldKey, snapshot.chunkPos)
        }
        for (snapshot in events.updated) {
            markPushingChunksAwake(worldKey, snapshot.chunkPos)
        }
        for (removed in events.removed) {
            markPushingChunksAwake(worldKey, removed.chunkPos)
        }
    }

    private fun drainAwakePushingChunks(
        worldKey: String,
        activeSimulationChunks: Set<ChunkPos>
    ): Set<ChunkPos> {
        if (activeSimulationChunks.isEmpty()) return emptySet()
        val queued = pushingWakeChunksByWorld.remove(worldKey) ?: return emptySet()
        val out = HashSet<ChunkPos>()
        for (chunkPos in queued) {
            if (chunkPos in activeSimulationChunks) {
                out.add(chunkPos)
            }
        }
        return out
    }

    private fun collectContactPushingChunks(
        worldKey: String,
        activeSimulationChunks: Set<ChunkPos>
    ): Set<ChunkPos> {
        if (activeSimulationChunks.isEmpty() || pushingContactStates.isEmpty()) return emptySet()
        val out = HashSet<ChunkPos>()
        for ((key, state) in pushingContactStates) {
            if (key.worldKey != worldKey) continue
            val ownerChunk = state.ownerChunk
            if (ownerChunk in activeSimulationChunks) {
                out.add(ownerChunk)
            }
            val next = state.remainingTicks - 1
            if (next <= 0) {
                pushingContactStates.remove(key, state)
            } else {
                pushingContactStates.replace(key, state, state.copy(remainingTicks = next))
            }
        }
        return out
    }

    private fun updatePushingContactStates(touchedContacts: Map<PushingContactKey, ChunkPos>) {
        if (touchedContacts.isEmpty()) return
        for ((contactKey, ownerChunk) in touchedContacts) {
            pushingContactStates[contactKey] = PushingContactState(
                ownerChunk = ownerChunk,
                remainingTicks = PUSHING_CONTACT_STICKY_TICKS
            )
        }
    }

    private fun newPushingContactKey(worldKey: String, entityA: Int, entityB: Int): PushingContactKey {
        return if (entityA <= entityB) {
            PushingContactKey(worldKey, entityA, entityB)
        } else {
            PushingContactKey(worldKey, entityB, entityA)
        }
    }

    private fun prunePushingStateForInactiveWorlds(activeWorldKeys: Set<String>) {
        if (activeWorldKeys.isEmpty()) {
            pushingWakeChunksByWorld.clear()
            pushingContactStates.clear()
            return
        }
        val wakeKeys = pushingWakeChunksByWorld.keys.toList()
        for (worldKey in wakeKeys) {
            if (worldKey !in activeWorldKeys) {
                pushingWakeChunksByWorld.remove(worldKey)
            }
        }
        for ((contactKey, state) in pushingContactStates) {
            if (contactKey.worldKey !in activeWorldKeys) {
                pushingContactStates.remove(contactKey, state)
            }
        }
    }

    private fun scheduleEntityPushing(tickSequence: Long, context: TickSimulationContext, deltaSeconds: Double) {
        if (deltaSeconds <= 0.0) return
        if (context.sessionsByWorld.isEmpty()) {
            pushingChunkLastTickNanos.clear()
            pushingWakeChunksByWorld.clear()
            pushingContactStates.clear()
            lastPushingChunkByPlayerEntityId.clear()
            return
        }
        prunePushingStateForInactiveWorlds(context.sessionsByWorld.keys)
        if (pushingContactStates.isNotEmpty()) {
            // Contact-tracking optimization disabled: keep no sticky contacts.
            pushingContactStates.clear()
        }
        pruneInactiveWorldChunkTimingState(context.sessionsByWorld.keys, pushingChunkLastTickNanos)
        for ((worldKey, worldSessions) in context.sessionsByWorld) {
            val world = WorldManager.world(worldKey)
            if (world == null) continue
            val sessionsSnapshot = worldSessions.toList()
            val activeSimulationChunks = context.activeChunksByWorld[worldKey] ?: emptySet()
            try {
                if (sessionsSnapshot.isEmpty()) {
                    pruneIdleWorldChunkTimingState(worldKey, emptySet(), pushingChunkLastTickNanos)
                    continue
                }
                if (activeSimulationChunks.isEmpty()) {
                    pruneIdleWorldChunkTimingState(worldKey, emptySet(), pushingChunkLastTickNanos)
                    continue
                }

                val chunksToProcess = HashSet<ChunkPos>()
                chunksToProcess.addAll(drainAwakePushingChunks(worldKey, activeSimulationChunks))
                if (chunksToProcess.isEmpty()) {
                    pruneIdleWorldChunkTimingState(worldKey, emptySet(), pushingChunkLastTickNanos)
                    continue
                }

                val playersByChunk = HashMap<ChunkPos, MutableList<PlayerSession>>()
                val playerChunkByEntityId = HashMap<Int, ChunkPos>()
                for (session in sessionsSnapshot) {
                    if (session.dead || session.gameMode == GAME_MODE_SPECTATOR) continue
                    val chunkPos = ChunkPos(
                        ChunkStreamingService.chunkXFromBlockX(session.x),
                        ChunkStreamingService.chunkZFromBlockZ(session.z)
                    )
                    if (chunkPos !in activeSimulationChunks) continue
                    if (chunkPos !in chunksToProcess) continue
                    playersByChunk.computeIfAbsent(chunkPos) { ArrayList() }.add(session)
                    playerChunkByEntityId[session.entityId] = chunkPos
                }
                if (playersByChunk.isEmpty()) {
                    pruneIdleWorldChunkTimingState(worldKey, chunksToProcess, pushingChunkLastTickNanos)
                    continue
                }

                val animalChunkByEntityId = HashMap<Int, ChunkPos>()
                val animalsByChunk = HashMap<ChunkPos, MutableList<AnimalSnapshot>>()
                val playerInfluenceChunks = collectNeighborChunks(
                    centers = playersByChunk.keys,
                    allowed = activeSimulationChunks
                )
                for (chunkPos in playerInfluenceChunks) {
                    val animals = world.animalsInChunk(chunkPos.x, chunkPos.z)
                    if (animals.isEmpty()) continue
                    val bucket = animalsByChunk.computeIfAbsent(chunkPos) { ArrayList(animals.size) }
                    for (animal in animals) {
                        animalChunkByEntityId[animal.entityId] = chunkPos
                        bucket.add(animal)
                    }
                }

                val hasPlayerCandidates = playersByChunk.values.sumOf { it.size } >= 2
                val hasAnimalCandidates = animalsByChunk.isNotEmpty()
                if (!hasPlayerCandidates && !hasAnimalCandidates) {
                    pruneIdleWorldChunkTimingState(worldKey, emptySet(), pushingChunkLastTickNanos)
                    continue
                }

                pruneIdleWorldChunkTimingState(worldKey, chunksToProcess, pushingChunkLastTickNanos)

                val playerImpulseX = ConcurrentHashMap<Int, DoubleAdder>()
                val playerImpulseZ = ConcurrentHashMap<Int, DoubleAdder>()
                val animalImpulseX = ConcurrentHashMap<Int, DoubleAdder>()
                val animalImpulseZ = ConcurrentHashMap<Int, DoubleAdder>()
                val scheduledChunks = ArrayList<ChunkPos>(playersByChunk.size)
                for (chunkPos in playersByChunk.keys) {
                    val key = WorldChunkKey(worldKey, chunkPos)
                    if (tryAcquireWorldChunkInFlight(key, pushingChunkInFlight)) {
                        scheduledChunks.add(chunkPos)
                    }
                }
                if (scheduledChunks.isEmpty()) {
                    continue
                }
                val scheduledChunkSet = scheduledChunks.toHashSet()
                val chunkProcessingFrame = world.beginChunkProcessingFrame(tickSequence)
                val remaining = AtomicInteger(scheduledChunks.size)
                val finalized = AtomicBoolean(false)
                val finalizeIfDone = fun() {
                    if (!finalized.compareAndSet(false, true)) {
                        return
                    }
                    val playerImpulses = HashMap<Int, Pair<Double, Double>>()
                    for ((entityId, xAdder) in playerImpulseX) {
                        val x = xAdder.sum()
                        val z = playerImpulseZ[entityId]?.sum() ?: 0.0
                        if (x == 0.0 && z == 0.0) continue
                        playerImpulses[entityId] = x to z
                    }
                    val animalImpulses = HashMap<Int, Pair<Double, Double>>()
                    for ((entityId, xAdder) in animalImpulseX) {
                        val x = xAdder.sum()
                        val z = animalImpulseZ[entityId]?.sum() ?: 0.0
                        if (x == 0.0 && z == 0.0) continue
                        animalImpulses[entityId] = x to z
                    }
                    applyPushingImmediately(worldKey, playerImpulses, animalImpulses)
                    world.finishChunkProcessingFrame(
                        frame = chunkProcessingFrame,
                        activeChunks = chunksToProcess,
                        includeZeroForInactive = false,
                        accumulateIntoLast = true
                    )
                }

                for (chunkPos in scheduledChunks) {
                    world.submitOnChunkActor(chunkPos) {
                        val worldChunkKey = WorldChunkKey(worldKey, chunkPos)
                        val startedAt = System.nanoTime()
                        try {
                            val chunkDeltaSeconds = consumeChunkElapsedDeltaSeconds(
                                worldChunkKey = worldChunkKey,
                                fallbackSeconds = deltaSeconds,
                                lastTickMap = pushingChunkLastTickNanos
                            )
                            if (chunkDeltaSeconds <= 0.0) {
                                return@submitOnChunkActor
                            }
                            applyChunkScopedPushing(
                                chunkPos = chunkPos,
                                deltaSeconds = chunkDeltaSeconds,
                                playersByChunk = playersByChunk,
                                playerChunkByEntityId = playerChunkByEntityId,
                                animalsByChunk = animalsByChunk,
                                animalChunkByEntityId = animalChunkByEntityId,
                                scheduledChunks = scheduledChunkSet,
                                playerImpulseX = playerImpulseX,
                                playerImpulseZ = playerImpulseZ,
                                animalImpulseX = animalImpulseX,
                                animalImpulseZ = animalImpulseZ
                            )
                        } finally {
                            world.recordChunkProcessingNanos(
                                frame = chunkProcessingFrame,
                                chunkPos = chunkPos,
                                elapsedNanos = System.nanoTime() - startedAt,
                                category = "pushing"
                            )
                            releaseWorldChunkInFlight(worldChunkKey, pushingChunkInFlight)
                            if (remaining.decrementAndGet() == 0) {
                                finalizeIfDone()
                            }
                        }
                    }
                }
            } catch (_: Throwable) {}
        }
    }

    private fun applyChunkScopedPushing(
        chunkPos: ChunkPos,
        deltaSeconds: Double,
        playersByChunk: Map<ChunkPos, List<PlayerSession>>,
        playerChunkByEntityId: Map<Int, ChunkPos>,
        animalsByChunk: Map<ChunkPos, List<AnimalSnapshot>>,
        animalChunkByEntityId: Map<Int, ChunkPos>,
        scheduledChunks: Set<ChunkPos>,
        playerImpulseX: ConcurrentHashMap<Int, DoubleAdder>,
        playerImpulseZ: ConcurrentHashMap<Int, DoubleAdder>,
        animalImpulseX: ConcurrentHashMap<Int, DoubleAdder>,
        animalImpulseZ: ConcurrentHashMap<Int, DoubleAdder>
    ) {
        val chunkPlayers = playersByChunk[chunkPos].orEmpty()
        val nearbyPlayers = collectNeighborPlayers(chunkPos, playersByChunk)
        val nearbyAnimals = collectNeighborAnimals(chunkPos, animalsByChunk)
        val nearbyAnimalSpatialIndex = buildAnimalSpatialIndex(nearbyAnimals)
        val processedPlayerPairs = HashSet<Long>()

        for (a in chunkPlayers) {
            if (a.dead || a.gameMode == GAME_MODE_SPECTATOR) continue
            val aChunk = playerChunkByEntityId[a.entityId] ?: continue
            val aMinY = a.y
            val aMaxY = a.y + playerCollisionHeight(a)
            for (b in nearbyPlayers) {
                if (a.entityId == b.entityId) continue
                if (b.dead || b.gameMode == GAME_MODE_SPECTATOR) continue
                val bChunk = playerChunkByEntityId[b.entityId] ?: continue
                if (ownerChunkForPair(aChunk, bChunk, scheduledChunks) != chunkPos) continue
                val pairKey = packEntityPair(a.entityId, b.entityId)
                if (!processedPlayerPairs.add(pairKey)) continue
                val bMinY = b.y
                val bMaxY = b.y + playerCollisionHeight(b)
                if (aMaxY <= bMinY || aMinY >= bMaxY) continue
                applyHorizontalPushPair(
                    ax = a.x, az = a.z, ar = PLAYER_HITBOX_HALF_WIDTH,
                    bx = b.x, bz = b.z, br = PLAYER_HITBOX_HALF_WIDTH,
                    deltaSeconds = deltaSeconds
                ) { pushAX, pushAZ, pushBX, pushBZ ->
                    addImpulse(playerImpulseX, playerImpulseZ, a.entityId, pushAX, pushAZ)
                    addImpulse(playerImpulseX, playerImpulseZ, b.entityId, pushBX, pushBZ)
                }
            }
        }

        for (player in chunkPlayers) {
            if (player.dead || player.gameMode == GAME_MODE_SPECTATOR) continue
            val playerMinY = player.y
            val playerMaxY = player.y + playerCollisionHeight(player)
            val playerChunk = playerChunkByEntityId[player.entityId] ?: continue
            val candidateAnimals = candidateAnimalsForPlayer(player, nearbyAnimalSpatialIndex)
            for (animal in candidateAnimals) {
                val animalChunk = animalChunkByEntityId[animal.entityId] ?: continue
                if (ownerChunkForPair(playerChunk, animalChunk, scheduledChunks) != chunkPos) continue
                val animalMinY = animal.y
                val animalMaxY = animal.y + animal.hitboxHeight
                if (playerMaxY <= animalMinY || playerMinY >= animalMaxY) continue
                applyHorizontalPushPair(
                    ax = player.x, az = player.z, ar = PLAYER_HITBOX_HALF_WIDTH,
                    bx = animal.x, bz = animal.z, br = animal.hitboxWidth * 0.5,
                    deltaSeconds = deltaSeconds
                ) { _, _, pushAX, pushAZ ->
                    addImpulse(animalImpulseX, animalImpulseZ, animal.entityId, pushAX, pushAZ)
                }
            }
        }

        applyAnimalAnimalPushingInChunk(
            chunkPos = chunkPos,
            deltaSeconds = deltaSeconds,
            nearbyIndex = nearbyAnimalSpatialIndex,
            animalChunkByEntityId = animalChunkByEntityId,
            scheduledChunks = scheduledChunks,
            animalImpulseX = animalImpulseX,
            animalImpulseZ = animalImpulseZ
        )
    }

    private data class AnimalSpatialIndex(
        val byCell: Map<Long, MutableList<AnimalSnapshot>>,
        val maxHalfWidth: Double
    )

    private fun buildAnimalSpatialIndex(animals: List<AnimalSnapshot>): AnimalSpatialIndex {
        if (animals.isEmpty()) return AnimalSpatialIndex(emptyMap(), 0.0)
        val byCell = HashMap<Long, MutableList<AnimalSnapshot>>()
        var maxHalfWidth = 0.0
        for (animal in animals) {
            val halfWidth = (animal.hitboxWidth * 0.5).coerceAtLeast(0.0)
            if (halfWidth > maxHalfWidth) {
                maxHalfWidth = halfWidth
            }
            val cellX = floor(animal.x / PUSHING_SPATIAL_CELL_SIZE).toInt()
            val cellZ = floor(animal.z / PUSHING_SPATIAL_CELL_SIZE).toInt()
            val key = packSpatialCell(cellX, cellZ)
            byCell.computeIfAbsent(key) { ArrayList() }.add(animal)
        }
        return AnimalSpatialIndex(byCell = byCell, maxHalfWidth = maxHalfWidth)
    }

    private fun candidateAnimalsForPlayer(
        player: PlayerSession,
        index: AnimalSpatialIndex
    ): List<AnimalSnapshot> {
        if (index.byCell.isEmpty()) return emptyList()
        val reach = PLAYER_HITBOX_HALF_WIDTH + index.maxHalfWidth + PUSHING_SPATIAL_REACH_MARGIN
        val minCellX = floor((player.x - reach) / PUSHING_SPATIAL_CELL_SIZE).toInt()
        val maxCellX = floor((player.x + reach) / PUSHING_SPATIAL_CELL_SIZE).toInt()
        val minCellZ = floor((player.z - reach) / PUSHING_SPATIAL_CELL_SIZE).toInt()
        val maxCellZ = floor((player.z + reach) / PUSHING_SPATIAL_CELL_SIZE).toInt()
        val out = ArrayList<AnimalSnapshot>()
        for (cellX in minCellX..maxCellX) {
            for (cellZ in minCellZ..maxCellZ) {
                val bucket = index.byCell[packSpatialCell(cellX, cellZ)] ?: continue
                out.addAll(bucket)
            }
        }
        return out
    }

    private fun applyAnimalAnimalPushingInChunk(
        chunkPos: ChunkPos,
        deltaSeconds: Double,
        nearbyIndex: AnimalSpatialIndex,
        animalChunkByEntityId: Map<Int, ChunkPos>,
        scheduledChunks: Set<ChunkPos>,
        animalImpulseX: ConcurrentHashMap<Int, DoubleAdder>,
        animalImpulseZ: ConcurrentHashMap<Int, DoubleAdder>
    ) {
        val byCell = nearbyIndex.byCell
        if (byCell.isEmpty()) return
        val reach = (nearbyIndex.maxHalfWidth * 2.0) + PUSHING_SPATIAL_REACH_MARGIN
        val cellRadius = ceil(reach / PUSHING_SPATIAL_CELL_SIZE).toInt().coerceAtLeast(1)

        for ((cellKey, cellAnimals) in byCell) {
            val cellX = (cellKey shr 32).toInt()
            val cellZ = cellKey.toInt()
            for (dx in 0..cellRadius) {
                for (dz in -cellRadius..cellRadius) {
                    if (dx == 0 && dz < 0) continue
                    val neighbor = byCell[packSpatialCell(cellX + dx, cellZ + dz)] ?: continue
                    if (dx == 0 && dz == 0) {
                        for (i in 0 until cellAnimals.size) {
                            val a = cellAnimals[i]
                            for (j in (i + 1) until cellAnimals.size) {
                                val b = cellAnimals[j]
                                applyAnimalPushPair(
                                    ownerChunk = chunkPos,
                                    deltaSeconds = deltaSeconds,
                                    a = a,
                                    b = b,
                                    animalChunkByEntityId = animalChunkByEntityId,
                                    scheduledChunks = scheduledChunks,
                                    animalImpulseX = animalImpulseX,
                                    animalImpulseZ = animalImpulseZ
                                )
                            }
                        }
                    } else {
                        for (a in cellAnimals) {
                            for (b in neighbor) {
                                applyAnimalPushPair(
                                    ownerChunk = chunkPos,
                                    deltaSeconds = deltaSeconds,
                                    a = a,
                                    b = b,
                                    animalChunkByEntityId = animalChunkByEntityId,
                                    scheduledChunks = scheduledChunks,
                                    animalImpulseX = animalImpulseX,
                                    animalImpulseZ = animalImpulseZ
                                )
                            }
                        }
                    }
                }
            }
        }
    }

    private fun applyAnimalPushPair(
        ownerChunk: ChunkPos,
        deltaSeconds: Double,
        a: AnimalSnapshot,
        b: AnimalSnapshot,
        animalChunkByEntityId: Map<Int, ChunkPos>,
        scheduledChunks: Set<ChunkPos>,
        animalImpulseX: ConcurrentHashMap<Int, DoubleAdder>,
        animalImpulseZ: ConcurrentHashMap<Int, DoubleAdder>
    ) {
        if (a.entityId == b.entityId) return
        val aChunk = animalChunkByEntityId[a.entityId] ?: return
        val bChunk = animalChunkByEntityId[b.entityId] ?: return
        if (ownerChunkForPair(aChunk, bChunk, scheduledChunks) != ownerChunk) return
        val aMinY = a.y
        val aMaxY = a.y + a.hitboxHeight
        val bMinY = b.y
        val bMaxY = b.y + b.hitboxHeight
        if (aMaxY <= bMinY || aMinY >= bMaxY) return
        applyHorizontalPushPair(
            ax = a.x, az = a.z, ar = a.hitboxWidth * 0.5,
            bx = b.x, bz = b.z, br = b.hitboxWidth * 0.5,
            deltaSeconds = deltaSeconds
        ) { pushAX, pushAZ, pushBX, pushBZ ->
            addImpulse(animalImpulseX, animalImpulseZ, a.entityId, pushAX, pushAZ)
            addImpulse(animalImpulseX, animalImpulseZ, b.entityId, pushBX, pushBZ)
        }
    }

    private fun packSpatialCell(cellX: Int, cellZ: Int): Long {
        return (cellX.toLong() shl 32) xor (cellZ.toLong() and 0xFFFFFFFFL)
    }

    private fun packEntityPair(entityA: Int, entityB: Int): Long {
        val low = min(entityA, entityB).toLong() and 0xFFFF_FFFFL
        val high = max(entityA, entityB).toLong() and 0xFFFF_FFFFL
        return (low shl 32) or high
    }

    private fun addImpulse(
        mapX: ConcurrentHashMap<Int, DoubleAdder>,
        mapZ: ConcurrentHashMap<Int, DoubleAdder>,
        entityId: Int,
        impulseX: Double,
        impulseZ: Double
    ) {
        if (impulseX != 0.0) {
            mapX.computeIfAbsent(entityId) { DoubleAdder() }.add(impulseX)
        }
        if (impulseZ != 0.0) {
            mapZ.computeIfAbsent(entityId) { DoubleAdder() }.add(impulseZ)
        }
    }

    private fun collectNeighborPlayers(
        center: ChunkPos,
        playersByChunk: Map<ChunkPos, List<PlayerSession>>
    ): List<PlayerSession> {
        val out = ArrayList<PlayerSession>()
        for (dz in -1..1) {
            for (dx in -1..1) {
                out.addAll(playersByChunk[ChunkPos(center.x + dx, center.z + dz)].orEmpty())
            }
        }
        return out
    }

    private fun collectNeighborAnimals(
        center: ChunkPos,
        animalsByChunk: Map<ChunkPos, List<AnimalSnapshot>>
    ): List<AnimalSnapshot> {
        val out = ArrayList<AnimalSnapshot>()
        for (dz in -1..1) {
            for (dx in -1..1) {
                out.addAll(animalsByChunk[ChunkPos(center.x + dx, center.z + dz)].orEmpty())
            }
        }
        return out
    }

    private fun collectNeighborChunks(
        centers: Set<ChunkPos>,
        allowed: Set<ChunkPos>
    ): Set<ChunkPos> {
        if (centers.isEmpty() || allowed.isEmpty()) return emptySet()
        val out = HashSet<ChunkPos>(centers.size * 4)
        for (center in centers) {
            for (dz in -1..1) {
                for (dx in -1..1) {
                    val chunkPos = ChunkPos(center.x + dx, center.z + dz)
                    if (chunkPos in allowed) {
                        out.add(chunkPos)
                    }
                }
            }
        }
        return out
    }

    private fun ownerChunkForPair(a: ChunkPos, b: ChunkPos, scheduledChunks: Set<ChunkPos>): ChunkPos {
        val aScheduled = a in scheduledChunks
        val bScheduled = b in scheduledChunks
        if (aScheduled && !bScheduled) return a
        if (bScheduled && !aScheduled) return b
        return if (a.x < b.x || (a.x == b.x && a.z <= b.z)) a else b
    }

    private fun applyPushingImmediately(
        worldKey: String,
        playerImpulses: Map<Int, Pair<Double, Double>>,
        animalImpulses: Map<Int, Pair<Double, Double>>
    ) {
        if (playerImpulses.isNotEmpty()) {
            for ((_, session) in sessions) {
                if (session.worldKey != worldKey) continue
                val impulse = playerImpulses[session.entityId] ?: continue
                applyPlayerPushImpulse(session, impulse.first, impulse.second)
            }
        }
        if (animalImpulses.isNotEmpty()) {
            val world = WorldManager.world(worldKey) ?: return
            for ((entityId, impulse) in animalImpulses) {
                world.addAnimalHorizontalImpulse(entityId, impulse.first, impulse.second)
            }
        }
    }

    private fun selectAnchorChunk(chunks: Set<ChunkPos>): ChunkPos? {
        if (chunks.isEmpty()) return null
        var selected: ChunkPos? = null
        for (chunk in chunks) {
            val current = selected
            if (current == null || chunk.x < current.x || (chunk.x == current.x && chunk.z < current.z)) {
                selected = chunk
            }
        }
        return selected
    }

    private fun collectThrownItemPlayerHits(
        world: org.macaroon3145.world.World,
        worldSessions: List<PlayerSession>,
        snapshots: List<ThrownItemSnapshot>,
        removedOut: MutableList<org.macaroon3145.world.ThrownItemRemovedEvent>,
        removedIds: MutableSet<Int>
    ) {
        for (snapshot in snapshots) {
            if (snapshot.entityId in removedIds) continue
            val current = world.thrownItemSnapshot(snapshot.entityId) ?: continue
            var hitTarget: PlayerSession? = null
            for (target in worldSessions) {
                if (target.entityId == current.ownerEntityId) continue
                if (target.dead) continue
                if (target.gameMode == GAME_MODE_SPECTATOR) continue
                if (!thrownItemHitsPlayer(current, target)) continue
                hitTarget = target
                break
            }
            if (hitTarget == null) continue
            val (hitX, hitY, hitZ) = resolveThrownItemImpactOnPlayer(current, hitTarget)
            val removed = world.removeThrownItem(
                entityId = current.entityId,
                hit = true,
                x = hitX,
                y = hitY,
                z = hitZ
            ) ?: continue
            removedIds.add(current.entityId)
            removedOut.add(removed)
        }
    }

    private fun collectThrownItemEntityHits(
        world: org.macaroon3145.world.World,
        worldSessions: List<PlayerSession>,
        snapshots: List<ThrownItemSnapshot>,
        removedOut: MutableList<org.macaroon3145.world.ThrownItemRemovedEvent>,
        removedIds: MutableSet<Int>
    ) {
        for (snapshot in snapshots) {
            if (snapshot.entityId in removedIds) continue
            val current = world.thrownItemSnapshot(snapshot.entityId) ?: continue

            var bestT: Double? = null
            var bestHitX = 0.0
            var bestHitY = 0.0
            var bestHitZ = 0.0

            for (target in worldSessions) {
                if (target.entityId == current.ownerEntityId) continue
                if (target.dead) continue
                if (target.gameMode == GAME_MODE_SPECTATOR) continue
                val t = thrownItemEntryTAgainstPlayer(current, target) ?: continue
                if (bestT == null || t < bestT) {
                    bestT = t
                    val hit = resolveThrownItemImpactAtT(current, t)
                    bestHitX = hit.first
                    bestHitY = hit.second
                    bestHitZ = hit.third
                }
            }

            val animalCandidates = collectCandidateAnimalsForThrown(world, current)
            for (animal in animalCandidates) {
                val t = thrownItemEntryTAgainstAnimal(current, animal) ?: continue
                if (bestT == null || t < bestT) {
                    bestT = t
                    val hit = resolveThrownItemImpactAtT(current, t)
                    bestHitX = hit.first
                    bestHitY = hit.second
                    bestHitZ = hit.third
                }
            }

            if (bestT == null) continue
            val removed = world.removeThrownItem(
                entityId = current.entityId,
                hit = true,
                x = bestHitX,
                y = bestHitY,
                z = bestHitZ
            ) ?: continue
            removedIds.add(current.entityId)
            removedOut.add(removed)
        }
    }

    private fun thrownItemEntryTAgainstPlayer(snapshot: ThrownItemSnapshot, target: PlayerSession): Double? {
        val hitbox = expandedPlayerHitbox(target)
        return segmentAabbEntryTime(
            x0 = snapshot.prevX,
            y0 = snapshot.prevY,
            z0 = snapshot.prevZ,
            x1 = snapshot.x,
            y1 = snapshot.y,
            z1 = snapshot.z,
            minX = hitbox[0],
            maxX = hitbox[1],
            minY = hitbox[2],
            maxY = hitbox[3],
            minZ = hitbox[4],
            maxZ = hitbox[5]
        )
    }

    private fun thrownItemEntryTAgainstAnimal(snapshot: ThrownItemSnapshot, target: AnimalSnapshot): Double? {
        val hitbox = expandedAnimalHitbox(target)
        return segmentAabbEntryTime(
            x0 = snapshot.prevX,
            y0 = snapshot.prevY,
            z0 = snapshot.prevZ,
            x1 = snapshot.x,
            y1 = snapshot.y,
            z1 = snapshot.z,
            minX = hitbox[0],
            maxX = hitbox[1],
            minY = hitbox[2],
            maxY = hitbox[3],
            minZ = hitbox[4],
            maxZ = hitbox[5]
        )
    }

    private fun resolveThrownItemImpactAtT(snapshot: ThrownItemSnapshot, t: Double): Triple<Double, Double, Double> {
        val clampedT = t.coerceIn(0.0, 1.0)
        return Triple(
            snapshot.prevX + (snapshot.x - snapshot.prevX) * clampedT,
            snapshot.prevY + (snapshot.y - snapshot.prevY) * clampedT,
            snapshot.prevZ + (snapshot.z - snapshot.prevZ) * clampedT
        )
    }

    private fun collectThrownItemAnimalHits(
        world: org.macaroon3145.world.World,
        snapshots: List<ThrownItemSnapshot>,
        removedOut: MutableList<org.macaroon3145.world.ThrownItemRemovedEvent>,
        removedIds: MutableSet<Int>
    ) {
        for (snapshot in snapshots) {
            if (snapshot.entityId in removedIds) continue
            val current = world.thrownItemSnapshot(snapshot.entityId) ?: continue
            val candidates = collectCandidateAnimalsForThrown(world, current)
            var hitTarget: AnimalSnapshot? = null
            for (animal in candidates) {
                if (!thrownItemHitsAnimal(current, animal)) continue
                hitTarget = animal
                break
            }
            if (hitTarget == null) continue
            val (hitX, hitY, hitZ) = resolveThrownItemImpactOnAnimal(current, hitTarget)
            val removed = world.removeThrownItem(
                entityId = current.entityId,
                hit = true,
                x = hitX,
                y = hitY,
                z = hitZ
            ) ?: continue
            removedIds.add(current.entityId)
            removedOut.add(removed)
        }
    }

    private fun collectCandidateAnimalsForThrown(
        world: org.macaroon3145.world.World,
        snapshot: ThrownItemSnapshot
    ): List<AnimalSnapshot> {
        val minChunkX = min(
            ChunkStreamingService.chunkXFromBlockX(snapshot.prevX),
            ChunkStreamingService.chunkXFromBlockX(snapshot.x)
        )
        val maxChunkX = max(
            ChunkStreamingService.chunkXFromBlockX(snapshot.prevX),
            ChunkStreamingService.chunkXFromBlockX(snapshot.x)
        )
        val minChunkZ = min(
            ChunkStreamingService.chunkZFromBlockZ(snapshot.prevZ),
            ChunkStreamingService.chunkZFromBlockZ(snapshot.z)
        )
        val maxChunkZ = max(
            ChunkStreamingService.chunkZFromBlockZ(snapshot.prevZ),
            ChunkStreamingService.chunkZFromBlockZ(snapshot.z)
        )
        val byEntityId = HashMap<Int, AnimalSnapshot>()
        for (chunkX in (minChunkX - 1)..(maxChunkX + 1)) {
            for (chunkZ in (minChunkZ - 1)..(maxChunkZ + 1)) {
                val animals = world.animalsInChunk(chunkX, chunkZ)
                for (animal in animals) {
                    byEntityId.putIfAbsent(animal.entityId, animal)
                }
            }
        }
        return byEntityId.values.toList()
    }

    private fun processEnderPearlImpacts(
        worldKey: String,
        removedEvents: List<org.macaroon3145.world.ThrownItemRemovedEvent>
    ) {
        for (removed in removedEvents) {
            if (!removed.hit) continue
            if (removed.kind != ThrownItemKind.ENDER_PEARL) continue
            val owner = sessions.values.firstOrNull {
                it.worldKey == worldKey && it.entityId == removed.ownerEntityId
            } ?: continue
            if (owner.dead) continue
            if (owner.gameMode == GAME_MODE_SPECTATOR) continue
            val safeTeleport = resolveSafeEnderPearlTeleport(owner, removed)
            if (!teleportPlayer(
                    owner,
                    safeTeleport.first,
                    safeTeleport.second,
                    safeTeleport.third,
                    null,
                    null,
                    reason = MoveReason.ENDER_PEARL
                )
            ) continue
            broadcastEnderPearlTeleportSound(worldKey, safeTeleport.first, safeTeleport.second, safeTeleport.third)
            damagePlayer(
                session = owner,
                amount = ENDER_PEARL_TELEPORT_DAMAGE,
                damageTypeId = FALL_DAMAGE_TYPE_ID,
                hurtSoundId = PLAYER_SMALL_FALL_SOUND_ID,
                bigHurtSoundId = PLAYER_BIG_FALL_SOUND_ID,
                deathMessage = PlayPackets.ChatComponent.Translate(
                    key = "death.fell.accident.generic",
                    args = listOf(playerNameComponent(owner))
                )
            )
        }
    }

    private fun resolveSafeEnderPearlTeleport(
        session: PlayerSession,
        impact: org.macaroon3145.world.ThrownItemRemovedEvent
    ): Triple<Double, Double, Double> {
        val hitAxis = impact.hitAxis ?: return Triple(impact.x, impact.y, impact.z)
        val hitDirection = impact.hitDirection ?: return Triple(impact.x, impact.y, impact.z)
        val hitBoundary = impact.hitBoundary ?: return Triple(impact.x, impact.y, impact.z)
        val face = hitBoundary + when (hitDirection) {
            1 -> THROWN_ITEM_HITBOX_RADIUS
            -1 -> -THROWN_ITEM_HITBOX_RADIUS
            else -> 0.0
        }
        val height = playerPoseHeight(session)
        val halfWidth = PLAYER_HITBOX_HALF_WIDTH
        return when (hitAxis) {
            0 -> {
                val x = if (hitDirection > 0) face - halfWidth else face + halfWidth
                Triple(x, impact.y, impact.z)
            }
            1 -> {
                val y = if (hitDirection > 0) face - height else face
                Triple(impact.x, y, impact.z)
            }
            2 -> {
                val z = if (hitDirection > 0) face - halfWidth else face + halfWidth
                Triple(impact.x, impact.y, z)
            }
            else -> Triple(impact.x, impact.y, impact.z)
        }
    }

    private fun broadcastEnderPearlTeleportSound(worldKey: String, x: Double, y: Double, z: Double) {
        val packet = PlayPackets.soundPacketByKey(
            soundKey = "minecraft:entity.player.teleport",
            soundSourceId = PLAYERS_SOUND_SOURCE_ID,
            x = x,
            y = y,
            z = z,
            volume = 1.0f,
            pitch = 1.0f,
            seed = ThreadLocalRandom.current().nextLong()
        )
        for ((id, other) in sessions) {
            if (other.worldKey != worldKey) continue
            val ctx = contexts[id] ?: continue
            if (!ctx.channel().isActive) continue
            ctx.write(packet)
            ctx.flush()
        }
    }

    private fun resolveThrownItemImpactOnPlayer(
        snapshot: ThrownItemSnapshot,
        target: PlayerSession
    ): Triple<Double, Double, Double> {
        val hitbox = expandedPlayerHitbox(target)
        val minX = hitbox[0]
        val maxX = hitbox[1]
        val minY = hitbox[2]
        val maxY = hitbox[3]
        val minZ = hitbox[4]
        val maxZ = hitbox[5]
        val entryT = segmentAabbEntryTime(
            x0 = snapshot.prevX,
            y0 = snapshot.prevY,
            z0 = snapshot.prevZ,
            x1 = snapshot.x,
            y1 = snapshot.y,
            z1 = snapshot.z,
            minX = minX,
            maxX = maxX,
            minY = minY,
            maxY = maxY,
            minZ = minZ,
            maxZ = maxZ
        )
        val clampedT = entryT?.coerceIn(0.0, 1.0) ?: 1.0
        val x = snapshot.prevX + (snapshot.x - snapshot.prevX) * clampedT
        val y = snapshot.prevY + (snapshot.y - snapshot.prevY) * clampedT
        val z = snapshot.prevZ + (snapshot.z - snapshot.prevZ) * clampedT
        return Triple(x.coerceIn(minX, maxX), y.coerceIn(minY, maxY), z.coerceIn(minZ, maxZ))
    }

    private fun resolveThrownItemImpactOnAnimal(
        snapshot: ThrownItemSnapshot,
        target: AnimalSnapshot
    ): Triple<Double, Double, Double> {
        val hitbox = expandedAnimalHitbox(target)
        val minX = hitbox[0]
        val maxX = hitbox[1]
        val minY = hitbox[2]
        val maxY = hitbox[3]
        val minZ = hitbox[4]
        val maxZ = hitbox[5]
        val entryT = segmentAabbEntryTime(
            x0 = snapshot.prevX,
            y0 = snapshot.prevY,
            z0 = snapshot.prevZ,
            x1 = snapshot.x,
            y1 = snapshot.y,
            z1 = snapshot.z,
            minX = minX,
            maxX = maxX,
            minY = minY,
            maxY = maxY,
            minZ = minZ,
            maxZ = maxZ
        )
        val clampedT = entryT?.coerceIn(0.0, 1.0) ?: 1.0
        val x = snapshot.prevX + (snapshot.x - snapshot.prevX) * clampedT
        val y = snapshot.prevY + (snapshot.y - snapshot.prevY) * clampedT
        val z = snapshot.prevZ + (snapshot.z - snapshot.prevZ) * clampedT
        return Triple(x.coerceIn(minX, maxX), y.coerceIn(minY, maxY), z.coerceIn(minZ, maxZ))
    }

    private fun thrownItemHitsPlayer(snapshot: ThrownItemSnapshot, target: PlayerSession): Boolean {
        val hitbox = expandedPlayerHitbox(target)
        val minX = hitbox[0]
        val maxX = hitbox[1]
        val minY = hitbox[2]
        val maxY = hitbox[3]
        val minZ = hitbox[4]
        val maxZ = hitbox[5]
        if (snapshot.x in minX..maxX && snapshot.y in minY..maxY && snapshot.z in minZ..maxZ) return true
        return segmentAabbEntryTime(
            x0 = snapshot.prevX,
            y0 = snapshot.prevY,
            z0 = snapshot.prevZ,
            x1 = snapshot.x,
            y1 = snapshot.y,
            z1 = snapshot.z,
            minX = minX,
            maxX = maxX,
            minY = minY,
            maxY = maxY,
            minZ = minZ,
            maxZ = maxZ
        ) != null
    }

    private fun thrownItemHitsAnimal(snapshot: ThrownItemSnapshot, target: AnimalSnapshot): Boolean {
        val hitbox = expandedAnimalHitbox(target)
        val minX = hitbox[0]
        val maxX = hitbox[1]
        val minY = hitbox[2]
        val maxY = hitbox[3]
        val minZ = hitbox[4]
        val maxZ = hitbox[5]
        if (snapshot.x in minX..maxX && snapshot.y in minY..maxY && snapshot.z in minZ..maxZ) return true
        return segmentAabbEntryTime(
            x0 = snapshot.prevX,
            y0 = snapshot.prevY,
            z0 = snapshot.prevZ,
            x1 = snapshot.x,
            y1 = snapshot.y,
            z1 = snapshot.z,
            minX = minX,
            maxX = maxX,
            minY = minY,
            maxY = maxY,
            minZ = minZ,
            maxZ = maxZ
        ) != null
    }

    private fun expandedPlayerHitbox(target: PlayerSession): DoubleArray {
        val aabb = playerAabb(
            session = target,
            expandHorizontal = THROWN_ITEM_HITBOX_RADIUS,
            expandDown = THROWN_ITEM_HITBOX_RADIUS,
            expandUp = THROWN_ITEM_HITBOX_RADIUS
        )
        return aabb.toDoubleArray()
    }

    private fun expandedAnimalHitbox(target: AnimalSnapshot): DoubleArray {
        val aabb = animalAabb(
            target = target,
            expandHorizontal = THROWN_ITEM_HITBOX_RADIUS,
            expandDown = THROWN_ITEM_HITBOX_RADIUS,
            expandUp = THROWN_ITEM_HITBOX_RADIUS
        )
        return aabb.toDoubleArray()
    }

    private fun playerCollisionAt(
        world: org.macaroon3145.world.World,
        x: Double,
        y: Double,
        z: Double,
        halfWidth: Double,
        height: Double
    ): Boolean {
        val playerBox = playerAabbAt(x, y, z, halfWidth, height)
        val minX = playerBox.minX
        val maxX = playerBox.maxX
        val minY = playerBox.minY
        val maxY = playerBox.maxY
        val minZ = playerBox.minZ
        val maxZ = playerBox.maxZ

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
                    if (parsed.blockKey in NON_SUPPORT_BLOCK_KEYS) continue
                    val resolved = BlockCollisionRegistry.boxesForStateId(stateId, parsed)
                    if (resolved == null) {
                        if (aabbOverlap(
                                playerBox,
                                Aabb(
                                    minX = bx.toDouble(),
                                    minY = by.toDouble(),
                                    minZ = bz.toDouble(),
                                    maxX = bx + 1.0,
                                    maxY = by + 1.0,
                                    maxZ = bz + 1.0
                                )
                            )
                        ) {
                            return true
                        }
                        continue
                    }
                    for (collisionBox in resolved) {
                        val boxMinX = bx + collisionBox.minX
                        val boxMinY = by + collisionBox.minY
                        val boxMinZ = bz + collisionBox.minZ
                        val boxMaxX = bx + collisionBox.maxX
                        val boxMaxY = by + collisionBox.maxY
                        val boxMaxZ = bz + collisionBox.maxZ
                        if (aabbOverlap(
                                playerBox,
                                Aabb(
                                    minX = boxMinX,
                                    minY = boxMinY,
                                    minZ = boxMinZ,
                                    maxX = boxMaxX,
                                    maxY = boxMaxY,
                                    maxZ = boxMaxZ
                                )
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
        a: Aabb,
        b: Aabb
    ): Boolean {
        return aabbOverlap(
            a.minX, a.minY, a.minZ,
            a.maxX, a.maxY, a.maxZ,
            b.minX, b.minY, b.minZ,
            b.maxX, b.maxY, b.maxZ
        )
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

    private fun segmentAabbEntryTime(
        x0: Double,
        y0: Double,
        z0: Double,
        x1: Double,
        y1: Double,
        z1: Double,
        minX: Double,
        maxX: Double,
        minY: Double,
        maxY: Double,
        minZ: Double,
        maxZ: Double
    ): Double? {
        val epsilon = 1.0e-8
        var tMin = 0.0
        var tMax = 1.0

        fun clip(p0: Double, p1: Double, min: Double, max: Double): Boolean {
            val d = p1 - p0
            if (kotlin.math.abs(d) <= epsilon) {
                return p0 in min..max
            }
            val inv = 1.0 / d
            var t1 = (min - p0) * inv
            var t2 = (max - p0) * inv
            if (t1 > t2) {
                val tmp = t1
                t1 = t2
                t2 = tmp
            }
            if (t2 < tMin || t1 > tMax) return false
            if (t1 > tMin) tMin = t1
            if (t2 < tMax) tMax = t2
            return tMin <= tMax
        }

        if (!clip(x0, x1, minX, maxX)) return null
        if (!clip(y0, y1, minY, maxY)) return null
        if (!clip(z0, z1, minZ, maxZ)) return null
        return tMin.coerceIn(0.0, 1.0)
    }

    private fun updateRetainedBaseWorldChunks(sessionsByWorld: Map<String, List<PlayerSession>>) {
        val connectedCount = sessionsByWorld.values.sumOf { it.size }
        if (retainedBaseChunksByPlayerEntityId.size > connectedCount) {
            val activePlayerEntityIds = HashSet<Int>(connectedCount)
            for (worldSessions in sessionsByWorld.values) {
                for (session in worldSessions) {
                    activePlayerEntityIds.add(session.entityId)
                }
            }
            val knownEntityIds = retainedBaseChunksByPlayerEntityId.keys.toList()
            for (entityId in knownEntityIds) {
                if (entityId in activePlayerEntityIds) continue
                val previous = retainedBaseChunksByPlayerEntityId.remove(entityId) ?: continue
                val worldKey = retainedBaseWorldKeyByPlayerEntityId.remove(entityId) ?: continue
                removeRetainedBaseFootprintRefs(worldKey, previous)
                publishRetainedBaseChunksForWorld(worldKey)
            }
        }
        pruneRetainedBaseChunkCaches(sessionsByWorld.keys)
    }

    private fun buildRetainedBaseChunkFootprint(session: PlayerSession): Set<ChunkPos> {
        if (session.targetChunks.isEmpty() && session.loadedChunks.isEmpty()) return emptySet()
        val footprint = HashSet<ChunkPos>(session.targetChunks.size + session.loadedChunks.size)
        footprint.addAll(session.targetChunks)
        footprint.addAll(session.loadedChunks)
        return footprint
    }

    private fun addRetainedBaseFootprintRefs(worldKey: String, footprint: Set<ChunkPos>) {
        if (footprint.isEmpty()) return
        val refs = retainedBaseRefCountByWorld.computeIfAbsent(worldKey) { ConcurrentHashMap() }
        for (chunkPos in footprint) {
            refs.compute(chunkPos) { _, count -> (count ?: 0) + 1 }
        }
    }

    private fun removeRetainedBaseFootprintRefs(worldKey: String, footprint: Set<ChunkPos>) {
        if (footprint.isEmpty()) return
        val refs = retainedBaseRefCountByWorld[worldKey] ?: return
        for (chunkPos in footprint) {
            refs.computeIfPresent(chunkPos) { _, count ->
                if (count <= 1) null else count - 1
            }
        }
        if (refs.isEmpty()) {
            retainedBaseRefCountByWorld.remove(worldKey, refs)
        }
    }

    private fun publishRetainedBaseChunksForWorld(worldKey: String) {
        val refs = retainedBaseRefCountByWorld[worldKey]
        val retained = if (refs == null || refs.isEmpty()) emptySet() else HashSet(refs.keys)
        val previous = retainedBaseChunksCacheByWorld[worldKey]
        if (previous == null || previous != retained) {
            FoliaSharedMemoryWorldGenerator.retainLoadedChunks(worldKey, retained)
            if (retained.isEmpty()) {
                retainedBaseChunksCacheByWorld.remove(worldKey)
            } else {
                retainedBaseChunksCacheByWorld[worldKey] = retained
            }
        }
    }

    private fun refreshRetainedBaseChunksForSession(session: PlayerSession) {
        val worldKey = session.worldKey
        synchronized(session) {
            val next = buildRetainedBaseChunkFootprint(session)
            val previous = retainedBaseChunksByPlayerEntityId[session.entityId]
            if (previous == next) {
                return
            }
            if (previous != null) {
                removeRetainedBaseFootprintRefs(worldKey, previous)
            }
            if (next.isEmpty()) {
                retainedBaseChunksByPlayerEntityId.remove(session.entityId)
                retainedBaseWorldKeyByPlayerEntityId.remove(session.entityId)
            } else {
                retainedBaseChunksByPlayerEntityId[session.entityId] = next
                retainedBaseWorldKeyByPlayerEntityId[session.entityId] = worldKey
                addRetainedBaseFootprintRefs(worldKey, next)
            }
        }
        publishRetainedBaseChunksForWorld(worldKey)
    }

    private fun removeRetainedBaseChunksForSession(session: PlayerSession) {
        val worldKey = session.worldKey
        synchronized(session) {
            val previous = retainedBaseChunksByPlayerEntityId.remove(session.entityId) ?: return
            retainedBaseWorldKeyByPlayerEntityId.remove(session.entityId)
            removeRetainedBaseFootprintRefs(worldKey, previous)
        }
        publishRetainedBaseChunksForWorld(worldKey)
    }

    private fun buildSimulationChunkFootprint(centerChunkX: Int, centerChunkZ: Int, radius: Int): Set<ChunkPos> {
        val radiusSq = radius * radius
        val out = HashSet<ChunkPos>((radius * radius * 3).coerceAtLeast(16))
        for (dz in -radius..radius) {
            for (dx in -radius..radius) {
                if (dx * dx + dz * dz <= radiusSq) {
                    out.add(ChunkPos(centerChunkX + dx, centerChunkZ + dz))
                }
            }
        }
        return out
    }

    private fun addSimulationFootprintRefs(worldKey: String, footprint: Set<ChunkPos>) {
        if (footprint.isEmpty()) return
        val refs = activeSimulationRefCountByWorld.computeIfAbsent(worldKey) { ConcurrentHashMap() }
        for (chunkPos in footprint) {
            refs.compute(chunkPos) { _, count -> (count ?: 0) + 1 }
        }
    }

    private fun removeSimulationFootprintRefs(worldKey: String, footprint: Set<ChunkPos>) {
        if (footprint.isEmpty()) return
        val refs = activeSimulationRefCountByWorld[worldKey] ?: return
        for (chunkPos in footprint) {
            refs.computeIfPresent(chunkPos) { _, count ->
                if (count <= 1) null else count - 1
            }
        }
        if (refs.isEmpty()) {
            activeSimulationRefCountByWorld.remove(worldKey, refs)
        }
    }

    private fun clearActiveSimulationChunkDeltas() {
        synchronized(activeSimulationDeltaLock) {
            activeSimulationDeltaByWorld.clear()
        }
    }

    private fun recordActiveSimulationChunkDelta(worldKey: String, previous: Set<ChunkPos>, next: Set<ChunkPos>) {
        if (previous == next) return
        val added = HashSet<ChunkPos>()
        val removed = HashSet<ChunkPos>()
        for (chunkPos in next) {
            if (chunkPos !in previous) added.add(chunkPos)
        }
        for (chunkPos in previous) {
            if (chunkPos !in next) removed.add(chunkPos)
        }
        if (added.isEmpty() && removed.isEmpty()) return

        synchronized(activeSimulationDeltaLock) {
            val previousDelta = activeSimulationDeltaByWorld[worldKey]
            if (previousDelta == null) {
                activeSimulationDeltaByWorld[worldKey] = SimulationChunkDelta(added = added, removed = removed)
                return
            }
            val mergedAdded = HashSet(previousDelta.added)
            val mergedRemoved = HashSet(previousDelta.removed)
            for (chunkPos in added) {
                if (!mergedRemoved.remove(chunkPos)) {
                    mergedAdded.add(chunkPos)
                }
            }
            for (chunkPos in removed) {
                if (!mergedAdded.remove(chunkPos)) {
                    mergedRemoved.add(chunkPos)
                }
            }
            if (mergedAdded.isEmpty() && mergedRemoved.isEmpty()) {
                activeSimulationDeltaByWorld.remove(worldKey)
            } else {
                activeSimulationDeltaByWorld[worldKey] = SimulationChunkDelta(added = mergedAdded, removed = mergedRemoved)
            }
        }
    }

    private fun ensureActiveSimulationChunkIndex(sessionsByWorld: Map<String, List<PlayerSession>>) {
        if (sessionsByWorld.isEmpty()) {
            activeSimulationRefCountByWorld.clear()
            activeSimulationChunksCacheByWorld.clear()
            activeSimulationStampByPlayerEntityId.clear()
            activeSimulationChunksByPlayerEntityId.clear()
            clearActiveSimulationChunkDeltas()
            return
        }

        val maxSimulation = ServerConfig.maxSimulationDistanceChunks.coerceIn(2, 32)
        activeSimulationLastMaxDistanceChunks = maxSimulation
        val changedWorldKeys = HashSet<String>()

        val activeWorldKeys = sessionsByWorld.keys
        val refWorldKeys = activeSimulationRefCountByWorld.keys.toList()
        for (worldKey in refWorldKeys) {
            if (worldKey in activeWorldKeys) continue
            activeSimulationRefCountByWorld.remove(worldKey)
            activeSimulationChunksCacheByWorld.remove(worldKey)
            changedWorldKeys.add(worldKey)
        }

        val connectedCount = sessionsByWorld.values.sumOf { it.size }
        for ((worldKey, worldSessions) in sessionsByWorld) {
            for (session in worldSessions) {
                val entityId = session.entityId
                val simulationRadius = session.chunkRadius.coerceAtMost(maxSimulation)
                val nextStamp = PlayerSimulationStamp(
                    worldKey = worldKey,
                    centerChunkX = session.centerChunkX,
                    centerChunkZ = session.centerChunkZ,
                    radius = simulationRadius
                )
                val prevStamp = activeSimulationStampByPlayerEntityId[entityId]
                if (prevStamp == nextStamp) {
                    continue
                }
                if (prevStamp != null) {
                    val previous = activeSimulationChunksByPlayerEntityId.remove(entityId)
                    if (previous != null) {
                        removeSimulationFootprintRefs(prevStamp.worldKey, previous)
                        changedWorldKeys.add(prevStamp.worldKey)
                    }
                }
                val nextFootprint = buildSimulationChunkFootprint(
                    centerChunkX = nextStamp.centerChunkX,
                    centerChunkZ = nextStamp.centerChunkZ,
                    radius = nextStamp.radius
                )
                activeSimulationStampByPlayerEntityId[entityId] = nextStamp
                activeSimulationChunksByPlayerEntityId[entityId] = nextFootprint
                addSimulationFootprintRefs(worldKey, nextFootprint)
                changedWorldKeys.add(worldKey)
            }
        }

        // Normal leave path already removes stamps/footprints eagerly.
        // Keep this fallback cleanup only for mismatch cases to avoid per-tick full scans.
        if (activeSimulationStampByPlayerEntityId.size > connectedCount) {
            val activePlayerEntityIds = HashSet<Int>(connectedCount)
            for (worldSessions in sessionsByWorld.values) {
                for (session in worldSessions) {
                    activePlayerEntityIds.add(session.entityId)
                }
            }
            val knownEntityIds = activeSimulationStampByPlayerEntityId.keys.toList()
            for (entityId in knownEntityIds) {
                if (entityId in activePlayerEntityIds) continue
                val prevStamp = activeSimulationStampByPlayerEntityId.remove(entityId) ?: continue
                val previous = activeSimulationChunksByPlayerEntityId.remove(entityId)
                if (previous != null) {
                    removeSimulationFootprintRefs(prevStamp.worldKey, previous)
                    changedWorldKeys.add(prevStamp.worldKey)
                }
            }
        }

        for (worldKey in changedWorldKeys) {
            val refs = activeSimulationRefCountByWorld[worldKey]
            val previous = activeSimulationChunksCacheByWorld[worldKey] ?: emptySet()
            if (refs == null || refs.isEmpty()) {
                activeSimulationChunksCacheByWorld.remove(worldKey)
                recordActiveSimulationChunkDelta(worldKey, previous, emptySet())
                continue
            }
            val next = HashSet(refs.keys)
            activeSimulationChunksCacheByWorld[worldKey] = next
            recordActiveSimulationChunkDelta(worldKey, previous, next)
        }
    }

    private fun pruneRetainedBaseChunkCaches(activeWorldKeys: Set<String>) {
        if (activeWorldKeys.isEmpty()) {
            for (worldKey in retainedBaseChunksCacheByWorld.keys) {
                FoliaSharedMemoryWorldGenerator.retainLoadedChunks(worldKey, emptySet())
            }
            retainedBaseChunksCacheByWorld.clear()
            retainedBaseRefCountByWorld.clear()
            retainedBaseChunksByPlayerEntityId.clear()
            retainedBaseWorldKeyByPlayerEntityId.clear()
            return
        }
        val retainKeys = retainedBaseChunksCacheByWorld.keys.toList()
        for (worldKey in retainKeys) {
            if (worldKey in activeWorldKeys) continue
            FoliaSharedMemoryWorldGenerator.retainLoadedChunks(worldKey, emptySet())
            retainedBaseChunksCacheByWorld.remove(worldKey)
            retainedBaseRefCountByWorld.remove(worldKey)
        }
        val refKeys = retainedBaseRefCountByWorld.keys.toList()
        for (worldKey in refKeys) {
            if (worldKey in activeWorldKeys) continue
            retainedBaseRefCountByWorld.remove(worldKey)
        }
        val playerKeys = retainedBaseWorldKeyByPlayerEntityId.keys.toList()
        for (entityId in playerKeys) {
            val worldKey = retainedBaseWorldKeyByPlayerEntityId[entityId] ?: continue
            if (worldKey in activeWorldKeys) continue
            retainedBaseWorldKeyByPlayerEntityId.remove(entityId)
            retainedBaseChunksByPlayerEntityId.remove(entityId)
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
            run {
                state.worldAgeTicks += tickDelta
                state.timeOfDayTicks += tickDelta
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
        onGround: Boolean,
        reason: MoveReason = MoveReason.PLAYER_INPUT,
        fireMoveEvents: Boolean = true
    ) {
        val session = sessions[channelId] ?: return
        if (session.suppressNextMoveEvent) {
            val isCorrectionEcho =
                kotlin.math.abs(x - session.suppressMoveEchoX) <= 1.0e-6 &&
                    kotlin.math.abs(y - session.suppressMoveEchoY) <= 1.0e-6 &&
                    kotlin.math.abs(z - session.suppressMoveEchoZ) <= 1.0e-6 &&
                    kotlin.math.abs(wrapDegrees(yaw - session.suppressMoveEchoYaw)) <= 0.001f &&
                    kotlin.math.abs(wrapDegrees(pitch - session.suppressMoveEchoPitch)) <= 0.001f
            session.suppressNextMoveEvent = false
            if (isCorrectionEcho) {
                return
            }
        }
        val shouldFireMoveEvents = fireMoveEvents
        var targetX = x
        var targetY = y
        var targetZ = z
        var targetYaw = yaw
        var targetPitch = pitch
        if (session.ridingAnimalEntityId >= 0) {
            session.yaw = targetYaw
            session.pitch = targetPitch
            session.onGround = onGround
            session.clientEyeX = session.x
            session.clientEyeY = session.y + playerEyeOffset(session)
            session.clientEyeZ = session.z
            return
        }
        if (shouldFireMoveEvents) {
            val moveDecision = org.macaroon3145.plugin.PluginSystem.beforePlayerMove(
                session = session,
                toX = targetX,
                toY = targetY,
                toZ = targetZ,
                toYaw = targetYaw,
                toPitch = targetPitch,
                reason = reason
            )
            if (moveDecision.cancelPosition) {
                targetX = session.x
                targetY = session.y
                targetZ = session.z
            }
            if (moveDecision.cancelRotation) {
                targetYaw = session.yaw
                targetPitch = session.pitch
            }
            if (moveDecision.cancelPosition || moveDecision.cancelRotation) {
                resyncPlayerPositionToSelf(
                    session = session,
                    x = targetX,
                    y = targetY,
                    z = targetZ,
                    yaw = targetYaw,
                    pitch = targetPitch,
                    suppressNextEvent = true
                )
            }
            if (moveDecision.cancelAll) {
                return
            }
        }
        val ctx = contexts[channelId]
        val world = WorldManager.world(session.worldKey)
        val previousX = session.x
        val previousY = session.y
        val previousZ = session.z
        val wasOnGround = session.onGround
        val deltaY = targetY - previousY
        val deltaX = targetX - previousX
        val deltaZ = targetZ - previousZ
        session.velocityX = deltaX
        session.velocityY = deltaY
        session.velocityZ = deltaZ
        session.lastHorizontalMovementSq = deltaX * deltaX + deltaZ * deltaZ
        if (deltaX != 0.0 || deltaY != 0.0 || deltaZ != 0.0) {
            animalSourceDirtyWorlds.add(session.worldKey)
        }
        session.x = targetX
        session.y = targetY
        session.z = targetZ
        session.yaw = targetYaw
        session.pitch = targetPitch
        session.onGround = onGround
        if (onGround) {
            session.airJumpAttemptCount = 0
        }
        markPushingWakeFromPlayerMovement(
            session = session,
            previousX = previousX,
            previousZ = previousZ,
            nextX = targetX,
            nextZ = targetZ,
            horizontalMovementSq = session.lastHorizontalMovementSq
        )
        updateFallDistance(session, world, wasOnGround, onGround, deltaY)
        applyMovementFoodExhaustion(
            session = session,
            world = world,
            deltaX = deltaX,
            deltaY = deltaY,
            deltaZ = deltaZ,
            wasOnGround = wasOnGround,
            onGround = onGround
        )

        val encodedX = encodeRelativePosition4096(targetX)
        val encodedY = encodeRelativePosition4096(targetY)
        val encodedZ = encodeRelativePosition4096(targetZ)
        val dx = encodedX - session.encodedX4096
        val dy = encodedY - session.encodedY4096
        val dz = encodedZ - session.encodedZ4096
        val requiresTeleportSync =
            dx < Short.MIN_VALUE.toLong() || dx > Short.MAX_VALUE.toLong() ||
                dy < Short.MIN_VALUE.toLong() || dy > Short.MAX_VALUE.toLong() ||
                dz < Short.MIN_VALUE.toLong() || dz > Short.MAX_VALUE.toLong()

        session.clientEyeX = targetX
        session.clientEyeY = targetY + playerEyeOffset(session)
        session.clientEyeZ = targetZ

        val packet = if (requiresTeleportSync) {
            PlayPackets.entityPositionSyncPacket(
                entityId = session.entityId,
                x = targetX,
                y = targetY,
                z = targetZ,
                yaw = targetYaw,
                pitch = targetPitch,
                onGround = onGround
            )
        } else {
            PlayPackets.entityRelativeMoveLookPacket(
                entityId = session.entityId,
                deltaX = dx.toInt(),
                deltaY = dy.toInt(),
                deltaZ = dz.toInt(),
                yaw = targetYaw,
                pitch = targetPitch,
                onGround = onGround
            )
        }
        val headLookPacket = PlayPackets.entityHeadLookPacket(
            entityId = session.entityId,
            headYaw = targetYaw
        )
        for ((id, other) in sessions) {
            if (id == channelId || other.worldKey != session.worldKey) continue
            val otherCtx = contexts[id] ?: continue
            if (otherCtx.channel().isActive) {
                otherCtx.write(packet)
                otherCtx.write(headLookPacket)
                otherCtx.flush()
            }
        }
        session.encodedX4096 = encodedX
        session.encodedY4096 = encodedY
        session.encodedZ4096 = encodedZ

        val currentChunkX = ChunkStreamingService.chunkXFromBlockX(targetX)
        val currentChunkZ = ChunkStreamingService.chunkZFromBlockZ(targetZ)
        if (currentChunkX != session.centerChunkX || currentChunkZ != session.centerChunkZ) {
            session.centerChunkX = currentChunkX
            session.centerChunkZ = currentChunkZ
            if (ctx != null && world != null) {
                requestChunkStream(session, ctx, world, currentChunkX, currentChunkZ, session.chunkRadius)
            }
        }

        if (!session.dead && session.gameMode == GAME_MODE_SURVIVAL && !wasOnGround && onGround) {
            applyLandingDamage(session)
        }
    }

    private fun resyncPlayerPositionToSelf(
        session: PlayerSession,
        x: Double = session.x,
        y: Double = session.y,
        z: Double = session.z,
        yaw: Float = session.yaw,
        pitch: Float = session.pitch,
        suppressNextEvent: Boolean = false
    ) {
        if (suppressNextEvent) {
            session.suppressNextMoveEvent = true
            session.suppressMoveEchoX = x
            session.suppressMoveEchoY = y
            session.suppressMoveEchoZ = z
            session.suppressMoveEchoYaw = yaw
            session.suppressMoveEchoPitch = pitch
        }
        val ctx = contexts[session.channelId] ?: return
        if (!ctx.channel().isActive) return
        ctx.executor().execute {
            if (!ctx.channel().isActive) return@execute
            val teleportId = nextTeleportId(session)
            ctx.writeAndFlush(
                PlayPackets.playerPositionPacket(
                    teleportId = teleportId,
                    x = x,
                    y = y,
                    z = z,
                    yaw = yaw,
                    pitch = pitch,
                    relativeFlags = 0
                )
            )
        }
    }

    private fun applyMovementFoodExhaustion(
        session: PlayerSession,
        world: org.macaroon3145.world.World?,
        deltaX: Double,
        deltaY: Double,
        deltaZ: Double,
        wasOnGround: Boolean,
        onGround: Boolean
    ) {
        if (session.dead) return
        if (session.gameMode != GAME_MODE_SURVIVAL) return
        if (world == null) return
        if (deltaX.isNaN() || deltaY.isNaN() || deltaZ.isNaN()) return
        if (deltaX.isInfinite() || deltaY.isInfinite() || deltaZ.isInfinite()) return
        val beforeFood = session.food
        val beforeSaturation = session.saturation

        val horizontalMeters = sqrt(deltaX * deltaX + deltaZ * deltaZ)
        val totalMeters = sqrt(deltaX * deltaX + deltaY * deltaY + deltaZ * deltaZ)
        val horizontalCentimeters = (horizontalMeters * VANILLA_STAT_CENTIMETER_SCALE).roundToLong().coerceAtLeast(0L)
        val totalCentimeters = (totalMeters * VANILLA_STAT_CENTIMETER_SCALE).roundToLong().coerceAtLeast(0L)

        val feetInWater = isWaterAt(world, session.x, session.y, session.z)
        val eyeInWater = isWaterAt(world, session.clientEyeX, session.clientEyeY, session.clientEyeZ)

        when {
            // Vanilla water movement exhaustion: 0.01 exhaustion per block moved in water.
            eyeInWater -> {
                if (totalCentimeters > 0L) {
                    applyFoodExhaustion(session, (totalCentimeters.toFloat() * VANILLA_WATER_EXHAUSTION_PER_CENTIMETER))
                }
            }
            feetInWater -> {
                if (horizontalCentimeters > 0L) {
                    applyFoodExhaustion(session, (horizontalCentimeters.toFloat() * VANILLA_WATER_EXHAUSTION_PER_CENTIMETER))
                }
            }
            // Vanilla sprinting exhaustion: 0.1 exhaustion per block moved on ground while sprinting.
            onGround && session.sprinting -> {
                if (horizontalCentimeters > 0L) {
                    applyFoodExhaustion(session, (horizontalCentimeters.toFloat() * VANILLA_SPRINT_EXHAUSTION_PER_CENTIMETER))
                }
            }
        }

        // Vanilla jump exhaustion.
        if (wasOnGround && !onGround && deltaY > VANILLA_JUMP_MIN_ASCENT_METERS) {
            applyFoodExhaustion(
                session,
                if (session.sprinting) VANILLA_SPRINT_JUMP_EXHAUSTION else VANILLA_JUMP_EXHAUSTION
            )
        }
        if (session.food != beforeFood || session.saturation != beforeSaturation) {
            sendHealthPacket(session)
        }
    }

    private fun isWaterAt(world: org.macaroon3145.world.World, x: Double, y: Double, z: Double): Boolean {
        val bx = floor(x).toInt()
        val by = floor(y).toInt()
        val bz = floor(z).toInt()
        val stateId = world.blockStateAt(bx, by, bz)
        if (stateId <= 0) return false
        val parsed = BlockStateRegistry.parsedState(stateId) ?: return false
        return parsed.blockKey == "minecraft:water"
    }

    private fun updateFallDistance(
        session: PlayerSession,
        world: org.macaroon3145.world.World?,
        wasOnGround: Boolean,
        onGround: Boolean,
        deltaY: Double
    ) {
        if (session.dead || session.gameMode == GAME_MODE_SPECTATOR) {
            session.fallDistance = 0.0
            return
        }
        val inWater = world != null && isWaterAt(world, session.x, session.y, session.z)
        if (!inWater && deltaY < 0.0) {
            session.fallDistance += -deltaY
        } else if (deltaY > 0.0) {
            // Vanilla resets fall distance when jumping/upward impulse starts.
            session.fallDistance = 0.0
        }
        if (onGround && wasOnGround) {
            session.fallDistance = 0.0
        }
    }

    private fun applyLandingDamage(session: PlayerSession) {
        val accumulatedFallDistance = session.fallDistance
        session.fallDistance = 0.0
        val damage = floor((accumulatedFallDistance + FALL_DAMAGE_EPSILON - SAFE_FALL_DISTANCE_BLOCKS) * FALL_DAMAGE_MULTIPLIER).toInt()
        if (damage > 0) {
            damagePlayer(
                session = session,
                amount = damage.toFloat(),
                damageTypeId = FALL_DAMAGE_TYPE_ID,
                hurtSoundId = PLAYER_SMALL_FALL_SOUND_ID,
                bigHurtSoundId = PLAYER_BIG_FALL_SOUND_ID,
                deathMessage = PlayPackets.ChatComponent.Translate(
                    key = "death.fell.accident.generic",
                    args = listOf(playerNameComponent(session))
                )
            )
        }
    }

    fun handlePlayerAttackEntity(attackerChannelId: ChannelId, targetEntityId: Int) {
        val attacker = sessions[attackerChannelId] ?: return
        if (attacker.dead) return
        if (attacker.gameMode == GAME_MODE_SPECTATOR) return
        val world = WorldManager.world(attacker.worldKey) ?: return
        val attackStrengthScale = playerAttackStrengthScale(attacker, 0.5f)
        val strongAttack = attackStrengthScale > VANILLA_STRONG_ATTACK_THRESHOLD
        val knockbackAttack = attacker.sprinting && strongAttack
        var playedSweep = false

        val playerTarget = sessions.values.firstOrNull {
            it.entityId == targetEntityId &&
                it.channelId != attackerChannelId &&
                it.worldKey == attacker.worldKey &&
                !it.dead &&
                it.gameMode != GAME_MODE_SPECTATOR &&
                it.gameMode != GAME_MODE_CREATIVE
        }
        if (playerTarget != null) {
            val dx = attacker.x - playerTarget.x
            val dy = attacker.y - playerTarget.y
            val dz = attacker.z - playerTarget.z
            val distanceSq = dx * dx + dy * dy + dz * dz
            if (distanceSq > MAX_PLAYER_MELEE_REACH_SQ) return
            resetPlayerAttackStrengthTicker(attacker)

            val criticalAttack = strongAttack && canCriticalAttack(attacker, playerTarget)
            val sweepAttack = canSweepAttack(attacker, strongAttack, criticalAttack, knockbackAttack)
            var damage = scaledPlayerBaseDamage(meleeAttackDamage(attacker).coerceAtLeast(1.0f), attackStrengthScale)
            if (criticalAttack) {
                damage *= VANILLA_CRITICAL_DAMAGE_MULTIPLIER
            }

            damagePlayer(
                session = playerTarget,
                amount = damage,
                damageTypeId = PLAYER_ATTACK_DAMAGE_TYPE_ID,
                hurtSoundId = PLAYER_HURT_SOUND_ID,
                bigHurtSoundId = PLAYER_HURT_SOUND_ID,
                deathMessage = PlayPackets.ChatComponent.Translate(
                    key = "death.attack.player",
                    args = listOf(
                        playerNameComponent(playerTarget),
                        playerNameComponent(attacker)
                    )
                )
            )
            if (criticalAttack) {
                broadcastEntityAnimation(attacker.worldKey, playerTarget.entityId, ENTITY_ANIMATION_CRITICAL_HIT)
            }

            val knockbackStrength = BASE_PLAYER_KNOCKBACK + if (knockbackAttack) SPRINT_KNOCKBACK_BONUS else 0.0
            applyVanillaKnockback(
                attacker = attacker,
                target = playerTarget,
                strength = knockbackStrength,
                attackDirX = attacker.x - playerTarget.x,
                attackDirZ = attacker.z - playerTarget.z
            )
            if (sweepAttack) {
                playedSweep = true
                applySweepAttack(
                    attacker = attacker,
                    primaryTarget = playerTarget,
                    damageTypeId = PLAYER_ATTACK_DAMAGE_TYPE_ID,
                    attackOriginX = attacker.x,
                    attackOriginZ = attacker.z
                )
            }
            playAttackFeedback(attacker, knockbackAttack, criticalAttack, strongAttack, playedSweep)
            return
        }

        val animalTarget = world.animalSnapshot(targetEntityId) ?: return
        val dx = attacker.x - animalTarget.x
        val dy = attacker.y - animalTarget.y
        val dz = attacker.z - animalTarget.z
        val distanceSq = dx * dx + dy * dy + dz * dz
        if (distanceSq > MAX_PLAYER_MELEE_REACH_SQ) return
        resetPlayerAttackStrengthTicker(attacker)

        val criticalAttack = strongAttack && canCriticalAttackAgainstPoint(attacker, animalTarget.x, animalTarget.y, animalTarget.z)
        var damage = scaledPlayerBaseDamage(meleeAttackDamage(attacker).coerceAtLeast(1.0f), attackStrengthScale)
        if (criticalAttack) {
            damage *= VANILLA_CRITICAL_DAMAGE_MULTIPLIER
        }
        val knockbackStrength = if (knockbackAttack) ANIMAL_SPRINT_ATTACK_KNOCKBACK else ANIMAL_BASE_ATTACK_KNOCKBACK
        val damageWithKnockback = world.damageAnimalWithKnockback(
            entityId = animalTarget.entityId,
            amount = damage,
            cause = AnimalDamageCause.PLAYER_ATTACK,
            attackerX = attacker.x,
            attackerZ = attacker.z,
            strength = knockbackStrength,
            preGravityYCompensation = KNOCKBACK_Y_PRE_GRAVITY_COMPENSATION
        ) ?: return
        val damageResult = damageWithKnockback.damageResult
        if (criticalAttack) {
            broadcastEntityAnimation(attacker.worldKey, animalTarget.entityId, ENTITY_ANIMATION_CRITICAL_HIT)
        }
        broadcastAnimalDamageFeedback(attacker.worldKey, damageResult.damage)
        if (damageResult.removed?.died == true) {
            val wasSaddled = damageResult.removed.entityId in saddledAnimalEntityIds
            spawnAnimalDeathDrops(world, damageResult.removed, attacker, dropSaddle = wasSaddled)
            broadcastAnimalDeath(attacker.worldKey, damageResult.removed)
            broadcastAnimalRemoval(attacker.worldKey, damageResult.removed.entityId)
        }
        if (damageWithKnockback.knockbackApplied) {
            broadcastAnimalVelocity(
                attacker.worldKey,
                animalTarget.entityId,
                damageWithKnockback.velocityX,
                damageWithKnockback.velocityY,
                damageWithKnockback.velocityZ
            )
            broadcastAttackKnockbackAt(
                worldKey = attacker.worldKey,
                x = damageWithKnockback.x,
                y = damageWithKnockback.y + damageWithKnockback.hitboxHeight * 0.5,
                z = damageWithKnockback.z
            )
        }
        playAttackFeedback(
            attacker = attacker,
            knockbackAttack = knockbackAttack,
            criticalAttack = criticalAttack,
            strongAttack = strongAttack,
            sweepAttack = false
        )
    }

    fun handlePlayerInteractEntity(channelId: ChannelId, targetEntityId: Int, hand: Int) {
        if (hand !in 0..1) return
        val session = sessions[channelId] ?: return
        if (session.dead || session.gameMode == GAME_MODE_SPECTATOR) return
        val world = WorldManager.world(session.worldKey) ?: return
        val animal = world.animalSnapshot(targetEntityId) ?: return
        if (animal.kind != AnimalKind.PIG) return

        val dx = session.x - animal.x
        val dy = session.y - animal.y
        val dz = session.z - animal.z
        if ((dx * dx) + (dy * dy) + (dz * dz) > MAX_PLAYER_MELEE_REACH_SQ) return

        val selectedSlot = session.selectedHotbarSlot.coerceIn(0, 8)
        val itemId = if (hand == 1) session.offhandItemId else session.hotbarItemIds[selectedSlot]
        val itemCount = if (hand == 1) session.offhandItemCount else session.hotbarItemCounts[selectedSlot]

        // Keep breeding interaction precedence over riding/saddle behavior.
        if (itemCount > 0 && itemId in pigFoodItemIds && itemId != carrotOnAStickItemId) {
            return
        }

        if (itemCount > 0 && itemId == saddleItemId && saddleItemId >= 0 && animal.entityId !in saddledAnimalEntityIds) {
            saddledAnimalEntityIds.add(animal.entityId)
            if (session.gameMode != GAME_MODE_CREATIVE) {
                consumePlacedItem(session, hand)
            }
            broadcastPigSaddleEquip(session.worldKey, animal.entityId, animal.x, animal.y, animal.z)
            return
        }

        if (tryMountPig(session, animal.entityId)) {
            return
        }
    }

    fun handlePlayerDismountRequest(channelId: ChannelId) {
        val session = sessions[channelId] ?: return
        dismountPlayerFromAnimal(session)
    }

    fun captureClientVehicleMove(
        channelId: ChannelId,
        x: Double,
        y: Double,
        z: Double,
        yaw: Float,
        pitch: Float,
        onGround: Boolean
    ) {
        val session = sessions[channelId] ?: return
        if (session.ridingAnimalEntityId < 0) return
        if (!x.isFinite() || !y.isFinite() || !z.isFinite() || !yaw.isFinite() || !pitch.isFinite()) return
        val dx = x - session.lastClientVehicleX
        val dy = y - session.lastClientVehicleY
        val dz = z - session.lastClientVehicleZ
        val distSq = dx * dx + dy * dy + dz * dz
        if (distSq > MAX_CLIENT_VEHICLE_POSE_DELTA_SQ) return
        session.lastClientVehicleX = x
        session.lastClientVehicleY = y
        session.lastClientVehicleZ = z
        session.lastClientVehicleYaw = yaw
        session.lastClientVehiclePitch = pitch
        session.lastClientVehicleOnGround = onGround
        session.lastClientVehiclePacketAtNanos = System.nanoTime()
        session.yaw = yaw
        session.pitch = pitch
        broadcastMountedAnimalHeadYawFromVehiclePacket(session, yaw)
    }

    private fun broadcastMountedAnimalHeadYawFromVehiclePacket(session: PlayerSession, yaw: Float) {
        val animalEntityId = session.ridingAnimalEntityId
        if (animalEntityId < 0) return
        val world = WorldManager.world(session.worldKey) ?: return
        val animal = world.animalSnapshot(animalEntityId) ?: return
        val chunkPos = animal.chunkPos
        val packet = PlayPackets.entityHeadLookPacket(animalEntityId, yaw)
        for ((id, other) in sessions) {
            if (other.worldKey != session.worldKey) continue
            if (!other.loadedChunks.contains(chunkPos)) continue
            val ctx = contexts[id] ?: continue
            if (!ctx.channel().isActive) continue
            ctx.write(packet)
            ctx.flush()
        }
    }

    private fun tryMountPig(session: PlayerSession, animalEntityId: Int): Boolean {
        if (session.sneaking) return false
        if (session.ridingAnimalEntityId == animalEntityId) return true
        if (animalEntityId !in saddledAnimalEntityIds) return false
        val existingRider = animalRiderEntityIdByAnimalEntityId[animalEntityId]
        if (existingRider != null && existingRider != session.entityId) return false
        if (session.ridingAnimalEntityId >= 0) {
            dismountPlayerFromAnimal(session)
        }
        animalRiderEntityIdByAnimalEntityId[animalEntityId] = session.entityId
        animalEntityIdByRiderEntityId[session.entityId] = animalEntityId
        session.ridingAnimalEntityId = animalEntityId
        // Seed vehicle pose baseline from mounted entity so the first client vehicle packet
        // is not rejected by the movement delta guard.
        val mounted = WorldManager.world(session.worldKey)?.animalSnapshot(animalEntityId)
        if (mounted != null) {
            session.lastClientVehicleX = mounted.x
            session.lastClientVehicleY = mounted.y
            session.lastClientVehicleZ = mounted.z
            session.lastClientVehicleYaw = mounted.yaw
            session.lastClientVehiclePitch = mounted.pitch
            session.lastClientVehicleOnGround = mounted.onGround
            session.lastClientVehiclePacketAtNanos = System.nanoTime()
        }
        broadcastAnimalPassengers(session.worldKey, animalEntityId)
        return true
    }

    private fun dismountPlayerFromAnimal(session: PlayerSession) {
        val mountedAnimalEntityId =
            animalEntityIdByRiderEntityId.remove(session.entityId)
                ?: session.ridingAnimalEntityId.takeIf { it >= 0 }
                ?: return
        val world = WorldManager.world(session.worldKey)
        val mountedAnimalSnapshot = world?.animalSnapshot(mountedAnimalEntityId)
        val mappedRider = animalRiderEntityIdByAnimalEntityId[mountedAnimalEntityId]
        if (mappedRider == session.entityId) {
            animalRiderEntityIdByAnimalEntityId.remove(mountedAnimalEntityId)
        }
        session.ridingAnimalEntityId = -1
        broadcastAnimalPassengers(session.worldKey, mountedAnimalEntityId)
        if (world != null && mountedAnimalSnapshot != null && mountedAnimalSnapshot.kind == AnimalKind.PIG) {
            val dismountPos = findPigDismountPosition(world, mountedAnimalSnapshot)
            if (dismountPos != null) {
                teleportPlayer(
                    target = session,
                    x = dismountPos.first,
                    y = dismountPos.second,
                    z = dismountPos.third,
                    yaw = null,
                    pitch = null,
                    reason = MoveReason.DISMOUNT
                )
            }
        }
    }

    private fun findPigDismountPosition(
        world: org.macaroon3145.world.World,
        animal: AnimalSnapshot
    ): Triple<Double, Double, Double>? {
        val baseYaw = animal.yaw
        val angles = floatArrayOf(
            baseYaw + 90.0f,
            baseYaw - 90.0f,
            baseYaw + 135.0f,
            baseYaw - 135.0f,
            baseYaw + 180.0f,
            baseYaw
        )
        val baseY = floor(animal.y + 1.0e-6)
        for (angle in angles) {
            val offset = passengerDismountOffset(
                passengerWidth = PLAYER_HITBOX_HALF_WIDTH * 2.0,
                vehicleWidth = animal.hitboxWidth,
                yawDegrees = angle
            )
            val x = animal.x + offset.first
            val z = animal.z + offset.second
            for (yOffset in intArrayOf(0, 1, -1)) {
                val y = baseY + yOffset
                if (!hasSupportBelow(world, x, y, z)) continue
                if (!isPlayerAabbClear(world, x, y, z, 1.8)) continue
                return Triple(x, y, z)
            }
        }
        return null
    }

    // Mirrors vanilla Entity#getPassengerDismountOffset shape (width-based radial offset).
    private fun passengerDismountOffset(
        passengerWidth: Double,
        vehicleWidth: Double,
        yawDegrees: Float
    ): Pair<Double, Double> {
        val half = ((passengerWidth + vehicleWidth) + 9.999999747378752e-6) / 2.0
        val yawRad = Math.toRadians(yawDegrees.toDouble())
        val sin = -sin(yawRad)
        val cos = cos(yawRad)
        val scale = max(abs(sin), abs(cos))
        if (scale <= 1.0e-9) return 0.0 to half
        return (sin * half / scale) to (cos * half / scale)
    }

    private fun hasSupportBelow(world: org.macaroon3145.world.World, x: Double, y: Double, z: Double): Boolean {
        val blockX = floor(x).toInt()
        val blockY = floor(y).toInt()
        val blockZ = floor(z).toInt()
        return isSupportBlock(world.blockStateAt(blockX, blockY - 1, blockZ))
    }

    private fun isPlayerAabbClear(
        world: org.macaroon3145.world.World,
        x: Double,
        y: Double,
        z: Double,
        height: Double
    ): Boolean {
        val playerBox = playerAabbAt(
            x = x,
            y = y,
            z = z,
            halfWidth = PLAYER_HITBOX_HALF_WIDTH,
            height = height
        )
        val minX = playerBox.minX
        val maxX = playerBox.maxX
        val minY = playerBox.minY
        val maxY = playerBox.maxY
        val minZ = playerBox.minZ
        val maxZ = playerBox.maxZ

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
                    if (stateId == 0) continue
                    if (isSupportBlock(stateId)) return false
                }
            }
        }
        return true
    }

    private fun dismountAnimalRider(worldKey: String, animalEntityId: Int) {
        val riderEntityId = animalRiderEntityIdByAnimalEntityId.remove(animalEntityId) ?: return
        animalEntityIdByRiderEntityId.remove(riderEntityId)
        val rider = sessions.values.firstOrNull { it.entityId == riderEntityId && it.worldKey == worldKey }
        if (rider != null) {
            rider.ridingAnimalEntityId = -1
            // Pig death/forced dismount path must re-anchor rider entity for observers.
            // While mounted, rider movement packets are intentionally suppressed.
            broadcastForcedDismountRiderSync(rider)
        }
        broadcastAnimalPassengers(worldKey, animalEntityId)
    }

    private fun broadcastForcedDismountRiderSync(rider: PlayerSession) {
        val positionPacket = PlayPackets.entityPositionSyncPacket(
            entityId = rider.entityId,
            x = rider.x,
            y = rider.y,
            z = rider.z,
            yaw = rider.yaw,
            pitch = rider.pitch,
            onGround = rider.onGround
        )
        val headLookPacket = PlayPackets.entityHeadLookPacket(
            entityId = rider.entityId,
            headYaw = rider.yaw
        )
        val statePacket = PlayPackets.playerSharedFlagsMetadataPacket(
            entityId = rider.entityId,
            sneaking = rider.sneaking,
            sprinting = rider.sprinting,
            swimming = rider.swimming,
            usingItemHand = rider.usingItemHand
        )
        for ((_, other) in sessions) {
            if (other.channelId == rider.channelId) continue
            if (other.worldKey != rider.worldKey) continue
            val ctx = contexts[other.channelId] ?: continue
            if (!ctx.channel().isActive) continue
            ctx.write(positionPacket)
            ctx.write(headLookPacket)
            ctx.write(statePacket)
            ctx.flush()
        }
    }

    private fun broadcastAnimalPassengers(worldKey: String, animalEntityId: Int) {
        val riderEntityId = animalRiderEntityIdByAnimalEntityId[animalEntityId]
        val passengers = if (riderEntityId != null) intArrayOf(riderEntityId) else IntArray(0)
        val packet = PlayPackets.setPassengersPacket(animalEntityId, passengers)
        for ((_, other) in sessions) {
            if (other.worldKey != worldKey) continue
            if (other.entityId != riderEntityId && animalEntityId !in other.visibleAnimalEntityIds) continue
            val ctx = contexts[other.channelId] ?: continue
            if (!ctx.channel().isActive) continue
            ctx.write(packet)
            ctx.flush()
        }
    }

    private fun broadcastPigSaddleEquip(worldKey: String, animalEntityId: Int, x: Double, y: Double, z: Double) {
        val saddleId = saddleItemId
        if (saddleId < 0) return
        val saddlePacket = PlayPackets.entityEquipmentPacket(
            animalEntityId,
            listOf(
                PlayPackets.EquipmentEntry(
                    slot = 7,
                    encodedItemStack = PlayPackets.encodeItemStack(saddleId, 1)
                )
            )
        )
        val soundPacket = PlayPackets.soundPacketByKey(
            soundKey = "minecraft:entity.pig.saddle",
            soundSourceId = NEUTRAL_SOUND_SOURCE_ID,
            x = x,
            y = y + 0.5,
            z = z,
            volume = 0.5f,
            pitch = 1.0f,
            seed = ThreadLocalRandom.current().nextLong()
        )
        for ((_, other) in sessions) {
            if (other.worldKey != worldKey) continue
            if (animalEntityId !in other.visibleAnimalEntityIds) continue
            val ctx = contexts[other.channelId] ?: continue
            if (!ctx.channel().isActive) continue
            ctx.write(saddlePacket)
            ctx.write(soundPacket)
            ctx.flush()
        }
    }

    private fun meleeAttackDamage(attacker: PlayerSession): Float {
        val heldItemId = attacker.hotbarItemIds[attacker.selectedHotbarSlot.coerceIn(0, 8)]
        val heldItemKey = heldItemKey(heldItemId) ?: return BASE_UNARMED_DAMAGE
        return vanillaAttackDamageByItemKey(heldItemKey)
    }

    private fun playerAttackStrengthScale(session: PlayerSession, partialTicks: Float): Float {
        val delayTicks = playerAttackStrengthDelayTicks(session)
        if (!delayTicks.isFinite() || delayTicks <= 1.0e-6f) return 1.0f
        // Vanilla increments attackStrengthTicker in whole ticks.
        // Keep delta-time accumulation, but quantize to whole ticks for damage scaling parity.
        val wholeTicks = floor(session.attackStrengthTickerTicks).toFloat()
        val scaled = ((wholeTicks + partialTicks) / delayTicks)
        return scaled.coerceIn(0.0f, 1.0f)
    }

    private fun resetPlayerAttackStrengthTicker(session: PlayerSession) {
        session.attackStrengthTickerTicks = 0.0
        attackCooldownActiveChannelIds.add(session.channelId)
    }

    private fun playerAttackStrengthDelayTicks(session: PlayerSession): Float {
        val heldItemId = session.hotbarItemIds[session.selectedHotbarSlot.coerceIn(0, 8)]
        val heldItemKey = heldItemKey(heldItemId)
        val attackSpeed = vanillaAttackSpeedByItemKey(heldItemKey)
        if (!attackSpeed.isFinite() || attackSpeed <= 1.0e-6f) return DEFAULT_ATTACK_STRENGTH_DELAY_TICKS
        return (MINECRAFT_TICKS_PER_SECOND.toFloat() / attackSpeed).coerceAtLeast(1.0f)
    }

    private fun scaledPlayerBaseDamage(baseDamage: Float, attackStrengthScale: Float): Float {
        val clamped = attackStrengthScale.coerceIn(0.0f, 1.0f)
        val factor = 0.2f + (clamped * clamped) * 0.8f
        return baseDamage * factor
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

    private fun vanillaAttackSpeedByItemKey(itemKey: String?): Float {
        if (itemKey.isNullOrEmpty()) return BASE_UNARMED_ATTACK_SPEED
        val key = itemKey.substringAfter("minecraft:")
        return when {
            key.endsWith("_sword") -> 1.6f
            key.endsWith("_axe") -> when {
                key.startsWith("wooden_") -> 0.8f
                key.startsWith("stone_") -> 0.8f
                key.startsWith("iron_") -> 0.9f
                key.startsWith("diamond_") -> 1.0f
                key.startsWith("netherite_") -> 1.0f
                key.startsWith("golden_") -> 1.0f
                else -> 0.8f
            }
            key.endsWith("_pickaxe") -> 1.2f
            key.endsWith("_shovel") -> 1.0f
            key.endsWith("_hoe") -> when {
                key.startsWith("wooden_") -> 1.0f
                key.startsWith("stone_") -> 2.0f
                key.startsWith("iron_") -> 3.0f
                key.startsWith("diamond_") -> 4.0f
                key.startsWith("netherite_") -> 4.0f
                key.startsWith("golden_") -> 1.0f
                else -> 1.0f
            }
            key == "trident" -> 1.1f
            else -> BASE_UNARMED_ATTACK_SPEED
        }
    }

    private fun canCriticalAttack(attacker: PlayerSession, target: PlayerSession): Boolean {
        return attacker.fallDistance > 0.0 &&
            !attacker.onGround &&
            !attacker.swimming &&
            !attacker.dead &&
            !target.dead
    }

    private fun canCriticalAttackAgainstPoint(attacker: PlayerSession, targetX: Double, targetY: Double, targetZ: Double): Boolean {
        if (attacker.dead || attacker.onGround || attacker.swimming) return false
        if (attacker.fallDistance <= 0.0) return false
        val dx = attacker.x - targetX
        val dy = attacker.y - targetY
        val dz = attacker.z - targetZ
        return dx * dx + dy * dy + dz * dz <= MAX_PLAYER_MELEE_REACH_SQ
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

    private fun playAttackFeedback(
        attacker: PlayerSession,
        knockbackAttack: Boolean,
        criticalAttack: Boolean,
        strongAttack: Boolean,
        sweepAttack: Boolean
    ) {
        if (knockbackAttack) {
            attacker.sprinting = false
            updateAndBroadcastPlayerState(attacker.channelId, sprinting = false)
            broadcastAttackSound(attacker, PLAYER_ATTACK_KNOCKBACK_SOUND_ID)
            return
        }
        if (criticalAttack) {
            broadcastAttackSound(attacker, PLAYER_ATTACK_CRIT_SOUND_ID)
            return
        }
        if (sweepAttack) {
            broadcastAttackSound(attacker, PLAYER_ATTACK_SWEEP_SOUND_ID)
            return
        }
        if (strongAttack) {
            broadcastAttackSound(attacker, PLAYER_ATTACK_STRONG_SOUND_ID)
            return
        }
        broadcastAttackSound(attacker, PLAYER_ATTACK_WEAK_SOUND_ID)
    }

    private fun applyVanillaKnockback(
        attacker: PlayerSession,
        target: PlayerSession,
        strength: Double,
        attackDirX: Double,
        attackDirZ: Double
    ) {
        if (strength <= 0.0) return

        // Match vanilla LivingEntity#knockback: use target's current motion as base.
        val horizontalBaseX = target.velocityX
        val horizontalBaseY = target.velocityY
        val horizontalBaseZ = target.velocityZ
        val (knockDirX, knockDirZ) = resolveAttackKnockbackDirection(attacker, attackDirX, attackDirZ)
        val norm = kotlin.math.sqrt((knockDirX * knockDirX) + (knockDirZ * knockDirZ))
        if (norm <= 1.0e-6) return

        val normalizedX = knockDirX / norm
        val normalizedZ = knockDirZ / norm
        val nextVx = (horizontalBaseX * 0.5) - (normalizedX * strength)
        val nextVz = (horizontalBaseZ * 0.5) - (normalizedZ * strength)
        val groundedForKnockback = target.onGround || kotlin.math.abs(horizontalBaseY) <= 1.0e-6
        val nextVy = if (groundedForKnockback) {
            minOf(0.4, (horizontalBaseY * 0.5) + strength)
        } else {
            horizontalBaseY
        }

        target.velocityX = nextVx
        target.velocityY = nextVy
        target.velocityZ = nextVz
        broadcastEntityVelocity(target, nextVx, nextVy, nextVz)
    }

    private fun resolveAttackKnockbackDirection(
        attacker: PlayerSession,
        attackDirX: Double,
        attackDirZ: Double
    ): Pair<Double, Double> {
        val dirNormSq = (attackDirX * attackDirX) + (attackDirZ * attackDirZ)
        if (dirNormSq > 1.0e-12) {
            return attackDirX to attackDirZ
        }
        val yawRad = Math.toRadians(attacker.yaw.toDouble())
        return sin(yawRad) to -cos(yawRad)
    }

    private fun applySweepAttack(
        attacker: PlayerSession,
        primaryTarget: PlayerSession,
        damageTypeId: Int,
        attackOriginX: Double,
        attackOriginZ: Double
    ) {
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
                        playerNameComponent(other),
                        playerNameComponent(attacker)
                    )
                )
            )
            applyVanillaKnockback(
                attacker = attacker,
                target = other,
                strength = VANILLA_SWEEP_KNOCKBACK,
                attackDirX = attackOriginX - other.x,
                attackDirZ = attackOriginZ - other.z
            )
        }
    }

    private fun broadcastAttackKnockbackAt(worldKey: String, x: Double, y: Double, z: Double) {
        val packet = PlayPackets.soundPacketByKey(
            soundKey = "minecraft:entity.player.attack.knockback",
            soundSourceId = PLAYERS_SOUND_SOURCE_ID,
            x = x,
            y = y,
            z = z,
            volume = 1.0f,
            pitch = (0.9f + ThreadLocalRandom.current().nextFloat() * 0.2f),
            seed = ThreadLocalRandom.current().nextLong()
        )
        for ((id, other) in sessions) {
            if (other.worldKey != worldKey) continue
            val ctx = contexts[id] ?: continue
            if (!ctx.channel().isActive) continue
            ctx.write(packet)
            ctx.flush()
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

    private fun broadcastAnimalVelocity(worldKey: String, entityId: Int, vx: Double, vy: Double, vz: Double) {
        val velocityPacket = PlayPackets.entityVelocityPacket(
            entityId = entityId,
            vx = vx,
            vy = vy,
            vz = vz
        )
        for ((id, other) in sessions) {
            if (other.worldKey != worldKey) continue
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

    private fun broadcastEntityAnimation(worldKey: String, entityId: Int, animationType: Int) {
        val packet = PlayPackets.entityAnimationPacket(entityId, animationType)
        for ((id, other) in sessions) {
            if (other.worldKey != worldKey) continue
            val ctx = contexts[id] ?: continue
            if (!ctx.channel().isActive) continue
            ctx.write(packet)
            ctx.flush()
        }
    }

    private fun broadcastAnimalDamageFeedback(worldKey: String, damage: org.macaroon3145.world.AnimalDamagedEvent) {
        val damageTypeId = when (damage.cause) {
            AnimalDamageCause.FALL -> FALL_DAMAGE_TYPE_ID
            AnimalDamageCause.PLAYER_ATTACK -> PLAYER_ATTACK_DAMAGE_TYPE_ID
            AnimalDamageCause.GENERIC -> PLAYER_ATTACK_DAMAGE_TYPE_ID
        }
        val hurtAnimationPacket = PlayPackets.hurtAnimationPacket(damage.entityId, 0f)
        val damageEventPacket = PlayPackets.damageEventPacket(damage.entityId, damageTypeId)
        val snapshot = WorldManager.world(worldKey)?.animalSnapshot(damage.entityId)
        val damageSoundPackets = snapshot?.let {
            val packets = ArrayList<ByteArray>(2)
            for (soundKey in animalDamageSoundKeys(damage)) {
                packets.add(
                    PlayPackets.soundPacketByKey(
                        soundKey = soundKey,
                        soundSourceId = NEUTRAL_SOUND_SOURCE_ID,
                        x = it.x,
                        y = it.y + it.hitboxHeight * 0.5,
                        z = it.z,
                        volume = 1.0f,
                        pitch = (0.9f + ThreadLocalRandom.current().nextFloat() * 0.2f),
                        seed = ThreadLocalRandom.current().nextLong()
                    )
                )
            }
            packets
        } ?: emptyList()
        for ((id, other) in sessions) {
            if (other.worldKey != worldKey) continue
            val ctx = contexts[id] ?: continue
            if (!ctx.channel().isActive) continue
            ctx.write(hurtAnimationPacket)
            ctx.write(damageEventPacket)
            for (soundPacket in damageSoundPackets) {
                ctx.write(soundPacket)
            }
            ctx.flush()
        }
    }

    private fun animalDamageSoundKeys(damage: org.macaroon3145.world.AnimalDamagedEvent): List<String> {
        return when (damage.cause) {
            AnimalDamageCause.FALL -> {
                if (damage.died) {
                    listOf(if (damage.amount > 4.0f) ANIMAL_BIG_FALL_SOUND_KEY else ANIMAL_SMALL_FALL_SOUND_KEY)
                } else {
                    listOf(
                        if (damage.amount > 4.0f) ANIMAL_BIG_FALL_SOUND_KEY else ANIMAL_SMALL_FALL_SOUND_KEY,
                        damage.kind.hurtSoundKey
                    )
                }
            }
            AnimalDamageCause.PLAYER_ATTACK, AnimalDamageCause.GENERIC -> {
                if (damage.died) emptyList() else listOf(damage.kind.hurtSoundKey)
            }
        }
    }

    private fun broadcastAnimalDeath(worldKey: String, removed: org.macaroon3145.world.AnimalRemovedEvent) {
        val packet = PlayPackets.entityEventPacket(removed.entityId, ENTITY_EVENT_DEATH_ANIMATION)
        val deathSoundPacket = PlayPackets.soundPacketByKey(
            soundKey = removed.kind.deathSoundKey,
            soundSourceId = NEUTRAL_SOUND_SOURCE_ID,
            x = removed.x,
            y = removed.y + 0.5,
            z = removed.z,
            volume = 1.0f,
            pitch = (0.9f + ThreadLocalRandom.current().nextFloat() * 0.2f),
            seed = ThreadLocalRandom.current().nextLong()
        )
        for ((id, other) in sessions) {
            if (other.worldKey != worldKey) continue
            val ctx = contexts[id] ?: continue
            if (!ctx.channel().isActive) continue
            ctx.write(packet)
            ctx.write(deathSoundPacket)
            ctx.flush()
        }
    }

    private fun broadcastAnimalRemoval(worldKey: String, entityId: Int) {
        scheduleAnimalRemoval(worldKey, entityId)
    }

    private fun spawnAnimalDeathDrops(
        world: org.macaroon3145.world.World,
        removed: org.macaroon3145.world.AnimalRemovedEvent,
        attacker: PlayerSession?,
        dropSaddle: Boolean
    ) {
        when (removed.kind) {
            AnimalKind.PIG -> spawnPigDeathDrops(world, removed, attacker, dropSaddle)
        }
    }

    private fun spawnPigDeathDrops(
        world: org.macaroon3145.world.World,
        removed: org.macaroon3145.world.AnimalRemovedEvent,
        attacker: PlayerSession?,
        dropSaddle: Boolean
    ) {
        val random = ThreadLocalRandom.current()
        if (dropSaddle && saddleItemId >= 0) {
            world.spawnDroppedItem(
                entityId = allocateEntityId(),
                itemId = saddleItemId,
                itemCount = 1,
                x = removed.x,
                y = removed.y + 0.1,
                z = removed.z,
                vx = random.nextDouble(BLOCK_DROP_HORIZONTAL_SPEED_MIN, BLOCK_DROP_HORIZONTAL_SPEED_MAX),
                vy = BLOCK_DROP_VERTICAL_SPEED,
                vz = random.nextDouble(BLOCK_DROP_HORIZONTAL_SPEED_MIN, BLOCK_DROP_HORIZONTAL_SPEED_MAX),
                pickupDelaySeconds = BLOCK_DROP_PICKUP_DELAY_SECONDS
            )
        }
        val rule = EntityLootDropCache.rule(AnimalKind.PIG.entityTypeKey) ?: return
        val itemId = if (shouldPigDropCooked(attacker)) {
            rule.cookedItemId ?: rule.rawItemId
        } else {
            rule.rawItemId
        }
        if (itemId < 0) return

        val minBase = rule.minCount.coerceAtLeast(0)
        val maxBase = rule.maxCount.coerceAtLeast(minBase)
        val baseCount = if (maxBase > minBase) random.nextInt(minBase, maxBase + 1) else maxBase
        val lootingLevel = playerMainHandLootingLevel(attacker)
        val lootingBonus = if (lootingLevel > 0) {
            val minLoot = (rule.lootingMinBonusPerLevel * lootingLevel).coerceAtLeast(0)
            val maxLoot = (rule.lootingMaxBonusPerLevel * lootingLevel).coerceAtLeast(minLoot)
            if (maxLoot > minLoot) random.nextInt(minLoot, maxLoot + 1) else maxLoot
        } else {
            0
        }
        val totalCount = (baseCount + lootingBonus).coerceAtLeast(1)

        world.spawnDroppedItem(
            entityId = allocateEntityId(),
            itemId = itemId,
            itemCount = totalCount,
            x = removed.x,
            y = removed.y + 0.1,
            z = removed.z,
            vx = random.nextDouble(BLOCK_DROP_HORIZONTAL_SPEED_MIN, BLOCK_DROP_HORIZONTAL_SPEED_MAX),
            vy = BLOCK_DROP_VERTICAL_SPEED,
            vz = random.nextDouble(BLOCK_DROP_HORIZONTAL_SPEED_MIN, BLOCK_DROP_HORIZONTAL_SPEED_MAX),
            pickupDelaySeconds = BLOCK_DROP_PICKUP_DELAY_SECONDS
        )
    }

    private fun playerMainHandLootingLevel(attacker: PlayerSession?): Int {
        // Current inventory/session model does not retain item enchantments.
        return 0
    }

    private fun shouldPigDropCooked(attacker: PlayerSession?): Boolean {
        // Current animal state does not expose burn state and player items do not carry enchant metadata.
        return false
    }

    private fun scheduleAnimalRemoval(worldKey: String, entityId: Int) {
        val key = "$worldKey:$entityId"
        if (!pendingAnimalRemovalKeys.add(key)) return
        pendingAnimalRemovals.add(
            PendingAnimalRemoval(
                worldKey = worldKey,
                entityId = entityId,
                dueAtNanos = System.nanoTime() + (ANIMAL_DEATH_REMOVE_DELAY_SECONDS * 1_000_000_000L).toLong()
            )
        )
    }

    private fun flushPendingAnimalRemovals() {
        if (pendingAnimalRemovals.isEmpty()) return
        val now = System.nanoTime()
        val removalsByContext = HashMap<ChannelHandlerContext, MutableList<Int>>()
        while (true) {
            val next = pendingAnimalRemovals.peek() ?: break
            if (next.dueAtNanos > now) break
            pendingAnimalRemovals.poll() ?: continue
            dismountAnimalRider(next.worldKey, next.entityId)
            saddledAnimalEntityIds.remove(next.entityId)
            pigBoostStatesByAnimalEntityId.remove(next.entityId)
            WorldManager.world(next.worldKey)?.removeAnimal(next.entityId, died = true)
            for ((id, other) in sessions) {
                if (other.worldKey != next.worldKey) continue
                if (!other.visibleAnimalEntityIds.remove(next.entityId)) continue
                other.animalTrackerStates.remove(next.entityId)
                val ctx = contexts[id] ?: continue
                if (!ctx.channel().isActive) continue
                removalsByContext.computeIfAbsent(ctx) { ArrayList() }.add(next.entityId)
            }
            pendingAnimalRemovalKeys.remove("${next.worldKey}:${next.entityId}")
        }
        for ((ctx, entityIds) in removalsByContext) {
            if (entityIds.isEmpty()) continue
            val packet = PlayPackets.removeEntitiesPacket(entityIds.toIntArray())
            enqueueChannelFlushPacket(ctx, packet)
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
        val effectiveAmount = effectiveDamageAfterPlayerInvulnerability(session, amount)
        if (effectiveAmount <= DAMAGE_EPSILON) return
        val nextHealth = (session.health - effectiveAmount).coerceAtLeast(0f)
        if (nextHealth == session.health) return
        session.health = nextHealth
        session.hurtInvulnerableSeconds = HURT_INVULNERABILITY_SECONDS
        session.lastHurtAmount = amount
        broadcastDamageFeedback(session, effectiveAmount, damageTypeId, hurtSoundId, bigHurtSoundId)
        sendHealthPacket(session)
        if (nextHealth <= 0f) {
            handlePlayerDeath(session, deathMessage)
        }
    }

    private fun effectiveDamageAfterPlayerInvulnerability(session: PlayerSession, incomingAmount: Float): Float {
        if (session.hurtInvulnerableSeconds <= 0.0) return incomingAmount
        return 0f
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
        dismountPlayerFromAnimal(session)
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

    private fun broadcastEntityEventInWorld(session: PlayerSession, eventId: Int) {
        val packet = PlayPackets.entityEventPacket(
            entityId = session.entityId,
            eventId = eventId
        )
        for ((id, other) in sessions) {
            if (other.worldKey != session.worldKey) continue
            val ctx = contexts[id] ?: continue
            if (!ctx.channel().isActive) continue
            ctx.write(packet)
            ctx.flush()
        }
    }

    private fun respawnPlayer(session: PlayerSession) {
        val world = WorldManager.world(session.worldKey) ?: WorldManager.defaultWorld()
        val spawn = FoliaSidecarSpawnPointProvider.spawnPointFor(world.key)
            ?: world.spawnPointForPlayer(session.profile.uuid)
        val ctx = contexts[session.channelId]
        dismountPlayerFromAnimal(session)
        session.dead = false
        session.health = MAX_PLAYER_HEALTH
        session.food = DEFAULT_PLAYER_FOOD
        session.saturation = DEFAULT_PLAYER_SATURATION
        session.foodExhaustion = 0f
        session.regenAccumulatorSeconds = 0.0
        stopUsingItem(session)
        lastUseItemSequenceByPlayerEntityId.remove(session.entityId)
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
        teleportPlayer(session, spawn.x, spawn.y, spawn.z, 0f, 0f, reason = MoveReason.RESPAWN)
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
        session.visibleThrownItemEntityIds.clear()
        session.thrownItemTrackerStates.clear()
        session.visibleAnimalEntityIds.clear()
        session.animalTrackerStates.clear()
        val toUnload = ArrayList<ChunkPos>(session.loadedChunks)
        session.loadedChunks.clear()
        for (pos in toUnload) {
            ctx.write(PlayPackets.unloadChunkPacket(pos.x, pos.z))
        }
        refreshRetainedBaseChunksForSession(session)
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
        animalSourceDirtyWorlds.add(session.worldKey)
        session.clientEyeY = session.y + playerEyeOffset(session)

        val metadataPacket = PlayPackets.playerSharedFlagsMetadataPacket(
            entityId = session.entityId,
            sneaking = session.sneaking,
            sprinting = session.sprinting,
            swimming = session.swimming,
            usingItemHand = session.usingItemHand
        )

        for ((id, other) in sessions) {
            if (id == channelId || other.worldKey != session.worldKey) continue
            val otherCtx = contexts[id] ?: continue
            if (!otherCtx.channel().isActive) continue
            otherCtx.writeAndFlush(metadataPacket)
        }
    }

    fun updatePlayerInputState(
        channelId: ChannelId,
        forward: Boolean,
        backward: Boolean,
        left: Boolean,
        right: Boolean,
        jump: Boolean,
        sneaking: Boolean,
        sprinting: Boolean,
        swimming: Boolean
    ) {
        val session = sessions[channelId] ?: return
        val wasSneaking = session.sneaking
        val jumpPressed = jump && !session.previousInputJump
        session.inputForward = forward
        session.inputBackward = backward
        session.inputLeft = left
        session.inputRight = right
        session.inputJump = jump
        session.previousInputJump = jump
        if (jumpPressed) {
            val fromGround = session.onGround
            val airJumpAttemptCount = if (fromGround) {
                0
            } else {
                val next = session.airJumpAttemptCount + 1
                session.airJumpAttemptCount = next
                next
            }
            org.macaroon3145.plugin.PluginSystem.onPlayerJump(
                session = session,
                x = session.x,
                y = session.y,
                z = session.z,
                yaw = session.yaw,
                pitch = session.pitch,
                fromGround = fromGround,
                airJumpAttemptCount = airJumpAttemptCount
            )
        }
        updateAndBroadcastPlayerState(
            channelId = channelId,
            sneaking = sneaking,
            sprinting = sprinting,
            swimming = swimming
        )
        // Some clients emit dismount intent via input sneak flag while mounted
        // (without a distinct Entity Action packet in the same moment).
        if (!wasSneaking && sneaking && session.ridingAnimalEntityId >= 0) {
            handlePlayerDismountRequest(channelId)
        }
    }

    fun updatePlayerFlyingState(channelId: ChannelId, flying: Boolean) {
        val session = sessions[channelId] ?: return
        session.flying = flying
        if (session.gameMode == GAME_MODE_CREATIVE && flying) {
            session.fallDistance = 0.0
        }
    }

    private fun playerEyeOffset(session: PlayerSession): Double {
        return when {
            session.swimming -> 0.4
            session.sneaking -> 1.27
            else -> 1.62
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
        val existing = sessions[channelId]
        if (existing != null) {
            dismountPlayerFromAnimal(existing)
            when (existing.openContainerType) {
                CONTAINER_TYPE_CRAFTING_TABLE -> closeCraftingTable(existing)
                CONTAINER_TYPE_FURNACE -> closeFurnace(existing)
            }
        }
        val removed = sessions.remove(channelId) ?: return
        contexts.remove(channelId)
        channelFlushStates.remove(channelId)
        itemTextMetaByPlayerUuid.remove(removed.profile.uuid)
        attackCooldownActiveChannelIds.remove(channelId)
        animalSourceDirtyWorlds.add(removed.worldKey)
        enderPearlCooldownEndNanosByPlayerEntityId.remove(removed.entityId)
        chorusFruitCooldownEndNanosByPlayerEntityId.remove(removed.entityId)
        lastUseItemSequenceByPlayerEntityId.remove(removed.entityId)
        lastPushingChunkByPlayerEntityId.remove(removed.entityId)
        val removedStamp = activeSimulationStampByPlayerEntityId.remove(removed.entityId)
        val removedFootprint = activeSimulationChunksByPlayerEntityId.remove(removed.entityId)
        if (removedStamp != null && removedFootprint != null) {
            removeSimulationFootprintRefs(removedStamp.worldKey, removedFootprint)
            val previous = activeSimulationChunksCacheByWorld[removedStamp.worldKey] ?: emptySet()
            val refs = activeSimulationRefCountByWorld[removedStamp.worldKey]
            if (refs == null || refs.isEmpty()) {
                activeSimulationChunksCacheByWorld.remove(removedStamp.worldKey)
                recordActiveSimulationChunkDelta(removedStamp.worldKey, previous, emptySet())
            } else {
                val next = HashSet(refs.keys)
                activeSimulationChunksCacheByWorld[removedStamp.worldKey] = next
                recordActiveSimulationChunkDelta(removedStamp.worldKey, previous, next)
            }
        }
        removeRetainedBaseChunksForSession(removed)

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

        val defaultQuitMessage = ServerI18n.translate(
            "multiplayer.player.left",
            listOf(ServerI18n.componentToText(playerNameComponent(removed)))
        )
        val quitMessage = org.macaroon3145.plugin.PluginSystem.onPlayerQuit(removed, defaultQuitMessage)
        if (quitMessage == null) {
            broadcastSystemMessage(
                removed.worldKey,
                PlayPackets.ChatComponent.Translate(
                    key = "multiplayer.player.left",
                    args = listOf(playerNameComponent(removed)),
                    color = "yellow"
                )
            )
        } else {
            broadcastSystemMessage(
                removed.worldKey,
                PlayPackets.ChatComponent.Text(
                    text = quitMessage,
                    color = "yellow"
                )
            )
        }
        ServerI18n.log("aerogel.log.info.player.left", removed.profile.username)
    }

    fun shutdown() {
        val shutdownMessagePacket = PlayPackets.systemChatPacket(
            PlayPackets.ChatComponent.Translate("multiplayer.disconnect.server_shutdown")
        )
        val notifyFutures = ArrayList<ChannelFuture>(contexts.size)
        for ((_, ctx) in contexts) {
            if (!ctx.channel().isActive) continue
            val future = runCatching { ctx.writeAndFlush(shutdownMessagePacket) }.getOrNull() ?: continue
            notifyFutures.add(future)
        }
        for (future in notifyFutures) {
            runCatching { future.syncUninterruptibly() }
        }

        val closeFutures = ArrayList<ChannelFuture>(contexts.size)
        for ((_, ctx) in contexts) {
            val future = runCatching { ctx.close() }.getOrNull() ?: continue
            closeFutures.add(future)
        }
        for (future in closeFutures) {
            runCatching { future.syncUninterruptibly() }
        }
        channelFlushStates.clear()
        contexts.clear()
        sessions.clear()
        itemTextMetaByPlayerUuid.clear()
        animalSourceDirtyWorlds.clear()
        animalRideControlsActiveWorlds.clear()
        attackCooldownActiveChannelIds.clear()
        chorusFruitCooldownEndNanosByPlayerEntityId.clear()
        lastUseItemSequenceByPlayerEntityId.clear()
        activeSimulationChunksCacheByWorld.clear()
        activeSimulationRefCountByWorld.clear()
        activeSimulationStampByPlayerEntityId.clear()
        activeSimulationChunksByPlayerEntityId.clear()
        clearActiveSimulationChunkDeltas()
        activeSimulationLastMaxDistanceChunks = -1
        retainedBaseChunksCacheByWorld.clear()
        retainedBaseRefCountByWorld.clear()
        retainedBaseChunksByPlayerEntityId.clear()
        retainedBaseWorldKeyByPlayerEntityId.clear()
        saddledAnimalEntityIds.clear()
        animalRiderEntityIdByAnimalEntityId.clear()
        animalEntityIdByRiderEntityId.clear()
        pigBoostStatesByAnimalEntityId.clear()
    }

    fun broadcastChat(channelId: ChannelId, message: String) {
        val sender = sessions[channelId] ?: return
        val clean = message.trim()
        if (clean.isEmpty()) return
        if (clean.startsWith("/")) {
            dispatchCommandAsync(sender, clean)
            return
        }
        ServerI18n.log("aerogel.log.chat.message", sender.profile.username, clean)
        val packet = PlayPackets.systemChatPacket(
            PlayPackets.ChatComponent.Text(
                text = "<",
                extra = listOf(
                    playerNameComponent(sender),
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

    fun registerDynamicCommand(name: String, aliases: List<String>, command: Command) {
        commandDispatcher.register(name, command)
        for (alias in aliases) {
            commandDispatcher.register(alias, command)
        }
    }

    fun unregisterDynamicCommand(name: String, aliases: List<String>) {
        commandDispatcher.unregister(name)
        for (alias in aliases) {
            commandDispatcher.unregister(alias)
        }
    }

    fun dynamicAndBuiltinCommandNames(): Set<String> = commandDispatcher.commandNames()

    fun sendPluginMessage(channelId: ChannelId, message: String) {
        val session = sessions[channelId] ?: return
        sendSystemComponent(session, parsePluginStyledMessage(message))
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
            "time" -> timeCommandCompletions(sender, providedArgs, activeArgIndex, activeArgPrefix)
            "perf" -> perfCommandCompletions(activeArgIndex, activeArgPrefix)
            "reload" -> reloadCommandCompletions(activeArgIndex, activeArgPrefix)
            else -> emptyList()
        }
    }

    private fun reloadCommandCompletions(
        activeArgIndex: Int,
        activeArgPrefix: String
    ): List<String> {
        if (activeArgIndex != 0) return emptyList()
        return org.macaroon3145.plugin.PluginSystem.reloadTargetNames().asSequence()
            .filter { it.startsWith(activeArgPrefix, ignoreCase = true) }
            .distinct()
            .sortedWith(String.CASE_INSENSITIVE_ORDER)
            .toList()
    }

    private fun timeCommandCompletions(
        sender: PlayerSession?,
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
            2 -> if (sender == null && subcommand in subcommands) worldKeys else emptyList()
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

    private fun perfCommandCompletions(
        activeArgIndex: Int,
        activeArgPrefix: String
    ): List<String> {
        if (activeArgIndex != 0) return emptyList()
        val candidates = WorldManager.allWorlds()
            .asSequence()
            .map { it.key.substringAfter(':', it.key) }
            .sortedWith(String.CASE_INSENSITIVE_ORDER)
            .toList()
        return candidates.asSequence()
            .filter { it.startsWith(activeArgPrefix, ignoreCase = true) }
            .distinct()
            .toList()
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
            if (!org.macaroon3145.plugin.PluginSystem.beforeCommandDispatch(sender, rawCommand)) {
                return@execute
            }
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
        run {
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
        run {
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
        pitch: Float?,
        reason: MoveReason = MoveReason.TELEPORT
    ): Boolean {
        if (y < -64.0 || y > 320.0) return false
        var targetX = x
        var targetY = y
        var targetZ = z
        val hasRotation = yaw != null && pitch != null
        var nextYaw = yaw ?: target.yaw
        var nextPitch = pitch ?: target.pitch
        val moveDecision = org.macaroon3145.plugin.PluginSystem.beforePlayerMove(
            session = target,
            toX = targetX,
            toY = targetY,
            toZ = targetZ,
            toYaw = nextYaw,
            toPitch = nextPitch,
            reason = reason
        )
        if (moveDecision.cancelPosition) {
            targetX = target.x
            targetY = target.y
            targetZ = target.z
        }
        if (moveDecision.cancelRotation) {
            nextYaw = target.yaw
            nextPitch = target.pitch
        }
        if (moveDecision.cancelPosition || moveDecision.cancelRotation) {
            resyncPlayerPositionToSelf(
                session = target,
                x = targetX,
                y = targetY,
                z = targetZ,
                yaw = nextYaw,
                pitch = nextPitch,
                suppressNextEvent = true
            )
        }
        if (moveDecision.cancelAll) {
            return false
        }
        val selfCtx = contexts[target.channelId]
        if (selfCtx != null && selfCtx.channel().isActive) {
            selfCtx.executor().execute {
                if (selfCtx.channel().isActive) {
                    val teleportId = nextTeleportId(target)
                    val packet = if (hasRotation) {
                        PlayPackets.playerPositionPacket(
                            teleportId = teleportId,
                            x = targetX,
                            y = targetY,
                            z = targetZ,
                            yaw = nextYaw,
                            pitch = nextPitch,
                            relativeFlags = 0
                        )
                    } else {
                        // Keep client yaw/pitch unchanged by using relative-rotation flags with zero deltas.
                        PlayPackets.playerPositionPacket(
                            teleportId = teleportId,
                            x = targetX,
                            y = targetY,
                            z = targetZ,
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
            x = targetX,
            y = targetY,
            z = targetZ,
            yaw = nextYaw,
            pitch = nextPitch,
            onGround = false,
            reason = reason,
            fireMoveEvents = false
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

    private fun parsePluginStyledMessage(raw: String): PlayPackets.ChatComponent {
        if (raw.isEmpty()) return PlayPackets.ChatComponent.Text("")
        val segments = ArrayList<PlayPackets.ChatComponent>()
        var index = 0
        var color: String? = null
        var bold: Boolean? = null
        var italic: Boolean? = null
        var underlined: Boolean? = null

        fun emitText(text: String) {
            if (text.isEmpty()) return
            segments += PlayPackets.ChatComponent.Text(
                text = text,
                color = color,
                bold = bold,
                italic = italic,
                underlined = underlined
            )
        }

        while (index < raw.length) {
            val tagStart = raw.indexOf('<', index)
            if (tagStart < 0) {
                emitText(raw.substring(index))
                break
            }
            if (tagStart > index) {
                emitText(raw.substring(index, tagStart))
            }
            val tagEnd = raw.indexOf('>', tagStart + 1)
            if (tagEnd < 0) {
                emitText(raw.substring(tagStart))
                break
            }
            val token = raw.substring(tagStart + 1, tagEnd).trim().lowercase(Locale.ROOT)
            when {
                token.matches(Regex("#[0-9a-f]{6}")) -> color = token
                token == "bold" -> bold = true
                token == "/bold" -> bold = false
                token == "italic" -> italic = true
                token == "/italic" -> italic = false
                token == "underlined" -> underlined = true
                token == "/underlined" -> underlined = false
                token == "reset" -> {
                    color = null
                    bold = null
                    italic = null
                    underlined = null
                }
                else -> emitText(raw.substring(tagStart, tagEnd + 1))
            }
            index = tagEnd + 1
        }

        if (segments.isEmpty()) return PlayPackets.ChatComponent.Text(raw)
        if (segments.size == 1) return segments[0]
        val first = segments.first() as? PlayPackets.ChatComponent.Text
        if (first != null) {
            return first.copy(extra = first.extra + segments.drop(1))
        }
        return PlayPackets.ChatComponent.Text("", extra = segments)
    }

    private fun maybeSendChunkMsptActionBar(nowNanos: Long) {
        val sweepAt = nextChunkMsptActionBarSweepAtNanos
        if (sweepAt > 0L && nowNanos < sweepAt) return
        var minNextDue = Long.MAX_VALUE
        for (session in sessions.values) {
            val dueAt = session.lastChunkMsptActionBarAtNanos + CHUNK_MSPT_ACTION_BAR_INTERVAL_NANOS
            if (nowNanos < dueAt) {
                if (dueAt < minNextDue) minNextDue = dueAt
                continue
            }
            val ctx = contexts[session.channelId] ?: continue
            if (!ctx.channel().isActive) continue
            val world = WorldManager.world(session.worldKey) ?: continue
            val isIdle = world.isChunkIdle(session.centerChunkX, session.centerChunkZ)
            val message = if (isIdle) {
                String.format(
                    Locale.ROOT,
                    "Chunk (%d, %d) 유휴 상태",
                    session.centerChunkX,
                    session.centerChunkZ
                )
            } else {
                val mspt = world.chunkEwmaMspt(session.centerChunkX, session.centerChunkZ)
                val tps = world.chunkEwmaTps(session.centerChunkX, session.centerChunkZ)
                String.format(
                    Locale.ROOT,
                    "Chunk (%d, %d) MSPT: %.3f ms | TPS: %.2f",
                    session.centerChunkX,
                    session.centerChunkZ,
                    mspt,
                    tps
                )
            }
            val packet = PlayPackets.systemChatPacket(
                PlayPackets.ChatComponent.Text(message),
                overlay = true
            )
            session.lastChunkMsptActionBarAtNanos = nowNanos
            val nextDue = nowNanos + CHUNK_MSPT_ACTION_BAR_INTERVAL_NANOS
            if (nextDue < minNextDue) minNextDue = nextDue
            ctx.executor().execute {
                if (ctx.channel().isActive) {
                    ctx.writeAndFlush(packet)
                }
            }
        }
        nextChunkMsptActionBarSweepAtNanos =
            if (minNextDue == Long.MAX_VALUE) nowNanos + CHUNK_MSPT_ACTION_BAR_INTERVAL_NANOS
            else minNextDue
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
        val packetComponent = if (key.startsWith("aerogel.")) {
            val renderedArgs = args.map { ServerI18n.componentToText(it) }.toTypedArray()
            PlayPackets.ChatComponent.Text(
                text = ServerI18n.trFor(session.locale, key, *renderedArgs),
                color = color
            )
        } else {
            PlayPackets.ChatComponent.Translate(
                key = key,
                args = args.toList(),
                color = color
            )
        }
        val packet = PlayPackets.systemChatPacket(
            packetComponent
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
        val message = if (key.startsWith("aerogel.")) {
            val renderedArgs = args.map { ServerI18n.componentToText(it) }.toTypedArray()
            PlayPackets.ChatComponent.Text(
                text = ServerI18n.trFor(session.locale, key, *renderedArgs),
                italic = true
            )
        } else {
            PlayPackets.ChatComponent.Translate(key = key, args = args.toList(), italic = true)
        }
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

    private fun playerNameComponent(session: PlayerSession): PlayPackets.ChatComponent.Text {
        return playerNameComponent(
            profile = session.profile,
            displayName = effectiveDisplayName(session)
        )
    }

    private fun playerNameComponent(profile: ConnectionProfile): PlayPackets.ChatComponent.Text {
        val resolvedDisplayName = sessions.values
            .firstOrNull { it.profile.uuid == profile.uuid }
            ?.let(::effectiveDisplayName)
            ?: profile.username
        return playerNameComponent(profile, resolvedDisplayName)
    }

    private fun playerNameComponent(profile: ConnectionProfile, displayName: String): PlayPackets.ChatComponent.Text {
        return PlayPackets.ChatComponent.Text(
            text = displayName,
            hoverEntity = PlayPackets.HoverEntity(
                entityType = "minecraft:player",
                uuid = profile.uuid,
                name = displayName
            )
        )
    }

    private fun effectiveDisplayName(session: PlayerSession): String {
        return session.displayName?.takeIf { it.isNotEmpty() } ?: session.profile.username
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
        animalSourceDirtyWorlds.add(session.worldKey)
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
            val previousOffhandItemId = session.offhandItemId
            val previousOffhandCount = session.offhandItemCount
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
            maybeBroadcastOffhandSlotChangeSound(
                session = session,
                previousItemId = previousOffhandItemId,
                previousCount = previousOffhandCount,
                nextItemId = session.offhandItemId,
                nextCount = session.offhandItemCount
            )
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
            val previousItemId = session.armorItemIds[armorIndex]
            val previousCount = session.armorItemCounts[armorIndex]
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
            maybeBroadcastArmorSlotChangeSound(
                session = session,
                previousItemId = previousItemId,
                previousCount = previousCount,
                nextItemId = session.armorItemIds[armorIndex],
                nextCount = session.armorItemCounts[armorIndex]
            )
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

    fun handleContainerClick(
        channelId: ChannelId,
        containerId: Int,
        _stateId: Int,
        slot: Int,
        button: Int,
        clickType: Int
    ) {
        val session = sessions[channelId] ?: return
        if (clickType == CLICK_TYPE_QUICK_CRAFT) {
            val shouldResync = handleQuickCraftDrag(session, containerId, slot, button)
            if (shouldResync) {
                resyncContainerForSession(session, containerId)
            }
            return
        }
        resetQuickCraftDrag(session)
        if (clickType == CLICK_TYPE_QUICK_MOVE) {
            handleQuickMoveClick(session, containerId, slot)
            resyncContainerForSession(session, containerId)
            return
        }
        if (clickType == CLICK_TYPE_SWAP) {
            handleHotbarSwapClick(session, containerId, slot, button)
            resyncContainerForSession(session, containerId)
            return
        }
        if (clickType == CLICK_TYPE_PICKUP_ALL) {
            handlePickupAllDoubleClick(session, containerId, slot, button)
            resyncContainerForSession(session, containerId)
            return
        }
        // Support vanilla pickup clicks only; other modes are safely ignored with a full resync.
        if (clickType != CLICK_TYPE_PICKUP) {
            resyncContainerForSession(session, containerId)
            return
        }
        when {
            containerId == PLAYER_INVENTORY_CONTAINER_ID -> {
                handlePlayerInventoryPickupClick(session, slot, button)
                resyncContainerForSession(session, PLAYER_INVENTORY_CONTAINER_ID)
            }
            session.openContainerType == CONTAINER_TYPE_CRAFTING_TABLE && session.openContainerId == containerId -> {
                handleCraftingTablePickupClick(session, slot, button)
                resyncContainerForSession(session, containerId)
            }
            session.openContainerType == CONTAINER_TYPE_FURNACE && session.openContainerId == containerId -> {
                handleFurnacePickupClick(session, containerId, slot, button)
                resyncContainerForSession(session, containerId)
            }
            else -> {
                // stale container id from client
                resyncContainerForSession(
                    session,
                    when (session.openContainerType) {
                        CONTAINER_TYPE_CRAFTING_TABLE, CONTAINER_TYPE_FURNACE -> session.openContainerId
                        else -> PLAYER_INVENTORY_CONTAINER_ID
                    }
                )
            }
        }
    }

    private fun handleQuickCraftDrag(
        session: PlayerSession,
        containerId: Int,
        slot: Int,
        button: Int
    ): Boolean {
        val stage = button and 3
        val mouseButton = (button shr 2) and 3
        when (stage) {
            0 -> {
                if (!isQuickCraftContainerValid(session, containerId)) {
                    resetQuickCraftDrag(session)
                    return true
                }
                if (mouseButton !in 0..1) {
                    resetQuickCraftDrag(session)
                    return true
                }
                session.quickCraftActive = true
                session.quickCraftContainerId = containerId
                session.quickCraftMouseButton = mouseButton
                session.quickCraftSlots.clear()
                return false
            }
            1 -> {
                if (!session.quickCraftActive || session.quickCraftContainerId != containerId) return true
                if (session.cursorItemId < 0 || session.cursorItemCount <= 0) return true
                if (!isQuickCraftTargetSlot(session, containerId, slot)) return true
                session.quickCraftSlots.add(slot)
                return false
            }
            2 -> {
                if (!session.quickCraftActive || session.quickCraftContainerId != containerId) {
                    resetQuickCraftDrag(session)
                    return true
                }
                applyQuickCraftDrag(session, containerId)
                resetQuickCraftDrag(session)
                return true
            }
            else -> {
                resetQuickCraftDrag(session)
                return true
            }
        }
    }

    private fun applyQuickCraftDrag(session: PlayerSession, containerId: Int) {
        if (session.cursorItemId < 0 || session.cursorItemCount <= 0) return
        val targets = session.quickCraftSlots.toList().sorted()
        if (targets.isEmpty()) return
        if (session.quickCraftMouseButton == 1) {
            for (slot in targets) {
                if (session.cursorItemCount <= 0) break
                quickCraftPlaceIntoSlot(session, containerId, slot, 1)
            }
            return
        }
        // Left-drag: distribute as evenly as possible across tracked slots.
        var placedAny: Boolean
        do {
            placedAny = false
            for (slot in targets) {
                if (session.cursorItemCount <= 0) break
                val placed = quickCraftPlaceIntoSlot(session, containerId, slot, 1)
                if (placed > 0) placedAny = true
            }
        } while (placedAny && session.cursorItemCount > 0)
    }

    private fun quickCraftPlaceIntoSlot(
        session: PlayerSession,
        containerId: Int,
        slot: Int,
        maxToPlace: Int
    ): Int {
        if (maxToPlace <= 0) return 0
        if (session.cursorItemId < 0 || session.cursorItemCount <= 0) return 0
        val placeCap = maxToPlace.coerceAtMost(session.cursorItemCount)
        if (placeCap <= 0) return 0

        val (currentId, currentCount) = readContainerSlot(session, containerId, slot) ?: return 0
        if (currentId < 0 || currentCount <= 0) {
            writeContainerSlot(session, containerId, slot, session.cursorItemId, placeCap)
            session.cursorItemCount -= placeCap
            if (session.cursorItemCount <= 0) {
                session.cursorItemId = -1
                session.cursorItemCount = 0
            }
            onContainerSlotMutated(session, containerId, slot)
            return placeCap
        }
        if (currentId != session.cursorItemId) return 0
        if (currentCount >= MAX_HOTBAR_STACK_SIZE) return 0
        val canAdd = (MAX_HOTBAR_STACK_SIZE - currentCount).coerceAtMost(placeCap)
        if (canAdd <= 0) return 0
        writeContainerSlot(session, containerId, slot, currentId, currentCount + canAdd)
        session.cursorItemCount -= canAdd
        if (session.cursorItemCount <= 0) {
            session.cursorItemId = -1
            session.cursorItemCount = 0
        }
        onContainerSlotMutated(session, containerId, slot)
        return canAdd
    }

    private fun onContainerSlotMutated(session: PlayerSession, containerId: Int, slot: Int) {
        if (containerId == PLAYER_INVENTORY_CONTAINER_ID && slot in 36..44) {
            val hotbar = slot - 36
            if (hotbar == session.selectedHotbarSlot) {
                session.selectedBlockStateId = if (session.hotbarItemCounts[hotbar] > 0) session.hotbarBlockStateIds[hotbar] else 0
                broadcastHeldItemEquipment(session)
            }
            return
        }
        if (session.openContainerType == CONTAINER_TYPE_CRAFTING_TABLE && containerId == session.openContainerId && slot in TABLE_PLAYER_HOTBAR_SLOT_RANGE) {
            val hotbar = slot - TABLE_PLAYER_HOTBAR_SLOT_RANGE.first
            if (hotbar == session.selectedHotbarSlot) {
                session.selectedBlockStateId = if (session.hotbarItemCounts[hotbar] > 0) session.hotbarBlockStateIds[hotbar] else 0
                broadcastHeldItemEquipment(session)
            }
            return
        }
        if (session.openContainerType == CONTAINER_TYPE_FURNACE && containerId == session.openContainerId && slot in FURNACE_PLAYER_HOTBAR_SLOT_RANGE) {
            val hotbar = slot - FURNACE_PLAYER_HOTBAR_SLOT_RANGE.first
            if (hotbar == session.selectedHotbarSlot) {
                session.selectedBlockStateId = if (session.hotbarItemCounts[hotbar] > 0) session.hotbarBlockStateIds[hotbar] else 0
                broadcastHeldItemEquipment(session)
            }
        }
    }

    private fun readContainerSlot(session: PlayerSession, containerId: Int, slot: Int): Pair<Int, Int>? {
        if (containerId == PLAYER_INVENTORY_CONTAINER_ID) {
            if (slot in PLAYER_CRAFT_INPUT_SLOT_RANGE) {
                val idx = slot - PLAYER_CRAFT_INPUT_SLOT_RANGE.first
                return session.playerCraftItemIds[idx] to session.playerCraftItemCounts[idx]
            }
            val accessor = inventorySlotAccessor(session, slot, includeOffhand = true) ?: return null
            return accessor.first.invoke()
        }
        if (session.openContainerType == CONTAINER_TYPE_CRAFTING_TABLE && containerId == session.openContainerId) {
            if (slot in TABLE_CRAFT_INPUT_SLOT_RANGE) {
                val idx = slot - TABLE_CRAFT_INPUT_SLOT_RANGE.first
                return session.tableCraftItemIds[idx] to session.tableCraftItemCounts[idx]
            }
            val accessor = inventorySlotAccessorForTable(session, slot) ?: return null
            return accessor.first.invoke()
        }
        if (session.openContainerType == CONTAINER_TYPE_FURNACE && containerId == session.openContainerId) {
            val furnace = openFurnaceState(session) ?: return null
            return when (slot) {
                FURNACE_INPUT_SLOT -> run { furnace.inputItemId to furnace.inputCount }
                FURNACE_FUEL_SLOT -> run { furnace.fuelItemId to furnace.fuelCount }
                FURNACE_RESULT_SLOT -> run { furnace.resultItemId to furnace.resultCount }
                else -> {
                    val accessor = inventorySlotAccessorForFurnace(session, slot) ?: return null
                    accessor.first.invoke()
                }
            }
        }
        return null
    }

    private fun writeContainerSlot(session: PlayerSession, containerId: Int, slot: Int, itemId: Int, count: Int) {
        if (containerId == PLAYER_INVENTORY_CONTAINER_ID) {
            if (slot in PLAYER_CRAFT_INPUT_SLOT_RANGE) {
                val idx = slot - PLAYER_CRAFT_INPUT_SLOT_RANGE.first
                writeCraftSlot(session.playerCraftItemIds, session.playerCraftItemCounts, idx, itemId, count)
                return
            }
            val accessor = inventorySlotAccessor(session, slot, includeOffhand = true) ?: return
            accessor.second.invoke(itemId, count)
            return
        }
        if (session.openContainerType == CONTAINER_TYPE_CRAFTING_TABLE && containerId == session.openContainerId) {
            if (slot in TABLE_CRAFT_INPUT_SLOT_RANGE) {
                val idx = slot - TABLE_CRAFT_INPUT_SLOT_RANGE.first
                writeCraftSlot(session.tableCraftItemIds, session.tableCraftItemCounts, idx, itemId, count)
                return
            }
            val accessor = inventorySlotAccessorForTable(session, slot) ?: return
            accessor.second.invoke(itemId, count)
            return
        }
        if (session.openContainerType == CONTAINER_TYPE_FURNACE && containerId == session.openContainerId) {
            val furnace = openFurnaceState(session) ?: return
            when (slot) {
                FURNACE_INPUT_SLOT -> writeFurnaceSlot(
                    furnace,
                    slot = FURNACE_INPUT_SLOT,
                    itemId = itemId,
                    count = count
                )
                FURNACE_FUEL_SLOT -> writeFurnaceSlot(
                    furnace,
                    slot = FURNACE_FUEL_SLOT,
                    itemId = itemId,
                    count = count
                )
                FURNACE_RESULT_SLOT -> writeFurnaceSlot(
                    furnace,
                    slot = FURNACE_RESULT_SLOT,
                    itemId = itemId,
                    count = count
                )
                else -> {
                    val accessor = inventorySlotAccessorForFurnace(session, slot) ?: return
                    accessor.second.invoke(itemId, count)
                }
            }
            if (slot == FURNACE_INPUT_SLOT || slot == FURNACE_FUEL_SLOT || slot == FURNACE_RESULT_SLOT) {
                refreshFurnaceActivity(
                    key = FurnaceKey(session.worldKey, session.openFurnaceX, session.openFurnaceY, session.openFurnaceZ),
                    furnace = furnace
                )
            }
            return
        }
    }

    private fun inventorySlotAccessorForTable(
        session: PlayerSession,
        slot: Int
    ): Pair<() -> Pair<Int, Int>, (Int, Int) -> Unit>? {
        val mapped = when (slot) {
            in TABLE_PLAYER_MAIN_SLOT_RANGE -> slot - TABLE_PLAYER_MAIN_SLOT_RANGE.first
            in TABLE_PLAYER_HOTBAR_SLOT_RANGE -> 27 + (slot - TABLE_PLAYER_HOTBAR_SLOT_RANGE.first)
            else -> -1
        }
        if (mapped !in 0..35) return null
        val inventorySlot = if (mapped < 27) inventorySlotForMainInventoryIndex(mapped) else 36 + (mapped - 27)
        return inventorySlotAccessor(session, inventorySlot, includeOffhand = false)
    }

    private fun inventorySlotAccessorForFurnace(
        session: PlayerSession,
        slot: Int
    ): Pair<() -> Pair<Int, Int>, (Int, Int) -> Unit>? {
        val mapped = when (slot) {
            in FURNACE_PLAYER_MAIN_SLOT_RANGE -> slot - FURNACE_PLAYER_MAIN_SLOT_RANGE.first
            in FURNACE_PLAYER_HOTBAR_SLOT_RANGE -> 27 + (slot - FURNACE_PLAYER_HOTBAR_SLOT_RANGE.first)
            else -> -1
        }
        if (mapped !in 0..35) return null
        val inventorySlot = if (mapped < 27) inventorySlotForMainInventoryIndex(mapped) else 36 + (mapped - 27)
        return inventorySlotAccessor(session, inventorySlot, includeOffhand = false)
    }

    private fun isQuickCraftContainerValid(session: PlayerSession, containerId: Int): Boolean {
        if (containerId == PLAYER_INVENTORY_CONTAINER_ID) return true
        return when (session.openContainerType) {
            CONTAINER_TYPE_CRAFTING_TABLE, CONTAINER_TYPE_FURNACE -> containerId == session.openContainerId
            else -> false
        }
    }

    private fun isQuickCraftTargetSlot(session: PlayerSession, containerId: Int, slot: Int): Boolean {
        if (containerId == PLAYER_INVENTORY_CONTAINER_ID) {
            if (slot in PLAYER_CRAFT_INPUT_SLOT_RANGE) return true
            return inventorySlotAccessor(session, slot, includeOffhand = true) != null
        }
        if (session.openContainerType == CONTAINER_TYPE_CRAFTING_TABLE && containerId == session.openContainerId) {
            if (slot in TABLE_CRAFT_INPUT_SLOT_RANGE) return true
            return inventorySlotAccessorForTable(session, slot) != null
        }
        if (session.openContainerType == CONTAINER_TYPE_FURNACE && containerId == session.openContainerId) {
            return when (slot) {
                FURNACE_INPUT_SLOT, FURNACE_FUEL_SLOT -> true
                FURNACE_RESULT_SLOT -> false
                else -> inventorySlotAccessorForFurnace(session, slot) != null
            }
        }
        return false
    }

    private fun resetQuickCraftDrag(session: PlayerSession) {
        session.quickCraftActive = false
        session.quickCraftContainerId = -1
        session.quickCraftMouseButton = 0
        session.quickCraftSlots.clear()
    }

    private fun handlePickupAllDoubleClick(
        session: PlayerSession,
        containerId: Int,
        slot: Int,
        button: Int
    ) {
        if (!isQuickCraftContainerValid(session, containerId)) return
        if (button != 0) return

        val cursorItemId = session.cursorItemId
        val cursorCount = session.cursorItemCount
        if (cursorItemId < 0 || cursorCount <= 0) {
            val clickedItemId = readContainerSlot(session, containerId, slot)?.first ?: -1
            if (clickedItemId < 0) return
            quickMoveAllMatchingFromSlot(session, containerId, slot, clickedItemId)
            return
        }
        val targetItemId = when {
            cursorItemId >= 0 && cursorCount > 0 -> cursorItemId
            else -> readContainerSlot(session, containerId, slot)?.first ?: -1
        }
        if (targetItemId < 0) return
        if (cursorItemId >= 0 && cursorCount > 0 && cursorItemId != targetItemId) return

        var nextCursorCount = if (cursorItemId >= 0 && cursorCount > 0) cursorCount else 0
        if (nextCursorCount >= MAX_HOTBAR_STACK_SIZE) return

        val candidateSlots = pickupAllCandidateSlots(session, containerId)
        for (candidateSlot in candidateSlots) {
            if (nextCursorCount >= MAX_HOTBAR_STACK_SIZE) break
            val (slotItemId, slotItemCount) = readContainerSlot(session, containerId, candidateSlot) ?: continue
            if (slotItemId != targetItemId || slotItemCount <= 0) continue
            val remaining = MAX_HOTBAR_STACK_SIZE - nextCursorCount
            if (remaining <= 0) break
            val take = slotItemCount.coerceAtMost(remaining)
            if (take <= 0) continue
            val nextSlotCount = slotItemCount - take
            if (nextSlotCount <= 0) {
                writeContainerSlot(session, containerId, candidateSlot, -1, 0)
            } else {
                writeContainerSlot(session, containerId, candidateSlot, slotItemId, nextSlotCount)
            }
            onContainerSlotMutated(session, containerId, candidateSlot)
            nextCursorCount += take
        }

        if (nextCursorCount <= 0) return
        session.cursorItemId = targetItemId
        session.cursorItemCount = nextCursorCount
    }

    private fun pickupAllCandidateSlots(session: PlayerSession, containerId: Int): List<Int> {
        return if (containerId == PLAYER_INVENTORY_CONTAINER_ID) {
            buildList(40) {
                addAll(PLAYER_CRAFT_INPUT_SLOT_RANGE)
                addAll(9..35)
                addAll(36..44)
                add(45)
            }
        } else if (session.openContainerType == CONTAINER_TYPE_FURNACE && containerId == session.openContainerId) {
            buildList(38) {
                add(FURNACE_INPUT_SLOT)
                add(FURNACE_FUEL_SLOT)
                addAll(FURNACE_PLAYER_MAIN_SLOT_RANGE)
                addAll(FURNACE_PLAYER_HOTBAR_SLOT_RANGE)
            }
        } else if (session.openContainerType == CONTAINER_TYPE_CRAFTING_TABLE && containerId == session.openContainerId) {
            buildList(45) {
                addAll(TABLE_CRAFT_INPUT_SLOT_RANGE)
                addAll(TABLE_PLAYER_MAIN_SLOT_RANGE)
                addAll(TABLE_PLAYER_HOTBAR_SLOT_RANGE)
            }
        } else {
            emptyList()
        }
    }

    private fun handleQuickMoveClick(session: PlayerSession, containerId: Int, slot: Int) {
        when {
            containerId == PLAYER_INVENTORY_CONTAINER_ID && slot == PLAYER_CRAFT_RESULT_SLOT -> {
                craftAllFromPlayerResultToInventory(session)
            }
            session.openContainerType == CONTAINER_TYPE_CRAFTING_TABLE &&
                containerId == session.openContainerId &&
                slot == TABLE_CRAFT_RESULT_SLOT -> {
                craftAllFromTableResultToInventory(session)
            }
            session.openContainerType == CONTAINER_TYPE_FURNACE &&
                containerId == session.openContainerId &&
                slot == FURNACE_RESULT_SLOT -> {
                quickMoveFromFurnaceResult(session, containerId)
            }
            else -> {
                if (session.openContainerType == CONTAINER_TYPE_FURNACE && containerId == session.openContainerId) {
                    quickMoveWithinFurnace(session, containerId, slot)
                } else if (session.openContainerType == CONTAINER_TYPE_CRAFTING_TABLE && containerId == session.openContainerId) {
                    quickMoveWithinCraftingTable(session, containerId, slot)
                } else {
                    quickMoveSingleStack(session, containerId, slot)
                }
            }
        }
    }

    private fun craftAllFromPlayerResultToInventory(session: PlayerSession) {
        val containerId = PLAYER_INVENTORY_CONTAINER_ID
        val targetSlots = PLAYER_CRAFT_RESULT_QUICK_MOVE_TARGET_SLOTS
        repeat(MAX_SHIFT_CRAFT_ITERATIONS) {
            val match = currentPlayerCraftMatch(session) ?: return
            val resultItemId = itemIdByKey[match.recipe.result.itemKey] ?: return
            val resultCount = match.recipe.result.count
            if (!canInsertIntoContainerSlots(session, containerId, resultItemId, resultCount, targetSlots)) return
            consumeMatchedIngredients(session.playerCraftItemIds, session.playerCraftItemCounts, match.inputSlotsToConsume)
            if (!tryInsertIntoContainerSlots(session, containerId, resultItemId, resultCount, targetSlots)) return
        }
    }

    private fun craftAllFromTableResultToInventory(session: PlayerSession) {
        val containerId = session.openContainerId
        val targetSlots = TABLE_CRAFT_RESULT_QUICK_MOVE_TARGET_SLOTS
        repeat(MAX_SHIFT_CRAFT_ITERATIONS) {
            val match = currentTableCraftMatch(session) ?: return
            val resultItemId = itemIdByKey[match.recipe.result.itemKey] ?: return
            val resultCount = match.recipe.result.count
            if (!canInsertIntoContainerSlots(session, containerId, resultItemId, resultCount, targetSlots)) return
            consumeMatchedIngredients(session.tableCraftItemIds, session.tableCraftItemCounts, match.inputSlotsToConsume)
            if (!tryInsertIntoContainerSlots(session, containerId, resultItemId, resultCount, targetSlots)) return
        }
    }

    private fun canInsertIntoContainerSlots(
        session: PlayerSession,
        containerId: Int,
        itemId: Int,
        itemCount: Int,
        targetSlots: IntArray
    ): Boolean {
        if (itemId < 0 || itemCount <= 0 || targetSlots.isEmpty()) return false
        var remaining = itemCount

        for (slot in targetSlots) {
            val (slotItemId, slotCount) = readContainerSlot(session, containerId, slot) ?: continue
            if (slotItemId != itemId || slotCount <= 0 || slotCount >= MAX_HOTBAR_STACK_SIZE) continue
            remaining -= (MAX_HOTBAR_STACK_SIZE - slotCount)
            if (remaining <= 0) return true
        }
        for (slot in targetSlots) {
            val (slotItemId, slotCount) = readContainerSlot(session, containerId, slot) ?: continue
            if (slotItemId >= 0 && slotCount > 0) continue
            remaining -= MAX_HOTBAR_STACK_SIZE
            if (remaining <= 0) return true
        }
        return false
    }

    private fun tryInsertIntoContainerSlots(
        session: PlayerSession,
        containerId: Int,
        itemId: Int,
        itemCount: Int,
        targetSlots: IntArray
    ): Boolean {
        if (itemId < 0 || itemCount <= 0 || targetSlots.isEmpty()) return false
        var remaining = itemCount

        for (slot in targetSlots) {
            if (remaining <= 0) break
            val (slotItemId, slotCount) = readContainerSlot(session, containerId, slot) ?: continue
            if (slotItemId != itemId || slotCount <= 0 || slotCount >= MAX_HOTBAR_STACK_SIZE) continue
            val added = minOf(MAX_HOTBAR_STACK_SIZE - slotCount, remaining)
            if (added <= 0) continue
            writeContainerSlot(session, containerId, slot, slotItemId, slotCount + added)
            onContainerSlotMutated(session, containerId, slot)
            remaining -= added
        }
        for (slot in targetSlots) {
            if (remaining <= 0) break
            val (slotItemId, slotCount) = readContainerSlot(session, containerId, slot) ?: continue
            if (slotItemId >= 0 && slotCount > 0) continue
            val added = minOf(MAX_HOTBAR_STACK_SIZE, remaining)
            if (added <= 0) continue
            writeContainerSlot(session, containerId, slot, itemId, added)
            onContainerSlotMutated(session, containerId, slot)
            remaining -= added
        }
        return remaining <= 0
    }

    private fun quickMoveFromFurnaceResult(session: PlayerSession, containerId: Int) {
        val furnace = openFurnaceState(session) ?: return
        val resultItemId: Int
        val resultCount: Int
        run {
            resultItemId = furnace.resultItemId
            resultCount = furnace.resultCount
        }
        if (resultItemId < 0 || resultCount <= 0) return
        if (!tryInsertIntoContainerSlots(session, containerId, resultItemId, resultCount, FURNACE_RESULT_QUICK_MOVE_TARGET_SLOTS)) return
        run {
            furnace.resultItemId = -1
            furnace.resultCount = 0
        }
    }

    private fun quickMoveWithinCraftingTable(session: PlayerSession, containerId: Int, slot: Int) {
        if (slot in TABLE_CRAFT_INPUT_SLOT_RANGE) {
            quickMoveSingleStack(session, containerId, slot)
            return
        }
        if (slot !in TABLE_PLAYER_MAIN_SLOT_RANGE && slot !in TABLE_PLAYER_HOTBAR_SLOT_RANGE) {
            quickMoveSingleStack(session, containerId, slot)
            return
        }
        val from = readContainerSlot(session, containerId, slot) ?: return
        val itemId = from.first
        val count = from.second
        if (itemId < 0 || count <= 0) return
        val remainingToGrid = moveItemIntoContainerSlots(
            session = session,
            containerId = containerId,
            itemId = itemId,
            remaining = count,
            targets = TABLE_CRAFT_INPUT_SLOT_RANGE.toList()
        )
        if (remainingToGrid != count) {
            if (remainingToGrid <= 0) writeContainerSlot(session, containerId, slot, -1, 0)
            else writeContainerSlot(session, containerId, slot, itemId, remainingToGrid)
            onContainerSlotMutated(session, containerId, slot)
            return
        }
        quickMoveSingleStack(session, containerId, slot)
    }

    private fun quickMoveWithinFurnace(session: PlayerSession, containerId: Int, slot: Int) {
        val stack = readContainerSlot(session, containerId, slot) ?: return
        val itemId = stack.first
        val count = stack.second
        if (itemId < 0 || count <= 0) return

        val moved = when {
            slot == FURNACE_INPUT_SLOT || slot == FURNACE_FUEL_SLOT -> {
                quickMoveSingleStack(session, containerId, slot)
            }
            slot in FURNACE_PLAYER_MAIN_SLOT_RANGE || slot in FURNACE_PLAYER_HOTBAR_SLOT_RANGE -> {
                if (isFuelItemId(itemId)) {
                    val fuelRemaining = moveItemIntoContainerSlots(
                        session = session,
                        containerId = containerId,
                        itemId = itemId,
                        remaining = count,
                        targets = listOf(FURNACE_FUEL_SLOT)
                    )
                    if (fuelRemaining < count) {
                        if (fuelRemaining <= 0) {
                            writeContainerSlot(session, containerId, slot, -1, 0)
                            onContainerSlotMutated(session, containerId, slot)
                            true
                        } else if (isSmeltableItemId(itemId)) {
                            val smeltRemaining = moveItemIntoContainerSlots(
                                session = session,
                                containerId = containerId,
                                itemId = itemId,
                                remaining = fuelRemaining,
                                targets = listOf(FURNACE_INPUT_SLOT)
                            )
                            if (smeltRemaining <= 0) writeContainerSlot(session, containerId, slot, -1, 0)
                            else writeContainerSlot(session, containerId, slot, itemId, smeltRemaining)
                            onContainerSlotMutated(session, containerId, slot)
                            true
                        } else {
                            writeContainerSlot(session, containerId, slot, itemId, fuelRemaining)
                            onContainerSlotMutated(session, containerId, slot)
                            true
                        }
                    } else if (isSmeltableItemId(itemId)) {
                        moveItemIntoContainerSlots(
                            session = session,
                            containerId = containerId,
                            itemId = itemId,
                            remaining = count,
                            targets = listOf(FURNACE_INPUT_SLOT)
                        ).let { remaining ->
                            if (remaining != count) {
                                if (remaining <= 0) writeContainerSlot(session, containerId, slot, -1, 0)
                                else writeContainerSlot(session, containerId, slot, itemId, remaining)
                                onContainerSlotMutated(session, containerId, slot)
                                true
                            } else {
                                false
                            }
                        }
                    } else {
                        false
                    }
                } else if (isSmeltableItemId(itemId)) {
                    moveItemIntoContainerSlots(
                        session = session,
                        containerId = containerId,
                        itemId = itemId,
                        remaining = count,
                        targets = listOf(FURNACE_INPUT_SLOT)
                    ).let { remaining ->
                        if (remaining != count) {
                            if (remaining <= 0) writeContainerSlot(session, containerId, slot, -1, 0)
                            else writeContainerSlot(session, containerId, slot, itemId, remaining)
                            onContainerSlotMutated(session, containerId, slot)
                            true
                        } else {
                            false
                        }
                    }
                } else {
                    quickMoveSingleStack(session, containerId, slot)
                }
            }
            else -> false
        }
        if (!moved) return
    }

    private fun quickMoveAllMatchingFromSlot(
        session: PlayerSession,
        containerId: Int,
        slot: Int,
        itemId: Int
    ) {
        if (itemId < 0) return
        quickMoveSingleStack(session, containerId, slot)
        val candidates = quickMoveSourceSlots(session, containerId, slot).filter { it != slot }
        for (candidate in candidates) {
            val stack = readContainerSlot(session, containerId, candidate) ?: continue
            if (stack.first != itemId || stack.second <= 0) continue
            quickMoveSingleStack(session, containerId, candidate)
        }
    }

    private fun quickMoveSingleStack(session: PlayerSession, containerId: Int, fromSlot: Int): Boolean {
        val from = readContainerSlot(session, containerId, fromSlot) ?: return false
        val itemId = from.first
        var remaining = from.second
        if (itemId < 0 || remaining <= 0) return false

        val targets = quickMoveTargetSlots(session, containerId, fromSlot, itemId)
        if (targets.isEmpty()) return false
        remaining = moveItemIntoContainerSlots(
            session = session,
            containerId = containerId,
            itemId = itemId,
            remaining = remaining,
            targets = targets
        )
        if (remaining == from.second) return false
        if (remaining <= 0) {
            writeContainerSlot(session, containerId, fromSlot, -1, 0)
        } else {
            writeContainerSlot(session, containerId, fromSlot, itemId, remaining)
        }
        onContainerSlotMutated(session, containerId, fromSlot)
        return true
    }

    private fun moveItemIntoContainerSlots(
        session: PlayerSession,
        containerId: Int,
        itemId: Int,
        remaining: Int,
        targets: List<Int>
    ): Int {
        var outRemaining = remaining
        if (itemId < 0 || outRemaining <= 0) return outRemaining

        for (target in targets) {
            if (outRemaining <= 0) break
            val current = readContainerSlot(session, containerId, target) ?: continue
            val currentId = current.first
            val currentCount = current.second
            if (currentId != itemId || currentCount <= 0 || currentCount >= MAX_HOTBAR_STACK_SIZE) continue
            val add = minOf(MAX_HOTBAR_STACK_SIZE - currentCount, outRemaining)
            if (add <= 0) continue
            writeContainerSlot(session, containerId, target, currentId, currentCount + add)
            onContainerSlotMutated(session, containerId, target)
            outRemaining -= add
        }

        for (target in targets) {
            if (outRemaining <= 0) break
            val current = readContainerSlot(session, containerId, target) ?: continue
            val currentId = current.first
            val currentCount = current.second
            if (currentId >= 0 && currentCount > 0) continue
            val add = minOf(MAX_HOTBAR_STACK_SIZE, outRemaining)
            if (add <= 0) continue
            writeContainerSlot(session, containerId, target, itemId, add)
            onContainerSlotMutated(session, containerId, target)
            outRemaining -= add
        }

        return outRemaining
    }

    private fun quickMoveSourceSlots(session: PlayerSession, containerId: Int, slot: Int): List<Int> {
        if (containerId == PLAYER_INVENTORY_CONTAINER_ID) {
            return when (slot) {
                in 9..35 -> (9..35).toList()
                in 36..44 -> (36..44).toList()
                in PLAYER_CRAFT_INPUT_SLOT_RANGE -> PLAYER_CRAFT_INPUT_SLOT_RANGE.toList()
                in 5..8 -> (5..8).toList()
                45 -> listOf(45)
                else -> emptyList()
            }
        }
        if (session.openContainerType == CONTAINER_TYPE_FURNACE && containerId == session.openContainerId) {
            return when (slot) {
                in FURNACE_PLAYER_MAIN_SLOT_RANGE -> FURNACE_PLAYER_MAIN_SLOT_RANGE.toList()
                in FURNACE_PLAYER_HOTBAR_SLOT_RANGE -> FURNACE_PLAYER_HOTBAR_SLOT_RANGE.toList()
                FURNACE_INPUT_SLOT, FURNACE_FUEL_SLOT, FURNACE_RESULT_SLOT -> listOf(slot)
                else -> emptyList()
            }
        }
        return when (slot) {
            in TABLE_PLAYER_MAIN_SLOT_RANGE -> TABLE_PLAYER_MAIN_SLOT_RANGE.toList()
            in TABLE_PLAYER_HOTBAR_SLOT_RANGE -> TABLE_PLAYER_HOTBAR_SLOT_RANGE.toList()
            in TABLE_CRAFT_INPUT_SLOT_RANGE -> TABLE_CRAFT_INPUT_SLOT_RANGE.toList()
            else -> emptyList()
        }
    }

    private fun quickMoveTargetSlots(session: PlayerSession, containerId: Int, slot: Int, itemId: Int): List<Int> {
        if (containerId == PLAYER_INVENTORY_CONTAINER_ID) {
            return when (slot) {
                in PLAYER_CRAFT_INPUT_SLOT_RANGE -> (9..35).toList() + (36..44).toList()
                in 5..8 -> (9..35).toList() + (36..44).toList()
                45 -> (9..35).toList() + (36..44).toList()
                in 9..35 -> {
                    val out = ArrayList<Int>(37)
                    preferredEquipmentTargetSlot(session, itemId)?.let { out.add(it) }
                    out.addAll(36..44)
                    out
                }
                in 36..44 -> {
                    val out = ArrayList<Int>(37)
                    preferredEquipmentTargetSlot(session, itemId)?.let { out.add(it) }
                    out.addAll(9..35)
                    out
                }
                else -> emptyList()
            }
        }
        if (session.openContainerType == CONTAINER_TYPE_FURNACE && containerId == session.openContainerId) {
            return when (slot) {
                FURNACE_INPUT_SLOT, FURNACE_FUEL_SLOT, FURNACE_RESULT_SLOT ->
                    FURNACE_PLAYER_MAIN_SLOT_RANGE.toList() + FURNACE_PLAYER_HOTBAR_SLOT_RANGE.toList()
                in FURNACE_PLAYER_MAIN_SLOT_RANGE -> FURNACE_PLAYER_HOTBAR_SLOT_RANGE.toList()
                in FURNACE_PLAYER_HOTBAR_SLOT_RANGE -> FURNACE_PLAYER_MAIN_SLOT_RANGE.toList()
                else -> emptyList()
            }
        }
        return when (slot) {
            in TABLE_CRAFT_INPUT_SLOT_RANGE -> TABLE_PLAYER_MAIN_SLOT_RANGE.toList() + TABLE_PLAYER_HOTBAR_SLOT_RANGE.toList()
            in TABLE_PLAYER_MAIN_SLOT_RANGE -> TABLE_PLAYER_HOTBAR_SLOT_RANGE.toList()
            in TABLE_PLAYER_HOTBAR_SLOT_RANGE -> TABLE_PLAYER_MAIN_SLOT_RANGE.toList()
            else -> emptyList()
        }
    }

    private fun preferredEquipmentTargetSlot(session: PlayerSession, itemId: Int): Int? {
        if (itemId < 0 || itemId >= itemKeyById.size) return null
        val itemKey = itemKeyById[itemId]
        val preferredSlot = when {
            itemKey == "minecraft:shield" -> 45
            itemKey == "minecraft:elytra" ||
                itemKey.endsWith("_chestplate") -> 6
            itemKey.endsWith("_leggings") -> 7
            itemKey.endsWith("_boots") -> 8
            itemKey.endsWith("_helmet") ||
                itemKey == "minecraft:turtle_helmet" -> 5
            else -> null
        } ?: return null
        val current = readContainerSlot(session, PLAYER_INVENTORY_CONTAINER_ID, preferredSlot) ?: return null
        if (current.first >= 0 && current.second > 0) return null
        return preferredSlot
    }

    private fun handleHotbarSwapClick(session: PlayerSession, containerId: Int, slot: Int, hotbarButton: Int) {
        if (!isQuickCraftContainerValid(session, containerId)) return
        if (tryCraftResultToHotbarBySwap(session, containerId, slot, hotbarButton)) return

        if (hotbarButton == OFFHAND_SWAP_BUTTON) {
            handleOffhandSwapClick(session, containerId, slot)
            return
        }
        if (hotbarButton !in 0..8) return

        val clicked = readContainerSlot(session, containerId, slot) ?: return
        val clickedItemId = clicked.first
        val clickedCount = clicked.second
        val clickedHotbarIndex = hotbarIndexForContainerSlot(session, containerId, slot)
        if (clickedHotbarIndex == hotbarButton) return

        val targetItemId = session.hotbarItemIds[hotbarButton]
        val targetCount = session.hotbarItemCounts[hotbarButton]
        val targetMeta = captureHotbarMeta(session, hotbarButton)
        val clickedMeta = if (clickedHotbarIndex != null) captureHotbarMeta(session, clickedHotbarIndex) else null

        writeContainerSlot(session, containerId, slot, targetItemId, targetCount)
        setHotbarSlotWithMeta(session, hotbarButton, clickedItemId, clickedCount, clickedMeta)
        if (clickedHotbarIndex != null) {
            setHotbarSlotWithMeta(session, clickedHotbarIndex, targetItemId, targetCount, targetMeta)
        }

        updateSelectedBlockStateAndEquipmentAfterHotbarMutation(
            session = session,
            changedHotbarA = hotbarButton,
            changedHotbarB = clickedHotbarIndex
        )
    }

    private fun tryCraftResultToHotbarBySwap(
        session: PlayerSession,
        containerId: Int,
        slot: Int,
        hotbarButton: Int
    ): Boolean {
        if (hotbarButton !in 0..8) return false
        val isPlayerCraftResult = containerId == PLAYER_INVENTORY_CONTAINER_ID && slot == PLAYER_CRAFT_RESULT_SLOT
        val isTableCraftResult =
            session.openContainerType == CONTAINER_TYPE_CRAFTING_TABLE &&
                containerId == session.openContainerId &&
                slot == TABLE_CRAFT_RESULT_SLOT
        if (!isPlayerCraftResult && !isTableCraftResult) return false

        val match = if (isPlayerCraftResult) currentPlayerCraftMatch(session) else currentTableCraftMatch(session)
        if (match == null) return true
        val resultItemId = itemIdByKey[match.recipe.result.itemKey] ?: return true
        val resultCount = match.recipe.result.count.coerceAtLeast(1)

        val targetItemId = session.hotbarItemIds[hotbarButton]
        val targetCount = session.hotbarItemCounts[hotbarButton]
        val canPlace = when {
            targetItemId < 0 || targetCount <= 0 -> true
            targetItemId == resultItemId && targetCount + resultCount <= MAX_HOTBAR_STACK_SIZE -> true
            else -> false
        }
        if (!canPlace) return true

        if (isPlayerCraftResult) {
            consumeMatchedIngredients(session.playerCraftItemIds, session.playerCraftItemCounts, match.inputSlotsToConsume)
        } else {
            consumeMatchedIngredients(session.tableCraftItemIds, session.tableCraftItemCounts, match.inputSlotsToConsume)
        }

        val nextCount = if (targetItemId < 0 || targetCount <= 0) resultCount else (targetCount + resultCount)
        setHotbarSlotWithMeta(session, hotbarButton, resultItemId, nextCount, null)
        updateSelectedBlockStateAndEquipmentAfterHotbarMutation(
            session = session,
            changedHotbarA = hotbarButton,
            changedHotbarB = null
        )
        return true
    }

    private fun handleOffhandSwapClick(session: PlayerSession, containerId: Int, slot: Int) {
        val clicked = readContainerSlot(session, containerId, slot) ?: return
        val clickedItemId = clicked.first
        val clickedCount = clicked.second
        val clickedHotbarIndex = hotbarIndexForContainerSlot(session, containerId, slot)

        val offhandItemId = session.offhandItemId
        val offhandCount = session.offhandItemCount

        writeContainerSlot(session, containerId, slot, offhandItemId, offhandCount)
        session.offhandItemId = if (clickedItemId >= 0 && clickedCount > 0) clickedItemId else -1
        session.offhandItemCount = if (clickedItemId >= 0 && clickedCount > 0) clickedCount.coerceAtMost(MAX_HOTBAR_STACK_SIZE) else 0
        maybeBroadcastOffhandSlotChangeSound(
            session = session,
            previousItemId = offhandItemId,
            previousCount = offhandCount,
            nextItemId = session.offhandItemId,
            nextCount = session.offhandItemCount
        )

        if (clickedHotbarIndex != null) {
            setHotbarSlotWithMeta(session, clickedHotbarIndex, offhandItemId, offhandCount, null)
        }
        broadcastHeldItemEquipment(session)
    }

    private fun isResultSlot(containerId: Int, session: PlayerSession, slot: Int): Boolean {
        return when {
            containerId == PLAYER_INVENTORY_CONTAINER_ID -> slot == PLAYER_CRAFT_RESULT_SLOT
            session.openContainerType == CONTAINER_TYPE_CRAFTING_TABLE && containerId == session.openContainerId ->
                slot == TABLE_CRAFT_RESULT_SLOT
            session.openContainerType == CONTAINER_TYPE_FURNACE && containerId == session.openContainerId ->
                slot == FURNACE_RESULT_SLOT
            else -> false
        }
    }

    private fun hotbarIndexForContainerSlot(session: PlayerSession, containerId: Int, slot: Int): Int? {
        return when {
            containerId == PLAYER_INVENTORY_CONTAINER_ID && slot in 36..44 -> slot - 36
            session.openContainerType == CONTAINER_TYPE_CRAFTING_TABLE &&
                containerId == session.openContainerId &&
                slot in TABLE_PLAYER_HOTBAR_SLOT_RANGE -> slot - TABLE_PLAYER_HOTBAR_SLOT_RANGE.first
            session.openContainerType == CONTAINER_TYPE_FURNACE &&
                containerId == session.openContainerId &&
                slot in FURNACE_PLAYER_HOTBAR_SLOT_RANGE -> slot - FURNACE_PLAYER_HOTBAR_SLOT_RANGE.first
            else -> null
        }
    }

    private data class HotbarMeta(
        val blockStateId: Int,
        val blockEntityTypeId: Int,
        val blockEntityNbtPayload: ByteArray?
    )

    private fun captureHotbarMeta(session: PlayerSession, hotbar: Int): HotbarMeta {
        return HotbarMeta(
            blockStateId = session.hotbarBlockStateIds[hotbar],
            blockEntityTypeId = session.hotbarBlockEntityTypeIds[hotbar],
            blockEntityNbtPayload = session.hotbarBlockEntityNbtPayloads[hotbar]
        )
    }

    private fun setHotbarSlotWithMeta(
        session: PlayerSession,
        hotbar: Int,
        itemId: Int,
        count: Int,
        inheritedMeta: HotbarMeta?
    ) {
        if (itemId >= 0 && count > 0) {
            session.hotbarItemIds[hotbar] = itemId
            session.hotbarItemCounts[hotbar] = count.coerceAtMost(MAX_HOTBAR_STACK_SIZE)
            if (inheritedMeta != null) {
                session.hotbarBlockStateIds[hotbar] = inheritedMeta.blockStateId
                session.hotbarBlockEntityTypeIds[hotbar] = inheritedMeta.blockEntityTypeId
                session.hotbarBlockEntityNbtPayloads[hotbar] = inheritedMeta.blockEntityNbtPayload
            } else {
                session.hotbarBlockStateIds[hotbar] = itemBlockStateId(itemId)
                session.hotbarBlockEntityTypeIds[hotbar] = -1
                session.hotbarBlockEntityNbtPayloads[hotbar] = null
            }
            return
        }
        session.hotbarItemIds[hotbar] = -1
        session.hotbarItemCounts[hotbar] = 0
        session.hotbarBlockStateIds[hotbar] = 0
        session.hotbarBlockEntityTypeIds[hotbar] = -1
        session.hotbarBlockEntityNbtPayloads[hotbar] = null
    }

    private fun updateSelectedBlockStateAndEquipmentAfterHotbarMutation(
        session: PlayerSession,
        changedHotbarA: Int,
        changedHotbarB: Int?
    ) {
        val selected = session.selectedHotbarSlot.coerceIn(0, 8)
        if (selected == changedHotbarA || selected == changedHotbarB) {
            session.selectedBlockStateId = if (session.hotbarItemCounts[selected] > 0) {
                session.hotbarBlockStateIds[selected]
            } else {
                0
            }
            broadcastHeldItemEquipment(session)
        }
    }

    fun handleContainerClose(channelId: ChannelId, containerId: Int) {
        val session = sessions[channelId] ?: return
        resetQuickCraftDrag(session)
        if (containerId != session.openContainerId) return
        when (session.openContainerType) {
            CONTAINER_TYPE_CRAFTING_TABLE -> closeCraftingTable(session)
            CONTAINER_TYPE_FURNACE -> closeFurnace(session)
        }
    }

    private fun handlePlayerInventoryPickupClick(session: PlayerSession, slot: Int, button: Int) {
        if (slot == PLAYER_CRAFT_RESULT_SLOT) {
            craftFromPlayerResult(session)
            return
        }
        if (slot in PLAYER_CRAFT_INPUT_SLOT_RANGE) {
            val gridIndex = slot - PLAYER_CRAFT_INPUT_SLOT_RANGE.first
            pickupSwap(
                readSlot = { readCraftSlot(session.playerCraftItemIds, session.playerCraftItemCounts, gridIndex) },
                writeSlot = { id, count -> writeCraftSlot(session.playerCraftItemIds, session.playerCraftItemCounts, gridIndex, id, count) },
                session = session,
                rightClick = button == 1
            )
            return
        }
        pickupInventorySlot(session, slot, button == 1, includeOffhand = true)
    }

    private fun handleCraftingTablePickupClick(session: PlayerSession, slot: Int, button: Int) {
        if (slot == TABLE_CRAFT_RESULT_SLOT) {
            craftFromTableResult(session)
            return
        }
        if (slot in TABLE_CRAFT_INPUT_SLOT_RANGE) {
            val gridIndex = slot - TABLE_CRAFT_INPUT_SLOT_RANGE.first
            pickupSwap(
                readSlot = { readCraftSlot(session.tableCraftItemIds, session.tableCraftItemCounts, gridIndex) },
                writeSlot = { id, count -> writeCraftSlot(session.tableCraftItemIds, session.tableCraftItemCounts, gridIndex, id, count) },
                session = session,
                rightClick = button == 1
            )
            return
        }
        pickupInventorySlot(session, slot, button == 1, includeOffhand = false, tableContainer = true)
    }

    private fun handleFurnacePickupClick(session: PlayerSession, containerId: Int, slot: Int, button: Int) {
        if (button !in 0..1) return
        val rightClick = button == 1
        val furnace = openFurnaceState(session) ?: return
        val furnaceKey = FurnaceKey(session.worldKey, session.openFurnaceX, session.openFurnaceY, session.openFurnaceZ)
        when (slot) {
            FURNACE_RESULT_SLOT -> {
                takeFromOutputSlot(
                    readSlot = { furnace.resultItemId to furnace.resultCount },
                    writeSlot = { id, count ->
                        furnace.resultItemId = if (count > 0) id else -1
                        furnace.resultCount = count.coerceAtLeast(0)
                        furnace.dirty = true
                    },
                    session = session,
                    rightClick = rightClick
                )
                refreshFurnaceActivity(furnaceKey, furnace)
                return
            }
            FURNACE_INPUT_SLOT -> {
                pickupSwap(
                    readSlot = { furnace.inputItemId to furnace.inputCount },
                    writeSlot = { id, count -> writeFurnaceSlot(furnace, FURNACE_INPUT_SLOT, id, count) },
                    session = session,
                    rightClick = rightClick
                )
                refreshFurnaceActivity(furnaceKey, furnace)
                return
            }
            FURNACE_FUEL_SLOT -> {
                val cursorItemId = session.cursorItemId
                val cursorCount = session.cursorItemCount
                if (cursorItemId >= 0 && cursorCount > 0 && furnace.fuelCount <= 0 && !isFuelItemId(cursorItemId)) {
                    return
                }
                pickupSwap(
                    readSlot = { furnace.fuelItemId to furnace.fuelCount },
                    writeSlot = { id, count ->
                        if (count > 0 && id >= 0 && !isFuelItemId(id)) return@pickupSwap
                        writeFurnaceSlot(furnace, FURNACE_FUEL_SLOT, id, count)
                    },
                    session = session,
                    rightClick = rightClick
                )
                refreshFurnaceActivity(furnaceKey, furnace)
                return
            }
            else -> {
                pickupInventorySlot(session, slot, rightClick, includeOffhand = false, furnaceContainer = true)
            }
        }
        onContainerSlotMutated(session, containerId, slot)
    }

    private fun pickupInventorySlot(
        session: PlayerSession,
        slot: Int,
        rightClick: Boolean,
        includeOffhand: Boolean,
        tableContainer: Boolean = false,
        furnaceContainer: Boolean = false
    ) {
        val read: (() -> Pair<Int, Int>)?
        val write: ((Int, Int) -> Unit)?
        if (tableContainer) {
            val mapped = when (slot) {
                in TABLE_PLAYER_MAIN_SLOT_RANGE -> slot - TABLE_PLAYER_MAIN_SLOT_RANGE.first
                in TABLE_PLAYER_HOTBAR_SLOT_RANGE -> 27 + (slot - TABLE_PLAYER_HOTBAR_SLOT_RANGE.first)
                else -> -1
            }
            if (mapped in 0..35) {
                val inventorySlot = if (mapped < 27) inventorySlotForMainInventoryIndex(mapped) else 36 + (mapped - 27)
                val pair = inventorySlotAccessor(session, inventorySlot, includeOffhand = false)
                read = pair?.first
                write = pair?.second
            } else {
                read = null
                write = null
            }
        } else if (furnaceContainer) {
            val mapped = when (slot) {
                in FURNACE_PLAYER_MAIN_SLOT_RANGE -> slot - FURNACE_PLAYER_MAIN_SLOT_RANGE.first
                in FURNACE_PLAYER_HOTBAR_SLOT_RANGE -> 27 + (slot - FURNACE_PLAYER_HOTBAR_SLOT_RANGE.first)
                else -> -1
            }
            if (mapped in 0..35) {
                val inventorySlot = if (mapped < 27) inventorySlotForMainInventoryIndex(mapped) else 36 + (mapped - 27)
                val pair = inventorySlotAccessor(session, inventorySlot, includeOffhand = false)
                read = pair?.first
                write = pair?.second
            } else {
                read = null
                write = null
            }
        } else {
            val pair = inventorySlotAccessor(session, slot, includeOffhand)
            read = pair?.first
            write = pair?.second
        }
        if (read == null || write == null) return
        pickupSwap(read, write, session, rightClick)
        if (!tableContainer && slot in 36..44) {
            val hotbar = slot - 36
            if (session.selectedHotbarSlot == hotbar) {
                session.selectedBlockStateId = if (session.hotbarItemCounts[hotbar] > 0) session.hotbarBlockStateIds[hotbar] else 0
                broadcastHeldItemEquipment(session)
            }
        }
    }

    private fun inventorySlotAccessor(
        session: PlayerSession,
        slot: Int,
        includeOffhand: Boolean
    ): Pair<() -> Pair<Int, Int>, (Int, Int) -> Unit>? {
        return when {
            slot in 36..44 -> {
                val hotbar = slot - 36
                ({ session.hotbarItemIds[hotbar] to session.hotbarItemCounts[hotbar] }) to { id, count ->
                    session.hotbarItemIds[hotbar] = id
                    session.hotbarItemCounts[hotbar] = count
                    if (count <= 0 || id < 0) {
                        session.hotbarBlockStateIds[hotbar] = 0
                        session.hotbarBlockEntityTypeIds[hotbar] = -1
                        session.hotbarBlockEntityNbtPayloads[hotbar] = null
                    }
                }
            }
            slot in 9..35 -> {
                val index = mainInventoryIndexForInventorySlot(slot)
                ({ session.mainInventoryItemIds[index] to session.mainInventoryItemCounts[index] }) to { id, count ->
                    session.mainInventoryItemIds[index] = id
                    session.mainInventoryItemCounts[index] = count
                }
            }
            slot in 5..8 -> {
                val armorIndex = armorIndexForInventorySlot(slot)
                ({ session.armorItemIds[armorIndex] to session.armorItemCounts[armorIndex] }) to { id, count ->
                    val previousItemId = session.armorItemIds[armorIndex]
                    val previousCount = session.armorItemCounts[armorIndex]
                    session.armorItemIds[armorIndex] = id
                    session.armorItemCounts[armorIndex] = count
                    maybeBroadcastArmorSlotChangeSound(
                        session = session,
                        previousItemId = previousItemId,
                        previousCount = previousCount,
                        nextItemId = id,
                        nextCount = count
                    )
                }
            }
            includeOffhand && slot == 45 -> {
                ({ session.offhandItemId to session.offhandItemCount }) to { id, count ->
                    val previousItemId = session.offhandItemId
                    val previousCount = session.offhandItemCount
                    session.offhandItemId = id
                    session.offhandItemCount = count
                    maybeBroadcastOffhandSlotChangeSound(
                        session = session,
                        previousItemId = previousItemId,
                        previousCount = previousCount,
                        nextItemId = id,
                        nextCount = count
                    )
                }
            }
            else -> null
        }
    }

    private fun pickupSwap(
        readSlot: () -> Pair<Int, Int>,
        writeSlot: (Int, Int) -> Unit,
        session: PlayerSession,
        rightClick: Boolean
    ) {
        var (slotItem, slotCount) = readSlot()
        var cursorItem = session.cursorItemId
        var cursorCount = session.cursorItemCount

        if (cursorItem < 0 || cursorCount <= 0) {
            if (slotItem < 0 || slotCount <= 0) return
            if (rightClick && slotCount > 1) {
                val take = slotCount / 2
                cursorItem = slotItem
                cursorCount = take
                slotCount -= take
                if (slotCount <= 0) {
                    slotItem = -1
                    slotCount = 0
                }
            } else {
                cursorItem = slotItem
                cursorCount = slotCount
                slotItem = -1
                slotCount = 0
            }
            writeSlot(slotItem, slotCount)
            session.cursorItemId = cursorItem
            session.cursorItemCount = cursorCount
            return
        }

        if (slotItem < 0 || slotCount <= 0) {
            if (rightClick) {
                writeSlot(cursorItem, 1)
                cursorCount -= 1
                if (cursorCount <= 0) {
                    cursorItem = -1
                    cursorCount = 0
                }
            } else {
                writeSlot(cursorItem, cursorCount)
                cursorItem = -1
                cursorCount = 0
            }
            session.cursorItemId = cursorItem
            session.cursorItemCount = cursorCount
            return
        }

        if (slotItem == cursorItem && slotCount < MAX_HOTBAR_STACK_SIZE) {
            if (rightClick) {
                slotCount += 1
                cursorCount -= 1
            } else {
                val move = (MAX_HOTBAR_STACK_SIZE - slotCount).coerceAtMost(cursorCount)
                slotCount += move
                cursorCount -= move
            }
            if (cursorCount <= 0) {
                cursorItem = -1
                cursorCount = 0
            }
            writeSlot(slotItem, slotCount)
            session.cursorItemId = cursorItem
            session.cursorItemCount = cursorCount
            return
        }

        writeSlot(cursorItem, cursorCount)
        session.cursorItemId = slotItem
        session.cursorItemCount = slotCount
    }

    private fun takeFromOutputSlot(
        readSlot: () -> Pair<Int, Int>,
        writeSlot: (Int, Int) -> Unit,
        session: PlayerSession,
        rightClick: Boolean
    ) {
        val (slotItemId, slotCount) = readSlot()
        if (slotItemId < 0 || slotCount <= 0) return
        val take = if (rightClick) 1 else slotCount
        if (take <= 0) return

        if (session.cursorItemId >= 0 && session.cursorItemCount > 0) {
            if (session.cursorItemId != slotItemId) return
            if (session.cursorItemCount + take > MAX_HOTBAR_STACK_SIZE) return
            session.cursorItemCount += take
        } else {
            session.cursorItemId = slotItemId
            session.cursorItemCount = take
        }

        val remain = slotCount - take
        if (remain <= 0) {
            writeSlot(-1, 0)
        } else {
            writeSlot(slotItemId, remain)
        }
    }

    private fun craftFromPlayerResult(session: PlayerSession) {
        val match = currentPlayerCraftMatch(session) ?: return
        val resultItemId = itemIdByKey[match.recipe.result.itemKey] ?: return
        if (!canTakeResultIntoCursor(session, resultItemId, match.recipe.result.count)) return
        consumeMatchedIngredients(session.playerCraftItemIds, session.playerCraftItemCounts, match.inputSlotsToConsume)
        addResultToCursor(session, resultItemId, match.recipe.result.count)
    }

    private fun craftFromTableResult(session: PlayerSession) {
        val match = currentTableCraftMatch(session) ?: return
        val resultItemId = itemIdByKey[match.recipe.result.itemKey] ?: return
        if (!canTakeResultIntoCursor(session, resultItemId, match.recipe.result.count)) return
        consumeMatchedIngredients(session.tableCraftItemIds, session.tableCraftItemCounts, match.inputSlotsToConsume)
        addResultToCursor(session, resultItemId, match.recipe.result.count)
    }

    private fun currentPlayerCraftMatch(session: PlayerSession): CraftingRecipeCache.RecipeMatch? {
        val keys = Array(4) { i -> itemKeyForStack(session.playerCraftItemIds[i], session.playerCraftItemCounts[i]) }
        return CraftingRecipeCache.findFirstMatch(2, 2, keys)
    }

    private fun currentTableCraftMatch(session: PlayerSession): CraftingRecipeCache.RecipeMatch? {
        val keys = Array(9) { i -> itemKeyForStack(session.tableCraftItemIds[i], session.tableCraftItemCounts[i]) }
        return CraftingRecipeCache.findFirstMatch(3, 3, keys)
    }

    private fun currentSmeltingRecipe(furnace: FurnaceState): SmeltingRecipeCache.SmeltingRecipe? {
        if (furnace.inputItemId < 0 || furnace.inputCount <= 0) return null
        return SmeltingRecipeCache.recipeByInputItemId(furnace.inputItemId, itemKeyById)
    }

    private fun fuelBurnSeconds(itemId: Int): Double {
        if (itemId < 0) return 0.0
        return FurnaceFuelCache.burnSecondsByItemId(itemId)
    }

    private fun isFuelItemId(itemId: Int): Boolean = fuelBurnSeconds(itemId) > 0.0

    private fun isSmeltableItemId(itemId: Int): Boolean {
        if (itemId < 0) return false
        return SmeltingRecipeCache.recipeByInputItemId(itemId, itemKeyById) != null
    }

    private fun canSmeltNow(
        furnace: FurnaceState,
        recipe: SmeltingRecipeCache.SmeltingRecipe?
    ): Boolean {
        if (recipe == null) return false
        if (furnace.inputItemId < 0 || furnace.inputCount <= 0) return false
        val resultItemId = itemIdByKey[recipe.resultItemKey] ?: return false
        if (furnace.resultCount <= 0 || furnace.resultItemId < 0) return true
        if (furnace.resultItemId != resultItemId) return false
        return furnace.resultCount + recipe.resultCount <= MAX_HOTBAR_STACK_SIZE
    }

    private fun smeltOneItem(
        furnace: FurnaceState,
        recipe: SmeltingRecipeCache.SmeltingRecipe
    ) {
        val resultItemId = itemIdByKey[recipe.resultItemKey] ?: return
        furnace.inputCount -= 1
        if (furnace.inputCount <= 0) {
            furnace.inputItemId = -1
            furnace.inputCount = 0
        }
        if (furnace.resultCount <= 0 || furnace.resultItemId < 0) {
            furnace.resultItemId = resultItemId
            furnace.resultCount = recipe.resultCount.coerceAtLeast(1)
        } else if (furnace.resultItemId == resultItemId) {
            furnace.resultCount = (furnace.resultCount + recipe.resultCount).coerceAtMost(MAX_HOTBAR_STACK_SIZE)
        }
        furnace.cookTotalSeconds = recipe.cookSeconds.coerceAtLeast(MIN_FURNACE_COOK_SECONDS)
    }

    private fun itemKeyForStack(itemId: Int, count: Int): String? {
        if (itemId < 0 || count <= 0) return null
        val key = itemKeyById.getOrNull(itemId) ?: return null
        if (key.isEmpty()) return null
        return key
    }

    private fun consumeMatchedIngredients(itemIds: IntArray, itemCounts: IntArray, slots: IntArray) {
        for (slot in slots) {
            if (slot !in itemIds.indices) continue
            val next = itemCounts[slot] - 1
            if (next <= 0) {
                itemIds[slot] = -1
                itemCounts[slot] = 0
            } else {
                itemCounts[slot] = next
            }
        }
    }

    private fun canTakeResultIntoCursor(session: PlayerSession, itemId: Int, count: Int): Boolean {
        if (count <= 0) return false
        if (session.cursorItemId < 0 || session.cursorItemCount <= 0) return true
        if (session.cursorItemId != itemId) return false
        return session.cursorItemCount + count <= MAX_HOTBAR_STACK_SIZE
    }

    private fun addResultToCursor(session: PlayerSession, itemId: Int, count: Int) {
        if (session.cursorItemId < 0 || session.cursorItemCount <= 0) {
            session.cursorItemId = itemId
            session.cursorItemCount = count
            return
        }
        session.cursorItemCount = (session.cursorItemCount + count).coerceAtMost(MAX_HOTBAR_STACK_SIZE)
    }

    private fun readCraftSlot(ids: IntArray, counts: IntArray, index: Int): Pair<Int, Int> {
        if (index !in ids.indices) return -1 to 0
        return ids[index] to counts[index]
    }

    private fun writeCraftSlot(ids: IntArray, counts: IntArray, index: Int, itemId: Int, count: Int) {
        if (index !in ids.indices) return
        if (itemId < 0 || count <= 0) {
            ids[index] = -1
            counts[index] = 0
        } else {
            ids[index] = itemId
            counts[index] = count.coerceAtMost(MAX_HOTBAR_STACK_SIZE)
        }
    }

    private fun resyncContainerForSession(session: PlayerSession, containerId: Int) {
        val ctx = contexts[session.channelId] ?: return
        if (!ctx.channel().isActive) return
        val stateId = nextInventoryStateId(session)
        when (containerId) {
            PLAYER_INVENTORY_CONTAINER_ID -> {
                ctx.writeAndFlush(
                    PlayPackets.containerSetContentPacket(
                        containerId = PLAYER_INVENTORY_CONTAINER_ID,
                        stateId = stateId,
                        encodedItems = encodePlayerInventoryContainerItems(session),
                        encodedCarried = encodeCursorStack(session)
                    )
                )
            }
            session.openContainerId -> {
                when (session.openContainerType) {
                    CONTAINER_TYPE_CRAFTING_TABLE -> {
                        ctx.writeAndFlush(
                            PlayPackets.containerSetContentPacket(
                                containerId = session.openContainerId,
                                stateId = stateId,
                                encodedItems = encodeCraftingTableContainerItems(session),
                                encodedCarried = encodeCursorStack(session)
                            )
                        )
                    }
                    CONTAINER_TYPE_FURNACE -> {
                        ctx.write(
                            PlayPackets.containerSetContentPacket(
                                containerId = session.openContainerId,
                                stateId = stateId,
                                encodedItems = encodeFurnaceContainerItems(session),
                                encodedCarried = encodeCursorStack(session)
                            )
                        )
                        val furnace = openFurnaceState(session)
                        if (furnace != null) {
                            val properties = encodeFurnaceProperties(furnace)
                            for (propertyIndex in properties.indices) {
                                ctx.write(
                                    PlayPackets.containerSetDataPacket(
                                        containerId = session.openContainerId,
                                        property = propertyIndex,
                                        value = properties[propertyIndex]
                                    )
                                )
                            }
                        }
                        ctx.flush()
                    }
                }
            }
        }
    }

    private fun resyncOpenFurnaceViewers(key: FurnaceKey) {
        for (session in sessions.values) {
            if (session.worldKey != key.worldKey) continue
            if (session.openContainerType != CONTAINER_TYPE_FURNACE) continue
            if (session.openFurnaceX != key.x || session.openFurnaceY != key.y || session.openFurnaceZ != key.z) continue
            resyncContainerForSession(session, session.openContainerId)
        }
    }

    private fun encodedHotbarSlot(session: PlayerSession, slot: Int): ByteArray {
        if (slot !in 0..8) return emptyItemStack()
        val itemId = session.hotbarItemIds[slot]
        val count = session.hotbarItemCounts[slot]
        if (itemId < 0 || count <= 0) return emptyItemStack()
        val meta = resolveHotbarTextMeta(session, slot, itemId)
        return encodedItemStack(itemId, count, meta)
    }

    private fun encodedMainSlot(session: PlayerSession, index: Int): ByteArray {
        if (index !in 0..26) return emptyItemStack()
        val itemId = session.mainInventoryItemIds[index]
        val count = session.mainInventoryItemCounts[index]
        if (itemId < 0 || count <= 0) return emptyItemStack()
        val meta = resolveMainTextMeta(session, index, itemId)
        return encodedItemStack(itemId, count, meta)
    }

    private fun encodedArmorSlot(session: PlayerSession, armorIndex: Int): ByteArray {
        if (armorIndex !in 0..3) return emptyItemStack()
        val itemId = session.armorItemIds[armorIndex]
        val count = session.armorItemCounts[armorIndex]
        if (itemId < 0 || count <= 0) return emptyItemStack()
        val meta = resolveArmorTextMeta(session, armorIndex, itemId)
        return encodedItemStack(itemId, count, meta)
    }

    private fun encodedOffhandSlot(session: PlayerSession): ByteArray {
        val itemId = session.offhandItemId
        val count = session.offhandItemCount
        if (itemId < 0 || count <= 0) return emptyItemStack()
        val meta = resolveOffhandTextMeta(session, itemId)
        return encodedItemStack(itemId, count, meta)
    }

    private fun encodePlayerInventoryContainerItems(session: PlayerSession): List<ByteArray> {
        val out = ArrayList<ByteArray>(46)
        val playerMatch = currentPlayerCraftMatch(session)
        val resultStack = if (playerMatch != null) {
            val resultItemId = itemIdByKey[playerMatch.recipe.result.itemKey] ?: -1
            if (resultItemId >= 0) encodedItemStack(resultItemId, playerMatch.recipe.result.count) else emptyItemStack()
        } else {
            emptyItemStack()
        }
        out.add(resultStack)
        for (i in 0 until 4) {
            val id = session.playerCraftItemIds[i]
            val count = session.playerCraftItemCounts[i]
            out.add(if (id >= 0 && count > 0) encodedItemStack(id, count) else emptyItemStack())
        }
        // Player inventory container slots 5..8 are helmet, chestplate, leggings, boots.
        // Internal armor arrays are boots(0), leggings(1), chestplate(2), helmet(3).
        for (armorSlot in 3 downTo 0) {
            out.add(encodedArmorSlot(session, armorSlot))
        }
        for (main in 0 until 27) {
            out.add(encodedMainSlot(session, main))
        }
        for (hotbar in 0 until 9) {
            out.add(encodedHotbarSlot(session, hotbar))
        }
        out.add(encodedOffhandSlot(session))
        return out
    }

    private fun encodeCraftingTableContainerItems(session: PlayerSession): List<ByteArray> {
        val out = ArrayList<ByteArray>(46)
        val tableMatch = currentTableCraftMatch(session)
        val resultStack = if (tableMatch != null) {
            val resultItemId = itemIdByKey[tableMatch.recipe.result.itemKey] ?: -1
            if (resultItemId >= 0) encodedItemStack(resultItemId, tableMatch.recipe.result.count) else emptyItemStack()
        } else {
            emptyItemStack()
        }
        out.add(resultStack)
        for (i in 0 until 9) {
            val id = session.tableCraftItemIds[i]
            val count = session.tableCraftItemCounts[i]
            out.add(if (id >= 0 && count > 0) encodedItemStack(id, count) else emptyItemStack())
        }
        for (main in 0 until 27) {
            out.add(encodedMainSlot(session, main))
        }
        for (hotbar in 0 until 9) {
            out.add(encodedHotbarSlot(session, hotbar))
        }
        return out
    }

    private fun encodeFurnaceContainerItems(session: PlayerSession): List<ByteArray> {
        val furnace = openFurnaceState(session) ?: createDetachedFurnaceState()
        val out = ArrayList<ByteArray>(39)
        val inputId: Int
        val inputCount: Int
        val fuelId: Int
        val fuelCount: Int
        val resultId: Int
        val resultCount: Int
        run {
            inputId = furnace.inputItemId
            inputCount = furnace.inputCount
            fuelId = furnace.fuelItemId
            fuelCount = furnace.fuelCount
            resultId = furnace.resultItemId
            resultCount = furnace.resultCount
        }
        out.add(if (inputId >= 0 && inputCount > 0) encodedItemStack(inputId, inputCount) else emptyItemStack())
        out.add(if (fuelId >= 0 && fuelCount > 0) encodedItemStack(fuelId, fuelCount) else emptyItemStack())
        out.add(if (resultId >= 0 && resultCount > 0) encodedItemStack(resultId, resultCount) else emptyItemStack())
        for (main in 0 until 27) {
            out.add(encodedMainSlot(session, main))
        }
        for (hotbar in 0 until 9) {
            out.add(encodedHotbarSlot(session, hotbar))
        }
        return out
    }

    private fun encodeCursorStack(session: PlayerSession): ByteArray {
        return if (session.cursorItemId >= 0 && session.cursorItemCount > 0) {
            encodedItemStack(session.cursorItemId, session.cursorItemCount)
        } else {
            emptyItemStack()
        }
    }

    private fun encodeFurnaceProperties(furnace: FurnaceState): IntArray {
        run {
            return intArrayOf(
                secondsToMinecraftTicksInt(furnace.burnRemainingSeconds),
                secondsToMinecraftTicksInt(furnace.burnTotalSeconds),
                secondsToMinecraftTicksInt(furnace.cookProgressSeconds),
                secondsToMinecraftTicksInt(furnace.cookTotalSeconds)
            )
        }
    }

    private fun secondsToMinecraftTicksInt(seconds: Double): Int {
        if (!seconds.isFinite() || seconds <= 0.0) return 0
        return (seconds * MINECRAFT_TICKS_PER_SECOND).toInt().coerceAtLeast(0).coerceAtMost(Short.MAX_VALUE.toInt())
    }

    private fun closeCraftingTable(session: PlayerSession) {
        resetQuickCraftDrag(session)
        val closingContainerId = session.openContainerId
        for (i in session.tableCraftItemIds.indices) {
            val itemId = session.tableCraftItemIds[i]
            val count = session.tableCraftItemCounts[i]
            if (itemId >= 0 && count > 0) {
                tryInsertItemIntoInventory(session, itemId, count)
                session.tableCraftItemIds[i] = -1
                session.tableCraftItemCounts[i] = 0
            }
        }
        val ctx = contexts[session.channelId]
        session.openContainerType = CONTAINER_TYPE_PLAYER_INVENTORY
        session.openContainerId = PLAYER_INVENTORY_CONTAINER_ID
        if (ctx != null && ctx.channel().isActive) {
            val stateId = nextInventoryStateId(session)
            ctx.write(PlayPackets.containerClosePacket(closingContainerId))
            ctx.writeAndFlush(
                PlayPackets.containerSetContentPacket(
                    containerId = PLAYER_INVENTORY_CONTAINER_ID,
                    stateId = stateId,
                    encodedItems = encodePlayerInventoryContainerItems(session),
                    encodedCarried = encodeCursorStack(session)
                )
            )
        }
    }

    private fun closeFurnace(session: PlayerSession) {
        resetQuickCraftDrag(session)
        val closingContainerId = session.openContainerId
        session.openContainerType = CONTAINER_TYPE_PLAYER_INVENTORY
        session.openContainerId = PLAYER_INVENTORY_CONTAINER_ID
        session.openFurnaceX = 0
        session.openFurnaceY = 0
        session.openFurnaceZ = 0
        val ctx = contexts[session.channelId]
        if (ctx != null && ctx.channel().isActive) {
            val stateId = nextInventoryStateId(session)
            ctx.write(PlayPackets.containerClosePacket(closingContainerId))
            ctx.writeAndFlush(
                PlayPackets.containerSetContentPacket(
                    containerId = PLAYER_INVENTORY_CONTAINER_ID,
                    stateId = stateId,
                    encodedItems = encodePlayerInventoryContainerItems(session),
                    encodedCarried = encodeCursorStack(session)
                )
            )
        }
    }

    private fun openCraftingTable(session: PlayerSession) {
        if (session.openContainerType == CONTAINER_TYPE_FURNACE) {
            closeFurnace(session)
        } else if (session.openContainerType == CONTAINER_TYPE_CRAFTING_TABLE) {
            closeCraftingTable(session)
        }
        val ctx = contexts[session.channelId] ?: return
        if (!ctx.channel().isActive) return
        val containerId = session.nextContainerId
        session.nextContainerId = (session.nextContainerId + 1).coerceAtMost(Int.MAX_VALUE)
        session.openContainerId = containerId
        session.openContainerType = CONTAINER_TYPE_CRAFTING_TABLE
        val stateId = nextInventoryStateId(session)
        ctx.write(
            PlayPackets.openScreenPacket(
                containerId = containerId,
                menuTypeId = CRAFTING_TABLE_MENU_TYPE_ID,
                title = PlayPackets.ChatComponent.Translate("container.crafting")
            )
        )
        ctx.writeAndFlush(
            PlayPackets.containerSetContentPacket(
                containerId = containerId,
                stateId = stateId,
                encodedItems = encodeCraftingTableContainerItems(session),
                encodedCarried = encodeCursorStack(session)
            )
        )
    }

    private fun openFurnace(session: PlayerSession, x: Int, y: Int, z: Int) {
        if (session.openContainerType == CONTAINER_TYPE_CRAFTING_TABLE) {
            closeCraftingTable(session)
        } else if (session.openContainerType == CONTAINER_TYPE_FURNACE) {
            closeFurnace(session)
        }
        val ctx = contexts[session.channelId] ?: return
        if (!ctx.channel().isActive) return
        val containerId = session.nextContainerId
        session.nextContainerId = (session.nextContainerId + 1).coerceAtMost(Int.MAX_VALUE)
        session.openContainerId = containerId
        session.openContainerType = CONTAINER_TYPE_FURNACE
        session.openFurnaceX = x
        session.openFurnaceY = y
        session.openFurnaceZ = z
        val stateId = nextInventoryStateId(session)
        ctx.write(
            PlayPackets.openScreenPacket(
                containerId = containerId,
                menuTypeId = furnaceMenuTypeId,
                title = PlayPackets.ChatComponent.Translate("container.furnace")
            )
        )
        ctx.writeAndFlush(
            PlayPackets.containerSetContentPacket(
                containerId = containerId,
                stateId = stateId,
                encodedItems = encodeFurnaceContainerItems(session),
                encodedCarried = encodeCursorStack(session)
            )
        )
        val furnace = openFurnaceState(session) ?: return
        val properties = encodeFurnaceProperties(furnace)
        for (propertyIndex in properties.indices) {
            ctx.write(
                PlayPackets.containerSetDataPacket(
                    containerId = containerId,
                    property = propertyIndex,
                    value = properties[propertyIndex]
                )
            )
        }
        ctx.flush()
    }

    private fun isCraftingTableState(stateId: Int): Boolean {
        if (stateId <= 0) return false
        val parsed = BlockStateRegistry.parsedState(stateId) ?: return false
        return parsed.blockKey == "minecraft:crafting_table"
    }

    private fun isFurnaceState(stateId: Int): Boolean {
        if (stateId <= 0) return false
        val parsed = BlockStateRegistry.parsedState(stateId) ?: return false
        return parsed.blockKey == "minecraft:furnace"
    }

    private fun createDetachedFurnaceState(): FurnaceState {
        return FurnaceState(
            inputItemId = -1,
            inputCount = 0,
            fuelItemId = -1,
            fuelCount = 0,
            resultItemId = -1,
            resultCount = 0,
            burnRemainingSeconds = 0.0,
            burnTotalSeconds = 0.0,
            cookProgressSeconds = 0.0,
            cookTotalSeconds = DEFAULT_FURNACE_COOK_SECONDS,
            dirty = false
        )
    }

    private fun openFurnaceState(session: PlayerSession): FurnaceState? {
        if (session.openContainerType != CONTAINER_TYPE_FURNACE) return null
        val key = FurnaceKey(
            worldKey = session.worldKey,
            x = session.openFurnaceX,
            y = session.openFurnaceY,
            z = session.openFurnaceZ
        )
        return furnaceStates.computeIfAbsent(key) { createDetachedFurnaceState() }
    }

    private fun furnaceStateAt(worldKey: String, x: Int, y: Int, z: Int): FurnaceState {
        val key = FurnaceKey(worldKey, x, y, z)
        return furnaceStates.computeIfAbsent(key) { createDetachedFurnaceState() }
    }

    private fun writeFurnaceSlot(furnace: FurnaceState, slot: Int, itemId: Int, count: Int) {
        run {
            val normalizedId = if (itemId >= 0 && count > 0) itemId else -1
            val normalizedCount = if (itemId >= 0 && count > 0) count.coerceAtMost(MAX_HOTBAR_STACK_SIZE) else 0
            when (slot) {
                FURNACE_INPUT_SLOT -> {
                    furnace.inputItemId = normalizedId
                    furnace.inputCount = normalizedCount
                    if (normalizedCount <= 0) {
                        furnace.inputItemId = -1
                        furnace.inputCount = 0
                    }
                    furnace.cookProgressSeconds = 0.0
                }
                FURNACE_FUEL_SLOT -> {
                    furnace.fuelItemId = normalizedId
                    furnace.fuelCount = normalizedCount
                    if (normalizedCount <= 0) {
                        furnace.fuelItemId = -1
                        furnace.fuelCount = 0
                    }
                }
                FURNACE_RESULT_SLOT -> {
                    furnace.resultItemId = normalizedId
                    furnace.resultCount = normalizedCount
                    if (normalizedCount <= 0) {
                        furnace.resultItemId = -1
                        furnace.resultCount = 0
                    }
                }
            }
        }
    }

    fun placeSelectedBlock(
        channelId: ChannelId,
        x: Int,
        y: Int,
        z: Int,
        clickedX: Int,
        clickedY: Int,
        clickedZ: Int,
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
        val offhand = hand == 1
        val slot = session.selectedHotbarSlot.coerceIn(0, 8)
        val selectedItemId = if (offhand) session.offhandItemId else session.hotbarItemIds[slot]
        val selectedItemCount = if (offhand) session.offhandItemCount else session.hotbarItemCounts[slot]
        val clickedStateId = world.blockStateAt(clickedX, clickedY, clickedZ)
        if (!session.sneaking && isFurnaceState(clickedStateId) && isChunkLoadedForSession(session, clickedX, clickedZ)) {
            openFurnace(session, clickedX, clickedY, clickedZ)
            return
        }
        if (!session.sneaking && isCraftingTableState(clickedStateId) && isChunkLoadedForSession(session, clickedX, clickedZ)) {
            openCraftingTable(session)
            return
        }
        if (selectedItemCount <= 0 || selectedItemId < 0) {
            if (PLACEMENT_DEBUG_LOG_ENABLED) {
                logger.info(
                    "Placement drop: player={} reason=empty_hand offhand={} slot={} itemId={} count={}",
                    session.profile.username, offhand, slot, selectedItemId, selectedItemCount
                )
            }
            return
        }
        if (selectedItemId == pigSpawnEggItemId) {
            if (tryHandlePigSpawnEggUseOn(session, world, hand, clickedX, clickedY, clickedZ, faceId)) {
                return
            }
        }

        // Buckets are not normal block items; handle server-side placement explicitly.
        if (selectedItemId == emptyBucketItemId) {
            if (!isChunkLoadedForSession(session, clickedX, clickedZ)) {
                return
            }
            if (!canPickupWaterState(clickedStateId)) {
                return
            }
        setBlockAndBroadcast(
            session = session,
            x = clickedX,
            y = clickedY,
            z = clickedZ,
            stateId = 0,
            reason = BlockChangeReason.PLAYER_BUCKET_FILL,
            logAsFluidSync = true
        )
            world.resetFluidTickAccumulator()
            replaceUsedEmptyBucketWithWaterBucket(session, hand)
            return
        }
        if (selectedItemId == waterBucketItemId) {
            if (!isChunkLoadedForSession(session, x, z)) {
                return
            }
            val targetStateBeforeEvent = world.blockStateAt(x, y, z)
            if (!canPlaceWaterIntoState(targetStateBeforeEvent)) {
                if (PLACEMENT_DEBUG_LOG_ENABLED) {
                    logger.info(
                        "Placement drop: player={} reason=water_bucket_target_blocked pos=({}, {}, {}) stateId={}",
                        session.profile.username, x, y, z, targetStateBeforeEvent
                    )
                }
                return
            }
            if (waterSourceStateId <= 0) return
            if (!org.macaroon3145.plugin.PluginSystem.beforeBlockPlace(
                    session = session,
                    x = x,
                    y = y,
                    z = z,
                    clickedX = clickedX,
                    clickedY = clickedY,
                    clickedZ = clickedZ,
                    hand = hand,
                    faceId = faceId
                )
            ) {
                return
            }
            val targetStateAfterEvent = world.blockStateAt(x, y, z)
            if (targetStateAfterEvent != targetStateBeforeEvent) {
                if (session.gameMode != GAME_MODE_CREATIVE) {
                    replaceUsedWaterBucketWithEmptyBucket(session, hand)
                }
                return
            }
            setBlockAndBroadcast(
                session = session,
                x = x,
                y = y,
                z = z,
                stateId = waterSourceStateId,
                reason = BlockChangeReason.PLAYER_BUCKET_EMPTY,
                logAsFluidSync = true
            )
            world.resetFluidTickAccumulator()
            if (session.gameMode != GAME_MODE_CREATIVE) {
                replaceUsedWaterBucketWithEmptyBucket(session, hand)
            }
            return
        }
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
        // Vanilla-like replaceable placement: allow replacing water and other replaceable blocks.
        val targetStateAtPlacement = world.blockStateAt(x, y, z)
        if (!canPlaceBlockIntoState(targetStateAtPlacement)) {
            if (PLACEMENT_DEBUG_LOG_ENABLED) {
                logger.info(
                    "Placement drop: player={} reason=target_not_replaceable pos=({}, {}, {}) stateId={}",
                    session.profile.username, x, y, z, targetStateAtPlacement
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
        val targetStateBeforeEvent = targetStateAtPlacement
        var pairedStateBeforeEvent: Int? = null
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
            pairedStateBeforeEvent = pairedCurrentState
            if (!canPlaceBlockIntoState(pairedCurrentState)) {
                if (PLACEMENT_DEBUG_LOG_ENABLED) {
                    logger.info(
                        "Placement drop: player={} reason=paired_not_replaceable paired=({}, {}, {}) stateId={}",
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
        if (!org.macaroon3145.plugin.PluginSystem.beforeBlockPlace(
                session = session,
                x = x,
                y = y,
                z = z,
                clickedX = clickedX,
                clickedY = clickedY,
                clickedZ = clickedZ,
                hand = hand,
                faceId = faceId
            )
        ) {
            return
        }
        val targetStateAfterEvent = world.blockStateAt(x, y, z)
        val pairedStateAfterEvent = pairedPlacement?.let { world.blockStateAt(it.x, it.y, it.z) }
        val changedByPlugin = targetStateAfterEvent != targetStateBeforeEvent ||
            (pairedPlacement != null && pairedStateAfterEvent != pairedStateBeforeEvent)
        if (changedByPlugin) {
            if (session.gameMode != GAME_MODE_CREATIVE) {
                consumePlacedItem(session, hand)
            }
            return
        }
        setBlockAndBroadcast(
            session = session,
            x = x,
            y = y,
            z = z,
            stateId = blockStateId,
            reason = BlockChangeReason.PLAYER_PLACE,
            blockEntityTypeId = if (offhand) -1 else session.hotbarBlockEntityTypeIds[slot],
            blockEntityNbtPayload = if (offhand) null else session.hotbarBlockEntityNbtPayloads[slot]
        )
        if (pairedPlacement != null) {
            setBlockAndBroadcast(
                session = session,
                x = pairedPlacement.x,
                y = pairedPlacement.y,
                z = pairedPlacement.z,
                stateId = pairedPlacement.stateId,
                reason = BlockChangeReason.PLAYER_PLACE
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

    private fun tryHandlePigSpawnEggUseOn(
        session: PlayerSession,
        world: org.macaroon3145.world.World,
        hand: Int,
        clickedX: Int,
        clickedY: Int,
        clickedZ: Int,
        faceId: Int
    ): Boolean {
        val spawnPos = resolveSpawnEggUseOnPosition(
            world = world,
            clickedX = clickedX,
            clickedY = clickedY,
            clickedZ = clickedZ,
            faceId = faceId,
            kind = AnimalKind.PIG
        ) ?: return false
        if (!isChunkLoadedForSession(session, floor(spawnPos.first).toInt(), floor(spawnPos.third).toInt())) return false
        val spawnedSnapshots = ArrayList<AnimalSnapshot>(100)
        repeat(100) {
            val entityId = allocateEntityId()
            if (!world.spawnAnimal(
                    entityId = entityId,
                    kind = AnimalKind.PIG,
                    x = spawnPos.first,
                    y = spawnPos.second,
                    z = spawnPos.third,
                    yaw = session.yaw
                )
            ) {
                return@repeat
            }
            val snapshot = world.animalSnapshot(entityId) ?: return@repeat
            spawnedSnapshots.add(snapshot)
        }
        if (spawnedSnapshots.isEmpty()) return false
        for (other in sessions.values) {
            if (other.worldKey != session.worldKey) continue
            val ctx = contexts[other.channelId] ?: continue
            if (!ctx.channel().isActive) continue
            var wrote = false
            for (snapshot in spawnedSnapshots) {
                if (!other.loadedChunks.contains(snapshot.chunkPos)) continue
                if (!other.visibleAnimalEntityIds.add(snapshot.entityId)) continue
                writeAnimalSpawnPackets(other, ctx, snapshot)
                ctx.write(
                    PlayPackets.soundPacketByKey(
                        soundKey = snapshot.kind.ambientSoundKey,
                        soundSourceId = NEUTRAL_SOUND_SOURCE_ID,
                        x = snapshot.x,
                        y = snapshot.y + snapshot.hitboxHeight * 0.5,
                        z = snapshot.z,
                        volume = 1.0f,
                        pitch = (0.9f + ThreadLocalRandom.current().nextFloat() * 0.2f),
                        seed = ThreadLocalRandom.current().nextLong()
                    )
                )
                wrote = true
            }
            if (wrote) {
                ctx.flush()
            }
        }
        if (session.gameMode != GAME_MODE_CREATIVE) {
            consumePlacedItem(session, hand)
        }
        return true
    }

    private fun resolveSpawnEggUseOnPosition(
        world: org.macaroon3145.world.World,
        clickedX: Int,
        clickedY: Int,
        clickedZ: Int,
        faceId: Int,
        kind: AnimalKind
    ): Triple<Double, Double, Double>? {
        val dimension = EntityHitboxRegistry.dimension(kind.entityTypeKey) ?: return null
        val halfWidth = dimension.width * 0.5
        val height = dimension.height
        val epsilon = 1.0e-4

        val clickedStateId = world.blockStateAt(clickedX, clickedY, clickedZ)
        val parsed = if (clickedStateId > 0) BlockStateRegistry.parsedState(clickedStateId) else null
        val boxes = if (parsed == null || isOpenSpawnSpace(clickedStateId)) {
            emptyArray()
        } else {
            BlockCollisionRegistry.boxesForStateId(clickedStateId, parsed)
                ?: arrayOf(BlockCollisionRegistry.CollisionBox(0.0, 0.0, 0.0, 1.0, 1.0, 1.0))
        }

        if (boxes.isEmpty()) {
            val target = when (faceId) {
                0 -> BlockPos(clickedX, clickedY - 1, clickedZ)
                1 -> BlockPos(clickedX, clickedY + 1, clickedZ)
                2 -> BlockPos(clickedX, clickedY, clickedZ - 1)
                3 -> BlockPos(clickedX, clickedY, clickedZ + 1)
                4 -> BlockPos(clickedX - 1, clickedY, clickedZ)
                5 -> BlockPos(clickedX + 1, clickedY, clickedZ)
                else -> BlockPos(clickedX, clickedY, clickedZ)
            }
            return Triple(target.x + 0.5, target.y.toDouble(), target.z + 0.5)
        }

        var minX = Double.POSITIVE_INFINITY
        var minY = Double.POSITIVE_INFINITY
        var minZ = Double.POSITIVE_INFINITY
        var maxX = Double.NEGATIVE_INFINITY
        var maxY = Double.NEGATIVE_INFINITY
        var maxZ = Double.NEGATIVE_INFINITY
        for (box in boxes) {
            minX = min(minX, box.minX)
            minY = min(minY, box.minY)
            minZ = min(minZ, box.minZ)
            maxX = max(maxX, box.maxX)
            maxY = max(maxY, box.maxY)
            maxZ = max(maxZ, box.maxZ)
        }

        val baseX = clickedX.toDouble()
        val baseY = clickedY.toDouble()
        val baseZ = clickedZ.toDouble()
        val centerX = baseX + 0.5
        val centerZ = baseZ + 0.5
        return when (faceId) {
            0 -> Triple(centerX, (baseY + minY) - height - epsilon, centerZ)
            1 -> Triple(centerX, (baseY + maxY) + epsilon, centerZ)
            2 -> Triple(centerX, baseY, (baseZ + minZ) - halfWidth - epsilon)
            3 -> Triple(centerX, baseY, (baseZ + maxZ) + halfWidth + epsilon)
            4 -> Triple((baseX + minX) - halfWidth - epsilon, baseY, centerZ)
            5 -> Triple((baseX + maxX) + halfWidth + epsilon, baseY, centerZ)
            else -> Triple(centerX, baseY, centerZ)
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

    private fun canPlaceWaterIntoState(stateId: Int): Boolean {
        if (stateId == 0) return true
        val parsed = BlockStateRegistry.parsedState(stateId) ?: return false
        return parsed.blockKey in WATER_BUCKET_REPLACEABLE_BLOCK_KEYS
    }

    private fun canPlaceBlockIntoState(stateId: Int): Boolean {
        if (stateId == 0) return true
        val parsed = BlockStateRegistry.parsedState(stateId) ?: return false
        return parsed.blockKey in BLOCK_PLACEMENT_REPLACEABLE_BLOCK_KEYS
    }

    private fun clickedBlockFromPlacementTarget(x: Int, y: Int, z: Int, faceId: Int): BlockPos {
        return when (faceId) {
            0 -> BlockPos(x, y + 1, z)
            1 -> BlockPos(x, y - 1, z)
            2 -> BlockPos(x, y, z + 1)
            3 -> BlockPos(x, y, z - 1)
            4 -> BlockPos(x + 1, y, z)
            5 -> BlockPos(x - 1, y, z)
            else -> BlockPos(x, y, z)
        }
    }

    private fun canPickupWaterState(stateId: Int): Boolean {
        if (stateId <= 0) return false
        val parsed = BlockStateRegistry.parsedState(stateId) ?: return false
        if (parsed.blockKey != "minecraft:water") return false
        val levelRaw = parsed.properties["level"] ?: return stateId == waterSourceStateId
        val level = levelRaw.toIntOrNull() ?: return false
        return level == 0
    }

    private fun replaceUsedWaterBucketWithEmptyBucket(session: PlayerSession, hand: Int) {
        if (emptyBucketItemId < 0) return
        val ctx = contexts[session.channelId]
        if (hand == 1) {
            if (session.offhandItemId != waterBucketItemId || session.offhandItemCount <= 0) return
            session.offhandItemId = emptyBucketItemId
            session.offhandItemCount = 1
            if (ctx != null && ctx.channel().isActive) {
                ctx.writeAndFlush(
                    PlayPackets.containerSetSlotPacket(
                        containerId = 0,
                        stateId = nextInventoryStateId(session),
                        slot = 45,
                        encodedItemStack = encodedItemStack(emptyBucketItemId, 1)
                    )
                )
            }
            broadcastHeldItemEquipment(session)
            return
        }
        val slot = session.selectedHotbarSlot.coerceIn(0, 8)
        if (session.hotbarItemIds[slot] != waterBucketItemId || session.hotbarItemCounts[slot] <= 0) return
        session.hotbarItemIds[slot] = emptyBucketItemId
        session.hotbarItemCounts[slot] = 1
        session.hotbarBlockStateIds[slot] = 0
        session.hotbarBlockEntityTypeIds[slot] = -1
        session.hotbarBlockEntityNbtPayloads[slot] = null
        session.selectedBlockStateId = 0
        if (ctx != null && ctx.channel().isActive) {
            ctx.writeAndFlush(
                PlayPackets.containerSetSlotPacket(
                    containerId = 0,
                    stateId = nextInventoryStateId(session),
                    slot = 36 + slot,
                    encodedItemStack = encodedItemStack(emptyBucketItemId, 1)
                )
            )
        }
        broadcastHeldItemEquipment(session)
    }

    private fun replaceUsedEmptyBucketWithWaterBucket(session: PlayerSession, hand: Int) {
        if (waterBucketItemId < 0) return
        if (session.gameMode == GAME_MODE_CREATIVE) {
            // Vanilla ItemUtils.createFilledResult(..., canUseAsExtraSlot=true):
            // in creative/infinite mode keep held stack and only add filled bucket if missing.
            if (!inventoryContainsItem(session, waterBucketItemId)) {
                tryInsertItemIntoInventory(session, waterBucketItemId, 1)
            }
            return
        }

        val ctx = contexts[session.channelId]
        if (hand == 1) {
            if (session.offhandItemId == emptyBucketItemId && session.offhandItemCount == 1) {
                session.offhandItemId = waterBucketItemId
                session.offhandItemCount = 1
                if (ctx != null && ctx.channel().isActive) {
                    ctx.writeAndFlush(
                        PlayPackets.containerSetSlotPacket(
                            containerId = 0,
                            stateId = nextInventoryStateId(session),
                            slot = 45,
                            encodedItemStack = encodedItemStack(waterBucketItemId, 1)
                        )
                    )
                }
                broadcastHeldItemEquipment(session)
                return
            }
            consumePlacedItem(session, hand)
            if (!tryInsertItemIntoInventory(session, waterBucketItemId, 1)) {
                spawnDroppedItemFromPlayer(session, waterBucketItemId, 1, dropStackMotion = false)
            }
            return
        }

        val slot = session.selectedHotbarSlot.coerceIn(0, 8)
        if (session.hotbarItemIds[slot] == emptyBucketItemId && session.hotbarItemCounts[slot] == 1) {
            session.hotbarItemIds[slot] = waterBucketItemId
            session.hotbarItemCounts[slot] = 1
            session.hotbarBlockStateIds[slot] = 0
            session.hotbarBlockEntityTypeIds[slot] = -1
            session.hotbarBlockEntityNbtPayloads[slot] = null
            session.selectedBlockStateId = 0
            if (ctx != null && ctx.channel().isActive) {
                ctx.writeAndFlush(
                    PlayPackets.containerSetSlotPacket(
                        containerId = 0,
                        stateId = nextInventoryStateId(session),
                        slot = 36 + slot,
                        encodedItemStack = encodedItemStack(waterBucketItemId, 1)
                    )
                )
            }
            broadcastHeldItemEquipment(session)
            return
        }

        consumePlacedItem(session, hand)
        if (!tryInsertItemIntoInventory(session, waterBucketItemId, 1)) {
            spawnDroppedItemFromPlayer(session, waterBucketItemId, 1, dropStackMotion = false)
        }
    }

    private fun inventoryContainsItem(session: PlayerSession, itemId: Int): Boolean {
        if (itemId < 0) return false
        for (slot in 0..8) {
            if (session.hotbarItemCounts[slot] > 0 && session.hotbarItemIds[slot] == itemId) return true
        }
        for (index in session.mainInventoryItemIds.indices) {
            if (session.mainInventoryItemCounts[index] > 0 && session.mainInventoryItemIds[index] == itemId) return true
        }
        if (session.offhandItemCount > 0 && session.offhandItemId == itemId) return true
        return false
    }

    private fun tryInsertItemIntoInventory(session: PlayerSession, itemId: Int, itemCount: Int): Boolean {
        if (itemId < 0 || itemCount <= 0) return false
        var remaining = itemCount
        val changedHotbarSlots = LinkedHashSet<Int>()
        val changedMainInventorySlots = LinkedHashSet<Int>()
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
            if (session.mainInventoryItemCounts[index] > 0 && session.mainInventoryItemIds[index] >= 0) continue
            val added = minOf(MAX_HOTBAR_STACK_SIZE, remaining)
            session.mainInventoryItemIds[index] = itemId
            session.mainInventoryItemCounts[index] = added
            remaining -= added
            changedMainInventorySlots.add(index)
        }

        if (remaining > 0) return false

        session.selectedBlockStateId = if (session.hotbarItemCounts[selectedSlot] > 0) {
            session.hotbarBlockStateIds[selectedSlot]
        } else {
            0
        }

        val ctx = contexts[session.channelId]
        if (ctx != null && ctx.channel().isActive) {
            for (slot in changedHotbarSlots) {
                val encoded = if (session.hotbarItemIds[slot] >= 0 && session.hotbarItemCounts[slot] > 0) {
                    encodedItemStack(session.hotbarItemIds[slot], session.hotbarItemCounts[slot])
                } else {
                    emptyItemStack()
                }
                ctx.write(
                    PlayPackets.containerSetSlotPacket(
                        containerId = 0,
                        stateId = nextInventoryStateId(session),
                        slot = 36 + slot,
                        encodedItemStack = encoded
                    )
                )
            }
            for (index in changedMainInventorySlots) {
                val encoded = if (session.mainInventoryItemIds[index] >= 0 && session.mainInventoryItemCounts[index] > 0) {
                    encodedItemStack(session.mainInventoryItemIds[index], session.mainInventoryItemCounts[index])
                } else {
                    emptyItemStack()
                }
                ctx.write(
                    PlayPackets.containerSetSlotPacket(
                        containerId = 0,
                        stateId = nextInventoryStateId(session),
                        slot = inventorySlotForMainInventoryIndex(index),
                        encodedItemStack = encoded
                    )
                )
            }
            ctx.flush()
        }

        val selectedAfterItemId = session.hotbarItemIds[selectedSlot]
        val selectedAfterCount = session.hotbarItemCounts[selectedSlot]
        if (selectedBeforeItemId != selectedAfterItemId || selectedBeforeCount != selectedAfterCount) {
            broadcastHeldItemEquipment(session)
        }
        return true
    }

    fun handleUseItem(channelId: ChannelId, hand: Int, sequence: Int = -1) {
        if (hand !in 0..1) return
        val session = sessions[channelId] ?: return
        val world = WorldManager.world(session.worldKey) ?: return
        if (session.gameMode == GAME_MODE_SPECTATOR) return
        if (!org.macaroon3145.plugin.PluginSystem.beforePlayerInteract(
                session = session,
                action = org.macaroon3145.api.event.PlayerInteractAction.RIGHT_CLICK_AIR,
                hand = org.macaroon3145.api.entity.Hand.fromId(hand),
                clickedX = null,
                clickedY = null,
                clickedZ = null,
                faceId = null
            )
        ) {
            return
        }
        if (sequence >= 0) {
            val previous = lastUseItemSequenceByPlayerEntityId.put(session.entityId, sequence)
            if (previous != null && sequence <= previous) return
        }

        val slot = session.selectedHotbarSlot.coerceIn(0, 8)
        val itemId = if (hand == 1) session.offhandItemId else session.hotbarItemIds[slot]
        val itemCount = if (hand == 1) session.offhandItemCount else session.hotbarItemCounts[slot]
        if (itemId < 0 || itemCount <= 0) return
        if (tryConsumeMountedPigBoost(session, itemId)) return
        if (tryEquipArmorFromHand(session, hand, itemId)) return
        if (tryStartUsingShield(session, hand, itemId)) return
        if (tryStartUsingSpyglass(session, hand, itemId)) return
        if (tryHandleBucketUseItem(session, world, hand, itemId)) return
        if (tryConsumeFoodItem(session, hand, itemId)) return

        val kind = when (itemId) {
            snowballItemId -> ThrownItemKind.SNOWBALL
            eggItemId -> ThrownItemKind.EGG
            blueEggItemId -> ThrownItemKind.BLUE_EGG
            brownEggItemId -> ThrownItemKind.BROWN_EGG
            enderPearlItemId -> ThrownItemKind.ENDER_PEARL
            else -> return
        }
        var enderPearlCooldownEndNanos = 0L
        if (kind == ThrownItemKind.ENDER_PEARL) {
            enderPearlCooldownEndNanos = tryReserveEnderPearlCooldown(session) ?: return
        }
        val spawned = spawnThrownItemFromPlayer(session, world, kind)
        if (!spawned) {
            if (kind == ThrownItemKind.ENDER_PEARL && enderPearlCooldownEndNanos > 0L) {
                enderPearlCooldownEndNanosByPlayerEntityId.remove(session.entityId, enderPearlCooldownEndNanos)
            }
            return
        }
        if (session.gameMode != GAME_MODE_CREATIVE) {
            consumePlacedItem(session, hand)
        }
    }

    private fun tryReserveEnderPearlCooldown(session: PlayerSession): Long? {
        val now = System.nanoTime()
        var reservedEndAtNanos = 0L
        var blocked = false
        enderPearlCooldownEndNanosByPlayerEntityId.compute(session.entityId) { _, previous ->
            if (previous != null && now < previous) {
                blocked = true
                return@compute previous
            }
            reservedEndAtNanos = now + ENDER_PEARL_COOLDOWN_NANOS
            reservedEndAtNanos
        }
        if (blocked) {
            return null
        }
        sendEnderPearlCooldownUpdate(session, ENDER_PEARL_COOLDOWN_TICKS)
        return reservedEndAtNanos
    }

    private fun sendEnderPearlCooldownUpdate(session: PlayerSession, overrideTicks: Int? = null) {
        val cooldownTicks = overrideTicks ?: run {
            val endAtNanos = enderPearlCooldownEndNanosByPlayerEntityId[session.entityId] ?: return@run 0
            val remainingNanos = (endAtNanos - System.nanoTime()).coerceAtLeast(0L)
            val tickNanos = 1_000_000_000L / MINECRAFT_TICKS_PER_SECOND.toLong()
            ceil(remainingNanos.toDouble() / tickNanos.toDouble()).toInt().coerceAtLeast(0)
        }
        val ctx = contexts[session.channelId] ?: return
        if (!ctx.channel().isActive) return
        val packet = PlayPackets.cooldownPacket(
            cooldownGroup = ENDER_PEARL_COOLDOWN_GROUP,
            cooldownTicks = cooldownTicks
        )
        ctx.executor().execute {
            if (ctx.channel().isActive) {
                ctx.writeAndFlush(packet)
            }
        }
    }

    private fun tryConsumeMountedPigBoost(session: PlayerSession, itemId: Int): Boolean {
        if (itemId != carrotOnAStickItemId || carrotOnAStickItemId < 0) return false
        val animalEntityId = session.ridingAnimalEntityId
        if (animalEntityId < 0) return false
        if (animalEntityId !in saddledAnimalEntityIds) return true
        val world = WorldManager.world(session.worldKey) ?: return true
        val animal = world.animalSnapshot(animalEntityId)
        if (animal == null || animal.kind != AnimalKind.PIG) {
            dismountPlayerFromAnimal(session)
            return true
        }
        while (true) {
            val current = pigBoostStatesByAnimalEntityId[animalEntityId]
            if (current != null && current.elapsedSeconds < current.durationSeconds) {
                return true
            }
            val next = PigBoostState(
                elapsedSeconds = 0.0,
                durationSeconds = randomPigBoostDurationSeconds()
            )
            if (current == null) {
                if (pigBoostStatesByAnimalEntityId.putIfAbsent(animalEntityId, next) == null) {
                    return true
                }
            } else {
                if (pigBoostStatesByAnimalEntityId.replace(animalEntityId, current, next)) {
                    return true
                }
            }
        }
    }

    private fun randomPigBoostDurationSeconds(): Double {
        val durationTicks = PIG_BOOST_MIN_TICKS + ThreadLocalRandom.current().nextInt(PIG_BOOST_RANDOM_SPREAD_TICKS)
        return durationTicks / MINECRAFT_TICKS_PER_SECOND
    }

    private fun tryEquipArmorFromHand(session: PlayerSession, hand: Int, itemId: Int): Boolean {
        if (hand !in 0..1) return false
        val targetSlot = armorEquipTargetSlot(itemId) ?: return false
        val current = readContainerSlot(session, PLAYER_INVENTORY_CONTAINER_ID, targetSlot) ?: return false

        val selectedSlot = session.selectedHotbarSlot.coerceIn(0, 8)
        val handItemId = if (hand == 1) session.offhandItemId else session.hotbarItemIds[selectedSlot]
        val handItemCount = if (hand == 1) session.offhandItemCount else session.hotbarItemCounts[selectedSlot]
        if (handItemId != itemId || handItemCount <= 0) return false
        val equippedItemId = current.first
        val equippedCount = current.second

        if (equippedItemId >= 0 && equippedCount > 0) {
            if (handItemCount == 1) {
                if (hand == 1) {
                    session.offhandItemId = equippedItemId
                    session.offhandItemCount = equippedCount
                } else {
                    session.hotbarItemIds[selectedSlot] = equippedItemId
                    session.hotbarItemCounts[selectedSlot] = equippedCount
                    session.hotbarBlockStateIds[selectedSlot] = itemBlockStateId(equippedItemId)
                    session.hotbarBlockEntityTypeIds[selectedSlot] = -1
                    session.hotbarBlockEntityNbtPayloads[selectedSlot] = null
                }
            } else {
                if (hand == 1) {
                    session.offhandItemCount -= 1
                } else {
                    session.hotbarItemCounts[selectedSlot] -= 1
                }
                if (!tryInsertItemIntoInventory(session, equippedItemId, equippedCount)) {
                    spawnDroppedItemFromPlayer(
                        session = session,
                        itemId = equippedItemId,
                        itemCount = equippedCount,
                        dropStackMotion = equippedCount > 1
                    )
                }
            }
        } else {
            if (hand == 1) {
                session.offhandItemCount -= 1
                if (session.offhandItemCount <= 0) {
                    session.offhandItemId = -1
                    session.offhandItemCount = 0
                }
            } else {
                session.hotbarItemCounts[selectedSlot] -= 1
                if (session.hotbarItemCounts[selectedSlot] <= 0) {
                    session.hotbarItemIds[selectedSlot] = -1
                    session.hotbarItemCounts[selectedSlot] = 0
                    session.hotbarBlockStateIds[selectedSlot] = 0
                    session.hotbarBlockEntityTypeIds[selectedSlot] = -1
                    session.hotbarBlockEntityNbtPayloads[selectedSlot] = null
                }
            }
        }

        val armorIndex = armorIndexForInventorySlot(targetSlot)
        if (armorIndex !in 0..3) return false
        val previousArmorItemId = session.armorItemIds[armorIndex]
        val previousArmorCount = session.armorItemCounts[armorIndex]
        session.armorItemIds[armorIndex] = itemId
        session.armorItemCounts[armorIndex] = 1

        if (hand == 0) {
            session.selectedBlockStateId = if (session.hotbarItemCounts[selectedSlot] > 0) {
                session.hotbarBlockStateIds[selectedSlot]
            } else {
                0
            }
        }

        val ctx = contexts[session.channelId]
        if (ctx != null && ctx.channel().isActive) {
            val stateId = nextInventoryStateId(session)
            val handSlot = if (hand == 1) 45 else (36 + selectedSlot)
            val handEncoded = if (hand == 1) {
                if (session.offhandItemId >= 0 && session.offhandItemCount > 0) {
                    encodedItemStack(session.offhandItemId, session.offhandItemCount)
                } else {
                    emptyItemStack()
                }
            } else {
                if (session.hotbarItemIds[selectedSlot] >= 0 && session.hotbarItemCounts[selectedSlot] > 0) {
                    encodedItemStack(session.hotbarItemIds[selectedSlot], session.hotbarItemCounts[selectedSlot])
                } else {
                    emptyItemStack()
                }
            }
            val armorEncoded = if (session.armorItemIds[armorIndex] >= 0 && session.armorItemCounts[armorIndex] > 0) {
                encodedItemStack(session.armorItemIds[armorIndex], session.armorItemCounts[armorIndex])
            } else {
                emptyItemStack()
            }
            ctx.write(
                PlayPackets.containerSetSlotPacket(
                    containerId = PLAYER_INVENTORY_CONTAINER_ID,
                    stateId = stateId,
                    slot = handSlot,
                    encodedItemStack = handEncoded
                )
            )
            ctx.writeAndFlush(
                PlayPackets.containerSetSlotPacket(
                    containerId = PLAYER_INVENTORY_CONTAINER_ID,
                    stateId = stateId,
                    slot = targetSlot,
                    encodedItemStack = armorEncoded
                )
            )
        }

        broadcastHeldItemEquipment(session)
        maybeBroadcastArmorSlotChangeSound(
            session = session,
            previousItemId = previousArmorItemId,
            previousCount = previousArmorCount,
            nextItemId = session.armorItemIds[armorIndex],
            nextCount = session.armorItemCounts[armorIndex]
        )
        return true
    }

    private fun armorEquipTargetSlot(itemId: Int): Int? {
        if (itemId < 0 || itemId >= itemKeyById.size) return null
        val itemKey = itemKeyById[itemId]
        return when {
            itemKey == "minecraft:elytra" ||
                itemKey.endsWith("_chestplate") -> 6
            itemKey.endsWith("_leggings") -> 7
            itemKey.endsWith("_boots") -> 8
            itemKey.endsWith("_helmet") ||
                itemKey == "minecraft:turtle_helmet" -> 5
            else -> null
        }
    }

    private fun tryConsumeFoodItem(session: PlayerSession, hand: Int, itemId: Int): Boolean {
        val foodData = FoodItemCache.byItemId(itemId) ?: return false
        if (session.gameMode != GAME_MODE_CREATIVE && session.food >= MAX_PLAYER_FOOD && !foodData.alwaysEdible) return false
        if (itemId == chorusFruitItemId && isChorusFruitCooldownActive(session)) {
            return true
        }
        if (session.usingItemRemainingSeconds > 0.0) {
            // Ignore competing use attempts while already consuming.
            // This prevents hand-switch rearming that can double-apply consume effects.
            return true
        }
        session.usingItemHand = hand
        session.usingItemId = itemId
        session.usingItemRemainingSeconds = foodData.consumeSeconds
        session.usingItemSoundKey = foodData.useSoundKey
        session.usingItemSoundDelaySeconds = FOOD_USE_SOUND_INITIAL_DELAY_SECONDS
        broadcastUsingItemState(session)
        return true
    }

    private fun tryStartUsingShield(session: PlayerSession, hand: Int, itemId: Int): Boolean {
        if (itemId != shieldItemId || shieldItemId < 0) return false
        val alreadyUsingShield = session.usingItemRemainingSeconds > 0.0 &&
            session.usingItemId == shieldItemId &&
            session.usingItemHand == hand
        if (alreadyUsingShield) return true
        if (session.usingItemRemainingSeconds > 0.0) {
            stopUsingItem(session)
        }
        session.usingItemHand = hand
        session.usingItemId = shieldItemId
        session.usingItemRemainingSeconds = Double.POSITIVE_INFINITY
        session.usingItemSoundKey = null
        session.usingItemSoundDelaySeconds = 0.0
        broadcastUsingItemState(session)
        return true
    }

    private fun tryStartUsingSpyglass(session: PlayerSession, hand: Int, itemId: Int): Boolean {
        if (itemId != spyglassItemId || spyglassItemId < 0) return false
        val alreadyUsingSpyglass = session.usingItemRemainingSeconds > 0.0 &&
            session.usingItemId == spyglassItemId &&
            session.usingItemHand == hand
        if (alreadyUsingSpyglass) return true
        if (session.usingItemRemainingSeconds > 0.0) {
            stopUsingItem(session)
        }
        session.usingItemHand = hand
        session.usingItemId = spyglassItemId
        session.usingItemRemainingSeconds = Double.POSITIVE_INFINITY
        session.usingItemSoundKey = null
        session.usingItemSoundDelaySeconds = 0.0
        broadcastUsingItemState(session)
        broadcastFoodSoundInWorld(
            session = session,
            soundKey = SPYGLASS_USE_SOUND_KEY,
            volume = 1.0f,
            pitch = 1.0f,
            includeSelf = true
        )
        return true
    }

    private fun finishConsumingFoodItem(session: PlayerSession, hand: Int, expectedItemId: Int): Boolean {
        val slot = session.selectedHotbarSlot.coerceIn(0, 8)
        val currentItemId = if (hand == 1) session.offhandItemId else session.hotbarItemIds[slot]
        val currentCount = if (hand == 1) session.offhandItemCount else session.hotbarItemCounts[slot]
        if (currentItemId != expectedItemId || currentCount <= 0) return false
        val foodData = FoodItemCache.byItemId(expectedItemId) ?: return false
        if (session.gameMode != GAME_MODE_CREATIVE && session.food >= MAX_PLAYER_FOOD && !foodData.alwaysEdible) return false

        val previousFood = session.food
        val previousSaturation = session.saturation
        val nextFood = (session.food + foodData.nutrition).coerceAtMost(MAX_PLAYER_FOOD)
        val saturationGain = (foodData.nutrition.toFloat() * foodData.saturationModifier * 2f).coerceAtLeast(0f)
        val nextSaturation = (session.saturation + saturationGain).coerceAtMost(nextFood.toFloat())

        session.food = nextFood
        session.saturation = nextSaturation
        if (session.gameMode != GAME_MODE_CREATIVE) {
            consumeFoodItemAndHandleRemainder(session, hand, foodData.remainderItemId)
        }
        broadcastEntityEventInWorld(session, ENTITY_EVENT_FINISH_USING_ITEM)
        broadcastFoodFinishSound(session, foodData.finishSoundKey)
        if (expectedItemId == chorusFruitItemId) {
            applyChorusFruitConsumeEffect(session)
            applyChorusFruitCooldown(session)
        }

        if (session.food != previousFood || session.saturation != previousSaturation) {
            sendHealthPacket(session)
        }
        return true
    }

    private fun isChorusFruitCooldownActive(session: PlayerSession): Boolean {
        val endAt = chorusFruitCooldownEndNanosByPlayerEntityId[session.entityId] ?: return false
        return if (System.nanoTime() < endAt) {
            true
        } else {
            chorusFruitCooldownEndNanosByPlayerEntityId.remove(session.entityId, endAt)
            false
        }
    }

    private fun applyChorusFruitCooldown(session: PlayerSession) {
        val now = System.nanoTime()
        val endAt = now + CHORUS_FRUIT_COOLDOWN_NANOS
        chorusFruitCooldownEndNanosByPlayerEntityId[session.entityId] = endAt
        sendChorusFruitCooldownUpdate(session, CHORUS_FRUIT_COOLDOWN_TICKS)
    }

    private fun sendChorusFruitCooldownUpdate(session: PlayerSession, overrideTicks: Int? = null) {
        val cooldownTicks = overrideTicks ?: run {
            val endAtNanos = chorusFruitCooldownEndNanosByPlayerEntityId[session.entityId] ?: return@run 0
            val remainingNanos = (endAtNanos - System.nanoTime()).coerceAtLeast(0L)
            val tickNanos = 1_000_000_000L / MINECRAFT_TICKS_PER_SECOND.toLong()
            ceil(remainingNanos.toDouble() / tickNanos.toDouble()).toInt().coerceAtLeast(0)
        }
        val ctx = contexts[session.channelId] ?: return
        if (!ctx.channel().isActive) return
        val packet = PlayPackets.cooldownPacket(
            cooldownGroup = CHORUS_FRUIT_COOLDOWN_GROUP,
            cooldownTicks = cooldownTicks
        )
        ctx.executor().execute {
            if (ctx.channel().isActive) {
                ctx.writeAndFlush(packet)
            }
        }
    }

    fun handleReleaseUseItem(channelId: ChannelId) {
        val session = sessions[channelId] ?: return
        stopUsingItem(session)
    }

    private fun stopUsingItem(session: PlayerSession) {
        val wasUsing = session.usingItemRemainingSeconds > 0.0 && session.usingItemHand in 0..1
        val usedItemId = session.usingItemId
        session.usingItemHand = -1
        session.usingItemId = -1
        session.usingItemRemainingSeconds = 0.0
        session.usingItemSoundKey = null
        session.usingItemSoundDelaySeconds = 0.0
        if (wasUsing) {
            broadcastUsingItemState(session)
            if (usedItemId == spyglassItemId && spyglassItemId >= 0) {
                broadcastFoodSoundInWorld(
                    session = session,
                    soundKey = SPYGLASS_STOP_USING_SOUND_KEY,
                    volume = 1.0f,
                    pitch = 1.0f,
                    includeSelf = true
                )
            }
        }
    }

    private fun broadcastUsingItemState(session: PlayerSession) {
        val metadataPacket = PlayPackets.playerSharedFlagsMetadataPacket(
            entityId = session.entityId,
            sneaking = session.sneaking,
            sprinting = session.sprinting,
            swimming = session.swimming,
            usingItemHand = session.usingItemHand
        )
        for ((id, other) in sessions) {
            if (id == session.channelId || other.worldKey != session.worldKey) continue
            val otherCtx = contexts[id] ?: continue
            if (!otherCtx.channel().isActive) continue
            otherCtx.writeAndFlush(metadataPacket)
        }
    }

    private fun consumeFoodItemAndHandleRemainder(session: PlayerSession, hand: Int, remainderItemId: Int) {
        if (remainderItemId < 0) {
            consumePlacedItem(session, hand)
            return
        }
        val ctx = contexts[session.channelId]
        if (hand == 1) {
            if (session.offhandItemCount == 1 && session.offhandItemId >= 0) {
                session.offhandItemId = remainderItemId
                session.offhandItemCount = 1
                if (ctx != null && ctx.channel().isActive) {
                    ctx.writeAndFlush(
                        PlayPackets.containerSetSlotPacket(
                            containerId = 0,
                            stateId = nextInventoryStateId(session),
                            slot = 45,
                            encodedItemStack = encodedItemStack(remainderItemId, 1)
                        )
                    )
                }
                broadcastHeldItemEquipment(session)
                return
            }
            consumePlacedItem(session, hand)
            if (!tryInsertItemIntoInventory(session, remainderItemId, 1)) {
                spawnDroppedItemFromPlayer(session, remainderItemId, 1, dropStackMotion = false)
            }
            return
        }

        val slot = session.selectedHotbarSlot.coerceIn(0, 8)
        if (session.hotbarItemCounts[slot] == 1 && session.hotbarItemIds[slot] >= 0) {
            session.hotbarItemIds[slot] = remainderItemId
            session.hotbarItemCounts[slot] = 1
            session.hotbarBlockStateIds[slot] = 0
            session.hotbarBlockEntityTypeIds[slot] = -1
            session.hotbarBlockEntityNbtPayloads[slot] = null
            session.selectedBlockStateId = 0
            if (ctx != null && ctx.channel().isActive) {
                ctx.writeAndFlush(
                    PlayPackets.containerSetSlotPacket(
                        containerId = 0,
                        stateId = nextInventoryStateId(session),
                        slot = 36 + slot,
                        encodedItemStack = encodedItemStack(remainderItemId, 1)
                    )
                )
            }
            broadcastHeldItemEquipment(session)
            return
        }

        consumePlacedItem(session, hand)
        if (!tryInsertItemIntoInventory(session, remainderItemId, 1)) {
            spawnDroppedItemFromPlayer(session, remainderItemId, 1, dropStackMotion = false)
        }
    }

    private fun applyChorusFruitConsumeEffect(session: PlayerSession): Boolean {
        val world = WorldManager.world(session.worldKey) ?: return false
        val (minY, maxY) = chorusTeleportYBounds(session.worldKey)
        if (session.ridingAnimalEntityId >= 0) {
            stopRidingForTeleport(session)
        }

        val random = ThreadLocalRandom.current()
        val height = playerPoseHeight(session)
        repeat(CHORUS_FRUIT_RANDOM_TELEPORT_ATTEMPTS) {
            val targetX = session.x + (random.nextDouble() - 0.5) * CHORUS_FRUIT_RANDOM_TELEPORT_DIAMETER
            val targetYRaw = session.y + (random.nextDouble() - 0.5) * CHORUS_FRUIT_RANDOM_TELEPORT_DIAMETER
            val targetZ = session.z + (random.nextDouble() - 0.5) * CHORUS_FRUIT_RANDOM_TELEPORT_DIAMETER
            val clampedTargetY = targetYRaw.coerceIn(minY.toDouble(), maxY.toDouble())
            val resolvedY = resolveChorusTeleportLandingY(world, targetX, clampedTargetY, targetZ, minY) ?: return@repeat
            if (!isPlayerAabbClearCachedPrecise(world, targetX, resolvedY, targetZ, height)) return@repeat
            if (playerAabbContainsLiquidCached(world, targetX, resolvedY, targetZ, height)) return@repeat
            if (!teleportPlayer(session, targetX, resolvedY, targetZ, null, null, reason = MoveReason.CHORUS_FRUIT)) return@repeat

            broadcastEntityEventInWorld(session, ENTITY_EVENT_TELEPORT)
            broadcastFoodSoundInWorld(
                session = session,
                soundKey = CHORUS_FRUIT_TELEPORT_SOUND_KEY,
                volume = 1.0f,
                pitch = 1.0f,
                includeSelf = true
            )
            session.fallDistance = 0.0
            return true
        }
        return false
    }

    private fun chorusTeleportYBounds(worldKey: String): Pair<Int, Int> {
        return dimensionMinMaxYByWorldKey[worldKey]
            ?: (CHORUS_FRUIT_MIN_TELEPORT_Y_FALLBACK to CHORUS_FRUIT_MAX_TELEPORT_Y_FALLBACK)
    }

    private fun resolveChorusTeleportLandingY(
        world: org.macaroon3145.world.World,
        x: Double,
        y: Double,
        z: Double,
        minY: Int
    ): Double? {
        val minX = x - PLAYER_HITBOX_HALF_WIDTH
        val maxX = x + PLAYER_HITBOX_HALF_WIDTH
        val minZ = z - PLAYER_HITBOX_HALF_WIDTH
        val maxZ = z + PLAYER_HITBOX_HALF_WIDTH
        val startX = floor(minX).toInt()
        val endX = floor(maxX - 1.0e-7).toInt()
        val startZ = floor(minZ).toInt()
        val endZ = floor(maxZ - 1.0e-7).toInt()

        var bestTop = Double.NEGATIVE_INFINITY
        val maxBy = floor(y + CHORUS_TELEPORT_CONTACT_EPSILON).toInt()
        for (by in maxBy downTo minY) {
            for (bx in startX..endX) {
                for (bz in startZ..endZ) {
                    val stateId = world.blockStateAtIfCached(bx, by, bz) ?: return null
                    if (stateId <= 0) continue
                    val parsed = BlockStateRegistry.parsedState(stateId)
                    if (parsed != null && parsed.blockKey in NON_SUPPORT_BLOCK_KEYS) continue
                    val resolved = if (parsed == null) null else BlockCollisionRegistry.boxesForStateId(stateId, parsed)
                    if (resolved == null) {
                        val boxMinX = bx.toDouble()
                        val boxMaxX = bx + 1.0
                        val boxMinZ = bz.toDouble()
                        val boxMaxZ = bz + 1.0
                        if (!aabbOverlapHorizontal(minX, maxX, minZ, maxZ, boxMinX, boxMaxX, boxMinZ, boxMaxZ)) continue
                        val top = by + 1.0
                        if (top <= y + CHORUS_TELEPORT_CONTACT_EPSILON && top > bestTop) {
                            bestTop = top
                        }
                        continue
                    }
                    for (collisionBox in resolved) {
                        if (collisionBox.maxY - collisionBox.minY <= CHORUS_TELEPORT_CONTACT_EPSILON) continue
                        val boxMinX = bx + collisionBox.minX
                        val boxMaxX = bx + collisionBox.maxX
                        val boxMinZ = bz + collisionBox.minZ
                        val boxMaxZ = bz + collisionBox.maxZ
                        if (!aabbOverlapHorizontal(minX, maxX, minZ, maxZ, boxMinX, boxMaxX, boxMinZ, boxMaxZ)) continue
                        val top = by + collisionBox.maxY
                        if (top <= y + CHORUS_TELEPORT_CONTACT_EPSILON && top > bestTop) {
                            bestTop = top
                        }
                    }
                }
            }
        }
        return if (bestTop.isFinite()) bestTop else null
    }

    private fun isPlayerAabbClearCachedPrecise(
        world: org.macaroon3145.world.World,
        x: Double,
        y: Double,
        z: Double,
        height: Double
    ): Boolean {
        val playerBox = playerAabbAt(
            x = x,
            y = y,
            z = z,
            halfWidth = PLAYER_HITBOX_HALF_WIDTH,
            height = height
        )
        val startX = floor(playerBox.minX).toInt()
        val endX = floor(playerBox.maxX - 1.0e-7).toInt()
        val startY = floor(playerBox.minY).toInt()
        val endY = floor(playerBox.maxY - 1.0e-7).toInt()
        val startZ = floor(playerBox.minZ).toInt()
        val endZ = floor(playerBox.maxZ - 1.0e-7).toInt()
        for (bx in startX..endX) {
            for (by in startY..endY) {
                for (bz in startZ..endZ) {
                    val stateId = world.blockStateAtIfCached(bx, by, bz) ?: return false
                    if (stateId <= 0) continue
                    val parsed = BlockStateRegistry.parsedState(stateId) ?: continue
                    if (parsed.blockKey in NON_SUPPORT_BLOCK_KEYS) continue
                    val resolved = BlockCollisionRegistry.boxesForStateId(stateId, parsed)
                    if (resolved == null) {
                        if (aabbOverlap(
                                playerBox,
                                Aabb(
                                    minX = bx.toDouble(),
                                    minY = by.toDouble(),
                                    minZ = bz.toDouble(),
                                    maxX = bx + 1.0,
                                    maxY = by + 1.0,
                                    maxZ = bz + 1.0
                                )
                            )
                        ) {
                            return false
                        }
                        continue
                    }
                    for (collisionBox in resolved) {
                        val boxMinX = bx + collisionBox.minX
                        val boxMinY = by + collisionBox.minY
                        val boxMinZ = bz + collisionBox.minZ
                        val boxMaxX = bx + collisionBox.maxX
                        val boxMaxY = by + collisionBox.maxY
                        val boxMaxZ = bz + collisionBox.maxZ
                        if (aabbOverlap(
                                playerBox,
                                Aabb(
                                    minX = boxMinX,
                                    minY = boxMinY,
                                    minZ = boxMinZ,
                                    maxX = boxMaxX,
                                    maxY = boxMaxY,
                                    maxZ = boxMaxZ
                                )
                            )
                        ) {
                            return false
                        }
                    }
                }
            }
        }
        return true
    }

    private fun aabbOverlapHorizontal(
        aMinX: Double,
        aMaxX: Double,
        aMinZ: Double,
        aMaxZ: Double,
        bMinX: Double,
        bMaxX: Double,
        bMinZ: Double,
        bMaxZ: Double
    ): Boolean {
        return aMaxX > bMinX && aMinX < bMaxX &&
            aMaxZ > bMinZ && aMinZ < bMaxZ
    }

    private fun playerAabbContainsLiquidCached(
        world: org.macaroon3145.world.World,
        x: Double,
        y: Double,
        z: Double,
        height: Double
    ): Boolean {
        val playerBox = playerAabbAt(
            x = x,
            y = y,
            z = z,
            halfWidth = PLAYER_HITBOX_HALF_WIDTH,
            height = height
        )
        val startX = floor(playerBox.minX).toInt()
        val endX = floor(playerBox.maxX - 1.0e-7).toInt()
        val startY = floor(playerBox.minY).toInt()
        val endY = floor(playerBox.maxY - 1.0e-7).toInt()
        val startZ = floor(playerBox.minZ).toInt()
        val endZ = floor(playerBox.maxZ - 1.0e-7).toInt()
        for (bx in startX..endX) {
            for (by in startY..endY) {
                for (bz in startZ..endZ) {
                    val stateId = world.blockStateAtIfCached(bx, by, bz) ?: return true
                    if (isLiquidBlockState(stateId)) return true
                }
            }
        }
        return false
    }

    private fun stopRidingForTeleport(session: PlayerSession) {
        val mountedAnimalEntityId = animalEntityIdByRiderEntityId.remove(session.entityId)
            ?: session.ridingAnimalEntityId.takeIf { it >= 0 }
            ?: return
        val mappedRider = animalRiderEntityIdByAnimalEntityId[mountedAnimalEntityId]
        if (mappedRider == session.entityId) {
            animalRiderEntityIdByAnimalEntityId.remove(mountedAnimalEntityId)
        }
        session.ridingAnimalEntityId = -1
        broadcastAnimalPassengers(session.worldKey, mountedAnimalEntityId)
    }

    private fun isLiquidBlockState(stateId: Int): Boolean {
        if (stateId <= 0) return false
        return liquidStateCache.computeIfAbsent(stateId) { id ->
            val parsed = BlockStateRegistry.parsedState(id) ?: return@computeIfAbsent false
            val key = parsed.blockKey
            key == "minecraft:water" ||
                key == "minecraft:lava" ||
                key == "minecraft:bubble_column" ||
                parsed.properties["waterlogged"] == "true"
        }
    }

    private fun spawnThrownItemFromPlayer(
        session: PlayerSession,
        world: org.macaroon3145.world.World,
        kind: ThrownItemKind
    ): Boolean {
        val yawRad = Math.toRadians(session.yaw.toDouble())
        val pitchRad = Math.toRadians(session.pitch.toDouble())
        val cosPitch = cos(pitchRad)
        val dirX = -sin(yawRad) * cosPitch
        val dirY = -sin(pitchRad)
        val dirZ = cos(yawRad) * cosPitch

        val spawnX = session.clientEyeX + (dirX * 0.4)
        val spawnY = session.clientEyeY - 0.1
        val spawnZ = session.clientEyeZ + (dirZ * 0.4)
        val launchSpeed = THROWN_ITEM_LAUNCH_SPEED
        val vx = dirX * launchSpeed
        val vy = dirY * launchSpeed + THROWN_ITEM_LAUNCH_VERTICAL_BOOST
        val vz = dirZ * launchSpeed
        val entityId = allocateEntityId()
        if (!world.spawnThrownItem(entityId, session.entityId, kind, spawnX, spawnY, spawnZ, vx, vy, vz)) {
            return false
        }
        // Keep shooter-side client prediction aligned by sending authoritative spawn immediately.
        val snapshot = world.thrownItemSnapshot(entityId)
        if (snapshot != null && session.loadedChunks.contains(snapshot.chunkPos)) {
            if (session.visibleThrownItemEntityIds.add(snapshot.entityId)) {
                val selfCtx = contexts[session.channelId]
                if (selfCtx != null && selfCtx.channel().isActive) {
                    writeThrownItemSpawnPackets(session, selfCtx, snapshot)
                    selfCtx.flush()
                }
            }
        }
        return true
    }

    private fun tryHandleBucketUseItem(
        session: PlayerSession,
        world: org.macaroon3145.world.World,
        hand: Int,
        itemId: Int
    ): Boolean {
        if (itemId != emptyBucketItemId) return false
        val waterPos = raycastWaterSourceBlock(session, world) ?: return false
        if (!isChunkLoadedForSession(session, waterPos.x, waterPos.z)) return false
        val clickedState = world.blockStateAt(waterPos.x, waterPos.y, waterPos.z)
        if (!canPickupWaterState(clickedState)) return false
        setBlockAndBroadcast(
            session = session,
            x = waterPos.x,
            y = waterPos.y,
            z = waterPos.z,
            stateId = 0,
            reason = BlockChangeReason.PLAYER_BUCKET_FILL,
            logAsFluidSync = true
        )
        world.resetFluidTickAccumulator()
        replaceUsedEmptyBucketWithWaterBucket(session, hand)
        return true
    }

    private fun raycastWaterSourceBlock(
        session: PlayerSession,
        world: org.macaroon3145.world.World
    ): BlockPos? {
        val yawRad = Math.toRadians(session.yaw.toDouble())
        val pitchRad = Math.toRadians(session.pitch.toDouble())
        val cosPitch = cos(pitchRad)
        val dirX = -sin(yawRad) * cosPitch
        val dirY = -sin(pitchRad)
        val dirZ = cos(yawRad) * cosPitch
        val eyeX = session.clientEyeX
        val eyeY = session.clientEyeY
        val eyeZ = session.clientEyeZ
        val maxDistance = 5.0
        val step = 0.1
        var distance = 0.0
        var lastX = Int.MIN_VALUE
        var lastY = Int.MIN_VALUE
        var lastZ = Int.MIN_VALUE
        while (distance <= maxDistance) {
            val sampleX = eyeX + dirX * distance
            val sampleY = eyeY + dirY * distance
            val sampleZ = eyeZ + dirZ * distance
            val blockX = kotlin.math.floor(sampleX).toInt()
            val blockY = kotlin.math.floor(sampleY).toInt()
            val blockZ = kotlin.math.floor(sampleZ).toInt()
            if (blockX == lastX && blockY == lastY && blockZ == lastZ) {
                distance += step
                continue
            }
            lastX = blockX
            lastY = blockY
            lastZ = blockZ

            val stateId = world.blockStateAt(blockX, blockY, blockZ)
            if (stateId == 0) {
                distance += step
                continue
            }
            return if (canPickupWaterState(stateId)) BlockPos(blockX, blockY, blockZ) else null
        }
        return null
    }

    private fun thrownItemLaunchSoundPacket(kind: ThrownItemKind, x: Double, y: Double, z: Double): ByteArray {
        val soundKey = when (kind) {
            ThrownItemKind.SNOWBALL -> "minecraft:entity.snowball.throw"
            ThrownItemKind.EGG, ThrownItemKind.BLUE_EGG, ThrownItemKind.BROWN_EGG -> "minecraft:entity.egg.throw"
            ThrownItemKind.ENDER_PEARL -> "minecraft:entity.ender_pearl.throw"
        }
        return PlayPackets.soundPacketByKey(
            soundKey = soundKey,
            soundSourceId = NEUTRAL_SOUND_SOURCE_ID,
            x = x,
            y = y,
            z = z,
            volume = 0.5f,
            pitch = 0.4f / (ThreadLocalRandom.current().nextFloat() * 0.4f + 0.8f),
            seed = ThreadLocalRandom.current().nextLong()
        )
    }

    private fun broadcastFoodUseProgressSound(session: PlayerSession, soundKey: String) {
        broadcastFoodSoundInWorld(
            session = session,
            soundKey = soundKey,
            volume = 1.0f,
            pitch = 1.0f,
            includeSelf = false
        )
    }

    private fun broadcastFoodFinishSound(session: PlayerSession, soundKey: String) {
        broadcastFoodSoundInWorld(
            session = session,
            soundKey = soundKey,
            volume = 0.5f,
            pitch = ThreadLocalRandom.current().nextFloat() * 0.1f + 0.9f,
            includeSelf = true
        )
    }

    private fun broadcastFoodSoundInWorld(
        session: PlayerSession,
        soundKey: String,
        volume: Float,
        pitch: Float,
        includeSelf: Boolean
    ) {
        val packet = PlayPackets.soundPacketByKey(
            soundKey = soundKey,
            soundSourceId = PLAYERS_SOUND_SOURCE_ID,
            x = session.x,
            y = session.y,
            z = session.z,
            volume = volume,
            pitch = pitch,
            seed = ThreadLocalRandom.current().nextLong()
        )
        for ((id, other) in sessions) {
            if (other.worldKey != session.worldKey) continue
            if (!includeSelf && other.entityId == session.entityId) continue
            val ctx = contexts[id] ?: continue
            if (!ctx.channel().isActive) continue
            ctx.write(packet)
            ctx.flush()
        }
    }

    private fun maybeBroadcastArmorSlotChangeSound(
        session: PlayerSession,
        previousItemId: Int,
        previousCount: Int,
        nextItemId: Int,
        nextCount: Int
    ) {
        val hadItem = previousItemId >= 0 && previousCount > 0
        val hasItem = nextItemId >= 0 && nextCount > 0
        if (!hasItem) return
        // Ignore no-op rewrites (e.g. container resync) to prevent duplicate equip sounds.
        if (hadItem == hasItem && previousItemId == nextItemId && previousCount == nextCount) return
        val key = armorEquipSoundKey(nextItemId) ?: return
        val packet = PlayPackets.soundPacketByKey(
            soundKey = key,
            soundSourceId = PLAYERS_SOUND_SOURCE_ID,
            x = session.x,
            y = session.y,
            z = session.z,
            volume = 1.0f,
            pitch = 1.0f,
            seed = ThreadLocalRandom.current().nextLong()
        )
        for ((id, other) in sessions) {
            if (other.worldKey != session.worldKey) continue
            val ctx = contexts[id] ?: continue
            if (!ctx.channel().isActive) continue
            ctx.write(packet)
            ctx.flush()
        }
    }

    private fun armorEquipSoundKey(itemId: Int): String? {
        if (itemId < 0 || itemId >= itemKeyById.size) return null
        val itemKey = itemKeyById[itemId]
        return armorEquipSoundKeyByItemKey[itemKey] ?: "minecraft:item.armor.equip_generic"
    }

    private fun buildVanillaArmorEquipSoundMap(): Map<String, String> {
        val out = HashMap<String, String>(64)
        val armorItemKeys = LinkedHashSet<String>()
        armorItemKeys.addAll(loadItemTag("head_armor"))
        armorItemKeys.addAll(loadItemTag("chest_armor"))
        armorItemKeys.addAll(loadItemTag("leg_armor"))
        armorItemKeys.addAll(loadItemTag("foot_armor"))

        for (itemKey in armorItemKeys) {
            val soundKey = when {
                itemKey == "minecraft:turtle_helmet" -> "minecraft:item.armor.equip_turtle"
                itemKey.startsWith("minecraft:leather_") -> "minecraft:item.armor.equip_leather"
                itemKey.startsWith("minecraft:copper_") -> "minecraft:item.armor.equip_copper"
                itemKey.startsWith("minecraft:chainmail_") -> "minecraft:item.armor.equip_chain"
                itemKey.startsWith("minecraft:iron_") -> "minecraft:item.armor.equip_iron"
                itemKey.startsWith("minecraft:golden_") -> "minecraft:item.armor.equip_gold"
                itemKey.startsWith("minecraft:diamond_") -> "minecraft:item.armor.equip_diamond"
                itemKey.startsWith("minecraft:netherite_") -> "minecraft:item.armor.equip_netherite"
                else -> null
            }
            if (soundKey != null) {
                out[itemKey] = soundKey
            }
        }

        // Non-armor-slot equippables.
        out["minecraft:elytra"] = "minecraft:item.armor.equip_elytra"
        out["minecraft:wolf_armor"] = "minecraft:item.armor.equip_wolf"
        return out
    }

    private fun maybeBroadcastOffhandSlotChangeSound(
        session: PlayerSession,
        previousItemId: Int,
        previousCount: Int,
        nextItemId: Int,
        nextCount: Int
    ) {
        val hadItem = previousItemId >= 0 && previousCount > 0
        val hasItem = nextItemId >= 0 && nextCount > 0
        if (hadItem == hasItem && previousItemId == nextItemId) return
        if (!hasItem) return
        if (shieldItemId < 0) return
        val effectiveItemId = nextItemId
        if (effectiveItemId != shieldItemId) return
        val packet = PlayPackets.soundPacketByKey(
            soundKey = "minecraft:item.armor.equip_generic",
            soundSourceId = PLAYERS_SOUND_SOURCE_ID,
            x = session.x,
            y = session.y,
            z = session.z,
            volume = 1.0f,
            pitch = 1.0f,
            seed = ThreadLocalRandom.current().nextLong()
        )
        for ((id, other) in sessions) {
            if (other.worldKey != session.worldKey) continue
            val ctx = contexts[id] ?: continue
            if (!ctx.channel().isActive) continue
            ctx.write(packet)
            ctx.flush()
        }
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

        val spawnX = session.clientEyeX + (dirX * 0.4)
        val spawnY = session.clientEyeY - 0.27
        val spawnZ = session.clientEyeZ + (dirZ * 0.4)
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
        val chunkItemsCache = HashMap<ChunkPos, List<DroppedItemSnapshot>>()

        for ((collectorOrder, collector) in collectors.withIndex()) {
            val playerChunkX = ChunkStreamingService.chunkXFromBlockX(collector.x)
            val playerChunkZ = ChunkStreamingService.chunkZFromBlockZ(collector.z)

            for (chunkX in (playerChunkX - 1)..(playerChunkX + 1)) {
                for (chunkZ in (playerChunkZ - 1)..(playerChunkZ + 1)) {
                    val chunkPos = ChunkPos(chunkX, chunkZ)
                    val items = chunkItemsCache.getOrPut(chunkPos) {
                        world.droppedItemsInChunk(chunkX, chunkZ)
                    }
                    if (items.isEmpty()) continue
                    for (snapshot in items) {
                        if (snapshot.pickupDelaySeconds > DROPPED_ITEM_PICKUP_DELAY_EPSILON) continue
                        if (!canPlayerPickDroppedItem(collector, snapshot)) continue
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

            val removedSnapshot = world.removeDroppedItemIfUuidMatches(snapshot.entityId, snapshot.uuid) ?: continue
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
        val playerCenterY = session.y + (playerPoseHeight(session) * 0.5)
        val itemCenterY = snapshot.y + (DROPPED_ITEM_HEIGHT * 0.5)
        val dx = session.x - snapshot.x
        val dy = playerCenterY - itemCenterY
        val dz = session.z - snapshot.z
        return (dx * dx) + (dy * dy) + (dz * dz)
    }

    private fun canPlayerPickDroppedItem(session: PlayerSession, snapshot: DroppedItemSnapshot): Boolean {
        val playerBox = playerAabb(
            session = session,
            expandHorizontal = DROPPED_ITEM_PICKUP_EXPAND_HORIZONTAL,
            expandDown = DROPPED_ITEM_PICKUP_EXPAND_DOWN,
            expandUp = DROPPED_ITEM_PICKUP_EXPAND_UP
        )
        val itemBox = droppedItemAabb(snapshot)
        return aabbOverlap(playerBox, itemBox)
    }

    private fun canAbsorbDroppedItem(session: PlayerSession, itemId: Int, itemCount: Int): Boolean {
        if (itemId < 0 || itemCount <= 0) return false
        val maxStackSize = itemMaxStackSize(itemId)
        var remaining = itemCount

        for (slot in 0..8) {
            if (session.hotbarItemCounts[slot] <= 0 || session.hotbarItemIds[slot] != itemId) continue
            val free = maxStackSize - session.hotbarItemCounts[slot]
            if (free <= 0) continue
            remaining -= free
            if (remaining <= 0) return true
        }
        for (slot in 0..8) {
            if (session.hotbarItemCounts[slot] > 0 && session.hotbarItemIds[slot] >= 0) continue
            remaining -= maxStackSize
            if (remaining <= 0) return true
        }

        for (index in session.mainInventoryItemIds.indices) {
            if (session.mainInventoryItemCounts[index] <= 0 || session.mainInventoryItemIds[index] != itemId) continue
            val free = maxStackSize - session.mainInventoryItemCounts[index]
            if (free <= 0) continue
            remaining -= free
            if (remaining <= 0) return true
        }
        for (index in session.mainInventoryItemIds.indices) {
            if (session.mainInventoryItemCounts[index] > 0 && session.mainInventoryItemIds[index] >= 0) continue
            remaining -= maxStackSize
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
        val maxStackSize = itemMaxStackSize(itemId)
        var remaining = itemCount
        val changedHotbarSlots = ArrayList<Int>(3)
        val changedMainInventorySlots = ArrayList<Int>(3)
        val selectedSlot = session.selectedHotbarSlot.coerceIn(0, 8)
        val selectedBeforeItemId = session.hotbarItemIds[selectedSlot]
        val selectedBeforeCount = session.hotbarItemCounts[selectedSlot]

        for (slot in 0..8) {
            if (remaining <= 0) break
            if (session.hotbarItemCounts[slot] <= 0 || session.hotbarItemIds[slot] != itemId) continue
            val free = maxStackSize - session.hotbarItemCounts[slot]
            if (free <= 0) continue
            val added = minOf(free, remaining)
            session.hotbarItemCounts[slot] += added
            remaining -= added
            changedHotbarSlots.add(slot)
        }

        for (slot in 0..8) {
            if (remaining <= 0) break
            if (session.hotbarItemCounts[slot] > 0 && session.hotbarItemIds[slot] >= 0) continue
            val added = minOf(maxStackSize, remaining)
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
            val free = maxStackSize - session.mainInventoryItemCounts[index]
            if (free <= 0) continue
            val added = minOf(free, remaining)
            session.mainInventoryItemCounts[index] += added
            remaining -= added
            changedMainInventorySlots.add(index)
        }

        for (index in session.mainInventoryItemIds.indices) {
            if (remaining <= 0) break
            if (session.mainInventoryItemCounts[index] > 0 && session.mainInventoryItemIds[index] >= 0) continue
            val added = minOf(maxStackSize, remaining)
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
        maybeBroadcastOffhandSlotChangeSound(
            session = session,
            previousItemId = offItemId,
            previousCount = offItemCount,
            nextItemId = session.offhandItemId,
            nextCount = session.offhandItemCount
        )

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
        return encodedHotbarSlot(session, slot)
    }

    private fun armorEncodedItemStack(session: PlayerSession, armorIndex: Int): ByteArray {
        return encodedArmorSlot(session, armorIndex)
    }

    private fun offhandEncodedItemStack(session: PlayerSession): ByteArray {
        return encodedOffhandSlot(session)
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
        val existing = outboundByContext[ctx]
        if (existing != null) {
            existing.add(packet)
            return
        }
        if (packet.size <= SMALL_ENTITY_PACKET_IMMEDIATE_BYTES) {
            enqueueChannelFlushPacket(ctx, packet)
            return
        }
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
        val encodedX = encodeDroppedItemPosition4096(snapshot.x)
        val encodedY = encodeDroppedItemPosition4096(snapshot.y)
        val encodedZ = encodeDroppedItemPosition4096(snapshot.z)

        val dx = encodedX - state.encodedX4096
        val dy = encodedY - state.encodedY4096
        val dz = encodedZ - state.encodedZ4096
        val hasPositionDelta = dx != 0L || dy != 0L || dz != 0L
        val onGroundChanged = state.lastOnGround != snapshot.onGround

        val packets = ArrayList<ByteArray>(2)
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
            movementPacketSent = true
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
            secondsSinceHardSync = 0.0
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
        val encodedX = encodeDroppedItemPosition4096(snapshot.x)
        val encodedY = encodeDroppedItemPosition4096(snapshot.y)
        val encodedZ = encodeDroppedItemPosition4096(snapshot.z)
        val dx = encodedX - state.encodedX4096
        val dy = encodedY - state.encodedY4096
        val dz = encodedZ - state.encodedZ4096
        val hasPositionDelta = dx != 0L || dy != 0L || dz != 0L
        val onGroundChanged = state.lastOnGround != snapshot.onGround

        val packets = ArrayList<ByteArray>(2)
        val shouldEmitMovement = hasPositionDelta || onGroundChanged
        var movementPacketSent = false

        if (shouldEmitMovement) {
            val requiresHardSync =
                dx < Short.MIN_VALUE.toLong() || dx > Short.MAX_VALUE.toLong() ||
                    dy < Short.MIN_VALUE.toLong() || dy > Short.MAX_VALUE.toLong() ||
                    dz < Short.MIN_VALUE.toLong() || dz > Short.MAX_VALUE.toLong() ||
                    state.secondsSinceHardSync >= MAX_DROPPED_ITEM_RELATIVE_SECONDS_BEFORE_HARD_SYNC
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
            movementPacketSent = true
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

    private fun syncThrownItemSnapshot(
        snapshot: ThrownItemSnapshot,
        worldSessions: List<PlayerSession>,
        outboundByContext: MutableMap<ChannelHandlerContext, MutableList<ByteArray>>,
        sendPositionUpdate: Boolean,
        deltaSeconds: Double
    ) {
        val chunkPos = snapshot.chunkPos
        var removePacket: ByteArray? = null
        val (syncVx, syncVy, syncVz) = thrownItemNetworkVelocity(snapshot)
        for (session in worldSessions) {
            val shouldBeVisible = session.loadedChunks.contains(chunkPos)
            val ctx = contexts[session.channelId] ?: continue
            if (!ctx.channel().isActive) continue

            if (shouldBeVisible) {
                if (session.visibleThrownItemEntityIds.add(snapshot.entityId)) {
                    queueThrownItemSpawnPackets(
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
                    val movementPackets = thrownItemMovementPackets(
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

            if (session.visibleThrownItemEntityIds.remove(snapshot.entityId)) {
                session.thrownItemTrackerStates.remove(snapshot.entityId)
                val packet = removePacket ?: PlayPackets.removeEntitiesPacket(intArrayOf(snapshot.entityId)).also {
                    removePacket = it
                }
                enqueueDroppedItemPacket(outboundByContext, ctx, packet)
            }
        }
    }

    private fun queueThrownItemSpawnPackets(
        outboundByContext: MutableMap<ChannelHandlerContext, MutableList<ByteArray>>,
        ctx: ChannelHandlerContext,
        session: PlayerSession,
        snapshot: ThrownItemSnapshot,
        syncVx: Double,
        syncVy: Double,
        syncVz: Double
    ) {
        val entityTypeId = when (snapshot.kind) {
            ThrownItemKind.SNOWBALL -> PlayPackets.snowballEntityTypeId()
            ThrownItemKind.EGG, ThrownItemKind.BLUE_EGG, ThrownItemKind.BROWN_EGG -> PlayPackets.eggEntityTypeId()
            ThrownItemKind.ENDER_PEARL -> PlayPackets.enderPearlEntityTypeId()
        }
        enqueueDroppedItemPacket(
            outboundByContext,
            ctx,
            PlayPackets.addEntityPacket(
                entityId = snapshot.entityId,
                uuid = snapshot.uuid,
                entityTypeId = entityTypeId,
                x = snapshot.x,
                y = snapshot.y,
                z = snapshot.z,
                yaw = 0f,
                pitch = 0f,
                objectData = 0
            )
        )
        val particleItemId = thrownParticleItemId(snapshot.kind)
        if (particleItemId >= 0) {
            enqueueDroppedItemPacket(
                outboundByContext,
                ctx,
                PlayPackets.throwableItemMetadataPacket(
                    entityId = snapshot.entityId,
                    encodedItemStack = PlayPackets.encodeItemStack(particleItemId, 1)
                )
            )
        }
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
        session.thrownItemTrackerStates[snapshot.entityId] = newDroppedItemTrackerState(
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
                onGround = false,
                chunkPos = snapshot.chunkPos
            ),
            syncVx = syncVx,
            syncVy = syncVy,
            syncVz = syncVz
        )
    }

    private fun thrownItemMovementPackets(
        session: PlayerSession,
        snapshot: ThrownItemSnapshot,
        syncVx: Double,
        syncVy: Double,
        syncVz: Double,
        deltaSeconds: Double
    ): List<ByteArray> {
        val state = session.thrownItemTrackerStates.computeIfAbsent(snapshot.entityId) {
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
                    onGround = false,
                    chunkPos = snapshot.chunkPos
                ),
                syncVx = syncVx,
                syncVy = syncVy,
                syncVz = syncVz
            )
        }
        state.secondsSinceHardSync = (state.secondsSinceHardSync + deltaSeconds).coerceAtMost(120.0)
        val encodedX = encodeDroppedItemPosition4096(snapshot.x)
        val encodedY = encodeDroppedItemPosition4096(snapshot.y)
        val encodedZ = encodeDroppedItemPosition4096(snapshot.z)
        val dx = encodedX - state.encodedX4096
        val dy = encodedY - state.encodedY4096
        val dz = encodedZ - state.encodedZ4096
        val hasPositionDelta = dx != 0L || dy != 0L || dz != 0L

        val packets = ArrayList<ByteArray>(2)
        val shouldEmitMovement = hasPositionDelta
        var movementPacketSent = false

        if (shouldEmitMovement) {
            val requiresHardSync =
                dx < Short.MIN_VALUE.toLong() || dx > Short.MAX_VALUE.toLong() ||
                    dy < Short.MIN_VALUE.toLong() || dy > Short.MAX_VALUE.toLong() ||
                    dz < Short.MIN_VALUE.toLong() || dz > Short.MAX_VALUE.toLong() ||
                    state.secondsSinceHardSync >= MAX_DROPPED_ITEM_RELATIVE_SECONDS_BEFORE_HARD_SYNC
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
                        onGround = false
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
                        onGround = false
                    )
                )
            }
            state.encodedX4096 = encodedX
            state.encodedY4096 = encodedY
            state.encodedZ4096 = encodedZ
            movementPacketSent = true
        }

        val hadVelocity =
            abs(state.lastVx) > DROPPED_ITEM_VELOCITY_EPSILON ||
                abs(state.lastVy) > DROPPED_ITEM_VELOCITY_EPSILON ||
                abs(state.lastVz) > DROPPED_ITEM_VELOCITY_EPSILON
        if (movementPacketSent) {
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

    private fun thrownItemNetworkVelocity(snapshot: ThrownItemSnapshot): Triple<Double, Double, Double> {
        val effectiveTickScale = droppedItemNetworkVelocityTimeScale(ServerConfig.timeScale)
        val vx = sanitizeDroppedItemNetworkVelocity(snapshot.vx * effectiveTickScale)
        val vy = sanitizeDroppedItemNetworkVelocity(snapshot.vy * effectiveTickScale)
        val vz = sanitizeDroppedItemNetworkVelocity(snapshot.vz * effectiveTickScale)
        return Triple(vx, vy, vz)
    }

    private fun writeThrownItemSpawnPackets(session: PlayerSession, ctx: ChannelHandlerContext, snapshot: ThrownItemSnapshot) {
        val (syncVx, syncVy, syncVz) = thrownItemNetworkVelocity(snapshot)
        val entityTypeId = when (snapshot.kind) {
            ThrownItemKind.SNOWBALL -> PlayPackets.snowballEntityTypeId()
            ThrownItemKind.EGG, ThrownItemKind.BLUE_EGG, ThrownItemKind.BROWN_EGG -> PlayPackets.eggEntityTypeId()
            ThrownItemKind.ENDER_PEARL -> PlayPackets.enderPearlEntityTypeId()
        }
        ctx.write(
            PlayPackets.addEntityPacket(
                entityId = snapshot.entityId,
                uuid = snapshot.uuid,
                entityTypeId = entityTypeId,
                x = snapshot.x,
                y = snapshot.y,
                z = snapshot.z,
                yaw = 0f,
                pitch = 0f,
                objectData = 0
            )
        )
        val particleItemId = thrownParticleItemId(snapshot.kind)
        if (particleItemId >= 0) {
            ctx.write(
                PlayPackets.throwableItemMetadataPacket(
                    entityId = snapshot.entityId,
                    encodedItemStack = PlayPackets.encodeItemStack(particleItemId, 1)
                )
            )
        }
        ctx.write(PlayPackets.entityVelocityPacket(snapshot.entityId, syncVx, syncVy, syncVz))
        session.thrownItemTrackerStates[snapshot.entityId] = newDroppedItemTrackerState(
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
                onGround = false,
                chunkPos = snapshot.chunkPos
            ),
            syncVx = syncVx,
            syncVy = syncVy,
            syncVz = syncVz
        )
    }

    private fun thrownParticleItemId(kind: ThrownItemKind): Int {
        return when (kind) {
            ThrownItemKind.SNOWBALL -> snowballItemId
            ThrownItemKind.EGG -> eggItemId
            ThrownItemKind.BLUE_EGG -> blueEggItemId
            ThrownItemKind.BROWN_EGG -> brownEggItemId
            ThrownItemKind.ENDER_PEARL -> enderPearlItemId
        }
    }

    private fun sendThrownItemsForChunk(
        session: PlayerSession,
        ctx: ChannelHandlerContext,
        snapshots: List<ThrownItemSnapshot>
    ) {
        if (snapshots.isEmpty()) return
        for (snapshot in snapshots) {
            if (!session.visibleThrownItemEntityIds.add(snapshot.entityId)) continue
            writeThrownItemSpawnPackets(session, ctx, snapshot)
        }
    }

    private fun hideThrownItemsForChunk(
        session: PlayerSession,
        ctx: ChannelHandlerContext,
        chunkPos: ChunkPos,
        entityIds: IntArray
    ) {
        if (entityIds.isEmpty()) return
        val toRemove = ArrayList<Int>(entityIds.size)
        for (id in entityIds) {
            if (session.visibleThrownItemEntityIds.remove(id)) {
                session.thrownItemTrackerStates.remove(id)
                toRemove.add(id)
            }
        }
        if (toRemove.isNotEmpty()) {
            ctx.write(PlayPackets.removeEntitiesPacket(toRemove.toIntArray()))
        }
    }

    private fun syncAnimalSnapshot(
        snapshot: AnimalSnapshot,
        worldSessions: List<PlayerSession>,
        outboundByContext: MutableMap<ChannelHandlerContext, MutableList<ByteArray>>,
        sendPositionUpdate: Boolean,
        deltaSeconds: Double
    ) {
        syncMountedRiderPositionFromAnimalSnapshot(snapshot)
        val chunkPos = snapshot.chunkPos
        val riderEntityId = animalRiderEntityIdByAnimalEntityId[snapshot.entityId]
        var removePacket: ByteArray? = null
        val (syncVx, syncVy, syncVz) = animalNetworkVelocity(snapshot)
        for (session in worldSessions) {
            val isRider = riderEntityId != null && session.entityId == riderEntityId
            val shouldBeVisible = isRider || session.loadedChunks.contains(chunkPos)
            val ctx = contexts[session.channelId] ?: continue
            if (!ctx.channel().isActive) continue

            if (shouldBeVisible) {
                if (session.visibleAnimalEntityIds.add(snapshot.entityId)) {
                    queueAnimalSpawnPackets(
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
                    val movementPackets = animalMovementPackets(
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
                    enqueueDroppedItemPacket(
                        outboundByContext,
                        ctx,
                        PlayPackets.entityHeadLookPacket(snapshot.entityId, snapshot.headYaw)
                    )
                }
                continue
            }

            if (session.visibleAnimalEntityIds.remove(snapshot.entityId)) {
                session.animalTrackerStates.remove(snapshot.entityId)
                val packet = removePacket ?: PlayPackets.removeEntitiesPacket(intArrayOf(snapshot.entityId)).also {
                    removePacket = it
                }
                enqueueDroppedItemPacket(outboundByContext, ctx, packet)
            }
        }
    }

    private fun syncMountedRiderPositionFromAnimalSnapshot(snapshot: AnimalSnapshot) {
        val riderEntityId = animalRiderEntityIdByAnimalEntityId[snapshot.entityId] ?: return
        val rider = sessions.values.firstOrNull { it.entityId == riderEntityId } ?: return
        val newX = snapshot.x
        val newY = snapshot.y + (snapshot.hitboxHeight * PIG_RIDER_OFFSET_Y_FACTOR)
        val newZ = snapshot.z
        rider.x = newX
        rider.y = newY
        rider.z = newZ
        rider.onGround = snapshot.onGround
        rider.clientEyeX = newX
        rider.clientEyeY = newY + playerEyeOffset(rider)
        rider.clientEyeZ = newZ
        rider.encodedX4096 = encodeRelativePosition4096(newX)
        rider.encodedY4096 = encodeRelativePosition4096(newY)
        rider.encodedZ4096 = encodeRelativePosition4096(newZ)
        val currentChunkX = ChunkStreamingService.chunkXFromBlockX(newX)
        val currentChunkZ = ChunkStreamingService.chunkZFromBlockZ(newZ)
        if (currentChunkX != rider.centerChunkX || currentChunkZ != rider.centerChunkZ) {
            rider.centerChunkX = currentChunkX
            rider.centerChunkZ = currentChunkZ
            val ctx = contexts[rider.channelId]
            val world = WorldManager.world(rider.worldKey)
            if (ctx != null && world != null) {
                requestChunkStream(rider, ctx, world, currentChunkX, currentChunkZ, rider.chunkRadius)
            }
        }
    }

    private fun queueAnimalSpawnPackets(
        outboundByContext: MutableMap<ChannelHandlerContext, MutableList<ByteArray>>,
        ctx: ChannelHandlerContext,
        session: PlayerSession,
        snapshot: AnimalSnapshot,
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
                entityTypeId = animalEntityTypeId(snapshot.kind),
                x = snapshot.x,
                y = snapshot.y,
                z = snapshot.z,
                yaw = snapshot.yaw,
                pitch = snapshot.pitch,
                objectData = 0
            )
        )
        enqueueDroppedItemPacket(outboundByContext, ctx, PlayPackets.entityVelocityPacket(snapshot.entityId, syncVx, syncVy, syncVz))
        if (snapshot.entityId in saddledAnimalEntityIds && saddleItemId >= 0) {
            enqueueDroppedItemPacket(
                outboundByContext,
                ctx,
                PlayPackets.entityEquipmentPacket(
                    snapshot.entityId,
                    listOf(PlayPackets.EquipmentEntry(slot = 7, encodedItemStack = PlayPackets.encodeItemStack(saddleItemId, 1)))
                )
            )
        }
        val riderEntityId = animalRiderEntityIdByAnimalEntityId[snapshot.entityId]
        if (riderEntityId != null) {
            enqueueDroppedItemPacket(
                outboundByContext,
                ctx,
                PlayPackets.setPassengersPacket(snapshot.entityId, intArrayOf(riderEntityId))
            )
        }
        session.animalTrackerStates[snapshot.entityId] = newDroppedItemTrackerState(
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
        ).also { state ->
            state.lastYaw = snapshot.yaw
            state.lastPitch = snapshot.pitch
            state.lastHeadYaw = snapshot.headYaw
        }
    }

    private fun animalMovementPackets(
        session: PlayerSession,
        snapshot: AnimalSnapshot,
        syncVx: Double,
        syncVy: Double,
        syncVz: Double,
        deltaSeconds: Double
    ): List<ByteArray> {
        val state = session.animalTrackerStates.computeIfAbsent(snapshot.entityId) {
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
        val rotationChanged =
            abs(wrapDegrees(snapshot.yaw - state.lastYaw)) > ANIMAL_ROTATION_EPSILON_DEGREES ||
                abs(wrapDegrees(snapshot.pitch - state.lastPitch)) > ANIMAL_ROTATION_EPSILON_DEGREES
        state.secondsSinceHardSync = (state.secondsSinceHardSync + deltaSeconds).coerceAtMost(120.0)
        val encodedX = encodeDroppedItemPosition4096(snapshot.x)
        val encodedY = encodeDroppedItemPosition4096(snapshot.y)
        val encodedZ = encodeDroppedItemPosition4096(snapshot.z)
        val dx = encodedX - state.encodedX4096
        val dy = encodedY - state.encodedY4096
        val dz = encodedZ - state.encodedZ4096
        val hasPositionDelta = dx != 0L || dy != 0L || dz != 0L
        val onGroundChanged = state.lastOnGround != snapshot.onGround

        val packets = ArrayList<ByteArray>(2)
        var movementPacketSent = false
        if (hasPositionDelta || onGroundChanged || rotationChanged) {
            val requiresHardSync =
                dx < Short.MIN_VALUE.toLong() || dx > Short.MAX_VALUE.toLong() ||
                    dy < Short.MIN_VALUE.toLong() || dy > Short.MAX_VALUE.toLong() ||
                    dz < Short.MIN_VALUE.toLong() || dz > Short.MAX_VALUE.toLong() ||
                    state.secondsSinceHardSync >= MAX_DROPPED_ITEM_RELATIVE_SECONDS_BEFORE_HARD_SYNC
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
                        yaw = snapshot.yaw,
                        pitch = snapshot.pitch,
                        onGround = snapshot.onGround
                    )
                )
                state.secondsSinceHardSync = 0.0
            } else {
                packets.add(
                    PlayPackets.entityRelativeMoveLookPacket(
                        entityId = snapshot.entityId,
                        deltaX = dx.toInt(),
                        deltaY = dy.toInt(),
                        deltaZ = dz.toInt(),
                        yaw = snapshot.yaw,
                        pitch = snapshot.pitch,
                        onGround = snapshot.onGround
                    )
                )
            }
            state.encodedX4096 = encodedX
            state.encodedY4096 = encodedY
            state.encodedZ4096 = encodedZ
            state.lastOnGround = snapshot.onGround
            state.lastYaw = snapshot.yaw
            state.lastPitch = snapshot.pitch
            movementPacketSent = true
        }
        state.lastHeadYaw = snapshot.headYaw

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

    private fun animalNetworkVelocity(snapshot: AnimalSnapshot): Triple<Double, Double, Double> {
        val effectiveTickScale = droppedItemNetworkVelocityTimeScale(ServerConfig.timeScale)
        val vx = sanitizeDroppedItemNetworkVelocity(snapshot.vx * effectiveTickScale)
        val vy = if (snapshot.onGround) 0.0 else sanitizeDroppedItemNetworkVelocity(snapshot.vy * effectiveTickScale)
        val vz = sanitizeDroppedItemNetworkVelocity(snapshot.vz * effectiveTickScale)
        return Triple(vx, vy, vz)
    }

    private fun wrapDegrees(value: Float): Float {
        var out = value % 360f
        if (out >= 180f) out -= 360f
        if (out < -180f) out += 360f
        return out
    }

    private fun writeAnimalSpawnPackets(session: PlayerSession, ctx: ChannelHandlerContext, snapshot: AnimalSnapshot) {
        val (syncVx, syncVy, syncVz) = animalNetworkVelocity(snapshot)
        ctx.write(
            PlayPackets.addEntityPacket(
                entityId = snapshot.entityId,
                uuid = snapshot.uuid,
                entityTypeId = animalEntityTypeId(snapshot.kind),
                x = snapshot.x,
                y = snapshot.y,
                z = snapshot.z,
                yaw = snapshot.yaw,
                pitch = snapshot.pitch,
                objectData = 0
            )
        )
        ctx.write(PlayPackets.entityVelocityPacket(snapshot.entityId, syncVx, syncVy, syncVz))
        if (snapshot.entityId in saddledAnimalEntityIds && saddleItemId >= 0) {
            ctx.write(
                PlayPackets.entityEquipmentPacket(
                    snapshot.entityId,
                    listOf(PlayPackets.EquipmentEntry(slot = 7, encodedItemStack = PlayPackets.encodeItemStack(saddleItemId, 1)))
                )
            )
        }
        val riderEntityId = animalRiderEntityIdByAnimalEntityId[snapshot.entityId]
        if (riderEntityId != null) {
            ctx.write(PlayPackets.setPassengersPacket(snapshot.entityId, intArrayOf(riderEntityId)))
        }
        session.animalTrackerStates[snapshot.entityId] = newDroppedItemTrackerState(
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
        ).also { state ->
            state.lastYaw = snapshot.yaw
            state.lastPitch = snapshot.pitch
            state.lastHeadYaw = snapshot.headYaw
        }
    }

    private fun sendAnimalsForChunk(
        session: PlayerSession,
        ctx: ChannelHandlerContext,
        snapshots: List<AnimalSnapshot>
    ) {
        if (snapshots.isEmpty()) return
        for (snapshot in snapshots) {
            if (!session.visibleAnimalEntityIds.add(snapshot.entityId)) continue
            writeAnimalSpawnPackets(session, ctx, snapshot)
        }
    }

    private fun hideAnimalsForChunk(
        session: PlayerSession,
        ctx: ChannelHandlerContext,
        chunkPos: ChunkPos,
        entityIds: IntArray
    ) {
        if (entityIds.isEmpty()) return
        val toRemove = ArrayList<Int>(entityIds.size)
        for (id in entityIds) {
            if (session.visibleAnimalEntityIds.remove(id)) {
                session.animalTrackerStates.remove(id)
                toRemove.add(id)
            }
        }
        if (toRemove.isNotEmpty()) {
            ctx.write(PlayPackets.removeEntitiesPacket(toRemove.toIntArray()))
        }
    }

    private fun animalEntityTypeId(kind: AnimalKind): Int {
        return when (kind) {
            AnimalKind.PIG -> PlayPackets.pigEntityTypeId()
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

        val horizontalPlacement = oppositeHorizontalFacing(yawToHorizontalFacing(playerYaw))
        if (props.containsKey("horizontal_facing")) {
            props["horizontal_facing"] = horizontalPlacement
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
            val horizontal = horizontalPlacement
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

    private fun oppositeHorizontalFacing(facing: String): String {
        return when (facing) {
            "north" -> "south"
            "south" -> "north"
            "west" -> "east"
            "east" -> "west"
            else -> facing
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
        val breakDecision = org.macaroon3145.plugin.PluginSystem.beforeBlockBreak(session, x, y, z)
        if (!breakDecision.proceed) {
            return
        }
        if (breakDecision.modifiedByPlugin) {
            return
        }
        val stateAfterEvent = world.blockStateAt(x, y, z)
        if (stateAfterEvent != stateId) {
            return
        }
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
            setBlockAndBroadcast(session, pos.x, pos.y, pos.z, 0, reason = BlockChangeReason.PLAYER_BREAK)
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
        reason: BlockChangeReason = BlockChangeReason.SYSTEM,
        blockEntityTypeId: Int = -1,
        blockEntityNbtPayload: ByteArray? = null,
        logAsFluidSync: Boolean = false
    ) {
        if (y !in -64..319) return
        val world = WorldManager.world(session.worldKey) ?: return
        val oldStateId = world.blockStateAt(x, y, z)
        if (!org.macaroon3145.plugin.PluginSystem.beforeBlockChange(
                session = session,
                worldKey = session.worldKey,
                x = x,
                y = y,
                z = z,
                previousStateId = oldStateId,
                changedStateId = stateId,
                reason = reason
            )
        ) {
            resyncBlockToPlayer(session, x, y, z, oldStateId)
            return
        }
        val furnaceInventoryDrops = maybeDropAndClearFurnaceInventory(world, x, y, z, oldStateId, stateId)
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
        if (furnaceInventoryDrops.isNotEmpty()) {
            // Spawn after block state change so dropped items don't get pushed upward out of the still-solid furnace.
            spawnBrokenBlockDropsInWorld(world, x, y, z, furnaceInventoryDrops)
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
        var recipients = 0
        for ((id, other) in sessions) {
            if (other.worldKey != session.worldKey) continue
            if (!other.loadedChunks.contains(chunk)) continue
            val ctx = contexts[id] ?: continue
            if (!ctx.channel().isActive) continue
            recipients++
            ctx.executor().execute {
                if (!ctx.channel().isActive) return@execute
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
        triggerFallingBlocksNear(session, world, x, y, z)
    }

    fun pluginSetBlockStateAndBroadcast(
        worldKey: String,
        x: Int,
        y: Int,
        z: Int,
        stateId: Int,
        reason: BlockChangeReason = BlockChangeReason.PLUGIN
    ): Boolean {
        if (y !in -64..319) return false
        val world = WorldManager.world(worldKey) ?: return false
        val oldStateId = world.blockStateAt(x, y, z)
        if (!org.macaroon3145.plugin.PluginSystem.beforeBlockChange(
                session = null,
                worldKey = worldKey,
                x = x,
                y = y,
                z = z,
                previousStateId = oldStateId,
                changedStateId = stateId,
                reason = reason
            )
        ) {
            return false
        }
        val furnaceInventoryDrops = maybeDropAndClearFurnaceInventory(world, x, y, z, oldStateId, stateId)
        world.setBlockState(x, y, z, stateId)
        if (stateId != 0 && stateId != oldStateId) {
            world.requestDroppedItemUnstuckAtBlock(x, y, z)
        }
        // Plugin-driven setType/setState does not carry block entity payload,
        // so clear existing block entity data to keep state consistent.
        world.setBlockEntity(x, y, z, null)
        if (furnaceInventoryDrops.isNotEmpty()) {
            spawnBrokenBlockDropsInWorld(world, x, y, z, furnaceInventoryDrops)
        }
        val chunk = ChunkPos(x shr 4, z shr 4)
        val packet = PlayPackets.blockChangePacket(x, y, z, stateId)
        for ((id, other) in sessions) {
            if (other.worldKey != worldKey) continue
            if (!other.loadedChunks.contains(chunk)) continue
            val ctx = contexts[id] ?: continue
            if (!ctx.channel().isActive) continue
            ctx.executor().execute {
                if (!ctx.channel().isActive) return@execute
                ctx.write(packet)
                ctx.flush()
            }
        }
        return true
    }

    private fun maybeDropAndClearFurnaceInventory(
        world: org.macaroon3145.world.World,
        x: Int,
        y: Int,
        z: Int,
        oldStateId: Int,
        newStateId: Int
    ): List<org.macaroon3145.world.VanillaDrop> {
        if (oldStateId == newStateId) return emptyList()
        if (!isFurnaceState(oldStateId)) return emptyList()
        if (isFurnaceState(newStateId)) return emptyList()

        val key = FurnaceKey(world.key, x, y, z)
        val removed = furnaceStates.remove(key) ?: return emptyList()
        activeFurnaceKeys.remove(key)
        activeFurnaceKeysByWorld[key.worldKey]?.let { set ->
            set.remove(key)
            if (set.isEmpty()) {
                activeFurnaceKeysByWorld.remove(key.worldKey, set)
            }
        }
        val drops = run {
            ArrayList<org.macaroon3145.world.VanillaDrop>(3).apply {
                if (removed.inputItemId >= 0 && removed.inputCount > 0) {
                    add(org.macaroon3145.world.VanillaDrop(removed.inputItemId, removed.inputCount))
                }
                if (removed.fuelItemId >= 0 && removed.fuelCount > 0) {
                    add(org.macaroon3145.world.VanillaDrop(removed.fuelItemId, removed.fuelCount))
                }
                if (removed.resultItemId >= 0 && removed.resultCount > 0) {
                    add(org.macaroon3145.world.VanillaDrop(removed.resultItemId, removed.resultCount))
                }
            }
        }

        for (viewer in sessions.values) {
            if (viewer.worldKey != world.key) continue
            if (viewer.openContainerType != CONTAINER_TYPE_FURNACE) continue
            if (viewer.openFurnaceX != x || viewer.openFurnaceY != y || viewer.openFurnaceZ != z) continue
            closeFurnace(viewer)
        }
        return drops
    }

    private fun triggerFallingBlocksNear(session: PlayerSession, world: org.macaroon3145.world.World, x: Int, y: Int, z: Int) {
        triggerFallingBlockColumnFrom(session, world, x, y, z)
        if (y + 1 <= 319) {
            triggerFallingBlockColumnFrom(session, world, x, y + 1, z)
        }
    }

    private fun broadcastBlockChangeToLoadedPlayers(
        worldKey: String,
        chunkPos: ChunkPos,
        x: Int,
        y: Int,
        z: Int,
        stateId: Int
    ) {
        val packet = PlayPackets.blockChangePacket(x, y, z, stateId)
        var recipients = 0
        for ((id, other) in sessions) {
            if (other.worldKey != worldKey) continue
            if (!other.loadedChunks.contains(chunkPos)) continue
            val ctx = contexts[id] ?: continue
            if (!ctx.channel().isActive) continue
            recipients++
            ctx.executor().execute {
                if (!ctx.channel().isActive) return@execute
                ctx.write(packet)
                ctx.flush()
            }
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
        if (!org.macaroon3145.plugin.PluginSystem.beforeBlockChange(
                session = session,
                worldKey = world.key,
                x = x,
                y = y,
                z = z,
                previousStateId = stateId,
                changedStateId = 0,
                reason = BlockChangeReason.FALLING_BLOCK_START
            )
        ) {
            return false
        }
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
            ctx.executor().execute {
                if (!ctx.channel().isActive) return@execute
                if (!other.loadedChunks.contains(chunk)) return@execute
                ctx.write(clearPacket)
                if (other.visibleFallingBlockEntityIds.add(entityId)) {
                    writeFallingBlockSpawnPackets(other, ctx, snapshot)
                }
                ctx.flush()
            }
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
        if (clamped != GAME_MODE_CREATIVE) {
            session.flying = false
            session.fallDistance = 0.0
        }

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
            if (clamped != GAME_MODE_CREATIVE) {
                session.flying = false
                session.fallDistance = 0.0
            }
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
        spawnBrokenBlockDropsInWorld(world, x, y, z, drops)
    }

    private fun spawnBrokenBlockDropsInWorld(
        world: org.macaroon3145.world.World,
        x: Int,
        y: Int,
        z: Int,
        drops: List<org.macaroon3145.world.VanillaDrop>
    ) {
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

    private fun maybeSpawnFluidBrokenBlockDrops(world: org.macaroon3145.world.World, changed: org.macaroon3145.world.FluidBlockChange) {
        val previousStateId = changed.previousStateId
        if (previousStateId <= 0) return
        val previousState = BlockStateRegistry.parsedState(previousStateId) ?: return
        if (previousState.blockKey == "minecraft:water") return
        val nextState = BlockStateRegistry.parsedState(changed.stateId) ?: return
        if (nextState.blockKey != "minecraft:water") return

        val drops = runCatching {
            VanillaMiningRules.resolveDrops(world, previousStateId, -1, changed.x, changed.y, changed.z)
        }.onFailure { throwable ->
            logger.error(
                "Failed to resolve fluid-broken block drops for block at {} {} {} in {} (stateId={})",
                changed.x,
                changed.y,
                changed.z,
                world.key,
                previousStateId,
                throwable
            )
        }.getOrDefault(emptyList())
        if (drops.isEmpty()) return
        spawnBrokenBlockDropsInWorld(world, changed.x, changed.y, changed.z, drops)
    }

    private fun classifyFluidChangeReason(previousStateId: Int, nextStateId: Int): BlockChangeReason {
        val prevLevel = waterLevelForReason(previousStateId)
        val nextLevel = waterLevelForReason(nextStateId)
        return when {
            prevLevel == null && nextLevel != null -> BlockChangeReason.WATER_SPREAD
            prevLevel != null && nextLevel == null -> BlockChangeReason.WATER_RETRACT
            prevLevel != null && nextLevel != null && prevLevel != nextLevel -> BlockChangeReason.WATER_LEVEL_CHANGE
            else -> BlockChangeReason.SYSTEM
        }
    }

    private fun waterLevelForReason(stateId: Int): Int? {
        if (stateId <= 0) return null
        val parsed = BlockStateRegistry.parsedState(stateId) ?: return null
        if (parsed.blockKey != "minecraft:water") return null
        return parsed.properties["level"]?.toIntOrNull() ?: 0
    }

    private fun resolveFluidDependentDropsCached(
        world: org.macaroon3145.world.World,
        stateId: Int,
        x: Int,
        y: Int,
        z: Int
    ): List<org.macaroon3145.world.VanillaDrop> {
        fluidDependentDropCache[stateId]?.let { return it }
        val resolved = runCatching {
            VanillaMiningRules.resolveDrops(world, stateId, -1, x, y, z)
        }.onFailure { throwable ->
            logger.error(
                "Failed to resolve fluid-dependent block drops for block at {} {} {} in {} (stateId={})",
                x,
                y,
                z,
                world.key,
                stateId,
                throwable
            )
        }.getOrDefault(emptyList())
        fluidDependentDropCache.putIfAbsent(stateId, resolved)
        return resolved
    }

    private data class FluidDependentRemoval(
        val x: Int,
        val y: Int,
        val z: Int,
        val previousStateId: Int,
        val chunkPos: ChunkPos,
        val drops: List<org.macaroon3145.world.VanillaDrop>
    )

    private data class FluidChunkDependentResult(
        val removals: List<FluidDependentRemoval>,
        val spillSeeds: List<BlockPos>
    )

    private fun breakDependentBlocksForFluidByChunk(
        world: org.macaroon3145.world.World,
        removedCenters: Collection<BlockPos>
    ): List<org.macaroon3145.world.FluidBlockChange> {
        // Kept for compatibility/prewarm paths. Runtime tick path uses scheduleFluidDependentBreakRounds.
        if (removedCenters.isEmpty()) return emptyList()
        val changedOut = ArrayList<org.macaroon3145.world.FluidBlockChange>()
        val seenSeeds = HashSet<Long>(removedCenters.size * 2)
        val pendingByChunk = LinkedHashMap<ChunkPos, MutableSet<BlockPos>>()
        for (seed in removedCenters) {
            if (!seenSeeds.add(packBlockPos(seed.x, seed.y, seed.z))) continue
            pendingByChunk.computeIfAbsent(seed.chunkPos()) { LinkedHashSet() }.add(seed)
        }

        var rounds = 0
        while (pendingByChunk.isNotEmpty() && rounds < 8) {
            rounds++
            val currentRound = ArrayList<Map.Entry<ChunkPos, MutableSet<BlockPos>>>(pendingByChunk.entries)
            pendingByChunk.clear()
            for ((chunkPos, seeds) in currentRound) {
                if (seeds.isEmpty()) continue
                val result = breakDependentBlocksForFluidChunk(
                    world = world,
                    ownerChunk = chunkPos,
                    removedCenters = seeds
                )
                for (removal in result.removals) {
                    if (!org.macaroon3145.plugin.PluginSystem.beforeBlockChange(
                            session = null,
                            worldKey = world.key,
                            x = removal.x,
                            y = removal.y,
                            z = removal.z,
                            previousStateId = removal.previousStateId,
                            changedStateId = 0,
                            reason = BlockChangeReason.FLUID_SUPPORT_BREAK
                        )
                    ) {
                        continue
                    }
                    world.setBlockStateWithoutFluidUpdates(removal.x, removal.y, removal.z, 0)
                    world.setBlockEntity(removal.x, removal.y, removal.z, null)
                    changedOut.add(
                        org.macaroon3145.world.FluidBlockChange(
                            x = removal.x,
                            y = removal.y,
                            z = removal.z,
                            previousStateId = removal.previousStateId,
                            stateId = 0,
                            chunkPos = removal.chunkPos
                        )
                    )
                    if (removal.drops.isNotEmpty()) {
                        spawnBrokenBlockDropsInWorld(world, removal.x, removal.y, removal.z, removal.drops)
                    }
                }
                for (spill in result.spillSeeds) {
                    if (!seenSeeds.add(packBlockPos(spill.x, spill.y, spill.z))) continue
                    pendingByChunk.computeIfAbsent(spill.chunkPos()) { LinkedHashSet() }.add(spill)
                }
            }
        }
        return changedOut
    }

    private fun scheduleFluidDependentBreakRounds(
        world: org.macaroon3145.world.World,
        removedCenters: Collection<BlockPos>,
        onComplete: () -> Unit
    ) {
        if (removedCenters.isEmpty()) {
            onComplete()
            return
        }
        val seenSeeds = ConcurrentHashMap.newKeySet<Long>()
        val changedOut = ConcurrentLinkedQueue<org.macaroon3145.world.FluidBlockChange>()
        val initialByChunk = LinkedHashMap<ChunkPos, MutableSet<BlockPos>>()
        for (seed in removedCenters) {
            if (!seenSeeds.add(packBlockPos(seed.x, seed.y, seed.z))) continue
            initialByChunk.computeIfAbsent(seed.chunkPos()) { LinkedHashSet() }.add(seed)
        }

        fun runRound(round: Int, byChunk: Map<ChunkPos, Set<BlockPos>>) {
            if (byChunk.isEmpty() || round >= 8) {
                if (changedOut.isNotEmpty()) {
                    broadcastAppliedFluidChanges(world.key, ArrayList(changedOut))
                }
                onComplete()
                return
            }

            val nextByChunk = ConcurrentHashMap<ChunkPos, MutableSet<BlockPos>>()
            val remaining = AtomicInteger(byChunk.size)
            val completed = AtomicBoolean(false)

            fun completeRoundIfDone() {
                if (remaining.decrementAndGet() != 0) return
                if (!completed.compareAndSet(false, true)) return
                val nextSnapshot = LinkedHashMap<ChunkPos, Set<BlockPos>>(nextByChunk.size)
                for ((chunkPos, seeds) in nextByChunk) {
                    if (seeds.isEmpty()) continue
                    nextSnapshot[chunkPos] = seeds.toSet()
                }
                runRound(round + 1, nextSnapshot)
            }

            for ((chunkPos, seeds) in byChunk) {
                world.submitOnChunkActor(chunkPos) {
                    try {
                        val result = breakDependentBlocksForFluidChunk(
                            world = world,
                            ownerChunk = chunkPos,
                            removedCenters = seeds
                        )
                        for (removal in result.removals) {
                            if (!org.macaroon3145.plugin.PluginSystem.beforeBlockChange(
                                    session = null,
                                    worldKey = world.key,
                                    x = removal.x,
                                    y = removal.y,
                                    z = removal.z,
                                    previousStateId = removal.previousStateId,
                                    changedStateId = 0,
                                    reason = BlockChangeReason.FLUID_SUPPORT_BREAK
                                )
                            ) {
                                continue
                            }
                            world.setBlockStateWithoutFluidUpdates(removal.x, removal.y, removal.z, 0)
                            world.setBlockEntity(removal.x, removal.y, removal.z, null)
                            changedOut.add(
                                org.macaroon3145.world.FluidBlockChange(
                                    x = removal.x,
                                    y = removal.y,
                                    z = removal.z,
                                    previousStateId = removal.previousStateId,
                                    stateId = 0,
                                    chunkPos = removal.chunkPos
                                )
                            )
                            if (removal.drops.isNotEmpty()) {
                                spawnBrokenBlockDropsInWorld(world, removal.x, removal.y, removal.z, removal.drops)
                            }
                        }
                        for (spill in result.spillSeeds) {
                            if (!seenSeeds.add(packBlockPos(spill.x, spill.y, spill.z))) continue
                            nextByChunk
                                .computeIfAbsent(spill.chunkPos()) { ConcurrentHashMap.newKeySet() }
                                .add(spill)
                        }
                    } finally {
                        completeRoundIfDone()
                    }
                }
            }
        }

        val firstRound = LinkedHashMap<ChunkPos, Set<BlockPos>>(initialByChunk.size)
        for ((chunkPos, seeds) in initialByChunk) {
            if (seeds.isEmpty()) continue
            firstRound[chunkPos] = seeds.toSet()
        }
        runRound(0, firstRound)
    }

    private fun breakDependentBlocksForFluidChunk(
        world: org.macaroon3145.world.World,
        ownerChunk: ChunkPos,
        removedCenters: Collection<BlockPos>
    ): FluidChunkDependentResult {
        val pending = ArrayDeque<BlockPos>()
        val enqueued = HashSet<Long>()
        val dependents = ArrayList<BlockPos>(2)
        val removals = ArrayList<FluidDependentRemoval>()
        val spillSeedsByKey = LinkedHashMap<Long, BlockPos>()

        for (removedCenter in removedCenters) {
            enqueueDependentChecksForChunk(removedCenter, ownerChunk, pending, enqueued, spillSeedsByKey)
        }

        while (pending.isNotEmpty()) {
            val pos = pending.removeFirst()
            if (pos.chunkPos() != ownerChunk) {
                spillSeedsByKey.putIfAbsent(packBlockPos(pos.x, pos.y, pos.z), pos)
                continue
            }
            val stateId = world.blockStateAt(pos.x, pos.y, pos.z)
            if (stateId == 0) continue
            dependents.clear()
            appendDependentBlocksToBreak(world, pos.x, pos.y, pos.z, stateId, dependents)
            if (dependents.isEmpty()) continue

            for (dependent in dependents) {
                if (dependent.chunkPos() != ownerChunk) {
                    spillSeedsByKey.putIfAbsent(packBlockPos(dependent.x, dependent.y, dependent.z), dependent)
                    continue
                }
                val dependentStateId = world.blockStateAt(dependent.x, dependent.y, dependent.z)
                if (dependentStateId == 0) continue
                val drops = resolveFluidDependentDropsCached(
                    world = world,
                    stateId = dependentStateId,
                    x = dependent.x,
                    y = dependent.y,
                    z = dependent.z
                )
                removals.add(
                    FluidDependentRemoval(
                        x = dependent.x,
                        y = dependent.y,
                        z = dependent.z,
                        previousStateId = dependentStateId,
                        chunkPos = dependent.chunkPos(),
                        drops = drops
                    )
                )
                enqueueDependentChecksForChunk(dependent, ownerChunk, pending, enqueued, spillSeedsByKey)
            }
        }

        return FluidChunkDependentResult(
            removals = removals,
            spillSeeds = ArrayList(spillSeedsByKey.values)
        )
    }

    private fun enqueueDependentChecksForChunk(
        center: BlockPos,
        ownerChunk: ChunkPos,
        pending: ArrayDeque<BlockPos>,
        enqueued: MutableSet<Long>,
        spillSeeds: MutableMap<Long, BlockPos>
    ) {
        enqueueDependentCheckForChunk(center.x, center.y + 1, center.z, ownerChunk, pending, enqueued, spillSeeds)
        enqueueDependentCheckForChunk(center.x, center.y - 1, center.z, ownerChunk, pending, enqueued, spillSeeds)
        enqueueDependentCheckForChunk(center.x + 1, center.y, center.z, ownerChunk, pending, enqueued, spillSeeds)
        enqueueDependentCheckForChunk(center.x - 1, center.y, center.z, ownerChunk, pending, enqueued, spillSeeds)
        enqueueDependentCheckForChunk(center.x, center.y, center.z + 1, ownerChunk, pending, enqueued, spillSeeds)
        enqueueDependentCheckForChunk(center.x, center.y, center.z - 1, ownerChunk, pending, enqueued, spillSeeds)
    }

    private fun enqueueDependentCheckForChunk(
        x: Int,
        y: Int,
        z: Int,
        ownerChunk: ChunkPos,
        pending: ArrayDeque<BlockPos>,
        enqueued: MutableSet<Long>,
        spillSeeds: MutableMap<Long, BlockPos>
    ) {
        if (y !in -64..319) return
        val packed = packBlockPos(x, y, z)
        if (!enqueued.add(packed)) return
        val candidate = BlockPos(x, y, z)
        if (candidate.chunkPos() == ownerChunk) {
            pending.addLast(candidate)
        } else {
            spillSeeds.putIfAbsent(packed, candidate)
        }
    }

    private fun packBlockPos(x: Int, y: Int, z: Int): Long {
        val px = x.toLong() and 0x3FFFFFFL
        val py = (y - (-64)).toLong() and 0x3FFL
        val pz = z.toLong() and 0x3FFFFFFL
        return (px shl 38) or (pz shl 12) or py
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
                setBlockAndBroadcast(session, dependent.x, dependent.y, dependent.z, 0, reason = BlockChangeReason.PLAYER_BREAK_CHAIN)
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
        setBlockAndBroadcast(session, pos.x, pos.y, pos.z, 0, reason = BlockChangeReason.PLAYER_BREAK_CHAIN)
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

    private fun resolveHotbarTextMeta(session: PlayerSession, slot: Int, currentItemId: Int): ItemTextMeta? {
        val playerMeta = itemTextMetaByPlayerUuid[session.profile.uuid] ?: return null
        val meta = playerMeta.hotbar.getOrNull(slot) ?: return null
        if (meta.expectedItemId != currentItemId) {
            playerMeta.hotbar[slot] = null
            return null
        }
        return meta
    }

    private fun resolveMainTextMeta(session: PlayerSession, index: Int, currentItemId: Int): ItemTextMeta? {
        val playerMeta = itemTextMetaByPlayerUuid[session.profile.uuid] ?: return null
        val meta = playerMeta.main.getOrNull(index) ?: return null
        if (meta.expectedItemId != currentItemId) {
            playerMeta.main[index] = null
            return null
        }
        return meta
    }

    private fun resolveArmorTextMeta(session: PlayerSession, armorIndex: Int, currentItemId: Int): ItemTextMeta? {
        val playerMeta = itemTextMetaByPlayerUuid[session.profile.uuid] ?: return null
        val meta = playerMeta.armor.getOrNull(armorIndex) ?: return null
        if (meta.expectedItemId != currentItemId) {
            playerMeta.armor[armorIndex] = null
            return null
        }
        return meta
    }

    private fun resolveOffhandTextMeta(session: PlayerSession, currentItemId: Int): ItemTextMeta? {
        val playerMeta = itemTextMetaByPlayerUuid[session.profile.uuid] ?: return null
        val meta = playerMeta.offhand ?: return null
        if (meta.expectedItemId != currentItemId) {
            playerMeta.offhand = null
            return null
        }
        return meta
    }

    private fun encodedItemStack(itemId: Int, count: Int = 1, textMeta: ItemTextMeta? = null): ByteArray {
        val out = ByteArrayOutputStream(8)
        // 1.20.5+ Slot shape:
        // [count VarInt][itemId VarInt][added_components VarInt][components...][removed_components VarInt]
        NetworkUtils.writeVarInt(out, count.coerceAtLeast(1))
        NetworkUtils.writeVarInt(out, itemId)
        val meta = textMeta
        val name = meta?.name
        val lore = meta?.lore ?: emptyList()
        val hasName = name != null
        val hasLore = lore.isNotEmpty()
        val added = (if (hasName) 1 else 0) + (if (hasLore) 1 else 0)
        NetworkUtils.writeVarInt(out, added)
        NetworkUtils.writeVarInt(out, 0) // removed component count
        if (name != null) {
            NetworkUtils.writeVarInt(out, ITEM_COMPONENT_CUSTOM_NAME_ID)
            out.write(styledTextComponentPayload(name))
        }
        if (hasLore) {
            NetworkUtils.writeVarInt(out, ITEM_COMPONENT_LORE_ID)
            out.write(componentListPayload(lore))
        }
        return out.toByteArray()
    }

    private fun styledTextComponentPayload(text: String): ByteArray {
        return componentPayload(parsePluginStyledMessage(text))
    }

    private fun componentPayload(component: PlayPackets.ChatComponent): ByteArray {
        val out = ByteArrayOutputStream()
        val data = DataOutputStream(out)
        data.writeByte(10) // TAG_Compound
        writeInnerComponentTag(data, component)
        data.flush()
        return out.toByteArray()
    }

    private fun componentListPayload(lines: List<String>): ByteArray {
        val out = ByteArrayOutputStream()
        NetworkUtils.writeVarInt(out, lines.size)
        for (line in lines) {
            out.write(styledTextComponentPayload(line))
        }
        return out.toByteArray()
    }

    private fun writeInnerComponentTag(out: DataOutputStream, component: PlayPackets.ChatComponent) {
        when (component) {
            is PlayPackets.ChatComponent.Text -> {
                writeTagString(out, "type", "text")
                writeTagString(out, "text", component.text)
                if (component.color != null) writeTagString(out, "color", component.color)
                if (component.bold != null) writeTagBoolean(out, "bold", component.bold)
                if (component.italic != null) writeTagBoolean(out, "italic", component.italic)
                if (component.underlined != null) writeTagBoolean(out, "underlined", component.underlined)
                if (component.extra.isNotEmpty()) writeComponentListTag(out, "extra", component.extra)
            }
            is PlayPackets.ChatComponent.Translate -> {
                writeTagString(out, "type", "translatable")
                writeTagString(out, "translate", component.key)
                if (component.color != null) writeTagString(out, "color", component.color)
                if (component.bold != null) writeTagBoolean(out, "bold", component.bold)
                if (component.italic != null) writeTagBoolean(out, "italic", component.italic)
                if (component.underlined != null) writeTagBoolean(out, "underlined", component.underlined)
                if (component.args.isNotEmpty()) writeComponentListTag(out, "with", component.args)
                if (component.extra.isNotEmpty()) writeComponentListTag(out, "extra", component.extra)
            }
        }
        out.writeByte(0) // TAG_End
    }

    private fun writeComponentListTag(out: DataOutputStream, name: String, components: List<PlayPackets.ChatComponent>) {
        out.writeByte(9) // TAG_List
        out.writeUTF(name)
        out.writeByte(10) // TAG_Compound
        out.writeInt(components.size)
        for (child in components) {
            writeInnerComponentTag(out, child)
        }
    }

    private fun writeTagString(out: DataOutputStream, name: String, value: String) {
        out.writeByte(8) // TAG_String
        out.writeUTF(name)
        out.writeUTF(value)
    }

    private fun writeTagBoolean(out: DataOutputStream, name: String, value: Boolean) {
        out.writeByte(1) // TAG_Byte
        out.writeUTF(name)
        out.writeByte(if (value) 1 else 0)
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
        val playerAabb = playerAabb(session)
        val blockAabb = Aabb(
            minX = blockX.toDouble(),
            minY = blockY.toDouble(),
            minZ = blockZ.toDouble(),
            maxX = blockX + 1.0,
            maxY = blockY + 1.0,
            maxZ = blockZ + 1.0
        )
        return aabbOverlap(playerAabb, blockAabb)
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

    private fun readTypedIntField(root: kotlinx.serialization.json.JsonObject, key: String): Int? {
        val typed = root[key]?.jsonObject ?: return null
        return typed["value"]?.jsonPrimitive?.intOrNull
    }

    private fun effectiveChunkRadius(requestedViewDistance: Int): Int {
        val requested = requestedViewDistance.coerceAtLeast(0)
        return requested.coerceAtMost(ServerConfig.maxViewDistanceChunks.coerceAtLeast(2))
    }

    private const val MAX_DROPPED_ITEM_CATCH_UP_TICKS = 5
    private const val MAX_DROPPED_ITEM_LAG_TICKS_BEFORE_RESYNC = 10L
    private const val DROPPED_ITEM_COARSE_PARK_THRESHOLD_NANOS = 2_000_000L
    private const val DROPPED_ITEM_COARSE_PARK_GUARD_NANOS = 100_000L
    private const val CHUNK_MSPT_ACTION_BAR_INTERVAL_NANOS = 50_000_000L
    private const val GAME_MODE_SURVIVAL = 0
    private const val GAME_MODE_CREATIVE = 1
    private const val GAME_MODE_SPECTATOR = 3
    // Data component ids follow DataComponents static registration order in 1.21.11.
    private const val ITEM_COMPONENT_CUSTOM_NAME_ID = 6
    private const val ITEM_COMPONENT_LORE_ID = 11
    private const val MAX_PLAYER_HEALTH = 20f
    private const val MAX_PLAYER_FOOD = 20
    private const val DEFAULT_PLAYER_FOOD = 20
    private const val DEFAULT_PLAYER_SATURATION = 5f
    private const val SATURATED_REGEN_FOOD_THRESHOLD = 20
    private const val NORMAL_REGEN_FOOD_THRESHOLD = 18
    private const val SATURATED_REGEN_INTERVAL_SECONDS = 0.5
    private const val NORMAL_REGEN_INTERVAL_SECONDS = 4.0
    private const val SATURATED_REGEN_MAX_EXHAUSTION = 6f
    private const val NORMAL_REGEN_HEAL_AMOUNT = 1f
    private const val NORMAL_REGEN_EXHAUSTION = 6f
    private const val FOOD_EXHAUSTION_PER_LEVEL = 4f
    private const val VANILLA_STAT_CENTIMETER_SCALE = 100.0
    private const val VANILLA_WATER_EXHAUSTION_PER_CENTIMETER = 0.0001f // 0.01 per block
    private const val VANILLA_SPRINT_EXHAUSTION_PER_CENTIMETER = 0.001f // 0.1 per block
    private const val VANILLA_JUMP_EXHAUSTION = 0.05f
    private const val VANILLA_SPRINT_JUMP_EXHAUSTION = 0.2f
    private const val VANILLA_JUMP_MIN_ASCENT_METERS = 1.0e-6
    private const val FOOD_USE_SOUND_INITIAL_DELAY_SECONDS = 0.2
    private const val FOOD_USE_SOUND_INTERVAL_SECONDS = 0.2
    private const val MAX_ITEM_USE_SOUND_LOOPS_PER_TICK = 8
    private const val REGEN_TIME_EPSILON = 1.0e-9
    private const val MAX_PLAYER_REGEN_LOOPS_PER_TICK = 64
    private const val MINECRAFT_TICKS_PER_SECOND = 20.0
    private const val MINECRAFT_DAY_TICKS = 24_000.0
    private const val INITIAL_WORLD_TIME_TICKS = 0.0
    private const val WORLD_TIME_BROADCAST_INTERVAL_SECONDS = 1.0
    private const val MAX_FALLING_BLOCK_COLUMN_SCAN = 32
    private const val SAFE_FALL_DISTANCE_BLOCKS = 3.0
    private const val FALL_DAMAGE_MULTIPLIER = 1.0
    private const val FALL_DAMAGE_EPSILON = 1.0e-6
    private const val MAX_HOTBAR_STACK_SIZE = 64
    private const val PLAYER_DROPPED_ITEM_PICKUP_DELAY_SECONDS = 2.0
    private const val BLOCK_DROP_PICKUP_DELAY_SECONDS = 0.5
    private const val BLOCK_DROP_HORIZONTAL_SPEED_MIN = -0.1
    private const val BLOCK_DROP_HORIZONTAL_SPEED_MAX = 0.1
    private const val BLOCK_DROP_VERTICAL_SPEED = 0.2
    private const val THROWN_ITEM_LAUNCH_SPEED = 1.5
    private const val THROWN_ITEM_LAUNCH_VERTICAL_BOOST = 0.1
    private const val THROWN_ITEM_HITBOX_RADIUS = 0.125
    private const val PIG_BOOST_MIN_TICKS = 140
    private const val PIG_BOOST_RANDOM_SPREAD_TICKS = 841
    private const val CHORUS_FRUIT_RANDOM_TELEPORT_ATTEMPTS = 16
    private const val CHORUS_FRUIT_RANDOM_TELEPORT_DIAMETER = 16.0
    private const val CHORUS_TELEPORT_CONTACT_EPSILON = 1.0e-7
    private const val CHORUS_FRUIT_MIN_TELEPORT_Y_FALLBACK = -64
    private const val CHORUS_FRUIT_MAX_TELEPORT_Y_FALLBACK = 319
    private const val MAX_CLIENT_VEHICLE_POSE_AGE_NANOS = 500_000_000L
    private const val MAX_CLIENT_VEHICLE_POSE_DELTA_SQ = 64.0
    // Vanilla TemptGoal target condition range.
    private const val PIG_TEMPT_RANGE_BLOCKS = 10.0
    private const val NEUTRAL_SOUND_SOURCE_ID = 6
    private const val PLAYERS_SOUND_SOURCE_ID = 7
    private const val CHORUS_FRUIT_TELEPORT_SOUND_KEY = "minecraft:item.chorus_fruit.teleport"
    private const val SPYGLASS_USE_SOUND_KEY = "minecraft:item.spyglass.use"
    private const val SPYGLASS_STOP_USING_SOUND_KEY = "minecraft:item.spyglass.stop_using"
    private const val BIG_FALL_SOUND_DAMAGE_THRESHOLD = 5.0f
    private const val FALL_DAMAGE_TYPE_ID_FALLBACK = 17
    private val PLACEMENT_DEBUG_LOG_ENABLED: Boolean =
        System.getProperty("aerogel.debug.placement")?.toBooleanStrictOrNull() ?: false
    private const val PLAYER_HITBOX_HALF_WIDTH = 0.3
    private const val PIG_RIDER_OFFSET_Y_FACTOR = 0.75
    private val WATER_BUCKET_REPLACEABLE_BLOCK_KEYS = loadBlockTag("water_breakable")
    private val BLOCK_PLACEMENT_REPLACEABLE_BLOCK_KEYS = hashSetOf(
        "minecraft:water",
        "minecraft:lava",
        "minecraft:bubble_column"
    ).apply {
        addAll(loadBlockTag("replaceable"))
    }
    private const val DROPPED_ITEM_PICKUP_EXPAND_HORIZONTAL = 1.0
    private const val DROPPED_ITEM_PICKUP_EXPAND_UP = 0.5
    private const val DROPPED_ITEM_PICKUP_EXPAND_DOWN = 0.5
    private const val DROPPED_ITEM_HALF_WIDTH = 0.125
    private const val DROPPED_ITEM_HEIGHT = 0.25
    private const val DROPPED_ITEM_PICKUP_DELAY_EPSILON = 1.0e-9
    private const val DROPPED_ITEM_PICKUP_DISTANCE_EPSILON = 1.0e-9
    private const val PLAYER_RELATIVE_MOVE_SCALE = 4096.0
    private const val DROPPED_ITEM_RELATIVE_MOVE_SCALE = 4096.0
    private const val MAX_DROPPED_ITEM_RELATIVE_SECONDS_BEFORE_HARD_SYNC = 20.0
    private const val DROPPED_ITEM_VELOCITY_EPSILON = 1.0e-8
    private const val ANIMAL_ROTATION_EPSILON_DEGREES = 0.5f
    private const val SMALL_ENTITY_PACKET_IMMEDIATE_BYTES = 96
    private const val PLAYER_INVENTORY_CONTAINER_ID = 0
    private const val CONTAINER_TYPE_PLAYER_INVENTORY = 0
    private const val CONTAINER_TYPE_CRAFTING_TABLE = 1
    private const val CONTAINER_TYPE_FURNACE = 2
    private const val CRAFTING_TABLE_MENU_TYPE_ID = 12
    private val furnaceMenuTypeId: Int by lazy {
        RegistryCodec.entryIndex("minecraft:menu", "minecraft:furnace") ?: 14
    }
    private const val CLICK_TYPE_PICKUP = 0
    private const val CLICK_TYPE_QUICK_MOVE = 1
    private const val CLICK_TYPE_SWAP = 2
    private const val CLICK_TYPE_QUICK_CRAFT = 5
    private const val CLICK_TYPE_PICKUP_ALL = 6
    private const val OFFHAND_SWAP_BUTTON = 40
    private const val MAX_SHIFT_CRAFT_ITERATIONS = 2048
    private const val PLAYER_CRAFT_RESULT_SLOT = 0
    private val PLAYER_CRAFT_INPUT_SLOT_RANGE = 1..4
    private const val TABLE_CRAFT_RESULT_SLOT = 0
    private val TABLE_CRAFT_INPUT_SLOT_RANGE = 1..9
    private val TABLE_PLAYER_MAIN_SLOT_RANGE = 10..36
    private val TABLE_PLAYER_HOTBAR_SLOT_RANGE = 37..45
    private const val FURNACE_INPUT_SLOT = 0
    private const val FURNACE_FUEL_SLOT = 1
    private const val FURNACE_RESULT_SLOT = 2
    private val FURNACE_PLAYER_MAIN_SLOT_RANGE = 3..29
    private val FURNACE_PLAYER_HOTBAR_SLOT_RANGE = 30..38
    private val FURNACE_RESULT_QUICK_MOVE_TARGET_SLOTS: IntArray = (38 downTo 3).toList().toIntArray()
    private const val DEFAULT_FURNACE_COOK_SECONDS = 10.0
    private const val MIN_FURNACE_COOK_SECONDS = 0.05
    private val PLAYER_CRAFT_RESULT_QUICK_MOVE_TARGET_SLOTS: IntArray =
        (44 downTo 9).toList().toIntArray()
    private val TABLE_CRAFT_RESULT_QUICK_MOVE_TARGET_SLOTS: IntArray =
        (45 downTo 10).toList().toIntArray()
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
    private val NON_SUPPORT_BLOCK_KEYS = hashSetOf(
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
    ).apply {
        addAll(loadBlockTag("replaceable"))
        addAll(loadBlockTag("flowers"))
        addAll(loadBlockTag("small_flowers"))
        addAll(loadBlockTag("tall_flowers"))
        addAll(loadBlockTag("crops"))
        addAll(loadBlockTag("bee_growables"))
    }
    private val fallingGravityStateCache = ConcurrentHashMap<Int, Boolean>()
    private val fallingPassThroughStateCache = ConcurrentHashMap<Int, Boolean>()
    private val liquidStateCache = ConcurrentHashMap<Int, Boolean>()
    private val dimensionMinMaxYByWorldKey: Map<String, Pair<Int, Int>> by lazy {
        val registry = RegistryCodec.allRegistries()
            .firstOrNull { it.id == "minecraft:dimension_type" || it.id == "dimension_type" }
            ?: return@lazy emptyMap()
        val out = HashMap<String, Pair<Int, Int>>(registry.entries.size)
        for (entry in registry.entries) {
            val root = entry.value["value"]?.jsonObject ?: continue
            val minY = readTypedIntField(root, "min_y") ?: continue
            val height = readTypedIntField(root, "height")
                ?: readTypedIntField(root, "logical_height")
                ?: continue
            if (height <= 0) continue
            out[entry.key] = minY to (minY + height - 1)
        }
        out
    }
    private val FALLING_BLOCK_KEYS = loadBlockTag("falling_blocks")
    private val FALLING_BLOCK_PASS_THROUGH_KEYS = loadBlockTag("falling_block_pass_through")
    private fun loadBlockTag(tagName: String): Set<String> {
        return resolveBlockTag(tagName, HashSet())
    }

    private fun loadItemTag(tagName: String): Set<String> {
        return resolveItemTag(tagName, HashSet())
    }

    private fun resolveBlockTag(tagName: String, visited: MutableSet<String>): Set<String> {
        if (!visited.add(tagName)) return emptySet()
        val resourcePath = "/data/minecraft/tags/block/$tagName.json"
        val stream = PlayerSessionManager::class.java.getResourceAsStream(resourcePath) ?: return emptySet()
        val parser = Json { ignoreUnknownKeys = true }
        val root = stream.bufferedReader().use {
            parser.parseToJsonElement(it.readText()).jsonObject
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

    private fun resolveItemTag(tagName: String, visited: MutableSet<String>): Set<String> {
        if (!visited.add(tagName)) return emptySet()
        val resourcePath = "/data/minecraft/tags/item/$tagName.json"
        val stream = PlayerSessionManager::class.java.getResourceAsStream(resourcePath) ?: return emptySet()
        val parser = Json { ignoreUnknownKeys = true }
        val root = stream.bufferedReader().use {
            parser.parseToJsonElement(it.readText()).jsonObject
        }
        val out = LinkedHashSet<String>()
        for (value in root["values"]?.jsonArray.orEmpty()) {
            val raw = value.jsonPrimitive.content
            when {
                raw.startsWith("#minecraft:") -> out.addAll(resolveItemTag(raw.removePrefix("#minecraft:"), visited))
                raw.startsWith("#") -> out.addAll(resolveItemTag(raw.substring(1).substringAfter("minecraft:"), visited))
                raw.startsWith("minecraft:") -> out.add(raw)
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
    private val SNOWBALL_THROW_SOUND_ID: Int? by lazy {
        RegistryCodec.entryIndex("minecraft:sound_event", "minecraft:entity.snowball.throw")
            ?: RegistryCodec.entryIndex("sound_event", "minecraft:entity.snowball.throw")
            ?: SNOWBALL_THROW_SOUND_ID_FALLBACK
    }
    private val EGG_THROW_SOUND_ID: Int? by lazy {
        RegistryCodec.entryIndex("minecraft:sound_event", "minecraft:entity.egg.throw")
            ?: RegistryCodec.entryIndex("sound_event", "minecraft:entity.egg.throw")
            ?: EGG_THROW_SOUND_ID_FALLBACK
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
    private const val BASE_UNARMED_ATTACK_SPEED = 4.0f
    private const val DEFAULT_ATTACK_STRENGTH_DELAY_TICKS = 5.0f
    private const val INITIAL_ATTACK_STRENGTH_TICKS = 20.0
    private const val MAX_ATTACK_STRENGTH_TICKER = 200.0
    private const val HURT_INVULNERABILITY_SECONDS = 0.5
    private const val DAMAGE_EPSILON = 1.0e-4f
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
    private const val ANIMAL_BASE_ATTACK_KNOCKBACK = 0.4
    private const val ANIMAL_SPRINT_ATTACK_KNOCKBACK = 0.5
    private const val ENDER_PEARL_TELEPORT_DAMAGE = 5.0f
    private const val ENDER_PEARL_COOLDOWN_TICKS = 20
    private const val ENDER_PEARL_COOLDOWN_NANOS = ENDER_PEARL_COOLDOWN_TICKS * 50_000_000L
    private const val ENDER_PEARL_COOLDOWN_GROUP = "minecraft:ender_pearl"
    private const val CHORUS_FRUIT_COOLDOWN_TICKS = 20
    private const val CHORUS_FRUIT_COOLDOWN_NANOS = CHORUS_FRUIT_COOLDOWN_TICKS * 50_000_000L
    private const val CHORUS_FRUIT_COOLDOWN_GROUP = "minecraft:chorus_fruit"
    private const val VANILLA_ENTITY_PUSH_STRENGTH_PER_TICK = 0.05000000074505806
    private const val VANILLA_ENTITY_PUSH_MIN_DISTANCE = 0.009999999776482582
    private const val ENTITY_PUSH_MAX_PLAYER_VELOCITY = 0.45
    private const val PUSHING_CONTACT_STICKY_TICKS = 2
    private const val PUSHING_WAKE_MIN_HORIZONTAL_MOVEMENT_SQ = 1.0e-6
    private const val PUSHING_SPATIAL_CELL_SIZE = 1.0
    private const val PUSHING_SPATIAL_REACH_MARGIN = 0.1
    private const val PLAYER_SMALL_FALL_SOUND_ID_FALLBACK = 1257
    private const val PLAYER_BIG_FALL_SOUND_ID_FALLBACK = 1247
    private const val PLAYER_HURT_SOUND_ID_FALLBACK = 1251
    private const val PLAYER_ATTACK_KNOCKBACK_SOUND_ID_FALLBACK = 1242
    private const val PLAYER_ATTACK_CRIT_SOUND_ID_FALLBACK = 1241
    private const val PLAYER_ATTACK_STRONG_SOUND_ID_FALLBACK = 1244
    private const val PLAYER_ATTACK_SWEEP_SOUND_ID_FALLBACK = 1245
    private const val PLAYER_ATTACK_WEAK_SOUND_ID_FALLBACK = 1246
    private const val SNOWBALL_THROW_SOUND_ID_FALLBACK = 1494
    private const val EGG_THROW_SOUND_ID_FALLBACK = 639
    private const val ANIMAL_SMALL_FALL_SOUND_KEY = "minecraft:entity.generic.small_fall"
    private const val ANIMAL_BIG_FALL_SOUND_KEY = "minecraft:entity.generic.big_fall"
    private const val ANIMAL_DEATH_REMOVE_DELAY_SECONDS = 1.0
    private const val ENTITY_EVENT_DEATH_ANIMATION = 3
    private const val ENTITY_EVENT_FINISH_USING_ITEM = 9
    private const val ENTITY_EVENT_TELEPORT = 46
    private const val ENTITY_EVENT_THROWN_ITEM_BREAK_PARTICLES = 3
    private const val ENTITY_ANIMATION_CRITICAL_HIT = 4
    private const val KNOCKBACK_Y_PRE_GRAVITY_COMPENSATION = 0.08

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
            refreshRetainedBaseChunksForSession(session)
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
                                hideThrownItemsForChunk(session, ctx, pos, world.thrownItemEntityIdsInChunk(pos.x, pos.z))
                                hideAnimalsForChunk(session, ctx, pos, world.animalEntityIdsInChunk(pos.x, pos.z))
                                session.loadedChunks.remove(pos)
                                ctx.write(PlayPackets.unloadChunkPacket(pos.x, pos.z))
                            }
                            refreshRetainedBaseChunksForSession(session)
                            ctx.writeAndFlush(PlayPackets.chunkBatchFinishedPacket(totalSentCount))
                            drain()
                        }

                        if (unloadIdsByChunk.isEmpty()) {
                            finishUnloadAndFlush()
                            return@execute
                        }

                        val remainingUnloadFetches = AtomicInteger(unloadIdsByChunk.size)
                        val unloadCompletionScheduled = AtomicBoolean(false)
                        fun completeUnloadFetches() {
                            if (remainingUnloadFetches.decrementAndGet() != 0) return
                            if (!unloadCompletionScheduled.compareAndSet(false, true)) return
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
                        for (future in unloadIdsByChunk.values) {
                            future.whenComplete { _, _ ->
                                completeUnloadFetches()
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
                    sendThrownItemsForChunk(session, ctx, world.thrownItemsInChunk(chunkPos.x, chunkPos.z))
                    sendAnimalsForChunk(session, ctx, world.animalsInChunk(chunkPos.x, chunkPos.z))
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
