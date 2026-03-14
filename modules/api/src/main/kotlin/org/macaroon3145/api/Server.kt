package org.macaroon3145.api

import org.macaroon3145.api.entity.ConnectedPlayer
import org.macaroon3145.api.entity.InventoryAddResult
import org.macaroon3145.api.entity.Item
import org.macaroon3145.api.entity.PlayerRegistry
import org.macaroon3145.api.inventory.InventoryRuntimeBridge
import org.macaroon3145.api.plugin.PluginRuntime
import org.macaroon3145.api.scheduler.TaskScheduler
import org.macaroon3145.api.scheduler.TickScheduler
import org.macaroon3145.api.type.Sound
import org.macaroon3145.api.world.World
import org.macaroon3145.api.world.Location
import org.macaroon3145.api.world.WorldRegistry
import org.macaroon3145.network.handler.ItemStackState
import java.util.UUID

object Server {
    @Volatile
    private var worldRegistry: WorldRegistry? = null
    @Volatile
    private var boundMainTickScheduler: TickScheduler? = null
    @Volatile
    private var boundTaskScheduler: TaskScheduler? = null
    @Volatile
    private var playerRegistry: PlayerRegistry? = null
    @Volatile
    private var inventoryBridge: InventoryRuntimeBridge? = null
    @Volatile
    private var currentTps: Double = 0.0
    @Volatile
    private var currentMspt: Double = 0.0

    val mainTickScheduler: TickScheduler
        get() = PluginRuntime.currentContextOrNull()?.tickScheduler
            ?: checkNotNull(boundMainTickScheduler) { "Server is not initialized yet: mainTickScheduler is unavailable." }

    val taskScheduler: TaskScheduler
        get() = PluginRuntime.currentContextOrNull()?.taskScheduler
            ?: checkNotNull(boundTaskScheduler) { "Server is not initialized yet: taskScheduler is unavailable." }

    val tps: Double
        get() = currentTps

    val mspt: Double
        get() = currentMspt

    val defaultWorld: World
        get() = checkNotNull(worldRegistry?.defaultWorld()) { "Server is not initialized yet: defaultWorld is unavailable." }

    fun getWorld(key: String): World? = worldRegistry?.world(key)
    fun world(key: String): World? = getWorld(key)

    fun getPlayer(uuid: UUID): ConnectedPlayer? {
        val registry = checkNotNull(playerRegistry) { "Server is not initialized yet: playerRegistry is unavailable." }
        return registry.player(uuid)
    }

    fun getPlayer(name: String): ConnectedPlayer? {
        val registry = checkNotNull(playerRegistry) { "Server is not initialized yet: playerRegistry is unavailable." }
        return registry.player(name)
    }

    val players: List<ConnectedPlayer>
        get() {
            val registry = checkNotNull(playerRegistry) { "Server is not initialized yet: playerRegistry is unavailable." }
            return registry.players()
        }

    fun broadcastMessage(message: String) {
        val normalized = message
        players.forEach { player ->
            player.sendMessage(normalized)
        }
    }

    fun getWorlds(): List<World> = worldRegistry?.allWorlds() ?: emptyList()

    fun getMainTick(): Long = mainTickScheduler.currentTick

    fun isReady(): Boolean = worldRegistry != null

    fun bindWorldRegistry(registry: WorldRegistry) {
        worldRegistry = registry
    }

    fun bindMainTickScheduler(scheduler: TickScheduler) {
        boundMainTickScheduler = scheduler
    }

    fun bindTaskScheduler(scheduler: TaskScheduler) {
        boundTaskScheduler = scheduler
    }

    fun bindPlayerRegistry(registry: PlayerRegistry) {
        playerRegistry = registry
    }

    fun bindInventoryBridge(bridge: InventoryRuntimeBridge) {
        inventoryBridge = bridge
    }

    fun createChestInventory(size: Int, title: String?, location: Location?): Long {
        val bridge = checkNotNull(inventoryBridge) { "Server is not initialized yet: inventoryBridge is unavailable." }
        return bridge.createChestInventory(size, title, location)
    }

    fun openChestInventory(playerUuid: UUID, inventoryId: Long, page: Int = 0): Boolean {
        val bridge = checkNotNull(inventoryBridge) { "Server is not initialized yet: inventoryBridge is unavailable." }
        return bridge.openChestInventory(playerUuid, inventoryId, page)
    }

    fun chestInventoryCurrentPage(playerUuid: UUID, inventoryId: Long): Int? {
        val bridge = checkNotNull(inventoryBridge) { "Server is not initialized yet: inventoryBridge is unavailable." }
        return bridge.chestInventoryCurrentPage(playerUuid, inventoryId)
    }

    fun chestInventoryPageCount(inventoryId: Long): Int {
        val bridge = checkNotNull(inventoryBridge) { "Server is not initialized yet: inventoryBridge is unavailable." }
        return bridge.chestInventoryPageCount(inventoryId)
    }

    fun chestInventoryPageNavigationItems(inventoryId: Long): Boolean {
        val bridge = checkNotNull(inventoryBridge) { "Server is not initialized yet: inventoryBridge is unavailable." }
        return bridge.chestInventoryPageNavigationItems(inventoryId)
    }

    fun chestInventoryPageNavigationClickSound(inventoryId: Long): Boolean {
        val bridge = checkNotNull(inventoryBridge) { "Server is not initialized yet: inventoryBridge is unavailable." }
        return bridge.chestInventoryPageNavigationClickSound(inventoryId)
    }

    fun chestInventoryNavigationItem(inventoryId: Long, previous: Boolean): Item? {
        val bridge = checkNotNull(inventoryBridge) { "Server is not initialized yet: inventoryBridge is unavailable." }
        return bridge.chestInventoryNavigationItem(inventoryId, previous)
    }

    fun getChestInventorySlot(inventoryId: Long, slot: Int): ItemStackState? {
        val bridge = checkNotNull(inventoryBridge) { "Server is not initialized yet: inventoryBridge is unavailable." }
        return bridge.getChestInventorySlot(inventoryId, slot)
    }

    fun setChestInventorySlot(inventoryId: Long, slot: Int, itemId: Int, amount: Int): Boolean {
        val bridge = checkNotNull(inventoryBridge) { "Server is not initialized yet: inventoryBridge is unavailable." }
        return bridge.setChestInventorySlot(inventoryId, slot, itemId, amount)
    }

    fun setChestInventorySlot(inventoryId: Long, slot: Int, item: Item?): Boolean {
        val bridge = checkNotNull(inventoryBridge) { "Server is not initialized yet: inventoryBridge is unavailable." }
        return bridge.setChestInventorySlot(inventoryId, slot, item)
    }

    fun addChestInventoryItem(inventoryId: Long, item: Item, overflowDrop: Boolean): InventoryAddResult {
        val bridge = checkNotNull(inventoryBridge) { "Server is not initialized yet: inventoryBridge is unavailable." }
        return bridge.addChestInventoryItem(inventoryId, item, overflowDrop)
    }

    fun setChestInventorySize(inventoryId: Long, size: Int): Boolean {
        val bridge = checkNotNull(inventoryBridge) { "Server is not initialized yet: inventoryBridge is unavailable." }
        return bridge.setChestInventorySize(inventoryId, size)
    }

    fun setChestInventoryTitle(inventoryId: Long, title: String?): Boolean {
        val bridge = checkNotNull(inventoryBridge) { "Server is not initialized yet: inventoryBridge is unavailable." }
        return bridge.setChestInventoryTitle(inventoryId, title)
    }

    fun setChestInventoryNavigationItem(inventoryId: Long, previous: Boolean, item: Item?): Boolean {
        val bridge = checkNotNull(inventoryBridge) { "Server is not initialized yet: inventoryBridge is unavailable." }
        return bridge.setChestInventoryNavigationItem(inventoryId, previous, item)
    }

    fun setChestInventoryPageNavigationItems(inventoryId: Long, enabled: Boolean): Boolean {
        val bridge = checkNotNull(inventoryBridge) { "Server is not initialized yet: inventoryBridge is unavailable." }
        return bridge.setChestInventoryPageNavigationItems(inventoryId, enabled)
    }

    fun setChestInventoryPageNavigationClickSound(inventoryId: Long, enabled: Boolean): Boolean {
        val bridge = checkNotNull(inventoryBridge) { "Server is not initialized yet: inventoryBridge is unavailable." }
        return bridge.setChestInventoryPageNavigationClickSound(inventoryId, enabled)
    }

    fun setChestInventoryPageNavigationClickSoundKey(inventoryId: Long, sound: String): Boolean {
        val bridge = checkNotNull(inventoryBridge) { "Server is not initialized yet: inventoryBridge is unavailable." }
        return bridge.setChestInventoryPageNavigationClickSoundKey(inventoryId, sound)
    }

    fun chestInventoryReadOnly(inventoryId: Long): Boolean {
        val bridge = checkNotNull(inventoryBridge) { "Server is not initialized yet: inventoryBridge is unavailable." }
        return bridge.chestInventoryReadOnly(inventoryId)
    }

    fun setChestInventoryReadOnly(inventoryId: Long, readOnly: Boolean): Boolean {
        val bridge = checkNotNull(inventoryBridge) { "Server is not initialized yet: inventoryBridge is unavailable." }
        return bridge.setChestInventoryReadOnly(inventoryId, readOnly)
    }

    fun chestInventoryReadOnlyItem(inventoryId: Long, slot: Int): Boolean {
        val bridge = checkNotNull(inventoryBridge) { "Server is not initialized yet: inventoryBridge is unavailable." }
        return bridge.chestInventoryReadOnlyItem(inventoryId, slot)
    }

    fun setChestInventoryReadOnlyItem(inventoryId: Long, slot: Int, readOnly: Boolean): Boolean {
        val bridge = checkNotNull(inventoryBridge) { "Server is not initialized yet: inventoryBridge is unavailable." }
        return bridge.setChestInventoryReadOnlyItem(inventoryId, slot, readOnly)
    }

    fun chestInventoryViewers(inventoryId: Long): List<ConnectedPlayer> {
        val bridge = checkNotNull(inventoryBridge) { "Server is not initialized yet: inventoryBridge is unavailable." }
        return bridge.chestInventoryViewers(inventoryId)
    }

    fun playSound(playerUuid: UUID, sound: Sound, volume: Float = 1.0f, pitch: Float = 1.0f): Boolean {
        val bridge = checkNotNull(inventoryBridge) { "Server is not initialized yet: inventoryBridge is unavailable." }
        return bridge.playSound(playerUuid, sound, volume, pitch)
    }

    fun playSound(location: Location, sound: Sound, volume: Float = 1.0f, pitch: Float = 1.0f): Boolean {
        val bridge = checkNotNull(inventoryBridge) { "Server is not initialized yet: inventoryBridge is unavailable." }
        return bridge.playSound(location, sound, volume, pitch)
    }

    fun playSound(playerUuid: UUID, soundKey: String, volume: Float = 1.0f, pitch: Float = 1.0f): Boolean {
        val bridge = checkNotNull(inventoryBridge) { "Server is not initialized yet: inventoryBridge is unavailable." }
        return bridge.playSound(playerUuid, soundKey, volume, pitch)
    }

    fun playSound(location: Location, soundKey: String, volume: Float = 1.0f, pitch: Float = 1.0f): Boolean {
        val bridge = checkNotNull(inventoryBridge) { "Server is not initialized yet: inventoryBridge is unavailable." }
        return bridge.playSound(location, soundKey, volume, pitch)
    }

    fun updateTickPerformance(tps: Double, mspt: Double) {
        currentTps = if (tps.isFinite()) tps else 0.0
        currentMspt = if (mspt.isFinite()) mspt else 0.0
    }

    fun clearBindings() {
        worldRegistry = null
        boundMainTickScheduler = null
        boundTaskScheduler = null
        playerRegistry = null
        inventoryBridge = null
        currentTps = 0.0
        currentMspt = 0.0
    }
}
