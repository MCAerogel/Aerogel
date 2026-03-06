package org.macaroon3145.api.inventory

import org.macaroon3145.api.Server
import org.macaroon3145.api.entity.InventoryAddResult
import org.macaroon3145.api.entity.ConnectedPlayer
import org.macaroon3145.api.entity.InventorySlotView
import org.macaroon3145.api.entity.Item
import org.macaroon3145.api.type.Sound
import org.macaroon3145.api.type.ItemType
import org.macaroon3145.api.world.Location

object Chest

class Inventory<T>(
    size: Int,
    title: String? = null,
    location: Location? = null
) {
    var size: Int
        get() = sizeInternal
        set(value) {
            val normalized = normalizeChestSize(value)
            if (normalized == sizeInternal) return
            check(Server.setChestInventorySize(inventoryId, normalized)) {
                "Failed to update chest inventory size: $normalized"
            }
            sizeInternal = normalized
        }
    var title: String?
        get() = titleInternal
        set(value) {
            val normalized = normalizeTitle(value)
            if (normalized == titleInternal) return
            check(Server.setChestInventoryTitle(inventoryId, normalized)) {
                "Failed to update chest inventory title."
            }
            titleInternal = normalized
        }
    val location: Location?
    val pageSize: Int = 54
    val pages: Int
        get() = Server.chestInventoryPageCount(inventoryId).coerceAtLeast(1)

    private val inventoryId: Long
    private var sizeInternal: Int
    private var titleInternal: String?
    var readOnly: Boolean
        get() = Server.chestInventoryReadOnly(inventoryId)
        set(value) {
            check(Server.setChestInventoryReadOnly(inventoryId, value)) {
                "Failed to update inventory readOnly: $value"
            }
        }
    var pageNavigationItems: Boolean
        get() = Server.chestInventoryPageNavigationItems(inventoryId)
        set(value) {
            Server.setChestInventoryPageNavigationItems(inventoryId, value)
        }
    var pageNavigationClickSound: Boolean
        get() = Server.chestInventoryPageNavigationClickSound(inventoryId)
        set(value) {
            Server.setChestInventoryPageNavigationClickSound(inventoryId, value)
        }
    var prevPageItem: Item?
        get() = Server.chestInventoryNavigationItem(inventoryId, previous = true)
        set(value) {
            Server.setChestInventoryNavigationItem(inventoryId, previous = true, item = value)
        }
    var nextPageItem: Item?
        get() = Server.chestInventoryNavigationItem(inventoryId, previous = false)
        set(value) {
            Server.setChestInventoryNavigationItem(inventoryId, previous = false, item = value)
        }

    val slots: InventorySlotView
    val items: InventorySlotView
        get() = slots
    val readOnlyItems: MutableList<Boolean>

    init {
        val normalizedSize = normalizeChestSize(size)
        this.sizeInternal = normalizedSize
        this.titleInternal = normalizeTitle(title)
        this.location = location
        inventoryId = Server.createChestInventory(
            size = normalizedSize,
            title = this.titleInternal,
            location = location
        )
        slots = InventorySlotView(
            sizeProvider = { sizeInternal },
            getter = { slot ->
                val raw = Server.getChestInventorySlot(inventoryId, slot) ?: return@InventorySlotView null
                val itemId = raw.first
                val amount = raw.second
                if (itemId < 0 || amount <= 0) return@InventorySlotView null
                Item(
                    id = itemId,
                    type = ItemType.fromId(itemId),
                    amount = amount
                )
            },
            setter = { slot, item ->
                Server.setChestInventorySlot(
                    inventoryId = inventoryId,
                    slot = slot,
                    item = item
                )
            }
        )
        readOnlyItems = object : AbstractMutableList<Boolean>() {
            override val size: Int
                get() = sizeInternal.coerceAtLeast(0)

            override fun get(index: Int): Boolean {
                require(index in 0 until size) { "Slot out of range: $index" }
                return Server.chestInventoryReadOnlyItem(inventoryId, index)
            }

            override fun set(index: Int, element: Boolean): Boolean {
                require(index in 0 until size) { "Slot out of range: $index" }
                val previous = Server.chestInventoryReadOnlyItem(inventoryId, index)
                check(Server.setChestInventoryReadOnlyItem(inventoryId, index, element)) {
                    "Failed to update readOnly slot: $index"
                }
                return previous
            }

            override fun add(index: Int, element: Boolean) {
                throw UnsupportedOperationException("Inventory readOnly slots are fixed-size. Use slot assignment instead.")
            }

            override fun removeAt(index: Int): Boolean {
                throw UnsupportedOperationException("Inventory readOnly slots are fixed-size. Use slot assignment instead.")
            }
        }
    }

    private fun normalizeChestSize(raw: Int): Int {
        require(raw > 0) {
            "Chest inventory size must be greater than 0: $raw"
        }
        val normalizedLong = ((raw.toLong() + 8L) / 9L) * 9L
        require(normalizedLong <= Int.MAX_VALUE.toLong()) {
            "Chest inventory size is too large after normalization: $normalizedLong"
        }
        return normalizedLong.toInt()
    }

    private fun normalizeTitle(raw: String?): String? {
        return raw?.trim()?.ifEmpty { null }
    }

    fun open(player: ConnectedPlayer): Boolean {
        return Server.openChestInventory(player.uniqueId, inventoryId, 0)
    }

    fun open(player: ConnectedPlayer, page: Int): Boolean {
        return Server.openChestInventory(player.uniqueId, inventoryId, page)
    }

    fun currentPage(player: ConnectedPlayer): Int? {
        return Server.chestInventoryCurrentPage(player.uniqueId, inventoryId)
    }

    fun nextPage(player: ConnectedPlayer): Boolean {
        val current = currentPage(player) ?: return false
        val next = (current + 1).coerceAtMost(pages - 1)
        return open(player, next)
    }

    fun prevPage(player: ConnectedPlayer): Boolean {
        val current = currentPage(player) ?: return false
        val prev = (current - 1).coerceAtLeast(0)
        return open(player, prev)
    }

    fun viewers(): List<ConnectedPlayer> {
        return Server.chestInventoryViewers(inventoryId)
    }

    fun addItem(item: Item, overflowDrop: Boolean = false): InventoryAddResult {
        return Server.addChestInventoryItem(inventoryId, item, overflowDrop)
    }

    fun setPageNavigationClickSound(sound: Sound) {
        setPageNavigationClickSound(sound.key)
    }

    fun setPageNavigationClickSound(soundKey: String) {
        val normalized = normalizeSoundKey(soundKey)
        check(Server.setChestInventoryPageNavigationClickSoundKey(inventoryId, normalized)) {
            "Failed to update page navigation click sound: $normalized"
        }
    }

    private fun normalizeSoundKey(raw: String): String {
        val trimmed = raw.trim()
        require(trimmed.isNotEmpty()) { "Sound key cannot be blank." }
        val lower = trimmed.lowercase()
        return if (':' in lower) lower else "minecraft:$lower"
    }
}

interface InventoryRuntimeBridge {
    fun createChestInventory(size: Int, title: String?, location: Location?): Long
    fun openChestInventory(playerUuid: java.util.UUID, inventoryId: Long, page: Int): Boolean
    fun chestInventoryCurrentPage(playerUuid: java.util.UUID, inventoryId: Long): Int?
    fun chestInventoryPageCount(inventoryId: Long): Int
    fun chestInventoryPageNavigationItems(inventoryId: Long): Boolean
    fun chestInventoryPageNavigationClickSound(inventoryId: Long): Boolean
    fun chestInventoryNavigationItem(inventoryId: Long, previous: Boolean): Item?
    fun getChestInventorySlot(inventoryId: Long, slot: Int): Pair<Int, Int>?
    fun setChestInventorySlot(inventoryId: Long, slot: Int, item: Item?): Boolean
    fun setChestInventorySlot(inventoryId: Long, slot: Int, itemId: Int, amount: Int): Boolean
    fun addChestInventoryItem(inventoryId: Long, item: Item, overflowDrop: Boolean): InventoryAddResult
    fun setChestInventorySize(inventoryId: Long, size: Int): Boolean
    fun setChestInventoryTitle(inventoryId: Long, title: String?): Boolean
    fun setChestInventoryNavigationItem(inventoryId: Long, previous: Boolean, item: Item?): Boolean
    fun setChestInventoryPageNavigationItems(inventoryId: Long, enabled: Boolean): Boolean
    fun setChestInventoryPageNavigationClickSound(inventoryId: Long, enabled: Boolean): Boolean
    fun setChestInventoryPageNavigationClickSoundKey(inventoryId: Long, soundKey: String): Boolean
    fun chestInventoryReadOnly(inventoryId: Long): Boolean
    fun setChestInventoryReadOnly(inventoryId: Long, readOnly: Boolean): Boolean
    fun chestInventoryReadOnlyItem(inventoryId: Long, slot: Int): Boolean
    fun setChestInventoryReadOnlyItem(inventoryId: Long, slot: Int, readOnly: Boolean): Boolean
    fun chestInventoryViewers(inventoryId: Long): List<ConnectedPlayer>
    fun playSound(playerUuid: java.util.UUID, sound: Sound, volume: Float, pitch: Float): Boolean
    fun playSound(location: Location, sound: Sound, volume: Float, pitch: Float): Boolean
    fun playSound(playerUuid: java.util.UUID, soundKey: String, volume: Float, pitch: Float): Boolean
    fun playSound(location: Location, soundKey: String, volume: Float, pitch: Float): Boolean
}
