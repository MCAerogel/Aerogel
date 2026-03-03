package org.macaroon3145.api.entity

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.macaroon3145.api.type.ItemType
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.EOFException
import kotlin.collections.AbstractMutableList

enum class ArmorSlot {
    HELMET,
    CHESTPLATE,
    LEGGINGS,
    BOOTS;

    val armorIndex: Int
        get() = when (this) {
            HELMET -> 3
            CHESTPLATE -> 2
            LEGGINGS -> 1
            BOOTS -> 0
        }

    companion object {
        fun fromArmorIndex(index: Int): ArmorSlot? {
            return when (index) {
                3 -> HELMET
                2 -> CHESTPLATE
                1 -> LEGGINGS
                0 -> BOOTS
                else -> null
            }
        }
    }
}

/**
 * A mutable item model used in plugin API.
 * [type] can be null when this API version does not know the numeric item id yet.
 */
data class Item(
    val id: Int,
    val type: ItemType?,
    val amount: Int,
    var name: String? = null,
    var lore: List<String> = emptyList()
) {
    @JvmOverloads
    constructor(
        type: ItemType,
        amount: Int = 1,
        name: String? = null,
        lore: List<String> = emptyList()
    ) : this(
        id = type.id,
        type = type,
        amount = amount,
        name = name,
        lore = lore
    )

    fun setName(name: String?): Item {
        this.name = name
        return this
    }

    fun resetName(): Item {
        this.name = null
        return this
    }

    fun setLore(lines: List<String>): Item {
        lore = lines.toList()
        return this
    }

    fun setLore(vararg lines: String): Item {
        lore = lines.toList()
        return this
    }

    fun resetLore(): Item {
        lore = emptyList()
        return this
    }

    fun clone(): Item {
        return copy(lore = lore.toList())
    }

    fun toJsonString(pretty: Boolean = false): String {
        val serializer = if (pretty) prettyJson else compactJson
        return serializer.encodeToString(JsonObject.serializer(), toJsonObject())
    }

    fun toByteArray(): ByteArray {
        val out = ByteArrayOutputStream()
        val data = DataOutputStream(out)
        data.writeInt(ITEM_BINARY_MAGIC)
        data.writeInt(ITEM_BINARY_VERSION)
        data.writeInt(id)
        data.writeInt(amount)
        writeNullableString(data, name)
        data.writeInt(lore.size)
        for (line in lore) {
            data.writeUTF(line)
        }
        data.flush()
        return out.toByteArray()
    }

    fun toJsonObject(): JsonObject {
        val itemTypeKey = type?.key
        return buildJsonObject {
            put("id", JsonPrimitive(id))
            put("amount", JsonPrimitive(amount))
            if (itemTypeKey != null) {
                put("itemType", JsonPrimitive(itemTypeKey))
            }
            if (name != null) {
                put("name", JsonPrimitive(name))
            }
            put("lore", JsonArray(lore.map(::JsonPrimitive)))
        }
    }

    companion object {
        private val compactJson = Json { ignoreUnknownKeys = true }
        private val prettyJson = Json { prettyPrint = true; ignoreUnknownKeys = true }
        private const val ITEM_BINARY_MAGIC = 0x41475249 // 'AGRI'
        private const val ITEM_BINARY_VERSION = 1

        fun fromJsonString(jsonText: String): Item {
            val root = compactJson.parseToJsonElement(jsonText).jsonObject
            return fromJsonObject(root)
        }

        fun fromJsonObject(root: JsonObject): Item {
            val id = root["id"]?.jsonPrimitive?.intOrNull ?: root["itemId"]?.jsonPrimitive?.intOrNull
            val amount = root["amount"]?.jsonPrimitive?.intOrNull ?: 1
            val itemType = root["itemType"]?.jsonPrimitive?.contentOrNull?.let(ItemType::fromKey)
            val resolvedId = id ?: itemType?.id ?: -1
            val resolvedItemType = itemType ?: ItemType.fromId(resolvedId)
            val name = root["name"]?.jsonPrimitive?.contentOrNull
            val lore = root["lore"]?.jsonArray?.mapNotNull { it.jsonPrimitive.contentOrNull } ?: emptyList()
            return Item(
                id = resolvedId,
                type = resolvedItemType,
                amount = amount,
                name = name,
                lore = lore
            )
        }

        fun fromByteArray(bytes: ByteArray): Item {
            val input = DataInputStream(ByteArrayInputStream(bytes))
            val magic = input.readInt()
            if (magic != ITEM_BINARY_MAGIC) {
                throw IllegalArgumentException("Invalid Item binary payload magic")
            }
            val version = input.readInt()
            if (version != ITEM_BINARY_VERSION) {
                throw IllegalArgumentException("Unsupported Item binary payload version: $version")
            }
            val id = input.readInt()
            val amount = input.readInt()
            val name = readNullableString(input)
            val loreSize = input.readInt()
            if (loreSize < 0) throw IllegalArgumentException("Invalid lore size: $loreSize")
            val lore = ArrayList<String>(loreSize)
            repeat(loreSize) {
                lore.add(input.readUTF())
            }
            val itemType = ItemType.fromId(id)
            return Item(
                id = id,
                type = itemType,
                amount = amount,
                name = name,
                lore = lore
            )
        }

        private fun writeNullableString(out: DataOutputStream, value: String?) {
            if (value == null) {
                out.writeBoolean(false)
                return
            }
            out.writeBoolean(true)
            out.writeUTF(value)
        }

        private fun readNullableString(input: DataInputStream): String? {
            return try {
                if (!input.readBoolean()) null else input.readUTF()
            } catch (_: EOFException) {
                null
            }
        }
    }
}

/**
 * Fixed-size mutable slot view backed by runtime inventory state.
 */
class InventorySlotView(
    override val size: Int,
    private val getter: (Int) -> Item?,
    private val setter: (Int, Item?) -> Boolean
) : AbstractMutableList<Item?>() {
    override fun get(index: Int): Item? {
        require(index in 0 until size) { "Slot out of range: $index" }
        return getter(index)
    }

    override fun set(index: Int, element: Item?): Item? {
        require(index in 0 until size) { "Slot out of range: $index" }
        val previous = getter(index)
        check(setter(index, element)) { "Failed to update slot: $index" }
        return previous
    }

    override fun add(index: Int, element: Item?) {
        throw UnsupportedOperationException("Inventory slots are fixed-size. Use slot assignment instead.")
    }

    override fun removeAt(index: Int): Item? {
        throw UnsupportedOperationException("Inventory slots are fixed-size. Use slot assignment to null instead.")
    }
}

data class PlayerInventorySnapshot(
    val selectedHotbarSlot: Int,
    val hotbar: MutableList<Item?>,
    val main: MutableList<Item?>,
    val armor: MutableList<Item?>,
    val offhand: Item?
) {
    var hand: Item?
        get() {
            val index = selectedHotbarSlot.coerceIn(0, hotbar.lastIndex.coerceAtLeast(0))
            return hotbar.getOrNull(index)
        }
        set(value) {
            if (hotbar.isEmpty()) return
            val index = selectedHotbarSlot.coerceIn(0, hotbar.lastIndex)
            hotbar[index] = value
        }

    fun hotbar(slot: Int): Item? = hotbar.getOrNull(slot)

    fun main(slot: Int): Item? = main.getOrNull(slot)

    fun armor(slot: ArmorSlot): Item? = armor.getOrNull(slot.armorIndex)

    val helmet: Item?
        get() = armor(ArmorSlot.HELMET)
    val chestplate: Item?
        get() = armor(ArmorSlot.CHESTPLATE)
    val leggings: Item?
        get() = armor(ArmorSlot.LEGGINGS)
    val boots: Item?
        get() = armor(ArmorSlot.BOOTS)

    fun selectedHotbarItem(): Item? {
        return hand
    }
}
