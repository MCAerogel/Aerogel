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
import org.macaroon3145.api.plugin.PluginRuntime
import org.macaroon3145.api.type.ItemType
import org.macaroon3145.network.handler.ItemStackState
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
    data class ModelData(
        val floats: List<Float> = emptyList(),
        val flags: List<Boolean> = emptyList(),
        val strings: List<String> = emptyList(),
        val colors: List<Int> = emptyList()
    ) {
        fun isEmpty(): Boolean {
            return floats.isEmpty() && flags.isEmpty() && strings.isEmpty() && colors.isEmpty()
        }

        fun normalized(): ModelData {
            return ModelData(
                floats = floats.toList(),
                flags = flags.toList(),
                strings = strings.map { it.trim() },
                colors = colors.toList()
            )
        }

        operator fun plus(other: ModelData): ModelData {
            return ModelData(
                floats = this.floats + other.floats,
                flags = this.flags + other.flags,
                strings = this.strings + other.strings,
                colors = this.colors + other.colors
            ).normalized()
        }

        operator fun plus(other: String): ModelData {
            return this + ModelData(strings = listOf(other))
        }

        operator fun plus(other: Int): ModelData {
            return this + ModelData(floats = listOf(other.toFloat()))
        }

        operator fun plus(other: Float): ModelData {
            return this + ModelData(floats = listOf(other))
        }

        operator fun plus(other: Boolean): ModelData {
            return this + ModelData(flags = listOf(other))
        }
    }

    val maxAmount: Int
        get() {
            if (id < 0) return DEFAULT_MAX_STACK_SIZE
            val context = PluginRuntime.currentContextOrNull() ?: return DEFAULT_MAX_STACK_SIZE
            return context.types.itemMaxAmountById(id).coerceAtLeast(1)
        }

    data class Translation(
        val key: String,
        val args: List<String> = emptyList()
    )

    private var translatedName: Translation? = null
    private var translatedLore: List<Translation> = emptyList()
    private var modelDataInternal: ModelData = ModelData()

    var modelData: ModelData
        get() = modelDataInternal.normalized()
        set(value) {
            modelDataInternal = value.normalized()
        }

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
        this.translatedName = null
        return this
    }

    fun resetName(): Item {
        this.name = null
        this.translatedName = null
        return this
    }

    fun setLore(lines: List<String>): Item {
        lore = lines.toList()
        translatedLore = emptyList()
        return this
    }

    fun setLore(vararg lines: String): Item {
        lore = lines.toList()
        translatedLore = emptyList()
        return this
    }

    fun resetLore(): Item {
        lore = emptyList()
        translatedLore = emptyList()
        return this
    }

    fun trName(key: String, vararg args: String): Item {
        val normalizedKey = normalizeTranslationKey(key)
        translatedName = Translation(key = normalizedKey, args = args.toList())
        name = null
        return this
    }

    fun trLore(vararg keys: String): Item {
        translatedLore = keys
            .map(::normalizeTranslationKey)
            .filter { it.isNotEmpty() }
            .map { Translation(key = it) }
        lore = emptyList()
        return this
    }

    fun trLore(translations: List<Translation>): Item {
        translatedLore = translations
            .map { Translation(key = normalizeTranslationKey(it.key), args = it.args.toList()) }
            .filter { it.key.isNotEmpty() }
        lore = emptyList()
        return this
    }

    fun translatedName(): Translation? {
        val tr = translatedName ?: return null
        return tr.copy(args = tr.args.toList())
    }

    fun translatedLore(): List<Translation> = translatedLore.map { it.copy(args = it.args.toList()) }

    fun resolvedName(translator: (key: String, args: List<String>) -> String): String? {
        val tr = translatedName
        if (tr != null) return translator(tr.key, tr.args)
        return name
    }

    fun resolvedLore(translator: (key: String, args: List<String>) -> String): List<String> {
        if (translatedLore.isNotEmpty()) {
            return translatedLore.map { translator(it.key, it.args) }
        }
        return lore.toList()
    }

    fun clearTranslations(): Item {
        translatedName = null
        translatedLore = emptyList()
        return this
    }

    fun setModelData(value: Int): Item {
        modelData = ModelData(floats = listOf(value.toFloat()))
        return this
    }

    fun setModelData(value: Float): Item {
        modelData = ModelData(floats = listOf(value))
        return this
    }

    fun setModelData(value: Boolean): Item {
        modelData = ModelData(flags = listOf(value))
        return this
    }

    fun setModelData(value: String): Item {
        modelData = ModelData(strings = listOf(value))
        return this
    }

    fun setModelData(
        floats: List<Float> = emptyList(),
        flags: List<Boolean> = emptyList(),
        strings: List<String> = emptyList(),
        colors: List<Int> = emptyList()
    ): Item {
        modelData = ModelData(
            floats = floats.toList(),
            flags = flags.toList(),
            strings = strings.map { it.trim() },
            colors = colors.toList()
        )
        return this
    }

    fun addModelData(data: ModelData): Item {
        modelData = modelData + data
        return this
    }

    fun addModelData(value: Int): Item {
        return addModelData(ModelData(floats = listOf(value.toFloat())))
    }

    fun addModelData(value: Float): Item {
        return addModelData(ModelData(floats = listOf(value)))
    }

    fun addModelData(value: Boolean): Item {
        return addModelData(ModelData(flags = listOf(value)))
    }

    fun addModelData(value: String): Item {
        return addModelData(ModelData(strings = listOf(value)))
    }

    fun addModelData(
        floats: List<Float> = emptyList(),
        flags: List<Boolean> = emptyList(),
        strings: List<String> = emptyList(),
        colors: List<Int> = emptyList()
    ): Item {
        return addModelData(
            ModelData(
                floats = floats,
                flags = flags,
                strings = strings,
                colors = colors
            )
        )
    }

    fun resetModelData(): Item {
        modelData = ModelData()
        return this
    }

    fun clone(): Item {
        val cloned = Item(
            id = id,
            type = type,
            amount = amount,
            name = name,
            lore = lore.toList()
        )
        cloned.translatedName = translatedName?.let { it.copy(args = it.args.toList()) }
        cloned.translatedLore = translatedLore.map { it.copy(args = it.args.toList()) }
        cloned.modelData = modelData
        return cloned
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

    fun toItemStackState(fallback: ItemStackState? = null): ItemStackState {
        if (id < 0 || amount <= 0) return ItemStackState.empty()
        val translatedNameValue = translatedName()
        val translatedLoreValue = translatedLore()
        val base = fallback?.takeIf { it.itemId == id }
        if (base != null) {
            base.count = amount
            base.customName = name
            base.customLore = lore.toList()
            base.translatedNameKey = translatedNameValue?.key
            base.translatedNameArgs = translatedNameValue?.args ?: emptyList()
            base.translatedLore = translatedLoreValue.map { it.key to it.args }
            return base
        }
        return ItemStackState.of(
            itemId = id,
            count = amount,
            customName = name,
            customLore = lore.toList(),
            translatedNameKey = translatedNameValue?.key,
            translatedNameArgs = translatedNameValue?.args ?: emptyList(),
            translatedLore = translatedLoreValue.map { it.key to it.args }
        )
    }

    companion object {
        private const val DEFAULT_MAX_STACK_SIZE = 64
        private val compactJson = Json { ignoreUnknownKeys = true }
        private val prettyJson = Json { prettyPrint = true; ignoreUnknownKeys = true }
        private const val ITEM_BINARY_MAGIC = 0x41475249 // 'AGRI'
        private const val ITEM_BINARY_VERSION = 1

        private fun normalizeTranslationKey(key: String): String {
            return key.trim()
        }

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
    private val sizeProvider: () -> Int,
    private val getter: (Int) -> Item?,
    private val setter: (Int, Item?) -> Boolean
) : AbstractMutableList<Item?>() {
    constructor(
        size: Int,
        getter: (Int) -> Item?,
        setter: (Int, Item?) -> Boolean
    ) : this(
        sizeProvider = { size },
        getter = getter,
        setter = setter
    )

    override val size: Int
        get() = sizeProvider().coerceAtLeast(0)

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

data class InventoryAddResult(
    val requested: Int,
    val inserted: Int,
    val dropped: Int,
    val discarded: Int,
    val remaining: Int
) {
    val success: Boolean
        get() = remaining <= 0
}

data class PlayerInventory(
    val selectedHotbarSlot: Int,
    val hotbar: MutableList<Item?>,
    val main: MutableList<Item?>,
    val armor: MutableList<Item?>,
    val offhand: Item?,
    private val addItemDelegate: ((Item, Boolean) -> InventoryAddResult)? = null
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

    fun addItem(
        item: Item,
        overflowDrop: Boolean = true
    ): InventoryAddResult {
        val requested = item.amount.coerceAtLeast(0)
        if (item.id < 0 || requested <= 0) {
            return InventoryAddResult(
                requested = requested,
                inserted = 0,
                dropped = 0,
                discarded = 0,
                remaining = requested
            )
        }
        val delegate = addItemDelegate
        if (delegate != null) {
            return delegate(item.clone(), overflowDrop)
        }
        return addItemFallback(item, overflowDrop)
    }

    private fun addItemFallback(
        item: Item,
        overflowDrop: Boolean
    ): InventoryAddResult {
        val requested = item.amount.coerceAtLeast(0)
        if (item.id < 0 || requested <= 0) {
            return InventoryAddResult(requested, inserted = 0, dropped = 0, discarded = 0, remaining = requested)
        }
        val model = item.clone()
        val maxStack = model.maxAmount.coerceAtLeast(1)
        var remaining = requested
        fun cloneWithAmount(source: Item, amount: Int): Item {
            val cloned = source.clone()
            return cloned.copy(amount = amount).also {
                it.modelData = cloned.modelData
                val translatedName = cloned.translatedName()
                if (translatedName != null) {
                    it.trName(translatedName.key, *translatedName.args.toTypedArray())
                }
                val translatedLore = cloned.translatedLore()
                if (translatedLore.isNotEmpty()) {
                    it.trLore(translatedLore)
                }
            }
        }

        fun canMerge(existing: Item?): Boolean {
            if (existing == null || existing.id != model.id || existing.amount <= 0) return false
            return existing.name == model.name &&
                existing.lore == model.lore &&
                existing.translatedName() == model.translatedName() &&
                existing.translatedLore() == model.translatedLore() &&
                existing.modelData == model.modelData
        }

        for (index in hotbar.indices) {
            if (remaining <= 0) break
            val existing = hotbar[index]
            if (!canMerge(existing)) continue
            val current = existing ?: continue
            val free = maxStack - current.amount
            if (free <= 0) continue
            val added = minOf(free, remaining)
            hotbar[index] = cloneWithAmount(current, current.amount + added)
            remaining -= added
        }
        for (index in hotbar.indices) {
            if (remaining <= 0) break
            if (hotbar[index] != null) continue
            val added = minOf(maxStack, remaining)
            hotbar[index] = cloneWithAmount(model, added)
            remaining -= added
        }
        for (index in main.indices) {
            if (remaining <= 0) break
            val existing = main[index]
            if (!canMerge(existing)) continue
            val current = existing ?: continue
            val free = maxStack - current.amount
            if (free <= 0) continue
            val added = minOf(free, remaining)
            main[index] = cloneWithAmount(current, current.amount + added)
            remaining -= added
        }
        for (index in main.indices) {
            if (remaining <= 0) break
            if (main[index] != null) continue
            val added = minOf(maxStack, remaining)
            main[index] = cloneWithAmount(model, added)
            remaining -= added
        }

        val inserted = requested - remaining
        val dropped = if (remaining > 0 && overflowDrop) remaining else 0
        val discarded = if (remaining > 0 && !overflowDrop) remaining else 0
        return InventoryAddResult(
            requested = requested,
            inserted = inserted,
            dropped = dropped,
            discarded = discarded,
            remaining = 0
        )
    }

}

typealias PlayerInventorySnapshot = PlayerInventory
typealias Translation = Item.Translation
