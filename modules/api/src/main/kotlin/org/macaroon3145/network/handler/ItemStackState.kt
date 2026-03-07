package org.macaroon3145.network.handler

import org.macaroon3145.api.entity.Item
import org.macaroon3145.api.type.ItemType

data class ItemTextMeta(
    val expectedItemId: Int,
    val name: String?,
    val lore: List<String>,
    val translatedNameKey: String? = null,
    val translatedNameArgs: List<String> = emptyList(),
    val translatedLore: List<Pair<String, List<String>>> = emptyList()
) {
    fun hasTranslations(): Boolean {
        return translatedNameKey != null || translatedLore.isNotEmpty()
    }
}

data class ChestState(
    val itemIds: IntArray,
    val itemCounts: IntArray,
    val shulkerContents: Array<ChestState?> = arrayOfNulls(27),
    val textMeta: Array<ItemTextMeta?> = arrayOfNulls(27)
)

data class ItemStackState private constructor(
    var itemId: Int,
    var count: Int,
    var shulkerContents: ChestState? = null,
    var customName: String? = null,
    var customLore: List<String> = emptyList(),
    var translatedNameKey: String? = null,
    var translatedNameArgs: List<String> = emptyList(),
    var translatedLore: List<Pair<String, List<String>>> = emptyList()
) {
    fun isEmpty(): Boolean = itemId < 0 || count <= 0

    fun toItem(): Item {
        val item = Item(
            id = itemId,
            type = ItemType.fromId(itemId),
            amount = count,
            name = customName,
            lore = customLore.toList()
        )
        val translatedNameValue = translatedNameKey
        if (!translatedNameValue.isNullOrBlank()) {
            item.trName(translatedNameValue, *translatedNameArgs.toTypedArray())
        }
        if (translatedLore.isNotEmpty()) {
            item.trLore(
                translatedLore.map { (key, args) ->
                    Item.Translation(key = key, args = args)
                }
            )
        }
        return item
    }

    companion object {
        fun of(
            itemId: Int,
            count: Int,
            shulkerContents: ChestState? = null,
            customName: String? = null,
            customLore: List<String> = emptyList(),
            translatedNameKey: String? = null,
            translatedNameArgs: List<String> = emptyList(),
            translatedLore: List<Pair<String, List<String>>> = emptyList()
        ): ItemStackState {
            return ItemStackState(
                itemId = itemId,
                count = count,
                shulkerContents = shulkerContents,
                customName = customName,
                customLore = customLore,
                translatedNameKey = translatedNameKey,
                translatedNameArgs = translatedNameArgs,
                translatedLore = translatedLore
            )
        }

        fun empty(): ItemStackState = of(itemId = -1, count = 0, shulkerContents = null)
    }
}
