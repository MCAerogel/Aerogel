package org.macaroon3145.network.codec

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.util.concurrent.ConcurrentHashMap

object ItemBlockStateRegistry {
    private val json = Json { ignoreUnknownKeys = true }

    private val staticItemToState: Map<Int, Int> by lazy {
        val resource = ItemBlockStateRegistry::class.java.classLoader
            .getResourceAsStream("item-to-blockstate-1.21.11.json")
            ?: error("Missing item-to-blockstate-1.21.11.json resource")

        val root = resource.bufferedReader().use { json.parseToJsonElement(it.readText()) }.jsonObject
        val entries = root["entries"]?.jsonObject ?: return@lazy emptyMap()
        val map = HashMap<Int, Int>(entries.size)
        for ((itemIdText, stateIdElement) in entries) {
            val itemId = itemIdText.toIntOrNull() ?: continue
            val stateId = stateIdElement.jsonPrimitive.intOrNull ?: continue
            map[itemId] = stateId
        }
        map
    }

    private val staticStateToItem: Map<Int, Int> by lazy {
        val out = HashMap<Int, Int>(staticItemToState.size)
        for ((itemId, stateId) in staticItemToState) {
            out.putIfAbsent(stateId, itemId)
        }
        out
    }

    private val learnedItemToState = ConcurrentHashMap<Int, Int>()
    private val learnedStateToItem = ConcurrentHashMap<Int, Int>()

    fun blockStateIdForItem(itemId: Int): Int? {
        if (itemId < 0) return null
        return staticItemToState[itemId] ?: learnedItemToState[itemId]
    }

    fun itemIdForBlockState(stateId: Int): Int? {
        if (stateId <= 0) return null
        return staticStateToItem[stateId] ?: learnedStateToItem[stateId]
    }

    fun learn(itemId: Int, stateId: Int) {
        if (itemId < 0 || stateId <= 0) return
        // Never override canonical static mappings.
        if (staticItemToState.containsKey(itemId) || staticStateToItem.containsKey(stateId)) return
        learnedItemToState[itemId] = stateId
        learnedStateToItem[stateId] = itemId
    }

    fun prewarm() {
        // Force static mapping parse/build at startup instead of first pick-block.
        staticItemToState.size
        staticStateToItem.size
    }
}
