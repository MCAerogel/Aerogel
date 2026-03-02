package org.macaroon3145.network.codec

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive

object BlockStateRegistry {
    data class ParsedState(
        val blockKey: String,
        val properties: Map<String, String>
    )

    private val json = Json { ignoreUnknownKeys = true }

    private val itemToBlock: Map<Int, String> by lazy {
        val root = loadRoot()
        val obj = root["itemToBlock"]?.jsonObject ?: return@lazy emptyMap()
        val out = HashMap<Int, String>(obj.size)
        for ((itemIdText, blockKeyElement) in obj) {
            val itemId = itemIdText.toIntOrNull() ?: continue
            out[itemId] = blockKeyElement.jsonPrimitive.content
        }
        out
    }

    private val blockToItem: Map<String, Int> by lazy {
        val out = HashMap<String, Int>()
        for ((itemId, blockKey) in itemToBlock) {
            // Prefer first-seen mapping for stability.
            out.putIfAbsent(blockKey, itemId)
        }
        out
    }

    private val defaultByBlock: Map<String, Int> by lazy {
        val root = loadRoot()
        val obj = root["defaultByBlock"]?.jsonObject ?: return@lazy emptyMap()
        val out = HashMap<String, Int>(obj.size)
        for ((blockKey, stateIdElement) in obj) {
            val stateId = stateIdElement.jsonPrimitive.intOrNull ?: continue
            out[blockKey] = stateId
        }
        out
    }

    private val stateToId: Map<String, Int> by lazy {
        val root = loadRoot()
        val obj = root["stateToId"]?.jsonObject ?: return@lazy emptyMap()
        val out = HashMap<String, Int>(obj.size)
        for ((stateKey, stateIdElement) in obj) {
            val stateId = stateIdElement.jsonPrimitive.intOrNull ?: continue
            out[stateKey] = stateId
        }
        out
    }

    private val stateIdToParsed: Map<Int, ParsedState> by lazy {
        val out = HashMap<Int, ParsedState>(stateToId.size + defaultByBlock.size)
        for ((stateKey, id) in stateToId) {
            out[id] = parseStateKey(stateKey)
        }
        for ((blockKey, id) in defaultByBlock) {
            out.putIfAbsent(id, ParsedState(blockKey, emptyMap()))
        }
        out
    }

    private val verticalHalfCounterpartByStateId: Map<Int, Int> by lazy {
        val out = HashMap<Int, Int>()
        for ((stateId, parsed) in stateIdToParsed) {
            val half = parsed.properties["half"] ?: continue
            val opposite = when (half) {
                "lower" -> "upper"
                "upper" -> "lower"
                else -> null
            } ?: continue
            val props = HashMap(parsed.properties)
            props["half"] = opposite
            val counterpart = stateId(parsed.blockKey, props) ?: continue
            out[stateId] = counterpart
        }
        out
    }

    private val blockPropertyValues: Map<String, Map<String, Set<String>>> by lazy {
        val valuesByBlock = HashMap<String, HashMap<String, MutableSet<String>>>()
        for (parsed in stateIdToParsed.values) {
            val byProp = valuesByBlock.computeIfAbsent(parsed.blockKey) { HashMap() }
            for ((name, value) in parsed.properties) {
                byProp.computeIfAbsent(name) { HashSet() }.add(value)
            }
        }
        val out = HashMap<String, Map<String, Set<String>>>(valuesByBlock.size)
        for ((block, map) in valuesByBlock) {
            out[block] = map.mapValues { it.value.toSet() }
        }
        out
    }

    fun blockForItem(itemId: Int): String? = itemToBlock[itemId]

    fun itemIdForBlock(blockKey: String): Int? = blockToItem[blockKey]

    fun defaultStateId(blockKey: String): Int? = defaultByBlock[blockKey]

    fun stateId(blockKey: String, properties: Map<String, String>): Int? {
        if (properties.isEmpty()) return defaultByBlock[blockKey]
        val canonical = properties.entries
            .sortedBy { it.key }
            .joinToString(",") { "${it.key}=${it.value}" }
        return stateToId["$blockKey[$canonical]"]
    }

    fun parsedState(stateId: Int): ParsedState? = stateIdToParsed[stateId]

    fun allStateIds(): IntArray {
        val out = IntArray(stateIdToParsed.size)
        var i = 0
        for (stateId in stateIdToParsed.keys) {
            out[i++] = stateId
        }
        return out
    }

    fun verticalHalfCounterpartStateId(stateId: Int): Int? = verticalHalfCounterpartByStateId[stateId]

    fun propertyValues(blockKey: String, property: String): Set<String> {
        return blockPropertyValues[blockKey]?.get(property) ?: emptySet()
    }

    fun prewarm() {
        // Shift lazy JSON/materialization cost to startup.
        itemToBlock.size
        blockToItem.size
        defaultByBlock.size
        stateIdToParsed.size
        verticalHalfCounterpartByStateId.size
    }

    private fun parseStateKey(stateKey: String): ParsedState {
        val bracket = stateKey.indexOf('[')
        if (bracket < 0 || !stateKey.endsWith("]")) {
            return ParsedState(stateKey, emptyMap())
        }
        val blockKey = stateKey.substring(0, bracket)
        val rawProps = stateKey.substring(bracket + 1, stateKey.length - 1)
        if (rawProps.isEmpty()) return ParsedState(blockKey, emptyMap())
        val props = HashMap<String, String>()
        for (token in rawProps.split(',')) {
            val idx = token.indexOf('=')
            if (idx <= 0 || idx >= token.length - 1) continue
            props[token.substring(0, idx)] = token.substring(idx + 1)
        }
        return ParsedState(blockKey, props)
    }

    private fun loadRoot() = BlockStateRegistry::class.java.classLoader
        .getResourceAsStream("block-state-map-1.21.11.json")
        ?.bufferedReader()
        ?.use { json.parseToJsonElement(it.readText()) }
        ?.jsonObject
        ?: error("Missing block-state-map-1.21.11.json resource")
}
