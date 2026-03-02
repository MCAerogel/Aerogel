package org.macaroon3145.network.item

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

object FurnaceFuelCache {
    private val json = Json { ignoreUnknownKeys = true }
    private val burnSecondsByItemId = ConcurrentHashMap<Int, Double>()
    private val itemTags = ConcurrentHashMap<String, Set<String>>()
    private val loaded = AtomicBoolean(false)

    fun prewarm(itemIdByKey: Map<String, Int>) {
        if (!loaded.compareAndSet(false, true)) return
        runCatching { load(itemIdByKey) }
            .onFailure { loaded.set(false) }
            .getOrThrow()
    }

    fun burnSecondsByItemId(itemId: Int): Double {
        return burnSecondsByItemId[itemId] ?: 0.0
    }

    private fun load(itemIdByKey: Map<String, Int>) {
        val fuelPath = Path.of("src/main/resources/data/minecraft/item/furnace_fuel_values.json")
        val tagRoot = Path.of("src/main/resources/data/minecraft/tags/item")
        if (!Files.isRegularFile(fuelPath) || !Files.isDirectory(tagRoot)) return
        loadItemTagsFromResources(tagRoot, itemIdByKey)

        val root = runCatching {
            Files.newBufferedReader(fuelPath).use { json.parseToJsonElement(it.readText()).jsonObject }
        }.getOrNull() ?: return

        val out = HashMap<Int, Double>()
        val itemsObj = root["items"]?.jsonObject ?: JsonObject(emptyMap())
        for ((itemKey, burnSecondsElement) in itemsObj) {
            val itemId = itemIdByKey[itemKey] ?: continue
            val burnSeconds = burnSecondsElement.jsonPrimitive.content.toDoubleOrNull() ?: continue
            if (burnSeconds <= 0.0) continue
            out[itemId] = burnSeconds
        }

        val tagsObj = root["tags"]?.jsonObject ?: JsonObject(emptyMap())
        for ((tagKey, burnSecondsElement) in tagsObj) {
            val burnSeconds = burnSecondsElement.jsonPrimitive.content.toDoubleOrNull() ?: continue
            if (burnSeconds <= 0.0) continue
            val resolvedItems = itemTags[tagKey.removePrefix("minecraft:")].orEmpty()
            for (itemKey in resolvedItems) {
                val itemId = itemIdByKey[itemKey] ?: continue
                out.putIfAbsent(itemId, burnSeconds)
            }
        }

        burnSecondsByItemId.clear()
        burnSecondsByItemId.putAll(out)
    }

    private fun loadItemTagsFromResources(tagRoot: Path, itemIdByKey: Map<String, Int>) {
        itemTags.clear()
        val rawTagValues = HashMap<String, List<String>>()
        Files.walk(tagRoot).use { paths ->
            val iterator = paths.iterator()
            while (iterator.hasNext()) {
                val path = iterator.next()
                if (!Files.isRegularFile(path) || !path.fileName.toString().endsWith(".json")) continue
                val tagName = tagRoot.relativize(path).toString().replace('\\', '/').removeSuffix(".json")
                val root = runCatching {
                    Files.newBufferedReader(path).use { json.parseToJsonElement(it.readText()).jsonObject }
                }.getOrNull() ?: continue
                val values = root["values"]?.jsonArray?.mapNotNull { it.jsonPrimitive.contentOrNull } ?: continue
                rawTagValues[tagName] = values
            }
        }

        fun resolve(tag: String, visited: MutableSet<String>): Set<String> {
            if (!visited.add(tag)) return emptySet()
            itemTags[tag]?.let { return it }
            val values = rawTagValues[tag] ?: emptyList()
            val out = HashSet<String>()
            for (raw in values) {
                if (raw.startsWith("#minecraft:")) {
                    out.addAll(resolve(raw.removePrefix("#minecraft:"), visited))
                } else if (raw.startsWith("minecraft:") && itemIdByKey.containsKey(raw)) {
                    out.add(raw)
                }
            }
            itemTags[tag] = out
            return out
        }

        for (tag in rawTagValues.keys) {
            resolve(tag, HashSet())
        }
    }

    private val kotlinx.serialization.json.JsonPrimitive.contentOrNull: String?
        get() = runCatching { content }.getOrNull()
}
