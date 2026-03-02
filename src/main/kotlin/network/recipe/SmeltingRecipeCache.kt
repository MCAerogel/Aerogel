package org.macaroon3145.network.recipe

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.nio.file.Files
import java.nio.file.Path
import java.util.ArrayList
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.stream.Collectors

object SmeltingRecipeCache {
    data class SmeltingRecipe(
        val id: String,
        val ingredientItemKeys: Set<String>,
        val resultItemKey: String,
        val resultCount: Int,
        val cookSeconds: Double
    )

    private val json = Json { ignoreUnknownKeys = true }
    private val itemTags = ConcurrentHashMap<String, Set<String>>()
    private val recipeByIngredientItemKey = ConcurrentHashMap<String, SmeltingRecipe>()
    private val loaded = AtomicBoolean(false)

    fun prewarm(itemKeyToId: Map<String, Int>) {
        if (!loaded.compareAndSet(false, true)) return
        val recipes = runCatching { loadFromResources(itemKeyToId) }
            .onFailure { loaded.set(false) }
            .getOrThrow()
        recipeByIngredientItemKey.clear()
        for (recipe in recipes) {
            for (itemKey in recipe.ingredientItemKeys) {
                recipeByIngredientItemKey.putIfAbsent(itemKey, recipe)
            }
        }
    }

    fun recipeByInputItemId(itemId: Int, itemKeyById: List<String>): SmeltingRecipe? {
        if (itemId !in itemKeyById.indices) return null
        val itemKey = itemKeyById[itemId]
        if (itemKey.isEmpty()) return null
        return recipeByIngredientItemKey[itemKey]
    }

    private fun loadFromResources(itemKeyToId: Map<String, Int>): List<SmeltingRecipe> {
        val recipeRoot = Path.of("src/main/resources/data/minecraft/recipe")
        val tagRoot = Path.of("src/main/resources/data/minecraft/tags/item")
        if (!Files.isDirectory(recipeRoot) || !Files.isDirectory(tagRoot)) return emptyList()
        loadItemTagsFromResources(tagRoot, itemKeyToId)
        val recipeFiles = listJsonFiles(recipeRoot)
        if (recipeFiles.isEmpty()) return emptyList()
        return recipeFiles.parallelStream()
            .map { path ->
                val relative = recipeRoot.relativize(path).toString().replace('\\', '/')
                val id = relative.removeSuffix(".json")
                val root = parseJsonObject(path) ?: return@map null
                parseSmeltingRecipe(id, root)
            }
            .filter { it != null }
            .map { it!! }
            .sorted(compareBy(String.CASE_INSENSITIVE_ORDER) { it.id })
            .collect(Collectors.toList())
    }

    private fun parseSmeltingRecipe(id: String, root: JsonObject): SmeltingRecipe? {
        val type = root["type"]?.jsonPrimitive?.content ?: return null
        if (type != "minecraft:smelting") return null

        val ingredientItemKeys = parseIngredient(root["ingredient"] ?: return null)
        if (ingredientItemKeys.isEmpty()) return null

        val resultObj = root["result"]?.jsonObject ?: return null
        val resultItem = resultObj["id"]?.jsonPrimitive?.contentOrNull
            ?: resultObj["item"]?.jsonPrimitive?.contentOrNull
            ?: return null
        if (!resultItem.startsWith("minecraft:")) return null
        val resultCount = (resultObj["count"]?.jsonPrimitive?.intOrNull ?: 1).coerceAtLeast(1)
        val cookingTicks = (root["cookingtime"]?.jsonPrimitive?.intOrNull ?: 200).coerceAtLeast(1)
        val cookSeconds = cookingTicks / 20.0

        return SmeltingRecipe(
            id = id,
            ingredientItemKeys = ingredientItemKeys,
            resultItemKey = resultItem,
            resultCount = resultCount,
            cookSeconds = cookSeconds
        )
    }

    private fun loadItemTagsFromResources(tagRoot: Path, itemKeyToId: Map<String, Int>) {
        itemTags.clear()
        val tagFiles = listJsonFiles(tagRoot)
        val rawTagValues = ConcurrentHashMap<String, List<String>>(tagFiles.size)
        tagFiles.parallelStream().forEach { path ->
            val tagName = tagRoot.relativize(path).toString().replace('\\', '/').removeSuffix(".json")
            val root = parseJsonObject(path) ?: return@forEach
            val values = root["values"]?.jsonArray?.mapNotNull { it.jsonPrimitive.contentOrNull } ?: return@forEach
            rawTagValues[tagName] = values
        }

        fun resolve(tag: String, visited: MutableSet<String>): Set<String> {
            if (!visited.add(tag)) return emptySet()
            val existing = itemTags[tag]
            if (existing != null) return existing
            val values = rawTagValues[tag] ?: emptyList()
            val out = HashSet<String>()
            for (raw in values) {
                if (raw.startsWith("#minecraft:")) {
                    out.addAll(resolve(raw.removePrefix("#minecraft:"), visited))
                } else if (raw.startsWith("minecraft:") && itemKeyToId.containsKey(raw)) {
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

    private fun parseIngredient(element: JsonElement): Set<String> {
        val out = HashSet<String>()
        fun collectToken(raw: String) {
            when {
                raw.startsWith("#minecraft:") -> out.addAll(itemTags[raw.removePrefix("#minecraft:")].orEmpty())
                raw.startsWith("minecraft:") -> out.add(raw)
            }
        }
        fun collect(e: JsonElement) {
            when (e) {
                is JsonObject -> {
                    e["item"]?.jsonPrimitive?.contentOrNull?.let { collectToken(it) }
                    e["tag"]?.jsonPrimitive?.contentOrNull?.let { collectToken("minecraft:${it.removePrefix("minecraft:")}") }
                }
                is JsonPrimitive -> e.contentOrNull?.let { collectToken(it) }
                is JsonArray -> for (child in e) collect(child)
                else -> {
                    // no-op
                }
            }
        }
        collect(element)
        return out
    }

    private fun listJsonFiles(root: Path): List<Path> {
        Files.walk(root).use { paths ->
            val iterator = paths.iterator()
            val out = ArrayList<Path>()
            while (iterator.hasNext()) {
                val path = iterator.next()
                if (!Files.isRegularFile(path) || !path.fileName.toString().endsWith(".json")) continue
                out.add(path)
            }
            return out
        }
    }

    private fun parseJsonObject(path: Path): JsonObject? {
        return runCatching {
            Files.newBufferedReader(path).use { json.parseToJsonElement(it.readText()).jsonObject }
        }.getOrNull()
    }

    private val JsonPrimitive.contentOrNull: String?
        get() = runCatching { content }.getOrNull()
}
