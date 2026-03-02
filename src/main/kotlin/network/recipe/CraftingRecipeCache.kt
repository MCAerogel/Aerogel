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
import java.nio.file.Path
import java.nio.file.Files
import java.util.ArrayList
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.stream.Collectors

object CraftingRecipeCache {
    data class ItemStackDef(
        val itemKey: String,
        val count: Int
    )

    data class Ingredient(
        val itemKeys: Set<String>
    ) {
        fun matches(itemKey: String?): Boolean {
            if (itemKey == null) return false
            return itemKey in itemKeys
        }
    }

    data class RecipeMatch(
        val recipe: CraftingRecipe,
        val inputSlotsToConsume: IntArray
    )

    sealed interface CraftingRecipe {
        val id: String
        val result: ItemStackDef
        fun match(gridWidth: Int, gridHeight: Int, gridItems: Array<String?>): IntArray?
    }

    data class ShapedCraftingRecipe(
        override val id: String,
        override val result: ItemStackDef,
        val width: Int,
        val height: Int,
        val pattern: Array<Ingredient?>
    ) : CraftingRecipe {
        override fun match(gridWidth: Int, gridHeight: Int, gridItems: Array<String?>): IntArray? {
            if (width > gridWidth || height > gridHeight) return null
            for (offY in 0..(gridHeight - height)) {
                for (offX in 0..(gridWidth - width)) {
                    val consumed = IntArray(width * height)
                    var consumedIndex = 0
                    var ok = true
                    for (y in 0 until gridHeight) {
                        for (x in 0 until gridWidth) {
                            val gridIndex = y * gridWidth + x
                            val inPattern = x in offX until (offX + width) && y in offY until (offY + height)
                            if (!inPattern) {
                                if (gridItems[gridIndex] != null) {
                                    ok = false
                                    break
                                }
                                continue
                            }
                            val px = x - offX
                            val py = y - offY
                            val ingredient = pattern[py * width + px]
                            val actual = gridItems[gridIndex]
                            if (ingredient == null) {
                                if (actual != null) {
                                    ok = false
                                    break
                                }
                                continue
                            }
                            if (!ingredient.matches(actual)) {
                                ok = false
                                break
                            }
                            consumed[consumedIndex++] = gridIndex
                        }
                        if (!ok) break
                    }
                    if (ok) {
                        return consumed.copyOf(consumedIndex)
                    }
                }
            }
            return null
        }
    }

    data class ShapelessCraftingRecipe(
        override val id: String,
        override val result: ItemStackDef,
        val ingredients: List<Ingredient>
    ) : CraftingRecipe {
        override fun match(gridWidth: Int, gridHeight: Int, gridItems: Array<String?>): IntArray? {
            val presentSlots = ArrayList<Int>(gridWidth * gridHeight)
            val presentItems = ArrayList<String>(gridWidth * gridHeight)
            for (index in gridItems.indices) {
                val key = gridItems[index] ?: continue
                presentSlots.add(index)
                presentItems.add(key)
            }
            if (presentItems.size != ingredients.size) return null
            val used = BooleanArray(presentItems.size)
            val consume = IntArray(ingredients.size)
            for (ingredientIndex in ingredients.indices) {
                val ingredient = ingredients[ingredientIndex]
                var matched = false
                for (i in presentItems.indices) {
                    if (used[i]) continue
                    if (!ingredient.matches(presentItems[i])) continue
                    used[i] = true
                    consume[ingredientIndex] = presentSlots[i]
                    matched = true
                    break
                }
                if (!matched) return null
            }
            return consume
        }
    }

    private val json = Json { ignoreUnknownKeys = true }
    private val recipesByGridSize = ConcurrentHashMap<Int, List<CraftingRecipe>>()
    private val itemTags = ConcurrentHashMap<String, Set<String>>()
    private val loaded = AtomicBoolean(false)

    fun prewarm(itemKeyToId: Map<String, Int>) {
        if (!loaded.compareAndSet(false, true)) return
        val loadedRecipes = runCatching { loadFromResources(itemKeyToId) }
            .onFailure { loaded.set(false) }
            .getOrThrow()
        recipesByGridSize[4] = loadedRecipes.filter { recipeFits(it, 2, 2) }
        recipesByGridSize[9] = loadedRecipes.filter { recipeFits(it, 3, 3) }
    }

    fun findFirstMatch(
        gridWidth: Int,
        gridHeight: Int,
        gridItemKeys: Array<String?>
    ): RecipeMatch? {
        val recipes = recipesByGridSize[gridWidth * gridHeight] ?: return null
        for (recipe in recipes) {
            val consumed = recipe.match(gridWidth, gridHeight, gridItemKeys) ?: continue
            return RecipeMatch(recipe, consumed)
        }
        return null
    }

    private fun recipeFits(recipe: CraftingRecipe, gridWidth: Int, gridHeight: Int): Boolean {
        return when (recipe) {
            is ShapedCraftingRecipe -> recipe.width <= gridWidth && recipe.height <= gridHeight
            is ShapelessCraftingRecipe -> recipe.ingredients.size <= gridWidth * gridHeight
        }
    }

    private fun loadFromResources(itemKeyToId: Map<String, Int>): List<CraftingRecipe> {
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
                parseCraftingRecipe(id, root)
            }
            .filter { it != null }
            .map { it!! }
            .sorted(compareBy(String.CASE_INSENSITIVE_ORDER) { it.id })
            .collect(Collectors.toList())
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

    private fun parseCraftingRecipe(id: String, root: JsonObject): CraftingRecipe? {
        val type = root["type"]?.jsonPrimitive?.content ?: return null
        return when (type) {
            "minecraft:crafting_shaped" -> parseShaped(id, root)
            "minecraft:crafting_shapeless" -> parseShapeless(id, root)
            else -> null
        }
    }

    private fun parseShaped(id: String, root: JsonObject): CraftingRecipe? {
        val patternRows = root["pattern"]?.jsonArray?.mapNotNull { it.jsonPrimitive.contentOrNull } ?: return null
        if (patternRows.isEmpty()) return null
        val width = patternRows.maxOfOrNull { it.length } ?: return null
        val height = patternRows.size
        if (width <= 0 || height <= 0) return null
        val key = root["key"]?.jsonObject ?: return null
        val ingredientByChar = HashMap<Char, Ingredient?>()
        for ((symbol, element) in key) {
            if (symbol.length != 1) continue
            ingredientByChar[symbol[0]] = parseIngredient(element)
        }
        val pattern = arrayOfNulls<Ingredient>(width * height)
        for (y in 0 until height) {
            val row = patternRows[y]
            for (x in 0 until width) {
                val c = if (x < row.length) row[x] else ' '
                pattern[y * width + x] = if (c == ' ') null else ingredientByChar[c] ?: return null
            }
        }
        val result = parseResult(root["result"]) ?: return null
        return ShapedCraftingRecipe(id = id, result = result, width = width, height = height, pattern = pattern)
    }

    private fun parseShapeless(id: String, root: JsonObject): CraftingRecipe? {
        val ingredientsRaw = root["ingredients"]?.jsonArray ?: return null
        val ingredients = ArrayList<Ingredient>(ingredientsRaw.size)
        for (element in ingredientsRaw) {
            val ingredient = parseIngredient(element) ?: return null
            ingredients.add(ingredient)
        }
        if (ingredients.isEmpty()) return null
        val result = parseResult(root["result"]) ?: return null
        return ShapelessCraftingRecipe(id = id, result = result, ingredients = ingredients)
    }

    private fun parseIngredient(element: JsonElement): Ingredient? {
        val out = HashSet<String>()
        fun collectToken(raw: String) {
            when {
                raw.startsWith("#minecraft:") -> {
                    out.addAll(itemTags[raw.removePrefix("#minecraft:")].orEmpty())
                }
                raw.startsWith("minecraft:") -> {
                    out.add(raw)
                }
            }
        }
        fun collect(e: JsonElement) {
            when (e) {
                is JsonObject -> {
                    val item = e["item"]?.jsonPrimitive?.contentOrNull
                    if (item != null && item.startsWith("minecraft:")) {
                        out.add(item)
                    }
                    val tag = e["tag"]?.jsonPrimitive?.contentOrNull
                    if (tag != null) {
                        val key = tag.removePrefix("minecraft:")
                        out.addAll(itemTags[key].orEmpty())
                    }
                }
                is JsonPrimitive -> {
                    val raw = e.contentOrNull ?: return
                    collectToken(raw)
                }
                is JsonArray -> {
                    for (child in e) collect(child)
                }
                else -> {
                    // no-op
                }
            }
        }
        collect(element)
        if (out.isEmpty()) return null
        return Ingredient(out)
    }

    private fun parseResult(element: JsonElement?): ItemStackDef? {
        val obj = element?.jsonObject ?: return null
        val item = obj["id"]?.jsonPrimitive?.contentOrNull
            ?: obj["item"]?.jsonPrimitive?.contentOrNull
            ?: return null
        if (!item.startsWith("minecraft:")) return null
        val count = obj["count"]?.jsonPrimitive?.intOrNull ?: 1
        return ItemStackDef(item, count.coerceAtLeast(1))
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
