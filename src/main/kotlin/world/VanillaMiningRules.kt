package org.macaroon3145.world

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.macaroon3145.network.codec.BlockStateRegistry
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ThreadLocalRandom
import kotlin.math.roundToInt

data class VanillaDrop(val itemId: Int, val count: Int)

object VanillaMiningRules {
    private enum class ToolType {
        PICKAXE,
        AXE,
        SHOVEL,
        HOE,
        SWORD,
        SHEARS
    }

    private data class LootContext(
        val world: World,
        val blockKey: String,
        val blockStateProperties: Map<String, String>,
        val x: Int,
        val y: Int,
        val z: Int,
        val heldToolKey: String?
    )

    private data class HarvestKey(
        val blockKey: String,
        val heldItemId: Int
    )

    private val json = Json { ignoreUnknownKeys = true }
    private val itemKeysById by lazy {
        val resource = VanillaMiningRules::class.java.classLoader
            .getResourceAsStream("item-id-map-1.21.11.json")
            ?: error("Missing item-id-map-1.21.11.json resource")
        val root = resource.bufferedReader().use { json.parseToJsonElement(it.readText()) }.jsonObject
        val entries = root["entries"]?.jsonObject ?: return@lazy emptyList()
        val maxId = entries.values.maxOfOrNull { it.jsonPrimitive.intOrNull ?: -1 } ?: -1
        if (maxId < 0) return@lazy emptyList()
        val out = MutableList(maxId + 1) { "" }
        for ((itemKey, itemIdElement) in entries) {
            val itemId = itemIdElement.jsonPrimitive.intOrNull ?: continue
            if (itemId !in out.indices) continue
            out[itemId] = itemKey
        }
        out
    }
    private val itemIdsByKey by lazy {
        val out = HashMap<String, Int>(itemKeysById.size)
        for ((index, entry) in itemKeysById.withIndex()) {
            if (entry.isEmpty()) continue
            out[entry] = index
        }
        out
    }
    private val resourceTextCache = ConcurrentHashMap<String, String?>()
    private val jsonObjectCache = ConcurrentHashMap<String, JsonObject>()
    private val missingJsonObjectPaths = ConcurrentHashMap.newKeySet<String>()
    private val blockTagCache = ConcurrentHashMap<String, Set<String>>()
    private val itemTagCache = ConcurrentHashMap<String, Set<String>>()
    private val lootTableCache = ConcurrentHashMap<String, JsonObject?>()
    private val canHarvestCache = ConcurrentHashMap<HarvestKey, Boolean>()
    private val toolTypeByItemIdCache = ConcurrentHashMap<Int, ToolType?>()
    private val incorrectTagByItemIdCache = ConcurrentHashMap<Int, String?>()

    private val incorrectToolTags = listOf(
        "incorrect_for_wooden_tool",
        "incorrect_for_gold_tool",
        "incorrect_for_stone_tool",
        "incorrect_for_copper_tool",
        "incorrect_for_iron_tool",
        "incorrect_for_diamond_tool",
        "incorrect_for_netherite_tool"
    )
    private val commonFirstBreakBlockKeys = listOf(
        "minecraft:short_grass",
        "minecraft:tall_grass",
        "minecraft:fern",
        "minecraft:large_fern",
        "minecraft:wildflowers",
        "minecraft:rose_bush",
        "minecraft:lilac",
        "minecraft:peony",
        "minecraft:sunflower",
        "minecraft:pitcher_plant",
        "minecraft:torchflower",
        "minecraft:dandelion",
        "minecraft:poppy"
    )

    fun prewarm() {
        // Shift first-break lazy costs to startup.
        itemKeysById.size
        itemIdsByKey.size
        val toolTags = listOf("pickaxes", "axes", "shovels", "hoes", "swords")
        for (tag in toolTags) {
            itemTag(tag).size
        }
        val blockTags = listOf(
            "needs_stone_tool",
            "needs_iron_tool",
            "needs_diamond_tool",
            "mineable/pickaxe",
            "mineable/axe",
            "mineable/shovel",
            "mineable/hoe"
        ) + incorrectToolTags
        for (tag in blockTags) {
            blockTag(tag).size
        }
        for (blockKey in commonFirstBreakBlockKeys) {
            lootTableForBlock(blockKey)
        }

        // Eagerly load loot tables for all known block keys to avoid first-hit stalls.
        val allBlockKeys = LinkedHashSet<String>()
        for (stateId in BlockStateRegistry.allStateIds()) {
            val blockKey = BlockStateRegistry.parsedState(stateId)?.blockKey ?: continue
            allBlockKeys.add(blockKey)
        }
        for (blockKey in allBlockKeys) {
            lootTableForBlock(blockKey)
            canHarvestCache.putIfAbsent(HarvestKey(blockKey, -1), canHarvestDropsNoCache(blockKey, -1))
        }

        // Warm common tool classifications/harvest checks once.
        val toolItemIds = LinkedHashSet<Int>()
        for (itemKey in itemTag("pickaxes")) itemIdsByKey[itemKey]?.let { toolItemIds.add(it) }
        for (itemKey in itemTag("axes")) itemIdsByKey[itemKey]?.let { toolItemIds.add(it) }
        for (itemKey in itemTag("shovels")) itemIdsByKey[itemKey]?.let { toolItemIds.add(it) }
        for (itemKey in itemTag("hoes")) itemIdsByKey[itemKey]?.let { toolItemIds.add(it) }
        for (itemKey in itemTag("swords")) itemIdsByKey[itemKey]?.let { toolItemIds.add(it) }
        itemIdsByKey["minecraft:shears"]?.let { toolItemIds.add(it) }
        for (toolItemId in toolItemIds) {
            classifyToolTypeCached(toolItemId)
            incorrectTagForItemId(toolItemId)
            for (blockKey in allBlockKeys) {
                canHarvestCache.putIfAbsent(HarvestKey(blockKey, toolItemId), canHarvestDropsNoCache(blockKey, toolItemId))
            }
        }
    }

    fun resolveDrops(world: World, stateId: Int, heldItemId: Int, x: Int, y: Int, z: Int): List<VanillaDrop> {
        val parsed = BlockStateRegistry.parsedState(stateId) ?: return emptyList()
        if (!canHarvestDrops(parsed.blockKey, heldItemId)) return emptyList()

        val lootTable = lootTableForBlock(parsed.blockKey)
        val context = LootContext(
            world = world,
            blockKey = parsed.blockKey,
            blockStateProperties = parsed.properties,
            x = x,
            y = y,
            z = z,
            heldToolKey = itemKey(heldItemId)
        )
        val resolved = if (lootTable != null) {
            evaluateLootTable(lootTable, context)
        } else {
            fallbackSelfDrop(parsed.blockKey)
        }
        return resolved.filter { it.itemId >= 0 && it.count > 0 }
    }

    private fun fallbackSelfDrop(blockKey: String): List<VanillaDrop> {
        val itemId = BlockStateRegistry.itemIdForBlock(blockKey) ?: itemIdsByKey[blockKey] ?: return emptyList()
        return listOf(VanillaDrop(itemId, 1))
    }

    private fun itemKey(itemId: Int): String? {
        return itemKeysById.getOrNull(itemId)
    }

    private fun canHarvestDrops(blockKey: String, heldItemId: Int): Boolean {
        return canHarvestCache.computeIfAbsent(HarvestKey(blockKey, heldItemId)) {
            canHarvestDropsNoCache(blockKey, heldItemId)
        }
    }

    private fun canHarvestDropsNoCache(blockKey: String, heldItemId: Int): Boolean {
        if (blockTag("mineable/pickaxe").contains(blockKey)) {
            val toolType = classifyToolTypeCached(heldItemId) ?: return false
            if (toolType != ToolType.PICKAXE) return false
        }
        if (!requiresCorrectToolForDrops(blockKey)) return true
        val toolType = classifyToolTypeCached(heldItemId) ?: return false
        if (!toolMatchesMineableTag(blockKey, toolType)) return false

        val incorrectTag = incorrectTagForItemId(heldItemId)
        if (incorrectTag != null && blockTag(incorrectTag).contains(blockKey)) {
            return false
        }
        return true
    }

    private fun classifyToolTypeCached(heldItemId: Int): ToolType? {
        return toolTypeByItemIdCache.computeIfAbsent(heldItemId) { itemId ->
            val toolKey = itemKey(itemId) ?: return@computeIfAbsent null
            classifyToolType(toolKey)
        }
    }

    private fun incorrectTagForItemId(heldItemId: Int): String? {
        return incorrectTagByItemIdCache.computeIfAbsent(heldItemId) { itemId ->
            val toolKey = itemKey(itemId) ?: return@computeIfAbsent null
            incorrectTagForTool(toolKey)
        }
    }

    private fun requiresCorrectToolForDrops(blockKey: String): Boolean {
        if (blockTag("needs_stone_tool").contains(blockKey)) return true
        if (blockTag("needs_iron_tool").contains(blockKey)) return true
        if (blockTag("needs_diamond_tool").contains(blockKey)) return true
        return incorrectToolTags.any { blockTag(it).contains(blockKey) }
    }

    private fun toolMatchesMineableTag(blockKey: String, toolType: ToolType): Boolean {
        if (blockTag("mineable/pickaxe").contains(blockKey)) return toolType == ToolType.PICKAXE
        if (blockTag("mineable/axe").contains(blockKey)) return toolType == ToolType.AXE
        if (blockTag("mineable/shovel").contains(blockKey)) return toolType == ToolType.SHOVEL
        if (blockTag("mineable/hoe").contains(blockKey)) return toolType == ToolType.HOE
        return true
    }

    private fun classifyToolType(toolKey: String): ToolType? {
        return when {
            toolKey == "minecraft:shears" -> ToolType.SHEARS
            itemTag("pickaxes").contains(toolKey) -> ToolType.PICKAXE
            itemTag("axes").contains(toolKey) -> ToolType.AXE
            itemTag("shovels").contains(toolKey) -> ToolType.SHOVEL
            itemTag("hoes").contains(toolKey) -> ToolType.HOE
            itemTag("swords").contains(toolKey) -> ToolType.SWORD
            else -> null
        }
    }

    private fun incorrectTagForTool(toolKey: String): String? {
        return when {
            toolKey.startsWith("minecraft:wooden_") -> "incorrect_for_wooden_tool"
            toolKey.startsWith("minecraft:golden_") -> "incorrect_for_gold_tool"
            toolKey.startsWith("minecraft:stone_") -> "incorrect_for_stone_tool"
            toolKey.startsWith("minecraft:copper_") -> "incorrect_for_copper_tool"
            toolKey.startsWith("minecraft:iron_") -> "incorrect_for_iron_tool"
            toolKey.startsWith("minecraft:diamond_") -> "incorrect_for_diamond_tool"
            toolKey.startsWith("minecraft:netherite_") -> "incorrect_for_netherite_tool"
            else -> null
        }
    }

    private fun lootTableForBlock(blockKey: String): JsonObject? {
        return lootTableCache.computeIfAbsent(blockKey) { key ->
            readJsonObject("data/minecraft/loot_table/blocks/${key.substringAfter("minecraft:")}.json")
        }
    }

    private fun blockTag(tagName: String): Set<String> {
        return blockTagCache.computeIfAbsent(tagName) { loadTag("block", tagName) }
    }

    private fun itemTag(tagName: String): Set<String> {
        return itemTagCache.computeIfAbsent(tagName) { loadTag("item", tagName) }
    }

    private fun loadTag(kind: String, tagName: String): Set<String> {
        return resolveTag(kind, tagName, HashSet())
    }

    private fun resolveTag(kind: String, tagName: String, visited: MutableSet<String>): Set<String> {
        val visitKey = "$kind:$tagName"
        if (!visited.add(visitKey)) return emptySet()
        val root = readJsonObject("data/minecraft/tags/$kind/$tagName.json") ?: return emptySet()
        val out = LinkedHashSet<String>()
        for (element in root["values"]?.jsonArray.orEmpty()) {
            val raw = element.jsonPrimitive.content
            if (raw.startsWith("#")) {
                out.addAll(resolveTag(kind, raw.substring(1).substringAfter("minecraft:"), visited))
            } else {
                out.add(if (raw.startsWith("minecraft:")) raw else "minecraft:$raw")
            }
        }
        return out
    }

    private fun readJsonObject(path: String): JsonObject? {
        jsonObjectCache[path]?.let { return it }
        if (missingJsonObjectPaths.contains(path)) return null
        val text = readResourceText(path) ?: run {
            missingJsonObjectPaths.add(path)
            return null
        }
        val parsed = runCatching { json.parseToJsonElement(text).jsonObject }.getOrNull() ?: run {
            missingJsonObjectPaths.add(path)
            return null
        }
        jsonObjectCache[path] = parsed
        return parsed
    }

    private fun readResourceText(path: String): String? {
        return resourceTextCache.computeIfAbsent(path) { target ->
            val stream = VanillaMiningRules::class.java.classLoader.getResourceAsStream(target) ?: return@computeIfAbsent null
            stream.bufferedReader().use { it.readText() }
        }
    }

    private fun evaluateLootTable(table: JsonObject, context: LootContext): List<VanillaDrop> {
        val out = ArrayList<VanillaDrop>()
        for (poolElement in table["pools"]?.jsonArray.orEmpty()) {
            val pool = poolElement.jsonObject
            if (!evaluateConditions(pool["conditions"]?.jsonArray, context)) continue
            val rolls = evaluateRolls(pool["rolls"])
            repeat(rolls) {
                val drops = evaluatePoolEntries(pool["entries"]?.jsonArray, context)
                val applied = applyFunctions(drops, pool["functions"]?.jsonArray, context)
                out.addAll(applied)
            }
        }
        return out
    }

    private fun evaluateRolls(element: JsonElement?): Int {
        if (element == null) return 1
        return element.jsonPrimitive.content.toDoubleOrNull()?.roundToInt()?.coerceAtLeast(0) ?: 1
    }

    private fun evaluatePoolEntries(entries: JsonArray?, context: LootContext): List<VanillaDrop> {
        val list = entries ?: return emptyList()
        if (list.isEmpty()) return emptyList()
        if (list.size == 1) return evaluateEntry(list[0].jsonObject, context)

        val out = ArrayList<VanillaDrop>()
        for (entry in list) {
            out.addAll(evaluateEntry(entry.jsonObject, context))
        }
        return out
    }

    private fun evaluateEntry(entry: JsonObject, context: LootContext): List<VanillaDrop> {
        if (!evaluateConditions(entry["conditions"]?.jsonArray, context)) return emptyList()
        return when (entry["type"]?.jsonPrimitive?.content) {
            "minecraft:item" -> {
                val itemKey = entry["name"]?.jsonPrimitive?.content ?: return emptyList()
                val itemId = itemIdsByKey[itemKey] ?: return emptyList()
                val drops = applyFunctions(listOf(VanillaDrop(itemId, 1)), entry["functions"]?.jsonArray, context)
                drops.filter { it.count > 0 }
            }
            "minecraft:alternatives" -> {
                for (child in entry["children"]?.jsonArray.orEmpty()) {
                    val resolved = evaluateEntry(child.jsonObject, context)
                    if (resolved.isNotEmpty()) return resolved
                }
                emptyList()
            }
            "minecraft:sequence" -> {
                val out = ArrayList<VanillaDrop>()
                for (child in entry["children"]?.jsonArray.orEmpty()) {
                    val resolved = evaluateEntry(child.jsonObject, context)
                    if (resolved.isEmpty()) return emptyList()
                    out.addAll(resolved)
                }
                out
            }
            else -> emptyList()
        }
    }

    private fun applyFunctions(
        drops: List<VanillaDrop>,
        functions: JsonArray?,
        context: LootContext
    ): List<VanillaDrop> {
        if (functions == null || functions.isEmpty() || drops.isEmpty()) return drops
        var current = drops
        for (functionElement in functions) {
            val function = functionElement.jsonObject
            if (!evaluateConditions(function["conditions"]?.jsonArray, context)) continue
            current = when (function["function"]?.jsonPrimitive?.content) {
                "minecraft:set_count" -> applySetCount(current, function)
                "minecraft:limit_count" -> applyLimitCount(current, function)
                "minecraft:apply_bonus" -> current
                "minecraft:explosion_decay" -> current
                "minecraft:copy_components" -> current
                "minecraft:copy_state" -> current
                else -> current
            }
            if (current.isEmpty()) return emptyList()
        }
        return current
    }

    private fun applySetCount(drops: List<VanillaDrop>, function: JsonObject): List<VanillaDrop> {
        val count = evaluateCount(function["count"]) ?: return drops
        val add = function["add"]?.jsonPrimitive?.content?.toBooleanStrictOrNull() ?: false
        return drops.mapNotNull { drop ->
            val nextCount = if (add) drop.count + count else count
            drop.takeIf { nextCount > 0 }?.copy(count = nextCount)
        }
    }

    private fun applyLimitCount(drops: List<VanillaDrop>, function: JsonObject): List<VanillaDrop> {
        val limit = function["limit"]?.jsonObject ?: return drops
        val min = limit["min"]?.jsonPrimitive?.content?.toIntOrNull()
        val max = limit["max"]?.jsonPrimitive?.content?.toIntOrNull()
        return drops.mapNotNull { drop ->
            var count = drop.count
            if (min != null) count = maxOf(count, min)
            if (max != null) count = minOf(count, max)
            drop.takeIf { count > 0 }?.copy(count = count)
        }
    }

    private fun evaluateCount(element: JsonElement?): Int? {
        if (element == null) return null
        evaluateNumberProvider(element)?.let { return it.roundToInt() }
        val obj = runCatching { element.jsonObject }.getOrNull() ?: return null
        return when (obj["type"]?.jsonPrimitive?.content) {
            "minecraft:uniform" -> {
                val min = evaluateNumberProvider(obj["min"])?.roundToInt() ?: return null
                val max = evaluateNumberProvider(obj["max"])?.roundToInt() ?: return null
                val lower = minOf(min, max)
                val upper = maxOf(min, max)
                ThreadLocalRandom.current().nextInt(lower, upper + 1)
            }
            "minecraft:binomial" -> {
                val n = evaluateNumberProvider(obj["n"])?.roundToInt() ?: return null
                val p = evaluateNumberProvider(obj["p"]) ?: return null
                var count = 0
                repeat(n.coerceAtLeast(0)) {
                    if (ThreadLocalRandom.current().nextDouble() < p) count++
                }
                count
            }
            "minecraft:constant", "minecraft:score", "minecraft:storage", "minecraft:enchantment_level" -> {
                evaluateNumberProvider(obj["value"])?.roundToInt()
            }
            else -> null
        }
    }

    private fun evaluateNumberProvider(element: JsonElement?): Double? {
        if (element == null) return null
        val primitive = runCatching { element.jsonPrimitive }.getOrNull()
        if (primitive != null) {
            return primitive.content.toDoubleOrNull()
        }
        val obj = runCatching { element.jsonObject }.getOrNull() ?: return null
        return when (obj["type"]?.jsonPrimitive?.content) {
            null -> {
                obj["value"]?.let { evaluateNumberProvider(it) }
                    ?: obj["min"]?.let { evaluateNumberProvider(it) }
            }
            "minecraft:constant" -> evaluateNumberProvider(obj["value"])
            "minecraft:uniform" -> {
                val min = evaluateNumberProvider(obj["min"]) ?: return null
                val max = evaluateNumberProvider(obj["max"]) ?: return null
                val lower = minOf(min, max)
                val upper = maxOf(min, max)
                ThreadLocalRandom.current().nextDouble(lower, upper)
            }
            "minecraft:binomial" -> {
                val n = evaluateNumberProvider(obj["n"])?.roundToInt() ?: return null
                val p = evaluateNumberProvider(obj["p"]) ?: return null
                var count = 0
                repeat(n.coerceAtLeast(0)) {
                    if (ThreadLocalRandom.current().nextDouble() < p) count++
                }
                count.toDouble()
            }
            else -> obj["value"]?.let { evaluateNumberProvider(it) }
        }
    }

    private fun evaluateConditions(conditions: JsonArray?, context: LootContext): Boolean {
        if (conditions == null) return true
        for (conditionElement in conditions) {
            if (!evaluateCondition(conditionElement.jsonObject, context)) return false
        }
        return true
    }

    private fun evaluateCondition(condition: JsonObject, context: LootContext): Boolean {
        return when (condition["condition"]?.jsonPrimitive?.content) {
            "minecraft:survives_explosion" -> true
            "minecraft:match_tool" -> matchTool(condition["predicate"]?.jsonObject, context.heldToolKey)
            "minecraft:any_of" -> condition["terms"]?.jsonArray.orEmpty().any { evaluateCondition(it.jsonObject, context) }
            "minecraft:all_of" -> condition["terms"]?.jsonArray.orEmpty().all { evaluateCondition(it.jsonObject, context) }
            "minecraft:inverted" -> !evaluateCondition(condition["term"]?.jsonObject ?: return false, context)
            "minecraft:random_chance" -> {
                val chance = condition["chance"]?.jsonPrimitive?.content?.toDoubleOrNull() ?: return false
                ThreadLocalRandom.current().nextDouble() < chance
            }
            "minecraft:table_bonus" -> {
                val chances = condition["chances"]?.jsonArray ?: return false
                val chance = chances.firstOrNull()?.jsonPrimitive?.content?.toDoubleOrNull() ?: return false
                ThreadLocalRandom.current().nextDouble() < chance
            }
            "minecraft:block_state_property" -> matchesBlockStateProperty(condition, context)
            "minecraft:location_check" -> matchesLocationCheck(condition, context)
            "minecraft:entity_properties" -> {
                val entity = condition["entity"]?.jsonPrimitive?.content
                entity == "this" && (condition["predicate"]?.jsonObject?.isEmpty() != false)
            }
            else -> false
        }
    }

    private fun matchTool(predicate: JsonObject?, heldToolKey: String?): Boolean {
        if (predicate == null) return false
        if (heldToolKey == null) return false

        val items = predicate["items"]
        if (items != null) {
            val allowed = when (items) {
                is JsonArray -> items.map { it.jsonPrimitive.content }
                else -> listOf(items.jsonPrimitive.content)
            }
            if (heldToolKey !in allowed) return false
        }

        val tag = predicate["tag"]?.jsonPrimitive?.content
        if (tag != null) {
            val tagName = tag.substringAfter("minecraft:")
            if (!itemTag(tagName).contains(heldToolKey)) return false
        }

        val predicates = predicate["predicates"]?.jsonObject
        val enchantments = predicates?.get("minecraft:enchantments")?.jsonArray
        if (enchantments != null && enchantments.isNotEmpty()) return false

        return true
    }

    private fun matchesBlockStateProperty(condition: JsonObject, context: LootContext): Boolean {
        val block = condition["block"]?.jsonPrimitive?.content
        if (block != null && block != context.blockKey) return false
        val properties = condition["properties"]?.jsonObject ?: return true
        for ((name, value) in properties) {
            if (context.blockStateProperties[name] != value.jsonPrimitive.content) return false
        }
        return true
    }

    private fun matchesLocationCheck(condition: JsonObject, context: LootContext): Boolean {
        val predicate = condition["predicate"]?.jsonObject ?: return false
        val blockPredicate = predicate["block"]?.jsonObject ?: return false
        val offsetX = condition["offsetX"]?.jsonPrimitive?.content?.toIntOrNull() ?: 0
        val offsetY = condition["offsetY"]?.jsonPrimitive?.content?.toIntOrNull() ?: 0
        val offsetZ = condition["offsetZ"]?.jsonPrimitive?.content?.toIntOrNull() ?: 0
        val stateId = context.world.blockStateAt(context.x + offsetX, context.y + offsetY, context.z + offsetZ)
        val parsed = BlockStateRegistry.parsedState(stateId) ?: return false

        val blocks = blockPredicate["blocks"]
        if (blocks != null) {
            val allowed = when (blocks) {
                is JsonArray -> blocks.map { it.jsonPrimitive.content }
                else -> listOf(blocks.jsonPrimitive.content)
            }
            if (parsed.blockKey !in allowed) return false
        }

        val state = blockPredicate["state"]?.jsonObject ?: return true
        for ((name, value) in state) {
            if (parsed.properties[name] != value.jsonPrimitive.content) return false
        }
        return true
    }
}
