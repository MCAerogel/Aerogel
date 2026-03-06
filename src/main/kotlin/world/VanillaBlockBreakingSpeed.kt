package org.macaroon3145.world

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.doubleOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.macaroon3145.network.codec.BlockStateRegistry
import java.util.concurrent.ConcurrentHashMap

object VanillaBlockBreakingSpeed {
    data class BreakContext(
        val stateId: Int,
        val heldItemKey: String?,
        val onGround: Boolean,
        val underwater: Boolean,
        val aquaAffinity: Boolean,
        val efficiencyLevel: Int,
        val hasteLevel: Int,
        val miningFatigueLevel: Int
    )

    private enum class ToolType {
        PICKAXE,
        AXE,
        SHOVEL,
        HOE,
        SWORD,
        SHEARS,
        UNKNOWN
    }

    private data class TagHardnessRule(
        val tag: String,
        val hardness: Double
    )

    private data class Profile(
        val defaultHardness: Double,
        val tagRules: List<TagHardnessRule>,
        val overrides: Map<String, Double>
    )

    private data class DestroyTimeEntry(
        val hardness: Double,
        val diggable: Boolean
    )

    private val json = Json { ignoreUnknownKeys = true }
    private val resourceTextCache = ConcurrentHashMap<String, String?>()
    private val jsonObjectCache = ConcurrentHashMap<String, JsonObject>()
    private val missingJsonObjectPaths = ConcurrentHashMap.newKeySet<String>()
    private val blockTagCache = ConcurrentHashMap<String, Set<String>>()
    private val itemTagCache = ConcurrentHashMap<String, Set<String>>()
    private val toolTypeByItemKey = ConcurrentHashMap<String, ToolType>()
    private val hardnessByBlockKey = ConcurrentHashMap<String, Double>()
    private val diggableByBlockKey = ConcurrentHashMap<String, Boolean>()

    private val destroyTimeTable: Map<String, DestroyTimeEntry> by lazy {
        val root = loadJsonObject("vanilla-destroy-time-1.21.11.json") ?: return@lazy emptyMap()
        val out = HashMap<String, DestroyTimeEntry>(root.size)
        for ((blockKey, entryElement) in root) {
            val entry = entryElement.jsonObject
            val hardness = entry["hardness"]?.jsonPrimitive?.doubleOrNull ?: continue
            val diggable = entry["diggable"]?.jsonPrimitive?.content?.toBooleanStrictOrNull() ?: false
            out[blockKey] = DestroyTimeEntry(hardness = hardness, diggable = diggable)
        }
        out
    }

    private val profile: Profile by lazy {
        val root = loadJsonObject("block-mining-profile-1.21.11.json") ?: return@lazy Profile(
            defaultHardness = 1.5,
            tagRules = emptyList(),
            overrides = emptyMap()
        )
        val defaultHardness = root["default_hardness"]?.jsonPrimitive?.doubleOrNull ?: 1.5
        val tagRules = root["tag_hardness"]?.jsonArray.orEmpty().mapNotNull { el ->
            val obj = el.jsonObject
            val tag = obj["tag"]?.jsonPrimitive?.content ?: return@mapNotNull null
            val hardness = obj["hardness"]?.jsonPrimitive?.doubleOrNull ?: return@mapNotNull null
            TagHardnessRule(tag = tag, hardness = hardness)
        }
        val overrides = HashMap<String, Double>()
        for ((key, value) in root["overrides"]?.jsonObject.orEmpty()) {
            val hardness = value.jsonPrimitive.doubleOrNull ?: continue
            overrides[key] = hardness
        }
        Profile(defaultHardness = defaultHardness, tagRules = tagRules, overrides = overrides)
    }

    fun breakDurationSeconds(context: BreakContext): Double? {
        if (context.stateId <= 0) return null
        val parsed = BlockStateRegistry.parsedState(context.stateId) ?: return null
        val blockKey = parsed.blockKey
        if (!isDiggableBlock(blockKey)) return null
        val hardness = hardnessForBlock(blockKey)
        if (hardness <= 0.0) return null

        val toolSpeed = toolSpeedMultiplier(blockKey, context.heldItemKey)
        var speed = toolSpeed

        if (context.efficiencyLevel > 0 && toolSpeed > 1.0) {
            val eff = context.efficiencyLevel.coerceAtLeast(0)
            speed += (eff * eff + 1).toDouble()
        }

        if (context.hasteLevel > 0) {
            speed *= 1.0 + (0.2 * context.hasteLevel.coerceAtMost(10))
        }

        if (context.miningFatigueLevel > 0) {
            speed *= fatigueMultiplier(context.miningFatigueLevel)
        }

        if (context.underwater && !context.aquaAffinity) {
            speed *= 0.2
        }

        if (!context.onGround) {
            speed *= 0.2
        }

        if (speed <= 0.0) return null

        val canHarvest = canHarvest(blockKey, context.heldItemKey)
        val blockDamagePerTick = speed / hardness / if (canHarvest) 30.0 else 100.0
        if (blockDamagePerTick <= 0.0) return null

        val ticks = 1.0 / blockDamagePerTick
        val seconds = ticks / 20.0
        return seconds.coerceIn(0.05, 120.0)
    }

    private fun fatigueMultiplier(level: Int): Double {
        return when (level.coerceAtLeast(1)) {
            1 -> 0.3
            2 -> 0.09
            3 -> 0.0027
            else -> 0.00081
        }
    }

    private fun hardnessForBlock(blockKey: String): Double {
        return hardnessByBlockKey.computeIfAbsent(blockKey) { key ->
            destroyTimeTable[key]?.let { return@computeIfAbsent it.hardness }
            profile.overrides[key]?.let { return@computeIfAbsent it }
            for (rule in profile.tagRules) {
                if (blockTag(rule.tag).contains(key)) {
                    return@computeIfAbsent rule.hardness
                }
            }
            profile.defaultHardness
        }
    }

    private fun isDiggableBlock(blockKey: String): Boolean {
        return diggableByBlockKey.computeIfAbsent(blockKey) { key ->
            destroyTimeTable[key]?.diggable ?: true
        }
    }

    private fun canHarvest(blockKey: String, heldItemKey: String?): Boolean {
        if (blockTag("mineable/pickaxe").contains(blockKey)) {
            val toolType = classifyToolType(heldItemKey)
            if (toolType != ToolType.PICKAXE) return false
        }
        if (!requiresCorrectTool(blockKey)) return true
        val toolType = classifyToolType(heldItemKey)
        if (!toolMatchesMineableTag(blockKey, toolType)) return false
        val incorrectTag = incorrectTagForItem(heldItemKey) ?: return true
        return !blockTag(incorrectTag).contains(blockKey)
    }

    private fun toolSpeedMultiplier(blockKey: String, heldItemKey: String?): Double {
        val toolType = classifyToolType(heldItemKey)
        if (!toolMatchesMineableTag(blockKey, toolType)) {
            return if (toolType == ToolType.SHEARS && shearFastBlocks.contains(blockKey)) 5.0 else 1.0
        }
        return when (tierForItem(heldItemKey)) {
            "wood", "gold" -> if (isGoldTool(heldItemKey)) 12.0 else 2.0
            "stone" -> 4.0
            "iron" -> 6.0
            "diamond" -> 8.0
            "netherite" -> 9.0
            else -> if (toolType == ToolType.SHEARS) 5.0 else 1.0
        }
    }

    private fun classifyToolType(heldItemKey: String?): ToolType {
        val itemKey = heldItemKey ?: return ToolType.UNKNOWN
        return toolTypeByItemKey.computeIfAbsent(itemKey) {
            when {
                itemKey == "minecraft:shears" -> ToolType.SHEARS
                itemTag("pickaxes").contains(itemKey) -> ToolType.PICKAXE
                itemTag("axes").contains(itemKey) -> ToolType.AXE
                itemTag("shovels").contains(itemKey) -> ToolType.SHOVEL
                itemTag("hoes").contains(itemKey) -> ToolType.HOE
                itemTag("swords").contains(itemKey) -> ToolType.SWORD
                else -> ToolType.UNKNOWN
            }
        }
    }

    private fun toolMatchesMineableTag(blockKey: String, toolType: ToolType): Boolean {
        if (blockTag("mineable/pickaxe").contains(blockKey)) return toolType == ToolType.PICKAXE
        if (blockTag("mineable/axe").contains(blockKey)) return toolType == ToolType.AXE
        if (blockTag("mineable/shovel").contains(blockKey)) return toolType == ToolType.SHOVEL
        if (blockTag("mineable/hoe").contains(blockKey)) return toolType == ToolType.HOE
        return true
    }

    private fun requiresCorrectTool(blockKey: String): Boolean {
        if (blockTag("needs_stone_tool").contains(blockKey)) return true
        if (blockTag("needs_iron_tool").contains(blockKey)) return true
        if (blockTag("needs_diamond_tool").contains(blockKey)) return true
        return incorrectToolTags.any { blockTag(it).contains(blockKey) }
    }

    private fun tierForItem(itemKey: String?): String? {
        val key = itemKey ?: return null
        return when {
            key.startsWith("minecraft:wooden_") -> "wood"
            key.startsWith("minecraft:golden_") -> "gold"
            key.startsWith("minecraft:stone_") -> "stone"
            key.startsWith("minecraft:iron_") -> "iron"
            key.startsWith("minecraft:diamond_") -> "diamond"
            key.startsWith("minecraft:netherite_") -> "netherite"
            else -> null
        }
    }

    private fun isGoldTool(itemKey: String?): Boolean {
        val key = itemKey ?: return false
        return key.startsWith("minecraft:golden_")
    }

    private fun incorrectTagForItem(itemKey: String?): String? {
        val key = itemKey ?: return null
        return when {
            key.startsWith("minecraft:wooden_") -> "incorrect_for_wooden_tool"
            key.startsWith("minecraft:golden_") -> "incorrect_for_gold_tool"
            key.startsWith("minecraft:stone_") -> "incorrect_for_stone_tool"
            key.startsWith("minecraft:copper_") -> "incorrect_for_copper_tool"
            key.startsWith("minecraft:iron_") -> "incorrect_for_iron_tool"
            key.startsWith("minecraft:diamond_") -> "incorrect_for_diamond_tool"
            key.startsWith("minecraft:netherite_") -> "incorrect_for_netherite_tool"
            else -> null
        }
    }

    private fun blockTag(tagName: String): Set<String> {
        return blockTagCache.computeIfAbsent(tagName) { loadTag("block", it) }
    }

    private fun itemTag(tagName: String): Set<String> {
        return itemTagCache.computeIfAbsent(tagName) { loadTag("item", it) }
    }

    private fun loadTag(kind: String, tagName: String): Set<String> {
        return resolveTag(kind, tagName, HashSet())
    }

    private fun resolveTag(kind: String, tagName: String, visited: MutableSet<String>): Set<String> {
        val visitKey = "$kind:$tagName"
        if (!visited.add(visitKey)) return emptySet()
        val root = loadJsonObject("data/minecraft/tags/$kind/$tagName.json") ?: return emptySet()
        val values = root["values"]?.jsonArray ?: return emptySet()
        val out = LinkedHashSet<String>()
        for (entry in values) {
            val raw = entry.jsonPrimitive.content
            if (raw.startsWith("#")) {
                out.addAll(resolveTag(kind, raw.substring(1).substringAfter("minecraft:"), visited))
            } else {
                out.add(if (raw.startsWith("minecraft:")) raw else "minecraft:$raw")
            }
        }
        return out
    }

    private fun loadJsonObject(path: String): JsonObject? {
        jsonObjectCache[path]?.let { return it }
        if (missingJsonObjectPaths.contains(path)) return null
        val text = resourceTextCache.computeIfAbsent(path) { key ->
            val stream = VanillaBlockBreakingSpeed::class.java.classLoader.getResourceAsStream(key) ?: return@computeIfAbsent null
            stream.bufferedReader().use { it.readText() }
        } ?: run {
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

    private val incorrectToolTags = listOf(
        "incorrect_for_wooden_tool",
        "incorrect_for_gold_tool",
        "incorrect_for_stone_tool",
        "incorrect_for_copper_tool",
        "incorrect_for_iron_tool",
        "incorrect_for_diamond_tool",
        "incorrect_for_netherite_tool"
    )

    private val shearFastBlocks = setOf(
        "minecraft:cobweb",
        "minecraft:vine",
        "minecraft:tripwire"
    )

    fun prewarm() {
        // Force immutable mining resources/tag parse during startup.
        destroyTimeTable.size
        profile.overrides.size
        itemTag("pickaxes").size
        itemTag("axes").size
        itemTag("shovels").size
        itemTag("hoes").size
        itemTag("swords").size
        blockTag("mineable/pickaxe").size
        blockTag("mineable/axe").size
        blockTag("mineable/shovel").size
        blockTag("mineable/hoe").size
        blockTag("needs_stone_tool").size
        blockTag("needs_iron_tool").size
        blockTag("needs_diamond_tool").size
        for (tag in incorrectToolTags) {
            blockTag(tag).size
        }
    }
}
