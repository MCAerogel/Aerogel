package org.macaroon3145.world

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive

data class EntityLootDropRule(
    val rawItemId: Int,
    val cookedItemId: Int?,
    val minCount: Int,
    val maxCount: Int,
    val lootingMinBonusPerLevel: Int,
    val lootingMaxBonusPerLevel: Int
)

object EntityLootDropCache {
    private val json = Json { ignoreUnknownKeys = true }
    @Volatile private var rulesByEntityTypeKey: Map<String, EntityLootDropRule> = emptyMap()

    fun prewarm(itemIdByKey: Map<String, Int>, entityTypeKeys: Collection<String>) {
        val next = HashMap<String, EntityLootDropRule>()
        for (entityTypeKey in entityTypeKeys) {
            val rule = loadRuleForEntityTypeKey(entityTypeKey, itemIdByKey) ?: continue
            next[entityTypeKey] = rule
        }
        rulesByEntityTypeKey = next
    }

    fun rule(entityTypeKey: String): EntityLootDropRule? = rulesByEntityTypeKey[entityTypeKey]

    private fun loadRuleForEntityTypeKey(entityTypeKey: String, itemIdByKey: Map<String, Int>): EntityLootDropRule? {
        val entityName = entityTypeKey.substringAfter("minecraft:")
        val path = "data/minecraft/loot_table/entities/$entityName.json"
        val stream = EntityLootDropCache::class.java.classLoader.getResourceAsStream(path) ?: return null
        val root = stream.bufferedReader().use { json.parseToJsonElement(it.readText()).jsonObject }
        val pools = root["pools"]?.jsonArray ?: return null
        for (poolElement in pools) {
            val pool = poolElement.jsonObject
            val entries = pool["entries"]?.jsonArray ?: continue
            for (entryElement in entries) {
                val entry = entryElement.jsonObject
                if (entry["type"]?.jsonPrimitive?.content != "minecraft:item") continue
                val rawItemKey = entry["name"]?.jsonPrimitive?.content ?: continue
                val rawItemId = itemIdByKey[rawItemKey] ?: continue
                val functions = entry["functions"]?.jsonArray.orEmpty()
                val countRange = parseSetCountUniformRange(functions)
                val lootingRange = parseLootingBonusRange(functions)
                val cookedItemId = smeltedItemId(rawItemKey, itemIdByKey)
                return EntityLootDropRule(
                    rawItemId = rawItemId,
                    cookedItemId = cookedItemId,
                    minCount = countRange.first,
                    maxCount = countRange.second,
                    lootingMinBonusPerLevel = lootingRange.first,
                    lootingMaxBonusPerLevel = lootingRange.second
                )
            }
        }
        return null
    }

    private fun parseSetCountUniformRange(functions: List<kotlinx.serialization.json.JsonElement>): Pair<Int, Int> {
        for (element in functions) {
            val fn = element.jsonObject
            if (fn["function"]?.jsonPrimitive?.content != "minecraft:set_count") continue
            val count = fn["count"]?.jsonObject ?: continue
            if (count["type"]?.jsonPrimitive?.content != "minecraft:uniform") continue
            val min = count["min"]?.jsonPrimitive?.content?.toDoubleOrNull()?.toInt() ?: continue
            val max = count["max"]?.jsonPrimitive?.content?.toDoubleOrNull()?.toInt() ?: continue
            if (max < min) return min to min
            return min to max
        }
        return 1 to 1
    }

    private fun parseLootingBonusRange(functions: List<kotlinx.serialization.json.JsonElement>): Pair<Int, Int> {
        for (element in functions) {
            val fn = element.jsonObject
            if (fn["function"]?.jsonPrimitive?.content != "minecraft:enchanted_count_increase") continue
            val count = fn["count"]?.jsonObject ?: continue
            if (count["type"]?.jsonPrimitive?.content != "minecraft:uniform") continue
            val min = count["min"]?.jsonPrimitive?.content?.toDoubleOrNull()?.toInt() ?: continue
            val max = count["max"]?.jsonPrimitive?.content?.toDoubleOrNull()?.toInt() ?: continue
            if (max < min) return min to min
            return min to max
        }
        return 0 to 0
    }

    private fun smeltedItemId(rawItemKey: String, itemIdByKey: Map<String, Int>): Int? {
        val cookedKey = when (rawItemKey) {
            "minecraft:porkchop" -> "minecraft:cooked_porkchop"
            "minecraft:beef" -> "minecraft:cooked_beef"
            "minecraft:chicken" -> "minecraft:cooked_chicken"
            "minecraft:mutton" -> "minecraft:cooked_mutton"
            "minecraft:rabbit" -> "minecraft:cooked_rabbit"
            "minecraft:cod" -> "minecraft:cooked_cod"
            "minecraft:salmon" -> "minecraft:cooked_salmon"
            else -> null
        } ?: return null
        return itemIdByKey[cookedKey]
    }
}
