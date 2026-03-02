package org.macaroon3145.network.item

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.intOrNull
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

object FoodItemCache {
    data class FoodData(
        val nutrition: Int,
        val saturationModifier: Float,
        val alwaysEdible: Boolean,
        val remainderItemId: Int,
        val consumeSeconds: Double,
        val useSoundKey: String,
        val finishSoundKey: String
    )

    private val json = Json { ignoreUnknownKeys = true }
    private val byItemId = ConcurrentHashMap<Int, FoodData>()
    private val loaded = AtomicBoolean(false)

    fun prewarm(itemIdByKey: Map<String, Int>) {
        if (!loaded.compareAndSet(false, true)) return
        runCatching { load(itemIdByKey) }
            .onFailure { loaded.set(false) }
            .getOrThrow()
    }

    fun byItemId(itemId: Int): FoodData? = byItemId[itemId]

    private fun load(itemIdByKey: Map<String, Int>) {
        val stream = FoodItemCache::class.java.classLoader
            .getResourceAsStream("data/minecraft/item/food_properties.json")
            ?: return
        val resourceBytes = stream.use { it.readBytes() }

        val parsed = HashMap<Int, FoodData>()
        val root = json.parseToJsonElement(resourceBytes.decodeToString()).jsonObject
        val items = root["items"]?.jsonObject ?: return

        for ((itemKey, valueElement) in items) {
            val itemId = itemIdByKey[itemKey] ?: continue
            val obj = valueElement.jsonObject
            val nutrition = obj["nutrition"]?.jsonPrimitive?.intOrNull ?: continue
            val saturationModifier = obj["saturation_modifier"]?.jsonPrimitive?.content?.toFloatOrNull() ?: continue
            val alwaysEdible = obj["always_edible"]?.jsonPrimitive?.content?.toBooleanStrictOrNull() ?: false
            val remainderItemKey = itemRemainderByFoodItem[itemKey]
            val remainderItemId = if (remainderItemKey != null) itemIdByKey[remainderItemKey] ?: -1 else -1
            parsed[itemId] = FoodData(
                nutrition = nutrition.coerceAtLeast(0),
                saturationModifier = saturationModifier.coerceAtLeast(0f),
                alwaysEdible = alwaysEdible,
                remainderItemId = remainderItemId,
                consumeSeconds = itemConsumeSecondsByFoodItem[itemKey] ?: DEFAULT_CONSUME_SECONDS,
                useSoundKey = itemUseSoundByFoodItem[itemKey] ?: DEFAULT_EAT_SOUND_KEY,
                finishSoundKey = itemFinishSoundByFoodItem[itemKey] ?: DEFAULT_FINISH_SOUND_KEY
            )
        }
        byItemId.clear()
        byItemId.putAll(parsed)
    }

    // Vanilla Items#usingConvertsTo for edible items.
    private val itemRemainderByFoodItem = mapOf(
        "minecraft:mushroom_stew" to "minecraft:bowl",
        "minecraft:rabbit_stew" to "minecraft:bowl",
        "minecraft:beetroot_soup" to "minecraft:bowl",
        "minecraft:suspicious_stew" to "minecraft:bowl",
        "minecraft:honey_bottle" to "minecraft:glass_bottle"
    )

    // Vanilla Consumables: most food is 1.6s, with explicit per-item overrides.
    private val itemConsumeSecondsByFoodItem = mapOf(
        "minecraft:dried_kelp" to 0.8,
        "minecraft:honey_bottle" to 2.0
    )

    private val itemUseSoundByFoodItem = mapOf(
        "minecraft:honey_bottle" to "minecraft:entity.generic.drink"
    )

    private val itemFinishSoundByFoodItem = mapOf(
        "minecraft:honey_bottle" to "minecraft:entity.generic.drink"
    )

    private const val DEFAULT_CONSUME_SECONDS = 1.6
    private const val DEFAULT_EAT_SOUND_KEY = "minecraft:entity.generic.eat"
    private const val DEFAULT_FINISH_SOUND_KEY = "minecraft:entity.player.burp"

}
