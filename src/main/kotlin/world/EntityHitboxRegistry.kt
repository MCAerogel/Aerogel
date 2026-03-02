package org.macaroon3145.world

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.doubleOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive

object EntityHitboxRegistry {
    data class Dimension(
        val width: Double,
        val height: Double
    ) {
        val halfWidth: Double get() = width * 0.5
    }

    private val json = Json { ignoreUnknownKeys = true }
    private val dimensionsByEntityKey: Map<String, Dimension> by lazy {
        val resource = EntityHitboxRegistry::class.java.classLoader
            .getResourceAsStream("entity-hitbox-1.21.11.json")
            ?: return@lazy emptyMap()
        val root = resource.bufferedReader().use { json.parseToJsonElement(it.readText()).jsonObject }
        val out = HashMap<String, Dimension>(root.size)
        for ((entityKey, value) in root) {
            val obj = value.jsonObject
            val width = obj["width"]?.jsonPrimitive?.doubleOrNull ?: continue
            val height = obj["height"]?.jsonPrimitive?.doubleOrNull ?: continue
            if (!width.isFinite() || !height.isFinite()) continue
            if (width <= 0.0 || height <= 0.0) continue
            out[entityKey] = Dimension(width = width, height = height)
        }
        out
    }

    fun dimension(entityKey: String): Dimension? = dimensionsByEntityKey[entityKey]

    fun prewarm() {
        dimensionsByEntityKey.size
    }
}
