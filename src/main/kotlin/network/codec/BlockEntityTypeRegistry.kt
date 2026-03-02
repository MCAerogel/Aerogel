package org.macaroon3145.network.codec

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive

object BlockEntityTypeRegistry {
    private val json = Json { ignoreUnknownKeys = true }

    private val byKey: Map<String, Int> by lazy {
        val resource = BlockEntityTypeRegistry::class.java.classLoader
            .getResourceAsStream("block-entity-type-ids-1.21.11.json")
            ?: error("Missing block-entity-type-ids-1.21.11.json resource")
        val root = resource.bufferedReader().use { json.parseToJsonElement(it.readText()) }.jsonObject
        val entries = root["entries"]?.jsonObject ?: return@lazy emptyMap()
        val map = HashMap<String, Int>(entries.size)
        for ((key, valueElement) in entries) {
            val id = valueElement.jsonPrimitive.intOrNull ?: continue
            map[key] = id
        }
        map
    }

    fun idOf(typeKey: String): Int? = byKey[typeKey]

    fun prewarm() {
        byKey.size
    }
}
