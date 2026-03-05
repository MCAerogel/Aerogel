package org.macaroon3145.network.codec

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive

object MenuRegistry {
    private const val MENU_REGISTRY_RESOURCE = "menu-registry-1.21.11.json"
    private val json = Json { ignoreUnknownKeys = true }

    private val menuIdByKey: Map<String, Int> by lazy {
        val stream = MenuRegistry::class.java.classLoader.getResourceAsStream(MENU_REGISTRY_RESOURCE)
            ?: error("Missing $MENU_REGISTRY_RESOURCE resource")
        val root = stream.bufferedReader().use { reader ->
            json.parseToJsonElement(reader.readText()).jsonObject
        }
        root.mapValues { (_, value) -> value.jsonPrimitive.int }
    }

    fun idOf(key: String): Int? = menuIdByKey[key]

    fun requireId(key: String): Int {
        return menuIdByKey[key] ?: error("Missing menu id for '$key' in $MENU_REGISTRY_RESOURCE")
    }

    fun prewarm() {
        menuIdByKey.size
    }
}
