package org.macaroon3145.network.codec

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive

object RegistryCodec {
    data class RegistryEntry(val key: String, val value: JsonObject)
    data class Registry(val id: String, val entries: List<RegistryEntry>)

    private val json = Json { ignoreUnknownKeys = true }

    private val registries: List<Registry> by lazy {
        val resource = RegistryCodec::class.java.classLoader
            .getResourceAsStream("registry-codec-1.21.11.json")
            ?: error("Missing registry-codec-1.21.11.json resource")

        val root = resource.bufferedReader().use { json.parseToJsonElement(it.readText()) }.jsonObject
        root.entries.map { (registryId, registryBodyElement) ->
            val entries = registryBodyElement.jsonObject["entries"]!!.jsonArray.map { entryElement ->
                val entry = entryElement.jsonObject
                RegistryEntry(
                    key = entry["key"]!!.jsonPrimitive.content,
                    value = entry["value"]!!.jsonObject
                )
            }
            Registry(registryId, entries)
        }
    }

    fun allRegistries(): List<Registry> = registries

    fun entryIndex(registryId: String, key: String): Int? {
        val registry = registries.firstOrNull { it.id == registryId } ?: return null
        return registry.entries.indexOfFirst { it.key == key }.takeIf { it >= 0 }
    }

    fun entryCount(registryId: String): Int? {
        return registries.firstOrNull { it.id == registryId }?.entries?.size
    }

    fun prewarm() {
        // Ensure registry codec parse happens during startup.
        registries.size
    }
}
