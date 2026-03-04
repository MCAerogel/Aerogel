package org.macaroon3145.network.handler

import org.macaroon3145.network.codec.NbtWriter
import org.macaroon3145.network.codec.RegistryCodec
import org.macaroon3145.network.NetworkUtils
import java.io.ByteArrayOutputStream
import java.io.DataOutputStream

object ConfigurationPackets {
    private data class RegistryTag(
        val name: String,
        val entryIds: IntArray
    )

    private data class TagRegistry(
        val registryId: String,
        val tags: List<RegistryTag>
    )

    // Vanilla 1.21.11 fluid registry IDs.
    // [0]=empty, [1]=flowing_water, [2]=water, [3]=flowing_lava, [4]=lava
    private const val FLUID_ID_FLOWING_WATER = 1
    private const val FLUID_ID_WATER = 2
    private const val FLUID_ID_FLOWING_LAVA = 3
    private const val FLUID_ID_LAVA = 4

    fun registryDataPackets(): List<ByteArray> {
        val requiredRegistries = setOf(
            "minecraft:dimension_type",
            "minecraft:worldgen/biome",
            "minecraft:damage_type",
            "minecraft:chat_type",
            "minecraft:cat_variant",
            "minecraft:chicken_variant",
            "minecraft:cow_variant",
            "minecraft:frog_variant",
            "minecraft:painting_variant",
            "minecraft:pig_variant",
            "minecraft:timeline",
            "minecraft:wolf_sound_variant",
            "minecraft:wolf_variant",
            "minecraft:zombie_nautilus_variant"
        )
        return RegistryCodec.allRegistries().filter { it.id in requiredRegistries }.map { registry ->
            registryDataPacket(registry.id, registry.entries)
        }
    }

    private fun registryDataPacket(registryId: String, entries: List<RegistryCodec.RegistryEntry>): ByteArray {
        val packet = ByteArrayOutputStream()
        val out = DataOutputStream(packet)
        NetworkUtils.writeVarInt(packet, 0x07)
        NetworkUtils.writeString(packet, registryId)
        NetworkUtils.writeVarInt(packet, entries.size)
        for (entry in entries) {
            NetworkUtils.writeString(packet, entry.key)
            out.writeBoolean(true)
            NbtWriter.writeAnonymousRoot(packet, entry.value)
        }
        return packet.toByteArray()
    }

    fun featureFlagsPacket(): ByteArray {
        val packet = ByteArrayOutputStream()
        NetworkUtils.writeVarInt(packet, 0x0C)
        NetworkUtils.writeVarInt(packet, 1)
        NetworkUtils.writeString(packet, "minecraft:vanilla")
        return packet.toByteArray()
    }

    fun tagsPacket(): ByteArray {
        val timeline = RegistryCodec.allRegistries().firstOrNull { it.id == "minecraft:timeline" }
        val timelineTags = if (timeline == null) {
            emptyList()
        } else {
            val timelineIndexByKey = timeline.entries.mapIndexed { idx, entry -> entry.key to idx }.toMap()
            val inOverworld = listOf("minecraft:day", "minecraft:moon", "minecraft:villager_schedule")
                .mapNotNull { timelineIndexByKey[it] }
                .toIntArray()
            listOf(
                RegistryTag("minecraft:in_overworld", inOverworld),
                RegistryTag("minecraft:in_nether", intArrayOf()),
                RegistryTag("minecraft:in_end", intArrayOf())
            )
        }

        val registries = ArrayList<TagRegistry>(2)
        registries += TagRegistry(
            registryId = "minecraft:fluid",
            tags = listOf(
                RegistryTag("minecraft:water", intArrayOf(FLUID_ID_FLOWING_WATER, FLUID_ID_WATER)),
                RegistryTag("minecraft:lava", intArrayOf(FLUID_ID_FLOWING_LAVA, FLUID_ID_LAVA))
            )
        )
        if (timelineTags.isNotEmpty()) {
            registries += TagRegistry("minecraft:timeline", timelineTags)
        }

        val packet = ByteArrayOutputStream()
        NetworkUtils.writeVarInt(packet, 0x0D)
        NetworkUtils.writeVarInt(packet, registries.size)
        for (registry in registries) {
            NetworkUtils.writeString(packet, registry.registryId)
            NetworkUtils.writeVarInt(packet, registry.tags.size)
            for (tag in registry.tags) {
                NetworkUtils.writeString(packet, tag.name)
                NetworkUtils.writeVarInt(packet, tag.entryIds.size)
                for (entryId in tag.entryIds) {
                    NetworkUtils.writeVarInt(packet, entryId)
                }
            }
        }

        return packet.toByteArray()
    }

    fun finishConfigurationPacket(): ByteArray {
        val packet = ByteArrayOutputStream()
        NetworkUtils.writeVarInt(packet, 0x03)
        return packet.toByteArray()
    }
}
