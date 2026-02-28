package org.macaroon3145.network.handler

import org.macaroon3145.network.codec.NbtWriter
import org.macaroon3145.network.codec.RegistryCodec
import org.macaroon3145.network.NetworkUtils
import java.io.ByteArrayOutputStream
import java.io.DataOutputStream

object ConfigurationPackets {
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
            ?: return ByteArrayOutputStream().also {
                NetworkUtils.writeVarInt(it, 0x0D)
                NetworkUtils.writeVarInt(it, 0)
            }.toByteArray()

        val timelineIndexByKey = timeline.entries.mapIndexed { idx, entry -> entry.key to idx }.toMap()
        val inOverworld = listOf("minecraft:day", "minecraft:moon", "minecraft:villager_schedule")
            .mapNotNull { timelineIndexByKey[it] }

        val packet = ByteArrayOutputStream()
        NetworkUtils.writeVarInt(packet, 0x0D)
        NetworkUtils.writeVarInt(packet, 1)
        NetworkUtils.writeString(packet, "minecraft:timeline")
        NetworkUtils.writeVarInt(packet, 3)

        NetworkUtils.writeString(packet, "minecraft:in_overworld")
        NetworkUtils.writeVarInt(packet, inOverworld.size)
        for (id in inOverworld) NetworkUtils.writeVarInt(packet, id)

        NetworkUtils.writeString(packet, "minecraft:in_nether")
        NetworkUtils.writeVarInt(packet, 0)

        NetworkUtils.writeString(packet, "minecraft:in_end")
        NetworkUtils.writeVarInt(packet, 0)

        return packet.toByteArray()
    }

    fun finishConfigurationPacket(): ByteArray {
        val packet = ByteArrayOutputStream()
        NetworkUtils.writeVarInt(packet, 0x03)
        return packet.toByteArray()
    }
}
