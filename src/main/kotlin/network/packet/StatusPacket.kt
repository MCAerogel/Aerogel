package org.macaroon3145.network.packet

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.encodeToStream
import org.macaroon3145.network.NetworkUtils
import java.io.ByteArrayOutputStream

@Serializable
data class StatusPacket(
    val version: Version,
    val players: Players,
    val description: Description,
    val favicon: String? = null,
    val enforcesSecureChat: Boolean = false
) {
    @OptIn(ExperimentalSerializationApi::class)
    fun serialize(): ByteArray {
        val outputStream = ByteArrayOutputStream()
        Json.encodeToStream(this, outputStream)
        val jsonBytes = outputStream.toByteArray()

        val packetData = ByteArrayOutputStream()
        NetworkUtils.writeVarInt(packetData, 0x00)
        NetworkUtils.writeString(packetData, String(jsonBytes, Charsets.UTF_8))
        return packetData.toByteArray()
    }
}

@Serializable
data class Version(
    val name: String,
    val protocol: Int
)

@Serializable
data class PlayerSample(
    val name: String,
    val id: String
)

@Serializable
data class Players(
    val max: Int,
    val online: Int,
    val sample: List<PlayerSample> = emptyList()
)

@Serializable
data class Description(val text: String)
