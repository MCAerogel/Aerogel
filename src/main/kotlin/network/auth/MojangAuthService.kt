package org.macaroon3145.network.auth

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.math.BigInteger
import java.net.HttpURLConnection
import java.net.URLEncoder
import java.net.URL
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.security.PublicKey
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import javax.crypto.SecretKey

data class MojangProperty(
    val name: String,
    val value: String,
    val signature: String?
)

data class MojangProfile(
    val uuid: UUID,
    val username: String,
    val properties: List<MojangProperty>
)

object MojangAuthService {
    private val json = Json { ignoreUnknownKeys = true }
    private val executor = Executors.newCachedThreadPool()

    fun verify(
        username: String,
        sharedSecret: SecretKey,
        publicKey: PublicKey
    ): CompletableFuture<MojangProfile> {
        return CompletableFuture.supplyAsync({
            val serverHash = createServerHash(sharedSecret, publicKey)
            val encodedUsername = URLEncoder.encode(username, StandardCharsets.UTF_8)
            val encodedHash = URLEncoder.encode(serverHash, StandardCharsets.UTF_8)
            val url = URL("https://sessionserver.mojang.com/session/minecraft/hasJoined?username=$encodedUsername&serverId=$encodedHash&unsigned=false")
            val connection = (url.openConnection() as HttpURLConnection).apply {
                requestMethod = "GET"
                connectTimeout = 5000
                readTimeout = 5000
            }
            val code = connection.responseCode
            if (code != 200) {
                throw IllegalStateException("Session verify failed: HTTP $code")
            }

            val body = connection.inputStream.bufferedReader(StandardCharsets.UTF_8).use { it.readText() }
            val root = json.parseToJsonElement(body).jsonObject
            val rawId = root["id"]?.jsonPrimitive?.content ?: throw IllegalStateException("Missing Mojang profile id")
            val verifiedName = root["name"]?.jsonPrimitive?.content ?: username
            val properties = root["properties"]?.jsonArray?.map { element ->
                val obj = element.jsonObject
                MojangProperty(
                    name = obj["name"]!!.jsonPrimitive.content,
                    value = obj["value"]!!.jsonPrimitive.content,
                    signature = obj["signature"]?.jsonPrimitive?.content
                )
            } ?: emptyList()

            MojangProfile(
                uuid = parseUndashedUuid(rawId),
                username = verifiedName,
                properties = properties
            )
        }, executor)
    }

    private fun createServerHash(sharedSecret: SecretKey, publicKey: PublicKey): String {
        val digest = MessageDigest.getInstance("SHA-1")
        digest.update(byteArrayOf())
        digest.update(sharedSecret.encoded)
        digest.update(publicKey.encoded)
        return BigInteger(digest.digest()).toString(16)
    }

    private fun parseUndashedUuid(raw: String): UUID {
        val normalized = raw.lowercase()
        require(normalized.length == 32) { "Invalid UUID from Mojang: $raw" }
        val dashed = buildString(36) {
            append(normalized.substring(0, 8))
            append('-')
            append(normalized.substring(8, 12))
            append('-')
            append(normalized.substring(12, 16))
            append('-')
            append(normalized.substring(16, 20))
            append('-')
            append(normalized.substring(20, 32))
        }
        return UUID.fromString(dashed)
    }

    fun shutdown() {
        executor.shutdownNow()
    }
}
