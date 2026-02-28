package org.macaroon3145.network.handler

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.SimpleChannelInboundHandler
import org.macaroon3145.config.ServerConfig
import org.macaroon3145.network.NetworkUtils
import org.macaroon3145.network.auth.MojangAuthService
import org.macaroon3145.network.auth.MojangProperty
import org.macaroon3145.network.transcoder.MinecraftCompressionDecoder
import org.macaroon3145.network.transcoder.MinecraftCompressionEncoder
import org.macaroon3145.network.transcoder.MinecraftCipherDecoder
import org.macaroon3145.network.transcoder.MinecraftCipherEncoder
import java.io.ByteArrayOutputStream
import java.io.DataOutputStream
import java.security.KeyPairGenerator
import java.security.SecureRandom
import java.util.UUID
import javax.crypto.Cipher
import javax.crypto.spec.IvParameterSpec
import javax.crypto.spec.SecretKeySpec

data class ConnectionProfile(
    val protocolVersion: Int,
    val username: String,
    val uuid: UUID,
    val properties: List<MojangProperty> = emptyList()
)

data class ClientSettings(
    val locale: String,
    val skinPartsMask: Int,
    val viewDistance: Int
)

class LoginHandler(private val protocolVersion: Int) : SimpleChannelInboundHandler<ByteBuf>() {
    private val secureRandom = SecureRandom()
    private var pendingUsername: String? = null
    private var verifyToken: ByteArray? = null

    override fun channelRead0(ctx: ChannelHandlerContext, buf: ByteBuf) {
        val packetId = NetworkUtils.readVarInt(buf)
        if (packetId == 0x00) {
            handleLoginStart(ctx, buf)
            return
        }

        if (ServerConfig.onlineMode && packetId == 0x01) {
            handleEncryptionResponse(ctx, buf)
            return
        }

        ctx.close()
    }

    private fun handleLoginStart(ctx: ChannelHandlerContext, buf: ByteBuf) {
        val username = NetworkUtils.readString(buf)
        val uuid = NetworkUtils.readUUID(buf)
        if (!ServerConfig.onlineMode) {
            completeLogin(ctx, ConnectionProfile(protocolVersion, username, uuid))
            return
        }

        pendingUsername = username
        val token = ByteArray(4)
        secureRandom.nextBytes(token)
        verifyToken = token
        ctx.writeAndFlush(createEncryptionRequest(token))
    }

    private fun handleEncryptionResponse(ctx: ChannelHandlerContext, buf: ByteBuf) {
        val expectedToken = verifyToken ?: run {
            ctx.close()
            return
        }

        val encryptedSecret = readByteArray(buf)
        val encryptedToken = readByteArray(buf)
        val rsaCipher = Cipher.getInstance("RSA")
        rsaCipher.init(Cipher.DECRYPT_MODE, keyPair.private)
        val secretBytes = rsaCipher.doFinal(encryptedSecret)
        val token = rsaCipher.doFinal(encryptedToken)
        if (!token.contentEquals(expectedToken)) {
            ctx.close()
            return
        }

        val sharedSecret = SecretKeySpec(secretBytes, "AES")
        enableEncryption(ctx, sharedSecret.encoded)

        val username = pendingUsername ?: run {
            ctx.close()
            return
        }

        MojangAuthService.verify(username, sharedSecret, keyPair.public).whenComplete { verified, err ->
            ctx.executor().execute {
                if (!ctx.channel().isActive) return@execute
                if (err != null || verified == null) {
                    ctx.close()
                    return@execute
                }
                completeLogin(
                    ctx,
                    ConnectionProfile(
                        protocolVersion = protocolVersion,
                        username = verified.username,
                        uuid = verified.uuid,
                        properties = verified.properties
                    )
                )
            }
        }
    }

    private fun completeLogin(ctx: ChannelHandlerContext, profile: ConnectionProfile) {
        if (ServerConfig.compressionThreshold >= 0) {
            ctx.write(createSetCompressionPacket(ServerConfig.compressionThreshold))
            enableCompression(ctx, ServerConfig.compressionThreshold)
        }
        ctx.pipeline().replace(this, "loginAckHandler", LoginAckHandler(profile))
        ctx.writeAndFlush(createLoginSuccessPacket(profile))
    }

    private fun createLoginSuccessPacket(profile: ConnectionProfile): ByteArray {
        val packet = ByteArrayOutputStream()
        NetworkUtils.writeVarInt(packet, 0x02)
        NetworkUtils.writeUUID(packet, profile.uuid)
        NetworkUtils.writeString(packet, profile.username)
        val out = DataOutputStream(packet)
        NetworkUtils.writeVarInt(packet, profile.properties.size)
        for (property in profile.properties) {
            NetworkUtils.writeString(packet, property.name)
            NetworkUtils.writeString(packet, property.value)
            if (property.signature == null) {
                out.writeBoolean(false)
            } else {
                out.writeBoolean(true)
                NetworkUtils.writeString(packet, property.signature)
            }
        }
        return packet.toByteArray()
    }

    private fun createEncryptionRequest(token: ByteArray): ByteArray {
        val packet = ByteArrayOutputStream()
        val out = DataOutputStream(packet)
        NetworkUtils.writeVarInt(packet, 0x01)
        NetworkUtils.writeString(packet, "")
        writeByteArray(packet, keyPair.public.encoded)
        writeByteArray(packet, token)
        out.writeBoolean(true)
        return packet.toByteArray()
    }

    private fun createSetCompressionPacket(threshold: Int): ByteArray {
        val packet = ByteArrayOutputStream()
        NetworkUtils.writeVarInt(packet, 0x03)
        NetworkUtils.writeVarInt(packet, threshold.coerceAtLeast(0))
        return packet.toByteArray()
    }

    private fun enableCompression(ctx: ChannelHandlerContext, threshold: Int) {
        val pipeline = ctx.pipeline()
        if (pipeline.get("compressionDecoder") == null) {
            pipeline.addAfter("frameDecoder", "compressionDecoder", MinecraftCompressionDecoder(threshold))
        }
        if (pipeline.get("compressionEncoder") == null) {
            pipeline.addAfter(
                "frameEncoder",
                "compressionEncoder",
                MinecraftCompressionEncoder(
                    threshold = threshold,
                    defaultLevel = ServerConfig.compressionLevel,
                    chunkLevel = ServerConfig.compressionChunkLevel
                )
            )
        }
    }

    private fun enableEncryption(ctx: ChannelHandlerContext, secret: ByteArray) {
        val decryptCipher = Cipher.getInstance("AES/CFB8/NoPadding")
        decryptCipher.init(Cipher.DECRYPT_MODE, SecretKeySpec(secret, "AES"), IvParameterSpec(secret))
        val encryptCipher = Cipher.getInstance("AES/CFB8/NoPadding")
        encryptCipher.init(Cipher.ENCRYPT_MODE, SecretKeySpec(secret, "AES"), IvParameterSpec(secret))
        ctx.pipeline().addBefore("frameDecoder", "cipherDecoder", MinecraftCipherDecoder(decryptCipher))
        ctx.pipeline().addBefore("frameEncoder", "cipherEncoder", MinecraftCipherEncoder(encryptCipher))
    }

    private fun readByteArray(buf: ByteBuf): ByteArray {
        val size = NetworkUtils.readVarInt(buf)
        val data = ByteArray(size)
        buf.readBytes(data)
        return data
    }

    private fun writeByteArray(stream: ByteArrayOutputStream, bytes: ByteArray) {
        NetworkUtils.writeVarInt(stream, bytes.size)
        stream.write(bytes)
    }

    companion object {
        private val keyPair = KeyPairGenerator.getInstance("RSA").apply {
            initialize(1024)
        }.generateKeyPair()
    }
}
