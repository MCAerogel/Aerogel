package org.macaroon3145.network.transcoder

import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.MessageToMessageEncoder
import org.macaroon3145.network.NetworkUtils
import java.io.ByteArrayOutputStream
import java.util.zip.Deflater

class MinecraftCompressionEncoder(
    private val threshold: Int,
    defaultLevel: Int,
    private val chunkLevel: Int
) : MessageToMessageEncoder<ByteArray>() {
    private val defaultLevel = defaultLevel.coerceIn(0, 9)

    private class DeflaterState(initialLevel: Int) {
        val deflater = Deflater(initialLevel)
        var level = initialLevel
        val temp = ByteArray(16 * 1024)
    }

    private val stateLocal = ThreadLocal.withInitial { DeflaterState(defaultLevel) }

    override fun encode(ctx: ChannelHandlerContext, msg: ByteArray, out: MutableList<Any>) {
        if (threshold < 0) {
            out.add(msg)
            return
        }

        if (msg.size < threshold) {
            val packet = ByteArrayOutputStream(msg.size + 5)
            NetworkUtils.writeVarInt(packet, 0)
            packet.write(msg)
            out.add(packet.toByteArray())
            return
        }

        val level = compressionLevelForPacket(msg)
        if (level < 0) {
            val packet = ByteArrayOutputStream(msg.size + 5)
            NetworkUtils.writeVarInt(packet, 0)
            packet.write(msg)
            out.add(packet.toByteArray())
            return
        }
        val compressed = deflate(msg, level)
        val packet = ByteArrayOutputStream(compressed.size + 5)
        NetworkUtils.writeVarInt(packet, msg.size)
        packet.write(compressed)
        out.add(packet.toByteArray())
    }

    override fun handlerRemoved(ctx: ChannelHandlerContext) {
        // Avoid keeping native zlib state around after pipeline teardown.
        val state = stateLocal.get()
        state.deflater.end()
        stateLocal.remove()
        super.handlerRemoved(ctx)
    }

    private fun compressionLevelForPacket(msg: ByteArray): Int {
        val packetId = readLeadingVarIntOrMinusOne(msg)
        return if (packetId == CHUNK_WITH_LIGHT_PACKET_ID) {
            chunkLevel.coerceIn(-1, 9)
        } else {
            defaultLevel
        }
    }

    private fun deflate(bytes: ByteArray, level: Int): ByteArray {
        val state = stateLocal.get()
        if (state.level != level) {
            state.deflater.setLevel(level)
            state.level = level
        }

        val deflater = state.deflater
        deflater.reset()
        deflater.setInput(bytes)
        deflater.finish()

        val output = ByteArrayOutputStream((bytes.size * 3) / 4)
        val buffer = state.temp
        while (!deflater.finished()) {
            val n = deflater.deflate(buffer)
            if (n <= 0) break
            output.write(buffer, 0, n)
        }
        return output.toByteArray()
    }

    private fun readLeadingVarIntOrMinusOne(bytes: ByteArray): Int {
        var numRead = 0
        var result = 0
        while (numRead < 5 && numRead < bytes.size) {
            val read = bytes[numRead].toInt() and 0xFF
            val value = read and 0x7F
            result = result or (value shl (7 * numRead))
            numRead++
            if ((read and 0x80) == 0) {
                return result
            }
        }
        return -1
    }

    private companion object {
        // protocol 1.21.11 clientbound level_chunk_with_light
        private const val CHUNK_WITH_LIGHT_PACKET_ID = 0x2C
    }
}
