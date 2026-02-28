package org.macaroon3145.network.transcoder

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.MessageToByteEncoder
import org.macaroon3145.network.NetworkUtils
import java.io.ByteArrayOutputStream

class MinecraftVarIntFrameEncoder : MessageToByteEncoder<ByteArray>() {
    override fun encode(ctx: ChannelHandlerContext, msg: ByteArray, out: ByteBuf) {
        val lengthBuffer = ByteArrayOutputStream()
        NetworkUtils.writeVarInt(lengthBuffer, msg.size)
        val lengthBytes = lengthBuffer.toByteArray()
        out.writeBytes(lengthBytes)
        out.writeBytes(msg)
    }
}