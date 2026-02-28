package org.macaroon3145.network.transcoder

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.MessageToByteEncoder
import javax.crypto.Cipher

class MinecraftCipherEncoder(private val cipher: Cipher) : MessageToByteEncoder<ByteBuf>() {
    override fun encode(ctx: ChannelHandlerContext, msg: ByteBuf, out: ByteBuf) {
        val readable = msg.readableBytes()
        val plain = ByteArray(readable)
        msg.readBytes(plain)
        val encrypted = cipher.update(plain)
        if (encrypted != null && encrypted.isNotEmpty()) {
            out.writeBytes(encrypted)
        }
    }
}
