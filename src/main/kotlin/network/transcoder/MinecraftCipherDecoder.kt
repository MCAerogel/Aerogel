package org.macaroon3145.network.transcoder

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.ByteToMessageDecoder
import javax.crypto.Cipher

class MinecraftCipherDecoder(private val cipher: Cipher) : ByteToMessageDecoder() {
    override fun decode(ctx: ChannelHandlerContext, input: ByteBuf, out: MutableList<Any>) {
        if (!input.isReadable) return
        val readable = input.readableBytes()
        val encrypted = ByteArray(readable)
        input.readBytes(encrypted)
        val decrypted = cipher.update(encrypted)
        if (decrypted != null && decrypted.isNotEmpty()) {
            out.add(ctx.alloc().buffer(decrypted.size).writeBytes(decrypted))
        }
    }
}
