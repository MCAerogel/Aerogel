package org.macaroon3145.network.transcoder

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.ByteToMessageDecoder
import org.macaroon3145.network.NetworkUtils

class MinecraftVarIntFrameDecoder: ByteToMessageDecoder() {
    override fun decode(ctx: ChannelHandlerContext, inBuf: ByteBuf, out: MutableList<Any>) {
        inBuf.markReaderIndex()
        val length: Int
        try {
            length = NetworkUtils.readVarInt(inBuf)
        } catch (_: IndexOutOfBoundsException) {
            inBuf.resetReaderIndex()
            return
        }
        if (inBuf.readableBytes() < length) {
            inBuf.resetReaderIndex()
            return
        }
        out.add(inBuf.readBytes(length))
    }
}