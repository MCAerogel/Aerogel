package org.macaroon3145.network.transcoder

import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.DecoderException
import io.netty.handler.codec.MessageToMessageDecoder
import org.macaroon3145.network.NetworkUtils
import java.util.zip.DataFormatException
import java.util.zip.Inflater

class MinecraftCompressionDecoder(private val threshold: Int) : MessageToMessageDecoder<ByteBuf>() {
    private val inflaterLocal = ThreadLocal.withInitial { Inflater() }

    override fun decode(ctx: ChannelHandlerContext, msg: ByteBuf, out: MutableList<Any>) {
        val expectedUncompressedLength = NetworkUtils.readVarInt(msg)
        if (expectedUncompressedLength == 0) {
            out.add(msg.readRetainedSlice(msg.readableBytes()))
            return
        }

        if (threshold >= 0 && expectedUncompressedLength < threshold) {
            throw DecoderException(
                "Badly compressed packet - size of $expectedUncompressedLength is below threshold $threshold"
            )
        }

        val compressed = ByteArray(msg.readableBytes())
        msg.readBytes(compressed)
        val uncompressed = inflate(inflaterLocal.get(), compressed, expectedUncompressedLength)
        out.add(Unpooled.wrappedBuffer(uncompressed))
    }

    override fun handlerRemoved(ctx: ChannelHandlerContext) {
        val inflater = inflaterLocal.get()
        inflater.end()
        inflaterLocal.remove()
        super.handlerRemoved(ctx)
    }

    private fun inflate(inflater: Inflater, compressed: ByteArray, expectedLength: Int): ByteArray {
        val out = ByteArray(expectedLength)
        return try {
            inflater.reset()
            inflater.setInput(compressed)
            var offset = 0
            while (!inflater.finished() && offset < out.size) {
                val read = inflater.inflate(out, offset, out.size - offset)
                if (read <= 0) {
                    if (inflater.needsInput()) break
                    throw DecoderException("Inflater stalled while decoding compressed packet")
                }
                offset += read
            }
            if (offset != expectedLength || !inflater.finished()) {
                throw DecoderException(
                    "Mismatched decompressed size: expected=$expectedLength, actual=$offset"
                )
            }
            out
        } catch (e: DataFormatException) {
            throw DecoderException("Invalid compressed packet payload", e)
        }
    }
}
