package org.macaroon3145.network.handler

import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelDuplexHandler
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelPromise
import org.macaroon3145.api.packet.PacketDirection
import org.macaroon3145.api.packet.PacketEnvelope
import org.macaroon3145.plugin.PluginSystem
import org.macaroon3145.network.NetworkUtils

class PluginPacketBridgeHandler : ChannelDuplexHandler() {
    override fun handlerAdded(ctx: ChannelHandlerContext) {
        ProtocolPhaseTracker.update(ctx.channel(), ProtocolPhaseTracker.current(ctx.channel()))
        super.handlerAdded(ctx)
    }

    override fun channelRead(ctx: ChannelHandlerContext, msg: Any) {
        if (msg !is ByteBuf) {
            super.channelRead(ctx, msg)
            return
        }

        val transformed = transformInbound(ctx, msg)
        if (transformed == null) {
            msg.release()
            return
        }

        if (transformed === msg) {
            super.channelRead(ctx, msg)
        } else {
            msg.release()
            super.channelRead(ctx, transformed)
        }
    }

    override fun write(ctx: ChannelHandlerContext, msg: Any, promise: ChannelPromise) {
        if (msg !is ByteArray) {
            super.write(ctx, msg, promise)
            return
        }

        val transformed = transformOutbound(ctx, msg)
        if (transformed == null) {
            promise.setSuccess()
            return
        }

        super.write(ctx, transformed, promise)
    }

    private fun transformInbound(ctx: ChannelHandlerContext, msg: ByteBuf): ByteBuf? {
        val duplicate = msg.duplicate()
        val packetId = runCatching { NetworkUtils.readVarInt(duplicate) }.getOrNull() ?: return msg
        val payload = ByteArray(duplicate.readableBytes())
        duplicate.readBytes(payload)
        val phase = ProtocolPhaseTracker.current(ctx.channel())

        val packet = PacketEnvelope(
            direction = PacketDirection.INBOUND,
            phase = phase,
            packetId = packetId,
            payload = payload,
            channelKey = ctx.channel().id().asShortText()
        )
        val intercepted = runCatching { PluginSystem.interceptPacket(packet) }
            .getOrElse { error ->
                ctx.fireExceptionCaught(error)
                packet
            } ?: return null
        if (intercepted === packet) {
            return msg
        }
        return intercepted.toByteBuf(ctx)
    }

    private fun transformOutbound(ctx: ChannelHandlerContext, msg: ByteArray): ByteArray? {
        val packetId = readVarIntFromByteArray(msg) ?: return msg
        val payloadOffset = varIntSize(packetId)
        if (payloadOffset > msg.size) return msg
        val payload = msg.copyOfRange(payloadOffset, msg.size)
        val phase = ProtocolPhaseTracker.current(ctx.channel())

        val packet = PacketEnvelope(
            direction = PacketDirection.OUTBOUND,
            phase = phase,
            packetId = packetId,
            payload = payload,
            channelKey = ctx.channel().id().asShortText()
        )
        val intercepted = runCatching { PluginSystem.interceptPacket(packet) }
            .getOrElse { error ->
                ctx.fireExceptionCaught(error)
                packet
            } ?: return null
        if (intercepted === packet) {
            return msg
        }
        return intercepted.toByteArray()
    }

    private fun PacketEnvelope.toByteArray(): ByteArray {
        val out = java.io.ByteArrayOutputStream()
        NetworkUtils.writeVarInt(out, packetId)
        out.write(payload)
        return out.toByteArray()
    }

    private fun PacketEnvelope.toByteBuf(ctx: ChannelHandlerContext): ByteBuf {
        val out = Unpooled.buffer()
        NetworkUtils.writeVarInt(out, packetId)
        out.writeBytes(payload)
        return out
    }

    private fun readVarIntFromByteArray(bytes: ByteArray): Int? {
        var numRead = 0
        var result = 0
        while (numRead < bytes.size) {
            val read = bytes[numRead].toInt() and 0xFF
            val value = read and 0x7F
            result = result or (value shl (7 * numRead))
            numRead++
            if (numRead > 5) return null
            if ((read and 0x80) == 0) {
                return result
            }
        }
        return null
    }

    private fun varIntSize(value: Int): Int {
        var v = value
        var size = 0
        do {
            v = v ushr 7
            size++
        } while (v != 0)
        return size
    }
}
