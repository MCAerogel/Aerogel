package org.macaroon3145.network.handler

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.SimpleChannelInboundHandler
import org.macaroon3145.api.packet.ProtocolPhase
import org.macaroon3145.network.NetworkUtils

class HandshakeHandler : SimpleChannelInboundHandler<ByteBuf>() {
    override fun channelRead0(ctx: ChannelHandlerContext, buf: ByteBuf) {
        if (!NetworkUtils.isHandshakePacket(buf)) {
            ctx.fireChannelRead(buf.retain())
            return
        }
        val handshake = NetworkUtils.readHandshake(buf)
        when (handshake.nextState) {
            1 -> {
                ProtocolPhaseTracker.update(ctx.channel(), ProtocolPhase.STATUS)
                ctx.pipeline().replace(
                    this,
                    "statusHandler",
                    StatusHandler(handshake.protocolVersion)
                )
            }
            2 -> {
                ProtocolPhaseTracker.update(ctx.channel(), ProtocolPhase.LOGIN)
                ctx.pipeline().replace(this, "loginHandler", LoginHandler(handshake.protocolVersion))
            }
        }
    }

    override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
        ctx.close()
    }
}
