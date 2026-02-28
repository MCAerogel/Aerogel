package org.macaroon3145.network.handler

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.SimpleChannelInboundHandler
import org.macaroon3145.network.NetworkUtils

class LoginAckHandler(private val profile: ConnectionProfile) : SimpleChannelInboundHandler<ByteBuf>() {
    override fun channelRead0(ctx: ChannelHandlerContext, buf: ByteBuf) {
        val packetId = NetworkUtils.readVarInt(buf)
        if (packetId != 0x03) {
            ctx.close()
            return
        }

        ctx.pipeline().replace(this, "configurationHandler", ConfigurationHandler(profile))
        for (packet in ConfigurationPackets.registryDataPackets()) {
            ctx.writeAndFlush(packet)
        }
        ctx.writeAndFlush(ConfigurationPackets.featureFlagsPacket())
        ctx.writeAndFlush(ConfigurationPackets.tagsPacket())
        ctx.writeAndFlush(ConfigurationPackets.finishConfigurationPacket())
    }
}
