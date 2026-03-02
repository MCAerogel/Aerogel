package org.macaroon3145.network.handler

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.SimpleChannelInboundHandler
import org.macaroon3145.Aerogel
import org.macaroon3145.network.NetworkUtils
import org.macaroon3145.network.packet.Description
import org.macaroon3145.network.packet.Players
import org.macaroon3145.network.packet.PlayerSample
import org.macaroon3145.network.packet.StatusPacket
import org.macaroon3145.network.packet.Version
import org.macaroon3145.perf.PerformanceMonitor
import java.io.ByteArrayOutputStream
import java.io.DataOutputStream

class StatusHandler(private val protocolVersion: Int) : SimpleChannelInboundHandler<ByteBuf>() {
    override fun channelRead0(ctx: ChannelHandlerContext, buf: ByteBuf) {
        val packetId = NetworkUtils.readVarInt(buf)
        when (packetId) {
            0x00 -> sendStatusResponse(ctx)
            0x01 -> sendPingResponse(ctx, buf)
            else -> ctx.close()
        }
    }

    private fun sendStatusResponse(ctx: ChannelHandlerContext) {
        val tps = PerformanceMonitor.tpsString()
        val mspt = PerformanceMonitor.msptString()
        val online = PlayerSessionManager.onlineCount()
        val samples: List<PlayerSample> = PlayerSessionManager.statusPlayerSamples()
        val packet = StatusPacket(
            version = Version(Aerogel.VERSION, protocolVersion),
            players = Players(max = 20, online = online, sample = samples),
            description = Description("§aAerogel\n§7TPS: $tps | MSPT: $mspt"),
            enforcesSecureChat = false
        )
        ctx.writeAndFlush(packet.serialize())
    }

    private fun sendPingResponse(ctx: ChannelHandlerContext, buf: ByteBuf) {
        if (buf.readableBytes() < 8) {
            ctx.close()
            return
        }

        val payload = buf.readLong()
        val packet = ByteArrayOutputStream()
        NetworkUtils.writeVarInt(packet, 0x01)
        DataOutputStream(packet).writeLong(payload)
        ctx.writeAndFlush(packet.toByteArray())
    }

    override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
        ctx.close()
    }
}
