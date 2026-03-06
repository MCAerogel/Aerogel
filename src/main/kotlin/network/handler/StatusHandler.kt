package org.macaroon3145.network.handler

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.SimpleChannelInboundHandler
import org.macaroon3145.Aerogel
import org.macaroon3145.config.ServerConfig
import org.macaroon3145.network.NetworkUtils
import org.macaroon3145.network.packet.Description
import org.macaroon3145.network.packet.Players
import org.macaroon3145.network.packet.PlayerSample
import org.macaroon3145.network.packet.StatusPacket
import org.macaroon3145.network.packet.Version
import org.macaroon3145.perf.PerformanceMonitor
import java.awt.Graphics2D
import java.awt.RenderingHints
import java.awt.image.BufferedImage
import java.io.ByteArrayOutputStream
import java.io.DataOutputStream
import java.nio.file.Files
import java.nio.file.Path
import java.util.Base64
import java.util.concurrent.atomic.AtomicBoolean
import javax.imageio.ImageIO
import org.slf4j.LoggerFactory

class StatusHandler(private val protocolVersion: Int) : SimpleChannelInboundHandler<ByteBuf>() {
    private companion object {
        private val logger = LoggerFactory.getLogger(StatusHandler::class.java)
        private val iconPath: Path = Path.of("icon.png")
        private const val ICON_SIZE = 64
        private val iconCacheLoading = AtomicBoolean(false)

        @Volatile
        private var cachedFavicon: String? = null

        @Volatile
        private var iconCacheInitialized: Boolean = false
    }

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
            players = Players(max = ServerConfig.maxPlayers, online = online, sample = samples),
            description = Description("§aAerogel\n§7TPS: $tps | MSPT: $mspt"),
            favicon = loadRootIconFavicon(),
            enforcesSecureChat = false
        )
        ctx.writeAndFlush(packet.serialize())
    }

    private fun loadRootIconFavicon(): String? {
        if (iconCacheInitialized) {
            return cachedFavicon
        }
        if (iconCacheLoading.compareAndSet(false, true)) {
            try {
                if (iconCacheInitialized) {
                    return cachedFavicon
                }
                return initializeCachedIconOnce()
            } finally {
                iconCacheLoading.set(false)
            }
        }
        return cachedFavicon
    }

    private fun initializeCachedIconOnce(): String? {
        return try {
            val encoded = if (!Files.isRegularFile(iconPath)) {
                null
            } else {
                val iconBytes = normalizeToMinecraftIconBytes(Files.readAllBytes(iconPath))
                if (iconBytes == null || iconBytes.isEmpty()) null
                else "data:image/png;base64," + Base64.getEncoder().encodeToString(iconBytes)
            }
            cachedFavicon = encoded
            iconCacheInitialized = true
            encoded
        } catch (t: Throwable) {
            logger.warn("Failed to load favicon from {}", iconPath.toAbsolutePath(), t)
            iconCacheInitialized = true
            cachedFavicon
        }
    }

    private fun normalizeToMinecraftIconBytes(sourceBytes: ByteArray): ByteArray? {
        if (sourceBytes.isEmpty()) return null
        val source = ImageIO.read(sourceBytes.inputStream()) ?: return null
        val srcW = source.width.coerceAtLeast(1)
        val srcH = source.height.coerceAtLeast(1)
        val square = minOf(srcW, srcH)
        val srcX = (srcW - square) / 2
        val srcY = (srcH - square) / 2

        val out = BufferedImage(ICON_SIZE, ICON_SIZE, BufferedImage.TYPE_INT_ARGB)
        val g = out.createGraphics()
        try {
            configureHighQuality(g)
            g.drawImage(
                source,
                0, 0, ICON_SIZE, ICON_SIZE,
                srcX, srcY, srcX + square, srcY + square,
                null
            )
        } finally {
            g.dispose()
        }

        return ByteArrayOutputStream().use { baos ->
            if (!ImageIO.write(out, "png", baos)) {
                null
            } else {
                baos.toByteArray()
            }
        }
    }

    private fun configureHighQuality(g: Graphics2D) {
        g.setRenderingHint(RenderingHints.KEY_INTERPOLATION, RenderingHints.VALUE_INTERPOLATION_BICUBIC)
        g.setRenderingHint(RenderingHints.KEY_RENDERING, RenderingHints.VALUE_RENDER_QUALITY)
        g.setRenderingHint(RenderingHints.KEY_ALPHA_INTERPOLATION, RenderingHints.VALUE_ALPHA_INTERPOLATION_QUALITY)
        g.setRenderingHint(RenderingHints.KEY_COLOR_RENDERING, RenderingHints.VALUE_COLOR_RENDER_QUALITY)
        g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON)
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
