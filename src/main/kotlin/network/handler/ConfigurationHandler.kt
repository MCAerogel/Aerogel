package org.macaroon3145.network.handler

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.SimpleChannelInboundHandler
import org.macaroon3145.config.ServerConfig
import org.macaroon3145.network.NetworkUtils
import org.macaroon3145.world.FoliaSidecarSpawnPointProvider
import org.macaroon3145.world.WorldManager

class ConfigurationHandler(private val profile: ConnectionProfile) : SimpleChannelInboundHandler<ByteBuf>() {
    private var clientSettings: ClientSettings? = null

    override fun channelRead0(ctx: ChannelHandlerContext, buf: ByteBuf) {
        val packetId = NetworkUtils.readVarInt(buf)
        when (packetId) {
            0x00 -> handleClientInformation(buf)
            0x03 -> completeConfiguration(ctx)
            else -> return
        }
    }

    private fun completeConfiguration(ctx: ChannelHandlerContext) {
        val world = WorldManager.defaultWorld()
        val spawn = FoliaSidecarSpawnPointProvider.spawnPointFor(world.key)
            ?: world.spawnPointForPlayer(profile.uuid)
        val spawnX = spawn.x
        val spawnY = spawn.y
        val spawnZ = spawn.z
        val skinPartsMask = clientSettings?.skinPartsMask ?: 0x7F
        val viewDistance = clientSettings?.viewDistance ?: 10
        val join = PlayerSessionManager.prepareJoin(
            ctx = ctx,
            profile = profile,
            locale = clientSettings?.locale ?: "en_us",
            worldKey = world.key,
            skinPartsMask = skinPartsMask,
            requestedViewDistance = viewDistance,
            spawnX = spawnX,
            spawnY = spawnY,
            spawnZ = spawnZ
        )
        val session = join.session
        ctx.pipeline().replace(this, "playHandler", PlayHandler(profile, session))
        ctx.writeAndFlush(PlayPackets.loginPacket(session.entityId, world.key, session.gameMode))
        ctx.writeAndFlush(PlayPackets.serverBrandPacket("Aerogel"))
        ctx.writeAndFlush(PlayPackets.playerInfoPacket(profile, session.gameMode, session.pingMs))
        ctx.writeAndFlush(PlayPackets.commandsPacket(includeOperatorCommands = PlayerSessionManager.isOperatorSession(session)))
        ctx.writeAndFlush(PlayPackets.playerSkinPartsMetadataPacket(entityId = session.entityId, skinPartsMask = skinPartsMask))
        ctx.writeAndFlush(PlayPackets.updateViewDistancePacket(session.chunkRadius))
        ctx.writeAndFlush(PlayPackets.updateViewPositionPacket(session.centerChunkX, session.centerChunkZ))
        ctx.writeAndFlush(PlayPackets.spawnPositionPacket(world.key, session.x, session.y, session.z))
        ctx.writeAndFlush(
            PlayPackets.playerPositionPacket(
                teleportId = 1,
                x = session.x,
                y = session.y,
                z = session.z
            )
        )
        val (worldAge, timeOfDay) = PlayerSessionManager.worldTimeSnapshot(world.key)
        ctx.writeAndFlush(PlayPackets.timeUpdatePacket(worldAge = worldAge, timeOfDay = timeOfDay, tickDayTime = true))
        val playerTickRate = (20.0 * ServerConfig.playerTimeScale).toFloat()
        ctx.writeAndFlush(PlayPackets.tickingStatePacket(tickRate = playerTickRate, isFrozen = false))
        ctx.writeAndFlush(PlayPackets.tickingStepPacket(tickSteps = 0))
        ctx.writeAndFlush(PlayPackets.gameStateGameModePacket(session.gameMode))
        ctx.writeAndFlush(PlayPackets.gameStateImmediateRespawnPacket(enabled = false))
        ctx.writeAndFlush(PlayPackets.setHealthPacket(session.health, session.food, session.saturation))
        ctx.writeAndFlush(PlayPackets.gameStateStartLoadingPacket())
        PlayerSessionManager.finishJoin(join)
        PlayerSessionManager.triggerChunkStream(session.channelId)
    }

    private fun handleClientInformation(buf: ByteBuf) {
        val locale = readBoundedString(buf, 16) ?: return
        if (buf.readableBytes() < 1) return
        val viewDistance = buf.readByte().toInt().coerceAtLeast(0)
        NetworkUtils.readVarInt(buf) // chat mode
        if (buf.readableBytes() < 2) return
        buf.readBoolean() // chat colors
        val skinParts = buf.readUnsignedByte().toInt()
        NetworkUtils.readVarInt(buf) // main hand
        if (buf.readableBytes() < 2) return
        buf.readBoolean() // text filtering
        buf.readBoolean() // allow server listing

        clientSettings = ClientSettings(locale = locale, skinPartsMask = skinParts, viewDistance = viewDistance)
    }

    private fun readBoundedString(buf: ByteBuf, maxLength: Int): String? {
        val length = try {
            NetworkUtils.readVarInt(buf)
        } catch (_: Throwable) {
            return null
        }
        if (length < 0 || length > maxLength || buf.readableBytes() < length) {
            return null
        }
        val bytes = ByteArray(length)
        buf.readBytes(bytes)
        return String(bytes, Charsets.UTF_8)
    }

    override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
        ctx.close()
    }
}
