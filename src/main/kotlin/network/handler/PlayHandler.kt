package org.macaroon3145.network.handler

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.SimpleChannelInboundHandler
import org.macaroon3145.i18n.ServerI18n
import org.macaroon3145.network.NetworkUtils
import org.macaroon3145.world.BlockPos
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit

class PlayHandler(
    private val profile: ConnectionProfile,
    private val session: PlayerSession
) : SimpleChannelInboundHandler<ByteBuf>() {
    @Volatile
    private var clientSkinPartsMask: Int = session.skinPartsMask
    @Volatile
    private var lastKeepAliveResponseAt: Long = System.currentTimeMillis()
    private var keepAliveTask: ScheduledFuture<*>? = null

    override fun channelRead0(ctx: ChannelHandlerContext, buf: ByteBuf) {
        val packetId = NetworkUtils.readVarInt(buf)
        when (packetId) {
            0x06 -> handleChatCommand(buf, signed = false)
            0x07 -> handleChatCommand(buf, signed = true)
            0x08 -> handleChatMessage(buf)
            0x0E -> handleCommandSuggestion(buf)
            // Serverbound Client Information (play)
            0x0D -> handleClientInformation(buf)
            0x0B, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F, 0x20 -> {
                if (!handleKeepAliveOrMovement(packetId, buf)) {
                    tryHandleMovementByShape(packetId, buf)
                }
            }
            0x23, 0x24 -> handlePickItemFromBlock(buf)
            0x28 -> handlePlayerDigging(buf)
            0x29 -> handleEntityAction(buf)
            0x2A -> handlePlayerInput(buf)
            0x34 -> handleSetCarriedItem(buf)
            0x35 -> handleSetCommandBlock(buf)
            0x37 -> handleSetCreativeModeSlot(buf)
            0x3C -> handleAnimation(buf)
            0x3F -> handlePlayerBlockPlacement(buf)
            0x00 -> {
                // teleport confirm
            }
            else -> {
                // Fallback: protocol IDs can shift across minor versions.
                // Accept canonical movement payload shapes even if packetId differs.
                tryHandleMovementByShape(packetId, buf)
            }
        }
    }

    private fun handleChatMessage(buf: ByteBuf) {
        val message = readBoundedString(buf, 256) ?: return
        if (buf.readableBytes() < 8 + 8) return
        buf.readLong() // timestamp
        buf.readLong() // salt
        if (buf.readableBytes() < 1) return
        val hasSignature = buf.readBoolean()
        if (hasSignature) {
            if (buf.readableBytes() < 256) return
            buf.skipBytes(256)
        }
        try {
            NetworkUtils.readVarInt(buf) // ackOffset
        } catch (_: Throwable) {
            return
        }
        // FixedBitSet(20) => 3 bytes
        if (buf.readableBytes() < 3 + 1) return
        buf.skipBytes(3) // ackList
        buf.readByte() // checksum

        PlayerSessionManager.broadcastChat(session.channelId, message)
    }

    private fun handleChatCommand(buf: ByteBuf, signed: Boolean) {
        val command = readBoundedString(buf, 256) ?: return
        // Signed variant carries extra signature fields after command string.
        // We intentionally ignore them and execute server-side command logic.
        @Suppress("UNUSED_PARAMETER")
        val _signed = signed
        PlayerSessionManager.submitCommand(session.channelId, command)
    }

    private fun handleCommandSuggestion(buf: ByteBuf) {
        val requestId = try {
            NetworkUtils.readVarInt(buf)
        } catch (_: Throwable) {
            return
        }
        val input = readBoundedString(buf, 32500) ?: return
        PlayerSessionManager.sendCommandSuggestions(session.channelId, requestId, input)
    }

    private fun handleKeepAliveOrMovement(packetId: Int, buf: ByteBuf): Boolean {
        val readable = buf.readableBytes()
        // 1.21.x/1.21.11: handle both known and shifted IDs by payload shape.
        when {
            readable == (8 * 3 + 4 * 2 + 1) -> handleMovePositionRotation(buf) // 33 bytes
            readable == (8 * 3 + 1) -> handleMovePosition(buf) // 25 bytes
            readable == (4 * 2 + 1) -> handleMoveRotation(buf) // 9 bytes
            readable == 8 && (packetId == 0x0B || packetId == 0x1B) -> {
                val echoedId = buf.readLong()
                val now = System.currentTimeMillis()
                val rtt = (now - echoedId).coerceIn(0L, 60_000L).toInt()
                lastKeepAliveResponseAt = now
                PlayerSessionManager.updateAndBroadcastPing(session.channelId, rtt)
            }
            readable == 1 && (packetId == 0x1D || packetId == 0x20) -> handleMoveStatusOnly(buf) // 1 byte
            else -> return false
        }
        return true
    }

    private fun tryHandleMovementByShape(packetId: Int, buf: ByteBuf): Boolean {
        val readable = buf.readableBytes()
        return when (readable) {
            33 -> {
                handleMovePositionRotation(buf)
                true
            }
            25 -> {
                handleMovePosition(buf)
                true
            }
            9 -> {
                handleMoveRotation(buf)
                true
            }
            1 -> {
                if (packetId == 0x1D || packetId == 0x20) {
                    handleMoveStatusOnly(buf)
                    true
                } else {
                    false
                }
            }
            else -> false
        }
    }

    private fun handleClientInformation(buf: ByteBuf) {
        val locale = readBoundedString(buf, 16) ?: return
        if (buf.readableBytes() < 1) return
        val viewDistance = buf.readByte().toInt().coerceAtLeast(0)
        NetworkUtils.readVarInt(buf) // chat mode
        if (buf.readableBytes() < 2) return
        buf.readBoolean() // chat colors
        val skinParts = buf.readUnsignedByte().toInt() // model customization mask
        NetworkUtils.readVarInt(buf) // main hand
        if (buf.readableBytes() < 2) return
        buf.readBoolean() // text filtering
        buf.readBoolean() // allow server listing

        clientSkinPartsMask = skinParts
        PlayerSessionManager.updateViewDistance(session.channelId, viewDistance)
        ServerI18n.logCustom(
            ServerI18n.label("aerogel.label.client_settings_received"),
            ServerI18n.punct(": "),
            ServerI18n.style("user", ServerI18n.Color.GRAY),
            ServerI18n.punct("="),
            ServerI18n.style(profile.username, ServerI18n.Color.GREEN),
            ServerI18n.punct(", "),
            ServerI18n.style("locale", ServerI18n.Color.GRAY),
            ServerI18n.punct("="),
            ServerI18n.style(locale, ServerI18n.Color.GREEN),
            ServerI18n.punct(", "),
            ServerI18n.style("viewDistance", ServerI18n.Color.GRAY),
            ServerI18n.punct("="),
            ServerI18n.typed(viewDistance),
            ServerI18n.punct(", "),
            ServerI18n.style("skinPartsMask", ServerI18n.Color.GRAY),
            ServerI18n.punct("="),
            ServerI18n.style("0x${skinParts.toString(16)}", ServerI18n.Color.CYAN)
        )
    }

    private fun handleEntityAction(buf: ByteBuf) {
        try {
            NetworkUtils.readVarInt(buf) // entityId (client-side self id)
            val action = NetworkUtils.readVarInt(buf)
            NetworkUtils.readVarInt(buf) // jump boost
            when (action) {
                1 -> PlayerSessionManager.updateAndBroadcastPlayerState(session.channelId, sprinting = true)
                2 -> PlayerSessionManager.updateAndBroadcastPlayerState(session.channelId, sprinting = false)
            }
        } catch (_: Throwable) {
            return
        }
    }

    private fun handlePlayerInput(buf: ByteBuf) {
        if (buf.readableBytes() < 1) return
        val flags = buf.readUnsignedByte().toInt()
        val sneaking = (flags and (1 shl 5)) != 0
        val sprinting = (flags and (1 shl 6)) != 0
        val swimming = sprinting && sneaking && !session.onGround
        PlayerSessionManager.updateAndBroadcastPlayerState(
            session.channelId,
            sneaking = sneaking,
            sprinting = sprinting,
            swimming = swimming
        )
    }

    private fun handleAnimation(buf: ByteBuf) {
        val hand = try {
            NetworkUtils.readVarInt(buf)
        } catch (_: Throwable) {
            return
        }
        PlayerSessionManager.broadcastSwingAnimation(
            channelId = session.channelId,
            offHand = hand == 1
        )
    }

    private fun handlePickItemFromBlock(buf: ByteBuf) {
        if (buf.readableBytes() < 8) return
        val pos = readBlockPos(buf)
        if (buf.readableBytes() > 0) {
            buf.readBoolean() // includeData
        }
        PlayerSessionManager.setSelectedBlockFromWorld(session.channelId, pos.x, pos.y, pos.z)
    }

    private fun handleSetCarriedItem(buf: ByteBuf) {
        val slot = try {
            // Serverbound set carried item is a short in modern protocol.
            // Reading as VarInt pins slot to 0 for values 0..8 because high byte is 0.
            if (buf.readableBytes() >= 2) {
                buf.readShort().toInt()
            } else {
                NetworkUtils.readVarInt(buf)
            }
        } catch (_: Throwable) {
            return
        }
        PlayerSessionManager.updateSelectedHotbarSlot(session.channelId, slot)
    }

    private fun handleSetCreativeModeSlot(buf: ByteBuf) {
        if (buf.readableBytes() < 2) return
        val slot = buf.readShort().toInt()
        if (buf.readableBytes() <= 0) return
        val itemBytes = ByteArray(buf.readableBytes())
        buf.readBytes(itemBytes)
        PlayerSessionManager.applyCreativeSlot(session.channelId, slot, itemBytes)
    }

    private fun handleSetCommandBlock(buf: ByteBuf) {
        if (buf.readableBytes() < 8) return
        val pos = readBlockPos(buf)
        val command = readBoundedString(buf, 32767) ?: return
        val mode = try {
            NetworkUtils.readVarInt(buf)
        } catch (_: Throwable) {
            return
        }
        if (buf.readableBytes() < 1) return
        val flags = buf.readUnsignedByte().toInt()
        val trackOutput = (flags and 0x01) != 0
        val conditional = (flags and 0x02) != 0
        val automatic = (flags and 0x04) != 0

        // Mode is currently consumed for protocol correctness.
        // State-specific command block mode handling can be added on top of this persisted payload path.
        @Suppress("UNUSED_VARIABLE")
        val _mode = mode

        PlayerSessionManager.updateCommandBlock(
            channelId = session.channelId,
            x = pos.x,
            y = pos.y,
            z = pos.z,
            command = command,
            trackOutput = trackOutput,
            conditional = conditional,
            automatic = automatic
        )
    }

    private fun handlePlayerDigging(buf: ByteBuf) {
        val action = try {
            NetworkUtils.readVarInt(buf)
        } catch (_: Throwable) {
            return
        }
        if (buf.readableBytes() < 9) return
        val pos = readBlockPos(buf)
        if (buf.readableBytes() < 1) return
        buf.readUnsignedByte() // face
        if (buf.readableBytes() > 0) {
            try {
                NetworkUtils.readVarInt(buf) // sequence (1.19+)
            } catch (_: Throwable) {
                // ignore
            }
        }
        // START_DIGGING or FINISHED_DIGGING
        if (action == 0 || action == 2) {
            PlayerSessionManager.breakBlock(session.channelId, pos.x, pos.y, pos.z)
            return
        }
        // DROP_ITEM_STACK / DROP_ITEM
        if (action == 3 || action == 4) {
            PlayerSessionManager.dropSelectedItem(
                channelId = session.channelId,
                dropStack = action == 3
            )
            return
        }
        // SWAP_ITEM_WITH_OFFHAND
        if (action == 6) {
            PlayerSessionManager.swapMainHandWithOffhand(session.channelId)
        }
    }

    private fun handlePlayerBlockPlacement(buf: ByteBuf) {
        val hand = try {
            NetworkUtils.readVarInt(buf) // hand: 0=main, 1=offhand
        } catch (_: Throwable) {
            return
        }
        try {
            if (hand !in 0..1) return
        } catch (_: Throwable) {
            return
        }
        if (buf.readableBytes() < 8 + 1) return
        val clicked = readBlockPos(buf)
        val faceId = try {
            NetworkUtils.readVarInt(buf)
        } catch (_: Throwable) {
            return
        }
        if (buf.readableBytes() < 4 * 3 + 1) return
        val cursorX = buf.readFloat()
        val cursorY = buf.readFloat()
        val cursorZ = buf.readFloat()
        buf.readBoolean() // insideBlock
        if (buf.readableBytes() >= 1) {
            buf.readBoolean() // worldBorderHit (1.21.2+)
        }
        if (buf.readableBytes() > 0) {
            try {
                NetworkUtils.readVarInt(buf) // sequence
            } catch (_: Throwable) {
                // ignore
            }
        }

        val target = offsetByFace(clicked, faceId)
        PlayerSessionManager.placeSelectedBlock(
            channelId = session.channelId,
            x = target.x,
            y = target.y,
            z = target.z,
            hand = hand,
            faceId = faceId,
            cursorX = cursorX,
            cursorY = cursorY,
            cursorZ = cursorZ
        )
    }

    private fun handleMovePosition(buf: ByteBuf) {
        if (buf.readableBytes() < 8 * 3 + 1) return
        val x = buf.readDouble()
        val y = buf.readDouble()
        val z = buf.readDouble()
        val flags = buf.readUnsignedByte().toInt()
        val onGround = (flags and 0x01) != 0
        PlayerSessionManager.updateAndBroadcastMovement(
            session.channelId,
            x,
            y,
            z,
            session.yaw,
            session.pitch,
            onGround
        )
    }

    private fun handleMovePositionRotation(buf: ByteBuf) {
        if (buf.readableBytes() < 8 * 3 + 4 * 2 + 1) return
        val x = buf.readDouble()
        val y = buf.readDouble()
        val z = buf.readDouble()
        val yaw = buf.readFloat()
        val pitch = buf.readFloat()
        val flags = buf.readUnsignedByte().toInt()
        val onGround = (flags and 0x01) != 0
        PlayerSessionManager.updateAndBroadcastMovement(
            session.channelId,
            x,
            y,
            z,
            yaw,
            pitch,
            onGround
        )
    }

    private fun handleMoveRotation(buf: ByteBuf) {
        if (buf.readableBytes() < 4 * 2 + 1) return
        val yaw = buf.readFloat()
        val pitch = buf.readFloat()
        val flags = buf.readUnsignedByte().toInt()
        val onGround = (flags and 0x01) != 0
        PlayerSessionManager.updateAndBroadcastMovement(
            session.channelId,
            session.x,
            session.y,
            session.z,
            yaw,
            pitch,
            onGround
        )
    }

    private fun handleMoveStatusOnly(buf: ByteBuf) {
        if (buf.readableBytes() < 1) return
        val flags = buf.readUnsignedByte().toInt()
        val onGround = (flags and 0x01) != 0
        PlayerSessionManager.updateAndBroadcastMovement(
            session.channelId,
            session.x,
            session.y,
            session.z,
            session.yaw,
            session.pitch,
            onGround
        )
    }

    override fun handlerAdded(ctx: ChannelHandlerContext) {
        lastKeepAliveResponseAt = System.currentTimeMillis()
        keepAliveTask = ctx.executor().scheduleAtFixedRate(
            {
                if (!ctx.channel().isActive) return@scheduleAtFixedRate
                val now = System.currentTimeMillis()
                ctx.writeAndFlush(PlayPackets.keepAlivePacket(now))
            },
            5,
            5,
            TimeUnit.SECONDS
        )
    }

    override fun channelInactive(ctx: ChannelHandlerContext) {
        keepAliveTask?.cancel(false)
        PlayerSessionManager.leave(session.channelId)
        super.channelInactive(ctx)
    }

    override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
        keepAliveTask?.cancel(false)
        PlayerSessionManager.leave(session.channelId)
        ctx.close()
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

    private fun readBlockPos(buf: ByteBuf): BlockPos {
        val packed = buf.readLong()
        val x = (packed shr 38).toInt()
        val y = (packed shl 52 shr 52).toInt()
        val z = (packed shl 26 shr 38).toInt()
        return BlockPos(x, y, z)
    }

    private fun offsetByFace(pos: BlockPos, face: Int): BlockPos {
        return when (face) {
            0 -> BlockPos(pos.x, pos.y - 1, pos.z) // down
            1 -> BlockPos(pos.x, pos.y + 1, pos.z) // up
            2 -> BlockPos(pos.x, pos.y, pos.z - 1) // north
            3 -> BlockPos(pos.x, pos.y, pos.z + 1) // south
            4 -> BlockPos(pos.x - 1, pos.y, pos.z) // west
            5 -> BlockPos(pos.x + 1, pos.y, pos.z) // east
            else -> pos
        }
    }
}
