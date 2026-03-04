package org.macaroon3145.network.handler

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.SimpleChannelInboundHandler
import org.macaroon3145.network.NetworkUtils
import org.macaroon3145.world.BlockPos
import org.slf4j.LoggerFactory
import java.io.IOException
import java.nio.channels.ClosedChannelException
import java.util.Locale
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit

class PlayHandler(
    private val profile: ConnectionProfile,
    private val session: PlayerSession
) : SimpleChannelInboundHandler<ByteBuf>() {
    private companion object {
        private val logger = LoggerFactory.getLogger(PlayHandler::class.java)
        private val placementDebugEnabled: Boolean =
            System.getProperty("aerogel.debug.placement")?.toBooleanStrictOrNull() ?: false
    }

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
            0x0B -> handleClientStatus(buf)
            0x0E -> handleCommandSuggestion(buf)
            0x11 -> handleContainerClick(buf)
            0x12 -> handleContainerClose(buf)
            // Vanilla 1.21.11 C2S Play: PlayPackets.INTERACT (PlayerInteractEntityC2SPacket) = 0x19.
            0x19 -> handleUseEntity(buf)
            // Serverbound Client Information (play)
            0x0D -> handleClientInformation(buf)
            0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F, 0x20 -> {
                if (!handleKeepAliveOrMovement(packetId, buf)) {
                    tryHandleMovementByShape(packetId, buf)
                }
            }
            // Vanilla 1.21.11 C2S Play: ServerboundMoveVehiclePacket = 0x21.
            0x21 -> handleMoveVehicle(buf)
            // Vanilla 1.21.11 C2S Play: UpdatePlayerAbilitiesC2SPacket = 0x27.
            0x27 -> handleUpdatePlayerAbilities(buf)
            0x23, 0x24 -> handlePickItemFromBlock(buf)
            0x28 -> handlePlayerDigging(buf)
            0x29 -> handleEntityAction(buf)
            0x2A -> handlePlayerInput(buf)
            0x34 -> handleSetCarriedItem(buf)
            0x35 -> handleSetCommandBlock(buf)
            0x37 -> handleSetCreativeModeSlot(buf)
            0x3C -> handleAnimation(buf)
            0x3F -> handlePlayerBlockPlacement(buf)
            0x40 -> handleUseItem(buf)
            0x00 -> {
                // teleport confirm
            }
            else -> {
                // Fallback: protocol IDs can shift across minor versions.
                // Accept canonical movement payload shapes even if packetId differs.
                if (tryHandlePlayerInputByShape(packetId, buf)) return
                if (tryHandleEntityActionByShape(packetId, buf)) return
                if (packetId == 0x27 && tryHandlePlayerAbilitiesByShape(packetId, buf)) return
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

    private fun handleClientStatus(buf: ByteBuf) {
        val action = try {
            NetworkUtils.readVarInt(buf)
        } catch (_: Throwable) {
            return
        }
        // Vanilla ClientStatusC2SPacket:
        // 0 = PERFORM_RESPAWN, 1 = REQUEST_STATS
        if (action == 0) {
            PlayerSessionManager.respawnIfDead(session.channelId)
        }
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
                val nowNano = System.nanoTime()
                val rttMsExact = ((nowNano - echoedId).toDouble() / 1_000_000.0).coerceIn(0.0, 60_000.0)
                lastKeepAliveResponseAt = System.currentTimeMillis()
                PlayerSessionManager.updateAndBroadcastPing(session.channelId, rttMsExact)
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

    private fun tryHandlePlayerAbilitiesByShape(packetId: Int, buf: ByteBuf): Boolean {
        if (packetId != 0x27) return false
        if (buf.readableBytes() != 1) return false
        val flags = buf.readUnsignedByte().toInt()
        val flying = (flags and 0x02) != 0
        PlayerSessionManager.updatePlayerFlyingState(session.channelId, flying)
        return true
    }

    private fun handleUpdatePlayerAbilities(buf: ByteBuf) {
        if (buf.readableBytes() < 1) return
        val flags = buf.readUnsignedByte().toInt()
        val flying = (flags and 0x02) != 0
        PlayerSessionManager.updatePlayerFlyingState(session.channelId, flying)
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
        PlayerSessionManager.updateLocale(session.channelId, locale)
        PlayerSessionManager.updateViewDistance(session.channelId, viewDistance)
    }

    private fun handleEntityAction(buf: ByteBuf) {
        try {
            val entityId = NetworkUtils.readVarInt(buf) // client-side self id
            val action = NetworkUtils.readVarInt(buf)
            NetworkUtils.readVarInt(buf) // jump boost
            if (entityId != session.entityId) return
            applyEntityAction(action)
        } catch (_: Throwable) {
            return
        }
    }

    private fun applyEntityAction(action: Int) {
        when (action) {
            0 -> {
                PlayerSessionManager.updateAndBroadcastPlayerState(session.channelId, sneaking = true)
                PlayerSessionManager.handlePlayerDismountRequest(session.channelId)
            }
            1 -> PlayerSessionManager.updateAndBroadcastPlayerState(session.channelId, sneaking = false)
            3 -> PlayerSessionManager.updateAndBroadcastPlayerState(session.channelId, sprinting = true)
            4 -> PlayerSessionManager.updateAndBroadcastPlayerState(session.channelId, sprinting = false)
        }
    }

    private fun tryHandleEntityActionByShape(packetId: Int, buf: ByteBuf): Boolean {
        if (buf.readableBytes() !in 3..12) return false
        if (packetId !in 0x26..0x2E) return false
        buf.markReaderIndex()
        val entityId = try {
            NetworkUtils.readVarInt(buf)
        } catch (_: Throwable) {
            buf.resetReaderIndex()
            return false
        }
        val action = try {
            NetworkUtils.readVarInt(buf)
        } catch (_: Throwable) {
            buf.resetReaderIndex()
            return false
        }
        val jumpBoost = try {
            NetworkUtils.readVarInt(buf)
        } catch (_: Throwable) {
            buf.resetReaderIndex()
            return false
        }
        if (buf.readableBytes() != 0) {
            buf.resetReaderIndex()
            return false
        }
        if (entityId != session.entityId) {
            buf.resetReaderIndex()
            return false
        }
        if (action !in 0..8 || jumpBoost !in 0..255) {
            buf.resetReaderIndex()
            return false
        }
        applyEntityAction(action)
        return true
    }

    private fun handlePlayerInput(buf: ByteBuf) {
        if (buf.readableBytes() < 1) return
        val flags = buf.readUnsignedByte().toInt()
        val forward = (flags and (1 shl 0)) != 0
        val backward = (flags and (1 shl 1)) != 0
        val left = (flags and (1 shl 2)) != 0
        val right = (flags and (1 shl 3)) != 0
        val jump = (flags and (1 shl 4)) != 0
        val sneaking = (flags and (1 shl 5)) != 0
        val sprinting = (flags and (1 shl 6)) != 0
        val swimming = (flags and (1 shl 7)) != 0
        PlayerSessionManager.updatePlayerInputState(
            channelId = session.channelId,
            forward = forward,
            backward = backward,
            left = left,
            right = right,
            jump = jump,
            sneaking = sneaking,
            sprinting = sprinting,
            swimming = swimming
        )
    }

    private fun tryHandlePlayerInputByShape(packetId: Int, buf: ByteBuf): Boolean {
        if (buf.readableBytes() != 1) return false
        // Avoid stealing known on-ground status packets.
        if (packetId == 0x1D || packetId == 0x20) return false
        if (packetId !in 0x24..0x30) return false
        buf.markReaderIndex()
        val flags = try {
            buf.readUnsignedByte().toInt()
        } catch (_: Throwable) {
            buf.resetReaderIndex()
            return false
        }
        val forward = (flags and (1 shl 0)) != 0
        val backward = (flags and (1 shl 1)) != 0
        val left = (flags and (1 shl 2)) != 0
        val right = (flags and (1 shl 3)) != 0
        val jump = (flags and (1 shl 4)) != 0
        val sneaking = (flags and (1 shl 5)) != 0
        val sprinting = (flags and (1 shl 6)) != 0
        val swimming = (flags and (1 shl 7)) != 0
        PlayerSessionManager.updatePlayerInputState(
            channelId = session.channelId,
            forward = forward,
            backward = backward,
            left = left,
            right = right,
            jump = jump,
            sneaking = sneaking,
            sprinting = sprinting,
            swimming = swimming
        )
        return true
    }

    private fun handleMoveVehicle(buf: ByteBuf) {
        if (session.ridingAnimalEntityId < 0) return
        val expectedBytes = (8 * 3) + (4 * 2) + 1 // x,y,z,yaw,pitch,onGround
        if (buf.readableBytes() != expectedBytes) return
        buf.markReaderIndex()
        try {
            val x = buf.readDouble()
            val y = buf.readDouble()
            val z = buf.readDouble()
            val yaw = buf.readFloat()
            val pitch = buf.readFloat()
            val onGround = buf.readBoolean()
            if (buf.readableBytes() != 0) {
                buf.resetReaderIndex()
                return
            } else {
                PlayerSessionManager.captureClientVehicleMove(
                    channelId = session.channelId,
                    x = x,
                    y = y,
                    z = z,
                    yaw = yaw,
                    pitch = pitch,
                    onGround = onGround
                )
            }
        } catch (_: Throwable) {
            buf.resetReaderIndex()
        }
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

    private fun handleUseEntity(buf: ByteBuf) {
        val targetEntityId = try {
            NetworkUtils.readVarInt(buf)
        } catch (_: Throwable) {
            return
        }
        val actionType = try {
            NetworkUtils.readVarInt(buf)
        } catch (_: Throwable) {
            return
        }
        var hand = 0
        when (actionType) {
            0 -> { // interact
                try {
                    hand = NetworkUtils.readVarInt(buf)
                } catch (_: Throwable) {
                    return
                }
            }
            2 -> { // interact_at
                if (buf.readableBytes() < 12) return
                buf.readFloat()
                buf.readFloat()
                buf.readFloat()
                try {
                    hand = NetworkUtils.readVarInt(buf)
                } catch (_: Throwable) {
                    return
                }
            }
            1 -> {
                // ATTACK
            }
            else -> return
        }
        if (buf.readableBytes() >= 1) {
            buf.readBoolean() // using secondary action
        }
        if (actionType == 1) {
            PlayerSessionManager.handlePlayerAttackEntity(session.channelId, targetEntityId)
        } else if (actionType == 0) {
            PlayerSessionManager.handlePlayerInteractEntity(
                channelId = session.channelId,
                targetEntityId = targetEntityId,
                hand = hand
            )
        }
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
        var sequence = -1
        if (buf.readableBytes() > 0) {
            try {
                sequence = NetworkUtils.readVarInt(buf) // sequence (1.19+)
            } catch (_: Throwable) {
                // ignore
            }
        }
        // Survival should only break on FINISHED_DIGGING.
        // Creative still breaks immediately on START_DIGGING.
        if (action == 0 || action == 2) {
            PlayerSessionManager.handleBlockDiggingAction(session.channelId, action, pos.x, pos.y, pos.z)
            PlayerSessionManager.acknowledgeBlockChangedSequence(session.channelId, sequence)
            return
        }
        // DROP_ITEM_STACK / DROP_ITEM
        if (action == 3 || action == 4) {
            PlayerSessionManager.dropSelectedItem(
                channelId = session.channelId,
                dropStack = action == 3
            )
            PlayerSessionManager.acknowledgeBlockChangedSequence(session.channelId, sequence)
            return
        }
        // RELEASE_USE_ITEM
        if (action == 5) {
            PlayerSessionManager.handleReleaseUseItem(session.channelId)
            PlayerSessionManager.acknowledgeBlockChangedSequence(session.channelId, sequence)
            return
        }
        // SWAP_ITEM_WITH_OFFHAND
        if (action == 6) {
            PlayerSessionManager.swapMainHandWithOffhand(session.channelId)
            PlayerSessionManager.acknowledgeBlockChangedSequence(session.channelId, sequence)
            return
        }
        PlayerSessionManager.acknowledgeBlockChangedSequence(session.channelId, sequence)
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
        var sequence = -1
        if (buf.readableBytes() > 0) {
            try {
                sequence = NetworkUtils.readVarInt(buf) // sequence
            } catch (_: Throwable) {
                // ignore
            }
        }

        val target = offsetByFace(clicked, faceId)
        if (placementDebugEnabled) {
            logger.info(
                "Placement packet: player={} clicked=({}, {}, {}) faceId={} target=({}, {}, {}) hand={} cursor=({}, {}, {})",
                profile.username,
                clicked.x,
                clicked.y,
                clicked.z,
                faceId,
                target.x,
                target.y,
                target.z,
                hand,
                cursorX,
                cursorY,
                cursorZ
            )
        }
        PlayerSessionManager.placeSelectedBlock(
            channelId = session.channelId,
            x = target.x,
            y = target.y,
            z = target.z,
            clickedX = clicked.x,
            clickedY = clicked.y,
            clickedZ = clicked.z,
            hand = hand,
            faceId = faceId,
            cursorX = cursorX,
            cursorY = cursorY,
            cursorZ = cursorZ
        )
        PlayerSessionManager.acknowledgeBlockChangedSequence(session.channelId, sequence)
    }

    private fun handleUseItem(buf: ByteBuf) {
        val hand = try {
            NetworkUtils.readVarInt(buf)
        } catch (_: Throwable) {
            return
        }
        if (hand !in 0..1) return
        // 1.21.11: UseItem carries at least hand + sequence.
        // Parse sequence strictly so unrelated packets (e.g. container close) cannot be misclassified.
        val sequence = try {
            NetworkUtils.readVarInt(buf) // sequence
        } catch (_: Throwable) {
            return
        }
        // Some revisions include yaw/pitch (float, float). If present, consume exactly 8 bytes.
        val remaining = buf.readableBytes()
        if (remaining == 8) {
            if (buf.readableBytes() < 8) return
            buf.readFloat()
            buf.readFloat()
        } else if (remaining != 0) {
            return
        }
        PlayerSessionManager.handleUseItem(session.channelId, hand, sequence)
        PlayerSessionManager.acknowledgeBlockChangedSequence(session.channelId, sequence)
    }

    private fun handleContainerClick(buf: ByteBuf) {
        val containerId = try {
            NetworkUtils.readVarInt(buf)
        } catch (_: Throwable) {
            return
        }
        val stateId = try {
            NetworkUtils.readVarInt(buf)
        } catch (_: Throwable) {
            return
        }
        if (buf.readableBytes() < 3) return
        val slot = buf.readShort().toInt()
        val button = buf.readByte().toInt()
        val clickType = try {
            NetworkUtils.readVarInt(buf)
        } catch (_: Throwable) {
            return
        }
        PlayerSessionManager.handleContainerClick(
            channelId = session.channelId,
            containerId = containerId,
            _stateId = stateId,
            slot = slot,
            button = button,
            clickType = clickType
        )
    }

    private fun handleContainerClose(buf: ByteBuf) {
        val containerId = try {
            NetworkUtils.readVarInt(buf)
        } catch (_: Throwable) {
            return
        }
        PlayerSessionManager.handleContainerClose(session.channelId, containerId)
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
                val now = System.nanoTime()
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
        if (isPeerDisconnect(cause) || !ctx.channel().isActive) {
            logger.info(
                "Play connection closed: player={} channel={} reason={}",
                session.profile.username,
                session.channelId,
                disconnectReason(cause)
            )
        } else {
            logger.error("PlayHandler exception for player={} channel={}", session.profile.username, session.channelId, cause)
        }
        PlayerSessionManager.leave(session.channelId)
        ctx.close()
    }

    private fun isPeerDisconnect(throwable: Throwable?): Boolean {
        var current = throwable
        while (current != null) {
            if (current is ClosedChannelException) return true
            if (current is IOException) {
                val msg = current.message?.lowercase(Locale.ROOT).orEmpty()
                if (
                    msg.contains("connection reset") ||
                    msg.contains("broken pipe") ||
                    msg.contains("forcibly closed") ||
                    msg.contains("connection aborted") ||
                    msg.contains("connection closed")
                ) {
                    return true
                }
            }
            if (current.javaClass.name == "io.netty.channel.unix.Errors\$NativeIoException") return true
            current = current.cause
        }
        return false
    }

    private fun disconnectReason(throwable: Throwable?): String {
        val type = throwable?.javaClass?.simpleName ?: "unknown"
        val message = throwable?.message?.takeIf { it.isNotBlank() } ?: "closed"
        return "$type: $message"
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
