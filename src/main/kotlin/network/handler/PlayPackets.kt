package org.macaroon3145.network.handler

import org.macaroon3145.network.NetworkUtils
import org.macaroon3145.network.codec.RegistryCodec
import org.macaroon3145.world.GeneratedChunk
import java.io.ByteArrayOutputStream
import java.io.DataOutputStream
import kotlin.math.floor

object PlayPackets {
    data class EquipmentEntry(
        val slot: Int,
        val encodedItemStack: ByteArray
    )

    data class HoverEntity(
        val entityType: String,
        val uuid: java.util.UUID,
        val name: String? = null
    )

    sealed interface ChatComponent {
        data class Text(
            val text: String,
            val extra: List<ChatComponent> = emptyList(),
            val hoverText: String? = null,
            val hoverEntity: HoverEntity? = null,
            val color: String? = null,
            val underlined: Boolean? = null,
            val italic: Boolean? = null
        ) : ChatComponent

        data class Translate(
            val key: String,
            val args: List<ChatComponent> = emptyList(),
            val extra: List<ChatComponent> = emptyList(),
            val color: String? = null,
            val underlined: Boolean? = null,
            val italic: Boolean? = null
        ) : ChatComponent
    }
    private const val LOGIN_PACKET_ID = 0x30
    private const val ADD_ENTITY_PACKET_ID = 0x01
    private const val ENTITY_ANIMATION_PACKET_ID = 0x02
    private const val TELEPORT_ENTITY_PACKET_ID = 0x23
    private const val MOVE_ENTITY_POS_PACKET_ID = 0x33
    private const val MOVE_ENTITY_POS_ROT_PACKET_ID = 0x34
    private const val BLOCK_CHANGE_PACKET_ID = 0x08
    // Protocol 774 (MC 1.21.11): WorldEventPacket
    private const val LEVEL_EVENT_PACKET_ID = 0x2D
    private const val KEEP_ALIVE_PACKET_ID = 0x2B
    private const val PLAYER_INFO_REMOVE_PACKET_ID = 0x43
    private const val GAME_STATE_CHANGE_PACKET_ID = 0x26
    private const val CHUNK_BATCH_FINISHED_PACKET_ID = 0x0B
    private const val CHUNK_BATCH_START_PACKET_ID = 0x0C
    private const val MAP_CHUNK_PACKET_ID = 0x2C
    private const val UNLOAD_CHUNK_PACKET_ID = 0x25
    private const val PLAYER_INFO_PACKET_ID = 0x44
    private const val ENTITY_METADATA_PACKET_ID = 0x61
    private const val ENTITY_EQUIPMENT_PACKET_ID = 0x64
    private const val ENTITY_VELOCITY_PACKET_ID = 0x63
    private const val POSITION_PACKET_ID = 0x46
    private const val REMOVE_ENTITIES_PACKET_ID = 0x4B
    private const val TAKE_ITEM_ENTITY_PACKET_ID = 0x7A
    private const val ENTITY_HEAD_LOOK_PACKET_ID = 0x51
    private const val UPDATE_VIEW_POSITION_PACKET_ID = 0x5C
    private const val UPDATE_VIEW_DISTANCE_PACKET_ID = 0x5D
    private const val SPAWN_POSITION_PACKET_ID = 0x5F
    private const val TIME_UPDATE_PACKET_ID = 0x6F
    private const val SYSTEM_CHAT_PACKET_ID = 0x77
    private const val SET_HELD_SLOT_PACKET_ID = 0x67
    private const val CONTAINER_SET_SLOT_PACKET_ID = 0x14
    private const val COMMAND_SUGGESTIONS_PACKET_ID = 0x0F
    private const val COMMANDS_PACKET_ID = 0x10
    private const val CUSTOM_PAYLOAD_PACKET_ID = 0x18
    private const val TICKING_STATE_PACKET_ID = 0x7D
    private const val TICKING_STEP_PACKET_ID = 0x7E

    private const val TELEPORT_ID = 1
    private const val SPAWN_X = 0
    private const val SPAWN_Y = 65
    private const val SPAWN_Z = 0
    private const val WORLD_HEIGHT = 384
    private const val FALLBACK_PLAYER_ENTITY_TYPE_ID_1_21_11 = 155
    private const val FALLBACK_ITEM_ENTITY_TYPE_ID_1_21_11 = 71
    private const val COMMAND_ARGUMENT_TYPE_GAME_PROFILE_ID_1_21_11 = 7
    private const val COMMAND_ARGUMENT_TYPE_VEC3_ID_1_21_11 = 10
    private const val COMMAND_ARGUMENT_TYPE_GAMEMODE_ID_1_21_11 = 42
    private const val COMMAND_ARGUMENT_TYPE_ENTITY_ID_1_21_11 = 6
    private const val ENTITY_ARGUMENT_FLAG_SINGLE = 0x01
    private const val ENTITY_ARGUMENT_FLAG_PLAYERS_ONLY = 0x02
    // 1.21.11: player model customization is tracked on PlayerLikeEntity before PlayerEntity fields.
    private const val PLAYER_SKIN_PARTS_METADATA_INDEX = 16
    private const val ENTITY_SHARED_FLAGS_METADATA_INDEX = 0
    private const val ENTITY_POSE_METADATA_INDEX = 6
    private const val ENTITY_POSE_METADATA_TYPE_ID = 20 // entity_pose in 1.21.11
    private const val ENTITY_FLAGS_SNEAKING = 0x02
    private const val ENTITY_FLAGS_SPRINTING = 0x08
    private const val ENTITY_FLAGS_SWIMMING = 0x10
    private const val POSE_STANDING = 0
    private const val POSE_SWIMMING = 3
    private const val POSE_CROUCHING = 5
    private val cachedItemEntityTypeId: Int by lazy {
        RegistryCodec.entryIndex("minecraft:entity_type", "minecraft:item")
            ?: FALLBACK_ITEM_ENTITY_TYPE_ID_1_21_11
    }

    fun loginPacket(entityId: Int, worldKey: String, gameMode: Int): ByteArray {
        val packet = ByteArrayOutputStream()
        val out = DataOutputStream(packet)
        val clampedGameMode = gameMode.coerceIn(0, 3)
        NetworkUtils.writeVarInt(packet, LOGIN_PACKET_ID)
        out.writeInt(entityId)
        out.writeBoolean(false)
        NetworkUtils.writeVarInt(packet, 1)
        NetworkUtils.writeString(packet, worldKey)
        NetworkUtils.writeVarInt(packet, 20)
        NetworkUtils.writeVarInt(packet, 10)
        NetworkUtils.writeVarInt(packet, 10)
        out.writeBoolean(false)
        out.writeBoolean(true)
        out.writeBoolean(false)

        NetworkUtils.writeVarInt(packet, clampedGameMode)
        NetworkUtils.writeString(packet, worldKey)
        out.writeLong(0L)
        out.writeByte(clampedGameMode)
        out.writeByte(0xFF)
        out.writeBoolean(false)
        out.writeBoolean(true)
        out.writeBoolean(false)
        NetworkUtils.writeVarInt(packet, 0)
        NetworkUtils.writeVarInt(packet, 63)

        out.writeBoolean(false)
        return packet.toByteArray()
    }

    fun updateViewPositionPacket(chunkX: Int = SPAWN_X shr 4, chunkZ: Int = SPAWN_Z shr 4): ByteArray {
        val packet = ByteArrayOutputStream()
        NetworkUtils.writeVarInt(packet, UPDATE_VIEW_POSITION_PACKET_ID)
        NetworkUtils.writeVarInt(packet, chunkX)
        NetworkUtils.writeVarInt(packet, chunkZ)
        return packet.toByteArray()
    }

    fun updateViewDistancePacket(viewDistanceChunks: Int): ByteArray {
        val packet = ByteArrayOutputStream()
        NetworkUtils.writeVarInt(packet, UPDATE_VIEW_DISTANCE_PACKET_ID)
        NetworkUtils.writeVarInt(packet, viewDistanceChunks.coerceAtLeast(0))
        return packet.toByteArray()
    }

    fun spawnPositionPacket(worldKey: String, x: Double, y: Double, z: Double): ByteArray {
        val packet = ByteArrayOutputStream()
        val out = DataOutputStream(packet)
        NetworkUtils.writeVarInt(packet, SPAWN_POSITION_PACKET_ID)
        NetworkUtils.writeString(packet, worldKey)
        out.writeLong(packPosition(floor(x).toInt(), floor(y).toInt(), floor(z).toInt()))
        out.writeFloat(0f)
        out.writeFloat(0f)
        return packet.toByteArray()
    }

    fun playerPositionPacket(
        x: Double,
        y: Double,
        z: Double,
        yaw: Float = 0f,
        pitch: Float = 0f,
        relativeFlags: Int = 0
    ): ByteArray {
        val packet = ByteArrayOutputStream()
        val out = DataOutputStream(packet)
        NetworkUtils.writeVarInt(packet, POSITION_PACKET_ID)
        NetworkUtils.writeVarInt(packet, TELEPORT_ID)
        out.writeDouble(x)
        out.writeDouble(y)
        out.writeDouble(z)
        out.writeDouble(0.0)
        out.writeDouble(0.0)
        out.writeDouble(0.0)
        out.writeFloat(yaw)
        out.writeFloat(pitch)
        out.writeInt(relativeFlags)
        return packet.toByteArray()
    }

    fun timeUpdatePacket(worldAge: Long, timeOfDay: Long, tickDayTime: Boolean): ByteArray {
        val packet = ByteArrayOutputStream()
        val out = DataOutputStream(packet)
        NetworkUtils.writeVarInt(packet, TIME_UPDATE_PACKET_ID)
        out.writeLong(worldAge)
        out.writeLong(timeOfDay)
        out.writeBoolean(tickDayTime)
        return packet.toByteArray()
    }

    fun tickingStatePacket(tickRate: Float, isFrozen: Boolean = false): ByteArray {
        val packet = ByteArrayOutputStream()
        val out = DataOutputStream(packet)
        NetworkUtils.writeVarInt(packet, TICKING_STATE_PACKET_ID)
        out.writeFloat(tickRate)
        out.writeBoolean(isFrozen)
        return packet.toByteArray()
    }

    fun tickingStepPacket(tickSteps: Int): ByteArray {
        val packet = ByteArrayOutputStream()
        NetworkUtils.writeVarInt(packet, TICKING_STEP_PACKET_ID)
        NetworkUtils.writeVarInt(packet, tickSteps.coerceAtLeast(0))
        return packet.toByteArray()
    }

    fun systemChatPacket(message: String, overlay: Boolean = false): ByteArray {
        return systemChatPacket(ChatComponent.Text(message), overlay)
    }

    fun systemChatPacket(component: ChatComponent, overlay: Boolean = false): ByteArray {
        val packet = ByteArrayOutputStream()
        val out = DataOutputStream(packet)
        NetworkUtils.writeVarInt(packet, SYSTEM_CHAT_PACKET_ID)
        writeComponentNbt(out, component)
        out.writeBoolean(overlay)
        return packet.toByteArray()
    }

    fun keepAlivePacket(id: Long): ByteArray {
        val packet = ByteArrayOutputStream()
        val out = DataOutputStream(packet)
        NetworkUtils.writeVarInt(packet, KEEP_ALIVE_PACKET_ID)
        out.writeLong(id)
        return packet.toByteArray()
    }

    fun setHeldSlotPacket(slot: Int): ByteArray {
        val packet = ByteArrayOutputStream()
        NetworkUtils.writeVarInt(packet, SET_HELD_SLOT_PACKET_ID)
        NetworkUtils.writeVarInt(packet, slot.coerceIn(0, 8))
        return packet.toByteArray()
    }

    fun containerSetSlotPacket(containerId: Int, stateId: Int, slot: Int, encodedItemStack: ByteArray): ByteArray {
        val packet = ByteArrayOutputStream()
        val out = DataOutputStream(packet)
        NetworkUtils.writeVarInt(packet, CONTAINER_SET_SLOT_PACKET_ID)
        NetworkUtils.writeVarInt(packet, containerId)
        NetworkUtils.writeVarInt(packet, stateId)
        out.writeShort(slot)
        packet.write(encodedItemStack)
        return packet.toByteArray()
    }

    fun commandsPacket(includeOperatorCommands: Boolean): ByteArray {
        data class CmdNode(
            val flags: Int,
            val children: IntArray,
            val literal: String? = null,
            val argumentName: String? = null,
            val parserTypeId: Int? = null,
            val parserPayload: ByteArray = ByteArray(0),
            val suggestionType: String? = null
        )

        val nodes = ArrayList<CmdNode>()
        fun addNode(node: CmdNode): Int {
            nodes += node
            return nodes.lastIndex
        }
        fun setNode(index: Int, node: CmdNode) {
            nodes[index] = node
        }
        fun entityPayload(single: Boolean, playersOnly: Boolean): ByteArray {
            var flags = 0
            if (single) flags = flags or ENTITY_ARGUMENT_FLAG_SINGLE
            if (playersOnly) flags = flags or ENTITY_ARGUMENT_FLAG_PLAYERS_ONLY
            return byteArrayOf(flags.toByte())
        }
        fun addLiteral(literal: String, children: IntArray = intArrayOf(), executable: Boolean = false): Int {
            return addNode(
                CmdNode(
                    flags = 0x01 or (if (executable) 0x04 else 0),
                    children = children,
                    literal = literal
                )
            )
        }
        fun addArgument(
            name: String,
            parserTypeId: Int,
            parserPayload: ByteArray = ByteArray(0),
            children: IntArray = intArrayOf(),
            executable: Boolean = false,
            suggestionType: String? = null
        ): Int {
            return addNode(
                CmdNode(
                    flags = 0x02 or (if (executable) 0x04 else 0) or (if (suggestionType != null) 0x10 else 0),
                    children = children,
                    argumentName = name,
                    parserTypeId = parserTypeId,
                    parserPayload = parserPayload,
                    suggestionType = suggestionType
                )
            )
        }

        val rootIndex = addNode(CmdNode(flags = 0x00, children = intArrayOf()))
        val rootChildren = ArrayList<Int>(4)

        if (includeOperatorCommands) {
            fun addTpSubtree(literal: String) {
                val literalIndex = addLiteral(literal)
                val destinationIndex = addArgument(
                    name = "destination",
                    parserTypeId = COMMAND_ARGUMENT_TYPE_ENTITY_ID_1_21_11,
                    parserPayload = entityPayload(single = true, playersOnly = false),
                    executable = true
                )
                val locationIndex = addArgument(
                    name = "location",
                    parserTypeId = COMMAND_ARGUMENT_TYPE_VEC3_ID_1_21_11,
                    executable = true
                )
                val targetsIndex = addArgument(
                    name = "targets",
                    parserTypeId = COMMAND_ARGUMENT_TYPE_ENTITY_ID_1_21_11,
                    parserPayload = entityPayload(single = false, playersOnly = false),
                    suggestionType = null
                )
                val targetDestinationIndex = addArgument(
                    name = "destination",
                    parserTypeId = COMMAND_ARGUMENT_TYPE_ENTITY_ID_1_21_11,
                    parserPayload = entityPayload(single = true, playersOnly = false),
                    executable = true
                )
                val targetLocationIndex = addArgument(
                    name = "location",
                    parserTypeId = COMMAND_ARGUMENT_TYPE_VEC3_ID_1_21_11,
                    executable = true
                )
                setNode(
                    literalIndex,
                    nodes[literalIndex].copy(children = intArrayOf(destinationIndex, locationIndex, targetsIndex))
                )
                setNode(
                    targetsIndex,
                    nodes[targetsIndex].copy(children = intArrayOf(targetDestinationIndex, targetLocationIndex))
                )
                rootChildren += literalIndex
            }

            fun addGamemodeSubtree() {
                val literalIndex = addLiteral("gamemode")
                val modeIndex = addArgument(
                    name = "gamemode",
                    parserTypeId = COMMAND_ARGUMENT_TYPE_GAMEMODE_ID_1_21_11,
                    executable = true
                )
                val targetIndex = addArgument(
                    name = "target",
                    parserTypeId = COMMAND_ARGUMENT_TYPE_ENTITY_ID_1_21_11,
                    parserPayload = entityPayload(single = false, playersOnly = false),
                    executable = true
                )
                setNode(literalIndex, nodes[literalIndex].copy(children = intArrayOf(modeIndex)))
                setNode(modeIndex, nodes[modeIndex].copy(children = intArrayOf(targetIndex)))
                rootChildren += literalIndex
            }

            fun addOpSubtree() {
                val literalIndex = addLiteral("op")
                val targetIndex = addArgument(
                    name = "targets",
                    parserTypeId = COMMAND_ARGUMENT_TYPE_GAME_PROFILE_ID_1_21_11,
                    executable = true
                )
                setNode(literalIndex, nodes[literalIndex].copy(children = intArrayOf(targetIndex)))
                rootChildren += literalIndex
            }

            addTpSubtree("tp")
            addTpSubtree("teleport")
            addGamemodeSubtree()
            addOpSubtree()
        }
        setNode(rootIndex, nodes[rootIndex].copy(children = rootChildren.toIntArray()))

        val packet = ByteArrayOutputStream()
        NetworkUtils.writeVarInt(packet, COMMANDS_PACKET_ID)
        NetworkUtils.writeVarInt(packet, nodes.size)
        for (node in nodes) {
            packet.write(node.flags)
            NetworkUtils.writeVarInt(packet, node.children.size)
            for (child in node.children) {
                NetworkUtils.writeVarInt(packet, child)
            }
            if ((node.flags and 0x08) != 0) {
                NetworkUtils.writeVarInt(packet, 0) // redirect index (unused)
            }
            when (node.flags and 0x03) {
                0x01 -> {
                    NetworkUtils.writeString(packet, node.literal ?: "")
                }
                0x02 -> {
                    NetworkUtils.writeString(packet, node.argumentName ?: "args")
                    NetworkUtils.writeVarInt(packet, node.parserTypeId ?: COMMAND_ARGUMENT_TYPE_ENTITY_ID_1_21_11)
                    packet.write(node.parserPayload)
                    if ((node.flags and 0x10) != 0) {
                        NetworkUtils.writeString(packet, node.suggestionType ?: "minecraft:ask_server")
                    }
                }
            }
        }
        NetworkUtils.writeVarInt(packet, rootIndex)
        return packet.toByteArray()
    }

    fun commandSuggestionsPacket(requestId: Int, start: Int, length: Int, suggestions: List<String>): ByteArray {
        val packet = ByteArrayOutputStream()
        val out = DataOutputStream(packet)
        NetworkUtils.writeVarInt(packet, COMMAND_SUGGESTIONS_PACKET_ID)
        NetworkUtils.writeVarInt(packet, requestId.coerceAtLeast(0))
        NetworkUtils.writeVarInt(packet, start.coerceAtLeast(0))
        NetworkUtils.writeVarInt(packet, length.coerceAtLeast(0))
        NetworkUtils.writeVarInt(packet, suggestions.size)
        for (suggestion in suggestions) {
            NetworkUtils.writeString(packet, suggestion)
            out.writeBoolean(false) // tooltip present
        }
        return packet.toByteArray()
    }

    fun serverBrandPacket(brand: String = "Aerogel"): ByteArray {
        val packet = ByteArrayOutputStream()
        NetworkUtils.writeVarInt(packet, CUSTOM_PAYLOAD_PACKET_ID)
        NetworkUtils.writeString(packet, "minecraft:brand")
        NetworkUtils.writeString(packet, brand)
        return packet.toByteArray()
    }

    fun playerInfoPacket(profile: ConnectionProfile, gameMode: Int, latencyMs: Int): ByteArray {
        val packet = ByteArrayOutputStream()
        val out = DataOutputStream(packet)
        val clampedGameMode = gameMode.coerceIn(0, 3)
        NetworkUtils.writeVarInt(packet, PLAYER_INFO_PACKET_ID)

        val actionFlags = 0x01 or 0x04 or 0x08 or 0x10 or 0x40
        out.writeByte(actionFlags)
        NetworkUtils.writeVarInt(packet, 1)
        NetworkUtils.writeUUID(packet, profile.uuid)

        NetworkUtils.writeString(packet, profile.username)
        NetworkUtils.writeVarInt(packet, profile.properties.size)
        for (property in profile.properties) {
            NetworkUtils.writeString(packet, property.name)
            NetworkUtils.writeString(packet, property.value)
            if (property.signature == null) {
                out.writeBoolean(false)
            } else {
                out.writeBoolean(true)
                NetworkUtils.writeString(packet, property.signature)
            }
        }

        NetworkUtils.writeVarInt(packet, clampedGameMode) // gamemode
        NetworkUtils.writeVarInt(packet, 1) // listed
        NetworkUtils.writeVarInt(packet, latencyMs.coerceAtLeast(0)) // latency
        out.writeBoolean(true) // show hat

        return packet.toByteArray()
    }

    fun playerInfoLatencyUpdatePacket(uuid: java.util.UUID, latencyMs: Int): ByteArray {
        val packet = ByteArrayOutputStream()
        val out = DataOutputStream(packet)
        NetworkUtils.writeVarInt(packet, PLAYER_INFO_PACKET_ID)
        out.writeByte(0x10) // update latency
        NetworkUtils.writeVarInt(packet, 1)
        NetworkUtils.writeUUID(packet, uuid)
        NetworkUtils.writeVarInt(packet, latencyMs.coerceAtLeast(0))
        return packet.toByteArray()
    }

    fun playerInfoGameModeUpdatePacket(uuid: java.util.UUID, gameMode: Int): ByteArray {
        val packet = ByteArrayOutputStream()
        val out = DataOutputStream(packet)
        NetworkUtils.writeVarInt(packet, PLAYER_INFO_PACKET_ID)
        out.writeByte(0x04) // update game mode
        NetworkUtils.writeVarInt(packet, 1)
        NetworkUtils.writeUUID(packet, uuid)
        NetworkUtils.writeVarInt(packet, gameMode.coerceIn(0, 3))
        return packet.toByteArray()
    }

    fun playerSkinPartsMetadataPacket(entityId: Int = 1, skinPartsMask: Int = 0x7F): ByteArray {
        val packet = ByteArrayOutputStream()
        val out = DataOutputStream(packet)
        NetworkUtils.writeVarInt(packet, ENTITY_METADATA_PACKET_ID)
        NetworkUtils.writeVarInt(packet, entityId)

        // Player model parts data tracker index (1.21.11). 0x7F enables all outer layers.
        out.writeByte(PLAYER_SKIN_PARTS_METADATA_INDEX)
        NetworkUtils.writeVarInt(packet, 0) // metadata type: byte
        out.writeByte(skinPartsMask)

        out.writeByte(0xFF) // metadata terminator
        return packet.toByteArray()
    }

    fun playerInfoRemovePacket(uuids: List<java.util.UUID>): ByteArray {
        val packet = ByteArrayOutputStream()
        NetworkUtils.writeVarInt(packet, PLAYER_INFO_REMOVE_PACKET_ID)
        NetworkUtils.writeVarInt(packet, uuids.size)
        for (uuid in uuids) {
            NetworkUtils.writeUUID(packet, uuid)
        }
        return packet.toByteArray()
    }

    fun entityAnimationPacket(entityId: Int, animationType: Int): ByteArray {
        val packet = ByteArrayOutputStream()
        val out = DataOutputStream(packet)
        NetworkUtils.writeVarInt(packet, ENTITY_ANIMATION_PACKET_ID)
        NetworkUtils.writeVarInt(packet, entityId)
        out.writeByte(animationType)
        return packet.toByteArray()
    }

    fun blockChangePacket(x: Int, y: Int, z: Int, blockStateId: Int): ByteArray {
        val packet = ByteArrayOutputStream()
        val out = DataOutputStream(packet)
        NetworkUtils.writeVarInt(packet, BLOCK_CHANGE_PACKET_ID)
        out.writeLong(packPosition(x, y, z))
        NetworkUtils.writeVarInt(packet, blockStateId)
        return packet.toByteArray()
    }

    fun levelEventPacket(eventId: Int, x: Int, y: Int, z: Int, data: Int, global: Boolean): ByteArray {
        val packet = ByteArrayOutputStream()
        val out = DataOutputStream(packet)
        NetworkUtils.writeVarInt(packet, LEVEL_EVENT_PACKET_ID)
        out.writeInt(eventId)
        out.writeLong(packPosition(x, y, z))
        out.writeInt(data)
        out.writeBoolean(global)
        return packet.toByteArray()
    }

    fun blockEntityDataPacket(x: Int, y: Int, z: Int, blockEntityTypeId: Int, nbtPayload: ByteArray): ByteArray {
        val packet = ByteArrayOutputStream()
        val out = DataOutputStream(packet)
        NetworkUtils.writeVarInt(packet, 0x06) // clientbound minecraft:block_entity_data
        out.writeLong(packPosition(x, y, z))
        NetworkUtils.writeVarInt(packet, blockEntityTypeId)
        packet.write(nbtPayload)
        return packet.toByteArray()
    }

    fun addPlayerEntityPacket(
        entityId: Int,
        uuid: java.util.UUID,
        x: Double,
        y: Double,
        z: Double,
        yaw: Float,
        pitch: Float
    ): ByteArray {
        return addEntityPacket(
            entityId = entityId,
            uuid = uuid,
            entityTypeId = playerEntityTypeId(),
            x = x,
            y = y,
            z = z,
            yaw = yaw,
            pitch = pitch,
            objectData = 0
        )
    }

    fun addEntityPacket(
        entityId: Int,
        uuid: java.util.UUID,
        entityTypeId: Int,
        x: Double,
        y: Double,
        z: Double,
        yaw: Float,
        pitch: Float,
        objectData: Int
    ): ByteArray {
        val packet = ByteArrayOutputStream()
        val out = DataOutputStream(packet)
        NetworkUtils.writeVarInt(packet, ADD_ENTITY_PACKET_ID)
        NetworkUtils.writeVarInt(packet, entityId)
        NetworkUtils.writeUUID(packet, uuid)
        NetworkUtils.writeVarInt(packet, entityTypeId)
        out.writeDouble(x)
        out.writeDouble(y)
        out.writeDouble(z)
        // 1.21.9+ spawn entity layout writes low-precision velocity before rotations.
        // Zero velocity is encoded as a single zero byte.
        out.writeByte(0)
        out.writeByte(angleByte(pitch))
        out.writeByte(angleByte(yaw))
        out.writeByte(angleByte(yaw))
        NetworkUtils.writeVarInt(packet, objectData)
        return packet.toByteArray()
    }

    fun entityVelocityPacket(entityId: Int, vx: Double, vy: Double, vz: Double): ByteArray {
        val packet = ByteArrayOutputStream()
        val out = DataOutputStream(packet)
        NetworkUtils.writeVarInt(packet, ENTITY_VELOCITY_PACKET_ID)
        NetworkUtils.writeVarInt(packet, entityId)
        writeLpVector3(out, packet, vx, vy, vz)
        return packet.toByteArray()
    }

    fun itemEntityMetadataPacket(entityId: Int, encodedItemStack: ByteArray): ByteArray {
        val packet = ByteArrayOutputStream()
        val out = DataOutputStream(packet)
        NetworkUtils.writeVarInt(packet, ENTITY_METADATA_PACKET_ID)
        NetworkUtils.writeVarInt(packet, entityId)
        // ItemEntity metadata index:
        // MetadataDef.ItemEntity.ITEM => base Entity(0..7) + local(0) = 8
        out.writeByte(8)
        // Metadata.TYPE_ITEM_STACK = 7
        NetworkUtils.writeVarInt(packet, 7)
        packet.write(encodedItemStack)
        out.writeByte(0xFF)
        return packet.toByteArray()
    }

    fun entityEquipmentPacket(entityId: Int, entries: List<EquipmentEntry>): ByteArray {
        val packet = ByteArrayOutputStream()
        val out = DataOutputStream(packet)
        NetworkUtils.writeVarInt(packet, ENTITY_EQUIPMENT_PACKET_ID)
        NetworkUtils.writeVarInt(packet, entityId)
        if (entries.isEmpty()) return packet.toByteArray()
        for (i in entries.indices) {
            val entry = entries[i]
            val hasNext = i < entries.size - 1
            val slotByte = (entry.slot and 0x7F) or (if (hasNext) 0x80 else 0)
            out.writeByte(slotByte)
            packet.write(entry.encodedItemStack)
        }
        return packet.toByteArray()
    }

    fun encodeItemStack(itemId: Int, count: Int = 1): ByteArray {
        val out = ByteArrayOutputStream(8)
        val clampedCount = count.coerceAtLeast(1)
        NetworkUtils.writeVarInt(out, clampedCount)
        NetworkUtils.writeVarInt(out, itemId)
        NetworkUtils.writeVarInt(out, 0) // added components
        NetworkUtils.writeVarInt(out, 0) // removed components
        return out.toByteArray()
    }

    fun itemEntityTypeId(): Int {
        return cachedItemEntityTypeId
    }

    fun entityPositionSyncPacket(
        entityId: Int,
        x: Double,
        y: Double,
        z: Double,
        vx: Double = 0.0,
        vy: Double = 0.0,
        vz: Double = 0.0,
        yaw: Float,
        pitch: Float,
        onGround: Boolean
    ): ByteArray {
        val packet = ByteArrayOutputStream()
        val out = DataOutputStream(packet)
        NetworkUtils.writeVarInt(packet, TELEPORT_ENTITY_PACKET_ID)
        NetworkUtils.writeVarInt(packet, entityId)
        out.writeDouble(x)
        out.writeDouble(y)
        out.writeDouble(z)
        out.writeDouble(vx)
        out.writeDouble(vy)
        out.writeDouble(vz)
        out.writeFloat(yaw)
        out.writeFloat(pitch)
        out.writeBoolean(onGround)
        return packet.toByteArray()
    }

    fun entityRelativeMovePacket(
        entityId: Int,
        deltaX: Int,
        deltaY: Int,
        deltaZ: Int,
        onGround: Boolean
    ): ByteArray {
        val packet = ByteArrayOutputStream()
        val out = DataOutputStream(packet)
        NetworkUtils.writeVarInt(packet, MOVE_ENTITY_POS_PACKET_ID)
        NetworkUtils.writeVarInt(packet, entityId)
        out.writeShort(deltaX.coerceIn(Short.MIN_VALUE.toInt(), Short.MAX_VALUE.toInt()))
        out.writeShort(deltaY.coerceIn(Short.MIN_VALUE.toInt(), Short.MAX_VALUE.toInt()))
        out.writeShort(deltaZ.coerceIn(Short.MIN_VALUE.toInt(), Short.MAX_VALUE.toInt()))
        out.writeBoolean(onGround)
        return packet.toByteArray()
    }

    fun entityRelativeMoveLookPacket(
        entityId: Int,
        deltaX: Int,
        deltaY: Int,
        deltaZ: Int,
        yaw: Float,
        pitch: Float,
        onGround: Boolean
    ): ByteArray {
        val packet = ByteArrayOutputStream()
        val out = DataOutputStream(packet)
        NetworkUtils.writeVarInt(packet, MOVE_ENTITY_POS_ROT_PACKET_ID)
        NetworkUtils.writeVarInt(packet, entityId)
        out.writeShort(deltaX.coerceIn(Short.MIN_VALUE.toInt(), Short.MAX_VALUE.toInt()))
        out.writeShort(deltaY.coerceIn(Short.MIN_VALUE.toInt(), Short.MAX_VALUE.toInt()))
        out.writeShort(deltaZ.coerceIn(Short.MIN_VALUE.toInt(), Short.MAX_VALUE.toInt()))
        out.writeByte(angleByte(yaw))
        out.writeByte(angleByte(pitch))
        out.writeBoolean(onGround)
        return packet.toByteArray()
    }

    fun entityHeadLookPacket(entityId: Int, headYaw: Float): ByteArray {
        val packet = ByteArrayOutputStream()
        val out = DataOutputStream(packet)
        NetworkUtils.writeVarInt(packet, ENTITY_HEAD_LOOK_PACKET_ID)
        NetworkUtils.writeVarInt(packet, entityId)
        out.writeByte(angleByte(headYaw))
        return packet.toByteArray()
    }

    fun playerSharedFlagsMetadataPacket(
        entityId: Int,
        sneaking: Boolean,
        sprinting: Boolean,
        swimming: Boolean
    ): ByteArray {
        val packet = ByteArrayOutputStream()
        val out = DataOutputStream(packet)
        NetworkUtils.writeVarInt(packet, ENTITY_METADATA_PACKET_ID)
        NetworkUtils.writeVarInt(packet, entityId)

        out.writeByte(ENTITY_SHARED_FLAGS_METADATA_INDEX)
        NetworkUtils.writeVarInt(packet, 0) // metadata type: byte

        var flags = 0
        if (sneaking) flags = flags or ENTITY_FLAGS_SNEAKING
        if (sprinting) flags = flags or ENTITY_FLAGS_SPRINTING
        if (swimming) flags = flags or ENTITY_FLAGS_SWIMMING
        out.writeByte(flags)

        // Pose must also be synchronized for crouching/swimming visuals on 1.21.x clients.
        out.writeByte(ENTITY_POSE_METADATA_INDEX)
        NetworkUtils.writeVarInt(packet, ENTITY_POSE_METADATA_TYPE_ID) // metadata type: entity_pose
        val pose = when {
            swimming -> POSE_SWIMMING
            sneaking -> POSE_CROUCHING
            else -> POSE_STANDING
        }
        NetworkUtils.writeVarInt(packet, pose)

        out.writeByte(0xFF) // metadata terminator
        return packet.toByteArray()
    }

    fun removeEntitiesPacket(entityIds: IntArray): ByteArray {
        val packet = ByteArrayOutputStream()
        NetworkUtils.writeVarInt(packet, REMOVE_ENTITIES_PACKET_ID)
        NetworkUtils.writeVarInt(packet, entityIds.size)
        for (id in entityIds) {
            NetworkUtils.writeVarInt(packet, id)
        }
        return packet.toByteArray()
    }

    fun takeItemEntityPacket(collectedEntityId: Int, collectorEntityId: Int, pickupItemCount: Int): ByteArray {
        val packet = ByteArrayOutputStream()
        NetworkUtils.writeVarInt(packet, TAKE_ITEM_ENTITY_PACKET_ID)
        NetworkUtils.writeVarInt(packet, collectedEntityId)
        NetworkUtils.writeVarInt(packet, collectorEntityId)
        NetworkUtils.writeVarInt(packet, pickupItemCount.coerceAtLeast(1))
        return packet.toByteArray()
    }

    fun unloadChunkPacket(chunkX: Int, chunkZ: Int): ByteArray {
        val packet = ByteArrayOutputStream()
        val out = DataOutputStream(packet)
        NetworkUtils.writeVarInt(packet, UNLOAD_CHUNK_PACKET_ID)
        // 1.20.2+ format: single long chunk key (x low 32, z high 32).
        val chunkKey = (chunkX.toLong() and 0xFFFF_FFFFL) or ((chunkZ.toLong() and 0xFFFF_FFFFL) shl 32)
        out.writeLong(chunkKey)
        return packet.toByteArray()
    }

    fun chunkBatchStartPacket(): ByteArray {
        val packet = ByteArrayOutputStream()
        NetworkUtils.writeVarInt(packet, CHUNK_BATCH_START_PACKET_ID)
        return packet.toByteArray()
    }

    fun chunkBatchFinishedPacket(batchSize: Int): ByteArray {
        val packet = ByteArrayOutputStream()
        NetworkUtils.writeVarInt(packet, CHUNK_BATCH_FINISHED_PACKET_ID)
        NetworkUtils.writeVarInt(packet, batchSize)
        return packet.toByteArray()
    }

    fun mapChunkPacket(chunkX: Int, chunkZ: Int, generated: GeneratedChunk): ByteArray {
        val packet = ByteArrayOutputStream()
        val out = DataOutputStream(packet)
        val chunkData = if (generated.chunkData.isNotEmpty()) generated.chunkData else buildAirChunkData()
        NetworkUtils.writeVarInt(packet, MAP_CHUNK_PACKET_ID)
        out.writeInt(chunkX)
        out.writeInt(chunkZ)

        // heightmaps
        NetworkUtils.writeVarInt(packet, generated.heightmaps.size)
        for (heightmap in generated.heightmaps) {
            NetworkUtils.writeVarInt(packet, heightmap.typeId)
            NetworkUtils.writeVarInt(packet, heightmap.values.size)
            for (value in heightmap.values) {
                out.writeLong(value)
            }
        }
        // chunkData (raw section data)
        NetworkUtils.writeVarInt(packet, chunkData.size)
        out.write(chunkData)
        // block entities
        NetworkUtils.writeVarInt(packet, 0)
        // skyLightMask
        NetworkUtils.writeVarInt(packet, generated.skyLightMask.size)
        for (value in generated.skyLightMask) out.writeLong(value)
        // blockLightMask
        NetworkUtils.writeVarInt(packet, generated.blockLightMask.size)
        for (value in generated.blockLightMask) out.writeLong(value)
        // emptySkyLightMask
        NetworkUtils.writeVarInt(packet, generated.emptySkyLightMask.size)
        for (value in generated.emptySkyLightMask) out.writeLong(value)
        // emptyBlockLightMask
        NetworkUtils.writeVarInt(packet, generated.emptyBlockLightMask.size)
        for (value in generated.emptyBlockLightMask) out.writeLong(value)
        // skyLight arrays
        NetworkUtils.writeVarInt(packet, generated.skyLight.size)
        for (bytes in generated.skyLight) {
            NetworkUtils.writeVarInt(packet, bytes.size)
            out.write(bytes)
        }
        // blockLight arrays
        NetworkUtils.writeVarInt(packet, generated.blockLight.size)
        for (bytes in generated.blockLight) {
            NetworkUtils.writeVarInt(packet, bytes.size)
            out.write(bytes)
        }
        return packet.toByteArray()
    }

    fun gameStateStartLoadingPacket(): ByteArray {
        val packet = ByteArrayOutputStream()
        val out = DataOutputStream(packet)
        NetworkUtils.writeVarInt(packet, GAME_STATE_CHANGE_PACKET_ID)
        out.writeByte(13)
        out.writeFloat(0f)
        return packet.toByteArray()
    }

    fun gameStateGameModePacket(gameMode: Int): ByteArray {
        val packet = ByteArrayOutputStream()
        val out = DataOutputStream(packet)
        NetworkUtils.writeVarInt(packet, GAME_STATE_CHANGE_PACKET_ID)
        out.writeByte(3) // change game mode
        out.writeFloat(gameMode.coerceIn(0, 3).toFloat())
        return packet.toByteArray()
    }

    private fun packPosition(x: Int, y: Int, z: Int): Long {
        val lx = (x.toLong() and 0x3FFFFFF) shl 38
        val lz = (z.toLong() and 0x3FFFFFF) shl 12
        val ly = y.toLong() and 0xFFF
        return lx or lz or ly
    }

    private fun writeComponentNbt(out: DataOutputStream, component: ChatComponent) {
        // NetworkBuffer.COMPONENT encoding used by 1.21.11 SystemChatPacket.
        out.writeByte(10) // TAG_Compound (root, unnamed)
        writeInnerComponent(out, component)
    }

    private fun writeInnerComponent(out: DataOutputStream, component: ChatComponent) {
        when (component) {
            is ChatComponent.Text -> {
                writeTagString(out, "text", component.text)
                if (component.color != null) {
                    writeTagString(out, "color", component.color)
                }
                if (component.underlined != null) {
                    writeTagBoolean(out, "underlined", component.underlined)
                }
                if (component.italic != null) {
                    writeTagBoolean(out, "italic", component.italic)
                }
                if (component.hoverText != null) {
                    writeHoverText(out, component.hoverText)
                }
                if (component.hoverEntity != null) {
                    writeHoverEntity(out, component.hoverEntity)
                }
                if (component.extra.isNotEmpty()) {
                    writeComponentList(out, "extra", component.extra)
                }
            }
            is ChatComponent.Translate -> {
                writeTagString(out, "translate", component.key)
                if (component.color != null) {
                    writeTagString(out, "color", component.color)
                }
                if (component.underlined != null) {
                    writeTagBoolean(out, "underlined", component.underlined)
                }
                if (component.italic != null) {
                    writeTagBoolean(out, "italic", component.italic)
                }
                if (component.args.isNotEmpty()) {
                    writeComponentList(out, "with", component.args)
                }
                if (component.extra.isNotEmpty()) {
                    writeComponentList(out, "extra", component.extra)
                }
            }
        }
        out.writeByte(0) // TAG_End
    }

    private fun writeComponentList(out: DataOutputStream, name: String, components: List<ChatComponent>) {
        out.writeByte(9) // TAG_List
        out.writeUTF(name)
        out.writeByte(10) // list type: TAG_Compound
        out.writeInt(components.size)
        for (child in components) {
            writeInnerComponent(out, child)
        }
    }

    private fun writeHoverText(out: DataOutputStream, text: String) {
        out.writeByte(10) // TAG_Compound
        out.writeUTF("hover_event")
        writeTagString(out, "action", "show_text")
        out.writeByte(10) // TAG_Compound
        out.writeUTF("value")
        writeInnerComponent(out, ChatComponent.Text(text))
        out.writeByte(0) // TAG_End (hover_event compound)
    }

    private fun writeHoverEntity(out: DataOutputStream, hover: HoverEntity) {
        out.writeByte(10) // TAG_Compound
        out.writeUTF("hover_event")
        writeTagString(out, "action", "show_entity")
        if (hover.name != null) {
            out.writeByte(10) // TAG_Compound
            out.writeUTF("name")
            writeInnerComponent(out, ChatComponent.Text(hover.name))
        }
        writeTagString(out, "id", hover.entityType)
        writeTagString(out, "uuid", hover.uuid.toString())
        out.writeByte(0) // TAG_End (hover_event compound)
    }

    private fun writeTagString(out: DataOutputStream, name: String, value: String) {
        out.writeByte(8) // TAG_String
        out.writeUTF(name)
        out.writeUTF(value)
    }

    private fun writeTagBoolean(out: DataOutputStream, name: String, value: Boolean) {
        out.writeByte(1) // TAG_Byte
        out.writeUTF(name)
        out.writeByte(if (value) 1 else 0)
    }

    private fun playerEntityTypeId(): Int {
        return RegistryCodec.entryIndex("minecraft:entity_type", "minecraft:player")
            ?: FALLBACK_PLAYER_ENTITY_TYPE_ID_1_21_11
    }

    private fun angleByte(degrees: Float): Int {
        val normalized = ((degrees % 360f) + 360f) % 360f
        return ((normalized * 256.0f / 360.0f).toInt() and 0xFF)
    }

    private fun writeLpVector3(out: DataOutputStream, packet: ByteArrayOutputStream, xIn: Double, yIn: Double, zIn: Double) {
        val x = sanitizeLpComponent(xIn)
        val y = sanitizeLpComponent(yIn)
        val z = sanitizeLpComponent(zIn)
        val absMax = maxOf(kotlin.math.abs(x), kotlin.math.abs(y), kotlin.math.abs(z))
        if (absMax < 3.051944088384301E-5) {
            out.writeByte(0)
            return
        }

        val scaleLong = kotlin.math.ceil(absMax).toLong().coerceAtLeast(1L)
        val hasContinuation = (scaleLong and 0b11L) != scaleLong
        val flags = if (hasContinuation) ((scaleLong and 0b11L) or 0b100L) else scaleLong

        val px = packLpComponent(x / scaleLong) shl 3
        val py = packLpComponent(y / scaleLong) shl 18
        val pz = packLpComponent(z / scaleLong) shl 33
        val packed = flags or px or py or pz

        out.writeByte((packed and 0xFF).toInt())
        out.writeByte(((packed ushr 8) and 0xFF).toInt())
        out.writeInt(((packed ushr 16) and 0xFFFF_FFFFL).toInt())
        if (hasContinuation) {
            NetworkUtils.writeVarInt(packet, (scaleLong ushr 2).toInt())
        }
    }

    private fun sanitizeLpComponent(value: Double): Double {
        if (value.isNaN()) return 0.0
        return value.coerceIn(-1.7179869183E10, 1.7179869183E10)
    }

    private fun packLpComponent(value: Double): Long {
        val clamped = value.coerceIn(-1.0, 1.0)
        val quantized = kotlin.math.round((clamped * 0.5 + 0.5) * 32766.0).toLong()
        return quantized and 0x7FFF
    }

    private fun buildAirChunkData(): ByteArray {
        val sectionCount = WORLD_HEIGHT / 16
        val plainsBiomeId = RegistryCodec.entryIndex("minecraft:worldgen/biome", "minecraft:plains") ?: 0

        val data = ByteArrayOutputStream()
        val out = DataOutputStream(data)
        repeat(sectionCount) {
            // non-empty block count
            out.writeShort(0)
            // block states: single-valued palette with air(0)
            writeSingleValuePalettedContainer(data, 0)
            // biomes: single-valued palette with plains
            writeSingleValuePalettedContainer(data, plainsBiomeId)
        }
        return data.toByteArray()
    }

    private fun writeSingleValuePalettedContainer(stream: ByteArrayOutputStream, valueId: Int) {
        val out = DataOutputStream(stream)
        out.writeByte(0)
        NetworkUtils.writeVarInt(stream, valueId)
    }

}
