package org.macaroon3145.network.handler

import org.macaroon3145.network.NetworkUtils
import org.macaroon3145.network.codec.RegistryCodec
import org.macaroon3145.world.GeneratedChunk
import org.macaroon3145.world.WorldManager
import java.io.ByteArrayOutputStream
import java.io.DataOutputStream
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlin.math.floor

object PlayPackets {
    data class ChunkBlockEntityEntry(
        val x: Int,
        val y: Int,
        val z: Int,
        val typeId: Int,
        val nbtPayload: ByteArray
    )

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
            val bold: Boolean? = null,
            val underlined: Boolean? = null,
            val italic: Boolean? = null
        ) : ChatComponent

        data class Translate(
            val key: String,
            val args: List<ChatComponent> = emptyList(),
            val extra: List<ChatComponent> = emptyList(),
            val color: String? = null,
            val bold: Boolean? = null,
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
    private const val ACK_BLOCK_CHANGED_PACKET_ID = 0x04
    private const val BLOCK_ACTION_PACKET_ID = 0x07
    private const val BLOCK_CHANGE_PACKET_ID = 0x08
    // 1.21.11 play/clientbound: with bundle delimiter at 0x00, block_destruction is 0x05.
    private const val BLOCK_DESTRUCTION_PACKET_ID = 0x05
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
    private const val SET_PASSENGERS_PACKET_ID = 0x69
    private const val POSITION_PACKET_ID = 0x46
    private const val REMOVE_ENTITIES_PACKET_ID = 0x4B
    private const val RESPAWN_PACKET_ID = 0x50
    private const val TAKE_ITEM_ENTITY_PACKET_ID = 0x7A
    private const val ENTITY_HEAD_LOOK_PACKET_ID = 0x51
    private const val UPDATE_VIEW_POSITION_PACKET_ID = 0x5C
    private const val UPDATE_VIEW_DISTANCE_PACKET_ID = 0x5D
    private const val SPAWN_POSITION_PACKET_ID = 0x5F
    private const val TIME_UPDATE_PACKET_ID = 0x6F
    private const val SYSTEM_CHAT_PACKET_ID = 0x77
    private const val SET_HEALTH_PACKET_ID = 0x66
    private const val SET_HELD_SLOT_PACKET_ID = 0x67
    private const val CONTAINER_SET_SLOT_PACKET_ID = 0x14
    private const val COOLDOWN_PACKET_ID = 0x16
    private const val CONTAINER_SET_CONTENT_PACKET_ID = 0x12
    private const val CONTAINER_SET_DATA_PACKET_ID = 0x13
    private const val CONTAINER_CLOSE_PACKET_ID = 0x11
    private const val OPEN_SCREEN_PACKET_ID = 0x39
    private const val COMMAND_SUGGESTIONS_PACKET_ID = 0x0F
    private const val COMMANDS_PACKET_ID = 0x10
    private const val CUSTOM_PAYLOAD_PACKET_ID = 0x18
    private const val DAMAGE_EVENT_PACKET_ID = 0x19
    private const val TICKING_STATE_PACKET_ID = 0x7D
    private const val TICKING_STEP_PACKET_ID = 0x7E
    private const val ENTITY_EVENT_PACKET_ID = 0x22
    private const val HURT_ANIMATION_PACKET_ID = 0x29
    private const val SOUND_ENTITY_PACKET_ID = 0x72
    private const val SOUND_PACKET_ID = 0x73
    private const val PLAYER_ABILITIES_PACKET_ID = 0x3E
    private const val UPDATE_ATTRIBUTES_PACKET_ID = 0x81
    private const val ATTRIBUTE_ID_MAP_RESOURCE = "/vanilla-attribute-id-map-1.21.11.json"

    private const val SPAWN_X = 0
    private const val SPAWN_Y = 65
    private const val SPAWN_Z = 0
    private const val WORLD_HEIGHT = 384
    private const val FALLBACK_PLAYER_ENTITY_TYPE_ID_1_21_11 = 155
    private const val FALLBACK_ITEM_ENTITY_TYPE_ID_1_21_11 = 71
    private const val FALLBACK_FALLING_BLOCK_ENTITY_TYPE_ID_1_21_11 = 51
    private const val FALLBACK_SNOWBALL_ENTITY_TYPE_ID_1_21_11 = 120
    private const val FALLBACK_EGG_ENTITY_TYPE_ID_1_21_11 = 39
    private const val FALLBACK_PIG_ENTITY_TYPE_ID_1_21_11 = 100
    private const val COMMAND_ARGUMENT_TYPE_GAME_PROFILE_ID_1_21_11 = 7
    private const val COMMAND_ARGUMENT_TYPE_VEC3_ID_1_21_11 = 10
    private const val COMMAND_ARGUMENT_TYPE_GAMEMODE_ID_1_21_11 = 42
    private const val COMMAND_ARGUMENT_TYPE_TIME_ID_1_21_11 = 43
    private const val COMMAND_ARGUMENT_TYPE_ENTITY_ID_1_21_11 = 6
    private const val COMMAND_ARGUMENT_TYPE_BRIGADIER_INTEGER_ID_1_21_11 = 3
    private const val COMMAND_ARGUMENT_TYPE_BRIGADIER_STRING_ID_1_21_11 = 5
    private const val ENTITY_ARGUMENT_FLAG_SINGLE = 0x01
    private const val ENTITY_ARGUMENT_FLAG_PLAYERS_ONLY = 0x02
    // 1.21.11: player model customization is tracked on PlayerLikeEntity before PlayerEntity fields.
    private const val PLAYER_SKIN_PARTS_METADATA_INDEX = 16
    private const val ENTITY_SHARED_FLAGS_METADATA_INDEX = 0
    private const val ENTITY_POSE_METADATA_INDEX = 6
    private const val LIVING_ENTITY_FLAGS_METADATA_INDEX = 8
    private const val LIVING_ENTITY_HEALTH_METADATA_INDEX = 9
    private const val ENTITY_POSE_METADATA_TYPE_ID = 20 // entity_pose in 1.21.11
    private const val FLOAT_METADATA_TYPE_ID = 3
    private const val ENTITY_FLAGS_SNEAKING = 0x02
    private const val ENTITY_FLAGS_SPRINTING = 0x08
    private const val ENTITY_FLAGS_SWIMMING = 0x10
    private const val LIVING_ENTITY_FLAG_IS_USING = 0x01
    private const val LIVING_ENTITY_FLAG_OFF_HAND = 0x02
    private const val POSE_STANDING = 0
    private const val POSE_SWIMMING = 3
    private const val POSE_CROUCHING = 5
    private const val POSE_DYING = 7
    private val cachedItemEntityTypeId: Int by lazy {
        RegistryCodec.entryIndex("minecraft:entity_type", "minecraft:item")
            ?: FALLBACK_ITEM_ENTITY_TYPE_ID_1_21_11
    }
    private val cachedFallingBlockEntityTypeId: Int by lazy {
        RegistryCodec.entryIndex("minecraft:entity_type", "minecraft:falling_block")
            ?: FALLBACK_FALLING_BLOCK_ENTITY_TYPE_ID_1_21_11
    }
    private val cachedSnowballEntityTypeId: Int by lazy {
        RegistryCodec.entryIndex("minecraft:entity_type", "minecraft:snowball")
            ?: FALLBACK_SNOWBALL_ENTITY_TYPE_ID_1_21_11
    }
    private val cachedEggEntityTypeId: Int by lazy {
        RegistryCodec.entryIndex("minecraft:entity_type", "minecraft:egg")
            ?: FALLBACK_EGG_ENTITY_TYPE_ID_1_21_11
    }
    private val cachedEnderPearlEntityTypeId: Int by lazy {
        RegistryCodec.entryIndex("minecraft:entity_type", "minecraft:ender_pearl")
            ?: cachedEggEntityTypeId
    }
    private val cachedPigEntityTypeId: Int by lazy {
        RegistryCodec.entryIndex("minecraft:entity_type", "minecraft:pig")
            ?: FALLBACK_PIG_ENTITY_TYPE_ID_1_21_11
    }
    private val cachedAttackSpeedAttributeId: Int? by lazy {
        loadAttributeIdMap()["minecraft:attack_speed"]
    }
    private val json = Json { ignoreUnknownKeys = true }

    fun prewarm() {
        cachedItemEntityTypeId
        cachedFallingBlockEntityTypeId
        cachedSnowballEntityTypeId
        cachedEggEntityTypeId
        cachedEnderPearlEntityTypeId
        cachedPigEntityTypeId
        cachedAttackSpeedAttributeId
    }

    private fun loadAttributeIdMap(): Map<String, Int> {
        return runCatching {
            val stream = PlayPackets::class.java.getResourceAsStream(ATTRIBUTE_ID_MAP_RESOURCE) ?: return@runCatching emptyMap()
            stream.bufferedReader().use { reader ->
                val root = json.parseToJsonElement(reader.readText()).jsonObject
                val entries = root["entries"]?.jsonObject ?: return@use emptyMap()
                entries.mapNotNull { (key, value) ->
                    value.jsonPrimitive.intOrNull?.let { key to it }
                }.toMap()
            }
        }.getOrDefault(emptyMap())
    }

    fun loginPacket(entityId: Int, worldKey: String, gameMode: Int): ByteArray {
        val packet = ByteArrayOutputStream()
        val out = DataOutputStream(packet)
        val clampedGameMode = gameMode.coerceIn(0, 3)
        val dimensionTypeId = RegistryCodec.entryIndex("minecraft:dimension_type", worldKey)
            ?: RegistryCodec.entryIndex("dimension_type", worldKey)
            ?: RegistryCodec.entryIndex("minecraft:dimension_type", "minecraft:overworld")
            ?: 0
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

        NetworkUtils.writeVarInt(packet, dimensionTypeId)
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
        teleportId: Int,
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
        NetworkUtils.writeVarInt(packet, teleportId.coerceAtLeast(0))
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

    fun setHealthPacket(health: Float, food: Int, saturation: Float): ByteArray {
        val packet = ByteArrayOutputStream()
        val out = DataOutputStream(packet)
        NetworkUtils.writeVarInt(packet, SET_HEALTH_PACKET_ID)
        out.writeFloat(health.coerceAtLeast(0f))
        NetworkUtils.writeVarInt(packet, food.coerceAtLeast(0))
        out.writeFloat(saturation.coerceAtLeast(0f))
        return packet.toByteArray()
    }

    fun acknowledgeBlockChangedPacket(sequenceId: Int): ByteArray {
        val packet = ByteArrayOutputStream()
        NetworkUtils.writeVarInt(packet, ACK_BLOCK_CHANGED_PACKET_ID)
        NetworkUtils.writeVarInt(packet, sequenceId.coerceAtLeast(0))
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

    fun cooldownPacket(cooldownGroup: String, cooldownTicks: Int): ByteArray {
        val packet = ByteArrayOutputStream()
        NetworkUtils.writeVarInt(packet, COOLDOWN_PACKET_ID)
        NetworkUtils.writeString(packet, cooldownGroup)
        NetworkUtils.writeVarInt(packet, cooldownTicks.coerceAtLeast(0))
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

    fun containerSetContentPacket(
        containerId: Int,
        stateId: Int,
        encodedItems: List<ByteArray>,
        encodedCarried: ByteArray
    ): ByteArray {
        val packet = ByteArrayOutputStream()
        NetworkUtils.writeVarInt(packet, CONTAINER_SET_CONTENT_PACKET_ID)
        NetworkUtils.writeVarInt(packet, containerId)
        NetworkUtils.writeVarInt(packet, stateId)
        NetworkUtils.writeVarInt(packet, encodedItems.size.coerceAtLeast(0))
        for (encoded in encodedItems) {
            packet.write(encoded)
        }
        packet.write(encodedCarried)
        return packet.toByteArray()
    }

    fun containerSetDataPacket(containerId: Int, property: Int, value: Int): ByteArray {
        val packet = ByteArrayOutputStream()
        val out = DataOutputStream(packet)
        NetworkUtils.writeVarInt(packet, CONTAINER_SET_DATA_PACKET_ID)
        NetworkUtils.writeVarInt(packet, containerId)
        out.writeShort(property)
        out.writeShort(value)
        return packet.toByteArray()
    }

    fun containerClosePacket(containerId: Int): ByteArray {
        val packet = ByteArrayOutputStream()
        NetworkUtils.writeVarInt(packet, CONTAINER_CLOSE_PACKET_ID)
        NetworkUtils.writeVarInt(packet, containerId)
        return packet.toByteArray()
    }

    fun openScreenPacket(containerId: Int, menuTypeId: Int, title: ChatComponent): ByteArray {
        val packet = ByteArrayOutputStream()
        val out = DataOutputStream(packet)
        NetworkUtils.writeVarInt(packet, OPEN_SCREEN_PACKET_ID)
        NetworkUtils.writeVarInt(packet, containerId)
        NetworkUtils.writeVarInt(packet, menuTypeId.coerceAtLeast(0))
        writeComponentNbt(out, title)
        return packet.toByteArray()
    }

    fun commandsPacket(
        includeOperatorCommands: Boolean,
        dynamicCommands: Collection<String> = emptyList()
    ): ByteArray {
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
        fun brigadierIntegerPayloadNoBounds(): ByteArray = byteArrayOf(0)
        fun brigadierStringPayloadSingleWord(): ByteArray = byteArrayOf(0)
        fun brigadierStringPayloadGreedyPhrase(): ByteArray = byteArrayOf(2)
        fun timePayload(minTicks: Int): ByteArray {
            val out = ByteArrayOutputStream()
            DataOutputStream(out).writeInt(minTicks)
            return out.toByteArray()
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
                    executable = true,
                    suggestionType = "minecraft:ask_server"
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
                    suggestionType = "minecraft:ask_server"
                )
                val targetDestinationIndex = addArgument(
                    name = "destination",
                    parserTypeId = COMMAND_ARGUMENT_TYPE_ENTITY_ID_1_21_11,
                    parserPayload = entityPayload(single = true, playersOnly = false),
                    executable = true,
                    suggestionType = "minecraft:ask_server"
                )
                val targetLocationIndex = addArgument(
                    name = "location",
                    parserTypeId = COMMAND_ARGUMENT_TYPE_VEC3_ID_1_21_11,
                    executable = true
                )
                setNode(
                    literalIndex,
                    nodes[literalIndex].copy(children = intArrayOf(targetsIndex, destinationIndex, locationIndex))
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
                    executable = true,
                    suggestionType = "minecraft:ask_server"
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
                    executable = true,
                    suggestionType = "minecraft:ask_server"
                )
                setNode(literalIndex, nodes[literalIndex].copy(children = intArrayOf(targetIndex)))
                rootChildren += literalIndex
            }

            fun addDeopSubtree() {
                val literalIndex = addLiteral("deop")
                val targetIndex = addArgument(
                    name = "targets",
                    parserTypeId = COMMAND_ARGUMENT_TYPE_GAME_PROFILE_ID_1_21_11,
                    executable = true,
                    suggestionType = "minecraft:ask_server"
                )
                setNode(literalIndex, nodes[literalIndex].copy(children = intArrayOf(targetIndex)))
                rootChildren += literalIndex
            }

            fun addStopLiteral() {
                rootChildren += addLiteral("stop", executable = true)
            }

            fun addSaveLiteral() {
                rootChildren += addLiteral("save", executable = true)
            }

            fun addTimeSubtree() {
                val literalIndex = addLiteral("time")
                val setLiteralIndex = addLiteral("set")
                val addLiteralIndex = addLiteral("add")
                val queryLiteralIndex = addLiteral("query")
                val setDayLiteralIndex = addLiteral("day", executable = true)
                val setNoonLiteralIndex = addLiteral("noon", executable = true)
                val setNightLiteralIndex = addLiteral("night", executable = true)
                val setMidnightLiteralIndex = addLiteral("midnight", executable = true)

                val setValueIndex = addArgument(
                    name = "value",
                    parserTypeId = COMMAND_ARGUMENT_TYPE_TIME_ID_1_21_11,
                    parserPayload = timePayload(0),
                    executable = true
                )

                val addValueIndex = addArgument(
                    name = "value",
                    parserTypeId = COMMAND_ARGUMENT_TYPE_TIME_ID_1_21_11,
                    parserPayload = timePayload(0),
                    executable = true
                )

                val queryDaytimeLiteralIndex = addLiteral("daytime", executable = true)
                val queryGametimeLiteralIndex = addLiteral("gametime", executable = true)
                val queryDayLiteralIndex = addLiteral("day", executable = true)

                setNode(literalIndex, nodes[literalIndex].copy(children = intArrayOf(setLiteralIndex, addLiteralIndex, queryLiteralIndex)))
                setNode(
                    setLiteralIndex,
                    nodes[setLiteralIndex].copy(
                        children = intArrayOf(
                            setDayLiteralIndex,
                            setNoonLiteralIndex,
                            setNightLiteralIndex,
                            setMidnightLiteralIndex,
                            setValueIndex
                        )
                    )
                )
                setNode(addLiteralIndex, nodes[addLiteralIndex].copy(children = intArrayOf(addValueIndex)))
                setNode(
                    queryLiteralIndex,
                    nodes[queryLiteralIndex].copy(
                        children = intArrayOf(
                            queryDaytimeLiteralIndex,
                            queryGametimeLiteralIndex,
                            queryDayLiteralIndex
                        )
                    )
                )
                rootChildren += literalIndex
            }

            fun addPerfSubtree() {
                val literalIndex = addLiteral("perf", executable = true)
                val worldLiteralIndices = WorldManager.allWorlds()
                    .asSequence()
                    .map { it.key.substringAfter(':', it.key) }
                    .distinct()
                    .sortedWith(String.CASE_INSENSITIVE_ORDER)
                    .map { addLiteral(it, executable = true) }
                    .toList()
                if (worldLiteralIndices.isNotEmpty()) {
                    setNode(literalIndex, nodes[literalIndex].copy(children = worldLiteralIndices.toIntArray()))
                }
                rootChildren += literalIndex
            }

            fun addReloadSubtree() {
                val literalIndex = addLiteral("reload", executable = true)
                val targetArgumentIndex = addArgument(
                    name = "pluginName",
                    parserTypeId = COMMAND_ARGUMENT_TYPE_BRIGADIER_STRING_ID_1_21_11,
                    parserPayload = brigadierStringPayloadSingleWord(),
                    executable = true,
                    suggestionType = "minecraft:ask_server"
                )
                setNode(
                    literalIndex,
                    nodes[literalIndex].copy(children = intArrayOf(targetArgumentIndex))
                )
                rootChildren += literalIndex
            }

            addTpSubtree("tp")
            addTpSubtree("teleport")
            addGamemodeSubtree()
            addOpSubtree()
            addDeopSubtree()
            addStopLiteral()
            addSaveLiteral()
            addTimeSubtree()
            addPerfSubtree()
            addReloadSubtree()
        }

        val staticLiterals = hashSetOf(
            "tp",
            "teleport",
            "gamemode",
            "op",
            "deop",
            "stop",
            "save",
            "time",
            "perf",
            "reload"
        )
        val dynamicLiterals = dynamicCommands.asSequence()
            .map { it.trim().lowercase() }
            .filter { it.isNotEmpty() && it !in staticLiterals }
            .distinct()
            .sortedWith(String.CASE_INSENSITIVE_ORDER)
            .toList()
        for (literal in dynamicLiterals) {
            val literalIndex = addLiteral(literal, executable = true)
            val argsIndex = addArgument(
                name = "args",
                parserTypeId = COMMAND_ARGUMENT_TYPE_BRIGADIER_STRING_ID_1_21_11,
                parserPayload = brigadierStringPayloadGreedyPhrase(),
                executable = true,
                suggestionType = "minecraft:ask_server"
            )
            setNode(literalIndex, nodes[literalIndex].copy(children = intArrayOf(argsIndex)))
            rootChildren += literalIndex
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

    fun playerInfoPacket(
        profile: ConnectionProfile,
        displayName: String,
        gameMode: Int,
        latencyMs: Int,
        listed: Boolean = true
    ): ByteArray {
        val packet = ByteArrayOutputStream()
        val out = DataOutputStream(packet)
        val clampedGameMode = gameMode.coerceIn(0, 3)
        val safeDisplayName = displayName
            .trim()
            .ifEmpty { profile.username }
            .take(16)
        NetworkUtils.writeVarInt(packet, PLAYER_INFO_PACKET_ID)

        val actionFlags = 0x01 or 0x04 or 0x08 or 0x10 or 0x40
        out.writeByte(actionFlags)
        NetworkUtils.writeVarInt(packet, 1)
        NetworkUtils.writeUUID(packet, profile.uuid)

        NetworkUtils.writeString(packet, safeDisplayName)
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
        NetworkUtils.writeVarInt(packet, if (listed) 1 else 0) // listed
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

    fun entityEventPacket(entityId: Int, eventId: Int): ByteArray {
        val packet = ByteArrayOutputStream()
        val out = DataOutputStream(packet)
        NetworkUtils.writeVarInt(packet, ENTITY_EVENT_PACKET_ID)
        out.writeInt(entityId)
        out.writeByte(eventId)
        return packet.toByteArray()
    }

    fun hurtAnimationPacket(entityId: Int, yaw: Float): ByteArray {
        val packet = ByteArrayOutputStream()
        val out = DataOutputStream(packet)
        NetworkUtils.writeVarInt(packet, HURT_ANIMATION_PACKET_ID)
        NetworkUtils.writeVarInt(packet, entityId)
        out.writeFloat(yaw)
        return packet.toByteArray()
    }

    fun damageEventPacket(entityId: Int, damageTypeId: Int): ByteArray {
        val packet = ByteArrayOutputStream()
        NetworkUtils.writeVarInt(packet, DAMAGE_EVENT_PACKET_ID)
        NetworkUtils.writeVarInt(packet, entityId)
        NetworkUtils.writeVarInt(packet, damageTypeId)
        NetworkUtils.writeVarInt(packet, 0)
        NetworkUtils.writeVarInt(packet, 0)
        packet.write(0)
        return packet.toByteArray()
    }

    fun soundEntityPacket(
        soundEventId: Int,
        soundSourceId: Int,
        entityId: Int,
        volume: Float,
        pitch: Float,
        seed: Long
    ): ByteArray {
        val packet = ByteArrayOutputStream()
        val out = DataOutputStream(packet)
        NetworkUtils.writeVarInt(packet, SOUND_ENTITY_PACKET_ID)
        // SoundEvent holder wire format uses registry reference as (id + 1).
        NetworkUtils.writeVarInt(packet, soundEventId + 1)
        NetworkUtils.writeVarInt(packet, soundSourceId)
        NetworkUtils.writeVarInt(packet, entityId)
        out.writeFloat(volume)
        out.writeFloat(pitch)
        out.writeLong(seed)
        return packet.toByteArray()
    }

    fun soundPacket(
        soundEventId: Int,
        soundSourceId: Int,
        x: Double,
        y: Double,
        z: Double,
        volume: Float,
        pitch: Float,
        seed: Long
    ): ByteArray {
        val packet = ByteArrayOutputStream()
        val out = DataOutputStream(packet)
        NetworkUtils.writeVarInt(packet, SOUND_PACKET_ID)
        // Holder<SoundEvent> registry reference is encoded as (id + 1).
        NetworkUtils.writeVarInt(packet, soundEventId + 1)
        NetworkUtils.writeVarInt(packet, soundSourceId)
        out.writeInt((x * 8.0).toInt())
        out.writeInt((y * 8.0).toInt())
        out.writeInt((z * 8.0).toInt())
        out.writeFloat(volume)
        out.writeFloat(pitch)
        out.writeLong(seed)
        return packet.toByteArray()
    }

    fun soundPacketByKey(
        soundKey: String,
        soundSourceId: Int,
        x: Double,
        y: Double,
        z: Double,
        volume: Float,
        pitch: Float,
        seed: Long
    ): ByteArray {
        val packet = ByteArrayOutputStream()
        val out = DataOutputStream(packet)
        NetworkUtils.writeVarInt(packet, SOUND_PACKET_ID)
        // Holder<SoundEvent> direct encoding:
        // 0 => inline SoundEvent payload (Identifier + Optional<Float> fixedRange).
        NetworkUtils.writeVarInt(packet, 0)
        NetworkUtils.writeString(packet, soundKey)
        out.writeBoolean(false) // fixedRange absent (variable-range sound)
        NetworkUtils.writeVarInt(packet, soundSourceId)
        out.writeInt((x * 8.0).toInt())
        out.writeInt((y * 8.0).toInt())
        out.writeInt((z * 8.0).toInt())
        out.writeFloat(volume)
        out.writeFloat(pitch)
        out.writeLong(seed)
        return packet.toByteArray()
    }

    fun updateAttackSpeedAttributePacket(entityId: Int, attackSpeed: Double): ByteArray {
        val attributeId = cachedAttackSpeedAttributeId ?: return ByteArray(0)
        val packet = ByteArrayOutputStream()
        val out = DataOutputStream(packet)
        NetworkUtils.writeVarInt(packet, UPDATE_ATTRIBUTES_PACKET_ID)
        NetworkUtils.writeVarInt(packet, entityId)
        NetworkUtils.writeVarInt(packet, 1) // one attribute snapshot
        // Holder<Attribute> registry id encoding in play protocol.
        NetworkUtils.writeVarInt(packet, attributeId)
        out.writeDouble(attackSpeed)
        NetworkUtils.writeVarInt(packet, 0) // no modifiers
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

    fun blockDestructionPacket(breakerEntityId: Int, x: Int, y: Int, z: Int, stage: Int): ByteArray {
        val packet = ByteArrayOutputStream()
        val out = DataOutputStream(packet)
        NetworkUtils.writeVarInt(packet, BLOCK_DESTRUCTION_PACKET_ID)
        NetworkUtils.writeVarInt(packet, breakerEntityId)
        out.writeLong(packPosition(x, y, z))
        out.writeByte(stage.coerceIn(-1, 9))
        return packet.toByteArray()
    }

    fun blockActionPacket(x: Int, y: Int, z: Int, actionId: Int, actionParam: Int, blockId: Int): ByteArray {
        val packet = ByteArrayOutputStream()
        val out = DataOutputStream(packet)
        NetworkUtils.writeVarInt(packet, BLOCK_ACTION_PACKET_ID)
        out.writeLong(packPosition(x, y, z))
        out.writeByte(actionId.coerceIn(0, 255))
        out.writeByte(actionParam.coerceIn(0, 255))
        NetworkUtils.writeVarInt(packet, blockId.coerceAtLeast(0))
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

    fun setPassengersPacket(vehicleEntityId: Int, passengerEntityIds: IntArray): ByteArray {
        val packet = ByteArrayOutputStream()
        NetworkUtils.writeVarInt(packet, SET_PASSENGERS_PACKET_ID)
        NetworkUtils.writeVarInt(packet, vehicleEntityId)
        NetworkUtils.writeVarInt(packet, passengerEntityIds.size.coerceAtLeast(0))
        for (passengerEntityId in passengerEntityIds) {
            NetworkUtils.writeVarInt(packet, passengerEntityId)
        }
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

    fun throwableItemMetadataPacket(entityId: Int, encodedItemStack: ByteArray): ByteArray {
        val packet = ByteArrayOutputStream()
        val out = DataOutputStream(packet)
        NetworkUtils.writeVarInt(packet, ENTITY_METADATA_PACKET_ID)
        NetworkUtils.writeVarInt(packet, entityId)
        // ThrowableItemProjectile item stack metadata index:
        // Base Entity metadata occupies 0..7. ThrowableItemProjectile defines local index 0 => global 8.
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
        return ItemStackCodec.encodeSimple(itemId, count)
    }

    fun itemEntityTypeId(): Int {
        return cachedItemEntityTypeId
    }

    fun fallingBlockEntityTypeId(): Int {
        return cachedFallingBlockEntityTypeId
    }

    fun snowballEntityTypeId(): Int {
        return cachedSnowballEntityTypeId
    }

    fun eggEntityTypeId(): Int {
        return cachedEggEntityTypeId
    }

    fun enderPearlEntityTypeId(): Int {
        return cachedEnderPearlEntityTypeId
    }

    fun pigEntityTypeId(): Int {
        return cachedPigEntityTypeId
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
        swimming: Boolean,
        usingItemHand: Int = -1,
        forcedPose: Int? = null
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
        val pose = forcedPose ?: when {
            swimming -> POSE_SWIMMING
            sneaking -> POSE_CROUCHING
            else -> POSE_STANDING
        }
        NetworkUtils.writeVarInt(packet, pose)

        out.writeByte(LIVING_ENTITY_FLAGS_METADATA_INDEX)
        NetworkUtils.writeVarInt(packet, 0) // metadata type: byte
        var livingFlags = 0
        if (usingItemHand in 0..1) {
            livingFlags = livingFlags or LIVING_ENTITY_FLAG_IS_USING
            if (usingItemHand == 1) {
                livingFlags = livingFlags or LIVING_ENTITY_FLAG_OFF_HAND
            }
        }
        out.writeByte(livingFlags)

        out.writeByte(0xFF) // metadata terminator
        return packet.toByteArray()
    }

    fun playerDeathMetadataPacket(entityId: Int): ByteArray {
        return playerSharedFlagsMetadataPacket(
            entityId = entityId,
            sneaking = false,
            sprinting = false,
            swimming = false,
            usingItemHand = -1,
            forcedPose = POSE_DYING
        )
    }

    fun playerHealthMetadataPacket(entityId: Int, health: Float): ByteArray {
        val packet = ByteArrayOutputStream()
        val out = DataOutputStream(packet)
        NetworkUtils.writeVarInt(packet, ENTITY_METADATA_PACKET_ID)
        NetworkUtils.writeVarInt(packet, entityId)
        out.writeByte(LIVING_ENTITY_HEALTH_METADATA_INDEX)
        NetworkUtils.writeVarInt(packet, FLOAT_METADATA_TYPE_ID) // metadata type: float
        out.writeFloat(health.coerceAtLeast(0f))
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

    fun mapChunkPacket(
        chunkX: Int,
        chunkZ: Int,
        generated: GeneratedChunk,
        blockEntities: List<ChunkBlockEntityEntry> = emptyList()
    ): ByteArray {
        val packet = ByteArrayOutputStream()
        val out = DataOutputStream(packet)
        val chunkData = if (generated.chunkData.isNotEmpty()) generated.chunkData else buildAirChunkData()
        NetworkUtils.writeVarInt(packet, MAP_CHUNK_PACKET_ID)
        out.writeInt(chunkX)
        out.writeInt(chunkZ)

        // heightmaps map<type,long[]> (1.21.11 level_chunk_with_light).
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
        NetworkUtils.writeVarInt(packet, blockEntities.size)
        for (entry in blockEntities) {
            val packedXZ = ((entry.x and 15) shl 4) or (entry.z and 15)
            out.writeByte(packedXZ and 0xFF)
            out.writeShort(entry.y)
            NetworkUtils.writeVarInt(packet, entry.typeId.coerceAtLeast(0))
            out.write(entry.nbtPayload)
        }
        val lightData = normalizedLightData(generated)
        writeLongArray(packet, out, lightData.skyLightMask)
        writeLongArray(packet, out, lightData.blockLightMask)
        writeLongArray(packet, out, lightData.emptySkyLightMask)
        writeLongArray(packet, out, lightData.emptyBlockLightMask)
        writeByteArrayList(packet, lightData.skyLight)
        writeByteArrayList(packet, lightData.blockLight)
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

    fun gameStateImmediateRespawnPacket(enabled: Boolean): ByteArray {
        val packet = ByteArrayOutputStream()
        val out = DataOutputStream(packet)
        NetworkUtils.writeVarInt(packet, GAME_STATE_CHANGE_PACKET_ID)
        out.writeByte(11)
        out.writeFloat(if (enabled) 1f else 0f)
        return packet.toByteArray()
    }

    fun playerAbilitiesPacket(
        invulnerable: Boolean,
        flying: Boolean,
        allowFlying: Boolean,
        instantBuild: Boolean,
        flyingSpeed: Float = 0.05f,
        walkingSpeed: Float = 0.1f
    ): ByteArray {
        val packet = ByteArrayOutputStream()
        val out = DataOutputStream(packet)
        NetworkUtils.writeVarInt(packet, PLAYER_ABILITIES_PACKET_ID)
        var flags = 0
        if (invulnerable) flags = flags or 0x01
        if (flying) flags = flags or 0x02
        if (allowFlying) flags = flags or 0x04
        if (instantBuild) flags = flags or 0x08
        out.writeByte(flags)
        out.writeFloat(flyingSpeed)
        out.writeFloat(walkingSpeed)
        return packet.toByteArray()
    }

    fun respawnPacket(
        worldKey: String,
        gameMode: Int,
        previousGameMode: Int = -1,
        hashedSeed: Long = 0L,
        isDebug: Boolean = false,
        isFlat: Boolean = false,
        portalCooldown: Int = 0,
        seaLevel: Int = 63,
        copyDataFlags: Int = 0
    ): ByteArray {
        val packet = ByteArrayOutputStream()
        val out = DataOutputStream(packet)
        NetworkUtils.writeVarInt(packet, RESPAWN_PACKET_ID)
        val dimensionTypeId = RegistryCodec.entryIndex("minecraft:dimension_type", worldKey)
            ?: RegistryCodec.entryIndex("dimension_type", worldKey)
            ?: RegistryCodec.entryIndex("minecraft:dimension_type", "minecraft:overworld")
            ?: 0
        NetworkUtils.writeVarInt(packet, dimensionTypeId)
        NetworkUtils.writeString(packet, worldKey)
        out.writeLong(hashedSeed)
        out.writeByte(gameMode.coerceIn(0, 3))
        out.writeByte(if (previousGameMode in 0..3) previousGameMode else -1)
        out.writeBoolean(isDebug)
        out.writeBoolean(isFlat)
        out.writeBoolean(false) // Optional<GlobalPos> death location: absent
        NetworkUtils.writeVarInt(packet, portalCooldown.coerceAtLeast(0))
        NetworkUtils.writeVarInt(packet, seaLevel.coerceAtLeast(0))
        out.writeByte(copyDataFlags and 0xFF)
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
                if (component.bold != null) {
                    writeTagBoolean(out, "bold", component.bold)
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
                if (component.bold != null) {
                    writeTagBoolean(out, "bold", component.bold)
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

    private data class NormalizedChunkLightData(
        val skyLightMask: LongArray,
        val blockLightMask: LongArray,
        val emptySkyLightMask: LongArray,
        val emptyBlockLightMask: LongArray,
        val skyLight: List<ByteArray>,
        val blockLight: List<ByteArray>
    )

    private fun normalizedLightData(generated: GeneratedChunk): NormalizedChunkLightData {
        val normalizedSky = normalizeLightSeries(generated.skyLightMask, generated.skyLight)
        val normalizedBlock = normalizeLightSeries(generated.blockLightMask, generated.blockLight)
        return NormalizedChunkLightData(
            skyLightMask = normalizedSky.first,
            blockLightMask = normalizedBlock.first,
            emptySkyLightMask = generated.emptySkyLightMask.copyOf(),
            emptyBlockLightMask = generated.emptyBlockLightMask.copyOf(),
            skyLight = normalizedSky.second,
            blockLight = normalizedBlock.second
        )
    }

    private fun normalizeLightSeries(mask: LongArray, arrays: List<ByteArray>): Pair<LongArray, List<ByteArray>> {
        if (mask.isEmpty() || arrays.isEmpty()) return LongArray(0) to emptyList()

        val normalizedMask = mask.copyOf()
        val expectedCount = countMaskBits(normalizedMask)
        if (expectedCount <= 0) return LongArray(0) to emptyList()

        val copiedArrays = arrays.map { it.copyOf() }.toMutableList()
        if (copiedArrays.size == expectedCount) return normalizedMask to copiedArrays

        if (copiedArrays.size > expectedCount) {
            return normalizedMask to copiedArrays.subList(0, expectedCount)
        }

        var missing = expectedCount - copiedArrays.size
        for (wordIndex in normalizedMask.indices.reversed()) {
            if (missing == 0) break
            var word = normalizedMask[wordIndex]
            if (word == 0L) continue
            for (bit in 63 downTo 0) {
                if (missing == 0) break
                val bitFlag = 1L shl bit
                if ((word and bitFlag) == 0L) continue
                word = word and bitFlag.inv()
                missing--
            }
            normalizedMask[wordIndex] = word
        }

        return normalizedMask to copiedArrays
    }

    private fun countMaskBits(mask: LongArray): Int = mask.sumOf { java.lang.Long.bitCount(it) }

    private fun writeLongArray(packet: ByteArrayOutputStream, out: DataOutputStream, values: LongArray) {
        NetworkUtils.writeVarInt(packet, values.size)
        for (value in values) {
            out.writeLong(value)
        }
    }

    private fun writeByteArrayList(packet: ByteArrayOutputStream, values: List<ByteArray>) {
        NetworkUtils.writeVarInt(packet, values.size)
        for (value in values) {
            NetworkUtils.writeVarInt(packet, value.size)
            packet.write(value)
        }
    }

}
