package org.macaroon3145.api.packet

import java.io.ByteArrayOutputStream
import kotlin.text.Charsets.UTF_8

enum class PacketDirection {
    INBOUND,
    OUTBOUND
}

enum class ProtocolPhase {
    HANDSHAKE,
    STATUS,
    LOGIN,
    CONFIGURATION,
    PLAY,
    UNKNOWN
}

enum class KnownPacketType(
    val phase: ProtocolPhase,
    val direction: PacketDirection,
    val packetId: Int
) {
    HANDSHAKE_INTENTION(ProtocolPhase.HANDSHAKE, PacketDirection.INBOUND, 0x00),
    LOGIN_START(ProtocolPhase.LOGIN, PacketDirection.INBOUND, 0x00),
    LOGIN_ENCRYPTION_RESPONSE(ProtocolPhase.LOGIN, PacketDirection.INBOUND, 0x01),
    LOGIN_ACK(ProtocolPhase.LOGIN, PacketDirection.INBOUND, 0x03),
    CONFIG_CLIENT_INFORMATION(ProtocolPhase.CONFIGURATION, PacketDirection.INBOUND, 0x00),
    CONFIG_ACK_FINISH(ProtocolPhase.CONFIGURATION, PacketDirection.INBOUND, 0x03),
    PLAY_CHAT_COMMAND_UNSIGNED(ProtocolPhase.PLAY, PacketDirection.INBOUND, 0x06),
    PLAY_CHAT_COMMAND_SIGNED(ProtocolPhase.PLAY, PacketDirection.INBOUND, 0x07),
    PLAY_CHAT_MESSAGE(ProtocolPhase.PLAY, PacketDirection.INBOUND, 0x08),
    PLAY_COMMAND_SUGGESTION(ProtocolPhase.PLAY, PacketDirection.INBOUND, 0x0E),
    PLAY_LOGIN(ProtocolPhase.PLAY, PacketDirection.OUTBOUND, 0x30),
    PLAY_SYSTEM_CHAT(ProtocolPhase.PLAY, PacketDirection.OUTBOUND, 0x77),
    PLAY_COMMAND_SUGGESTIONS(ProtocolPhase.PLAY, PacketDirection.OUTBOUND, 0x0F),
    PLAY_COMMANDS(ProtocolPhase.PLAY, PacketDirection.OUTBOUND, 0x10);

    companion object {
        private val byKey = entries.associateBy { key(it.phase, it.direction, it.packetId) }

        fun resolve(phase: ProtocolPhase, direction: PacketDirection, packetId: Int): KnownPacketType? {
            return byKey[key(phase, direction, packetId)]
        }

        private fun key(phase: ProtocolPhase, direction: PacketDirection, packetId: Int): String {
            return "${phase.name}:${direction.name}:$packetId"
        }
    }
}

data class PacketEnvelope(
    val direction: PacketDirection,
    val phase: ProtocolPhase,
    val packetId: Int,
    val payload: ByteArray,
    val channelKey: String,
    val knownType: KnownPacketType? = KnownPacketType.resolve(phase, direction, packetId)
) {
    fun copyPayload(newPayload: ByteArray): PacketEnvelope {
        return copy(payload = newPayload)
    }

    fun copyPacket(packetId: Int, payload: ByteArray): PacketEnvelope {
        return copy(packetId = packetId, payload = payload)
    }

    fun reader(): PacketReader = PacketReader(payload)
}

class PacketReader(private val bytes: ByteArray) {
    var cursor: Int = 0
        private set

    fun remaining(): Int = bytes.size - cursor

    fun readBoolean(): Boolean {
        ensureReadable(1)
        return bytes[cursor++].toInt() != 0
    }

    fun readByte(): Byte {
        ensureReadable(1)
        return bytes[cursor++]
    }

    fun readShort(): Int {
        ensureReadable(2)
        return ((bytes[cursor++].toInt() and 0xFF) shl 8) or
            (bytes[cursor++].toInt() and 0xFF)
    }

    fun readInt(): Int {
        ensureReadable(4)
        return ((bytes[cursor++].toInt() and 0xFF) shl 24) or
            ((bytes[cursor++].toInt() and 0xFF) shl 16) or
            ((bytes[cursor++].toInt() and 0xFF) shl 8) or
            (bytes[cursor++].toInt() and 0xFF)
    }

    fun readLong(): Long {
        ensureReadable(8)
        var out = 0L
        repeat(8) {
            out = (out shl 8) or (bytes[cursor++].toLong() and 0xFFL)
        }
        return out
    }

    fun readVarInt(): Int {
        var numRead = 0
        var result = 0
        var read: Int
        do {
            read = readByte().toInt() and 0xFF
            val value = read and 0b0111_1111
            result = result or (value shl (7 * numRead))
            numRead++
            if (numRead > 5) throw IllegalStateException("VarInt too big")
        } while ((read and 0b1000_0000) != 0)
        return result
    }

    fun readString(maxLength: Int = 32_767): String {
        val size = readVarInt()
        if (size < 0 || size > maxLength) {
            throw IllegalArgumentException("String length out of bounds: $size")
        }
        ensureReadable(size)
        val value = String(bytes, cursor, size, UTF_8)
        cursor += size
        return value
    }

    fun readRemaining(): ByteArray {
        val out = bytes.copyOfRange(cursor, bytes.size)
        cursor = bytes.size
        return out
    }

    private fun ensureReadable(required: Int) {
        if (remaining() < required) {
            throw IndexOutOfBoundsException("Need $required bytes, but only ${remaining()} remaining")
        }
    }
}

class PacketWriter {
    private val out = ByteArrayOutputStream()

    fun writeBoolean(value: Boolean): PacketWriter {
        out.write(if (value) 1 else 0)
        return this
    }

    fun writeByte(value: Int): PacketWriter {
        out.write(value and 0xFF)
        return this
    }

    fun writeShort(value: Int): PacketWriter {
        out.write((value ushr 8) and 0xFF)
        out.write(value and 0xFF)
        return this
    }

    fun writeInt(value: Int): PacketWriter {
        out.write((value ushr 24) and 0xFF)
        out.write((value ushr 16) and 0xFF)
        out.write((value ushr 8) and 0xFF)
        out.write(value and 0xFF)
        return this
    }

    fun writeLong(value: Long): PacketWriter {
        for (shift in 56 downTo 0 step 8) {
            out.write(((value ushr shift) and 0xFF).toInt())
        }
        return this
    }

    fun writeVarInt(value: Int): PacketWriter {
        var v = value
        while (true) {
            if ((v and 0xFFFFFF80.toInt()) == 0) {
                out.write(v)
                return this
            }
            out.write((v and 0x7F) or 0x80)
            v = v ushr 7
        }
    }

    fun writeString(value: String): PacketWriter {
        val bytes = value.toByteArray(UTF_8)
        writeVarInt(bytes.size)
        out.write(bytes)
        return this
    }

    fun writeBytes(bytes: ByteArray): PacketWriter {
        out.write(bytes)
        return this
    }

    fun toByteArray(): ByteArray = out.toByteArray()
}

fun interface PacketInterceptor {
    fun intercept(packet: PacketEnvelope): PacketEnvelope?
}

interface PacketInterceptorHandle {
    val active: Boolean
    fun unregister()
}

interface PacketInterceptorRegistry {
    fun register(owner: String, interceptor: PacketInterceptor): PacketInterceptorHandle
    fun unregisterOwner(owner: String)

    fun intercept(packet: PacketEnvelope): PacketEnvelope?

    fun registerKnown(
        owner: String,
        knownType: KnownPacketType,
        interceptor: (PacketEnvelope) -> PacketEnvelope?
    ): PacketInterceptorHandle {
        return register(owner) { packet ->
            if (packet.knownType == knownType) interceptor(packet) else packet
        }
    }
}
