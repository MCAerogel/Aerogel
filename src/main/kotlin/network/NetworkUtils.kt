package org.macaroon3145.network

import io.netty.buffer.ByteBuf
import java.io.DataOutputStream
import java.io.OutputStream
import java.nio.charset.StandardCharsets
import java.util.UUID

object NetworkUtils {
    fun writeVarInt(stream: OutputStream, value: Int) {
        var i = value
        while (true) {
            if ((i and 0xFFFFFF80.toInt()) == 0) {
                stream.write(i)
                return
            }
            stream.write(i and 0x7F or 0x80)
            i = i ushr 7
        }
    }

    fun readVarInt(buf: ByteBuf): Int {
        var numRead = 0
        var result = 0
        var read: Byte
        do {
            read = buf.readByte()
            val value = (read.toInt() and 0b01111111)
            result = result or (value shl (7 * numRead))

            numRead++
            if (numRead > 5) throw RuntimeException("VarInt too big")
        } while ((read.toInt() and 0b10000000) != 0)
        return result
    }

    fun peekVarInt(buf: ByteBuf): Int {
        val readerIndex = buf.readerIndex()
        val value = readVarInt(buf)
        buf.readerIndex(readerIndex)
        return value
    }

    fun readString(buf: ByteBuf): String {
        val length = readVarInt(buf)
        val bytes = ByteArray(length)
        buf.readBytes(bytes)
        return String(bytes, Charsets.UTF_8)
    }

    fun writeString(stream: OutputStream, value: String) {
        val bytes = value.toByteArray(StandardCharsets.UTF_8)
        writeVarInt(stream, bytes.size)
        stream.write(bytes)
    }

    fun readUUID(buf: ByteBuf): UUID {
        val msb = buf.readLong()
        val lsb = buf.readLong()
        return UUID(msb, lsb)
    }

    fun writeUUID(stream: OutputStream, uuid: UUID) {
        val out = DataOutputStream(stream)
        out.writeLong(uuid.mostSignificantBits)
        out.writeLong(uuid.leastSignificantBits)
    }

    data class Handshake(val protocolVersion: Int, val serverAddress: String, val serverPort: Int, val nextState: Int)

    fun readHandshake(buf: ByteBuf): Handshake {
        val packetId = readVarInt(buf)
        if (packetId != 0x00) throw RuntimeException("Not a handshake packet")
        val protocolVersion = readVarInt(buf)
        val serverAddress = readString(buf)
        val serverPort = buf.readUnsignedShort()
        val nextState = readVarInt(buf)
        return Handshake(protocolVersion, serverAddress, serverPort, nextState)
    }

    fun isHandshakePacket(buf: ByteBuf): Boolean {
        val readerIndex = buf.readerIndex()
        val packetId = readVarInt(buf)
        buf.readerIndex(readerIndex)
        return packetId == 0x00
    }
}
