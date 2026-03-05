package org.macaroon3145.api.packet

import org.macaroon3145.api.plugin.PluginRuntime

fun <T : VersionlessPacket> onInbound(
    packetType: Class<T>,
    priority: PacketPriority = PacketPriority.NORMAL,
    listener: (VersionlessPacketEvent<T>) -> Unit
): PacketInterceptorHandle {
    val context = PluginRuntime.requireCurrentContext()
    return context.packets.onInbound(context.metadata.id, packetType, priority, listener)
}

inline fun <reified T : VersionlessPacket> onInbound(
    priority: PacketPriority = PacketPriority.NORMAL,
    noinline listener: (VersionlessPacketEvent<T>) -> Unit
): PacketInterceptorHandle {
    return onInbound(T::class.java, priority, listener)
}

fun <T : VersionlessPacket> onOutbound(
    packetType: Class<T>,
    priority: PacketPriority = PacketPriority.NORMAL,
    listener: (VersionlessPacketEvent<T>) -> Unit
): PacketInterceptorHandle {
    val context = PluginRuntime.requireCurrentContext()
    return context.packets.onOutbound(context.metadata.id, packetType, priority, listener)
}

inline fun <reified T : VersionlessPacket> onOutbound(
    priority: PacketPriority = PacketPriority.NORMAL,
    noinline listener: (VersionlessPacketEvent<T>) -> Unit
): PacketInterceptorHandle {
    return onOutbound(T::class.java, priority, listener)
}

fun supportsInbound(packetType: Class<out VersionlessPacket>): Boolean {
    val context = PluginRuntime.requireCurrentContext()
    return context.packets.supports(packetType, PacketDirection.INBOUND)
}

inline fun <reified T : VersionlessPacket> supportsInbound(): Boolean {
    return supportsInbound(T::class.java)
}

fun supportsOutbound(packetType: Class<out VersionlessPacket>): Boolean {
    val context = PluginRuntime.requireCurrentContext()
    return context.packets.supports(packetType, PacketDirection.OUTBOUND)
}

inline fun <reified T : VersionlessPacket> supportsOutbound(): Boolean {
    return supportsOutbound(T::class.java)
}

class VersionlessPacketListenerDsl internal constructor(
    private val registry: PacketInterceptorRegistry,
    private val owner: String
) {
    private val handles = ArrayList<PacketInterceptorHandle>()

    fun <T : VersionlessPacket> inbound(
        packetType: Class<T>,
        priority: PacketPriority = PacketPriority.NORMAL,
        listener: (VersionlessPacketEvent<T>) -> Unit
    ) {
        handles += registry.onInbound(owner, packetType, priority, listener)
    }

    inline fun <reified T : VersionlessPacket> inbound(
        priority: PacketPriority = PacketPriority.NORMAL,
        noinline listener: (VersionlessPacketEvent<T>) -> Unit
    ) {
        inbound(T::class.java, priority, listener)
    }

    fun <T : VersionlessPacket> outbound(
        packetType: Class<T>,
        priority: PacketPriority = PacketPriority.NORMAL,
        listener: (VersionlessPacketEvent<T>) -> Unit
    ) {
        handles += registry.onOutbound(owner, packetType, priority, listener)
    }

    inline fun <reified T : VersionlessPacket> outbound(
        priority: PacketPriority = PacketPriority.NORMAL,
        noinline listener: (VersionlessPacketEvent<T>) -> Unit
    ) {
        outbound(T::class.java, priority, listener)
    }

    internal fun snapshot(): List<PacketInterceptorHandle> = handles.toList()
}

fun versionlessPackets(block: VersionlessPacketListenerDsl.() -> Unit): List<PacketInterceptorHandle> {
    val context = PluginRuntime.requireCurrentContext()
    val dsl = VersionlessPacketListenerDsl(context.packets, context.metadata.id)
    dsl.block()
    return dsl.snapshot()
}

fun sendOutbound(player: org.macaroon3145.api.entity.ConnectedPlayer, packet: VersionlessPacket): Boolean {
    val context = PluginRuntime.requireCurrentContext()
    return context.packets.sendOutbound(player, packet)
}

fun sendOutbound(connection: PacketConnection, packet: VersionlessPacket): Boolean {
    val context = PluginRuntime.requireCurrentContext()
    return context.packets.sendOutbound(connection, packet)
}

fun sendRawOutbound(player: org.macaroon3145.api.entity.ConnectedPlayer, packetId: Int, payload: ByteArray): Boolean {
    val context = PluginRuntime.requireCurrentContext()
    return context.packets.sendRawOutbound(player, packetId, payload)
}

fun sendRawOutbound(connection: PacketConnection, packetId: Int, payload: ByteArray): Boolean {
    val context = PluginRuntime.requireCurrentContext()
    return context.packets.sendRawOutbound(connection, packetId, payload)
}
