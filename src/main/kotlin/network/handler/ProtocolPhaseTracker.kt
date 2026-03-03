package org.macaroon3145.network.handler

import io.netty.channel.Channel
import io.netty.util.AttributeKey
import org.macaroon3145.api.packet.ProtocolPhase

object ProtocolPhaseTracker {
    private val KEY: AttributeKey<ProtocolPhase> = AttributeKey.valueOf("aerogel:protocol_phase")

    fun current(channel: Channel): ProtocolPhase {
        return channel.attr(KEY).get() ?: ProtocolPhase.HANDSHAKE
    }

    fun update(channel: Channel, phase: ProtocolPhase) {
        channel.attr(KEY).set(phase)
    }
}
