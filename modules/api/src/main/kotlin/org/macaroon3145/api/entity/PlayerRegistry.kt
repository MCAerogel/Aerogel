package org.macaroon3145.api.entity

import java.util.UUID

interface PlayerRegistry {
    fun player(uuid: UUID): ConnectedPlayer?
    fun player(name: String): ConnectedPlayer?
    fun players(): List<ConnectedPlayer>
}
