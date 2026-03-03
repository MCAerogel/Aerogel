package org.macaroon3145.api.entity

import java.util.UUID

interface PlayerRegistry {
    fun player(uuid: UUID): Player
    fun player(name: String): Player?
}
