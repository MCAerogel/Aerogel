package org.macaroon3145.api.entity

enum class GameMode(
    val id: Int
) {
    SURVIVAL(0),
    CREATIVE(1),
    ADVENTURE(2),
    SPECTATOR(3);

    companion object {
        fun fromId(id: Int): GameMode {
            return entries.firstOrNull { it.id == id } ?: SURVIVAL
        }
    }
}
