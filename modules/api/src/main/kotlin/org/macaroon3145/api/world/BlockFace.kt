package org.macaroon3145.api.world

enum class BlockFace(
    val id: Int
) {
    DOWN(0),
    UP(1),
    NORTH(2),
    SOUTH(3),
    WEST(4),
    EAST(5);

    companion object {
        fun fromId(id: Int): BlockFace {
            return entries.firstOrNull { it.id == id } ?: UP
        }
    }
}
