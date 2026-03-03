package org.macaroon3145.api.entity

enum class Hand(
    val id: Int
) {
    MAIN_HAND(0),
    OFF_HAND(1);

    companion object {
        fun fromId(id: Int): Hand {
            return if (id == OFF_HAND.id) OFF_HAND else MAIN_HAND
        }
    }
}
