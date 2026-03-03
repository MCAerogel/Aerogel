package org.macaroon3145.api.world

import org.macaroon3145.api.type.BlockType

abstract class BlockState {
    abstract val id: Int
    abstract val key: String
    abstract val properties: Map<String, String>
}

abstract class Block {
    abstract val location: Location
    val chunk: Chunk
        get() = location.chunk
    abstract var type: BlockType
    abstract var state: BlockState
}
