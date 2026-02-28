package org.macaroon3145.world.generators

import org.macaroon3145.world.GeneratedChunk
import org.macaroon3145.world.WorldGenerator

object WorldGenerators {
    val emptyBase: WorldGenerator = WorldGenerator { GeneratedChunk() }
    val foliaSharedMemory: WorldGenerator = FoliaSharedMemoryWorldGenerator

    fun forWorldKey(@Suppress("UNUSED_PARAMETER") worldKey: String): WorldGenerator {
        return foliaSharedMemory
    }
}
