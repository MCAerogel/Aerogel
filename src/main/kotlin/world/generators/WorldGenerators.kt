package org.macaroon3145.world.generators

import org.macaroon3145.world.GeneratedChunk
import org.macaroon3145.world.WorldGenerator
import java.util.concurrent.ConcurrentHashMap

object WorldGenerators {
    val emptyBase: WorldGenerator = WorldGenerator { GeneratedChunk() }
    val foliaSharedMemory: WorldGenerator = FoliaSharedMemoryWorldGenerator
    private val chunkFutureWrapped = ConcurrentHashMap<String, WorldGenerator>()

    fun forWorldKey(worldKey: String, seed: Long): WorldGenerator {
        val cacheKey = "$worldKey#${seed.toULong().toString(16)}"
        return chunkFutureWrapped.computeIfAbsent(cacheKey) {
            ChunkFutureConnectedWorldGenerator(
                worldKey = worldKey,
                seed = seed
            )
        }
    }
}
