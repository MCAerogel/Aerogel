package org.macaroon3145.world

import java.util.concurrent.CompletableFuture

fun interface WorldGenerator {
    fun generateChunk(context: ChunkGenerationContext): GeneratedChunk
}

interface AsyncWorldGenerator : WorldGenerator {
    fun generateChunkAsync(context: ChunkGenerationContext): CompletableFuture<GeneratedChunk>
}

interface WarmupWorldGenerator {
    fun warmup(seed: Long)
}

interface BlockStateLookupWorldGenerator {
    fun blockStateAt(worldKey: String, x: Int, y: Int, z: Int): Int

    fun spawnPoint(worldKey: String): SpawnPoint? = null

    fun blockStateAtIfCached(worldKey: String, x: Int, y: Int, z: Int): Int? = null

    fun rawBrightnessAt(worldKey: String, x: Int, y: Int, z: Int): Int = 0

    fun rawBrightnessAtIfCached(worldKey: String, x: Int, y: Int, z: Int): Int? = null
}

interface LoadedChunkCacheWorldGenerator {
    fun retainLoadedChunk(chunkPos: ChunkPos)

    fun releaseLoadedChunk(chunkPos: ChunkPos)
}

fun interface ChunkEntityProcessor {
    fun process(context: ChunkGenerationContext, chunk: GeneratedChunk): GeneratedChunk
}
