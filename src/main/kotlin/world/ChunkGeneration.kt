package org.macaroon3145.world

data class ChunkPos(val x: Int, val z: Int)

data class BlockPos(val x: Int, val y: Int, val z: Int) {
    fun chunkPos(): ChunkPos = ChunkPos(x shr 4, z shr 4)
}

data class ChunkGenerationContext(
    val worldKey: String,
    val seed: Long,
    val chunkPos: ChunkPos,
    val isCancelled: () -> Boolean = { false }
)

data class HeightmapData(
    val typeId: Int,
    val values: LongArray
)

data class GeneratedChunk(
    val heightmaps: List<HeightmapData> = emptyList(),
    val chunkData: ByteArray = ByteArray(0),
    val skyLightMask: LongArray = LongArray(0),
    val blockLightMask: LongArray = LongArray(0),
    val emptySkyLightMask: LongArray = LongArray(0),
    val emptyBlockLightMask: LongArray = LongArray(0),
    val skyLight: List<ByteArray> = emptyList(),
    val blockLight: List<ByteArray> = emptyList()
)
