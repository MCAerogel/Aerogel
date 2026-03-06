package org.macaroon3145.world

import org.macaroon3145.network.codec.BlockStateRegistry
import java.util.concurrent.ConcurrentHashMap

object VanillaWaterSubmersion {
    private const val NO_WATER_SURFACE_HEIGHT = -1.0
    private const val SUBMERGED_EPSILON = 1.0e-7
    private val waterSurfaceHeightByStateCache = ConcurrentHashMap<Int, Double>()

    fun waterSurfaceHeightByStateId(stateId: Int): Double {
        if (stateId <= 0) return NO_WATER_SURFACE_HEIGHT
        return waterSurfaceHeightByStateCache.computeIfAbsent(stateId) { id ->
            val parsed = BlockStateRegistry.parsedState(id) ?: return@computeIfAbsent NO_WATER_SURFACE_HEIGHT
            when {
                parsed.blockKey == "minecraft:bubble_column" -> 1.0
                parsed.properties["waterlogged"] == "true" -> 1.0
                parsed.blockKey == "minecraft:water" -> {
                    val rawLevel = parsed.properties["level"]?.toIntOrNull() ?: 0
                    val normalizedLevel = if (rawLevel > 7) 0 else rawLevel.coerceIn(0, 7)
                    (8 - normalizedLevel) / 9.0
                }
                else -> NO_WATER_SURFACE_HEIGHT
            }
        }
    }

    fun isPointSubmergedInWaterByState(stateId: Int, fluidBlockY: Int, sampleY: Double): Boolean {
        val surfaceHeight = waterSurfaceHeightByStateId(stateId)
        if (surfaceHeight <= 0.0) return false
        val fluidSurfaceY = fluidBlockY + surfaceHeight
        return sampleY < fluidSurfaceY + SUBMERGED_EPSILON
    }

    fun prewarm() {
        // Shift lazy parse/cache path to startup when needed.
        BlockStateRegistry.allStateIds().forEach { stateId ->
            waterSurfaceHeightByStateId(stateId)
        }
    }
}
