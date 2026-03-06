package org.macaroon3145.world

import org.macaroon3145.network.codec.BlockStateRegistry
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class VanillaWaterSubmersionTest {
    @Test
    fun waterLevelBoundaryMatchesExpectedSurfaceHeight() {
        val flowingLevel7State = findStateId { parsed ->
            parsed.blockKey == "minecraft:water" && parsed.properties["level"] == "7"
        }
        val expectedHeight = 1.0 / 9.0
        assertEquals(expectedHeight, VanillaWaterSubmersion.waterSurfaceHeightByStateId(flowingLevel7State), 1.0e-9)

        val fluidBaseY = 100
        val surfaceY = fluidBaseY + expectedHeight
        assertTrue(
            VanillaWaterSubmersion.isPointSubmergedInWaterByState(
                stateId = flowingLevel7State,
                fluidBlockY = fluidBaseY,
                sampleY = surfaceY - 1.0e-6
            )
        )
        assertFalse(
            VanillaWaterSubmersion.isPointSubmergedInWaterByState(
                stateId = flowingLevel7State,
                fluidBlockY = fluidBaseY,
                sampleY = surfaceY + 1.0e-4
            )
        )
    }

    @Test
    fun bubbleColumnUsesFullBlockWaterHeight() {
        val bubbleColumnState = findStateId { parsed ->
            parsed.blockKey == "minecraft:bubble_column"
        }
        assertEquals(1.0, VanillaWaterSubmersion.waterSurfaceHeightByStateId(bubbleColumnState), 1.0e-9)
    }

    @Test
    fun waterloggedUsesFullBlockWaterHeight() {
        val waterloggedState = findStateId { parsed ->
            parsed.properties["waterlogged"] == "true"
        }
        assertEquals(1.0, VanillaWaterSubmersion.waterSurfaceHeightByStateId(waterloggedState), 1.0e-9)
    }

    private fun findStateId(predicate: (BlockStateRegistry.ParsedState) -> Boolean): Int {
        for (stateId in BlockStateRegistry.allStateIds()) {
            if (stateId <= 0) continue
            val parsed = BlockStateRegistry.parsedState(stateId) ?: continue
            if (predicate(parsed)) return stateId
        }
        error("No matching block state found for test predicate")
    }
}
