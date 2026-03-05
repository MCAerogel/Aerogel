package org.macaroon3145.api.world

import org.macaroon3145.api.type.BlockType

abstract class BlockState {
    abstract val id: Int
    abstract val key: String
    abstract val properties: Map<String, String>
}

abstract class Block {
    abstract val location: Location
    val center: Location
        get() = resolvePairedCenterLocation() ?: blockCenterLocation(location.blockX, location.blockY, location.blockZ)
    val chunk: Chunk
        get() = location.chunk
    abstract var type: BlockType
    abstract var state: BlockState

    private fun resolvePairedCenterLocation(): Location? {
        val properties = state.properties
        return resolveBedCenterLocation(properties)
            ?: resolveDoubleChestCenterLocation(properties)
            ?: resolveVerticalPairCenterLocation(properties)
    }

    private fun resolveBedCenterLocation(properties: Map<String, String>): Location? {
        if (!type.key.endsWith("_bed")) return null
        val part = properties["part"] ?: return null
        val facing = properties["facing"] ?: return null
        val (dx, dz) = horizontalFacingOffset(facing) ?: return null
        val (targetX, targetZ, expectedPart) = when (part) {
            "foot" -> Triple(location.blockX + dx, location.blockZ + dz, "head")
            "head" -> Triple(location.blockX - dx, location.blockZ - dz, "foot")
            else -> return null
        }
        val paired = blockAtOrNull(targetX, location.blockY, targetZ) ?: return null
        val pairedProperties = paired.state.properties
        if (paired.type.key != type.key) return null
        if (pairedProperties["part"] != expectedPart) return null
        if (pairedProperties["facing"] != facing) return null
        return midpointCenterLocation(location.blockX, location.blockY, location.blockZ, targetX, location.blockY, targetZ)
    }

    private fun resolveDoubleChestCenterLocation(properties: Map<String, String>): Location? {
        val chestType = properties["type"] ?: return null
        if (chestType != "left" && chestType != "right") return null
        val facing = properties["facing"] ?: return null
        val (forwardX, forwardZ) = horizontalFacingOffset(facing) ?: return null
        val leftX = forwardZ
        val leftZ = -forwardX
        val rightX = -forwardZ
        val rightZ = forwardX
        val (targetX, targetZ, expectedType) = when (chestType) {
            "left" -> Triple(location.blockX + rightX, location.blockZ + rightZ, "right")
            "right" -> Triple(location.blockX + leftX, location.blockZ + leftZ, "left")
            else -> return null
        }
        val paired = blockAtOrNull(targetX, location.blockY, targetZ) ?: return null
        val pairedProperties = paired.state.properties
        if (paired.type.key != type.key) return null
        if (pairedProperties["type"] != expectedType) return null
        if (pairedProperties["facing"] != facing) return null
        return midpointCenterLocation(location.blockX, location.blockY, location.blockZ, targetX, location.blockY, targetZ)
    }

    private fun resolveVerticalPairCenterLocation(properties: Map<String, String>): Location? {
        val pairKind = when {
            properties["half"] == "lower" || properties["half"] == "upper" -> PairKind("half", "lower", "upper")
            properties["type"] == "bottom" || properties["type"] == "top" -> PairKind("type", "bottom", "top")
            else -> return null
        }

        val selfPart = properties[pairKind.property] ?: return null
        val (lowerY, upperY, expectedUpperPart) = when (selfPart) {
            pairKind.lowerValue -> Triple(location.blockY, location.blockY + 1, pairKind.upperValue)
            pairKind.upperValue -> Triple(location.blockY - 1, location.blockY, pairKind.upperValue)
            else -> return null
        }
        val lowerBlock = blockAtOrNull(location.blockX, lowerY, location.blockZ) ?: return null
        val upperBlock = blockAtOrNull(location.blockX, upperY, location.blockZ) ?: return null
        val lowerProperties = lowerBlock.state.properties
        val upperProperties = upperBlock.state.properties
        if (lowerBlock.type.key != type.key || upperBlock.type.key != type.key) return null
        if (lowerProperties[pairKind.property] != pairKind.lowerValue) return null
        if (upperProperties[pairKind.property] != expectedUpperPart) return null
        return midpointCenterLocation(location.blockX, lowerY, location.blockZ, location.blockX, upperY, location.blockZ)
    }

    private fun blockCenterLocation(x: Int, y: Int, z: Int): Location {
        return Location(
            world = location.world,
            x = x + 0.5,
            y = y + 0.5,
            z = z + 0.5,
            yaw = location.yaw,
            pitch = location.pitch
        )
    }

    private fun midpointCenterLocation(
        x1: Int,
        y1: Int,
        z1: Int,
        x2: Int,
        y2: Int,
        z2: Int
    ): Location {
        return Location(
            world = location.world,
            x = ((x1 + x2) / 2.0) + 0.5,
            y = ((y1 + y2) / 2.0) + 0.5,
            z = ((z1 + z2) / 2.0) + 0.5,
            yaw = location.yaw,
            pitch = location.pitch
        )
    }

    private fun blockAtOrNull(x: Int, y: Int, z: Int): Block? {
        return runCatching { location.world.blockAt(x, y, z) }.getOrNull()
    }

    private fun horizontalFacingOffset(facing: String): Pair<Int, Int>? {
        return when (facing) {
            "north" -> 0 to -1
            "south" -> 0 to 1
            "east" -> 1 to 0
            "west" -> -1 to 0
            else -> null
        }
    }

    private data class PairKind(
        val property: String,
        val lowerValue: String,
        val upperValue: String
    )
}
