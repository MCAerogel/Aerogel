package org.macaroon3145.world

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.macaroon3145.network.codec.BlockStateRegistry
import java.util.concurrent.ConcurrentHashMap

object BlockCollisionRegistry {
    data class CollisionBox(
        val minX: Double,
        val minY: Double,
        val minZ: Double,
        val maxX: Double,
        val maxY: Double,
        val maxZ: Double
    )

    private val json = Json { ignoreUnknownKeys = true }
    private val resolverByBlockKey: Map<String, String> by lazy {
        val root = loadRoot()
        val out = HashMap<String, String>()
        val obj = root["resolverByBlockKey"]?.jsonObject ?: return@lazy out
        for ((k, v) in obj) {
            out[k] = v.jsonPrimitive.content
        }
        out
    }
    private val resolverBySuffix: Map<String, String> by lazy {
        val root = loadRoot()
        val out = HashMap<String, String>()
        val obj = root["resolverBySuffix"]?.jsonObject ?: return@lazy out
        for ((k, v) in obj) {
            out[k] = v.jsonPrimitive.content
        }
        out
    }
    private val boxesByStateId = ConcurrentHashMap<Int, Array<CollisionBox>>()

    fun prewarm() {
        resolverByBlockKey.size
        resolverBySuffix.size
        for (stateId in BlockStateRegistry.allStateIds()) {
            if (stateId <= 0) continue
            val parsed = BlockStateRegistry.parsedState(stateId) ?: continue
            val resolved = resolveBoxes(parsed) ?: continue
            boxesByStateId[stateId] = resolved
        }
    }

    fun boxesForStateId(stateId: Int, parsed: BlockStateRegistry.ParsedState): Array<CollisionBox>? {
        if (stateId <= 0) return null
        val cached = boxesByStateId[stateId]
        if (cached != null) return cached
        val resolved = resolveBoxes(parsed) ?: return null
        boxesByStateId[stateId] = resolved
        return resolved
    }

    private fun resolveBoxes(parsed: BlockStateRegistry.ParsedState): Array<CollisionBox>? {
        val resolver = resolverFor(parsed.blockKey) ?: return null
        return when (resolver) {
            "fence_gate" -> resolveFenceGate(parsed)
            "fence" -> resolveFence(parsed)
            "wall" -> resolveWall(parsed)
            "pane" -> resolvePane(parsed)
            "slab" -> resolveSlab(parsed)
            "stairs" -> resolveStairs(parsed)
            "snow" -> resolveSnow(parsed)
            "carpet" -> resolveCarpet()
            "trapdoor" -> resolveTrapdoor(parsed)
            "door" -> resolveDoor(parsed)
            "pressure_plate" -> resolvePressurePlate(parsed)
            "button" -> resolveButton(parsed)
            "bed" -> resolveBed()
            "anvil" -> resolveAnvil(parsed)
            "cake" -> resolveCake(parsed)
            "partial_15_16" -> resolvePartial15of16()
            "enchanting_table" -> resolveEnchantingTable()
            "daylight_detector" -> resolveDaylightDetector()
            "hopper" -> resolveHopper()
            "brewing_stand" -> resolveBrewingStand()
            "cauldron" -> resolveCauldron()
            "chain" -> resolveChain(parsed)
            "end_rod" -> resolveEndRod(parsed)
            "lightning_rod" -> resolveLightningRod(parsed)
            "cocoa" -> resolveCocoa(parsed)
            "candle" -> resolveCandle(parsed)
            "amethyst_bud" -> resolveAmethystBud(parsed, parsed.blockKey)
            "amethyst_cluster" -> resolveAmethystBud(parsed, parsed.blockKey)
            else -> null
        }
    }

    private fun resolverFor(blockKey: String): String? {
        resolverByBlockKey[blockKey]?.let { return it }
        for ((suffix, resolver) in resolverBySuffix) {
            if (blockKey.endsWith(suffix)) return resolver
        }
        return null
    }

    private fun b(
        minX: Double,
        minY: Double,
        minZ: Double,
        maxX: Double,
        maxY: Double,
        maxZ: Double
    ): CollisionBox = CollisionBox(minX, minY, minZ, maxX, maxY, maxZ)

    private fun resolveFenceGate(parsed: BlockStateRegistry.ParsedState): Array<CollisionBox>? {
        val open = parsed.properties["open"]?.toBooleanStrictOrNull() ?: false
        if (open) return emptyArray()
        val facing = parsed.properties["facing"] ?: "north"
        return if (facing == "east" || facing == "west") {
            arrayOf(b(0.375, 0.0, 0.0, 0.625, 1.5, 1.0))
        } else {
            arrayOf(b(0.0, 0.0, 0.375, 1.0, 1.5, 0.625))
        }
    }

    private fun resolveFence(parsed: BlockStateRegistry.ParsedState): Array<CollisionBox> {
        val out = ArrayList<CollisionBox>(5)
        out.add(b(0.375, 0.0, 0.375, 0.625, 1.5, 0.625))
        if ((parsed.properties["north"]?.toBooleanStrictOrNull() ?: false)) out.add(b(0.4375, 0.0, 0.0, 0.5625, 1.5, 0.375))
        if ((parsed.properties["south"]?.toBooleanStrictOrNull() ?: false)) out.add(b(0.4375, 0.0, 0.625, 0.5625, 1.5, 1.0))
        if ((parsed.properties["west"]?.toBooleanStrictOrNull() ?: false)) out.add(b(0.0, 0.0, 0.4375, 0.375, 1.5, 0.5625))
        if ((parsed.properties["east"]?.toBooleanStrictOrNull() ?: false)) out.add(b(0.625, 0.0, 0.4375, 1.0, 1.5, 0.5625))
        return out.toTypedArray()
    }

    private fun resolveWall(parsed: BlockStateRegistry.ParsedState): Array<CollisionBox> {
        val out = ArrayList<CollisionBox>(5)
        val up = parsed.properties["up"]?.toBooleanStrictOrNull() ?: true
        if (up) out.add(b(0.25, 0.0, 0.25, 0.75, 1.0, 0.75))
        fun h(side: String): Double? = when (parsed.properties[side]) {
            "low" -> 0.875
            "tall" -> 1.5
            else -> null
        }
        h("north")?.let { out.add(b(0.3125, 0.0, 0.0, 0.6875, it, 0.5)) }
        h("south")?.let { out.add(b(0.3125, 0.0, 0.5, 0.6875, it, 1.0)) }
        h("west")?.let { out.add(b(0.0, 0.0, 0.3125, 0.5, it, 0.6875)) }
        h("east")?.let { out.add(b(0.5, 0.0, 0.3125, 1.0, it, 0.6875)) }
        return out.toTypedArray()
    }

    private fun resolvePane(parsed: BlockStateRegistry.ParsedState): Array<CollisionBox> {
        val out = ArrayList<CollisionBox>(5)
        val t = 0.0625
        val min = 0.5 - t
        val max = 0.5 + t
        out.add(b(min, 0.0, min, max, 1.0, max))
        if ((parsed.properties["north"]?.toBooleanStrictOrNull() ?: false)) out.add(b(min, 0.0, 0.0, max, 1.0, min))
        if ((parsed.properties["south"]?.toBooleanStrictOrNull() ?: false)) out.add(b(min, 0.0, max, max, 1.0, 1.0))
        if ((parsed.properties["west"]?.toBooleanStrictOrNull() ?: false)) out.add(b(0.0, 0.0, min, min, 1.0, max))
        if ((parsed.properties["east"]?.toBooleanStrictOrNull() ?: false)) out.add(b(max, 0.0, min, 1.0, 1.0, max))
        return out.toTypedArray()
    }

    private fun resolveSlab(parsed: BlockStateRegistry.ParsedState): Array<CollisionBox> {
        return when (parsed.properties["type"]) {
            "top" -> arrayOf(b(0.0, 0.5, 0.0, 1.0, 1.0, 1.0))
            "bottom" -> arrayOf(b(0.0, 0.0, 0.0, 1.0, 0.5, 1.0))
            else -> arrayOf(b(0.0, 0.0, 0.0, 1.0, 1.0, 1.0))
        }
    }

    private enum class HorizontalDir { NORTH, SOUTH, WEST, EAST }

    private fun horizontalDir(raw: String?): HorizontalDir = when (raw) {
        "south" -> HorizontalDir.SOUTH
        "west" -> HorizontalDir.WEST
        "east" -> HorizontalDir.EAST
        else -> HorizontalDir.NORTH
    }

    private fun resolveStairs(parsed: BlockStateRegistry.ParsedState): Array<CollisionBox> {
        val half = parsed.properties["half"] ?: "bottom"
        val shape = parsed.properties["shape"] ?: "straight"
        val facing = horizontalDir(parsed.properties["facing"])
        val out = ArrayList<CollisionBox>(3)

        val baseMinY = if (half == "top") 0.5 else 0.0
        val baseMaxY = baseMinY + 0.5
        out.add(b(0.0, baseMinY, 0.0, 1.0, baseMaxY, 1.0))

        val stepMinY = if (half == "top") 0.0 else 0.5
        val stepMaxY = stepMinY + 0.5
        val stepBoxes = stairStepBoxes(shape, facing, stepMinY, stepMaxY)
        out.addAll(stepBoxes)
        return out.toTypedArray()
    }

    private fun stairStepBoxes(
        shape: String,
        facing: HorizontalDir,
        minY: Double,
        maxY: Double
    ): List<CollisionBox> {
        fun halfBox(dir: HorizontalDir): CollisionBox = when (dir) {
            HorizontalDir.NORTH -> b(0.0, minY, 0.0, 1.0, maxY, 0.5)
            HorizontalDir.SOUTH -> b(0.0, minY, 0.5, 1.0, maxY, 1.0)
            HorizontalDir.WEST -> b(0.0, minY, 0.0, 0.5, maxY, 1.0)
            HorizontalDir.EAST -> b(0.5, minY, 0.0, 1.0, maxY, 1.0)
        }
        fun quarterBox(front: HorizontalDir, side: String): CollisionBox {
            return when (front) {
                HorizontalDir.NORTH -> if (side == "left") b(0.0, minY, 0.0, 0.5, maxY, 0.5) else b(0.5, minY, 0.0, 1.0, maxY, 0.5)
                HorizontalDir.SOUTH -> if (side == "left") b(0.5, minY, 0.5, 1.0, maxY, 1.0) else b(0.0, minY, 0.5, 0.5, maxY, 1.0)
                HorizontalDir.WEST -> if (side == "left") b(0.0, minY, 0.5, 0.5, maxY, 1.0) else b(0.0, minY, 0.0, 0.5, maxY, 0.5)
                HorizontalDir.EAST -> if (side == "left") b(0.5, minY, 0.0, 1.0, maxY, 0.5) else b(0.5, minY, 0.5, 1.0, maxY, 1.0)
            }
        }
        return when (shape) {
            "outer_left" -> listOf(quarterBox(facing, "left"))
            "outer_right" -> listOf(quarterBox(facing, "right"))
            "inner_left" -> listOf(halfBox(facing), quarterBox(oppositeOf(facing), "left"))
            "inner_right" -> listOf(halfBox(facing), quarterBox(oppositeOf(facing), "right"))
            else -> listOf(halfBox(facing))
        }
    }

    private fun oppositeOf(dir: HorizontalDir): HorizontalDir = when (dir) {
        HorizontalDir.NORTH -> HorizontalDir.SOUTH
        HorizontalDir.SOUTH -> HorizontalDir.NORTH
        HorizontalDir.WEST -> HorizontalDir.EAST
        HorizontalDir.EAST -> HorizontalDir.WEST
    }

    private fun resolveSnow(parsed: BlockStateRegistry.ParsedState): Array<CollisionBox> {
        val layers = parsed.properties["layers"]?.toIntOrNull()?.coerceIn(1, 8) ?: 1
        return arrayOf(b(0.0, 0.0, 0.0, 1.0, layers / 8.0, 1.0))
    }

    private fun resolveCarpet(): Array<CollisionBox> {
        return arrayOf(b(0.0, 0.0, 0.0, 1.0, 0.0625, 1.0))
    }

    private fun resolveTrapdoor(parsed: BlockStateRegistry.ParsedState): Array<CollisionBox> {
        val open = parsed.properties["open"]?.toBooleanStrictOrNull() ?: false
        val half = parsed.properties["half"] ?: "bottom"
        if (!open) {
            return if (half == "top") arrayOf(b(0.0, 0.8125, 0.0, 1.0, 1.0, 1.0))
            else arrayOf(b(0.0, 0.0, 0.0, 1.0, 0.1875, 1.0))
        }
        return when (parsed.properties["facing"] ?: "north") {
            "north" -> arrayOf(b(0.0, 0.0, 0.8125, 1.0, 1.0, 1.0))
            "south" -> arrayOf(b(0.0, 0.0, 0.0, 1.0, 1.0, 0.1875))
            "west" -> arrayOf(b(0.8125, 0.0, 0.0, 1.0, 1.0, 1.0))
            else -> arrayOf(b(0.0, 0.0, 0.0, 0.1875, 1.0, 1.0))
        }
    }

    private fun resolveDoor(parsed: BlockStateRegistry.ParsedState): Array<CollisionBox> {
        val open = parsed.properties["open"]?.toBooleanStrictOrNull() ?: false
        val facing = parsed.properties["facing"] ?: "north"
        val hingeRight = (parsed.properties["hinge"] ?: "left") == "right"
        val t = 0.1875
        if (!open) {
            return if (facing == "north" || facing == "south") arrayOf(b(0.0, 0.0, 0.0, 1.0, 1.0, t))
            else arrayOf(b(0.0, 0.0, 0.0, t, 1.0, 1.0))
        }
        val openFacing = when (facing) {
            "north" -> if (hingeRight) "west" else "east"
            "south" -> if (hingeRight) "east" else "west"
            "west" -> if (hingeRight) "south" else "north"
            "east" -> if (hingeRight) "north" else "south"
            else -> "north"
        }
        return if (openFacing == "north" || openFacing == "south") arrayOf(b(0.0, 0.0, 0.0, 1.0, 1.0, t))
        else arrayOf(b(0.0, 0.0, 0.0, t, 1.0, 1.0))
    }

    private fun resolvePressurePlate(parsed: BlockStateRegistry.ParsedState): Array<CollisionBox> {
        val powered = parsed.properties["powered"]?.toBooleanStrictOrNull() ?: false
        val h = if (powered) 0.03125 else 0.0625
        return arrayOf(b(0.0625, 0.0, 0.0625, 0.9375, h, 0.9375))
    }

    private fun resolveButton(parsed: BlockStateRegistry.ParsedState): Array<CollisionBox> {
        val powered = parsed.properties["powered"]?.toBooleanStrictOrNull() ?: false
        val d = if (powered) 0.0625 else 0.125
        val face = parsed.properties["face"] ?: "wall"
        val facing = parsed.properties["facing"] ?: "north"
        return when (face) {
            "floor" -> arrayOf(b(0.3125, 0.0, 0.3125, 0.6875, d, 0.6875))
            "ceiling" -> arrayOf(b(0.3125, 1.0 - d, 0.3125, 0.6875, 1.0, 0.6875))
            else -> when (facing) {
                "north" -> arrayOf(b(0.3125, 0.375, 0.0, 0.6875, 0.625, d))
                "south" -> arrayOf(b(0.3125, 0.375, 1.0 - d, 0.6875, 0.625, 1.0))
                "west" -> arrayOf(b(0.0, 0.375, 0.3125, d, 0.625, 0.6875))
                else -> arrayOf(b(1.0 - d, 0.375, 0.3125, 1.0, 0.625, 0.6875))
            }
        }
    }

    private fun resolveBed(): Array<CollisionBox> {
        return arrayOf(b(0.0, 0.0, 0.0, 1.0, 0.5625, 1.0))
    }

    private fun resolveAnvil(parsed: BlockStateRegistry.ParsedState): Array<CollisionBox> {
        val facing = parsed.properties["facing"] ?: "north"
        return if (facing == "east" || facing == "west") {
            arrayOf(b(0.0, 0.0, 0.125, 1.0, 1.0, 0.875))
        } else {
            arrayOf(b(0.125, 0.0, 0.0, 0.875, 1.0, 1.0))
        }
    }

    private fun resolveCake(parsed: BlockStateRegistry.ParsedState): Array<CollisionBox> {
        val bites = parsed.properties["bites"]?.toIntOrNull()?.coerceIn(0, 6) ?: 0
        val minX = (1 + (bites * 2)) / 16.0
        return arrayOf(b(minX, 0.0, 0.0625, 0.9375, 0.5, 0.9375))
    }

    private fun resolvePartial15of16(): Array<CollisionBox> {
        return arrayOf(b(0.0, 0.0, 0.0, 1.0, 0.9375, 1.0))
    }

    private fun resolveEnchantingTable(): Array<CollisionBox> {
        return arrayOf(b(0.0, 0.0, 0.0, 1.0, 0.75, 1.0))
    }

    private fun resolveDaylightDetector(): Array<CollisionBox> {
        return arrayOf(b(0.0, 0.0, 0.0, 1.0, 0.375, 1.0))
    }

    private fun resolveHopper(): Array<CollisionBox> {
        return arrayOf(
            b(0.0, 0.625, 0.0, 1.0, 1.0, 1.0),
            b(0.25, 0.0, 0.25, 0.75, 0.625, 0.75)
        )
    }

    private fun resolveBrewingStand(): Array<CollisionBox> {
        return arrayOf(
            b(0.0, 0.0, 0.0, 1.0, 0.125, 1.0),
            b(0.4375, 0.0, 0.4375, 0.5625, 0.875, 0.5625)
        )
    }

    private fun resolveCauldron(): Array<CollisionBox> {
        return arrayOf(
            b(0.0, 0.0, 0.0, 1.0, 0.3125, 1.0),
            b(0.0, 0.0, 0.0, 0.125, 1.0, 1.0),
            b(0.875, 0.0, 0.0, 1.0, 1.0, 1.0),
            b(0.125, 0.0, 0.0, 0.875, 1.0, 0.125),
            b(0.125, 0.0, 0.875, 0.875, 1.0, 1.0)
        )
    }

    private fun resolveChain(parsed: BlockStateRegistry.ParsedState): Array<CollisionBox> {
        return when (parsed.properties["axis"] ?: "y") {
            "x" -> arrayOf(b(0.0, 0.40625, 0.40625, 1.0, 0.59375, 0.59375))
            "z" -> arrayOf(b(0.40625, 0.40625, 0.0, 0.59375, 0.59375, 1.0))
            else -> arrayOf(b(0.40625, 0.0, 0.40625, 0.59375, 1.0, 0.59375))
        }
    }

    private fun resolveEndRod(parsed: BlockStateRegistry.ParsedState): Array<CollisionBox> {
        return when (parsed.properties["facing"] ?: "up") {
            "east", "west" -> arrayOf(b(0.0, 0.4375, 0.4375, 1.0, 0.5625, 0.5625))
            "north", "south" -> arrayOf(b(0.4375, 0.4375, 0.0, 0.5625, 0.5625, 1.0))
            else -> arrayOf(b(0.4375, 0.0, 0.4375, 0.5625, 1.0, 0.5625))
        }
    }

    private fun resolveLightningRod(parsed: BlockStateRegistry.ParsedState): Array<CollisionBox> {
        return when (parsed.properties["facing"] ?: "up") {
            "east", "west" -> arrayOf(b(0.0, 0.375, 0.375, 1.0, 0.625, 0.625))
            "north", "south" -> arrayOf(b(0.375, 0.375, 0.0, 0.625, 0.625, 1.0))
            else -> arrayOf(b(0.375, 0.0, 0.375, 0.625, 1.0, 0.625))
        }
    }

    private fun resolveCocoa(parsed: BlockStateRegistry.ParsedState): Array<CollisionBox> {
        val age = parsed.properties["age"]?.toIntOrNull()?.coerceIn(0, 2) ?: 0
        val size = when (age) {
            0 -> 0.375
            1 -> 0.5
            else -> 0.625
        }
        val minY = 0.375
        val maxY = 0.75
        val facing = parsed.properties["facing"] ?: "north"
        return when (facing) {
            "north" -> arrayOf(b(0.5 - size * 0.5, minY, 1.0 - size, 0.5 + size * 0.5, maxY, 1.0))
            "south" -> arrayOf(b(0.5 - size * 0.5, minY, 0.0, 0.5 + size * 0.5, maxY, size))
            "west" -> arrayOf(b(1.0 - size, minY, 0.5 - size * 0.5, 1.0, maxY, 0.5 + size * 0.5))
            else -> arrayOf(b(0.0, minY, 0.5 - size * 0.5, size, maxY, 0.5 + size * 0.5))
        }
    }

    private fun resolveCandle(parsed: BlockStateRegistry.ParsedState): Array<CollisionBox> {
        val count = parsed.properties["candles"]?.toIntOrNull()?.coerceIn(1, 4) ?: 1
        val r = when (count) {
            1 -> 0.125
            2 -> 0.1875
            3 -> 0.21875
            else -> 0.25
        }
        val h = when (count) {
            1 -> 0.375
            2 -> 0.4375
            else -> 0.5
        }
        return arrayOf(b(0.5 - r, 0.0, 0.5 - r, 0.5 + r, h, 0.5 + r))
    }

    private fun resolveAmethystBud(parsed: BlockStateRegistry.ParsedState, key: String): Array<CollisionBox> {
        val (radius, length) = when {
            key.endsWith("small_amethyst_bud") -> Pair(0.1875, 0.25)
            key.endsWith("medium_amethyst_bud") -> Pair(0.25, 0.3125)
            key.endsWith("large_amethyst_bud") -> Pair(0.3125, 0.4375)
            else -> Pair(0.375, 0.5)
        }
        val cMin = 0.5 - radius
        val cMax = 0.5 + radius
        return when (parsed.properties["facing"] ?: "up") {
            "down" -> arrayOf(b(cMin, 1.0 - length, cMin, cMax, 1.0, cMax))
            "north" -> arrayOf(b(cMin, cMin, 1.0 - length, cMax, cMax, 1.0))
            "south" -> arrayOf(b(cMin, cMin, 0.0, cMax, cMax, length))
            "west" -> arrayOf(b(1.0 - length, cMin, cMin, 1.0, cMax, cMax))
            "east" -> arrayOf(b(0.0, cMin, cMin, length, cMax, cMax))
            else -> arrayOf(b(cMin, 0.0, cMin, cMax, length, cMax))
        }
    }

    private fun loadRoot() = BlockCollisionRegistry::class.java.classLoader
        .getResourceAsStream("block-collision-aabb-1.21.11.json")
        ?.bufferedReader()
        ?.use { json.parseToJsonElement(it.readText()) }
        ?.jsonObject
        ?: error("Missing block-collision-aabb-1.21.11.json resource")
}
