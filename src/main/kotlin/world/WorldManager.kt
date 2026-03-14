package org.macaroon3145.world

import org.macaroon3145.world.generators.WorldGenerators
import java.util.concurrent.ConcurrentHashMap
import kotlin.random.Random

object WorldManager {
    private val worlds = ConcurrentHashMap<String, World>()
    @Volatile
    private var defaultWorldKey: String = "minecraft:overworld"

    private val builtinWorldKeys = listOf(
        "minecraft:overworld",
        "minecraft:the_nether",
        "minecraft:the_end"
    )
    private const val AIR_STATE_ID = 0

    fun bootstrap(
        worldSeeds: Map<String, Long> = emptyMap(),
        defaultWorld: String? = null
    ) {
        worlds.clear()
        for (worldKey in builtinWorldKeys) {
            val seed = worldSeeds[worldKey] ?: Random.nextLong()
            val generator = WorldGenerators.forWorldKey(worldKey, seed)
            val lookupGenerator = generator as? BlockStateLookupWorldGenerator
            val baseProvider: (Int, Int, Int) -> Int = if (lookupGenerator == null) {
                { _, _, _ -> AIR_STATE_ID }
            } else {
                { x, y, z -> lookupGenerator.blockStateAt(worldKey, x, y, z) }
            }
            val cachedProvider: (Int, Int, Int) -> Int? = if (lookupGenerator == null) {
                { _, _, _ -> null }
            } else {
                { x, y, z -> lookupGenerator.blockStateAtIfCached(worldKey, x, y, z) }
            }
            val rawBrightnessProvider: (Int, Int, Int) -> Int = if (lookupGenerator == null) {
                { _, _, _ -> 0 }
            } else {
                { x, y, z -> lookupGenerator.rawBrightnessAt(worldKey, x, y, z) }
            }
            val cachedRawBrightnessProvider: (Int, Int, Int) -> Int? = if (lookupGenerator == null) {
                { _, _, _ -> null }
            } else {
                { x, y, z -> lookupGenerator.rawBrightnessAtIfCached(worldKey, x, y, z) }
            }
            registerWorld(
                key = worldKey,
                seed = seed,
                generator = generator,
                baseBlockStateProvider = baseProvider,
                cachedBlockStateProvider = cachedProvider,
                rawBrightnessProvider = rawBrightnessProvider,
                cachedRawBrightnessProvider = cachedRawBrightnessProvider
            )
        }
        defaultWorldKey = defaultWorld?.takeIf { worlds.containsKey(it) } ?: "minecraft:overworld"
        prewarmWorldsAsync()
    }

    private fun prewarmWorldsAsync() {
        for (world in worlds.values) {
            val warmup = world.generator as? WarmupWorldGenerator ?: continue
            val seed = world.seed
            Thread(
                {
                    runCatching { warmup.warmup(seed) }
                },
                "aerogel-world-prewarm-${world.key.substringAfterLast(':')}"
            ).apply {
                isDaemon = true
                start()
            }
        }
    }

    fun registerWorld(
        key: String,
        seed: Long,
        generator: WorldGenerator,
        baseBlockStateProvider: (Int, Int, Int) -> Int = { _, _, _ -> AIR_STATE_ID },
        cachedBlockStateProvider: (Int, Int, Int) -> Int? = { _, _, _ -> null },
        rawBrightnessProvider: (Int, Int, Int) -> Int = { _, _, _ -> 0 },
        cachedRawBrightnessProvider: (Int, Int, Int) -> Int? = { _, _, _ -> null }
    ): World {
        val world = World(
            key = key,
            seed = seed,
            generator = generator,
            baseBlockStateProvider = baseBlockStateProvider,
            cachedBlockStateProvider = cachedBlockStateProvider,
            rawBrightnessProvider = rawBrightnessProvider,
            cachedRawBrightnessProvider = cachedRawBrightnessProvider
        )
        worlds[key] = world
        return world
    }

    fun setDefaultWorld(key: String) {
        require(worlds.containsKey(key)) { "World not registered: $key" }
        defaultWorldKey = key
    }

    fun defaultWorld(): World {
        return worlds[defaultWorldKey] ?: error("Default world not registered: $defaultWorldKey")
    }

    fun allWorlds(): List<World> = worlds.values.toList()

    fun world(key: String): World? = worlds[key]
}
