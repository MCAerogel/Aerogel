package org.macaroon3145.perf

import org.macaroon3145.config.ServerConfig
import org.macaroon3145.network.handler.ChunkStreamingService
import org.macaroon3145.world.ChunkPos
import org.macaroon3145.world.WorldManager
import org.macaroon3145.world.storage.VanillaLevelDatSeedStore
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.util.Properties
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import kotlin.math.max
import kotlin.random.Random

object TpPracticalBenchmark {
    @JvmStatic
    fun main() {
        val props = loadProperties()
        applyServerConfig(props)

        val worldSeeds = resolveWorldSeeds(props)
        val defaultWorld = propString(props, "world.default", "minecraft:overworld")
        WorldManager.bootstrap(worldSeeds = worldSeeds, defaultWorld = defaultWorld)
        val world = WorldManager.defaultWorld()

        val teleports = propInt(props, "tpbench.teleports", 80).coerceAtLeast(1)
        val radius = propInt(props, "tpbench.radius", ServerConfig.maxViewDistanceChunks).coerceAtLeast(2)
        val jumpChunks = propInt(props, "tpbench.jump-chunks", 48).coerceAtLeast(1)
        val intervalMillis = propLong(props, "tpbench.interval-ms", 120L).coerceAtLeast(0L)
        val maxInFlight = propInt(props, "tpbench.max-inflight", max(256, radius * radius * 4)).coerceAtLeast(32)
        val maxSubmitPerTeleport = propInt(props, "tpbench.max-submit-per-teleport", radius * radius * 2).coerceAtLeast(1)
        val drainMillis = propLong(props, "tpbench.drain-ms", 20_000L).coerceAtLeast(0L)
        val windowChunks = propLong(props, "tpbench.window-chunks", 100L).coerceAtLeast(10L)
        val pathSeed = propLong(props, "tpbench.path-seed", 6969000000015129972L)

        println(
            "[tpbench] config world=${world.key} teleports=$teleports radius=$radius jumpChunks=$jumpChunks " +
                "intervalMs=$intervalMillis maxInFlight=$maxInFlight maxSubmitPerTeleport=$maxSubmitPerTeleport " +
                "windowChunks=$windowChunks pathSeed=$pathSeed"
        )

        val rng = Random(pathSeed)
        var centerX = 0
        var centerZ = 0

        val currentViewRef = AtomicReference<Set<ChunkPos>>(emptySet())
        val loadedInView = ConcurrentHashMap.newKeySet<ChunkPos>()
        val inFlight = ConcurrentHashMap<ChunkPos, CompletableFuture<*>>()
        val submitExecutor = Executors.newVirtualThreadPerTaskExecutor()

        val totalCompleted = AtomicLong(0L)
        val usefulCompleted = AtomicLong(0L)
        val staleCompleted = AtomicLong(0L)
        val failedCompleted = AtomicLong(0L)
        val skippedAlreadyLoaded = AtomicLong(0L)
        val skippedAlreadyInflight = AtomicLong(0L)

        val windowLock = Any()
        var windowStartNanos = System.nanoTime()
        var windowCounter = 0L

        fun recordUsefulChunk() {
            synchronized(windowLock) {
                windowCounter++
                if (windowCounter < windowChunks) return
                val now = System.nanoTime()
                val elapsedNanos = (now - windowStartNanos).coerceAtLeast(1L)
                val avgPer100 = (elapsedNanos.toDouble() / windowCounter.toDouble()) * 100.0 / 1_000_000_000.0
                println(
                    "[tpbench] avg_per_100_chunks=${"%.3f".format(avgPer100)}s " +
                        "(window=$windowCounter useful, inFlight=${inFlight.size}, loadedInView=${loadedInView.size})"
                )
                windowStartNanos = now
                windowCounter = 0L
            }
        }

        fun submitIfNeeded(chunkPos: ChunkPos) {
            if (loadedInView.contains(chunkPos)) {
                skippedAlreadyLoaded.incrementAndGet()
                return
            }
            inFlight.compute(chunkPos) { key, existing ->
                if (existing != null) {
                    skippedAlreadyInflight.incrementAndGet()
                    return@compute existing
                }
                val future = CompletableFuture.supplyAsync({ world.buildChunk(key) }, submitExecutor)
                future.whenComplete { _, error ->
                    inFlight.remove(key, future)
                    totalCompleted.incrementAndGet()
                    if (error != null) {
                        failedCompleted.incrementAndGet()
                        return@whenComplete
                    }
                    val neededNow = currentViewRef.get().contains(key)
                    if (neededNow) {
                        loadedInView.add(key)
                        usefulCompleted.incrementAndGet()
                        recordUsefulChunk()
                    } else {
                        staleCompleted.incrementAndGet()
                    }
                }
                future
            }
        }

        try {
            val benchStartNanos = System.nanoTime()
            repeat(teleports) { step ->
                if (step > 0) {
                    centerX += rng.nextInt(-jumpChunks, jumpChunks + 1)
                    centerZ += rng.nextInt(-jumpChunks, jumpChunks + 1)
                }
                val targetCoords = ChunkStreamingService.buildSquareCoords(centerX, centerZ, radius)
                val targetSet = HashSet<ChunkPos>(targetCoords.size)
                targetSet.addAll(targetCoords)
                currentViewRef.set(targetSet)
                loadedInView.retainAll(targetSet)

                var submitted = 0
                for (chunkPos in targetCoords) {
                    if (inFlight.size >= maxInFlight) break
                    if (submitted >= maxSubmitPerTeleport) break
                    val beforeSize = inFlight.size
                    submitIfNeeded(chunkPos)
                    if (inFlight.size > beforeSize) {
                        submitted++
                    }
                }

                println(
                    "[tpbench] tp_step=${step + 1}/$teleports center=($centerX,$centerZ) " +
                        "target=${targetCoords.size} submitted=$submitted inFlight=${inFlight.size} " +
                        "useful=${usefulCompleted.get()} stale=${staleCompleted.get()} failed=${failedCompleted.get()}"
                )

                if (intervalMillis > 0L) {
                    Thread.sleep(intervalMillis)
                }
            }

            val drainDeadlineNanos = System.nanoTime() + drainMillis * 1_000_000L
            while (inFlight.isNotEmpty() && System.nanoTime() < drainDeadlineNanos) {
                Thread.sleep(20L)
            }

            val elapsedNanos = (System.nanoTime() - benchStartNanos).coerceAtLeast(1L)
            val elapsedSec = elapsedNanos.toDouble() / 1_000_000_000.0
            val useful = usefulCompleted.get()
            val total = totalCompleted.get()
            val stale = staleCompleted.get()
            val failed = failedCompleted.get()
            val cps = if (elapsedSec > 0.0) useful.toDouble() / elapsedSec else 0.0
            val avgPer100 = if (useful > 0) elapsedSec / useful.toDouble() * 100.0 else 0.0

            println(
                "[tpbench] summary elapsed=${"%.3f".format(elapsedSec)}s useful=$useful total=$total stale=$stale " +
                    "failed=$failed chunksPerSec=${"%.2f".format(cps)} avgPer100=${"%.3f".format(avgPer100)}s " +
                    "remainingInFlight=${inFlight.size} skippedLoaded=${skippedAlreadyLoaded.get()} " +
                    "skippedInflight=${skippedAlreadyInflight.get()}"
            )
        } finally {
            submitExecutor.shutdownNow()
        }
    }

    private fun loadProperties(): Properties {
        val props = Properties()
        val externalConfigPath = Path.of("aerogel.properties")
        if (Files.exists(externalConfigPath)) {
            Files.newBufferedReader(externalConfigPath, StandardCharsets.UTF_8).use { reader ->
                props.load(reader)
            }
        }
        return props
    }

    private fun applyServerConfig(props: Properties) {
        ServerConfig.maxViewDistanceChunks = propInt(props, "max-view-distance-chunks", 32).coerceAtLeast(2)
        ServerConfig.maxSimulationDistanceChunks = propInt(props, "max-simulation-distance-chunks", 16).coerceAtLeast(2)
        ServerConfig.chunkWorkerThreads = propInt(props, "chunk-worker-threads", 0).coerceAtLeast(0)
    }

    private fun resolveWorldSeeds(props: Properties): LinkedHashMap<String, Long> {
        val worldSeeds = linkedMapOf<String, Long>()

        val persisted = VanillaLevelDatSeedStore.load()
        for ((worldKey, seed) in persisted) {
            worldSeeds[worldKey] = seed
        }

        val legacySeed = props.getProperty("world.seed")?.toLongOrNull()
        if (legacySeed != null && "minecraft:overworld" !in worldSeeds) {
            worldSeeds["minecraft:overworld"] = legacySeed
        }

        for ((rawKey, rawValue) in props) {
            val key = rawKey.toString()
            if (!key.startsWith("world.seed.")) continue
            val worldKey = key.removePrefix("world.seed.")
            val value = rawValue.toString().trim()
            val direct = value.toLongOrNull()
            if (direct != null) {
                worldSeeds.putIfAbsent(worldKey, direct)
                continue
            }
            val split = value.indexOf('=')
            if (split <= 0 || split >= value.lastIndex) continue
            val suffix = value.substring(0, split).trim()
            val recovered = value.substring(split + 1).trim().toLongOrNull() ?: continue
            worldSeeds.putIfAbsent("$worldKey:$suffix", recovered)
        }

        val baseSeed = worldSeeds["minecraft:overworld"] ?: worldSeeds.values.firstOrNull() ?: Random.nextLong()
        worldSeeds.putIfAbsent("minecraft:overworld", baseSeed)
        worldSeeds.putIfAbsent("minecraft:the_nether", baseSeed)
        worldSeeds.putIfAbsent("minecraft:the_end", baseSeed)
        return worldSeeds
    }

    private fun propString(props: Properties, key: String, defaultValue: String): String {
        return System.getProperty(key)
            ?.takeIf { it.isNotBlank() }
            ?: props.getProperty(key)?.takeIf { it.isNotBlank() }
            ?: defaultValue
    }

    private fun propInt(props: Properties, key: String, defaultValue: Int): Int {
        return System.getProperty(key)?.toIntOrNull()
            ?: props.getProperty(key)?.toIntOrNull()
            ?: defaultValue
    }

    private fun propLong(props: Properties, key: String, defaultValue: Long): Long {
        return System.getProperty(key)?.toLongOrNull()
            ?: props.getProperty(key)?.toLongOrNull()
            ?: defaultValue
    }
}
