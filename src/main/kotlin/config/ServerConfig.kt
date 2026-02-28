package org.macaroon3145.config

object ServerConfig {
    @Volatile
    var onlineMode: Boolean = false

    // -1 means uncapped.
    @Volatile
    var maxTps: Double = 20.0

    // Global simulation time scale.
    // 1.0 = normal speed, <1.0 = slower, >1.0 = faster.
    @Volatile
    var timeScale: Double = 1.0

    // Client-side perceived play speed scale via ticking_state packet.
    // 1.0 = normal speed, lower values are slower.
    @Volatile
    var playerTimeScale: Double = 1.0

    // Hard cap for per-player view distance in chunk radius.
    // Keep this conservative for custom worldgen to avoid client-side meshing stalls.
    @Volatile
    var maxViewDistanceChunks: Int = 10

    // Hard cap for per-player simulation distance in chunk radius.
    // Must not exceed view distance.
    @Volatile
    var maxSimulationDistanceChunks: Int = 8

    // 0 means auto sizing based on CPU cores.
    @Volatile
    var chunkWorkerThreads: Int = 0

    // Compression threshold for play packets.
    // -1 disables compression.
    @Volatile
    var compressionThreshold: Int = 1024

    // zlib compression level [0..9]
    // 0 = no extra compression work, 1 = fastest, 9 = best ratio.
    @Volatile
    var compressionLevel: Int = 1

    // Dedicated compression level for large chunk packets.
    // -1 disables chunk packet compression (largest bandwidth, lowest CPU).
    // Keep low to avoid chunk load latency spikes.
    @Volatile
    var compressionChunkLevel: Int = 1

    // 0=survival, 1=creative, 2=adventure, 3=spectator
    @Volatile
    var defaultGameMode: Int = 1
        private set

    fun setGameMode(gameMode: Int) {
        defaultGameMode = gameMode.coerceIn(0, 3)
    }
}
