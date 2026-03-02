package org.macaroon3145.world

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap

object FoliaSidecarSpawnPointProvider {
    private val snapshotPath: Path = Path.of(".aerogel-cache/folia/runtime/ipc/world-spawns.tsv")
    @Volatile private var lastLoadedMtime: Long = Long.MIN_VALUE
    @Volatile private var cached: Map<String, SpawnPoint> = emptyMap()

    fun spawnPointFor(worldKey: String): SpawnPoint? {
        refreshIfNeeded()
        return cached[worldKey]
    }

    private fun refreshIfNeeded() {
        if (!Files.isRegularFile(snapshotPath)) {
            if (cached.isNotEmpty()) {
                cached = emptyMap()
                lastLoadedMtime = Long.MIN_VALUE
            }
            return
        }

        val mtime = runCatching { Files.getLastModifiedTime(snapshotPath).toMillis() }.getOrNull() ?: return
        if (mtime == lastLoadedMtime) return

        val currentMtime = runCatching { Files.getLastModifiedTime(snapshotPath).toMillis() }.getOrNull() ?: return
        if (currentMtime == lastLoadedMtime) return
        cached = loadSnapshot(snapshotPath)
        lastLoadedMtime = currentMtime
    }

    private fun loadSnapshot(path: Path): Map<String, SpawnPoint> {
        val out = ConcurrentHashMap<String, SpawnPoint>()
        val lines = runCatching { Files.readAllLines(path, StandardCharsets.UTF_8) }.getOrElse { return emptyMap() }
        for (line in lines) {
            if (line.isBlank()) continue
            val parts = line.split('\t')
            if (parts.size < 4) continue
            val worldKey = parts[0]
            val x = parts[1].toDoubleOrNull() ?: continue
            val y = parts[2].toDoubleOrNull() ?: continue
            val z = parts[3].toDoubleOrNull() ?: continue
            out[worldKey] = SpawnPoint(x, y, z)
        }
        return out
    }
}
