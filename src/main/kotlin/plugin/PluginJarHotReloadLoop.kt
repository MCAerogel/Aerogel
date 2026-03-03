package org.macaroon3145.plugin

import org.slf4j.LoggerFactory
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

internal class PluginJarHotReloadLoop(
    private val onJarChanged: (Path) -> Boolean
) {
    private val logger = LoggerFactory.getLogger(PluginJarHotReloadLoop::class.java)
    private val running = AtomicBoolean(false)
    private val snapshotByPath = ConcurrentHashMap<Path, Long>()
    private var executor: ScheduledExecutorService? = null

    fun start() {
        if (!running.compareAndSet(false, true)) return
        val created = Executors.newSingleThreadScheduledExecutor { runnable ->
            Thread(runnable, "aerogel-plugin-jar-hotreload").apply { isDaemon = true }
        }
        executor = created
        created.scheduleWithFixedDelay(
            { runCatching { syncNow() }.onFailure { logger.warn("Plugin jar hot-reload loop failed", it) } },
            1000L,
            1000L,
            TimeUnit.MILLISECONDS
        )
    }

    fun stop() {
        running.set(false)
        executor?.shutdownNow()
        executor = null
        snapshotByPath.clear()
    }

    fun syncNow(): Int {
        val pluginsDir = Path.of("plugins")
        if (!Files.exists(pluginsDir)) return 0

        val currentJars = runCatching {
            Files.list(pluginsDir).use { stream ->
                stream
                    .filter { Files.isRegularFile(it) && it.fileName.toString().endsWith(".jar", ignoreCase = true) }
                    .sorted()
                    .toList()
            }
        }.getOrElse {
            logger.warn("Failed to scan plugin dir: {}", pluginsDir, it)
            return 0
        }

        val currentSet = currentJars.toSet()
        val removed = snapshotByPath.keys.filter { it !in currentSet }
        for (path in removed) {
            snapshotByPath.remove(path)
        }

        var changedCount = 0
        for (jar in currentJars) {
            val stamp = runCatching { Files.getLastModifiedTime(jar).toMillis() }.getOrDefault(0L)
            val previous = snapshotByPath.put(jar, stamp)
            if (previous == null) continue
            if (previous == stamp) continue
            val applied = runCatching { onJarChanged(jar) }
                .onFailure { logger.warn("Failed to apply changed plugin jar: {}", jar, it) }
                .getOrDefault(false)
            if (applied) {
                changedCount++
            }
        }
        return changedCount
    }
}
