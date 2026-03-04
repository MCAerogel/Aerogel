package org.macaroon3145.api

import org.macaroon3145.api.entity.Player
import org.macaroon3145.api.entity.PlayerRegistry
import org.macaroon3145.api.plugin.PluginRuntime
import org.macaroon3145.api.scheduler.TaskScheduler
import org.macaroon3145.api.scheduler.TickScheduler
import org.macaroon3145.api.world.World
import org.macaroon3145.api.world.WorldRegistry
import java.util.UUID

object Server {
    @Volatile
    private var worldRegistry: WorldRegistry? = null
    @Volatile
    private var boundMainTickScheduler: TickScheduler? = null
    @Volatile
    private var boundTaskScheduler: TaskScheduler? = null
    @Volatile
    private var playerRegistry: PlayerRegistry? = null
    @Volatile
    private var currentTps: Double = 0.0
    @Volatile
    private var currentMspt: Double = 0.0

    val mainTickScheduler: TickScheduler
        get() = PluginRuntime.currentContextOrNull()?.tickScheduler
            ?: checkNotNull(boundMainTickScheduler) { "Server is not initialized yet: mainTickScheduler is unavailable." }

    val taskScheduler: TaskScheduler
        get() = PluginRuntime.currentContextOrNull()?.taskScheduler
            ?: checkNotNull(boundTaskScheduler) { "Server is not initialized yet: taskScheduler is unavailable." }

    val tps: Double
        get() = currentTps

    val mspt: Double
        get() = currentMspt

    val defaultWorld: World
        get() = checkNotNull(worldRegistry?.defaultWorld()) { "Server is not initialized yet: defaultWorld is unavailable." }

    fun getWorld(key: String): World? = worldRegistry?.world(key)

    fun getPlayer(uuid: UUID): Player {
        val registry = checkNotNull(playerRegistry) { "Server is not initialized yet: playerRegistry is unavailable." }
        return registry.player(uuid)
    }

    fun getPlayer(name: String): Player? {
        val registry = checkNotNull(playerRegistry) { "Server is not initialized yet: playerRegistry is unavailable." }
        return registry.player(name)
    }

    fun getWorlds(): List<World> = worldRegistry?.allWorlds() ?: emptyList()

    fun getMainTick(): Long = mainTickScheduler.currentTick

    fun isReady(): Boolean = worldRegistry != null

    fun bindWorldRegistry(registry: WorldRegistry) {
        worldRegistry = registry
    }

    fun bindMainTickScheduler(scheduler: TickScheduler) {
        boundMainTickScheduler = scheduler
    }

    fun bindTaskScheduler(scheduler: TaskScheduler) {
        boundTaskScheduler = scheduler
    }

    fun bindPlayerRegistry(registry: PlayerRegistry) {
        playerRegistry = registry
    }

    fun updateTickPerformance(tps: Double, mspt: Double) {
        currentTps = if (tps.isFinite()) tps else 0.0
        currentMspt = if (mspt.isFinite()) mspt else 0.0
    }

    fun clearBindings() {
        worldRegistry = null
        boundMainTickScheduler = null
        boundTaskScheduler = null
        playerRegistry = null
        currentTps = 0.0
        currentMspt = 0.0
    }
}
