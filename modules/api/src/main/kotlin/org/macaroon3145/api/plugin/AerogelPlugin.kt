package org.macaroon3145.api.plugin

import org.macaroon3145.api.command.CommandRegistrar
import org.macaroon3145.api.data.PluginDataStore
import org.macaroon3145.api.entity.Player
import org.macaroon3145.api.event.EventBus
import org.macaroon3145.api.packet.PacketInterceptorRegistry
import org.macaroon3145.api.permission.PermissionService
import org.macaroon3145.api.scheduler.TaskScheduler
import org.macaroon3145.api.scheduler.TickScheduler
import org.macaroon3145.api.service.ServiceRegistry
import org.macaroon3145.api.type.TypeRegistry
import org.macaroon3145.api.world.WorldRegistry

interface AerogelPlugin {
    fun onLoad(context: PluginContext) {}

    fun onEnable(context: PluginContext) {}

    fun onDisable(context: PluginContext, reason: PluginDisableReason) {}

    fun onReload(context: PluginContext, reason: PluginReloadReason) {}

    fun listeners(): List<Any> = emptyList()
}

enum class PluginDisableReason {
    SHUTDOWN,
    RELOAD,
    DEPENDENCY_RELOAD,
    LOAD_FAILURE
}

enum class PluginReloadReason {
    COMMAND_RELOAD,
    DEPENDENCY_RELOAD,
    DEV_HOT_RELOAD
}

enum class PluginState {
    LOADING,
    LOADED,
    ENABLING,
    ENABLED,
    DISABLING,
    DISABLED,
    FAILED
}

data class PluginMetadata(
    val id: String,
    val name: String,
    val version: String,
    val apiVersion: String,
    val dependencies: List<String> = emptyList(),
    val softDependencies: List<String> = emptyList()
)

interface PluginContext {
    val metadata: PluginMetadata
    val state: PluginState
    val logger: PluginLogger
    val events: EventBus
    val commands: CommandRegistrar
    val scheduler: TaskScheduler
    val taskScheduler: TaskScheduler
    val tickScheduler: TickScheduler
    val services: ServiceRegistry
    val permissions: PermissionService
    val dataStore: PluginDataStore
    val packets: PacketInterceptorRegistry
    val types: TypeRegistry
    val worlds: WorldRegistry
    val plugins: PluginRegistry
    val i18n: PluginI18n
}

interface PluginRegistry {
    fun isLoaded(pluginId: String): Boolean
    fun metadata(pluginId: String): PluginMetadata?
    fun context(pluginId: String): PluginContext?
    fun loadedPluginIds(): List<String>
}

interface PluginLogger {
    fun info(message: String)
    fun warn(message: String)
    fun error(message: String, error: Throwable? = null)
}

interface PluginI18n {
    fun tr(key: String, vararg args: String): String
    fun trFor(localeTag: String?, key: String, vararg args: String): String
    fun tr(player: Player, key: String, vararg args: String): String
}
