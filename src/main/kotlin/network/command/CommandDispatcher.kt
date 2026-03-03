package org.macaroon3145.network.command

import org.macaroon3145.network.handler.PlayerSession
import java.util.concurrent.ConcurrentHashMap

class CommandDispatcher(
    commands: Map<String, Command> = emptyMap()
) {
    private val commands = ConcurrentHashMap<String, Command>()

    init {
        for ((name, command) in commands) {
            register(name, command)
        }
    }

    fun commandNames(): Set<String> = commands.keys

    fun register(name: String, command: Command) {
        commands[name.lowercase()] = command
    }

    fun registerAll(vararg names: String, command: Command) {
        for (name in names) {
            register(name, command)
        }
    }

    fun unregister(name: String) {
        commands.remove(name.lowercase())
    }

    fun dispatch(context: CommandContext, sender: PlayerSession?, rawCommand: String) {
        val stripped = rawCommand.removePrefix("/").trim()
        if (stripped.isEmpty()) return
        val parts = stripped.split(Regex("\\s+"))
        if (parts.isEmpty()) return

        val name = parts[0].lowercase()
        val args = if (parts.size > 1) parts.subList(1, parts.size) else emptyList()
        val command = commands[name]
        if (command == null) {
            context.sendUnknownCommandWithContext(sender, stripped)
            return
        }
        command.execute(context, sender, args)
    }

    companion object {
        fun default(): CommandDispatcher {
            val dispatcher = CommandDispatcher()
            dispatcher.register("op", OpCommand)
            dispatcher.register("deop", DeopCommand)
            dispatcher.registerAll("tp", "teleport", command = TpCommand)
            dispatcher.register("gamemode", GamemodeCommand)
            dispatcher.register("time", TimeCommand)
            dispatcher.register("perf", PerfCommand)
            dispatcher.register("dashboard", DashboardCommand)
            dispatcher.register("stop", StopCommand)
            dispatcher.register("reload", ReloadCommand)
            return dispatcher
        }
    }
}
