package org.macaroon3145.network.command

import org.macaroon3145.network.handler.PlayerSession

class CommandDispatcher(
    private val commands: Map<String, Command>
) {
    fun commandNames(): Set<String> = commands.keys

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
            return CommandDispatcher(
                mapOf(
                    "op" to OpCommand,
                    "deop" to DeopCommand,
                    "tp" to TpCommand,
                    "teleport" to TpCommand,
                    "gamemode" to GamemodeCommand,
                    "time" to TimeCommand,
                    "perf" to PerfCommand,
                    "stop" to StopCommand
                )
            )
        }
    }
}
