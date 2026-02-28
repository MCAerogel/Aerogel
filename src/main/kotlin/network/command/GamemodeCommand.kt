package org.macaroon3145.network.command

import org.macaroon3145.network.handler.PlayPackets
import org.macaroon3145.network.handler.PlayerSession

object GamemodeCommand : Command {
    override fun execute(context: CommandContext, sender: PlayerSession?, args: List<String>) {
        if (!context.isOperator(sender)) {
            val input = if (args.isEmpty()) "gamemode" else "gamemode ${args.joinToString(" ")}"
            context.sendUnknownCommandWithContext(sender, input)
            return
        }

        val modeToken = args.getOrNull(0)
        if (modeToken.isNullOrEmpty()) {
            if (sender == null) {
                context.sendSourceWarnTranslation(null, "command.unknown.argument")
            } else {
                context.sendSourceTranslation(sender, "command.unknown.argument")
            }
            return
        }

        val mode = parseGamemode(modeToken)
        if (mode == null) {
            if (sender == null) {
                context.sendSourceWarnTranslation(null, "argument.gamemode.invalid", PlayPackets.ChatComponent.Text(modeToken))
            } else {
                context.sendSourceTranslation(sender, "argument.gamemode.invalid", PlayPackets.ChatComponent.Text(modeToken))
            }
            return
        }

        val modeComponent = PlayPackets.ChatComponent.Translate("gameMode.${gamemodeName(mode)}")
        val targets = if (args.size >= 2) {
            EntitySelectorResolver.resolvePlayers(context, sender, args[1], single = false) ?: return
        } else {
            if (sender != null) {
                listOf(sender)
            } else {
                context.sendSourceWarnTranslation(null, "permissions.requires.player")
                return
            }
        }

        for (target in targets) {
            context.setGamemode(target, mode)
            if (sender != null && target.channelId == sender.channelId) {
                context.sendSourceTranslation(sender, "commands.gamemode.success.self", modeComponent)
            } else {
                context.sendSourceTranslation(
                    sender,
                    "commands.gamemode.success.other",
                    PlayPackets.ChatComponent.Text(target.profile.username),
                    modeComponent
                )
                context.sendTranslation(target, "gameMode.changed", modeComponent)
            }
        }
    }

    private fun parseGamemode(token: String): Int? {
        return when (token.lowercase()) {
            "0", "s", "survival" -> 0
            "1", "c", "creative" -> 1
            "2", "a", "adventure" -> 2
            "3", "sp", "spectator" -> 3
            else -> null
        }
    }

    private fun gamemodeName(mode: Int): String {
        return when (mode.coerceIn(0, 3)) {
            0 -> "survival"
            1 -> "creative"
            2 -> "adventure"
            else -> "spectator"
        }
    }
}
