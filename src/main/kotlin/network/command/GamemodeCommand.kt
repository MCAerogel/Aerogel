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
            context.sendSourceTranslationWithContext(
                sender,
                "command.unknown.command",
                "gamemode",
                errorStart = "gamemode".length
            )
            return
        }

        val mode = parseGamemode(modeToken)
        if (mode == null) {
            val input = buildString {
                append("gamemode ").append(modeToken)
                if (args.size > 1) {
                    append(' ').append(args.drop(1).joinToString(" "))
                }
            }
            context.sendSourceTranslationWithContext(
                sender,
                "argument.gamemode.invalid",
                input,
                errorStart = "gamemode ".length,
                PlayPackets.ChatComponent.Text(modeToken)
            )
            return
        }
        if (args.size > 2) {
            val input = buildString {
                append("gamemode ").append(args.joinToString(" "))
            }
            val targetToken = args[1]
            context.sendSourceTranslationWithContext(
                sender,
                "command.unknown.command",
                input,
                errorStart = "gamemode $modeToken $targetToken ".length
            )
            return
        }

        val modeComponent = PlayPackets.ChatComponent.Translate("gameMode.${gamemodeName(mode)}")
        val targets = if (args.size >= 2) {
            EntitySelectorResolver.resolvePlayers(context, sender, args[1], single = false) ?: return
        } else {
            if (sender != null) {
                listOf(sender)
            } else {
                context.sendSourceWarnTranslation(null, "aerogel.console.gamemode.requires.target")
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
                    context.playerNameComponent(target),
                    modeComponent
                )
                if (sender == null) {
                    context.sendTranslationFromTerminal(
                        target,
                        "commands.gamemode.success.other",
                        context.playerNameComponent(target),
                        modeComponent
                    )
                } else {
                    context.sendTranslation(target, "gameMode.changed", modeComponent)
                }
            }
        }
    }

    private fun parseGamemode(token: String): Int? {
        return when (token.lowercase()) {
            "survival" -> 0
            "creative" -> 1
            "adventure" -> 2
            "spectator" -> 3
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
