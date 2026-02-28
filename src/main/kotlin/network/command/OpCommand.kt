package org.macaroon3145.network.command

import org.macaroon3145.network.handler.PlayerSession
import org.macaroon3145.network.handler.PlayPackets

object OpCommand : Command {
    override fun execute(context: CommandContext, sender: PlayerSession?, args: List<String>) {
        if (!context.canUseOpCommand(sender)) {
            val input = if (args.isEmpty()) "op" else "op ${args.joinToString(" ")}"
            context.sendUnknownCommandWithContext(sender, input)
            return
        }
        val targetName = args.firstOrNull()
        if (targetName.isNullOrEmpty()) {
            if (sender == null) {
                context.sendSourceWarnTranslation(null, "aerogel.console.op.usage")
            } else {
                context.sendSourceTranslationWithContext(
                    sender,
                    "command.unknown.command",
                    "op",
                    errorStart = "op".length
                )
            }
            return
        }
        if (args.size > 1) {
            val input = buildString {
                append("op ").append(targetName)
                append(' ').append(args.drop(1).joinToString(" "))
            }
            context.sendSourceTranslationWithContext(
                sender,
                "command.unknown.command",
                input,
                errorStart = "op $targetName ".length
            )
            return
        }
        val target = context.findOnlinePlayer(targetName)
        if (target == null) {
            if (sender == null) {
                context.sendSourceErrorTranslation(null, "aerogel.console.op.not_found", PlayPackets.ChatComponent.Text(targetName))
            } else {
                context.sendSourceErrorTranslation(sender, "argument.entity.notfound.player")
            }
            return
        }
        val granted = context.grantOperator(target)
        if (granted) {
            if (sender == null) {
                context.sendSourceTranslation(null, "aerogel.console.op.granted", context.playerNameComponent(target))
                context.sendTranslationFromTerminal(target, "commands.op.success", context.playerNameComponent(target))
            } else {
                context.sendSourceTranslation(sender, "commands.op.success", context.playerNameComponent(target))
                if (target.channelId != sender.channelId) {
                    context.sendAdminTranslation(
                        target,
                        context.playerNameComponent(sender),
                        "commands.op.success",
                        context.playerNameComponent(target)
                    )
                }
            }
        } else {
            if (sender == null) {
                context.sendSourceWarnTranslation(null, "aerogel.console.op.already", context.playerNameComponent(target))
            } else {
                context.sendSourceTranslation(sender, "commands.op.failed")
            }
        }
    }
}
