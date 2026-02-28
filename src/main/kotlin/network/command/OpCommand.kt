package org.macaroon3145.network.command

import org.macaroon3145.network.handler.PlayerSession
import org.macaroon3145.network.handler.PlayPackets

object OpCommand : Command {
    override fun execute(context: CommandContext, sender: PlayerSession?, args: List<String>) {
        if (sender != null) {
            // Console-only for player senders.
            val input = if (args.isEmpty()) "op" else "op ${args.joinToString(" ")}"
            context.sendUnknownCommandWithContext(sender, input)
            return
        }
        val targetName = args.firstOrNull()
        if (targetName.isNullOrEmpty()) {
            context.sendSourceWarnTranslation(null, "aerogel.console.op.usage")
            return
        }
        val target = context.findOnlinePlayer(targetName)
        if (target == null) {
            context.sendSourceErrorTranslation(null, "aerogel.console.op.not_found", PlayPackets.ChatComponent.Text(targetName))
            return
        }
        val granted = context.grantOperator(target)
        if (granted) {
            context.sendSourceTranslation(null, "aerogel.console.op.granted", PlayPackets.ChatComponent.Text(target.profile.username))
            context.sendTranslationFromTerminal(target, "commands.op.success", PlayPackets.ChatComponent.Text(target.profile.username))
        } else {
            context.sendSourceWarnTranslation(null, "aerogel.console.op.already", PlayPackets.ChatComponent.Text(target.profile.username))
        }
    }
}
