package org.macaroon3145.network.command

import org.macaroon3145.network.handler.PlayPackets
import org.macaroon3145.network.handler.PlayerSession
import java.util.UUID

object DeopCommand : Command {
    override fun execute(context: CommandContext, sender: PlayerSession?, args: List<String>) {
        if (!context.canUseOpCommand(sender)) {
            val input = if (args.isEmpty()) "deop" else "deop ${args.joinToString(" ")}"
            context.sendUnknownCommandWithContext(sender, input)
            return
        }

        val targetArgument = args.firstOrNull()
        if (targetArgument.isNullOrEmpty()) {
            if (sender == null) {
                context.sendSourceWarnTranslation(null, "aerogel.console.deop.usage")
            } else {
                context.sendSourceTranslationWithContext(
                    sender,
                    "command.unknown.command",
                    "deop",
                    errorStart = "deop".length
                )
            }
            return
        }
        if (args.size > 1) {
            val input = buildString {
                append("deop ").append(targetArgument)
                append(' ').append(args.drop(1).joinToString(" "))
            }
            context.sendSourceTranslationWithContext(
                sender,
                "command.unknown.command",
                input,
                errorStart = "deop $targetArgument ".length
            )
            return
        }

        val parsedTargetUuid = runCatching { UUID.fromString(targetArgument) }.getOrNull()
        val target = parsedTargetUuid?.let(context::findOnlinePlayer) ?: context.findOnlinePlayer(targetArgument)
        val targetUuid = target?.profile?.uuid ?: parsedTargetUuid
        if (targetUuid == null) {
            if (sender == null) {
                context.sendSourceWarnTranslation(null, "aerogel.console.deop.not_found", PlayPackets.ChatComponent.Text(targetArgument))
            } else {
                context.sendSourceErrorTranslation(sender, "argument.entity.notfound.player")
            }
            return
        }
        val revoked = context.revokeOperator(targetUuid)
        val targetComponent = target?.let(context::playerNameComponent) ?: PlayPackets.ChatComponent.Text(targetUuid.toString())
        if (revoked) {
            if (sender == null) {
                context.sendSourceTranslation(null, "aerogel.console.deop.revoked", targetComponent)
            } else {
                context.sendSourceTranslation(sender, "commands.deop.success", targetComponent)
            }
            if (target != null && sender == null) {
                context.sendTranslationFromTerminal(target, "commands.deop.success", context.playerNameComponent(target))
            } else if (target != null && sender != null && target.channelId != sender.channelId) {
                context.sendAdminTranslation(
                    target,
                    context.playerNameComponent(sender),
                    "commands.deop.success",
                    context.playerNameComponent(target)
                )
            }
        } else {
            if (sender == null) {
                context.sendSourceWarnTranslation(null, "aerogel.console.deop.not_operator", targetComponent)
            } else {
                context.sendSourceTranslation(sender, "commands.deop.failed")
            }
        }
    }
}
