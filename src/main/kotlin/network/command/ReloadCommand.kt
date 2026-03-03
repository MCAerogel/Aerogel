package org.macaroon3145.network.command

import org.macaroon3145.network.handler.PlayPackets
import org.macaroon3145.network.handler.PlayerSession

object ReloadCommand : Command {
    override fun execute(context: CommandContext, sender: PlayerSession?, args: List<String>) {
        if (!context.isOperator(sender)) {
            val input = if (args.isEmpty()) "reload" else "reload ${args.joinToString(" ")}" 
            context.sendUnknownCommandWithContext(sender, input)
            return
        }

        if (args.isEmpty()) {
            val reloaded = context.reloadAllPlugins()
            context.sendSourceSuccessTranslation(
                sender,
                "aerogel.console.reload.all.done",
                PlayPackets.ChatComponent.Text(reloaded.toString())
            )
            return
        }

        if (args.size == 1) {
            val target = args[0]
            if (target.equals("all", ignoreCase = true)) {
                val reloaded = context.reloadAllPlugins()
                context.sendSourceSuccessTranslation(
                    sender,
                    "aerogel.console.reload.all.done",
                    PlayPackets.ChatComponent.Text(reloaded.toString())
                )
                return
            }
            val ok = context.reloadPlugin(target)
            if (ok) {
                context.sendSourceSuccessTranslation(
                    sender,
                    "aerogel.console.reload.plugin.done",
                    PlayPackets.ChatComponent.Text(target)
                )
            } else {
                context.sendSourceTranslationWithContext(
                    sender,
                    "command.unknown.command",
                    "reload $target",
                    errorStart = "reload ".length
                )
            }
            return
        }

        val input = "reload ${args.joinToString(" ")}"
        val errorStart = "reload ${args.first()} ".length
        context.sendSourceTranslationWithContext(
            sender,
            "command.unknown.command",
            input,
            errorStart = errorStart
        )
    }
}
