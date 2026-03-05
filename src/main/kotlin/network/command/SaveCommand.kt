package org.macaroon3145.network.command

object SaveCommand : Command {
    override fun execute(context: CommandContext, sender: org.macaroon3145.network.handler.PlayerSession?, args: List<String>) {
        if (!context.isOperator(sender)) {
            val input = if (args.isEmpty()) "save" else "save ${args.joinToString(" ")}"
            context.sendUnknownCommandWithContext(sender, input)
            return
        }

        if (args.isNotEmpty()) {
            context.sendSourceTranslationWithContext(
                sender,
                "command.unknown.command",
                "save ${args.joinToString(" ")}",
                errorStart = "save ".length
            )
            return
        }

        val requester = sender
        val started = context.saveWorldsAsync { ok ->
            if (requester == null) {
                if (ok) {
                    context.sendSourceSuccessTranslation(null, "aerogel.console.save.done")
                } else {
                    context.sendSourceErrorTranslation(null, "aerogel.console.save.failed")
                }
                return@saveWorldsAsync
            }
            if (ok) {
                context.sendSourceSuccessTranslation(requester, "aerogel.console.save.done")
            } else {
                context.sendSourceErrorTranslation(requester, "aerogel.console.save.failed")
            }
        }
        if (!started) {
            context.sendSourceWarnTranslation(sender, "aerogel.console.save.already")
            return
        }
    }
}
