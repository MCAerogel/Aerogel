package org.macaroon3145.network.command

object StopCommand : Command {
    override fun execute(context: CommandContext, sender: org.macaroon3145.network.handler.PlayerSession?, args: List<String>) {
        if (!context.isOperator(sender)) {
            val input = if (args.isEmpty()) "stop" else "stop ${args.joinToString(" ")}"
            context.sendUnknownCommandWithContext(sender, input)
            return
        }

        if (args.isNotEmpty()) {
            context.sendSourceWarnTranslation(sender, "aerogel.console.stop.usage")
            return
        }

        if (!context.stopServer()) {
            context.sendSourceWarnTranslation(sender, "aerogel.console.stop.already")
        }
    }
}
