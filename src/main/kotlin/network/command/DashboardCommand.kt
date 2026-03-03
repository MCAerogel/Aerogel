package org.macaroon3145.network.command

object DashboardCommand : Command {
    override fun execute(context: CommandContext, sender: org.macaroon3145.network.handler.PlayerSession?, args: List<String>) {
        if (!context.isOperator(sender)) {
            val input = if (args.isEmpty()) "dashboard" else "dashboard ${args.joinToString(" ")}"
            context.sendUnknownCommandWithContext(sender, input)
            return
        }

        if (args.isNotEmpty()) {
            context.sendSourceTranslationWithContext(
                sender,
                "command.unknown.argument",
                "dashboard ${args.joinToString(" ")}",
                errorStart = "dashboard ".length
            )
            return
        }

        if (!context.showDashboard()) {
            context.sendSourceWarnTranslation(sender, "aerogel.console.dashboard.unavailable")
            return
        }
        context.sendSourceTranslation(sender, "aerogel.console.dashboard.opened")
    }
}
