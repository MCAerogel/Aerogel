package org.macaroon3145.network.command

import org.macaroon3145.network.handler.PlayerSession

fun interface Command {
    fun execute(context: CommandContext, sender: PlayerSession?, args: List<String>)

    fun visibleTo(sender: PlayerSession?): Boolean = true

    fun complete(
        context: CommandContext,
        sender: PlayerSession?,
        providedArgs: List<String>,
        activeArgIndex: Int,
        activeArgPrefix: String
    ): List<String> = emptyList()
}
