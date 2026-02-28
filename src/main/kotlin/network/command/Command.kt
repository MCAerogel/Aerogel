package org.macaroon3145.network.command

import org.macaroon3145.network.handler.PlayerSession

fun interface Command {
    fun execute(context: CommandContext, sender: PlayerSession?, args: List<String>)
}
