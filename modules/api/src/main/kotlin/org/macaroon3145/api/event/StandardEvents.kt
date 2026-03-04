package org.macaroon3145.api.event

import org.macaroon3145.api.command.CommandSender

data class CommandDispatchEvent(
    val sender: CommandSender,
    val commandLine: String,
    val commandName: String,
    val args: List<String>,
    override var cancelled: Boolean = false,
    override var cancelReason: String? = null
) : CancellableEvent
