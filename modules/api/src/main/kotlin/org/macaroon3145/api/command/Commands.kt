package org.macaroon3145.api.command

interface CommandSender {
    val name: String
    fun sendMessage(message: String)
    fun hasPermission(node: String): Boolean
}

data class CommandInvocation(
    val name: String,
    val args: List<String>,
    val sender: CommandSender?
)

fun interface CommandExecutor {
    fun execute(invocation: CommandInvocation)
}

fun interface CommandCompleter {
    fun complete(invocation: CommandInvocation): List<String>
}

data class CommandSpec(
    val name: String,
    val aliases: List<String> = emptyList(),
    val permission: String? = null,
    val executor: CommandExecutor,
    val completer: CommandCompleter? = null
)

interface RegisteredCommand {
    val name: String
    fun unregister()
}

interface CommandRegistrar {
    fun register(owner: String, spec: CommandSpec): RegisteredCommand
    fun unregisterOwner(owner: String)
    fun commandNames(): Set<String>
}
