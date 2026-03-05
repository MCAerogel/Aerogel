package org.macaroon3145.api.command

import org.macaroon3145.api.plugin.PluginRuntime

@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class Command(
    val value: String,
    val aliases: Array<String> = []
)

@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class Permission(val value: String)

@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class PlayerOnly

@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class ConsoleOnly

interface CommandSender {
    val name: String
    fun sendMessage(message: String)
    fun hasPermission(node: String): Boolean
}

data class CommandInvocation(
    val name: String,
    val args: List<String>,
    val sender: CommandSender
)

fun interface CommandExecutor {
    fun execute(invocation: CommandInvocation)
}

fun interface CommandCompleter {
    fun complete(invocation: CommandInvocation): List<String>
}

class CommandContext(
    val name: String,
    val inputs: List<String>,
    val sender: CommandSender,
    val activeInputPrefix: String,
    private val onlinePlayerNamesProvider: () -> List<String>,
    private val worldKeysProvider: () -> List<String>,
    private val pluginIdsProvider: () -> List<String>,
    private val commandNamesProvider: () -> Set<String>,
    private val itemKeysProvider: () -> List<String>,
    private val blockKeysProvider: () -> List<String>
) {
    private var appendCompletions: Boolean = false

    val args: List<String>
        get() = if (inputs.size > 1) inputs.drop(1) else emptyList()

    val isPlayer: Boolean
        get() = !nameEquals(sender.name, "CONSOLE")

    val isConsole: Boolean
        get() = nameEquals(sender.name, "CONSOLE")

    val appendCompletionsToInput: Boolean
        get() = appendCompletions

    fun input(index: Int): String? = inputs.getOrNull(index)

    fun complete(candidates: Iterable<String>): List<String> {
        return candidates.asSequence()
            .filter { it.startsWith(activeInputPrefix, ignoreCase = true) }
            .distinct()
            .sortedWith(String.CASE_INSENSITIVE_ORDER)
            .toList()
    }

    fun completeSuffixes(suffixes: Iterable<String>): List<String> {
        appendCompletions = true
        return suffixes.asSequence()
            .distinct()
            .toList()
    }

    fun completeSuffixes(vararg suffixes: String): List<String> = completeSuffixes(suffixes.asList())

    fun completePlayers(): List<String> = complete(onlinePlayerNamesProvider())

    fun completeWorlds(): List<String> = complete(worldKeysProvider())

    fun completePlugins(): List<String> {
        // Keep provider order so plugin display names can be prioritized over ids.
        return pluginIdsProvider().asSequence()
            .filter { it.startsWith(activeInputPrefix, ignoreCase = true) }
            .distinct()
            .toList()
    }

    fun completeCommands(): List<String> = complete(commandNamesProvider())

    fun completeItems(): List<String> = completeMinecraftKeys(itemKeysProvider())

    fun completeBlocks(): List<String> = completeMinecraftKeys(blockKeysProvider())

    private fun completeMinecraftKeys(keys: List<String>): List<String> {
        val rawPrefix = activeInputPrefix
        return keys.asSequence()
            .map { key -> key.removePrefix("minecraft:") }
            .filter { stripped -> stripped.startsWith(rawPrefix, ignoreCase = true) }
            .distinct()
            .sortedWith(String.CASE_INSENSITIVE_ORDER)
            .toList()
    }

    private fun nameEquals(a: String, b: String): Boolean {
        return a.equals(b, ignoreCase = true)
    }
}

data class CommandHandler(
    val execute: (() -> Unit)? = null,
    val complete: (() -> List<String>?)? = null,
    val strict: Boolean = true
)

data class FunctionalCommandSpec(
    val name: String,
    val aliases: List<String> = emptyList(),
    val permission: String? = null,
    val playerOnly: Boolean = false,
    val consoleOnly: Boolean = false,
    val handler: (CommandContext) -> CommandHandler?
)

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
    fun register(owner: String, spec: FunctionalCommandSpec): RegisteredCommand {
        throw UnsupportedOperationException("Functional command registration is not supported by this registrar.")
    }
    fun registerAnnotated(owner: String, handler: Any): List<RegisteredCommand> = emptyList()
    fun unregisterOwner(owner: String)
    fun commandNames(): Set<String>
}

fun registerCommands(handler: Any): List<RegisteredCommand> {
    val context = PluginRuntime.requireCurrentContext()
    return context.commands.registerAnnotated(context.metadata.id, handler)
}

fun command(
    name: String,
    aliases: List<String> = emptyList(),
    permission: String? = null,
    playerOnly: Boolean = false,
    consoleOnly: Boolean = false,
    handler: CommandContext.() -> CommandHandler?
): RegisteredCommand {
    require(playerOnly.not() || consoleOnly.not()) {
        "Cannot enable both playerOnly and consoleOnly for '$name'."
    }
    val context = PluginRuntime.requireCurrentContext()
    return context.commands.register(
        owner = context.metadata.id,
        spec = FunctionalCommandSpec(
            name = name,
            aliases = aliases,
            permission = permission,
            playerOnly = playerOnly,
            consoleOnly = consoleOnly,
            handler = { commandContext -> handler(commandContext) }
        )
    )
}
