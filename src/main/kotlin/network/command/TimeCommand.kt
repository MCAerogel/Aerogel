package org.macaroon3145.network.command

import org.macaroon3145.network.handler.PlayerSession

object TimeCommand : Command {
    private sealed class ParsedTimeValue {
        data class Success(val ticks: Long) : ParsedTimeValue()
        data object InvalidUnit : ParsedTimeValue()
        data object InvalidNumber : ParsedTimeValue()
    }

    private val subcommands = setOf("add", "set", "query")
    private val setAliases = mapOf(
        "day" to 1_000L,
        "night" to 13_000L,
        "midnight" to 18_000L,
        "noon" to 6_000L
    )
    private const val dayTicks = 24_000L
    private const val ticksPerSecond = 20L
    private val timeWithUnitPattern = Regex("^(\\d+)([dstDST])$")

    override fun execute(context: CommandContext, sender: PlayerSession?, args: List<String>) {
        if (!context.isOperator(sender)) {
            val input = if (args.isEmpty()) "time" else "time ${args.joinToString(" ")}"
            context.sendUnknownCommandWithContext(sender, input)
            return
        }

        if (args.isEmpty()) {
            sendInvalidArgument(context, sender, args, 0)
            return
        }

        val subcommand = args[0].lowercase()
        if (subcommand !in subcommands) {
            sendInvalidArgument(context, sender, args, 0)
            return
        }

        when (subcommand) {
            "set" -> handleSet(context, sender, args)
            "add" -> handleAdd(context, sender, args)
            "query" -> handleQuery(context, sender, args)
        }
    }

    private fun handleSet(
        context: CommandContext,
        sender: PlayerSession?,
        args: List<String>
    ) {
        val valueIndex = 1
        val rawValue = args.getOrNull(valueIndex)
        if (rawValue == null) {
            sendInvalidArgument(context, sender, args, valueIndex)
            return
        }
        val valueToken = rawValue.trim()
        if (args.size > 3) {
            sendInvalidArgument(context, sender, args, 3)
            return
        }
        if (sender == null && args.getOrNull(2).isNullOrBlank()) {
            sendInvalidArgument(context, null, args, 2)
            return
        }
        val worldKey = resolveWorldKey(context, sender, args.getOrNull(2))
        val parsed = parseTimeValue(valueToken, allowSetAliases = true)
        if (parsed is ParsedTimeValue.InvalidUnit) {
            sendInvalidTimeUnit(context, sender, args, valueIndex)
            return
        }
        if (parsed is ParsedTimeValue.InvalidNumber) {
            sendInvalidArgument(context, sender, args, valueIndex)
            return
        }
        val ticks = (parsed as ParsedTimeValue.Success).ticks
        if (ticks < 0L) {
            sendNegativeTickError(context, sender, args, valueIndex, ticks)
            return
        }
        val normalized = normalizeTimeOfDay(ticks)
        val applied = context.setWorldTime(worldKey, normalized)
        if (applied == null) {
            sendInvalidWorld(context, sender, args, 2, worldKey)
            return
        }
        context.sendSourceTranslation(
            sender,
            "commands.time.set",
            org.macaroon3145.network.handler.PlayPackets.ChatComponent.Text(rawValue)
        )
    }

    private fun handleAdd(
        context: CommandContext,
        sender: PlayerSession?,
        args: List<String>
    ) {
        val valueIndex = 1
        val rawValue = args.getOrNull(valueIndex)
        if (rawValue == null) {
            sendInvalidArgument(context, sender, args, valueIndex)
            return
        }
        val valueToken = rawValue.trim()
        if (args.size > 3) {
            sendInvalidArgument(context, sender, args, 3)
            return
        }
        if (sender == null && args.getOrNull(2).isNullOrBlank()) {
            sendInvalidArgument(context, null, args, 2)
            return
        }
        val worldKey = resolveWorldKey(context, sender, args.getOrNull(2))
        val parsed = parseTimeValue(valueToken, allowSetAliases = false)
        if (parsed is ParsedTimeValue.InvalidUnit) {
            sendInvalidTimeUnit(context, sender, args, valueIndex)
            return
        }
        if (parsed is ParsedTimeValue.InvalidNumber) {
            sendInvalidArgument(context, sender, args, valueIndex)
            return
        }
        val delta = (parsed as ParsedTimeValue.Success).ticks
        if (delta < 0L) {
            sendNegativeTickError(context, sender, args, valueIndex, delta)
            return
        }
        val before = context.worldTimeSnapshot(worldKey)
        if (before == null) {
            sendInvalidWorld(context, sender, args, 2, worldKey)
            return
        }
        val applied = context.setWorldTime(worldKey, before.second + delta)
        if (applied == null) {
            sendInvalidWorld(context, sender, args, 2, worldKey)
            return
        }
        context.sendSourceTranslation(
            sender,
            "commands.time.set",
            org.macaroon3145.network.handler.PlayPackets.ChatComponent.Text(applied.second.toString())
        )
    }

    private fun handleQuery(
        context: CommandContext,
        sender: PlayerSession?,
        args: List<String>
    ) {
        val queryIndex = 1
        val kind = args.getOrNull(queryIndex)?.lowercase()
        if (kind == null) {
            sendInvalidArgument(context, sender, args, queryIndex)
            return
        }
        if (args.size > 3) {
            sendInvalidArgument(context, sender, args, 3)
            return
        }
        if (sender == null && args.getOrNull(2).isNullOrBlank()) {
            sendInvalidArgument(context, null, args, 2)
            return
        }
        val worldKey = resolveWorldKey(context, sender, args.getOrNull(2))
        val snapshot = context.worldTimeSnapshot(worldKey)
        if (snapshot == null) {
            sendInvalidWorld(context, sender, args, 2, worldKey)
            return
        }
        val value = when (kind) {
            "daytime" -> normalizeTimeOfDay(snapshot.second)
            "gametime" -> snapshot.first
            "day" -> snapshot.first / dayTicks
            else -> {
                sendInvalidArgument(context, sender, args, queryIndex)
                return
            }
        }
        context.sendSourceTranslation(
            sender,
            "commands.time.query",
            org.macaroon3145.network.handler.PlayPackets.ChatComponent.Text(value.toString())
        )
    }

    private fun parseTimeValue(token: String, allowSetAliases: Boolean): ParsedTimeValue {
        val lowered = token.lowercase()
        if (allowSetAliases) {
            val alias = setAliases[lowered]
            if (alias != null) return ParsedTimeValue.Success(alias)
        }
        val byUnit = parseTimeTokenWithUnit(lowered)
        if (byUnit != null) return ParsedTimeValue.Success(byUnit)
        val byNumber = lowered.toLongOrNull()
        if (byNumber != null) return ParsedTimeValue.Success(byNumber)
        return if (isInvalidTimeUnitToken(lowered)) ParsedTimeValue.InvalidUnit else ParsedTimeValue.InvalidNumber
    }

    private fun parseTimeTokenWithUnit(token: String): Long? {
        if (token == "d") return dayTicks
        if (token == "s") return ticksPerSecond
        if (token == "t") return 1L
        val match = timeWithUnitPattern.matchEntire(token) ?: return null
        val number = match.groupValues[1].toLongOrNull() ?: return null
        val unit = match.groupValues[2].lowercase()
        val multiplier = when (unit) {
            "d" -> dayTicks
            "s" -> ticksPerSecond
            "t" -> 1L
            else -> return null
        }
        return kotlin.runCatching { Math.multiplyExact(number, multiplier) }.getOrNull()
    }

    private fun isInvalidTimeUnitToken(token: String): Boolean {
        if (token.isEmpty()) return false
        if (token == "d" || token == "s" || token == "t") return false
        if (timeWithUnitPattern.matches(token)) return false
        return token.matches(Regex("^\\d+[a-z]+$"))
    }

    private fun normalizeTimeOfDay(value: Long): Long {
        var normalized = value % dayTicks
        if (normalized < 0L) normalized += dayTicks
        return normalized
    }

    private fun resolveWorldKey(context: CommandContext, sender: PlayerSession?, rawWorld: String?): String {
        if (rawWorld.isNullOrBlank()) return context.sourceWorldKey(sender)
        val token = rawWorld.trim()
        val all = context.worldKeys()
        if (all.any { it.equals(token, ignoreCase = true) }) {
            return all.first { it.equals(token, ignoreCase = true) }
        }
        if (!token.contains(':')) {
            val minecraftKey = "minecraft:$token"
            if (all.any { it.equals(minecraftKey, ignoreCase = true) }) {
                return all.first { it.equals(minecraftKey, ignoreCase = true) }
            }
        }
        return token
    }

    private fun sendInvalidArgument(
        context: CommandContext,
        sender: PlayerSession?,
        args: List<String>,
        argIndex: Int
    ) {
        val input = buildInput(args)
        context.sendSourceTranslationWithContext(
            sender,
            "command.unknown.command",
            input,
            errorStart = argumentStartIndex(args, argIndex)
        )
    }

    private fun sendInvalidWorld(
        context: CommandContext,
        sender: PlayerSession?,
        args: List<String>,
        worldArgIndex: Int,
        worldKey: String
    ) {
        context.sendSourceTranslationWithContext(
            sender,
            "argument.dimension.invalid",
            buildInput(args),
            errorStart = argumentStartIndex(args, worldArgIndex),
            org.macaroon3145.network.handler.PlayPackets.ChatComponent.Text(worldKey)
        )
    }

    private fun sendNegativeTickError(
        context: CommandContext,
        sender: PlayerSession?,
        args: List<String>,
        valueArgIndex: Int,
        value: Long
    ) {
        context.sendSourceTranslationWithContext(
            sender,
            "argument.time.tick_count_too_low",
            buildInput(args),
            errorStart = argumentStartIndex(args, valueArgIndex),
            org.macaroon3145.network.handler.PlayPackets.ChatComponent.Text("0"),
            org.macaroon3145.network.handler.PlayPackets.ChatComponent.Text(value.toString())
        )
    }

    private fun sendInvalidTimeUnit(
        context: CommandContext,
        sender: PlayerSession?,
        args: List<String>,
        valueArgIndex: Int
    ) {
        context.sendSourceTranslationWithContext(
            sender,
            "argument.time.invalid_unit",
            buildInput(args),
            errorStart = argumentStartIndex(args, valueArgIndex)
        )
    }

    private fun buildInput(args: List<String>): String {
        return if (args.isEmpty()) "time" else "time ${args.joinToString(" ")}"
    }

    private fun argumentStartIndex(args: List<String>, argIndex: Int): Int {
        var start = "time ".length
        for (i in 0 until argIndex.coerceAtMost(args.size)) {
            start += args[i].length + 1
        }
        return start
    }

}
