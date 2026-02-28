package org.macaroon3145.network.command

import org.macaroon3145.network.handler.PlayerSession
import kotlin.math.sqrt
import kotlin.random.Random

object EntitySelectorResolver {
    private sealed interface ResolveState {
        data class Success(val players: List<PlayerSession>) : ResolveState
        data class Error(val translationKey: String) : ResolveState
    }

    private data class TypeFilter(
        val negate: Boolean,
        val rawType: String
    )

    private enum class SortMode {
        ARBITRARY,
        NEAREST,
        FURTHEST,
        RANDOM
    }

    private data class DoubleRange(
        val min: Double?,
        val max: Double?
    ) {
        fun matches(value: Double): Boolean {
            val minValue = min
            val maxValue = max
            return when {
                minValue != null && maxValue != null && minValue <= maxValue -> value >= minValue && value <= maxValue
                minValue != null && maxValue != null -> value >= minValue || value <= maxValue
                minValue != null -> value >= minValue
                maxValue != null -> value <= maxValue
                else -> true
            }
        }
    }

    private data class FloatRange(
        val min: Float?,
        val max: Float?
    ) {
        fun matches(value: Float): Boolean {
            val minValue = min
            val maxValue = max
            return when {
                minValue != null && maxValue != null && minValue <= maxValue -> value >= minValue && value <= maxValue
                minValue != null && maxValue != null -> value >= minValue || value <= maxValue
                minValue != null -> value >= minValue
                maxValue != null -> value <= maxValue
                else -> true
            }
        }
    }

    private val supportedOptions = setOf(
        "name", "distance", "level",
        "x", "y", "z", "dx", "dy", "dz",
        "x_rotation", "y_rotation",
        "limit", "sort",
        "gamemode", "team", "type",
        "tag", "nbt", "scores", "advancements", "predicate"
    )

    fun resolvePlayers(
        context: CommandContext,
        sender: PlayerSession?,
        token: String,
        single: Boolean
    ): List<PlayerSession>? {
        val state = if (token.startsWith("@")) {
            resolveSelector(context, sender, token)
        } else {
            context.findOnlinePlayer(token)?.let { ResolveState.Success(listOf(it)) } ?: ResolveState.Error("argument.entity.notfound.player")
        }

        val resolved = when (state) {
            is ResolveState.Success -> state.players
            is ResolveState.Error -> {
                context.sendSourceTranslation(sender, state.translationKey)
                return null
            }
        }
        if (single && resolved.size > 1) {
            context.sendSourceTranslation(sender, "argument.entity.toomany")
            return null
        }
        return resolved
    }

    private fun resolveSelector(
        context: CommandContext,
        sender: PlayerSession?,
        token: String
    ): ResolveState {
        val parsed = EntitySelectorSyntax.parse(token) ?: return ResolveState.Error("argument.entity.invalid")
        val selectorType = parsed.selectorType
        if (parsed.options.any { it.key !in supportedOptions }) {
            return ResolveState.Error("argument.entity.options.unknown")
        }

        val players = if (sender == null) {
            context.onlinePlayers().toMutableList()
        } else {
            context.onlinePlayersInWorld(sender.worldKey).toMutableList()
        }
        val base = when (selectorType) {
            's' -> {
                if (sender == null) return ResolveState.Error("argument.entity.selector.not_allowed")
                mutableListOf(sender)
            }
            'a', 'e' -> players
            'p', 'n' -> players
            'r' -> players
            else -> return ResolveState.Error("argument.entity.selector.unknown")
        }

        val optionsByKey = parsed.options
            .groupBy(
                keySelector = { it.key.lowercase() },
                valueTransform = { it.value }
            )

        if (!applyBasicAndFilters(base, optionsByKey)) {
            return ResolveState.Error("argument.entity.invalid")
        }
        if (!applySpatialFilters(base, context, sender, optionsByKey)) {
            return ResolveState.Error("argument.entity.invalid")
        }

        val sortMode = parseSortMode(optionsByKey["sort"], selectorType) ?: return ResolveState.Error("argument.entity.invalid")
        val sorted = sort(base, context, sender, sortMode)

        val limit = parseLimit(
            optionsByKey["limit"],
            defaultValue = defaultLimit(selectorType, sorted.size)
        ) ?: return ResolveState.Error("argument.entity.invalid")
        val limited = if (limit >= sorted.size) sorted else sorted.subList(0, limit)
        return if (limited.isEmpty()) {
            ResolveState.Error(if (selectorType == 'e') "argument.entity.notfound.entity" else "argument.entity.notfound.player")
        } else {
            ResolveState.Success(limited)
        }
    }

    private fun applyBasicAndFilters(
        players: MutableList<PlayerSession>,
        optionsByKey: Map<String, List<String>>
    ): Boolean {
        applyNameFilters(players, optionsByKey["name"].orEmpty())

        for (raw in optionsByKey["type"].orEmpty()) {
            val filter = parseTypeFilter(raw) ?: return false
            applyTypeFilter(players, filter)
        }

        for (raw in optionsByKey["gamemode"].orEmpty()) {
            if (!applyGamemodeFilter(players, raw)) return false
        }

        applyTeamFilters(players, optionsByKey["team"].orEmpty())
        applyTagFilters(players, optionsByKey["tag"].orEmpty())

        return true
    }

    private fun applySpatialFilters(
        players: MutableList<PlayerSession>,
        context: CommandContext,
        sender: PlayerSession?,
        optionsByKey: Map<String, List<String>>
    ): Boolean {
        val originX = parseDoubleOption(optionsByKey["x"], default = context.sourceX(sender)) ?: return false
        val originY = parseDoubleOption(optionsByKey["y"], default = context.sourceY(sender)) ?: return false
        val originZ = parseDoubleOption(optionsByKey["z"], default = context.sourceZ(sender)) ?: return false
        val dx = parseDoubleOptional(optionsByKey["dx"]) ?: return false
        val dy = parseDoubleOptional(optionsByKey["dy"]) ?: return false
        val dz = parseDoubleOptional(optionsByKey["dz"]) ?: return false

        applyDeltaBoxFilter(players, originX, originY, originZ, dx, dy, dz)

        val distanceTokens = optionsByKey["distance"]
        val distanceRange = parseDoubleRangeOption(distanceTokens, nonNegative = true)
        if (distanceTokens != null && distanceRange == null) return false
        if (distanceRange != null) {
            players.removeIf { player ->
                val dist = sqrt(distanceSq(player, originX, originY, originZ))
                !distanceRange.matches(dist)
            }
        }

        val xRotationTokens = optionsByKey["x_rotation"]
        val xRotationRange = parseFloatRangeOption(xRotationTokens)
        if (xRotationTokens != null && xRotationRange == null) return false
        if (xRotationRange != null) {
            players.removeIf { player -> !xRotationRange.matches(player.pitch) }
        }

        val yRotationTokens = optionsByKey["y_rotation"]
        val yRotationRange = parseFloatRangeOption(yRotationTokens)
        if (yRotationTokens != null && yRotationRange == null) return false
        if (yRotationRange != null) {
            players.removeIf { player -> !yRotationRange.matches(player.yaw) }
        }

        // Currently unsupported in this player-only resolver model.
        if (optionsByKey["level"]?.isNotEmpty() == true) return false
        if (optionsByKey["nbt"]?.isNotEmpty() == true) return false
        if (optionsByKey["scores"]?.isNotEmpty() == true) return false
        if (optionsByKey["advancements"]?.isNotEmpty() == true) return false
        if (optionsByKey["predicate"]?.isNotEmpty() == true) return false

        return true
    }

    private fun applyNameFilters(players: MutableList<PlayerSession>, nameTokens: List<String>) {
        for (nameToken in nameTokens) {
            val negate = nameToken.startsWith("!")
            val name = if (negate) nameToken.substring(1) else nameToken
            players.removeIf { player ->
                val matches = player.profile.username == name
                if (negate) matches else !matches
            }
        }
    }

    private fun parseTypeFilter(rawToken: String): TypeFilter? {
        if (rawToken.isBlank()) return null
        val value = rawToken.trim()
        val negated = value.startsWith("!")
        val core = if (negated) value.substring(1) else value
        if (core.startsWith("#")) {
            return null
        }
        val normalized = EntitySelectorSyntax.normalizeEntityType(value)
        val negate = normalized.startsWith("!")
        val rawType = if (negate) normalized.substring(1) else normalized
        if (rawType.isEmpty()) return null
        return TypeFilter(negate = negate, rawType = rawType)
    }

    private fun applyTypeFilter(players: MutableList<PlayerSession>, filter: TypeFilter) {
        val isPlayerType = filter.rawType == "minecraft:player"
        players.removeIf {
            val matches = isPlayerType
            if (filter.negate) matches else !matches
        }
    }

    private fun applyGamemodeFilter(players: MutableList<PlayerSession>, rawToken: String): Boolean {
        if (rawToken.isBlank()) return false
        val negate = rawToken.startsWith("!")
        val token = if (negate) rawToken.substring(1) else rawToken
        val expected = parseGamemode(token) ?: return false
        players.removeIf { player ->
            val matches = player.gameMode == expected
            if (negate) matches else !matches
        }
        return true
    }

    private fun parseGamemode(token: String): Int? {
        return when (token.lowercase()) {
            "0", "survival" -> 0
            "1", "creative" -> 1
            "2", "adventure" -> 2
            "3", "spectator" -> 3
            else -> null
        }
    }

    private fun applyTeamFilters(players: MutableList<PlayerSession>, teamTokens: List<String>) {
        for (teamToken in teamTokens) {
            val negate = teamToken.startsWith("!")
            val expectedTeam = if (negate) teamToken.substring(1) else teamToken
            players.removeIf {
                val actualTeam = ""
                val matches = actualTeam == expectedTeam
                if (negate) matches else !matches
            }
        }
    }

    private fun applyTagFilters(players: MutableList<PlayerSession>, tagTokens: List<String>) {
        for (tagToken in tagTokens) {
            val negate = tagToken.startsWith("!")
            val expectedTag = if (negate) tagToken.substring(1) else tagToken
            players.removeIf {
                val matches = expectedTag.isEmpty()
                if (negate) matches else !matches
            }
        }
    }

    private fun parseSortMode(sortTokens: List<String>?, selectorType: Char): SortMode? {
        if (sortTokens.isNullOrEmpty()) return defaultSort(selectorType)
        if (sortTokens.size != 1) return null
        return when (sortTokens.first().lowercase()) {
            "nearest" -> SortMode.NEAREST
            "furthest" -> SortMode.FURTHEST
            "random" -> SortMode.RANDOM
            "arbitrary" -> SortMode.ARBITRARY
            else -> null
        }
    }

    private fun parseLimit(limitTokens: List<String>?, defaultValue: Int): Int? {
        if (limitTokens.isNullOrEmpty()) return defaultValue
        if (limitTokens.size != 1) return null
        val value = limitTokens.first().toIntOrNull() ?: return null
        if (value < 0) return null
        return value
    }

    private fun parseDoubleOption(tokens: List<String>?, default: Double): Double? {
        if (tokens.isNullOrEmpty()) return default
        if (tokens.size != 1) return null
        return tokens.first().toDoubleOrNull()
    }

    private fun parseDoubleOptional(tokens: List<String>?): Double? {
        if (tokens == null) return 0.0
        if (tokens.size != 1) return null
        return tokens.first().toDoubleOrNull()
    }

    private fun parseDoubleRangeOption(tokens: List<String>?, nonNegative: Boolean): DoubleRange? {
        if (tokens.isNullOrEmpty()) return null
        if (tokens.size != 1) return null
        val range = parseDoubleRange(tokens.first()) ?: return null
        if (nonNegative) {
            if ((range.min != null && range.min < 0.0) || (range.max != null && range.max < 0.0)) {
                return null
            }
        }
        return range
    }

    private fun parseFloatRangeOption(tokens: List<String>?): FloatRange? {
        if (tokens.isNullOrEmpty()) return null
        if (tokens.size != 1) return null
        return parseFloatRange(tokens.first())
    }

    private fun parseDoubleRange(token: String): DoubleRange? {
        val trimmed = token.trim()
        if (trimmed.isEmpty()) return null
        val idx = trimmed.indexOf("..")
        if (idx < 0) {
            val value = trimmed.toDoubleOrNull() ?: return null
            return DoubleRange(value, value)
        }

        val left = trimmed.substring(0, idx).trim()
        val right = trimmed.substring(idx + 2).trim()
        val min = if (left.isEmpty()) null else left.toDoubleOrNull() ?: return null
        val max = if (right.isEmpty()) null else right.toDoubleOrNull() ?: return null
        if (min == null && max == null) return null
        return DoubleRange(min, max)
    }

    private fun parseFloatRange(token: String): FloatRange? {
        val trimmed = token.trim()
        if (trimmed.isEmpty()) return null
        val idx = trimmed.indexOf("..")
        if (idx < 0) {
            val value = trimmed.toFloatOrNull() ?: return null
            return FloatRange(value, value)
        }

        val left = trimmed.substring(0, idx).trim()
        val right = trimmed.substring(idx + 2).trim()
        val min = if (left.isEmpty()) null else left.toFloatOrNull() ?: return null
        val max = if (right.isEmpty()) null else right.toFloatOrNull() ?: return null
        if (min == null && max == null) return null
        return FloatRange(min, max)
    }

    private fun applyDeltaBoxFilter(
        players: MutableList<PlayerSession>,
        originX: Double,
        originY: Double,
        originZ: Double,
        dx: Double,
        dy: Double,
        dz: Double
    ) {
        val hasBox = dx != 0.0 || dy != 0.0 || dz != 0.0
        if (!hasBox) return

        val minX = minOf(originX, originX + dx)
        val maxX = maxOf(originX, originX + dx)
        val minY = minOf(originY, originY + dy)
        val maxY = maxOf(originY, originY + dy)
        val minZ = minOf(originZ, originZ + dz)
        val maxZ = maxOf(originZ, originZ + dz)

        players.removeIf { player ->
            player.x < minX || player.x > maxX ||
                player.y < minY || player.y > maxY ||
                player.z < minZ || player.z > maxZ
        }
    }

    private fun sort(
        players: MutableList<PlayerSession>,
        context: CommandContext,
        sender: PlayerSession?,
        mode: SortMode
    ): MutableList<PlayerSession> {
        val sourceX = context.sourceX(sender)
        val sourceY = context.sourceY(sender)
        val sourceZ = context.sourceZ(sender)
        return when (mode) {
            SortMode.ARBITRARY -> players
            SortMode.RANDOM -> players.shuffled(Random.Default).toMutableList()
            SortMode.NEAREST -> players.sortedBy { distanceSq(it, sourceX, sourceY, sourceZ) }.toMutableList()
            SortMode.FURTHEST -> players.sortedByDescending { distanceSq(it, sourceX, sourceY, sourceZ) }.toMutableList()
        }
    }

    private fun defaultSort(selectorType: Char): SortMode {
        return when (selectorType) {
            'p', 'n' -> SortMode.NEAREST
            'r' -> SortMode.RANDOM
            else -> SortMode.ARBITRARY
        }
    }

    private fun defaultLimit(selectorType: Char, size: Int): Int {
        return when (selectorType) {
            'p', 'n', 'r', 's' -> 1
            else -> size
        }
    }

    private fun distanceSq(a: PlayerSession, x: Double, y: Double, z: Double): Double {
        val dx = a.x - x
        val dy = a.y - y
        val dz = a.z - z
        return dx * dx + dy * dy + dz * dz
    }
}
