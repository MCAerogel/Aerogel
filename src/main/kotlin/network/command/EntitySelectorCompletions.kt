package org.macaroon3145.network.command

import org.macaroon3145.network.codec.RegistryCodec

object EntitySelectorCompletions {
    data class FragmentSuggestions(
        val relativeStart: Int,
        val replaceLength: Int,
        val suggestions: List<String>
    )

    private val baseSelectors = listOf("@p", "@a", "@r", "@s", "@n", "@e")
    // Vanilla 1.21.11 EntitySelectorOptions.bootStrap() order.
    private val optionKeys = listOf(
        "name", "distance", "level",
        "x", "y", "z", "dx", "dy", "dz",
        "x_rotation", "y_rotation",
        "limit", "sort",
        "gamemode", "team", "type",
        "tag", "nbt", "scores", "advancements", "predicate"
    )
    private val sortValues = listOf("nearest", "furthest", "random", "arbitrary")
    private val gamemodeValues = listOf("survival", "creative", "adventure", "spectator")
    private val limitValues = listOf("1", "2", "5", "10")

    private val entityTypeValues: List<String> by lazy {
        val registry = RegistryCodec.allRegistries().firstOrNull { it.id == "minecraft:entity_type" }
        if (registry != null) {
            registry.entries.map { it.key }.sortedWith(String.CASE_INSENSITIVE_ORDER)
        } else {
            listOf("minecraft:player")
        }
    }

    fun suggest(prefix: String): List<String> {
        if (prefix.isEmpty()) return baseSelectors
        if (!prefix.startsWith("@")) return emptyList()

        val bracketIndex = prefix.indexOf('[')
        if (bracketIndex < 0) {
            return baseSelectors
                .filter { it.startsWith(prefix, ignoreCase = true) }
                .sortedWith(String.CASE_INSENSITIVE_ORDER)
        }

        val selectorHead = prefix.substring(0, bracketIndex)
        if (selectorHead !in baseSelectors) return emptyList()
        val bodyPrefix = prefix.substring(bracketIndex + 1)
        if (bodyPrefix.contains(']')) return emptyList()

        val parts = splitTopLevel(bodyPrefix, ',') ?: return emptyList()
        if (parts.isEmpty()) return emptyList()

        val activeRaw = parts.last()
        val committed = parts.dropLast(1)
            .map { it.trim() }
            .filter { it.isNotEmpty() }
        val usedStates = collectUsedStates(committed)

        val base = buildBase(selectorHead, committed)
        val active = activeRaw.trim()
        val completions = mutableListOf<String>()

        if (!active.contains('=')) {
            val keyPrefix = active.lowercase()
            val availableKeys = optionKeys.filter { isOptionStillApplicable(it, usedStates) }
            for (key in availableKeys) {
                if (key.startsWith(keyPrefix, ignoreCase = true)) {
                    completions += "$base$key="
                }
            }
        } else {
            val key = active.substringBefore('=').trim().lowercase()
            if (key.isEmpty()) return emptyList()
            val valuePrefix = active.substringAfter('=').trim()
            when (key) {
                "type" -> appendValueCompletions(completions, base, key, suggestTypeValues(valuePrefix))
                "sort" -> appendValueCompletions(completions, base, key, suggestSimpleValues(sortValues, valuePrefix))
                "gamemode" -> {
                    val values = gamemodeValues + gamemodeValues.map { "!$it" }
                    appendValueCompletions(completions, base, key, suggestSimpleValues(values, valuePrefix))
                }
                "limit" -> appendValueCompletions(completions, base, key, suggestSimpleValues(limitValues, valuePrefix))
                "name", "team", "tag", "predicate" -> {
                    appendValueCompletions(completions, base, key, suggestTextualValues(valuePrefix))
                }
                "distance", "level" -> {
                    val values = listOf("..10", "5..", "0..100", "0")
                    appendValueCompletions(completions, base, key, suggestSimpleValues(values, valuePrefix))
                }
                "x", "y", "z", "dx", "dy", "dz", "x_rotation", "y_rotation" -> {
                    val values = listOf("-100", "0", "64", "100")
                    appendValueCompletions(completions, base, key, suggestSimpleValues(values, valuePrefix))
                }
                "scores", "advancements", "nbt" -> {
                    appendValueCompletions(completions, base, key, suggestStructuredValues(valuePrefix))
                }
            }
        }

        return completions
            .filter { it.startsWith(prefix, ignoreCase = true) }
            .distinct()
            .sortedWith(String.CASE_INSENSITIVE_ORDER)
    }

    fun suggestFragment(prefix: String): FragmentSuggestions? {
        if (prefix.isEmpty() || !prefix.startsWith("@")) return null

        val bracketIndex = prefix.indexOf('[')
        if (bracketIndex < 0) {
            val exactHead = baseSelectors.firstOrNull { prefix.equals(it, ignoreCase = true) }
            if (exactHead != null) {
                return FragmentSuggestions(
                    relativeStart = prefix.length,
                    replaceLength = 0,
                    suggestions = listOf("[")
                )
            }

            val headWithTrailingGarbage = baseSelectors.firstOrNull { prefix.startsWith(it, ignoreCase = true) }
            if (headWithTrailingGarbage != null) {
                return FragmentSuggestions(
                    relativeStart = headWithTrailingGarbage.length,
                    replaceLength = prefix.length - headWithTrailingGarbage.length,
                    suggestions = listOf("[")
                )
            }

            val matched = baseSelectors
                .filter { it.startsWith(prefix, ignoreCase = true) }
                .sortedWith(String.CASE_INSENSITIVE_ORDER)
            if (matched.isEmpty()) return null
            return FragmentSuggestions(
                relativeStart = 0,
                replaceLength = prefix.length,
                suggestions = matched
            )
        }

        val selectorHead = prefix.substring(0, bracketIndex)
        if (selectorHead !in baseSelectors) return null
        val bodyPrefix = prefix.substring(bracketIndex + 1)
        if (bodyPrefix.contains(']')) return null

        val parts = splitTopLevel(bodyPrefix, ',') ?: return null
        if (parts.isEmpty()) return null

        val activeRaw = parts.last()
        val committed = parts.dropLast(1)
            .map { it.trim() }
            .filter { it.isNotEmpty() }
        val usedStates = collectUsedStates(committed)
        val active = activeRaw.trim()

        if (!active.contains('=')) {
            val keyPrefix = active.lowercase()
            val availableKeys = optionKeys.filter { isOptionStillApplicable(it, usedStates) }
            val suggestions = availableKeys
                .filter { it.startsWith(keyPrefix, ignoreCase = true) }
                .map { "$it=" }
                .distinct()
                .sortedWith(String.CASE_INSENSITIVE_ORDER)
            val replaceStart = prefix.lastIndexOf(',').let { if (it >= 0) it + 1 else bracketIndex + 1 }
            return FragmentSuggestions(
                relativeStart = replaceStart,
                replaceLength = prefix.length - replaceStart,
                suggestions = suggestions
            )
        }

        val key = active.substringBefore('=').trim().lowercase()
        if (key.isEmpty()) return null
        val valuePrefix = active.substringAfter('=').trim()
        val suggestions = when (key) {
            "type" -> suggestTypeValues(valuePrefix)
            "sort" -> suggestSimpleValues(sortValues, valuePrefix)
            "gamemode" -> {
                val values = gamemodeValues + gamemodeValues.map { "!$it" }
                suggestSimpleValues(values, valuePrefix)
            }
            "limit" -> suggestSimpleValues(limitValues, valuePrefix)
            "name", "team", "tag", "predicate" -> suggestTextualValues(valuePrefix)
            "distance", "level" -> suggestSimpleValues(listOf("..10", "5..", "0..100", "0"), valuePrefix)
            "x", "y", "z", "dx", "dy", "dz", "x_rotation", "y_rotation" -> {
                suggestSimpleValues(listOf("-100", "0", "64", "100"), valuePrefix)
            }
            "scores", "advancements", "nbt" -> suggestStructuredValues(valuePrefix)
            else -> emptyList()
        }
        val replaceStart = prefix.lastIndexOf('=') + 1
        return FragmentSuggestions(
            relativeStart = replaceStart,
            replaceLength = prefix.length - replaceStart,
            suggestions = suggestions
        )
    }

    private fun buildBase(selectorHead: String, committed: List<String>): String {
        return buildString {
            append(selectorHead).append('[')
            if (committed.isNotEmpty()) {
                append(committed.joinToString(","))
                append(',')
            }
        }
    }

    private fun parseOptionKey(optionToken: String): String? {
        val eqIndex = optionToken.indexOf('=')
        if (eqIndex <= 0) return null
        return optionToken.substring(0, eqIndex).trim().lowercase().ifEmpty { null }
    }

    private fun parseOptionValue(optionToken: String): String? {
        val eqIndex = optionToken.indexOf('=')
        if (eqIndex <= 0 || eqIndex >= optionToken.length - 1) return null
        return optionToken.substring(eqIndex + 1).trim()
    }

    private data class UsedState(
        var usedCount: Int = 0,
        var hasInverted: Boolean = false,
        var hasNonInverted: Boolean = false
    )

    private fun collectUsedStates(committed: List<String>): Map<String, UsedState> {
        val result = LinkedHashMap<String, UsedState>()
        for (token in committed) {
            val key = parseOptionKey(token) ?: continue
            val value = parseOptionValue(token).orEmpty()
            val state = result.getOrPut(key) { UsedState() }
            state.usedCount += 1
            if (value.startsWith("!")) state.hasInverted = true else state.hasNonInverted = true
        }
        return result
    }

    private fun isOptionStillApplicable(key: String, usedStates: Map<String, UsedState>): Boolean {
        val state = usedStates[key] ?: return true
        return when (key) {
            "tag", "nbt", "predicate" -> true
            // Vanilla allows additional negated values, but once a positive one is set it becomes inapplicable.
            "name", "gamemode", "team", "type" -> !state.hasNonInverted
            else -> false
        }
    }

    private fun appendValueCompletions(
        output: MutableList<String>,
        base: String,
        key: String,
        values: List<String>
    ) {
        for (value in values) {
            val pair = "$key=$value"
            output += "${base}${pair}]"
        }
    }

    private fun suggestSimpleValues(values: List<String>, prefix: String): List<String> {
        return values
            .filter { it.startsWith(prefix, ignoreCase = true) }
            .distinct()
    }

    private fun suggestTextualValues(prefix: String): List<String> {
        if (prefix.isEmpty()) return listOf("", "!")
        return listOf(prefix)
            .filter { it.startsWith(prefix, ignoreCase = true) }
            .distinct()
    }

    private fun suggestStructuredValues(prefix: String): List<String> {
        val templates = listOf("{}", "{foo=1}", "{example=true}")
        return templates
            .filter { prefix.isEmpty() || it.startsWith(prefix, ignoreCase = true) }
            .distinct()
    }

    private fun suggestTypeValues(valuePrefix: String): List<String> {
        val negate = valuePrefix.startsWith("!")
        val prefixCore = if (negate) valuePrefix.substring(1) else valuePrefix
        val namespaceRequested = prefixCore.contains(':')

        val baseValues = if (namespaceRequested) {
            entityTypeValues
        } else {
            entityTypeValues.map { it.removePrefix("minecraft:") }
        }

        val signedValues = if (negate) {
            baseValues.map { "!$it" }
        } else {
            baseValues + baseValues.map { "!$it" }
        }

        return signedValues
            .filter { it.startsWith(valuePrefix, ignoreCase = true) }
            .distinct()
            .sortedWith(String.CASE_INSENSITIVE_ORDER)
    }

    private fun splitTopLevel(value: String, delimiter: Char): List<String>? {
        val result = ArrayList<String>()
        var inSingle = false
        var inDouble = false
        var escaped = false
        var curly = 0
        var square = 0
        var round = 0
        var start = 0
        for (index in value.indices) {
            val ch = value[index]
            if (escaped) {
                escaped = false
                continue
            }
            when (ch) {
                '\\' -> if (inSingle || inDouble) escaped = true
                '\'' -> if (!inDouble) inSingle = !inSingle
                '"' -> if (!inSingle) inDouble = !inDouble
                '{' -> if (!inSingle && !inDouble) curly++
                '}' -> if (!inSingle && !inDouble) curly--
                '[' -> if (!inSingle && !inDouble) square++
                ']' -> if (!inSingle && !inDouble) square--
                '(' -> if (!inSingle && !inDouble) round++
                ')' -> if (!inSingle && !inDouble) round--
                delimiter -> if (!inSingle && !inDouble && curly == 0 && square == 0 && round == 0) {
                    result += value.substring(start, index)
                    start = index + 1
                }
            }
            if (curly < 0 || square < 0 || round < 0) return null
        }
        if (inSingle || inDouble || curly != 0 || square != 0 || round != 0) return null
        result += value.substring(start)
        return result
    }
}
