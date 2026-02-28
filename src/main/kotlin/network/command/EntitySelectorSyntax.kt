package org.macaroon3145.network.command

object EntitySelectorSyntax {
    data class Option(
        val key: String,
        val value: String
    )

    data class Parsed(
        val selectorType: Char,
        val options: List<Option>
    ) {
        fun firstValue(key: String): String? = options.firstOrNull { it.key == key }?.value
        fun values(key: String): List<String> = options.filter { it.key == key }.map { it.value }
    }

    fun parse(raw: String): Parsed? {
        if (raw.length < 2 || raw[0] != '@') return null
        val selectorType = raw[1]
        if (selectorType !in setOf('p', 'a', 'r', 's', 'n', 'e')) return null
        if (raw.length == 2) return Parsed(selectorType = selectorType, options = emptyList())

        if (raw.length < 4 || raw[2] != '[' || raw.last() != ']') return null
        val body = raw.substring(3, raw.length - 1).trim()
        if (body.isEmpty()) return Parsed(selectorType = selectorType, options = emptyList())

        val parts = splitTopLevel(body, ',') ?: return null
        val options = ArrayList<Option>(parts.size)
        for (part in parts) {
            val entry = parseOption(part) ?: return null
            options += entry
        }
        return Parsed(selectorType = selectorType, options = options)
    }

    fun normalizeEntityType(raw: String): String {
        val trimmed = raw.trim()
        if (trimmed.isEmpty()) return trimmed
        val negated = trimmed.startsWith("!")
        val token = if (negated) trimmed.substring(1) else trimmed
        if (token.isEmpty()) return trimmed
        val normalized = if (':' in token) token.lowercase() else "minecraft:${token.lowercase()}"
        return if (negated) "!$normalized" else normalized
    }

    private fun parseOption(raw: String): Option? {
        val trimmed = raw.trim()
        if (trimmed.isEmpty()) return null

        val eqIndex = findTopLevelEquals(trimmed)
        if (eqIndex <= 0 || eqIndex >= trimmed.length - 1) return null
        val key = trimmed.substring(0, eqIndex).trim()
        val value = trimmed.substring(eqIndex + 1).trim()
        if (key.isEmpty() || value.isEmpty()) return null
        return Option(key = key, value = value)
    }

    private fun findTopLevelEquals(value: String): Int {
        var inSingle = false
        var inDouble = false
        var escaped = false
        var curly = 0
        var square = 0
        var round = 0
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
                '=' -> if (!inSingle && !inDouble && curly == 0 && square == 0 && round == 0) return index
            }
            if (curly < 0 || square < 0 || round < 0) return -1
        }
        if (inSingle || inDouble || curly != 0 || square != 0 || round != 0) return -1
        return -1
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
                delimiter -> {
                    if (!inSingle && !inDouble && curly == 0 && square == 0 && round == 0) {
                        result += value.substring(start, index)
                        start = index + 1
                    }
                }
            }
            if (curly < 0 || square < 0 || round < 0) return null
        }
        if (inSingle || inDouble || curly != 0 || square != 0 || round != 0) return null
        result += value.substring(start)
        return result
    }
}
