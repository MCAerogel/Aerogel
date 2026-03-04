package org.macaroon3145.network.command

object CommandCompletionEncoding {
    private const val APPEND_MARKER = "\u0000__append__:"

    fun encodeAppend(suggestion: String): String = APPEND_MARKER + suggestion

    fun isAppendEncoded(suggestion: String): Boolean = suggestion.startsWith(APPEND_MARKER)

    fun decode(suggestion: String): String = suggestion.removePrefix(APPEND_MARKER)
}
