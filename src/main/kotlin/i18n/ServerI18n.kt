package org.macaroon3145.i18n

import com.google.gson.JsonParser
import org.macaroon3145.network.handler.PlayPackets
import java.io.InputStreamReader
import java.util.Locale
import java.util.concurrent.atomic.AtomicReference

object ServerI18n {
    private const val ANSI_RESET = "\u001B[0m"
    private const val ANSI_GRAY = "\u001B[39m"
    private const val ANSI_RED = "\u001B[91m"
    private const val ANSI_GREEN = "\u001B[92m"
    private const val ANSI_YELLOW = "\u001B[93m"
    private const val ANSI_BLUE = "\u001B[94m"
    private const val ANSI_MAGENTA = "\u001B[95m"
    private const val ANSI_CYAN = "\u001B[96m"
    private val catalogsRef = AtomicReference<Map<String, Map<String, String>>>(emptyMap())

    enum class Color(val ansi: String) {
        GRAY(ANSI_GRAY),
        RED(ANSI_RED),
        GREEN(ANSI_GREEN),
        YELLOW(ANSI_YELLOW),
        BLUE(ANSI_BLUE),
        MAGENTA(ANSI_MAGENTA),
        CYAN(ANSI_CYAN)
    }

    data class StyledArg(
        val value: String,
        val color: Color? = null
    )

    fun initialize() {
        val loaded = linkedMapOf<String, Map<String, String>>()
        loaded["en"] = loadCatalog("en")
        loaded["ko"] = loadCatalog("ko")
        catalogsRef.set(loaded)
    }

    private fun loadCatalog(code: String): Map<String, String> {
        val path = "lang/$code.json"
        val stream = Thread.currentThread().contextClassLoader.getResourceAsStream(path) ?: return emptyMap()
        stream.use { input ->
            InputStreamReader(input, Charsets.UTF_8).use { reader ->
                val json = JsonParser.parseReader(reader).asJsonObject
                val out = HashMap<String, String>(json.size())
                for ((key, value) in json.entrySet()) {
                    if (value.isJsonPrimitive && value.asJsonPrimitive.isString) {
                        out[key] = value.asString
                    }
                }
                return out
            }
        }
    }

    private fun activeCatalog(): Map<String, String> {
        val catalogs = catalogsRef.get()
        if (catalogs.isEmpty()) {
            initialize()
            return activeCatalog()
        }
        val lang = Locale.getDefault().language.lowercase(Locale.ROOT)
        return catalogs[lang] ?: catalogs["en"].orEmpty()
    }

    fun translate(key: String, args: List<String> = emptyList()): String {
        val template = activeCatalog()[key] ?: key
        return try {
            String.format(Locale.getDefault(), template, *args.toTypedArray())
        } catch (_: Throwable) {
            if (args.isEmpty()) template else "$template ${args.joinToString(" ")}"
        }
    }

    fun tr(key: String, vararg args: String): String = translate(key, args.toList())

    fun log(key: String, vararg args: String) {
        val message = tr(key, *args)
        val color = colorForKey(key)
        if (color == null) {
            println(message)
        } else {
            println("$color$message$ANSI_RESET")
        }
    }

    fun logStyled(key: String, vararg args: StyledArg) {
        val rendered = args.map { arg ->
            if (arg.color == null) {
                arg.value
            } else {
                "${arg.color.ansi}${arg.value}$ANSI_RESET"
            }
        }
        val message = translate(key, rendered)
        val color = colorForKey(key)
        if (color == null) {
            println(message)
        } else {
            println("$color$message$ANSI_RESET")
        }
    }

    fun style(text: String, color: Color? = null): StyledArg = StyledArg(text, color)

    fun label(key: String): StyledArg = StyledArg(tr(key), Color.GRAY)

    fun punct(text: String): StyledArg = StyledArg(text, Color.GRAY)

    fun path(value: String): StyledArg = StyledArg(value, Color.CYAN)

    fun typed(value: Any?): StyledArg {
        return when (value) {
            null -> StyledArg("null", Color.GRAY)
            is Boolean -> StyledArg(value.toString(), Color.YELLOW)
            is Byte, is Short, is Int, is Long, is Float, is Double -> StyledArg(value.toString(), Color.BLUE)
            else -> StyledArg(value.toString(), Color.GREEN)
        }
    }

    fun logCustom(vararg parts: StyledArg) {
        val line = buildString {
            for (part in parts) {
                if (part.color == null) {
                    append(part.value)
                } else {
                    append(part.color.ansi).append(part.value).append(ANSI_RESET)
                }
            }
        }
        println(line)
    }

    private fun colorForKey(key: String): String? {
        return when {
            key.contains(".warn.") -> ANSI_YELLOW
            key == "aerogel.log.done" -> ANSI_GREEN
            key.endsWith(".console.prefixed_error") -> ANSI_RED
            key.endsWith(".console.prefixed_warn") -> ANSI_YELLOW
            key.endsWith(".console.prefixed") -> ANSI_GREEN
            else -> null
        }
    }

    fun componentToText(component: PlayPackets.ChatComponent): String {
        return when (component) {
            is PlayPackets.ChatComponent.Text -> buildString {
                append(component.text)
                for (child in component.extra) append(componentToText(child))
            }
            is PlayPackets.ChatComponent.Translate -> {
                val args = component.args.map { componentToText(it) }
                val root = translate(component.key, args)
                if (component.extra.isEmpty()) {
                    root
                } else {
                    buildString {
                        append(root)
                        for (child in component.extra) append(componentToText(child))
                    }
                }
            }
        }
    }
}
