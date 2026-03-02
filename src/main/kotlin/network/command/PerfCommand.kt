package org.macaroon3145.network.command

import org.macaroon3145.i18n.ServerI18n
import org.macaroon3145.network.handler.PlayPackets
import org.macaroon3145.network.handler.PlayerSession
import org.macaroon3145.perf.PerformanceMonitor
import org.macaroon3145.world.WorldManager
import kotlin.math.abs

object PerfCommand : Command {
    private const val MAX_CHUNK_LINES = 6
    private const val MIN_LISTED_MSPT = 1.0e-9
    private const val ANSI_RESET = "\u001B[0m"
    private const val ANSI_GRAY = "\u001B[90m"
    private const val ANSI_WHITE = "\u001B[97m"

    private data class ChunkLine(
        val worldKey: String,
        val chunkX: Int,
        val chunkZ: Int,
        val tps: Double,
        val mspt: Double,
        val breakdownMs: Map<String, Double>
    )

    override fun execute(context: CommandContext, sender: PlayerSession?, args: List<String>) {
        if (!context.isOperator(sender)) {
            val input = if (args.isEmpty()) "perf" else "perf ${args.joinToString(" ")}"
            context.sendUnknownCommandWithContext(sender, input)
            return
        }

        if (args.size > 1) {
            context.sendSourceTranslationWithContext(
                sender,
                "command.unknown.argument",
                buildInput(args),
                errorStart = argumentStartIndex(args, 1)
            )
            return
        }

        val selectedWorld = if (args.isEmpty()) null else resolveWorldKey(context, args[0])
        if (selectedWorld != null && WorldManager.world(selectedWorld) == null) {
            context.sendSourceTranslationWithContext(
                sender,
                "argument.dimension.invalid",
                buildInput(args),
                errorStart = argumentStartIndex(args, 0),
                PlayPackets.ChatComponent.Text(selectedWorld)
            )
            return
        }

        sendOverallSection(context, sender)
        sendChunkSection(context, sender, selectedWorld)
    }

    private fun sendOverallSection(context: CommandContext, sender: PlayerSession?) {
        val tps = PerformanceMonitor.tpsString()
        val mspt = PerformanceMonitor.msptString()
        if (sender == null) {
            val overallTpsLabel = ServerI18n.tr("aerogel.commands.perf.label.overall_tps")
            val overallMsptLabel = ServerI18n.tr("aerogel.commands.perf.label.overall_mspt")
            context.sendConsoleMessage("$ANSI_GRAY$overallTpsLabel: $ANSI_WHITE$tps$ANSI_RESET")
            context.sendConsoleMessage("$ANSI_GRAY$overallMsptLabel: $ANSI_WHITE${mspt}ms$ANSI_RESET")
            return
        }
        val locale = sender.locale
        val overallTpsLabel = ServerI18n.trFor(locale, "aerogel.commands.perf.label.overall_tps")
        val overallMsptLabel = ServerI18n.trFor(locale, "aerogel.commands.perf.label.overall_mspt")
        context.sendMessage(sender, "§7$overallTpsLabel: §f$tps")
        context.sendMessage(sender, "§7$overallMsptLabel: §f${mspt}ms")
    }

    private fun sendChunkSection(context: CommandContext, sender: PlayerSession?, selectedWorld: String?) {
        val worldKeys = if (selectedWorld != null) {
            listOf(selectedWorld)
        } else {
            context.worldKeys().distinct()
        }
        val topChunks = collectTopChunks(worldKeys)

        if (sender == null) {
            if (topChunks.isEmpty()) {
                val header = ServerI18n.tr("aerogel.commands.perf.chunk_header")
                val none = ServerI18n.tr("aerogel.commands.perf.chunk_none")
                context.sendConsoleMessage("$ANSI_GRAY$header $ANSI_WHITE$none$ANSI_RESET")
                return
            }
            context.sendConsoleMessage("$ANSI_GRAY${ServerI18n.tr("aerogel.commands.perf.chunk_header")}$ANSI_RESET")
            for (entry in topChunks) {
                context.sendConsoleMessage(
                    "$ANSI_GRAY- $ANSI_WHITE" + ServerI18n.tr(
                        "aerogel.commands.perf.chunk_line",
                        displayWorldName(entry.worldKey),
                        entry.chunkX.toString(),
                        entry.chunkZ.toString(),
                        formatPerf(entry.tps),
                        formatPerf(entry.mspt)
                    ) + formatBreakdownSuffix(entry.breakdownMs) + ANSI_RESET
                )
            }
            return
        }

        val locale = sender.locale
        if (topChunks.isEmpty()) {
            val header = ServerI18n.trFor(locale, "aerogel.commands.perf.chunk_header")
            val none = ServerI18n.trFor(locale, "aerogel.commands.perf.chunk_none")
            context.sendMessage(sender, "§7$header §f$none")
            return
        }
        context.sendMessage(sender, "§7${ServerI18n.trFor(locale, "aerogel.commands.perf.chunk_header")}")
        for (entry in topChunks) {
            val line = ServerI18n.trFor(
                locale,
                "aerogel.commands.perf.chunk_line",
                displayWorldName(entry.worldKey),
                entry.chunkX.toString(),
                entry.chunkZ.toString(),
                formatPerf(entry.tps),
                formatPerf(entry.mspt)
            )
            context.sendMessage(sender, "§7   - §f$line${formatBreakdownSuffix(entry.breakdownMs)}")
        }
    }

    private fun collectTopChunks(worldKeys: List<String>): List<ChunkLine> {
        if (worldKeys.isEmpty()) return emptyList()
        val candidates = ArrayList<ChunkLine>(MAX_CHUNK_LINES * worldKeys.size.coerceAtLeast(1))
        for (worldKey in worldKeys) {
            val world = WorldManager.world(worldKey) ?: continue
            val topInWorld = world.topChunkStatsByEwmaMspt(limit = MAX_CHUNK_LINES, minMspt = MIN_LISTED_MSPT)
            for (entry in topInWorld) {
                if (world.isChunkIdle(entry.chunkPos.x, entry.chunkPos.z)) continue
                candidates += ChunkLine(
                    worldKey = world.key,
                    chunkX = entry.chunkPos.x,
                    chunkZ = entry.chunkPos.z,
                    tps = entry.tps,
                    mspt = entry.mspt,
                    breakdownMs = entry.breakdownMs
                )
            }
        }
        if (candidates.isEmpty()) return emptyList()
        candidates.sortWith(
            compareByDescending<ChunkLine> { it.mspt }
                .thenByDescending { it.tps }
                .thenBy { it.worldKey }
                .thenBy { it.chunkX }
                .thenBy { it.chunkZ }
        )
        return if (candidates.size <= MAX_CHUNK_LINES) candidates else candidates.subList(0, MAX_CHUNK_LINES)
    }

    private fun resolveWorldKey(context: CommandContext, rawWorld: String): String {
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

    private fun formatPerf(value: Double): String {
        val clamped = if (value.isFinite()) value else 0.0
        val magnitude = abs(clamped)
        val decimals = when {
            magnitude >= 1.0 -> 2
            magnitude >= 0.1 -> 3
            magnitude >= 0.01 -> 4
            magnitude >= 0.001 -> 5
            else -> 6
        }
        return String.format("%.${decimals}f", clamped)
    }

    private fun formatBreakdownSuffix(breakdownMs: Map<String, Double>): String {
        if (breakdownMs.isEmpty()) return ""
        val significant = breakdownMs
            .asSequence()
            .filter { (_, ms) -> ms.isFinite() && ms >= 0.0005 }
            .sortedByDescending { (_, ms) -> ms }
            .take(3)
            .toList()
        if (significant.isEmpty()) return ""
        val joined = significant.joinToString(", ") { (category, ms) -> "$category=${formatPerf(ms)}ms" }
        return " [$joined]"
    }

    private fun displayWorldName(worldKey: String): String {
        if (worldKey.startsWith("minecraft:")) return worldKey.substringAfter(':')
        return worldKey
    }

    private fun buildInput(args: List<String>): String {
        return if (args.isEmpty()) "perf" else "perf ${args.joinToString(" ")}"
    }

    private fun argumentStartIndex(args: List<String>, argIndex: Int): Int {
        var index = "perf ".length
        for (i in 0 until argIndex) {
            index += args.getOrNull(i)?.length ?: 0
            index += 1
        }
        return index
    }
}
