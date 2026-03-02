package org.macaroon3145

import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.Channel
import io.netty.channel.ChannelInitializer
import io.netty.channel.ChannelOption
import io.netty.channel.EventLoopGroup
import io.netty.channel.ServerChannel
import io.netty.channel.epoll.Epoll
import io.netty.channel.epoll.EpollEventLoopGroup
import io.netty.channel.epoll.EpollServerSocketChannel
import io.netty.channel.kqueue.KQueue
import io.netty.channel.kqueue.KQueueEventLoopGroup
import io.netty.channel.kqueue.KQueueServerSocketChannel
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import org.jline.reader.Candidate
import org.jline.reader.Completer
import org.jline.reader.EndOfFileException
import org.jline.reader.LineReaderBuilder
import org.jline.reader.UserInterruptException
import org.jline.terminal.TerminalBuilder
import org.macaroon3145.config.ServerConfig
import org.macaroon3145.i18n.ServerI18n
import org.macaroon3145.network.handler.HandshakeHandler
import org.macaroon3145.network.handler.ChunkStreamingService
import org.macaroon3145.network.handler.PlayPackets
import org.macaroon3145.network.handler.PlayerSessionManager
import org.macaroon3145.network.codec.BlockStateRegistry
import org.macaroon3145.network.codec.BlockEntityTypeRegistry
import org.macaroon3145.network.codec.ItemBlockStateRegistry
import org.macaroon3145.network.codec.RegistryCodec
import org.macaroon3145.network.command.EntitySelectorCompletions
import org.macaroon3145.network.transcoder.MinecraftVarIntFrameDecoder
import org.macaroon3145.network.transcoder.MinecraftVarIntFrameEncoder
import org.macaroon3145.perf.GameLoop
import org.macaroon3145.perf.PerformanceMonitor
import org.macaroon3145.world.DroppedItemSystem
import org.macaroon3145.world.BlockCollisionRegistry
import org.macaroon3145.world.EntityHitboxRegistry
import org.macaroon3145.world.World
import org.macaroon3145.world.WorldManager
import org.macaroon3145.world.VanillaMiningRules
import java.nio.file.Files
import java.nio.file.Path
import java.io.BufferedReader
import java.io.ByteArrayOutputStream
import java.io.InputStreamReader
import java.io.OutputStream
import java.io.PrintStream
import java.nio.charset.StandardCharsets
import java.util.Properties
import java.util.concurrent.Executors
import java.util.logging.Formatter
import java.util.logging.LogManager
import java.util.logging.LogRecord
import kotlin.concurrent.thread
import kotlin.random.Random

object Aerogel {
    const val VERSION = "1.21.11"
    const val PORT = 25565
}

private data class EventLoopSelection(
    val name: String,
    val bossGroup: EventLoopGroup,
    val workerGroup: EventLoopGroup,
    val serverChannelClass: Class<out ServerChannel>
)

fun main() {
    val mainStartNanos = System.nanoTime()
    disableOptionalUnsafePaths()
    installSafeDefaultUncaughtHandler()
    val props = Properties()
    val externalConfigPath = Path.of("aerogel.properties")
    ensureExternalConfigFile(externalConfigPath)
    if (Files.exists(externalConfigPath)) {
        Files.newBufferedReader(externalConfigPath, StandardCharsets.UTF_8).use { reader ->
            props.load(reader)
        }
    } else {
        Thread.currentThread().contextClassLoader.getResourceAsStream("aerogel.properties")?.use { input ->
            InputStreamReader(input, StandardCharsets.UTF_8).use { reader ->
                props.load(reader)
            }
        }
    }
    val parsedChunkWorkerThreads = props.getProperty("chunk-worker-threads")?.toIntOrNull()
        ?.coerceAtLeast(0)
        ?: 0
    val worldSeeds = resolveWorldSeeds(props)
    val defaultWorld = props.getProperty("world.default")
    val sidecarLevelSeed = worldSeeds["minecraft:overworld"] ?: worldSeeds.values.firstOrNull()
    if (sidecarLevelSeed != null) {
        System.setProperty("aerogel.folia.sidecar.level-seed", sidecarLevelSeed.toString())
    }
    worldSeeds["minecraft:overworld"]?.let { System.setProperty("aerogel.folia.seed.overworld", it.toString()) }
    worldSeeds["minecraft:the_nether"]?.let { System.setProperty("aerogel.folia.seed.the_nether", it.toString()) }
    worldSeeds["minecraft:the_end"]?.let { System.setProperty("aerogel.folia.seed.the_end", it.toString()) }
    ensureFoliaRuntimeReady(parsedChunkWorkerThreads)
    // Folia sidecar bootstrap may emit noisy startup output.
    installJvmStderrWarningFilter()
    installStdoutInfoFilter()
    configureJulFormatting()
    ServerConfig.onlineMode = props.getProperty("online-mode")?.toBooleanStrictOrNull() ?: true
    ServerConfig.maxTps = props.getProperty("max-tps")?.toDoubleOrNull()
        ?.takeIf { it == -1.0 || it > 0.0 }
        ?: 20.0
    ServerConfig.timeScale = (
        props.getProperty("time-scale")
            ?: props.getProperty("dropped-item-physics-time-scale")
        )?.toDoubleOrNull()
        ?.takeIf { it > 0.0 }
        ?: 1.0
    ServerConfig.playerTimeScale = props.getProperty("player-time-scale")?.toDoubleOrNull()
        ?.takeIf { it > 0.0 }
        ?.coerceAtMost(1.0)
        ?: 1.0
    ServerConfig.maxViewDistanceChunks = props.getProperty("max-view-distance-chunks")?.toIntOrNull()
        ?.coerceIn(2, 32)
        ?: 32
    ServerConfig.maxSimulationDistanceChunks = props.getProperty("max-simulation-distance-chunks")?.toIntOrNull()
        ?.coerceIn(2, 32)
        ?: 16
    ServerConfig.chunkWorkerThreads = parsedChunkWorkerThreads
    ServerConfig.compressionThreshold = props.getProperty("compression-threshold")?.toIntOrNull()
        ?.takeIf { it == -1 || it >= 0 }
        ?: 1024
    ServerConfig.compressionLevel = props.getProperty("compression-level")?.toIntOrNull()
        ?.coerceIn(0, 9)
        ?: 1
    ServerConfig.compressionChunkLevel = props.getProperty("compression-chunk-level")?.toIntOrNull()
        ?.coerceIn(-1, 9)
        ?: 1
    ServerConfig.setGameMode(parseGameMode(props.getProperty("default-gamemode")))
    ServerI18n.initialize()
    WorldManager.bootstrap(worldSeeds = worldSeeds, defaultWorld = defaultWorld)
    warmupPickBlockLookups()
    if (ServerConfig.compressionThreshold >= 0 && ServerConfig.compressionChunkLevel < 0) {
        ServerI18n.log("aerogel.log.warn.chunk_compression_disabled")
    }
    val eventLoops = selectEventLoops()
    ServerI18n.logCustom(
        ServerI18n.style("· ", ServerI18n.Color.GRAY),
        ServerI18n.label("aerogel.label.netty_transport"),
        ServerI18n.punct(": "),
        ServerI18n.style(eventLoops.name, ServerI18n.Color.MAGENTA)
    )
    var serverChannel: Channel? = null
    DebugConsole.withSpinner(
        progressMessage = ServerI18n.tr("aerogel.log.tcp.preparing", Aerogel.PORT.toString()),
        doneMessage = ServerI18n.tr("aerogel.log.tcp.prepared", Aerogel.PORT.toString())
    ) {
        serverChannel = ServerBootstrap().group(eventLoops.bossGroup, eventLoops.workerGroup)
            .channel(eventLoops.serverChannelClass)
            .childOption(ChannelOption.TCP_NODELAY, true)
            .childOption(ChannelOption.SO_KEEPALIVE, true)
            .childHandler(object : ChannelInitializer<SocketChannel>() {
                override fun initChannel(ch: SocketChannel) {
                    val pipeline = ch.pipeline()
                    pipeline.addLast("frameDecoder", MinecraftVarIntFrameDecoder())
                    pipeline.addLast("frameEncoder", MinecraftVarIntFrameEncoder())
                    pipeline.addLast("handshakeHandler", HandshakeHandler())
                }
            }).bind(Aerogel.PORT).sync().channel()
    }
    serverChannel?.let {
        ServerLifecycle.registerNetworking(it, eventLoops.bossGroup, eventLoops.workerGroup)
    }
    Runtime.getRuntime().addShutdownHook(
        Thread(
            { ServerLifecycle.stopServerForShutdownHook() },
            "aerogel-shutdown-hook"
        )
    )
    val elapsedNanos = (System.nanoTime() - mainStartNanos) - StartupTiming.interactiveInputNanos()
    val elapsedMillis = elapsedNanos.coerceAtLeast(0L) / 1_000_000.0
    ServerI18n.logCustom(
        ServerI18n.style(ServerI18n.tr("aerogel.label.done"), ServerI18n.Color.GREEN),
        ServerI18n.style(" (", ServerI18n.Color.GREEN),
        ServerI18n.style(String.format("%.2f", elapsedMillis / 1000.0), ServerI18n.Color.GREEN),
        ServerI18n.style("${ServerI18n.tr("aerogel.unit.seconds.short")})", ServerI18n.Color.GREEN)
    )
    startConsoleCommandLoop()
    PerformanceMonitor.start()
    GameLoop.start()
}

private fun warmupPickBlockLookups() {
    runCatching {
        DebugConsole.withSpinner(
            progressMessage = ServerI18n.tr("aerogel.log.cache.preparing"),
            doneMessage = ServerI18n.tr("aerogel.log.cache.prepared")
        ) {
            val tasks = listOf<() -> Unit>(
                { ItemBlockStateRegistry.prewarm() },
                { BlockStateRegistry.prewarm() },
                { BlockEntityTypeRegistry.prewarm() },
                { RegistryCodec.prewarm() },
                { VanillaMiningRules.prewarm() },
                { EntitySelectorCompletions.prewarm() },
                { PlayPackets.prewarm() },
                { World.prewarm() },
                { DroppedItemSystem.prewarm() },
                { BlockCollisionRegistry.prewarm() },
                { EntityHitboxRegistry.prewarm() },
                { PlayerSessionManager.prewarm() },
                { PlayerSessionManager.prewarmFluidDependentDropCache() },
                { PlayerSessionManager.prewarmFluidDependentRuntime() }
            )
            Executors.newVirtualThreadPerTaskExecutor().use { executor ->
                val futures = tasks.map { task ->
                    executor.submit<Unit> {
                        runCatching { task() }
                        Unit
                    }
                }
                for (future in futures) {
                    runCatching { future.get() }
                }
            }
        }
    }
}

private fun resolveWorldSeeds(props: Properties): LinkedHashMap<String, Long> {
    val worldSeeds = linkedMapOf<String, Long>()
    val legacyDefaultSeed = props.getProperty("world.seed")?.toLongOrNull()
    if (legacyDefaultSeed != null) {
        worldSeeds["minecraft:overworld"] = legacyDefaultSeed
    }
    for ((rawKey, rawValue) in props) {
        val key = rawKey.toString()
        if (!key.startsWith("world.seed.")) continue
        val worldKey = key.removePrefix("world.seed.")
        val value = rawValue.toString().trim()
        val directSeed = value.toLongOrNull()
        if (directSeed != null) {
            worldSeeds[worldKey] = directSeed
            continue
        }

        // Java .properties treats ':' as key/value delimiter, so
        // `world.seed.minecraft:overworld=1234` becomes:
        //   key=world.seed.minecraft, value=overworld=1234
        // Recover that common unescaped form.
        val split = value.indexOf('=')
        if (split <= 0 || split >= value.lastIndex) continue
        val suffix = value.substring(0, split).trim()
        val recoveredSeed = value.substring(split + 1).trim().toLongOrNull() ?: continue
        worldSeeds["$worldKey:$suffix"] = recoveredSeed
    }

    val builtinWorldKeys = listOf(
        "minecraft:overworld",
        "minecraft:the_nether",
        "minecraft:the_end"
    )
    for (worldKey in builtinWorldKeys) {
        worldSeeds.computeIfAbsent(worldKey) { Random.nextLong() }
    }
    return worldSeeds
}

private fun ensureExternalConfigFile(path: Path) {
    if (Files.exists(path)) return
    Thread.currentThread().contextClassLoader.getResourceAsStream("aerogel.properties")?.use { input ->
        Files.newOutputStream(path).use { output ->
            input.copyTo(output)
        }
    }
}

private fun disableOptionalUnsafePaths() {
    // Prevent JOML from using sun.misc.Unsafe path that emits terminal deprecation warnings.
    if (System.getProperty("joml.nounsafe").isNullOrEmpty()) {
        System.setProperty("joml.nounsafe", "true")
    }
}

private fun installJvmStderrWarningFilter() {
    System.setErr(createFilteredPrintStream(System.err) { line ->
        shouldSuppressJvmWarningLine(line)
    })
}

private fun installStdoutInfoFilter() {
    System.setOut(createFilteredPrintStream(System.out) { line ->
        false
    })
}

private fun createFilteredPrintStream(
    original: PrintStream,
    suppress: (String) -> Boolean
): PrintStream {
    val filtered = object : OutputStream() {
        private val localBuffer = ThreadLocal.withInitial { ByteArrayOutputStream(256) }

        override fun write(b: Int) {
            val buffer = localBuffer.get()
            if (b == '\n'.code) {
                flushLine(buffer)
            } else if (b != '\r'.code) {
                buffer.write(b)
            }
        }

        override fun flush() {
            val buffer = localBuffer.get()
            if (buffer.size() > 0) {
                flushLine(buffer)
            }
            original.flush()
        }

        private fun flushLine(buffer: ByteArrayOutputStream) {
            val line = buffer.toString(StandardCharsets.UTF_8)
            buffer.reset()
            if (suppress(line)) return
            original.println(line)
        }
    }
    return PrintStream(filtered, true, StandardCharsets.UTF_8)
}

private fun shouldSuppressJvmWarningLine(line: String): Boolean {
    if (!line.startsWith("WARNING: ")) return false
    return line.contains("A terminally deprecated method in sun.misc.Unsafe has been called") ||
        line.contains("sun.misc.Unsafe::objectFieldOffset has been called") ||
        line.contains("Please consider reporting this to the maintainers") ||
        line.contains("sun.misc.Unsafe::objectFieldOffset will be removed in a future release")
}

private fun stripAnsi(value: String): String {
    return value.replace(Regex("\\u001B\\[[;\\d]*m"), "")
}

private fun configureJulFormatting() {
    val root = LogManager.getLogManager().getLogger("")
    for (handler in root.handlers) {
        handler.formatter = object : Formatter() {
            override fun format(record: LogRecord): String {
                return buildString {
                    append(record.message)
                    append('\n')
                }
            }
        }
    }
}

private fun startConsoleCommandLoop() {
    thread(name = "aerogel-console-command-loop", isDaemon = false) {
        val usedJLine = runCatching {
            runJLineConsoleLoop()
        }.isSuccess
        if (!usedJLine) {
            runBufferedConsoleLoop()
        }
    }
}

private fun runJLineConsoleLoop() {
    val terminal = TerminalBuilder.builder()
        .system(true)
        .build()
    val completer = Completer { _, parsedLine, candidates ->
        val inputUptoCursor = parsedLine.line().take(parsedLine.cursor())
        val completions = PlayerSessionManager.consoleCommandCompletions(inputUptoCursor)
        for (completion in completions) {
            candidates.add(Candidate(completion))
        }
    }
    val reader = LineReaderBuilder.builder()
        .terminal(terminal)
        .completer(completer)
        .build()
    while (true) {
        val line = try {
            reader.readLine("")
        } catch (_: UserInterruptException) {
            continue
        } catch (_: EndOfFileException) {
            break
        }
        val command = line.trim()
        if (command.isEmpty()) continue
        PlayerSessionManager.submitConsoleCommand(command)
    }
}

private fun runBufferedConsoleLoop() {
    val reader = BufferedReader(InputStreamReader(System.`in`))
    while (true) {
        val line = try {
            reader.readLine()
        } catch (_: Throwable) {
            break
        } ?: break
        val command = line.trim()
        if (command.isEmpty()) continue
        PlayerSessionManager.submitConsoleCommand(command)
    }
}

private fun selectEventLoops(): EventLoopSelection {
    if (Epoll.isAvailable()) {
        return EventLoopSelection(
            name = "epoll",
            bossGroup = EpollEventLoopGroup(1),
            workerGroup = EpollEventLoopGroup(),
            serverChannelClass = EpollServerSocketChannel::class.java
        )
    }
    if (KQueue.isAvailable()) {
        return EventLoopSelection(
            name = "kqueue",
            bossGroup = KQueueEventLoopGroup(1),
            workerGroup = KQueueEventLoopGroup(),
            serverChannelClass = KQueueServerSocketChannel::class.java
        )
    }
    return EventLoopSelection(
        name = "nio",
        bossGroup = NioEventLoopGroup(1),
        workerGroup = NioEventLoopGroup(),
        serverChannelClass = NioServerSocketChannel::class.java
    )
}

private fun parseGameMode(raw: String?): Int {
    val normalized = raw?.trim()?.lowercase() ?: return 0
    return when (normalized) {
        "0", "survival" -> 0
        "1", "creative" -> 1
        "2", "adventure" -> 2
        "3", "spectator" -> 3
        else -> 1
    }
}

private fun logThreadCrashSafely(thread: Thread, error: Throwable) {
    runCatching {
        System.err.println("[Aerogel] Uncaught exception from ${thread.name}: ${error.javaClass.name}: ${error.message}")
    }
    if (error is StackOverflowError) {
        runCatching {
            System.err.println("[Aerogel] StackOverflowError occurred. Full stack trace skipped to prevent recursive overflow.")
        }
        return
    }
    runCatching {
        error.printStackTrace(System.err)
    }
}

private fun installSafeDefaultUncaughtHandler() {
    Thread.setDefaultUncaughtExceptionHandler { thread, error ->
        logThreadCrashSafely(thread, error)
    }
}
