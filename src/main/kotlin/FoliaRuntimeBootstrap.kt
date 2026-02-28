package org.macaroon3145

import com.google.gson.JsonObject
import com.google.gson.JsonParser
import org.macaroon3145.i18n.ServerI18n
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.security.MessageDigest
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.Comparator
import java.util.Locale
import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import java.util.jar.JarEntry
import java.util.jar.JarFile
import java.util.jar.JarOutputStream
import java.util.zip.CRC32

private data class FoliaDownloadInfo(
    val version: String,
    val build: Int,
    val fileName: String,
    val sha256: String
)

private object FoliaRuntimeBootstrap {
    private const val API_BASE = "https://api.papermc.io/v2/projects/folia"
    private val runtimeDir: Path = Path.of(".aerogel-cache/folia/runtime")
    private val jarPath: Path = runtimeDir.resolve("folia-server.jar")
    private val downloadMetadataPath: Path = runtimeDir.resolve("folia-download.properties")
    private val runtimeStatePath: Path = runtimeDir.resolve("runtime-state.properties")
    private val worldDirs = listOf("world", "world_nether", "world_the_end")
    private const val embeddedPluginJarEntry = "embedded/AerogelFoliaBridge.jar"
    private const val sidecarPluginFileName = "AerogelFoliaBridge.jar"
    private const val defaultSidecarXmsMb = 512
    private const val defaultSidecarXmxMb = 1024
    private const val defaultChunkGcPeriodTicks = 200
    private val bindHost = "127.0.0.1"
    private const val bindPort = "0"
    private const val FORCED_REGION_GRID_EXPONENT = 0
    private const val downloadInfoCacheTtlMillis = 6L * 60L * 60L * 1000L

    private val httpClient: HttpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(20))
        .followRedirects(HttpClient.Redirect.NORMAL)
        .build()

    private val started = AtomicBoolean(false)
    private val restartLock = Any()
    @Volatile
    private var process: Process? = null

    fun stop() {
        val running = process
        if (running != null && running.isAlive) {
            running.destroyForcibly()
            runCatching { running.waitFor() }
        }
        process = null
        started.set(false)
    }

    fun restart(configuredChunkWorkerThreads: Int) {
        synchronized(restartLock) {
            stop()
            ensureReady(configuredChunkWorkerThreads)
        }
    }

    fun ensureReady(configuredChunkWorkerThreads: Int) {
        Files.createDirectories(runtimeDir)
        ensureEulaAccepted(runtimeDir)
        installBridgePlugin(runtimeDir)
        val chunkIpcSlots = computeChunkIpcSlots(configuredChunkWorkerThreads)
        System.setProperty("aerogel.chunk.ipc.slots", chunkIpcSlots.toString())
        val regionGridExponent = FORCED_REGION_GRID_EXPONENT
        System.setProperty("aerogel.folia.region-grid-exponent", regionGridExponent.toString())

        if (!started.compareAndSet(false, true)) {
            val running = process
            if (running?.isAlive == true) {
                awaitBridgeReady(chunkIpcSlots, running, runtimeDir.resolve("folia-sidecar.log"))
                return
            }
            started.set(false)
        }

        ensureCachedFoliaJar()

        val javaBin = Path.of(System.getProperty("java.home"), "bin", "java").toString()
        prepareSidecarServerProperties(runtimeDir)
        prepareSidecarConfigFiles(runtimeDir, chunkIpcSlots, regionGridExponent)
        prepareRuntimeStateCache(regionGridExponent)
        prepareIpcDirectory(runtimeDir)

        val appArgs = listOf(
            "--nogui",
            "--noconsole",
            "--server-ip",
            bindHost,
            "--server-port",
            bindPort
        )

        val sidecarJvmArgs = resolveSidecarJvmArgs()
        val command = ArrayList<String>(4 + sidecarJvmArgs.size + appArgs.size)
        command.add(javaBin)
        command.addAll(sidecarJvmArgs)
        command.add("-Daerogel.runtime.dir=${runtimeDir.toAbsolutePath()}")
        command.add("-Daerogel.chunk.ipc.slots=$chunkIpcSlots")
        addOptionalSystemProperty(command, "aerogel.folia.seed.overworld")
        addOptionalSystemProperty(command, "aerogel.folia.seed.the_nether")
        addOptionalSystemProperty(command, "aerogel.folia.seed.the_end")
        command.add("-DMC_DEBUG_ENABLED=true")
        command.add("-DMC_DEBUG_DONT_SAVE_WORLD=true")
        command.add("-jar")
        command.add(jarPath.toAbsolutePath().toString())
        command.addAll(appArgs)

        val logPath = runtimeDir.resolve("folia-sidecar.log")
        val startedProcess = DebugConsole.withSpinner(
            progressMessage = ServerI18n.tr("aerogel.folia.runtime.starting"),
            doneMessage = ServerI18n.tr("aerogel.folia.runtime.starting.done")
        ) {
            val process = ProcessBuilder(command)
                .directory(runtimeDir.toFile())
                .redirectErrorStream(true)
                .redirectOutput(ProcessBuilder.Redirect.to(logPath.toFile()))
                .start()

            this.process = process
            Runtime.getRuntime().addShutdownHook(Thread({
                val running = this.process ?: return@Thread
                if (running.isAlive) {
                    // Kill immediately to skip Folia's graceful world shutdown/save sequence.
                    running.destroyForcibly()
                }
            }, "aerogel-folia-sidecar-shutdown"))

            Thread.sleep(250)
            if (!process.isAlive) {
                started.set(false)
                throw IllegalStateException(
                    "Folia sidecar exited immediately (code=${process.exitValue()}). Check ${logPath.toAbsolutePath()}"
                )
            }
            process
        }

        awaitBridgeReady(chunkIpcSlots, startedProcess, logPath)
    }

    private fun prepareIpcDirectory(runtimeDir: Path) {
        val ipcDir = runtimeDir.resolve("ipc")
        Files.createDirectories(ipcDir)
        Files.newDirectoryStream(ipcDir).use { entries ->
            for (entry in entries) {
                val name = entry.fileName.toString()
                if (name == "bridge-ready" || name == "world-spawns.tsv" || name.startsWith("chunk-request-") || name.startsWith("chunk-response-")) {
                    Files.deleteIfExists(entry)
                }
            }
        }
    }

    private fun addOptionalSystemProperty(command: MutableList<String>, key: String) {
        val value = System.getProperty(key)?.trim()?.takeIf { it.isNotEmpty() } ?: return
        command.add("-D$key=$value")
    }

    private fun awaitBridgeReady(chunkIpcSlots: Int, sidecar: Process, logPath: Path) {
        val ipcDir = runtimeDir.resolve("ipc")
        val readyMarker = ipcDir.resolve("bridge-ready")
        val timeoutMillis = System.getProperty("aerogel.folia.runtime.ready-timeout-ms")
            ?.toLongOrNull()
            ?.coerceAtLeast(1L)
            ?: 120_000L
        val deadlineNanos = System.nanoTime() + timeoutMillis * 1_000_000L
        val spinner = DebugConsole.newSpinner()

        while (true) {
            val nowNanos = System.nanoTime()
            spinner.tick(
                lineBuilder = { frame, elapsedSeconds ->
                    ServerI18n.tr("aerogel.folia.runtime.waiting_progress", frame, DebugConsole.localizedElapsed(elapsedSeconds))
                },
                nowNanos = nowNanos
            )

            if (!sidecar.isAlive) {
                spinner.clear()
                started.set(false)
                val exitCode = runCatching { sidecar.exitValue() }.getOrElse { -1 }
                throw IllegalStateException(
                    "Folia sidecar exited while waiting for IPC readiness (code=$exitCode). Check ${logPath.toAbsolutePath()}"
                )
            }
            if (Files.isRegularFile(readyMarker) && hasAllIpcSlots(ipcDir, chunkIpcSlots) && hasWorldSpawnSnapshot(ipcDir)) {
                spinner.finish { doneMark, elapsedSeconds ->
                    ServerI18n.tr("aerogel.folia.runtime.started_with_elapsed", doneMark, DebugConsole.localizedElapsed(elapsedSeconds))
                }
                return
            }
            if (nowNanos >= deadlineNanos) {
                spinner.clear()
                throw IllegalStateException(
                    "Folia sidecar IPC was not ready within ${timeoutMillis}ms (marker=${readyMarker.toAbsolutePath()})"
                )
            }
            Thread.sleep(25)
        }
    }

    private fun hasAllIpcSlots(ipcDir: Path, chunkIpcSlots: Int): Boolean {
        for (slotIndex in 0 until chunkIpcSlots) {
            if (!Files.isRegularFile(ipcDir.resolve("chunk-request-$slotIndex.mmap"))) return false
            if (!Files.isRegularFile(ipcDir.resolve("chunk-response-$slotIndex.mmap"))) return false
        }
        return true
    }

    private fun hasWorldSpawnSnapshot(ipcDir: Path): Boolean {
        return Files.isRegularFile(ipcDir.resolve("world-spawns.tsv"))
    }

    private fun computeChunkIpcSlots(configuredChunkWorkerThreads: Int): Int {
        if (configuredChunkWorkerThreads > 0) return configuredChunkWorkerThreads

        val jvmLogical = Runtime.getRuntime().availableProcessors().coerceAtLeast(1)
        val osLogical = detectOsLogicalCores()
        return maxOf(jvmLogical, osLogical).coerceAtLeast(1)
    }

    private fun detectOsLogicalCores(): Int {
        val linuxCpuInfo = runCatching {
            java.io.File("/proc/cpuinfo")
                .takeIf { it.isFile }
                ?.useLines { lines ->
                    lines.count { it.startsWith("processor\t:") || it.startsWith("processor :") }
                }
                ?: 0
        }.getOrDefault(0)
        val nprocValue = runCatching {
            val process = ProcessBuilder("nproc").redirectErrorStream(true).start()
            val out = process.inputStream.bufferedReader().use { it.readText().trim() }
            if (process.waitFor() == 0) out.toIntOrNull() ?: 0 else 0
        }.getOrDefault(0)
        val windowsEnv = System.getenv("NUMBER_OF_PROCESSORS")?.toIntOrNull() ?: 0
        return maxOf(linuxCpuInfo, nprocValue, windowsEnv, 1)
    }

    private fun resolveDownloadInfo(): FoliaDownloadInfo {
        val desiredVersion = Aerogel.VERSION
        val projectJson = getJson(API_BASE)
        val versions = projectJson.getAsJsonArray("versions")
            ?: throw IllegalStateException("Folia API: versions field missing")

        val available = versions.map { it.asString }
        if (available.isEmpty()) {
            throw IllegalStateException("Folia API returned no available versions")
        }

        val version = when {
            desiredVersion.isEmpty() -> available.maxWithOrNull(::compareMcVersion)!!
            desiredVersion in available -> desiredVersion
            else -> {
                val fallback = available.maxWithOrNull(::compareMcVersion)!!
                ServerI18n.log("aerogel.folia.download.resolve.fallback", desiredVersion, fallback)
                fallback
            }
        }

        val buildsJson = getJson("$API_BASE/versions/$version/builds")
        val builds = buildsJson.getAsJsonArray("builds")
            ?: throw IllegalStateException("Folia API: builds field missing for version $version")
        val latestBuild = builds.maxOfOrNull { it.asJsonObject.get("build").asInt }
            ?: throw IllegalStateException("No Folia builds found for version $version")

        val detail = getJson("$API_BASE/versions/$version/builds/$latestBuild")
        val app = detail.getAsJsonObject("downloads")
            ?.getAsJsonObject("application")
            ?: throw IllegalStateException("Folia API: downloads.application missing for $version#$latestBuild")

        val fileName = app.get("name")?.asString?.trim().orEmpty()
        val sha256 = app.get("sha256")?.asString?.trim().orEmpty()
        require(fileName.isNotEmpty()) { "Folia API: empty download filename for $version#$latestBuild" }
        require(sha256.isNotEmpty()) { "Folia API: empty download sha256 for $version#$latestBuild" }

        return FoliaDownloadInfo(
            version = version,
            build = latestBuild,
            fileName = fileName,
            sha256 = sha256
        )
    }

    private fun ensureCachedFoliaJar() {
        val cached = readCachedDownloadInfo()
        val nowMillis = System.currentTimeMillis()
        val jarExists = Files.isRegularFile(jarPath)
        val desiredVersion = Aerogel.VERSION
        val cacheFresh = cached != null &&
            jarExists &&
            cached.version == desiredVersion &&
            (nowMillis - readMetadataTimestamp(downloadMetadataPath) <= downloadInfoCacheTtlMillis)
        if (cacheFresh) return

        val downloadInfo = DebugConsole.withSpinner(
            progressMessage = ServerI18n.tr("aerogel.folia.download.resolve.start", desiredVersion),
            doneMessage = ServerI18n.tr("aerogel.folia.download.resolve.done", desiredVersion)
        ) {
            resolveDownloadInfo()
        }

        val cacheMatchesDownload = cached != null &&
            jarExists &&
            cached.version == downloadInfo.version &&
            cached.build == downloadInfo.build &&
            cached.fileName == downloadInfo.fileName &&
            cached.sha256.equals(downloadInfo.sha256, ignoreCase = true)
        if (cacheMatchesDownload) {
            writeCachedDownloadInfo(downloadInfo)
            return
        }

        if (jarExists) {
            val actualSha = sha256Hex(jarPath)
            if (actualSha.equals(downloadInfo.sha256, ignoreCase = true)) {
                writeCachedDownloadInfo(downloadInfo)
                return
            }
        }

        downloadFoliaJar(downloadInfo, jarPath)
        writeCachedDownloadInfo(downloadInfo)
    }

    private fun resolveSidecarJvmArgs(): List<String> {
        val explicitXms = System.getProperty("aerogel.folia.sidecar.xms")
            ?.trim()
            ?.takeIf { it.isNotEmpty() }
            ?.let { "-Xms$it" }
        val explicitXmx = System.getProperty("aerogel.folia.sidecar.xmx")
            ?.trim()
            ?.takeIf { it.isNotEmpty() }
            ?.let { "-Xmx$it" }
        if (explicitXms != null || explicitXmx != null) {
            return listOfNotNull(
                explicitXms ?: "-Xms${defaultSidecarXmsMb}M",
                explicitXmx ?: "-Xmx${defaultSidecarXmxMb}M"
            )
        }

        return listOf("-Xms${defaultSidecarXmsMb}M", "-Xmx${defaultSidecarXmxMb}M")
    }

    private fun downloadFoliaJar(info: FoliaDownloadInfo, targetPath: Path) {
        val url = "$API_BASE/versions/${info.version}/builds/${info.build}/downloads/${info.fileName}"
        DebugConsole.withSpinner(
            progressMessage = ServerI18n.tr("aerogel.folia.download.start", info.version, info.build.toString()),
            doneMessage = ServerI18n.tr("aerogel.folia.download.done")
        ) {
            Files.createDirectories(targetPath.parent ?: Path.of("."))

            val tmpPath = targetPath.resolveSibling("${targetPath.fileName}.part")
            downloadBinary(url, tmpPath)

            val actual = sha256Hex(tmpPath)
            require(actual.equals(info.sha256, ignoreCase = true)) {
                "Folia jar integrity mismatch: expected=${info.sha256}, actual=$actual"
            }

            Files.move(tmpPath, targetPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
        }
    }

    private fun getJson(url: String): JsonObject {
        val request = HttpRequest.newBuilder(URI.create(url))
            .header("User-Agent", "Aerogel/${Aerogel.VERSION}")
            .timeout(Duration.ofSeconds(30))
            .GET()
            .build()
        val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8))
        if (response.statusCode() !in 200..299) {
            throw IllegalStateException("HTTP ${response.statusCode()} for $url")
        }
        return JsonParser.parseString(response.body()).asJsonObject
    }

    private fun downloadBinary(url: String, targetPath: Path) {
        val request = HttpRequest.newBuilder(URI.create(url))
            .header("User-Agent", "Aerogel/${Aerogel.VERSION}")
            .timeout(Duration.ofMinutes(3))
            .GET()
            .build()
        val response = httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream())
        if (response.statusCode() !in 200..299) {
            throw IllegalStateException("Download failed: HTTP ${response.statusCode()} ($url)")
        }

        Files.newOutputStream(targetPath).use { output ->
            response.body().use { input ->
                input.copyTo(output)
            }
        }
    }

    private fun sha256Hex(path: Path): String {
        val digest = MessageDigest.getInstance("SHA-256")
        Files.newInputStream(path).use { input ->
            val buffer = ByteArray(DEFAULT_BUFFER_SIZE)
            while (true) {
                val read = input.read(buffer)
                if (read <= 0) break
                digest.update(buffer, 0, read)
            }
        }
        return digest.digest().joinToString("") { "%02x".format(it) }
    }

    private fun readCachedDownloadInfo(): FoliaDownloadInfo? {
        val props = readProperties(downloadMetadataPath) ?: return null
        val version = props.getProperty("version")?.trim().orEmpty()
        val build = props.getProperty("build")?.trim()?.toIntOrNull()
        val fileName = props.getProperty("fileName")?.trim().orEmpty()
        val sha256 = props.getProperty("sha256")?.trim().orEmpty()
        if (version.isEmpty() || build == null || fileName.isEmpty() || sha256.isEmpty()) return null
        return FoliaDownloadInfo(
            version = version,
            build = build,
            fileName = fileName,
            sha256 = sha256
        )
    }

    private fun writeCachedDownloadInfo(info: FoliaDownloadInfo) {
        writeProperties(
            downloadMetadataPath,
            linkedMapOf(
                "version" to info.version,
                "build" to info.build.toString(),
                "fileName" to info.fileName,
                "sha256" to info.sha256,
                "resolvedAtMillis" to System.currentTimeMillis().toString()
            )
        )
    }

    private fun readMetadataTimestamp(path: Path): Long {
        val props = readProperties(path) ?: return 0L
        return props.getProperty("resolvedAtMillis")?.trim()?.toLongOrNull() ?: 0L
    }

    private fun prepareSidecarServerProperties(runtimeDir: Path) {
        val sidecarLevelSeed = System.getProperty("aerogel.folia.sidecar.level-seed")
            ?.trim()
            ?.takeIf { it.isNotEmpty() }

        val sidecarProperties = linkedMapOf(
            "motd" to "Aerogel Folia Sidecar",
            "online-mode" to "false",
            "max-players" to "1",
            "spawn-protection" to "0",
            "enable-query" to "false",
            "enable-rcon" to "false",
            "save-user-cache-on-stop-only" to "true",
            "level-seed" to (sidecarLevelSeed ?: ""),
            "view-distance" to "2",
            "simulation-distance" to "2",
            // Bind to loopback + ephemeral port to avoid fixed port usage/conflicts.
            "server-ip" to bindHost,
            "server-port" to bindPort
        )

        val serverPropertiesPath = runtimeDir.resolve("server.properties")
        val lines = sidecarProperties.entries.map { (k, v) -> "$k=$v" }
        Files.write(serverPropertiesPath, lines, StandardCharsets.UTF_8)
    }

    private fun prepareSidecarConfigFiles(runtimeDir: Path, chunkIpcSlots: Int, regionGridExponent: Int) {
        val configDir = runtimeDir.resolve("config")
        Files.createDirectories(configDir)

        val configuredThreads = chunkIpcSlots.coerceAtLeast(1)
        val configuredIoThreads = configuredThreads
        val autosaveIntervalTicks = Int.MAX_VALUE

        val globalVersion = readExistingConfigVersion(configDir.resolve("paper-global.yml"), 31)
        writeConfigText(
            configDir.resolve("paper-global.yml"),
            """
            _version: $globalVersion
            threaded-regions:
              grid-exponent: $regionGridExponent
              threads: $configuredThreads
            chunk-system:
              worker-threads: $configuredThreads
              io-threads: $configuredIoThreads
            player-auto-save:
              rate: -1
              max-per-tick: -1
            """.trimIndent() + "\n"
        )

        val worldDefaultsVersion = readExistingConfigVersion(configDir.resolve("paper-world-defaults.yml"), 31)
        writeConfigText(
            configDir.resolve("paper-world-defaults.yml"),
            """
            _version: $worldDefaultsVersion
            chunks:
              auto-save-interval: $autosaveIntervalTicks
            """.trimIndent() + "\n"
        )

        writeConfigText(
            runtimeDir.resolve("bukkit.yml"),
            """
            settings:
              allow-end: true
            ticks-per:
              autosave: $autosaveIntervalTicks
            chunk-gc:
              period-in-ticks: $defaultChunkGcPeriodTicks
            """.trimIndent() + "\n"
        )

        val spigotVersion = readExistingConfigVersion(runtimeDir.resolve("spigot.yml"), 12)
        writeConfigText(
            runtimeDir.resolve("spigot.yml"),
            """
            config-version: $spigotVersion
            settings:
              save-user-cache-on-stop-only: true
            players:
              disable-saving: true
            advancements:
              disable-saving: true
            stats:
              disable-saving: true
            """.trimIndent() + "\n"
        )
    }

    private fun prepareRuntimeStateCache(regionGridExponent: Int) {
        val currentState = linkedMapOf(
            "minecraftVersion" to Aerogel.VERSION,
            "regionGridExponent" to regionGridExponent.toString(),
            "sidecarLevelSeed" to runtimeStateProperty("aerogel.folia.sidecar.level-seed"),
            "overworldSeed" to runtimeStateProperty("aerogel.folia.seed.overworld"),
            "netherSeed" to runtimeStateProperty("aerogel.folia.seed.the_nether"),
            "endSeed" to runtimeStateProperty("aerogel.folia.seed.the_end")
        )
        val existing = readProperties(runtimeStatePath)
        val hasCachedWorlds = worldDirs.any { Files.exists(runtimeDir.resolve(it)) }

        if (existing != null && !propertiesMatch(existing, currentState)) {
            purgeSidecarWorldData(runtimeDir)
        }

        if (existing == null || !propertiesMatch(existing, currentState) || !hasCachedWorlds) {
            writeProperties(runtimeStatePath, currentState)
        }

        cleanupTransientRuntimeFiles()
    }

    private fun readExistingConfigVersion(path: Path, fallback: Int): Int {
        if (!Files.isRegularFile(path)) return fallback
        return runCatching {
            Files.newBufferedReader(path, StandardCharsets.UTF_8).useLines { lines ->
                lines.firstNotNullOfOrNull { line ->
                    val trimmed = line.trim()
                    if (trimmed.startsWith("_version:")) {
                        trimmed.substringAfter(':').trim().toIntOrNull()
                    } else if (trimmed.startsWith("config-version:")) {
                        trimmed.substringAfter(':').trim().toIntOrNull()
                    } else {
                        null
                    }
                }
            } ?: fallback
        }.getOrDefault(fallback)
    }

    private fun writeConfigText(path: Path, content: String) {
        Files.createDirectories(path.parent ?: Path.of("."))
        Files.writeString(
            path,
            content,
            StandardCharsets.UTF_8,
            java.nio.file.StandardOpenOption.CREATE,
            java.nio.file.StandardOpenOption.TRUNCATE_EXISTING,
            java.nio.file.StandardOpenOption.WRITE
        )
    }

    private fun ensureEulaAccepted(runtimeDir: Path) {
        val rootEulaPath = Path.of("eula.txt")
        if (!isEulaAccepted(rootEulaPath)) {
            val eulaUrl = localizedEulaUrl()
            println(ServerI18n.tr("aerogel.eula.disclaimer"))
            println(ServerI18n.tr("aerogel.eula.notice", eulaUrl))
            print("${ServerI18n.tr("aerogel.eula.prompt")}: ")
            val inputStartNanos = System.nanoTime()
            val input = System.`in`.bufferedReader(StandardCharsets.UTF_8).readLine()
            StartupTiming.addInteractiveInputNanos(System.nanoTime() - inputStartNanos)
            if (input == null || input.trim().isNotEmpty()) {
                throw IllegalStateException(ServerI18n.tr("aerogel.eula.rejected"))
            }
            writeAcceptedEula(rootEulaPath, eulaUrl)
            ServerI18n.log("aerogel.log.warn.eula_accepted")
        }

        val sidecarEulaPath = runtimeDir.resolve("eula.txt")
        if (!isEulaAccepted(sidecarEulaPath)) {
            writeAcceptedEula(sidecarEulaPath, localizedEulaUrl())
        }
    }

    private fun isEulaAccepted(path: Path): Boolean {
        if (!Files.exists(path)) return false
        return Files.readAllLines(path, StandardCharsets.UTF_8).any { line ->
            line.trim().equals("eula=true", ignoreCase = true)
        }
    }

    private fun writeAcceptedEula(path: Path, eulaUrl: String) {
        val timestamp = DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(Instant.now().atOffset(ZoneOffset.UTC))
        val lines = listOf(
            "# By changing the setting below to TRUE you are indicating your agreement to Mojang's EULA.",
            "# $eulaUrl",
            "# $timestamp",
            "eula=true"
        )
        Files.createDirectories(path.parent ?: Path.of("."))
        Files.write(path, lines, StandardCharsets.UTF_8)
    }

    private fun localizedEulaUrl(): String {
        val language = Locale.getDefault().language.lowercase(Locale.ROOT)
        val localeSegment = when (language) {
            "ko" -> "ko-kr"
            else -> "en-us"
        }
        return "https://www.minecraft.net/$localeSegment/eula"
    }

    private fun runtimeStateProperty(key: String): String {
        return System.getProperty(key)?.trim().orEmpty()
    }

    private fun compareMcVersion(left: String, right: String): Int {
        val leftParts = left.split('.').map { it.toIntOrNull() ?: 0 }
        val rightParts = right.split('.').map { it.toIntOrNull() ?: 0 }
        val maxSize = maxOf(leftParts.size, rightParts.size)
        for (i in 0 until maxSize) {
            val l = leftParts.getOrElse(i) { 0 }
            val r = rightParts.getOrElse(i) { 0 }
            if (l != r) return l.compareTo(r)
        }
        return 0
    }

    private fun installBridgePlugin(runtimeDir: Path) {
        val pluginsDir = runtimeDir.resolve("plugins")
        Files.createDirectories(pluginsDir)
        val targetJar = pluginsDir.resolve(sidecarPluginFileName)
        val targetTmp = pluginsDir.resolve("$sidecarPluginFileName.part")

        val selfLocation = FoliaRuntimeBootstrap::class.java.protectionDomain.codeSource?.location?.toURI()
            ?: throw IllegalStateException("Unable to resolve Aerogel runtime location for embedded bridge plugin")
        val selfPath = Path.of(selfLocation)
        if (!Files.isRegularFile(selfPath)) {
            val devPluginJar = resolveDevBridgePluginJar()
            if (devPluginJar != null) {
                if (!needsBridgeInstall(devPluginJar, targetJar)) return
                DebugConsole.withSpinner(
                    progressMessage = ServerI18n.tr("aerogel.folia.bridge.prepare.start"),
                    doneMessage = ServerI18n.tr("aerogel.folia.bridge.prepare.done")
                ) {
                    Files.copy(devPluginJar, targetTmp, StandardCopyOption.REPLACE_EXISTING)
                    Files.move(targetTmp, targetJar, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
                }
                return
            }

            if (Files.isRegularFile(targetJar)) return
            throw IllegalStateException("Unable to resolve dev Folia bridge plugin jar at build/libs/$sidecarPluginFileName")
        }

        JarFile(selfPath.toFile()).use { jar ->
            val entry = jar.getJarEntry(embeddedPluginJarEntry)
                ?: throw IllegalStateException(
                    "Embedded bridge plugin entry '$embeddedPluginJarEntry' missing in ${selfPath.fileName}"
                )
            if (!needsBridgeInstall(entry, targetJar)) return
            DebugConsole.withSpinner(
                progressMessage = ServerI18n.tr("aerogel.folia.bridge.prepare.start"),
                doneMessage = ServerI18n.tr("aerogel.folia.bridge.prepare.done")
            ) {
                jar.getInputStream(entry).use { input ->
                    Files.newOutputStream(targetTmp).use { output ->
                        input.copyTo(output)
                    }
                }
                Files.move(targetTmp, targetJar, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
            }
        }
    }

    private fun needsBridgeInstall(sourceJar: Path, targetJar: Path): Boolean {
        if (!Files.isRegularFile(targetJar)) return true
        if (Files.size(sourceJar) != Files.size(targetJar)) return true
        return crc32Of(sourceJar) != crc32Of(targetJar)
    }

    private fun needsBridgeInstall(embeddedEntry: JarEntry, targetJar: Path): Boolean {
        if (!Files.isRegularFile(targetJar)) return true
        val entrySize = embeddedEntry.size
        if (entrySize >= 0L && entrySize != Files.size(targetJar)) return true
        val entryCrc = embeddedEntry.crc
        if (entryCrc < 0L) return true
        return entryCrc != crc32Of(targetJar)
    }

    private fun crc32Of(path: Path): Long {
        val crc = CRC32()
        Files.newInputStream(path).use { input ->
            val buffer = ByteArray(DEFAULT_BUFFER_SIZE)
            while (true) {
                val read = input.read(buffer)
                if (read <= 0) break
                crc.update(buffer, 0, read)
            }
        }
        return crc.value
    }

    private fun resolveDevBridgePluginJar(): Path? {
        val configuredPath = System.getProperty("aerogel.folia.plugin.jar")
            ?.trim()
            ?.takeIf { it.isNotEmpty() }
            ?.let { Path.of(it).toAbsolutePath().normalize() }
            ?.takeIf { Files.isRegularFile(it) }
        if (configuredPath != null) return configuredPath

        val outputJar = Path.of("build", "libs", sidecarPluginFileName).toAbsolutePath().normalize()
        val classesDir = Path.of("build", "classes", "java", "foliaPlugin").toAbsolutePath().normalize()
        val resourcesDir = Path.of("build", "resources", "foliaPlugin").toAbsolutePath().normalize()

        val hasInputs = Files.isDirectory(classesDir) && Files.isDirectory(resourcesDir)
        if (hasInputs && shouldRebuildDevPluginJar(outputJar, classesDir, resourcesDir)) {
            rebuildDevPluginJar(outputJar, classesDir, resourcesDir)
        }
        return outputJar.takeIf { Files.isRegularFile(it) }
    }

    private fun cleanupTransientRuntimeFiles() {
        Files.deleteIfExists(runtimeDir.resolve("folia-sidecar.log"))
        for (worldDirName in worldDirs) {
            deleteSessionLocks(runtimeDir.resolve(worldDirName))
        }
    }

    private fun deleteSessionLocks(root: Path) {
        if (!Files.exists(root)) return
        Files.walk(root).use { stream ->
            stream.filter { Files.isRegularFile(it) && it.fileName.toString() == "session.lock" }
                .forEach { Files.deleteIfExists(it) }
        }
    }

    private fun shouldRebuildDevPluginJar(outputJar: Path, classesDir: Path, resourcesDir: Path): Boolean {
        if (!Files.isRegularFile(outputJar)) return true
        val jarMtime = runCatching { Files.getLastModifiedTime(outputJar).toMillis() }.getOrDefault(0L)
        val classMtime = latestFileMtime(classesDir)
        val resourceMtime = latestFileMtime(resourcesDir)
        return maxOf(classMtime, resourceMtime) > jarMtime
    }

    private fun latestFileMtime(root: Path): Long {
        if (!Files.isDirectory(root)) return 0L
        return runCatching {
            Files.walk(root).use { stream ->
                stream.filter { Files.isRegularFile(it) }
                    .mapToLong { path -> Files.getLastModifiedTime(path).toMillis() }
                    .max()
                    .orElse(0L)
            }
        }.getOrDefault(0L)
    }

    private fun rebuildDevPluginJar(outputJar: Path, classesDir: Path, resourcesDir: Path) {
        DebugConsole.withSpinner(
            progressMessage = ServerI18n.tr("aerogel.folia.bridge.rebuild.start"),
            doneMessage = ServerI18n.tr("aerogel.folia.bridge.rebuild.done")
        ) {
            val tmpJar = outputJar.resolveSibling("${outputJar.fileName}.part")
            Files.createDirectories(outputJar.parent ?: Path.of("."))

            JarOutputStream(Files.newOutputStream(tmpJar)).use { jarOut ->
                addDirectoryToJar(jarOut, resourcesDir, resourcesDir)
                addDirectoryToJar(jarOut, classesDir, classesDir)
            }

            Files.move(tmpJar, outputJar, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
        }
    }

    private fun addDirectoryToJar(jarOut: JarOutputStream, rootDir: Path, sourceDir: Path) {
        if (!Files.isDirectory(sourceDir)) return
        Files.walk(sourceDir).use { stream ->
            stream.filter { Files.isRegularFile(it) }.forEach { file ->
                val entryName = rootDir.relativize(file).toString().replace('\\', '/')
                val entry = JarEntry(entryName)
                jarOut.putNextEntry(entry)
                Files.newInputStream(file).use { input -> input.copyTo(jarOut) }
                jarOut.closeEntry()
            }
        }
    }

    private fun purgeSidecarWorldData(runtimeDir: Path) {
        for (worldDirName in worldDirs) {
            deleteRecursivelyIfExists(runtimeDir.resolve(worldDirName))
        }
    }

    private fun readProperties(path: Path): Properties? {
        if (!Files.isRegularFile(path)) return null
        return runCatching {
            Properties().apply {
                Files.newInputStream(path).use { load(it) }
            }
        }.getOrNull()
    }

    private fun writeProperties(path: Path, values: Map<String, String>) {
        val props = Properties()
        for ((key, value) in values) {
            props.setProperty(key, value)
        }
        Files.createDirectories(path.parent ?: Path.of("."))
        Files.newOutputStream(path).use { props.store(it, null) }
    }

    private fun propertiesMatch(existing: Properties, expected: Map<String, String>): Boolean {
        for ((key, expectedValue) in expected) {
            if ((existing.getProperty(key) ?: "") != expectedValue) return false
        }
        return true
    }

    private fun deleteRecursivelyIfExists(path: Path) {
        if (!Files.exists(path)) return
        Files.walk(path).use { stream ->
            stream.sorted(Comparator.reverseOrder()).forEach { target ->
                Files.deleteIfExists(target)
            }
        }
    }
}

fun stopFoliaRuntime() {
    FoliaRuntimeBootstrap.stop()
}

fun ensureFoliaRuntimeReady(configuredChunkWorkerThreads: Int) {
    FoliaRuntimeBootstrap.ensureReady(configuredChunkWorkerThreads)
}

fun restartFoliaRuntime(configuredChunkWorkerThreads: Int) {
    FoliaRuntimeBootstrap.restart(configuredChunkWorkerThreads)
}
