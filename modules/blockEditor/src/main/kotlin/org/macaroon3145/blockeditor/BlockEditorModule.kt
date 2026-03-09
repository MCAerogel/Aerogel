package org.macaroon3145.blockeditor

import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonPrimitive
import org.slf4j.LoggerFactory
import java.io.ByteArrayOutputStream
import java.net.Inet4Address
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.NetworkInterface
import java.net.URLDecoder
import java.nio.charset.StandardCharsets
import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.security.MessageDigest
import java.security.SecureRandom
import java.util.Comparator
import java.util.Locale
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.CopyOnWriteArrayList
import java.util.jar.JarEntry
import java.util.jar.JarOutputStream
import javax.tools.DiagnosticCollector
import javax.tools.JavaFileObject
import javax.tools.StandardLocation
import javax.tools.ToolProvider

object BlockEditorModule {
    private val logger = LoggerFactory.getLogger(BlockEditorModule::class.java)
    private val json = Json {
        ignoreUnknownKeys = true
        encodeDefaults = true
        prettyPrint = false
    }
    private val started = AtomicBoolean(false)

    @Volatile
    private var server: HttpServer? = null

    @Volatile
    private var port: Int = 8787

    @Volatile
    private var host: String = "0.0.0.0"

    @Volatile
    private var advertisedHost: String = "127.0.0.1"

    @Volatile
    private var privateKey: String = ""

    fun start() {
        if (!started.compareAndSet(false, true)) return

        runCatching { Files.createDirectories(Path.of("plugins")) }
            .onFailure {
                started.set(false)
                throw IllegalStateException("Failed to create plugins directory", it)
            }

        val configuredPort = System.getProperty("aerogel.blockeditor.port")
            ?.toIntOrNull()
            ?.coerceIn(1, 65535)
            ?: 8787
        val configuredHost = System.getProperty("aerogel.blockeditor.host")
            ?.trim()
            ?.ifEmpty { "0.0.0.0" }
            ?: "0.0.0.0"

        val createdServer = HttpServer.create(InetSocketAddress(configuredHost, configuredPort), 0)
        createdServer.createContext("/") { exchange ->
            runCatching { handle(exchange) }
                .onFailure { error ->
                    logger.warn("Block editor request failed", error)
                    runCatching {
                        sendJson(
                            exchange = exchange,
                            status = 500,
                            payload = mapOf("ok" to false, "error" to "internal_error", "message" to (error.message ?: "unknown"))
                        )
                    }
                }
        }
        createdServer.executor = Executors.newFixedThreadPool(16) { runnable ->
            Thread(runnable, "aerogel-block-editor-http").apply { isDaemon = true }
        }
        createdServer.start()

        server = createdServer
        port = configuredPort
        host = configuredHost
        advertisedHost = resolveAdvertisedHost(configuredHost)
        privateKey = generatePrivateKey()
        logger.info("Block editor started on {}", securedPublicUrl())
    }

    fun stop() {
        if (!started.compareAndSet(true, false)) return
        server?.stop(0)
        server = null
    }

    fun url(): String = "http://$host:$port"

    fun securedUrl(): String = "${url()}/?privatekey=$privateKey"

    fun publicUrl(): String = "http://$advertisedHost:$port"

    fun securedPublicUrl(): String = "${publicUrl()}/?privatekey=$privateKey"

    private fun resolveAdvertisedHost(boundHost: String): String {
        val normalized = boundHost.trim().lowercase(Locale.ROOT)
        if (
            normalized.isNotEmpty()
            && normalized != "0.0.0.0"
            && normalized != "::"
            && normalized != "::0"
            && normalized != "[::]"
        ) {
            return boundHost
        }
        val candidates = mutableListOf<InetAddress>()
        runCatching {
            NetworkInterface.getNetworkInterfaces().toList().forEach { ni ->
                if (!ni.isUp || ni.isLoopback || ni.isVirtual) return@forEach
                ni.inetAddresses.toList()
                    .filterIsInstance<Inet4Address>()
                    .forEach { addr ->
                        if (!addr.isLoopbackAddress && !addr.isAnyLocalAddress && !addr.isLinkLocalAddress) {
                            candidates += addr
                        }
                    }
            }
        }
        val preferred = candidates.firstOrNull { it.isSiteLocalAddress }
            ?: candidates.firstOrNull()
        return preferred?.hostAddress ?: "127.0.0.1"
    }

    private fun handle(exchange: HttpExchange) {
        val method = exchange.requestMethod.uppercase(Locale.ROOT)
        val path = exchange.requestURI.path ?: "/"
        val query = queryParams(exchange)
        if (!isAuthorized(path, query)) {
            sendJson(exchange, 403, mapOf("ok" to false, "error" to "forbidden", "message" to "Invalid privatekey"))
            return
        }
        if (method == "GET" && path == "/") {
            serveResource(exchange, "block-editor/index.html", "text/html; charset=utf-8")
            return
        }
        if (method == "GET" && path.startsWith("/static/")) {
            val fileName = path.removePrefix("/static/")
            val contentType = when {
                fileName.endsWith(".css", ignoreCase = true) -> "text/css; charset=utf-8"
                fileName.endsWith(".js", ignoreCase = true) -> "application/javascript; charset=utf-8"
                fileName.endsWith(".svg", ignoreCase = true) -> "image/svg+xml; charset=utf-8"
                else -> "application/octet-stream"
            }
            serveResource(exchange, "block-editor/static/$fileName", contentType)
            return
        }
        if (method == "GET" && path == "/api/block-editor/health") {
            sendJson(
                exchange,
                200,
                mapOf(
                    "ok" to true,
                    "port" to port,
                    "url" to url(),
                    "securedUrl" to securedUrl(),
                    "publicUrl" to publicUrl(),
                    "securedPublicUrl" to securedPublicUrl()
                )
            )
            return
        }
        if (method == "GET" && path == "/api/block-editor/draft") {
            val draft = BlockEditorDraftRepository.loadDraft()
            sendJson(exchange, 200, mapOf("ok" to true, "draft" to (draft ?: JsonNull)))
            return
        }
        if (method == "POST" && path == "/api/block-editor/draft") {
            val payload = readBodyUtf8(exchange)
            runCatching { BlockEditorDraftRepository.saveDraft(payload) }
                .onFailure {
                    sendJson(exchange, 400, mapOf("ok" to false, "error" to "draft_save_failed", "message" to (it.message ?: "failed")))
                    return
                }
            sendJson(exchange, 200, mapOf("ok" to true))
            return
        }
        if (method == "GET" && path == "/api/block-editor/realtime/stream") {
            val clientId = query["clientId"]?.trim().orEmpty().ifBlank { "anonymous" }
            RealtimeSyncHub.openStream(exchange, clientId)
            return
        }
        if (method == "POST" && path == "/api/block-editor/realtime/publish") {
            val payloadText = readBodyUtf8(exchange)
            val root = runCatching { json.parseToJsonElement(payloadText) as? JsonObject }
                .getOrNull()
            if (root == null) {
                sendJson(exchange, 400, mapOf("ok" to false, "error" to "invalid_payload"))
                return
            }
            val clientId = root["clientId"]?.jsonPrimitive?.contentOrNull?.ifBlank { "anonymous" } ?: "anonymous"
            val type = root["type"]?.jsonPrimitive?.contentOrNull?.ifBlank { "state" } ?: "state"
            val payload = root["payload"] ?: root["state"]
            if (payload == null || payload is JsonNull) {
                sendJson(exchange, 400, mapOf("ok" to false, "error" to "missing_state"))
                return
            }
            RealtimeSyncHub.publish(clientId, type, payload)
            sendJson(exchange, 200, mapOf("ok" to true))
            return
        }
        if (method == "GET" && path == "/api/block-editor/plugins") {
            val plugins = BlockPluginRepository.listPlugins()
            sendJson(exchange, 200, mapOf("ok" to true, "plugins" to plugins))
            return
        }
        if (method == "GET" && path == "/api/block-editor/load") {
            val query = queryParams(exchange)
            val jarFile = query["jar"]?.trim().orEmpty()
            if (jarFile.isEmpty()) {
                sendJson(exchange, 400, mapOf("ok" to false, "error" to "missing_jar", "message" to "Query parameter 'jar' is required"))
                return
            }
            val source = runCatching { BlockPluginRepository.loadPluginSource(jarFile) }
                .getOrElse {
                    sendJson(exchange, 400, mapOf("ok" to false, "error" to "load_failed", "message" to (it.message ?: "failed")))
                    return
                }
            sendJson(
                exchange,
                200,
                mapOf(
                    "ok" to true,
                    "jar" to jarFile,
                    "pluginId" to source.pluginId,
                    "pluginName" to source.pluginName,
                    "version" to source.version,
                    "blocks" to source.blocks,
                    "links" to source.links
                )
            )
            return
        }
        if (method == "POST" && path == "/api/block-editor/save") {
            val payload = readBodyUtf8(exchange)
            val request = runCatching { json.decodeFromString(BlockEditorSaveRequest.serializer(), payload) }
                .getOrElse {
                    sendJson(
                        exchange,
                        400,
                        mapOf("ok" to false, "error" to "invalid_json", "message" to (it.message ?: "Invalid JSON payload"))
                    )
                    return
                }

            val result = runCatching { BlockPluginJarBuilder.save(request, payload) }
                .getOrElse {
                    sendJson(
                        exchange,
                        400,
                        mapOf("ok" to false, "error" to "save_failed", "message" to (it.message ?: "Failed to generate jar"))
                    )
                    return
                }

            sendJson(
                exchange,
                200,
                mapOf(
                    "ok" to true,
                    "pluginId" to result.pluginId,
                    "jarPath" to result.jarPath,
                    "mainClass" to result.mainClass,
                    "blockCount" to result.blockCount,
                    "reloadHint" to "PluginSystem hot-reload loop will detect jar changes in plugins/"
                )
            )
            return
        }

        if (method == "GET" && path == "/favicon.ico") {
            exchange.sendResponseHeaders(204, -1)
            exchange.close()
            return
        }

        sendJson(exchange, 404, mapOf("ok" to false, "error" to "not_found", "path" to path))
    }

    private fun readBodyUtf8(exchange: HttpExchange): String {
        val body = ByteArrayOutputStream()
        exchange.requestBody.use { input ->
            val buffer = ByteArray(8192)
            while (true) {
                val read = input.read(buffer)
                if (read <= 0) break
                body.write(buffer, 0, read)
                if (body.size() > 1_048_576) {
                    throw IllegalArgumentException("Request body is too large (max 1MB)")
                }
            }
        }
        return body.toString(StandardCharsets.UTF_8)
    }

    private fun queryParams(exchange: HttpExchange): Map<String, String> {
        val query = exchange.requestURI.rawQuery ?: return emptyMap()
        if (query.isBlank()) return emptyMap()
        return query.split('&')
            .asSequence()
            .mapNotNull { pair ->
                val index = pair.indexOf('=')
                if (index <= 0) return@mapNotNull null
                val key = URLDecoder.decode(pair.substring(0, index), StandardCharsets.UTF_8)
                val value = URLDecoder.decode(pair.substring(index + 1), StandardCharsets.UTF_8)
                key to value
            }
            .toMap()
    }

    private fun isAuthorized(path: String, query: Map<String, String>): Boolean {
        if (path == "/favicon.ico") return true
        if (path.startsWith("/static/")) return true
        val provided = query["privatekey"]?.trim().orEmpty()
        return provided.isNotEmpty() && provided == privateKey
    }

    private fun generatePrivateKey(): String {
        val randomBytes = ByteArray(64)
        SecureRandom().nextBytes(randomBytes)
        val seed = randomBytes + System.nanoTime().toString().toByteArray(StandardCharsets.UTF_8)
        val digest = MessageDigest.getInstance("SHA-512").digest(seed)
        return digest.joinToString("") { b -> "%02x".format(b) }
    }

    private fun sendBytes(exchange: HttpExchange, status: Int, contentType: String, bytes: ByteArray) {
        exchange.responseHeaders.add("Content-Type", contentType)
        exchange.sendResponseHeaders(status, bytes.size.toLong())
        exchange.responseBody.use { it.write(bytes) }
    }

    private fun sendJson(exchange: HttpExchange, status: Int, payload: Any) {
        val bytes = json.encodeToString(JsonElement.serializer(), payload.toJsonElement())
            .toByteArray(StandardCharsets.UTF_8)
        sendBytes(exchange, status, "application/json; charset=utf-8", bytes)
    }

    private fun serveResource(exchange: HttpExchange, resourcePath: String, contentType: String) {
        val bytes = javaClass.classLoader.getResourceAsStream(resourcePath)?.use { it.readAllBytes() }
        if (bytes == null) {
            sendJson(exchange, 404, mapOf("ok" to false, "error" to "resource_not_found", "path" to resourcePath))
            return
        }
        // Prevent stale editor assets in aggressive browser caches.
        exchange.responseHeaders.set("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
        exchange.responseHeaders.set("Pragma", "no-cache")
        exchange.responseHeaders.set("Expires", "0")
        sendBytes(exchange, 200, contentType, bytes)
    }
}

private object RealtimeSyncHub {
    private data class Client(val clientId: String, val exchange: HttpExchange)

    private val clients = CopyOnWriteArrayList<Client>()

    @Volatile
    private var lastState: JsonElement = JsonObject(emptyMap())

    fun publish(clientId: String, type: String, data: JsonElement) {
        if (type == "state") {
            lastState = data
        }
        val message = JsonObject(
            mapOf(
                "clientId" to JsonPrimitive(clientId),
                "type" to JsonPrimitive(type),
                "payload" to data
            )
        )
        broadcast(message)
    }

    fun openStream(exchange: HttpExchange, clientId: String) {
        exchange.responseHeaders.add("Content-Type", "text/event-stream; charset=utf-8")
        exchange.responseHeaders.add("Cache-Control", "no-cache, no-transform")
        exchange.responseHeaders.add("Connection", "keep-alive")
        exchange.responseHeaders.add("X-Accel-Buffering", "no")
        exchange.sendResponseHeaders(200, 0)

        val client = Client(clientId, exchange)
        clients.add(client)

        val initialPayload = JsonObject(
            mapOf(
                "clientId" to JsonPrimitive("server"),
                "type" to JsonPrimitive("state"),
                "payload" to lastState
            )
        )
        if (!send(client, initialPayload)) {
            remove(client)
            return
        }

        try {
            while (true) {
                Thread.sleep(15000)
                if (!sendRaw(client, ": keep-alive\n\n")) break
            }
        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
        } finally {
            remove(client)
        }
    }

    private fun broadcast(payload: JsonObject) {
        for (client in clients) {
            if (!send(client, payload)) {
                remove(client)
            }
        }
    }

    private fun send(client: Client, payload: JsonObject): Boolean {
        val data = Json.encodeToString(JsonElement.serializer(), payload)
        return sendRaw(client, "data: $data\n\n")
    }

    private fun sendRaw(client: Client, message: String): Boolean {
        return runCatching {
            val bytes = message.toByteArray(StandardCharsets.UTF_8)
            client.exchange.responseBody.write(bytes)
            client.exchange.responseBody.flush()
            true
        }.getOrDefault(false)
    }

    private fun remove(client: Client) {
        clients.remove(client)
        runCatching { client.exchange.close() }
    }
}

private object BlockEditorDraftRepository {
    private val json = Json {
        ignoreUnknownKeys = true
        encodeDefaults = true
        prettyPrint = false
    }
    private val draftPath: Path = Path.of(".aerogel-cache", "block-editor", "draft.json")

    fun loadDraft(): JsonObject? {
        if (!Files.exists(draftPath)) return null
        return runCatching {
            val text = Files.readString(draftPath, StandardCharsets.UTF_8)
            val parsed = json.parseToJsonElement(text)
            parsed as? JsonObject
        }.getOrNull()
    }

    fun saveDraft(rawPayload: String) {
        val parsed = json.parseToJsonElement(rawPayload)
        require(parsed is JsonObject) { "Draft payload must be a JSON object" }
        Files.createDirectories(draftPath.parent)
        val temp = draftPath.parent.resolve(".draft.json.tmp")
        Files.writeString(temp, rawPayload, StandardCharsets.UTF_8)
        Files.move(temp, draftPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
    }
}

private object BlockPluginRepository {
    private val json = Json { ignoreUnknownKeys = true }

    fun listPlugins(): List<Map<String, Any?>> {
        val pluginsDir = Path.of("plugins")
        if (!Files.exists(pluginsDir)) return emptyList()

        return Files.list(pluginsDir).use { stream ->
            stream
                .filter { Files.isRegularFile(it) && it.fileName.toString().endsWith(".jar", ignoreCase = true) }
                .sorted(Comparator.comparing<Path, String> { it.fileName.toString().lowercase(Locale.ROOT) })
                .map { jarPath ->
                    val descriptor = readDescriptor(jarPath)
                    val source = readSource(jarPath)
                    mapOf(
                        "jar" to jarPath.fileName.toString(),
                        "pluginId" to (source?.pluginId ?: descriptor?.id ?: jarPath.fileName.toString().removeSuffix(".jar")),
                        "pluginName" to (source?.pluginName ?: descriptor?.name ?: jarPath.fileName.toString().removeSuffix(".jar")),
                        "version" to (source?.version ?: descriptor?.version ?: "unknown"),
                        "hasBlockSource" to (source != null)
                    )
                }
                .toList()
        }
    }

    fun loadPluginSource(jarFileName: String): BlockEditorSaveRequest {
        val jarName = jarFileName.substringAfterLast('/').substringAfterLast('\\')
        require(jarName.endsWith(".jar", ignoreCase = true)) { "jar must end with .jar" }
        val jarPath = Path.of("plugins").resolve(jarName)
        require(Files.exists(jarPath) && Files.isRegularFile(jarPath)) { "plugin jar not found: $jarName" }

        readSource(jarPath)?.let { return it }

        val descriptor = readDescriptor(jarPath)
            ?: throw IllegalArgumentException("No block-editor source and no aerogel-plugin.json in $jarName")

        return BlockEditorSaveRequest(
            pluginId = descriptor.id,
            pluginName = descriptor.name,
            version = descriptor.version,
            blocks = emptyList()
        )
    }

    private fun readDescriptor(jarPath: Path): BlockPluginDescriptor? {
        return runCatching {
            FileSystems.newFileSystem(jarPath).use { fs ->
                val descriptorPath = fs.getPath("/aerogel-plugin.json")
                if (!Files.exists(descriptorPath)) return null
                val text = Files.readString(descriptorPath)
                json.decodeFromString(BlockPluginDescriptor.serializer(), text)
            }
        }.getOrNull()
    }

    private fun readSource(jarPath: Path): BlockEditorSaveRequest? {
        return runCatching {
            FileSystems.newFileSystem(jarPath).use { fs ->
                val sourcePath = fs.getPath("/block-editor/source.json")
                if (!Files.exists(sourcePath)) return null
                val text = Files.readString(sourcePath)
                json.decodeFromString(BlockEditorSaveRequest.serializer(), text)
            }
        }.getOrNull()
    }
}

private fun Any?.toJsonElement(): JsonElement {
    return when (this) {
        null -> JsonNull
        is JsonElement -> this
        is String -> JsonPrimitive(this)
        is Number -> JsonPrimitive(this)
        is Boolean -> JsonPrimitive(this)
        is Map<*, *> -> JsonObject(
            this.entries.associate { (key, value) ->
                key.toString() to value.toJsonElement()
            }
        )
        is Iterable<*> -> JsonArray(this.map { it.toJsonElement() })
        is Array<*> -> JsonArray(this.map { it.toJsonElement() })
        else -> JsonPrimitive(this.toString())
    }
}

private object BlockPluginJarBuilder {
    private val logger = LoggerFactory.getLogger(BlockPluginJarBuilder::class.java)
    private val idRegex = Regex("[a-z0-9_-]+")

    fun save(request: BlockEditorSaveRequest, rawPayload: String): BlockEditorSaveResult {
        val normalizedId = normalizePluginId(request.pluginId)
        val pluginName = request.pluginName.trim().ifEmpty { normalizedId }
        val version = request.version.trim().ifEmpty { "1.0.0" }
        val packageName = "org.macaroon3145.generated.blockeditor.${normalizedId.replace('-', '_')}"
        val className = buildClassName(normalizedId)
        val mainClass = "$packageName.$className"
        val source = BlockJavaSourceGenerator.generate(
            packageName = packageName,
            className = className,
            blocks = request.blocks,
            links = request.links
        )

        val descriptor = BlockPluginDescriptor(
            id = normalizedId,
            name = pluginName,
            version = version,
            apiVersion = "1.0",
            mainClass = mainClass,
            dependencies = emptyList(),
            softDependencies = emptyList()
        )
        val descriptorJson = Json.encodeToString(BlockPluginDescriptor.serializer(), descriptor)
        val jarPath = compileAndWriteJar(
            pluginId = normalizedId,
            packageName = packageName,
            className = className,
            sourceText = source,
            descriptorJson = descriptorJson,
            blockPayloadJson = rawPayload
        )

        return BlockEditorSaveResult(
            pluginId = normalizedId,
            jarPath = jarPath.toString(),
            mainClass = mainClass,
            blockCount = request.blocks.size
        )
    }

    private fun normalizePluginId(raw: String): String {
        val lowered = raw.trim().lowercase(Locale.ROOT)
        val cleaned = buildString(lowered.length) {
            lowered.forEach { ch ->
                when {
                    ch in 'a'..'z' || ch in '0'..'9' || ch == '-' || ch == '_' -> append(ch)
                    ch.isWhitespace() -> append('-')
                }
            }
        }.trim('-').ifBlank { "block-plugin" }

        if (!idRegex.matches(cleaned)) {
            throw IllegalArgumentException("pluginId must contain only [a-z0-9_-]")
        }
        return cleaned
    }

    private fun buildClassName(pluginId: String): String {
        val segments = pluginId.split('-', '_').filter { it.isNotBlank() }
        val base = segments.joinToString("") { seg ->
            seg.replaceFirstChar { if (it.isLowerCase()) it.titlecase(Locale.ROOT) else it.toString() }
        }.ifBlank { "BlockPlugin" }
        return if (base.first().isDigit()) "P$base" else "${base}Plugin"
    }

    private fun compileAndWriteJar(
        pluginId: String,
        packageName: String,
        className: String,
        sourceText: String,
        descriptorJson: String,
        blockPayloadJson: String
    ): Path {
        val compiler = ToolProvider.getSystemJavaCompiler()
            ?: throw IllegalStateException("JDK compiler not available. Run Aerogel with a JDK.")

        val tempRoot = Files.createTempDirectory("aerogel-block-editor-")
        try {
            val srcRoot = tempRoot.resolve("src")
            val classesRoot = tempRoot.resolve("classes")
            val packageDir = srcRoot.resolve(packageName.replace('.', '/'))
            Files.createDirectories(packageDir)
            Files.createDirectories(classesRoot)

            val sourceFile = packageDir.resolve("$className.java")
            Files.writeString(sourceFile, sourceText, StandardCharsets.UTF_8)

            val diagnostics = DiagnosticCollector<JavaFileObject>()
            compiler.getStandardFileManager(diagnostics, Locale.ROOT, StandardCharsets.UTF_8).use { fileManager ->
                fileManager.setLocation(StandardLocation.CLASS_OUTPUT, listOf(classesRoot.toFile()))

                val sourceUnits = fileManager.getJavaFileObjectsFromFiles(listOf(sourceFile.toFile()))
                val classPath = System.getProperty("java.class.path", "")
                val options = listOf("-encoding", "UTF-8", "-classpath", classPath)
                val task = compiler.getTask(null, fileManager, diagnostics, options, null, sourceUnits)
                val ok = task.call() == true
                if (!ok) {
                    val errorText = diagnostics.diagnostics.joinToString("\n") { diagnostic ->
                        "${diagnostic.kind}: ${diagnostic.getMessage(Locale.ROOT)} @ ${diagnostic.source?.name}:${diagnostic.lineNumber}"
                    }
                    throw IllegalArgumentException("Java compilation failed:\n$errorText")
                }
            }

            val pluginsDir = Path.of("plugins")
            Files.createDirectories(pluginsDir)
            val targetJar = pluginsDir.resolve("$pluginId.jar")
            val tempJar = pluginsDir.resolve(".$pluginId.jar.tmp")

            Files.newOutputStream(tempJar).use { output ->
                JarOutputStream(output).use { jar ->
                    addClassesToJar(classesRoot, jar)
                    writeJarTextEntry(jar, "aerogel-plugin.json", descriptorJson)
                    writeJarTextEntry(jar, "block-editor/source.json", blockPayloadJson)
                }
            }

            Files.move(tempJar, targetJar, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
            logger.info("Generated plugin jar: {}", targetJar)
            return targetJar
        } finally {
            runCatching {
                Files.walk(tempRoot)
                    .sorted(Comparator.reverseOrder())
                    .forEach { Files.deleteIfExists(it) }
            }
        }
    }

    private fun addClassesToJar(classesRoot: Path, jar: JarOutputStream) {
        Files.walk(classesRoot).use { stream ->
            stream.filter { Files.isRegularFile(it) }
                .forEach { classFile ->
                    val relative = classesRoot.relativize(classFile).toString().replace('\\', '/')
                    val entry = JarEntry(relative)
                    jar.putNextEntry(entry)
                    Files.newInputStream(classFile).use { input -> input.copyTo(jar) }
                    jar.closeEntry()
                }
        }
    }

    private fun writeJarTextEntry(jar: JarOutputStream, path: String, text: String) {
        val entry = JarEntry(path)
        jar.putNextEntry(entry)
        jar.write(text.toByteArray(StandardCharsets.UTF_8))
        jar.closeEntry()
    }
}

private object BlockJavaSourceGenerator {
    fun generate(
        packageName: String,
        className: String,
        blocks: List<BlockEditorBlock>,
        links: List<BlockEditorLink>
    ): String {
        val enableLines = ArrayList<String>()
        val joinLines = ArrayList<String>()
        val normalizedBlocks = blocks.mapIndexed { index, block ->
            block.copy(id = block.id.ifBlank { "n${index + 1}" })
        }
        val variableNameById = HashMap<String, String>()
        val variableFieldLines = normalizedBlocks.mapNotNull { block ->
            when (block.type.uppercase(Locale.ROOT)) {
                "VAR_TEXT" -> {
                    val name = javaIdentifier("text_${block.id}")
                    variableNameById[block.id] = name
                    val value = javaString(block.params["value"] ?: "")
                    "private String $name = \"$value\";"
                }
                "VAR_INTEGER" -> {
                    val name = javaIdentifier("integer_${block.id}")
                    variableNameById[block.id] = name
                    val value = (block.params["value"] ?: "").trim().toIntOrNull() ?: 0
                    "private int $name = $value;"
                }
                "VAR_DECIMAL" -> {
                    val name = javaIdentifier("decimal_${block.id}")
                    variableNameById[block.id] = name
                    val value = (block.params["value"] ?: "").trim().toDoubleOrNull() ?: 0.0
                    "private double $name = $value;"
                }
                else -> null
            }
        }
        val blockById = normalizedBlocks.associateBy { it.id }
        val outgoing = HashMap<String, MutableList<String>>()
        val incoming = HashMap<String, MutableList<BlockEditorLink>>()
        for (link in links) {
            if (link.from.isBlank() || link.to.isBlank()) continue
            if (blockById[link.from] == null || blockById[link.to] == null) continue
            val kind = link.kind?.trim().orEmpty()
            val normalizedKind = if (kind.isEmpty()) "exec" else kind
            if (normalizedKind == "exec") {
                outgoing.getOrPut(link.from) { ArrayList() }.add(link.to)
            }
            incoming.getOrPut(link.to) { ArrayList() }.add(link.copy(kind = normalizedKind))
        }

        fun javaCodeLines(raw: String): List<String> {
            if (raw.isBlank()) return emptyList()
            return raw
                .replace("\r\n", "\n")
                .replace('\r', '\n')
                .split('\n')
                .map { it.trimEnd() }
                .filter { it.isNotBlank() }
        }

        fun isDataLink(link: BlockEditorLink): Boolean {
            return when (link.kind) {
                "data-text", "data-int", "data-decimal", "data-any", "data", null -> true
                else -> false
            }
        }

        fun resolveDataExpression(blockId: String, visiting: MutableSet<String> = HashSet()): String? {
            if (!visiting.add(blockId)) return null
            val block = blockById[blockId] ?: return null
            return when (block.type.uppercase(Locale.ROOT)) {
                "VAR_TEXT", "VAR_INTEGER", "VAR_DECIMAL" -> variableNameById[block.id]
                "MATH_ADD" -> {
                    val incomingDataLinks = incoming[block.id].orEmpty().filter { isDataLink(it) }
                    val exactLeft = incomingDataLinks.firstOrNull { it.toPortClass == "in-data-a" }
                    val exactRight = incomingDataLinks.firstOrNull { it.toPortClass == "in-data-b" && it !== exactLeft }
                    val fallback = incomingDataLinks.filter { it !== exactLeft && it !== exactRight }
                    val leftSource = exactLeft ?: incomingDataLinks.firstOrNull()
                    val rightSource = exactRight ?: fallback.firstOrNull()
                    val leftExpr = leftSource?.let { resolveDataExpression(it.from, visiting) }
                    val rightExpr = rightSource?.let { resolveDataExpression(it.from, visiting) }
                    if (leftExpr == null && rightExpr == null) null
                    else "addValues(${leftExpr ?: "\"\""}, ${rightExpr ?: "\"\""})"
                }
                "MATH_SUB" -> {
                    val incomingDataLinks = incoming[block.id].orEmpty().filter { isDataLink(it) }
                    val exactLeft = incomingDataLinks.firstOrNull { it.toPortClass == "in-data-a" }
                    val exactRight = incomingDataLinks.firstOrNull { it.toPortClass == "in-data-b" && it !== exactLeft }
                    val fallback = incomingDataLinks.filter { it !== exactLeft && it !== exactRight }
                    val leftSource = exactLeft ?: incomingDataLinks.firstOrNull()
                    val rightSource = exactRight ?: fallback.firstOrNull()
                    val leftExpr = leftSource?.let { resolveDataExpression(it.from, visiting) }
                    val rightExpr = rightSource?.let { resolveDataExpression(it.from, visiting) }
                    if (leftExpr == null && rightExpr == null) null
                    else "subtractValues(${leftExpr ?: "0"}, ${rightExpr ?: "0"})"
                }
                "MATH_MUL" -> {
                    val incomingDataLinks = incoming[block.id].orEmpty().filter { isDataLink(it) }
                    val exactLeft = incomingDataLinks.firstOrNull { it.toPortClass == "in-data-a" }
                    val exactRight = incomingDataLinks.firstOrNull { it.toPortClass == "in-data-b" && it !== exactLeft }
                    val fallback = incomingDataLinks.filter { it !== exactLeft && it !== exactRight }
                    val leftSource = exactLeft ?: incomingDataLinks.firstOrNull()
                    val rightSource = exactRight ?: fallback.firstOrNull()
                    val leftExpr = leftSource?.let { resolveDataExpression(it.from, visiting) }
                    val rightExpr = rightSource?.let { resolveDataExpression(it.from, visiting) }
                    if (leftExpr == null && rightExpr == null) null
                    else "multiplyValues(${leftExpr ?: "0"}, ${rightExpr ?: "0"})"
                }
                "MATH_DIV" -> {
                    val incomingDataLinks = incoming[block.id].orEmpty().filter { isDataLink(it) }
                    val exactLeft = incomingDataLinks.firstOrNull { it.toPortClass == "in-data-a" }
                    val exactRight = incomingDataLinks.firstOrNull { it.toPortClass == "in-data-b" && it !== exactLeft }
                    val fallback = incomingDataLinks.filter { it !== exactLeft && it !== exactRight }
                    val leftSource = exactLeft ?: incomingDataLinks.firstOrNull()
                    val rightSource = exactRight ?: fallback.firstOrNull()
                    val leftExpr = leftSource?.let { resolveDataExpression(it.from, visiting) }
                    val rightExpr = rightSource?.let { resolveDataExpression(it.from, visiting) }
                    if (leftExpr == null && rightExpr == null) null
                    else "divideValues(${leftExpr ?: "0"}, ${rightExpr ?: "1"})"
                }
                else -> null
            }
        }

        fun resolveIncomingDataExpression(targetBlockId: String, preferredPortClass: String? = null): String? {
            val incomingDataLinks = incoming[targetBlockId].orEmpty().filter { isDataLink(it) }
            if (incomingDataLinks.isEmpty()) return null
            val preferred = preferredPortClass?.let { port ->
                incomingDataLinks.firstOrNull { it.toPortClass == port }
            }
            val candidates = if (preferred != null) listOf(preferred) + incomingDataLinks.filter { it !== preferred } else incomingDataLinks
            return candidates
                .asSequence()
                .mapNotNull { resolveDataExpression(it.from, HashSet()) }
                .firstOrNull()
        }

        fun actionStatement(block: BlockEditorBlock): String? {
            val message = javaString(block.params["message"] ?: "")
            return when (block.type.uppercase(Locale.ROOT)) {
                "ACTION_LOG_INFO", "ON_ENABLE_LOG" -> "context.getLogger().info(\\\"$message\\\");"
                "ACTION_SEND_JOINER_MESSAGE", "ON_PLAYER_JOIN_MESSAGE" -> "event.getPlayer().sendMessage(\\\"$message\\\");"
                "ACTION_SET_JOIN_MESSAGE", "ON_PLAYER_JOIN_SET_JOIN_MESSAGE" -> "event.setMessage(\\\"$message\\\");"
                "ACTION_SEND_MESSAGE", "FUNCTION_SEND_MESSAGE" -> {
                    val incomingLinks = incoming[block.id].orEmpty()
                    val linkedDataExpr = resolveIncomingDataExpression(block.id, "in-data")
                    val messageExpr = linkedDataExpr?.let { "String.valueOf($it)" } ?: "\"$message\""
                    val linkedPlayer = incomingLinks
                        .asSequence()
                        .mapNotNull { link ->
                            val sourceType = blockById[link.from]?.type?.uppercase(Locale.ROOT)
                            if (link.kind == "player" && sourceType == "EVENT_ON_PLAYER_JOIN") "event.getPlayer()" else null
                        }
                        .firstOrNull()
                    if (linkedPlayer != null) {
                        "$linkedPlayer.sendMessage($messageExpr);"
                    } else {
                        "sendMessage($messageExpr);"
                    }
                }
                "FUNCTION_BROADCAST_MESSAGE" -> {
                    val linkedDataExpr = resolveIncomingDataExpression(block.id, "in-data")
                    val messageExpr = linkedDataExpr?.let { "String.valueOf($it)" } ?: "\"$message\""
                    "broadcastMessage($messageExpr);"
                }
                "FUNCTION_WORK_CODE" -> {
                    val rawCode = block.params["code"] ?: ""
                    val lines = javaCodeLines(rawCode)
                    val inputDefs = (block.params["inputDefs"] ?: "")
                        .split(',')
                        .map { it.trim() }
                        .filter { it.matches(Regex("[A-Za-z_][A-Za-z0-9_]*")) }
                        .distinct()
                    val inputLines = inputDefs.map { name ->
                        val escaped = javaString(block.params["input_$name"] ?: "")
                        "String $name = \"$escaped\";"
                    }
                    val allLines = inputLines + lines
                    if (allLines.isEmpty()) null else allLines.joinToString("\n")
                }
                else -> null
            }
        }

        fun collectActionChain(startId: String, outLines: MutableList<String>) {
            val visited = HashSet<String>()

            fun dfs(nodeId: String) {
                val nextIds = outgoing[nodeId] ?: return
                for (nextId in nextIds) {
                    if (!visited.add(nextId)) continue
                    val node = blockById[nextId] ?: continue
                    actionStatement(node)?.let { outLines += it }
                    dfs(nextId)
                }
            }
            dfs(startId)
        }

        for (block in normalizedBlocks) {
            when (block.type.uppercase(Locale.ROOT)) {
                "EVENT_ON_ENABLE" -> collectActionChain(block.id, enableLines)
                "EVENT_ON_PLAYER_JOIN" -> {
                    val linkedDataExpr = resolveIncomingDataExpression(block.id, "in-data")
                    if (linkedDataExpr != null) {
                        joinLines += "event.setMessage(String.valueOf($linkedDataExpr));"
                    } else {
                        val message = block.params["message"]?.trim().orEmpty()
                        if (message.isNotEmpty()) {
                            joinLines += "event.setMessage(\"${javaString(message)}\");"
                        }
                    }
                    collectActionChain(block.id, joinLines)
                }
                "ON_ENABLE_LOG" -> actionStatement(block)?.let { enableLines += it }
                "ON_PLAYER_JOIN_MESSAGE", "ON_PLAYER_JOIN_SET_JOIN_MESSAGE" -> actionStatement(block)?.let { joinLines += it }
            }
        }

        val onEnableBody = if (enableLines.isEmpty()) "" else enableLines.joinToString("\n        ")
        val hasJoinListener = joinLines.isNotEmpty()
        val hasBroadcastUsage = normalizedBlocks.any { it.type.uppercase(Locale.ROOT) == "FUNCTION_BROADCAST_MESSAGE" }
        val hasAnyListener = hasJoinListener
        val listenersCode = if (hasAnyListener) {
            "return java.util.Collections.<Object>singletonList(this);"
        } else {
            "return java.util.Collections.emptyList();"
        }
        val joinHandlerCode = if (hasJoinListener) {
            """
    @org.macaroon3145.api.event.Subscribe
    public void onPlayerJoin(org.macaroon3145.api.event.PlayerJoinEvent event) {
        ${joinLines.joinToString("\n        ")}
    }
            """.trimIndent()
        } else {
            ""
        }
        val quitHandlerCode = ""
        val hasSendMessageUsage = normalizedBlocks.any {
            val type = it.type.uppercase(Locale.ROOT)
            type == "ACTION_SEND_MESSAGE" || type == "FUNCTION_SEND_MESSAGE"
        }
        val hasMathAddUsage = normalizedBlocks.any { it.type.uppercase(Locale.ROOT) == "MATH_ADD" }
        val hasMathSubUsage = normalizedBlocks.any { it.type.uppercase(Locale.ROOT) == "MATH_SUB" }
        val hasMathMulUsage = normalizedBlocks.any { it.type.uppercase(Locale.ROOT) == "MATH_MUL" }
        val hasMathDivUsage = normalizedBlocks.any { it.type.uppercase(Locale.ROOT) == "MATH_DIV" }
        val onlinePlayersFieldCode = ""
        val addValuesFunctionCode = if (hasMathAddUsage) {
            """
    private Object addValues(Object left, Object right) {
        Object l = left == null ? "" : left;
        Object r = right == null ? "" : right;
        if (l instanceof Number && r instanceof Number) {
            if (l instanceof Double || l instanceof Float || r instanceof Double || r instanceof Float) {
                return ((Number) l).doubleValue() + ((Number) r).doubleValue();
            }
            return ((Number) l).longValue() + ((Number) r).longValue();
        }
        return String.valueOf(l) + String.valueOf(r);
    }
            """.trimIndent()
        } else {
            ""
        }
        val subtractValuesFunctionCode = if (hasMathSubUsage) {
            """
    private Object subtractValues(Object left, Object right) {
        Number l = asNumber(left);
        Number r = asNumber(right);
        if (l instanceof Double || l instanceof Float || r instanceof Double || r instanceof Float) {
            return l.doubleValue() - r.doubleValue();
        }
        return l.longValue() - r.longValue();
    }
            """.trimIndent()
        } else {
            ""
        }
        val multiplyValuesFunctionCode = if (hasMathMulUsage) {
            """
    private Object multiplyValues(Object left, Object right) {
        Number l = asNumber(left);
        Number r = asNumber(right);
        if (l instanceof Double || l instanceof Float || r instanceof Double || r instanceof Float) {
            return l.doubleValue() * r.doubleValue();
        }
        return l.longValue() * r.longValue();
    }
            """.trimIndent()
        } else {
            ""
        }
        val divideValuesFunctionCode = if (hasMathDivUsage) {
            """
    private Object divideValues(Object left, Object right) {
        Number l = asNumber(left);
        Number r = asNumber(right);
        double divisor = r.doubleValue();
        if (Math.abs(divisor) < 1.0E-12) {
            return 0;
        }
        return l.doubleValue() / divisor;
    }
            """.trimIndent()
        } else {
            ""
        }
        val asNumberFunctionCode = if (hasMathSubUsage || hasMathMulUsage || hasMathDivUsage) {
            """
    private Number asNumber(Object value) {
        if (value instanceof Number) {
            return (Number) value;
        }
        if (value == null) {
            return 0;
        }
        String raw = String.valueOf(value).trim();
        if (raw.isEmpty()) {
            return 0;
        }
        try {
            if (raw.contains(".") || raw.contains("e") || raw.contains("E")) {
                return Double.parseDouble(raw);
            }
            return Long.parseLong(raw);
        } catch (Exception ignored) {
            return 0;
        }
    }
            """.trimIndent()
        } else {
            ""
        }
        val sendMessageFunctionCode = if (hasSendMessageUsage) {
            """
    private void sendMessage(String message) {
        System.out.println(message);
    }
            """.trimIndent()
        } else {
            ""
        }
        val broadcastMessageFunctionCode = if (hasBroadcastUsage) {
            """
    private void broadcastMessage(String message) {
        String normalized = message == null ? "" : message;
        org.macaroon3145.api.Server.broadcastMessage(normalized);
    }
            """.trimIndent()
        } else {
            ""
        }

        return """
package $packageName;

public final class $className implements org.macaroon3145.api.plugin.AerogelPlugin {
    ${if (variableFieldLines.isEmpty()) "" else variableFieldLines.joinToString("\n    ")}
    $onlinePlayersFieldCode

    @Override
    public void onEnable(org.macaroon3145.api.plugin.PluginContext context) {
        $onEnableBody
    }

    @Override
    public java.util.List<java.lang.Object> listeners() {
        $listenersCode
    }

$joinHandlerCode

$quitHandlerCode

    $addValuesFunctionCode

    $subtractValuesFunctionCode

    $multiplyValuesFunctionCode

    $divideValuesFunctionCode

    $asNumberFunctionCode

    $sendMessageFunctionCode

    $broadcastMessageFunctionCode
}
        """.trimIndent()
    }

    private fun javaString(raw: String): String {
        val decoded = decodeForWeb(raw)
        return buildString(decoded.length + 8) {
            decoded.forEach { ch ->
                when (ch) {
                    '\\' -> append("\\\\")
                    '"' -> append("\\\"")
                    '\n' -> append("\\n")
                    '\r' -> append("\\r")
                    '\t' -> append("\\t")
                    else -> append(ch)
                }
            }
        }
    }

    private fun decodeForWeb(raw: String): String {
        return runCatching { URLDecoder.decode(raw, StandardCharsets.UTF_8) }.getOrDefault(raw)
    }

    private fun javaIdentifier(raw: String): String {
        val base = raw.trim().ifEmpty { "value" }
        val sanitized = buildString(base.length + 4) {
            base.forEachIndexed { index, ch ->
                val candidate = when {
                    ch.isLetterOrDigit() || ch == '_' -> ch
                    else -> '_'
                }
                if (index == 0) {
                    if (!candidate.isLetter() && candidate != '_') append('_')
                }
                append(candidate)
            }
        }.trim('_').ifEmpty { "value" }
        return if (sanitized.first().isDigit()) "_$sanitized" else sanitized
    }
}

@Serializable
private data class BlockEditorSaveRequest(
    val pluginId: String,
    val pluginName: String = "",
    val version: String = "1.0.0",
    val blocks: List<BlockEditorBlock> = emptyList(),
    val links: List<BlockEditorLink> = emptyList()
)

@Serializable
private data class BlockEditorBlock(
    val id: String = "",
    val type: String,
    val params: Map<String, String> = emptyMap(),
    val x: Int = 40,
    val y: Int = 40
)

@Serializable
private data class BlockEditorLink(
    val from: String,
    val to: String,
    val kind: String? = null,
    val fromPortClass: String? = null,
    val toPortClass: String? = null
)

@Serializable
private data class BlockPluginDescriptor(
    val id: String,
    val name: String,
    val version: String,
    val apiVersion: String,
    val mainClass: String,
    val dependencies: List<String>,
    val softDependencies: List<String>
)

private data class BlockEditorSaveResult(
    val pluginId: String,
    val jarPath: String,
    val mainClass: String,
    val blockCount: Int
)
