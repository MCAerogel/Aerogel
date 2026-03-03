plugins {
    kotlin("jvm") version "2.3.0"
    kotlin("plugin.serialization") version "2.3.0"
    id("io.papermc.paperweight.userdev") version "2.0.0-beta.19"
}

group = "org.macaroon3145"
version = "1.0-SNAPSHOT"

val skikoVersion = "0.9.18"

repositories {
    mavenCentral()
    maven("https://repo.papermc.io/repository/maven-public/")
}

dependencies {
    testImplementation(kotlin("test"))
    implementation(project(":api"))

    implementation("io.netty:netty-all:4.2.10.Final")
    runtimeOnly("io.netty:netty-transport-native-epoll:4.2.10.Final:linux-x86_64")
    runtimeOnly("io.netty:netty-transport-native-epoll:4.2.10.Final:linux-aarch_64")
    runtimeOnly("io.netty:netty-transport-native-kqueue:4.2.10.Final:osx-x86_64")
    runtimeOnly("io.netty:netty-transport-native-kqueue:4.2.10.Final:osx-aarch_64")

    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.10.0")
    implementation("com.google.code.gson:gson:2.11.0")
    implementation("org.jline:jline:3.26.3")
    implementation("com.formdev:flatlaf:3.4.1")
    implementation("org.slf4j:slf4j-api:2.0.16")
    implementation("org.recast4j:recast:1.5.11")
    implementation("org.recast4j:detour:1.5.11")
    implementation("org.ow2.asm:asm:9.7.1")
    implementation("org.ow2.asm:asm-commons:9.7.1")
    implementation("net.bytebuddy:byte-buddy-agent:1.15.10")
    runtimeOnly("org.slf4j:slf4j-simple:2.0.16")
    implementation("org.jetbrains.skiko:skiko-awt:$skikoVersion")
    runtimeOnly("org.jetbrains.skiko:skiko-awt-runtime-windows-x64:$skikoVersion")
    runtimeOnly("org.jetbrains.skiko:skiko-awt-runtime-windows-arm64:$skikoVersion")
    runtimeOnly("org.jetbrains.skiko:skiko-awt-runtime-macos-x64:$skikoVersion")
    runtimeOnly("org.jetbrains.skiko:skiko-awt-runtime-macos-arm64:$skikoVersion")
    runtimeOnly("org.jetbrains.skiko:skiko-awt-runtime-linux-x64:$skikoVersion")
    runtimeOnly("org.jetbrains.skiko:skiko-awt-runtime-linux-arm64:$skikoVersion")

    // Mojang-mapped NMS classes via Paper userdev.
    paperweight.paperDevBundle("1.21.11-R0.1-SNAPSHOT")
}

val foliaPlugin by sourceSets.creating

configurations.named("${foliaPlugin.name}Implementation") {
    extendsFrom(configurations["implementation"])
}
configurations.named("${foliaPlugin.name}CompileOnly") {
    extendsFrom(configurations["compileOnly"])
}

kotlin {
    jvmToolchain(25)
}

tasks.test {
    useJUnitPlatform()
}

tasks.register<Jar>("aerogelJar") {
    group = "build"
    description = "Builds Aerogel.jar"
    dependsOn("classes")
    dependsOn(":api:jar")
    archiveBaseName.set("Aerogel")
    archiveVersion.set("")
    archiveClassifier.set("")
    destinationDirectory.set(layout.buildDirectory.dir("libs"))
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE

    val foliaPluginJar = tasks.named<Jar>("foliaPluginJar")
    dependsOn(foliaPluginJar)

    manifest {
        attributes["Main-Class"] = "org.macaroon3145.MainKt"
    }

    from(sourceSets.main.get().output)
    from(foliaPluginJar.flatMap { it.archiveFile }) {
        into("embedded")
        rename { "AerogelFoliaBridge.jar" }
    }
    from(
        configurations.runtimeClasspath.get().mapNotNull { dependency ->
            val name = dependency.name.lowercase()
            val path = dependency.path.lowercase()
            val isNmsOrUserdevRuntime =
                name.contains("dev-bundle") ||
                    name.contains("paper-api") ||
                    name.contains("mache") ||
                    name.contains("minecraft-server") ||
                    path.contains("/io/papermc/") ||
                    path.contains("/net/minecraft/")
            if (isNmsOrUserdevRuntime) {
                null
            } else if (dependency.isDirectory) {
                dependency
            } else {
                zipTree(dependency)
            }
        }
    )
    exclude("META-INF/*.SF", "META-INF/*.DSA", "META-INF/*.RSA")
}

tasks.register<Jar>("foliaPluginJar") {
    group = "build"
    description = "Builds AerogelFoliaBridge.jar"
    dependsOn(tasks.named(foliaPlugin.classesTaskName))
    archiveBaseName.set("AerogelFoliaBridge")
    archiveVersion.set("")
    archiveClassifier.set("")
    destinationDirectory.set(layout.buildDirectory.dir("libs"))
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE

    from(foliaPlugin.output)
}

tasks.named("build") {
    dependsOn("aerogelJar", "foliaPluginJar")
}

val aerogelJarTask = tasks.named<Jar>("aerogelJar")
val aerogelRunJvmArgs = listOf(
    "--enable-native-access=ALL-UNNAMED",
    "--sun-misc-unsafe-memory-access=allow",
    "-Xms4G",
    "-Xmx4G"
)

tasks.register<Exec>("runAerogelJar") {
    group = "application"
    description = "Builds Aerogel.jar and runs it with java -jar in the current terminal."
    dependsOn(aerogelJarTask)
    workingDir = project.projectDir
    standardInput = System.`in`
    standardOutput = System.out
    errorOutput = System.err
    isIgnoreExitValue = false

    doFirst {
        val jarFile = aerogelJarTask.get().archiveFile.get().asFile
        commandLine(
            listOf("java") +
                aerogelRunJvmArgs +
                listOf("-jar", jarFile.absolutePath)
        )
    }
}

tasks.register<Exec>("runAerogelJarInOsTerminal") {
    group = "application"
    description = "Builds Aerogel.jar and launches it in a new OS terminal window."
    dependsOn(aerogelJarTask)
    workingDir = project.projectDir
    isIgnoreExitValue = false

    doFirst {
        val jarFile = aerogelJarTask.get().archiveFile.get().asFile
        val jarPath = jarFile.absolutePath
        val os = System.getProperty("os.name").lowercase()

        when {
            os.contains("win") -> {
                val javaCommand = (
                    listOf("java") +
                        aerogelRunJvmArgs +
                        listOf("-jar", "\"$jarPath\"")
                    ).joinToString(" ")
                // Opens a new cmd window and keeps it open after process exit.
                commandLine(
                    "cmd", "/c",
                    "start", "\"Aerogel\"",
                    "cmd", "/k",
                    javaCommand
                )
            }
            os.contains("mac") -> {
                val escapedProjectDir = project.projectDir.absolutePath.replace("\"", "\\\"")
                val escapedJarPath = jarPath.replace("\"", "\\\"")
                val javaCommand = (
                    listOf("java") +
                        aerogelRunJvmArgs +
                        listOf("-jar", "\"$escapedJarPath\"")
                    ).joinToString(" ")
                commandLine(
                    "osascript", "-e",
                    "tell application \"Terminal\" to do script \"cd \\\"$escapedProjectDir\\\"; $javaCommand\""
                )
            }
            else -> {
                // Linux/Unix: tries common terminal emulators.
                val escapedProjectDir = project.projectDir.absolutePath.replace("'", "'\"'\"'")
                val escapedJarPath = jarPath.replace("'", "'\"'\"'")
                val javaCommand = (
                    listOf("java") +
                        aerogelRunJvmArgs +
                        listOf("-jar", "\"$escapedJarPath\"")
                    ).joinToString(" ")
                val script = """
                    if command -v x-terminal-emulator >/dev/null 2>&1; then
                      x-terminal-emulator -e sh -lc 'cd "$escapedProjectDir"; $javaCommand; exec sh'
                    elif command -v gnome-terminal >/dev/null 2>&1; then
                      gnome-terminal -- sh -lc 'cd "$escapedProjectDir"; $javaCommand; exec sh'
                    elif command -v konsole >/dev/null 2>&1; then
                      konsole -e sh -lc 'cd "$escapedProjectDir"; $javaCommand; exec sh'
                    elif command -v xfce4-terminal >/dev/null 2>&1; then
                      xfce4-terminal --command="sh -lc 'cd \"$escapedProjectDir\"; $javaCommand; exec sh'"
                    elif command -v xterm >/dev/null 2>&1; then
                      xterm -e sh -lc 'cd "$escapedProjectDir"; $javaCommand; exec sh'
                    else
                      echo "No terminal emulator found. Install one or run: java ${aerogelRunJvmArgs.joinToString(" ")} -jar \"$jarPath\""
                      exit 1
                    fi
                """.trimIndent()
                commandLine("sh", "-c", script)
            }
        }
    }
}
