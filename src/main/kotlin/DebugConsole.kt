package org.macaroon3145

import java.io.FileOutputStream
import java.io.PrintStream
import java.nio.charset.StandardCharsets
import java.util.Locale
import java.util.concurrent.atomic.AtomicBoolean
import org.macaroon3145.i18n.ServerI18n
import kotlin.concurrent.thread

object DebugConsole {
    private val rawOut = PrintStream(FileOutputStream(java.io.FileDescriptor.out), true, StandardCharsets.UTF_8)
    private val rawErr = PrintStream(FileOutputStream(java.io.FileDescriptor.err), true, StandardCharsets.UTF_8)
    private val ubuntuBrailleSpinnerFrames = arrayOf("⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏")

    fun newSpinner(): ConsoleSpinner {
        return ConsoleSpinner(
            out = rawOut,
            frames = ubuntuBrailleSpinnerFrames
        )
    }

    fun localizedElapsed(seconds: Double): String {
        return String.format(Locale.ROOT, "%.2f%s", seconds, ServerI18n.tr("aerogel.unit.seconds.short"))
    }

    inline fun <T> withSpinner(
        progressMessage: String,
        doneMessage: String = progressMessage,
        action: () -> T
    ): T {
        val spinner = newSpinner()
        val running = AtomicBoolean(true)
        val spinnerThread = thread(name = "aerogel-console-spinner", isDaemon = true) {
            while (running.get()) {
                spinner.tick(
                    lineBuilder = { frame, elapsedSeconds ->
                        "$frame $progressMessage (${localizedElapsed(elapsedSeconds)})"
                    }
                )
                Thread.sleep(40)
            }
        }
        return try {
            val result = action()
            running.set(false)
            spinnerThread.join()
            spinner.finish { mark, elapsedSeconds ->
                "$mark $doneMessage (${localizedElapsed(elapsedSeconds)})"
            }
            result
        } catch (error: Throwable) {
            running.set(false)
            spinnerThread.join()
            spinner.finish(doneMark = "✗") { mark, elapsedSeconds ->
                "$mark $progressMessage (${localizedElapsed(elapsedSeconds)})"
            }
            throw error
        }
    }

    inline fun <T> withSpinnerResult(
        progressMessage: String,
        crossinline doneMessage: (T) -> String,
        crossinline doneMark: (T) -> String = { "✓" },
        action: () -> T
    ): T {
        val spinner = newSpinner()
        val running = AtomicBoolean(true)
        val spinnerThread = thread(name = "aerogel-console-spinner", isDaemon = true) {
            while (running.get()) {
                spinner.tick(
                    lineBuilder = { frame, elapsedSeconds ->
                        "$frame $progressMessage (${localizedElapsed(elapsedSeconds)})"
                    }
                )
                Thread.sleep(40)
            }
        }
        return try {
            val result = action()
            running.set(false)
            spinnerThread.join()
            spinner.finish(doneMark = doneMark(result)) { mark, elapsedSeconds ->
                "$mark ${doneMessage(result)} (${localizedElapsed(elapsedSeconds)})"
            }
            result
        } catch (error: Throwable) {
            running.set(false)
            spinnerThread.join()
            spinner.finish(doneMark = "✗") { mark, elapsedSeconds ->
                "$mark $progressMessage (${localizedElapsed(elapsedSeconds)})"
            }
            throw error
        }
    }

    fun err(line: String) {
        rawErr.println(line)
    }

    fun errThrowable(prefix: String, throwable: Throwable?, maxDepth: Int = 3, maxFrames: Int = 20) {
        if (throwable == null) {
            err("$prefix <null>")
            return
        }
        var depth = 0
        var cur: Throwable? = throwable
        while (cur != null && depth < maxDepth) {
            err("$prefix cause[$depth]: ${cur.javaClass.name}: ${cur.message}")
            val frames = cur.stackTrace
            val limit = minOf(frames.size, maxFrames)
            for (i in 0 until limit) {
                err("$prefix   at ${frames[i]}")
            }
            cur = cur.cause
            depth++
        }
    }
}
