package org.macaroon3145

import java.io.PrintStream

class ConsoleSpinner(
    private val out: PrintStream,
    private val frames: Array<String>,
    private val ttyIntervalNanos: Long = 125_000_000L,
    private val nonTtyIntervalNanos: Long = 1_000_000_000L,
    private val interactive: Boolean = System.console() != null
) {
    private val startedNanos = System.nanoTime()
    private val intervalNanos = if (interactive) ttyIntervalNanos else nonTtyIntervalNanos
    private var lastRenderNanos = 0L
    private var frameIndex = 0
    private var lastLineLength = 0
    private var rendered = false

    fun tick(lineBuilder: (frame: String, elapsedSeconds: Double) -> String, nowNanos: Long = System.nanoTime()) {
        if (nowNanos - lastRenderNanos < intervalNanos) return
        lastRenderNanos = nowNanos
        val frame = frames[frameIndex]
        frameIndex = (frameIndex + 1) % frames.size
        val line = lineBuilder(frame, elapsedSeconds(nowNanos))
        render(line)
    }

    fun finish(doneMark: String = "✓", lineBuilder: (mark: String, elapsedSeconds: Double) -> String): Double {
        val elapsed = elapsedSeconds(System.nanoTime())
        val line = lineBuilder(doneMark, elapsed)
        render(line)
        if (interactive) {
            out.print("\n")
        }
        out.flush()
        return elapsed
    }

    fun clear() {
        if (!interactive || !rendered) return
        out.print("\r${" ".repeat(lastLineLength)}\r")
        out.flush()
        rendered = false
        lastLineLength = 0
    }

    private fun elapsedSeconds(nowNanos: Long): Double {
        return (nowNanos - startedNanos) / 1_000_000_000.0
    }

    private fun render(line: String) {
        if (interactive) {
            val paddedLine = if (line.length < lastLineLength) {
                line + " ".repeat(lastLineLength - line.length)
            } else {
                line
            }
            out.print("\r$paddedLine")
            out.flush()
            rendered = true
            lastLineLength = line.length
            return
        }
        out.println(line)
        out.flush()
    }
}
