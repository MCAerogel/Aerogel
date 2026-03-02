package org.macaroon3145.ui

import com.formdev.flatlaf.FlatDarkLaf
import com.formdev.flatlaf.FlatLightLaf
import org.macaroon3145.config.ServerConfig
import org.macaroon3145.i18n.ServerI18n
import org.macaroon3145.network.handler.PlayerSessionManager
import org.macaroon3145.perf.PerformanceMonitor
import org.macaroon3145.world.WorldManager
import java.awt.BasicStroke
import java.awt.BorderLayout
import java.awt.Color
import java.awt.Dimension
import java.awt.FlowLayout
import java.awt.Font
import java.awt.GradientPaint
import java.awt.Graphics
import java.awt.Graphics2D
import java.awt.GraphicsEnvironment
import java.awt.GridLayout
import java.awt.RenderingHints
import java.awt.geom.Path2D
import java.io.BufferedReader
import java.io.InputStreamReader
import java.text.DecimalFormat
import java.util.Locale
import java.util.concurrent.atomic.AtomicBoolean
import javax.swing.BorderFactory
import javax.swing.JFrame
import javax.swing.JLabel
import javax.swing.JPanel
import javax.swing.JScrollPane
import javax.swing.JTable
import javax.swing.SwingConstants
import javax.swing.SwingUtilities
import javax.swing.UIManager
import javax.swing.table.AbstractTableModel
import kotlin.math.max

object ServerDashboard {
    private const val GRAPH_HISTORY = 36_000 // 30 minutes at 20 TPS
    private const val MAX_CHUNK_LINES = 64
    private const val MIN_LISTED_MSPT = 1.0e-9

    private val started = AtomicBoolean(false)
    private var frame: JFrame? = null

    private lateinit var memoryLabel: JLabel
    private lateinit var tpsLabel: JLabel
    private lateinit var msptLabel: JLabel
    private lateinit var playersLabel: JLabel

    private lateinit var chunkTableModel: ChunkTableModel

    private lateinit var memoryGraph: MetricGraphPanel
    private lateinit var tpsGraph: MetricGraphPanel
    private lateinit var msptGraph: MetricGraphPanel
    private lateinit var playersGraph: MetricGraphPanel

    fun start() {
        if (!started.compareAndSet(false, true)) return
        if (GraphicsEnvironment.isHeadless()) return

        SwingUtilities.invokeLater {
            runCatching {
                installLookAndFeel()
                buildUi()
            }.onFailure {
                it.printStackTrace()
            }
        }
    }

    fun onTick() {
        if (!started.get()) return
        if (GraphicsEnvironment.isHeadless()) return

        val runtime = Runtime.getRuntime()
        val usedBytes = (runtime.totalMemory() - runtime.freeMemory()).coerceAtLeast(0L)
        val allocatedBytes = runtime.maxMemory().coerceAtLeast(1L)
        val tps = PerformanceMonitor.tps.coerceAtLeast(0.0)
        val mspt = PerformanceMonitor.mspt.coerceAtLeast(0.0)
        val onlinePlayers = PlayerSessionManager.onlineCount().coerceAtLeast(0)
        val chunkRows = collectTopChunks(MAX_CHUNK_LINES)

        val snapshot = TickSnapshot(
            usedBytes = usedBytes,
            allocatedBytes = allocatedBytes,
            tps = tps,
            mspt = mspt,
            players = onlinePlayers,
            chunks = chunkRows
        )

        SwingUtilities.invokeLater {
            val currentFrame = frame
            if (currentFrame == null || !currentFrame.isDisplayable) return@invokeLater
            applySnapshot(snapshot)
        }
    }

    private fun collectTopChunks(limit: Int): List<ChunkRow> {
        if (limit <= 0) return emptyList()

        val rows = ArrayList<ChunkRow>(limit)
        for (world in WorldManager.allWorlds()) {
            val stats = world.topChunkStatsByEwmaMspt(limit = limit, minMspt = MIN_LISTED_MSPT)
            for (entry in stats) {
                if (world.isChunkIdle(entry.chunkPos.x, entry.chunkPos.z)) continue
                rows += ChunkRow(
                    world = displayWorldName(world.key),
                    chunkX = entry.chunkPos.x,
                    chunkZ = entry.chunkPos.z,
                    tps = entry.tps,
                    mspt = entry.mspt
                )
            }
        }

        if (rows.isEmpty()) return emptyList()

        rows.sortWith(
            compareByDescending<ChunkRow> { it.mspt }
                .thenByDescending { it.tps }
                .thenBy { it.world }
                .thenBy { it.chunkX }
                .thenBy { it.chunkZ }
        )

        return if (rows.size <= limit) rows else rows.subList(0, limit)
    }

    private fun displayWorldName(worldKey: String): String {
        return if (worldKey.startsWith("minecraft:")) worldKey.substringAfter(':') else worldKey
    }

    private fun applySnapshot(snapshot: TickSnapshot) {
        val usedMb = bytesToMb(snapshot.usedBytes)
        val allocatedMb = bytesToMb(snapshot.allocatedBytes)

        memoryLabel.text = ServerI18n.tr(
            "aerogel.dashboard.card.memory.value",
            formatDouble(usedMb),
            formatDouble(allocatedMb)
        )
        tpsLabel.text = ServerI18n.tr("aerogel.dashboard.card.tps.value", formatDouble(snapshot.tps))
        msptLabel.text = ServerI18n.tr("aerogel.dashboard.card.mspt.value", formatDouble(snapshot.mspt))
        playersLabel.text = ServerI18n.tr(
            "aerogel.dashboard.card.players.value",
            snapshot.players.toString(),
            ServerConfig.maxPlayers.toString()
        )

        chunkTableModel.setRows(snapshot.chunks)

        memoryGraph.push(usedMb, max(allocatedMb, 1.0))
        tpsGraph.push(snapshot.tps, max(targetTpsUpperBound(), 1.0))
        msptGraph.push(snapshot.mspt)
        playersGraph.push(snapshot.players.toDouble(), max(ServerConfig.maxPlayers.toDouble(), 1.0))
    }

    private fun targetTpsUpperBound(): Double {
        val configured = ServerConfig.maxTps
        return if (configured > 0.0) configured else 20.0
    }

    private fun installLookAndFeel() {
        val dark = osPrefersDarkMode()
        runCatching {
            if (dark) {
                FlatDarkLaf.setup()
            } else {
                FlatLightLaf.setup()
            }
        }.onFailure {
            // Fallback to default Swing LAF if FlatLaf setup fails.
            runCatching { UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName()) }
        }
    }

    private fun buildUi() {
        val top = JPanel(GridLayout(1, 4, 8, 0))
        top.border = BorderFactory.createEmptyBorder(10, 10, 10, 10)

        memoryLabel = metricCard(ServerI18n.tr("aerogel.dashboard.card.memory"))
        tpsLabel = metricCard(ServerI18n.tr("aerogel.dashboard.card.tps"))
        msptLabel = metricCard(ServerI18n.tr("aerogel.dashboard.card.mspt"))
        playersLabel = metricCard(ServerI18n.tr("aerogel.dashboard.card.players"))

        top.add(wrapCard(memoryLabel))
        top.add(wrapCard(tpsLabel))
        top.add(wrapCard(msptLabel))
        top.add(wrapCard(playersLabel))

        chunkTableModel = ChunkTableModel()
        val table = JTable(chunkTableModel).apply {
            rowHeight = 22
            autoCreateRowSorter = true
            fillsViewportHeight = true
        }
        val tablePane = JScrollPane(table).apply {
            border = BorderFactory.createTitledBorder(ServerI18n.tr("aerogel.dashboard.table.title"))
            preferredSize = Dimension(900, 300)
        }
        val tableWrap = JPanel(BorderLayout()).apply {
            border = BorderFactory.createEmptyBorder(0, 8, 0, 8)
            add(tablePane, BorderLayout.CENTER)
        }

        val graphGrid = JPanel(GridLayout(2, 2, 8, 8)).apply {
            border = BorderFactory.createEmptyBorder(10, 10, 10, 10)
        }

        memoryGraph = MetricGraphPanel(Color(70, 170, 255), baselineMin = 1.0, valueUnit = ServerI18n.tr("aerogel.dashboard.graph.unit.mb"))
        tpsGraph = MetricGraphPanel(Color(72, 201, 176), baselineMin = 20.0, valueUnit = ServerI18n.tr("aerogel.dashboard.graph.unit.tps"))
        msptGraph = MetricGraphPanel(Color(255, 159, 67), baselineMin = 50.0, valueUnit = ServerI18n.tr("aerogel.dashboard.graph.unit.ms"))
        playersGraph = MetricGraphPanel(Color(245, 113, 193), baselineMin = 1.0, valueUnit = ServerI18n.tr("aerogel.dashboard.graph.unit.players"))

        graphGrid.add(createGraphCard(ServerI18n.tr("aerogel.dashboard.graph.memory"), memoryGraph))
        graphGrid.add(createGraphCard(ServerI18n.tr("aerogel.dashboard.graph.tps"), tpsGraph))
        graphGrid.add(createGraphCard(ServerI18n.tr("aerogel.dashboard.graph.mspt"), msptGraph))
        graphGrid.add(createGraphCard(ServerI18n.tr("aerogel.dashboard.graph.players"), playersGraph))

        val center = JPanel(BorderLayout()).apply {
            add(tableWrap, BorderLayout.CENTER)
            add(graphGrid, BorderLayout.SOUTH)
        }

        val dashboardFrame = JFrame(ServerI18n.tr("aerogel.dashboard.title")).apply {
            defaultCloseOperation = JFrame.HIDE_ON_CLOSE
            layout = BorderLayout()
            add(top, BorderLayout.NORTH)
            add(center, BorderLayout.CENTER)
            minimumSize = Dimension(1080, 780)
            preferredSize = Dimension(1200, 820)
            setLocationRelativeTo(null)
            isVisible = true
        }
        frame = dashboardFrame
    }

    private fun metricCard(title: String): JLabel {
        return JLabel("$title: --", SwingConstants.LEFT).apply {
            font = font.deriveFont(Font.BOLD, 14f)
            border = BorderFactory.createEmptyBorder(8, 10, 8, 10)
        }
    }

    private fun wrapCard(content: JLabel): JPanel {
        return JPanel(FlowLayout(FlowLayout.LEFT, 0, 0)).apply {
            border = BorderFactory.createCompoundBorder(
                BorderFactory.createLineBorder(UIManager.getColor("Component.borderColor") ?: Color.GRAY),
                BorderFactory.createEmptyBorder(6, 6, 6, 6)
            )
            add(content)
        }
    }

    private fun bytesToMb(bytes: Long): Double = bytes / (1024.0 * 1024.0)

    private fun formatDouble(value: Double): String = DECIMAL_2.format(value)

    private fun osPrefersDarkMode(): Boolean {
        val os = System.getProperty("os.name")?.lowercase(Locale.ROOT).orEmpty()
        return when {
            os.contains("win") -> windowsPrefersDarkMode()
            os.contains("mac") -> macPrefersDarkMode()
            else -> linuxPrefersDarkMode()
        }
    }

    private fun windowsPrefersDarkMode(): Boolean {
        val output = runCommand(
            "reg",
            "query",
            "HKCU\\Software\\Microsoft\\Windows\\CurrentVersion\\Themes\\Personalize",
            "/v",
            "AppsUseLightTheme"
        ) ?: return false
        return output.lines().any { line ->
            line.contains("AppsUseLightTheme", ignoreCase = true) &&
                line.trim().endsWith("0")
        }
    }

    private fun macPrefersDarkMode(): Boolean {
        val output = runCommand("defaults", "read", "-g", "AppleInterfaceStyle") ?: return false
        return output.contains("Dark", ignoreCase = true)
    }

    private fun linuxPrefersDarkMode(): Boolean {
        val gtkTheme = System.getenv("GTK_THEME")
        if (!gtkTheme.isNullOrBlank() && gtkTheme.lowercase(Locale.ROOT).contains("dark")) {
            return true
        }

        val colorScheme = runCommand("gsettings", "get", "org.gnome.desktop.interface", "color-scheme")
        if (!colorScheme.isNullOrBlank() && colorScheme.contains("prefer-dark", ignoreCase = true)) {
            return true
        }

        val gnomeTheme = runCommand("gsettings", "get", "org.gnome.desktop.interface", "gtk-theme")
        return !gnomeTheme.isNullOrBlank() && gnomeTheme.contains("dark", ignoreCase = true)
    }

    private fun runCommand(vararg command: String): String? {
        return runCatching {
            val process = ProcessBuilder(*command)
                .redirectErrorStream(true)
                .start()
            BufferedReader(InputStreamReader(process.inputStream)).use { reader ->
                val output = reader.readText().trim()
                val exit = process.waitFor()
                if (exit == 0) output else null
            }
        }.getOrNull()
    }

    private data class TickSnapshot(
        val usedBytes: Long,
        val allocatedBytes: Long,
        val tps: Double,
        val mspt: Double,
        val players: Int,
        val chunks: List<ChunkRow>
    )

    private data class ChunkRow(
        val world: String,
        val chunkX: Int,
        val chunkZ: Int,
        val tps: Double,
        val mspt: Double
    )

    private data class GraphWindowOption(
        val minutes: Int,
        private val label: String
    ) {
        override fun toString(): String = label
    }

    private fun visibleWindowSamples(windowMinutes: Int): Int {
        val targetTps = targetTpsUpperBound().coerceAtLeast(1.0)
        val samplesPerSecond = targetTps.toInt().coerceAtLeast(1)
        return (windowMinutes * 60 * samplesPerSecond).coerceAtLeast(2)
    }

    private fun createGraphCard(title: String, graphPanel: MetricGraphPanel): JPanel {
        val combo = javax.swing.JComboBox(
            arrayOf(
                GraphWindowOption(3, ServerI18n.tr("aerogel.dashboard.graph.window.3m")),
                GraphWindowOption(5, ServerI18n.tr("aerogel.dashboard.graph.window.5m")),
                GraphWindowOption(10, ServerI18n.tr("aerogel.dashboard.graph.window.10m")),
                GraphWindowOption(30, ServerI18n.tr("aerogel.dashboard.graph.window.30m"))
            )
        ).apply {
            selectedIndex = 0
            addActionListener {
                val selected = selectedItem as? GraphWindowOption ?: return@addActionListener
                graphPanel.setWindowMinutes(selected.minutes)
            }
        }
        val titleLabel = JLabel(title).apply {
            font = font.deriveFont(Font.BOLD, 12f)
        }
        val controls = JPanel(BorderLayout()).apply {
            isOpaque = false
            border = BorderFactory.createEmptyBorder(4, 6, 0, 6)
            add(titleLabel, BorderLayout.WEST)
            add(combo, BorderLayout.EAST)
        }
        return JPanel(BorderLayout()).apply {
            border = BorderFactory.createCompoundBorder(
                BorderFactory.createLineBorder(UIManager.getColor("Component.borderColor") ?: Color.GRAY),
                BorderFactory.createEmptyBorder(4, 4, 4, 4)
            )
            add(controls, BorderLayout.NORTH)
            add(graphPanel, BorderLayout.CENTER)
        }
    }

    private class ChunkTableModel : AbstractTableModel() {
        private val columns = arrayOf(
            ServerI18n.tr("aerogel.dashboard.table.column.world"),
            ServerI18n.tr("aerogel.dashboard.table.column.chunk_x"),
            ServerI18n.tr("aerogel.dashboard.table.column.chunk_z"),
            ServerI18n.tr("aerogel.dashboard.table.column.tps"),
            ServerI18n.tr("aerogel.dashboard.table.column.mspt")
        )
        private var rows: List<ChunkRow> = emptyList()

        fun setRows(nextRows: List<ChunkRow>) {
            rows = nextRows
            fireTableDataChanged()
        }

        override fun getRowCount(): Int = rows.size

        override fun getColumnCount(): Int = columns.size

        override fun getColumnName(column: Int): String = columns[column]

        override fun getValueAt(rowIndex: Int, columnIndex: Int): Any {
            val row = rows[rowIndex]
            return when (columnIndex) {
                0 -> row.world
                1 -> row.chunkX
                2 -> row.chunkZ
                3 -> DECIMAL_2.format(row.tps)
                4 -> DECIMAL_2.format(row.mspt)
                else -> ""
            }
        }
    }

    private class MetricGraphPanel(
        private val lineColor: Color,
        private val baselineMin: Double = 1.0,
        private val valueUnit: String
    ) : JPanel() {
        private val history = ArrayDeque<Double>(GRAPH_HISTORY)
        private var fixedUpperBound: Double? = null
        @Volatile private var windowMinutes: Int = 3
        @Volatile private var hoveredIndex: Int? = null

        init {
            preferredSize = Dimension(420, 160)
            minimumSize = Dimension(300, 130)
            border = BorderFactory.createEmptyBorder(4, 4, 4, 4)
            addMouseMotionListener(object : java.awt.event.MouseMotionAdapter() {
                override fun mouseMoved(e: java.awt.event.MouseEvent) {
                    hoveredIndex = hoverIndexForEvent(e)
                    repaint()
                }
            })
            addMouseListener(object : java.awt.event.MouseAdapter() {
                override fun mouseExited(e: java.awt.event.MouseEvent) {
                    hoveredIndex = null
                    repaint()
                }
            })
        }

        fun push(value: Double, upperBound: Double? = null) {
            val safe = if (value.isFinite()) value else 0.0
            if (history.size >= GRAPH_HISTORY) {
                history.removeFirst()
            }
            history.addLast(safe)
            fixedUpperBound = upperBound
            repaint()
        }

        fun setWindowMinutes(minutes: Int) {
            windowMinutes = minutes.coerceAtLeast(1)
            repaint()
        }

        private fun formatSecondsAgo(seconds: Double): String {
            val total = seconds.coerceAtLeast(0.0).toInt()
            val min = total / 60
            val sec = total % 60
            return String.format("%d:%02d", min, sec)
        }

        private fun windowedSamples(): List<Double> {
            val windowSamples = visibleWindowSamples(windowMinutes)
            val source = if (history.size <= windowSamples) {
                history.toList()
            } else {
                history.drop(history.size - windowSamples)
            }
            return if (source.size < windowSamples) {
                val padded = ArrayList<Double>(windowSamples)
                repeat(windowSamples - source.size) { padded.add(0.0) }
                padded.addAll(source)
                padded
            } else {
                source
            }
        }

        override fun paintComponent(g: Graphics) {
            super.paintComponent(g)
            val g2 = g.create() as Graphics2D
            try {
                g2.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON)

                val insets = insets
                val x0 = insets.left
                val y0 = insets.top
                val w = width - insets.left - insets.right
                val h = height - insets.top - insets.bottom
                if (w <= 10 || h <= 10) return

                g2.color = UIManager.getColor("Panel.background") ?: background
                g2.fillRoundRect(x0, y0, w, h, 12, 12)

                if (history.isEmpty()) return

                val chartX = x0 + 8
                val chartY = y0 + 4
                val chartW = w - 16
                val chartH = h - 8
                if (chartW < 10 || chartH < 10) return

                val upper = (fixedUpperBound ?: history.maxOrNull() ?: baselineMin).coerceAtLeast(baselineMin)
                val lower = 0.0
                val range = (upper - lower).coerceAtLeast(1e-6)

                g2.color = (UIManager.getColor("Component.borderColor") ?: Color.GRAY)
                g2.stroke = BasicStroke(1f)
                g2.drawRoundRect(chartX, chartY, chartW, chartH, 8, 8)

                val list = windowedSamples()
                val points = list.size
                if (points < 2) return

                val path = Path2D.Double()
                var lastX = 0.0
                for (i in 0 until points) {
                    val value = list[i].coerceIn(lower, upper)
                    val x = chartX + (i.toDouble() / (points - 1).toDouble()) * chartW
                    val y = chartY + chartH - ((value - lower) / range) * chartH
                    if (i == 0) {
                        path.moveTo(x, y)
                    } else {
                        path.lineTo(x, y)
                    }
                    lastX = x
                }

                val area = Path2D.Double(path)
                area.lineTo(lastX, chartY + chartH.toDouble())
                area.lineTo(chartX.toDouble(), chartY + chartH.toDouble())
                area.closePath()

                val fill = GradientPaint(
                    chartX.toFloat(),
                    chartY.toFloat(),
                    Color(lineColor.red, lineColor.green, lineColor.blue, 80),
                    chartX.toFloat(),
                    (chartY + chartH).toFloat(),
                    Color(lineColor.red, lineColor.green, lineColor.blue, 8)
                )
                g2.paint = fill
                g2.fill(area)

                g2.color = lineColor
                g2.stroke = BasicStroke(2f)
                g2.draw(path)

                val hovered = hoveredIndex
                if (hovered != null && hovered in 0 until points) {
                    val hv = list[hovered].coerceIn(lower, upper)
                    val hx = chartX + (hovered.toDouble() / (points - 1).toDouble()) * chartW
                    val hy = chartY + chartH - ((hv - lower) / range) * chartH
                    g2.color = Color(lineColor.red, lineColor.green, lineColor.blue, 90)
                    g2.stroke = BasicStroke(1f)
                    g2.drawLine(hx.toInt(), chartY, hx.toInt(), chartY + chartH)
                    g2.color = lineColor
                    g2.fillOval((hx - 4.5).toInt(), (hy - 4.5).toInt(), 9, 9)
                    g2.color = UIManager.getColor("Panel.background") ?: background
                    g2.stroke = BasicStroke(1.4f)
                    g2.drawOval((hx - 4.5).toInt(), (hy - 4.5).toInt(), 9, 9)

                    val detail = detailTextForIndex(hovered, list)
                    if (detail.isNotEmpty()) {
                        g2.font = g2.font.deriveFont(Font.PLAIN, 11f)
                        val fm = g2.fontMetrics
                        val padX = 8
                        val padY = 5
                        val boxW = fm.stringWidth(detail) + (padX * 2)
                        val boxH = fm.height + (padY * 2)
                        var boxX = hx.toInt() + 10
                        var boxY = hy.toInt() - boxH - 8
                        if (boxX + boxW > chartX + chartW) boxX = hx.toInt() - boxW - 10
                        if (boxX < chartX) boxX = chartX
                        if (boxY < chartY) boxY = hy.toInt() + 10
                        if (boxY + boxH > chartY + chartH) boxY = (chartY + chartH - boxH).coerceAtLeast(chartY)
                        g2.color = Color(24, 24, 24, 225)
                        g2.fillRoundRect(boxX, boxY, boxW, boxH, 10, 10)
                        g2.color = Color(245, 245, 245, 235)
                        g2.drawRoundRect(boxX, boxY, boxW, boxH, 10, 10)
                        g2.color = Color(255, 255, 255, 245)
                        g2.drawString(detail, boxX + padX, boxY + padY + fm.ascent)
                    }
                }

                g2.color = UIManager.getColor("Label.foreground") ?: foreground
                g2.font = g2.font.deriveFont(Font.PLAIN, 11f)
                val latest = list.last()
                g2.drawString(
                    ServerI18n.tr("aerogel.dashboard.graph.now", DECIMAL_2.format(latest)),
                    chartX + 6,
                    chartY + 14
                )
                g2.drawString(
                    ServerI18n.tr("aerogel.dashboard.graph.max", DECIMAL_2.format(upper)),
                    chartX + chartW - 85,
                    chartY + 14
                )
            } finally {
                g2.dispose()
            }
        }

        private fun detailTextForIndex(index: Int, list: List<Double>): String {
            val points = list.size
            if (points < 2 || index !in 0 until points) return ""
            val value = list[index]
            val targetTps = targetTpsUpperBound().coerceAtLeast(1.0)
            val secondsAgo = ((points - 1 - index).coerceAtLeast(0)) / targetTps
            return ServerI18n.tr(
                "aerogel.dashboard.graph.tooltip",
                DECIMAL_4.format(value),
                valueUnit,
                formatSecondsAgo(secondsAgo)
            )
        }

        private fun hoverIndexForEvent(event: java.awt.event.MouseEvent): Int? {
            if (history.isEmpty()) return null
            val insets = insets
            val x0 = insets.left
            val y0 = insets.top
            val w = width - insets.left - insets.right
            val h = height - insets.top - insets.bottom
            if (w <= 10 || h <= 10) return null
            val chartX = x0 + 8
            val chartY = y0 + 4
            val chartW = w - 16
            val chartH = h - 8
            if (event.x < chartX || event.x > chartX + chartW || event.y < chartY || event.y > chartY + chartH) {
                return null
            }
            val list = windowedSamples()
            val points = list.size
            if (points < 2) return null
            val clamped = (event.x - chartX).coerceIn(0, chartW)
            return ((clamped.toDouble() / chartW.toDouble()) * (points - 1))
                .toInt()
                .coerceIn(0, points - 1)
        }
    }

    private val DECIMAL_2 = DecimalFormat("0.00")
    private val DECIMAL_4 = DecimalFormat("0.0000")
}
