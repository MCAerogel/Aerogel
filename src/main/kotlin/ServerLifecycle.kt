package org.macaroon3145

import io.netty.channel.Channel
import io.netty.channel.EventLoopGroup
import org.macaroon3145.i18n.ServerI18n
import org.macaroon3145.network.handler.PlayerSessionManager
import org.macaroon3145.perf.GameLoop
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.system.exitProcess

object ServerLifecycle {
    private val stopping = AtomicBoolean(false)

    @Volatile
    private var serverChannel: Channel? = null

    @Volatile
    private var bossGroup: EventLoopGroup? = null

    @Volatile
    private var workerGroup: EventLoopGroup? = null

    fun registerNetworking(
        serverChannel: Channel,
        bossGroup: EventLoopGroup,
        workerGroup: EventLoopGroup
    ) {
        this.serverChannel = serverChannel
        this.bossGroup = bossGroup
        this.workerGroup = workerGroup
    }

    fun stopServer(): Boolean {
        if (!stopping.compareAndSet(false, true)) {
            return false
        }

        Thread({
            DebugConsole.withSpinner(
                progressMessage = ServerI18n.tr("aerogel.log.stop.progress"),
                doneMessage = ServerI18n.tr("aerogel.log.stop.done")
            ) {
                GameLoop.stop()
                PlayerSessionManager.shutdown()
                runCatching { serverChannel?.close()?.syncUninterruptibly() }
                val bossShutdown = runCatching {
                    bossGroup?.shutdownGracefully(0L, 1L, TimeUnit.SECONDS)
                }.getOrNull()
                val workerShutdown = runCatching {
                    workerGroup?.shutdownGracefully(0L, 1L, TimeUnit.SECONDS)
                }.getOrNull()
                runCatching { bossShutdown?.syncUninterruptibly() }
                runCatching { workerShutdown?.syncUninterruptibly() }
                runCatching { stopFoliaRuntime() }
            }
            exitProcess(0)
        }, "aerogel-stop-sequence").apply {
            isDaemon = false
            start()
        }
        return true
    }
}
