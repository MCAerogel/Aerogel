package org.macaroon3145.network.command

import org.macaroon3145.network.handler.PlayPackets
import org.macaroon3145.network.handler.PlayerSession
import java.util.UUID

interface CommandContext {
    fun sendConsoleMessage(message: String)
    fun sendMessage(target: PlayerSession, message: String)
    fun sendTranslation(target: PlayerSession, key: String, vararg args: PlayPackets.ChatComponent)
    fun sendErrorTranslation(target: PlayerSession, key: String, vararg args: PlayPackets.ChatComponent)
    fun sendUnknownCommandWithContext(target: PlayerSession?, input: String)
    fun sendSourceTranslationWithContext(
        target: PlayerSession?,
        key: String,
        input: String,
        errorStart: Int,
        vararg args: PlayPackets.ChatComponent
    )
    fun sendSourceTranslation(target: PlayerSession?, key: String, vararg args: PlayPackets.ChatComponent)
    fun sendSourceSuccessTranslation(target: PlayerSession?, key: String, vararg args: PlayPackets.ChatComponent)
    fun sendSourceErrorTranslation(target: PlayerSession?, key: String, vararg args: PlayPackets.ChatComponent)
    fun sendSourceWarnTranslation(target: PlayerSession?, key: String, vararg args: PlayPackets.ChatComponent)
    fun sendTranslationFromTerminal(target: PlayerSession, key: String, vararg args: PlayPackets.ChatComponent)
    fun sendAdminTranslation(target: PlayerSession, source: PlayPackets.ChatComponent, key: String, vararg args: PlayPackets.ChatComponent)
    fun sourceWorldKey(target: PlayerSession?): String
    fun sourceX(target: PlayerSession?): Double
    fun sourceY(target: PlayerSession?): Double
    fun sourceZ(target: PlayerSession?): Double
    fun onlinePlayers(): List<PlayerSession>
    fun onlinePlayersInWorld(worldKey: String): List<PlayerSession>
    fun findOnlinePlayer(name: String): PlayerSession?
    fun findOnlinePlayer(uuid: UUID): PlayerSession?
    fun isOperator(session: PlayerSession?): Boolean
    fun canUseOpCommand(sender: PlayerSession?): Boolean
    fun grantOperator(target: PlayerSession): Boolean
    fun revokeOperator(uuid: UUID): Boolean
    fun playerNameComponent(target: PlayerSession): PlayPackets.ChatComponent
    fun teleport(target: PlayerSession, x: Double, y: Double, z: Double, yaw: Float? = null, pitch: Float? = null): Boolean
    fun setGamemode(target: PlayerSession, mode: Int)
    fun worldKeys(): List<String>
    fun worldTimeSnapshot(worldKey: String): Pair<Long, Long>?
    fun setWorldTime(worldKey: String, timeOfDayTicks: Long): Pair<Long, Long>?
    fun addWorldTime(worldKey: String, deltaTicks: Long): Pair<Long, Long>?
    fun showDashboard(): Boolean
    fun stopServer(): Boolean
    fun reloadAllPlugins(): Int
    fun reloadPlugin(pluginId: String): Boolean
    fun pluginIds(): List<String>
}
