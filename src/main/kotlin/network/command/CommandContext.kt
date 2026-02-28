package org.macaroon3145.network.command

import org.macaroon3145.network.handler.PlayPackets
import org.macaroon3145.network.handler.PlayerSession

interface CommandContext {
    fun sendConsoleMessage(message: String)
    fun sendMessage(target: PlayerSession, message: String)
    fun sendTranslation(target: PlayerSession, key: String, vararg args: PlayPackets.ChatComponent)
    fun sendErrorTranslation(target: PlayerSession, key: String, vararg args: PlayPackets.ChatComponent)
    fun sendUnknownCommandWithContext(target: PlayerSession?, input: String)
    fun sendSourceTranslation(target: PlayerSession?, key: String, vararg args: PlayPackets.ChatComponent)
    fun sendSourceErrorTranslation(target: PlayerSession?, key: String, vararg args: PlayPackets.ChatComponent)
    fun sendSourceWarnTranslation(target: PlayerSession?, key: String, vararg args: PlayPackets.ChatComponent)
    fun sendTranslationFromTerminal(target: PlayerSession, key: String, vararg args: PlayPackets.ChatComponent)
    fun sourceWorldKey(target: PlayerSession?): String
    fun sourceX(target: PlayerSession?): Double
    fun sourceY(target: PlayerSession?): Double
    fun sourceZ(target: PlayerSession?): Double
    fun onlinePlayers(): List<PlayerSession>
    fun onlinePlayersInWorld(worldKey: String): List<PlayerSession>
    fun findOnlinePlayer(name: String): PlayerSession?
    fun isOperator(session: PlayerSession?): Boolean
    fun canUseOpCommand(sender: PlayerSession?): Boolean
    fun grantOperator(target: PlayerSession): Boolean
    fun teleport(target: PlayerSession, x: Double, y: Double, z: Double, yaw: Float? = null, pitch: Float? = null): Boolean
    fun setGamemode(target: PlayerSession, mode: Int)
}
