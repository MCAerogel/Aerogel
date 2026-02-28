package org.macaroon3145.network.command

import org.macaroon3145.network.handler.PlayerSession
import org.macaroon3145.network.handler.PlayPackets
import java.util.Locale

object TpCommand : Command {
    override fun execute(context: CommandContext, sender: PlayerSession?, args: List<String>) {
        if (!context.isOperator(sender)) {
            val input = if (args.isEmpty()) "tp" else "tp ${args.joinToString(" ")}"
            context.sendUnknownCommandWithContext(sender, input)
            return
        }

        when (args.size) {
            1 -> {
                if (sender == null) {
                    context.sendSourceWarnTranslation(null, "aerogel.console.tp.requires.self")
                    return
                }
                tpSelfToPlayer(context, sender, args[0])
            }
            2 -> tpPlayersToPlayer(context, sender, args[0], args[1])
            3 -> {
                if (sender == null) {
                    context.sendSourceWarnTranslation(null, "aerogel.console.tp.requires.self")
                    return
                }
                tpToCoordinates(context, sender, sender, args, 0)
            }
            4 -> tpPlayersToCoordinates(context, sender, args[0], args, 1)
            else -> if (sender == null) {
                context.sendSourceWarnTranslation(null, "command.unknown.argument")
            } else {
                context.sendSourceTranslation(sender, "command.unknown.argument")
            }
        }
    }

    private fun tpSelfToPlayer(context: CommandContext, sender: PlayerSession, destinationName: String) {
        val destination = EntitySelectorResolver.resolvePlayers(context, sender, destinationName, single = true)?.firstOrNull() ?: return
        context.teleport(sender, destination.x, destination.y, destination.z, destination.yaw, destination.pitch)
        context.sendSourceTranslation(
            sender,
            "commands.teleport.success.entity.single",
            PlayPackets.ChatComponent.Text(sender.profile.username),
            PlayPackets.ChatComponent.Text(destination.profile.username)
        )
    }

    private fun tpPlayersToPlayer(context: CommandContext, sender: PlayerSession?, sourceToken: String, destinationToken: String) {
        val sources = EntitySelectorResolver.resolvePlayers(context, sender, sourceToken, single = false) ?: return
        val destination = EntitySelectorResolver.resolvePlayers(context, sender, destinationToken, single = true)?.firstOrNull() ?: return
        for (source in sources) {
            context.teleport(source, destination.x, destination.y, destination.z, destination.yaw, destination.pitch)
            if (sender == null || source.channelId != sender.channelId) {
                context.sendTranslation(
                    source,
                    "commands.teleport.success.entity.single",
                    PlayPackets.ChatComponent.Text(source.profile.username),
                    PlayPackets.ChatComponent.Text(destination.profile.username)
                )
            }
        }
        if (sources.size == 1) {
            context.sendSourceTranslation(
                sender,
                "commands.teleport.success.entity.single",
                PlayPackets.ChatComponent.Text(sources.first().profile.username),
                PlayPackets.ChatComponent.Text(destination.profile.username)
            )
        } else {
            context.sendSourceTranslation(
                sender,
                "commands.teleport.success.entity.multiple",
                PlayPackets.ChatComponent.Text(sources.size.toString()),
                PlayPackets.ChatComponent.Text(destination.profile.username)
            )
        }
    }

    private fun tpPlayersToCoordinates(
        context: CommandContext,
        sender: PlayerSession?,
        sourceToken: String,
        args: List<String>,
        startIndex: Int
    ) {
        val sources = EntitySelectorResolver.resolvePlayers(context, sender, sourceToken, single = false) ?: return
        val x = parseCoordinate(context, sender, args.getOrNull(startIndex), 0)
        val y = parseCoordinate(context, sender, args.getOrNull(startIndex + 1), 1)
        val z = parseCoordinate(context, sender, args.getOrNull(startIndex + 2), 2)
        if (x == null || y == null || z == null) {
            if (sender == null) {
                context.sendSourceWarnTranslation(null, "command.unknown.argument")
            } else {
                context.sendSourceTranslation(sender, "command.unknown.argument")
            }
            return
        }

        for (source in sources) {
            if (!context.teleport(source, x, y, z, null, null)) {
                context.sendSourceTranslation(sender, "commands.teleport.invalidPosition")
                return
            }
        }

        if (sources.size == 1) {
            context.sendSourceTranslation(
                sender,
                "commands.teleport.success.location.single",
                PlayPackets.ChatComponent.Text(sources.first().profile.username),
                PlayPackets.ChatComponent.Text(formatCoord(x)),
                PlayPackets.ChatComponent.Text(formatCoord(y)),
                PlayPackets.ChatComponent.Text(formatCoord(z))
            )
        } else {
            context.sendSourceTranslation(
                sender,
                "commands.teleport.success.location.multiple",
                PlayPackets.ChatComponent.Text(sources.size.toString()),
                PlayPackets.ChatComponent.Text(formatCoord(x)),
                PlayPackets.ChatComponent.Text(formatCoord(y)),
                PlayPackets.ChatComponent.Text(formatCoord(z))
            )
        }
    }

    private fun tpToCoordinates(
        context: CommandContext,
        sender: PlayerSession,
        target: PlayerSession,
        args: List<String>,
        startIndex: Int
    ) {
        val x = parseCoordinate(context, sender, args.getOrNull(startIndex), 0)
        val y = parseCoordinate(context, sender, args.getOrNull(startIndex + 1), 1)
        val z = parseCoordinate(context, sender, args.getOrNull(startIndex + 2), 2)
        if (x == null || y == null || z == null) {
            context.sendSourceTranslation(sender, "command.unknown.argument")
            return
        }

        // No rotation arguments were provided -> keep the player's current yaw/pitch.
        if (!context.teleport(target, x, y, z, null, null)) {
            context.sendSourceTranslation(sender, "commands.teleport.invalidPosition")
            return
        }
        context.sendSourceTranslation(
            sender,
            "commands.teleport.success.location.single",
            PlayPackets.ChatComponent.Text(target.profile.username),
            PlayPackets.ChatComponent.Text(formatCoord(x)),
            PlayPackets.ChatComponent.Text(formatCoord(y)),
            PlayPackets.ChatComponent.Text(formatCoord(z))
        )
        if (target.channelId != sender.channelId) {
            context.sendTranslation(
                target,
                "commands.teleport.success.location.single",
                PlayPackets.ChatComponent.Text(target.profile.username),
                PlayPackets.ChatComponent.Text(formatCoord(x)),
                PlayPackets.ChatComponent.Text(formatCoord(y)),
                PlayPackets.ChatComponent.Text(formatCoord(z))
            )
        }
    }

    private fun formatCoord(value: Double): String = String.format(Locale.ROOT, "%f", value)

    private fun parseCoordinate(
        context: CommandContext,
        sender: PlayerSession?,
        token: String?,
        axis: Int
    ): Double? {
        val raw = token?.trim() ?: return null
        if (raw.isEmpty() || raw.startsWith("^")) return null
        if (!raw.startsWith("~")) {
            return raw.toDoubleOrNull()
        }

        val base = when (axis) {
            0 -> context.sourceX(sender)
            1 -> context.sourceY(sender)
            else -> context.sourceZ(sender)
        }
        val suffix = raw.substring(1)
        if (suffix.isEmpty()) return base
        val offset = suffix.toDoubleOrNull() ?: return null
        return base + offset
    }
}
