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
                    context.sendSourceWarnTranslation(null, "permissions.requires.entity")
                    return
                }
                tpSelfToPlayer(context, sender, args[0])
            }
            2 -> tpPlayersToPlayer(context, sender, args[0], args[1])
            3 -> {
                if (sender == null) {
                    context.sendSourceWarnTranslation(null, "permissions.requires.entity")
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
        val x = args.getOrNull(startIndex)?.toDoubleOrNull()
        val y = args.getOrNull(startIndex + 1)?.toDoubleOrNull()
        val z = args.getOrNull(startIndex + 2)?.toDoubleOrNull()
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
        val x = args.getOrNull(startIndex)?.toDoubleOrNull()
        val y = args.getOrNull(startIndex + 1)?.toDoubleOrNull()
        val z = args.getOrNull(startIndex + 2)?.toDoubleOrNull()
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
}
