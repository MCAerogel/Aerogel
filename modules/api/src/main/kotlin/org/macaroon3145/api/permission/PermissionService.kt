package org.macaroon3145.api.permission

import org.macaroon3145.api.command.CommandSender

enum class PermissionResult {
    ALLOW,
    DENY,
    UNSET
}

interface PermissionService {
    fun check(sender: CommandSender?, node: String): PermissionResult

    fun has(sender: CommandSender?, node: String): Boolean {
        return check(sender, node) == PermissionResult.ALLOW
    }
}
