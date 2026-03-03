package org.macaroon3145.api.event

import org.macaroon3145.api.plugin.PluginContext
import org.macaroon3145.api.plugin.PluginState

data class PluginStateChangeEvent(
    val context: PluginContext,
    val previousState: PluginState,
    val currentState: PluginState
) : Event
