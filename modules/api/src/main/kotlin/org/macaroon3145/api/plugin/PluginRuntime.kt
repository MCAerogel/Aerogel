package org.macaroon3145.api.plugin

object PluginRuntime {
    @Volatile
    private var ownerProvider: (() -> String?)? = null
    @Volatile
    private var currentContextProvider: (() -> PluginContext?)? = null
    @Volatile
    private var contextProvider: ((String) -> PluginContext?)? = null

    /**
     * Runtime wiring entrypoint used by the server implementation.
     */
    fun bind(
        ownerProvider: () -> String?,
        currentContextProvider: () -> PluginContext?,
        contextProvider: (String) -> PluginContext?
    ) {
        this.ownerProvider = ownerProvider
        this.currentContextProvider = currentContextProvider
        this.contextProvider = contextProvider
    }

    /**
     * Runtime teardown entrypoint used by the server implementation.
     */
    fun clearBindings() {
        ownerProvider = null
        currentContextProvider = null
        contextProvider = null
    }

    fun currentOwnerOrNull(): String? = ownerProvider?.invoke()

    fun currentContextOrNull(): PluginContext? {
        currentContextProvider?.invoke()?.let { return it }
        val owner = currentOwnerOrNull() ?: return null
        val resolver = contextProvider ?: return null
        return resolver(owner)
    }

    fun requireCurrentContext(): PluginContext {
        return checkNotNull(currentContextOrNull()) {
            "No active plugin context found. Call this API inside plugin lifecycle/event callbacks."
        }
    }
}
