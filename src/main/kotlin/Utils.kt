package org.macaroon3145

object Utils {
    private const val SERVER_CHUNK_CACHE_CLASS = "net.minecraft.server.level.ServerChunkCache"
    private const val SERVER_LEVEL_CLASS = "net.minecraft.server.level.ServerLevel"
    private const val MINECRAFT_SERVER_CLASS = "net.minecraft.server.MinecraftServer"
    private const val LEVEL_STORAGE_ACCESS_CLASS = "net.minecraft.world.level.storage.LevelStorageSource\$LevelStorageAccess"
    private const val DATA_FIXER_CLASS = "com.mojang.datafixers.DataFixer"
    private const val STRUCTURE_TEMPLATE_MANAGER_CLASS =
        "net.minecraft.world.level.levelgen.structure.templatesystem.StructureTemplateManager"
    private const val CHUNK_GENERATOR_CLASS = "net.minecraft.world.level.chunk.ChunkGenerator"
    private const val CHUNK_STATUS_UPDATE_LISTENER_CLASS = "net.minecraft.world.level.entity.ChunkStatusUpdateListener"
    private const val DIMENSION_DATA_STORAGE_CLASS = "net.minecraft.world.level.storage.DimensionDataStorage"

    /**
     * Reflection-only lookup for a vanilla `ServerChunkCache` instance.
     *
     * Supported inputs:
     * - `ServerChunkCache` instance itself
     * - `ServerLevel` instance (uses no-arg method or field reflection)
     * - `MinecraftServer` instance (resolves overworld first, then chunk cache)
     */
    @JvmStatic
    fun getServerChunkCacheReflectively(target: Any): Any {
        findServerChunkCache(target)?.let { return it }
        throw IllegalArgumentException(
            "Could not resolve $SERVER_CHUNK_CACHE_CLASS from ${target.javaClass.name} via reflection"
        )
    }

    @JvmStatic
    fun getServerLevelReflectively(target: Any): Any {
        findServerLevel(target)?.let { return it }
        throw IllegalArgumentException(
            "Could not resolve $SERVER_LEVEL_CLASS from ${target.javaClass.name} via reflection"
        )
    }

    /**
     * Reflection-only constructor call for vanilla ServerChunkCache (1.21.11).
     *
     * Note: `ServerChunkCache` has no no-arg constructor, so all constructor args are required.
     */
    @JvmStatic
    fun newServerChunkCacheReflectively(
        serverLevel: Any,
        levelStorageAccess: Any,
        dataFixer: Any,
        structureTemplateManager: Any,
        executor: java.util.concurrent.Executor,
        chunkGenerator: Any,
        viewDistance: Int,
        simulationDistance: Int,
        syncWrites: Boolean,
        chunkStatusUpdateListener: Any,
        dimensionDataStorageSupplier: java.util.function.Supplier<*>
    ): Any {
        require(serverLevel.javaClass.name == SERVER_LEVEL_CLASS || isTypeOrSuper(serverLevel.javaClass, SERVER_LEVEL_CLASS)) {
            "serverLevel must be $SERVER_LEVEL_CLASS-compatible: ${serverLevel.javaClass.name}"
        }
        require(levelStorageAccess.javaClass.name == LEVEL_STORAGE_ACCESS_CLASS || isTypeOrSuper(levelStorageAccess.javaClass, LEVEL_STORAGE_ACCESS_CLASS)) {
            "levelStorageAccess must be $LEVEL_STORAGE_ACCESS_CLASS-compatible: ${levelStorageAccess.javaClass.name}"
        }
        require(implementsOrMatches(dataFixer.javaClass, DATA_FIXER_CLASS)) {
            "dataFixer must be $DATA_FIXER_CLASS-compatible: ${dataFixer.javaClass.name}"
        }
        require(
            structureTemplateManager.javaClass.name == STRUCTURE_TEMPLATE_MANAGER_CLASS ||
                isTypeOrSuper(structureTemplateManager.javaClass, STRUCTURE_TEMPLATE_MANAGER_CLASS)
        ) {
            "structureTemplateManager must be $STRUCTURE_TEMPLATE_MANAGER_CLASS-compatible: ${structureTemplateManager.javaClass.name}"
        }
        require(
            chunkGenerator.javaClass.name == CHUNK_GENERATOR_CLASS ||
                isTypeOrSuper(chunkGenerator.javaClass, CHUNK_GENERATOR_CLASS)
        ) {
            "chunkGenerator must be $CHUNK_GENERATOR_CLASS-compatible: ${chunkGenerator.javaClass.name}"
        }
        require(implementsOrMatches(chunkStatusUpdateListener.javaClass, CHUNK_STATUS_UPDATE_LISTENER_CLASS)) {
            "chunkStatusUpdateListener must be $CHUNK_STATUS_UPDATE_LISTENER_CLASS-compatible: ${chunkStatusUpdateListener.javaClass.name}"
        }

        val checkedSupplier = java.util.function.Supplier<Any?> {
            val value = dimensionDataStorageSupplier.get()
            if (value != null) {
                require(value.javaClass.name == DIMENSION_DATA_STORAGE_CLASS || isTypeOrSuper(value.javaClass, DIMENSION_DATA_STORAGE_CLASS)) {
                    "dimensionDataStorageSupplier returned non-$DIMENSION_DATA_STORAGE_CLASS: ${value.javaClass.name}"
                }
            }
            value
        }

        val ctor = loadClass(SERVER_CHUNK_CACHE_CLASS).getDeclaredConstructor(
            loadClass(SERVER_LEVEL_CLASS),
            loadClass(LEVEL_STORAGE_ACCESS_CLASS),
            loadClass(DATA_FIXER_CLASS),
            loadClass(STRUCTURE_TEMPLATE_MANAGER_CLASS),
            java.util.concurrent.Executor::class.java,
            loadClass(CHUNK_GENERATOR_CLASS),
            java.lang.Integer.TYPE,
            java.lang.Integer.TYPE,
            java.lang.Boolean.TYPE,
            loadClass(CHUNK_STATUS_UPDATE_LISTENER_CLASS),
            java.util.function.Supplier::class.java
        )
        ctor.isAccessible = true

        return ctor.newInstance(
            serverLevel,
            levelStorageAccess,
            dataFixer,
            structureTemplateManager,
            executor,
            chunkGenerator,
            viewDistance,
            simulationDistance,
            syncWrites,
            chunkStatusUpdateListener,
            checkedSupplier
        )
    }

    private fun findServerChunkCache(target: Any?): Any? {
        if (target == null) return null

        val clazz = target.javaClass
        if (clazz.name == SERVER_CHUNK_CACHE_CLASS) return target

        findServerLevel(target)?.let { level ->
            if (level !== target) {
                findServerChunkCache(level)?.let { return it }
            }
        }

        // 1) Most stable path: any no-arg method returning ServerChunkCache (e.g. ServerLevel#getChunkSource)
        invokeNoArgMethodReturningType(target, clazz, SERVER_CHUNK_CACHE_CLASS)?.let { return it }

        // 2) Fallback to field scan (e.g. ServerLevel#chunkSource)
        readFieldByType(target, clazz, SERVER_CHUNK_CACHE_CLASS)?.let { return it }

        // 3) MinecraftServer -> overworld() / level field -> ServerLevel -> chunkSource
        if (isTypeOrSuper(clazz, MINECRAFT_SERVER_CLASS)) {
            invokeNamedNoArg(target, "overworld")?.let { overworld ->
                findServerChunkCache(overworld)?.let { return it }
            }

            // Fallback: scan for any method/field returning ServerLevel and recurse.
            invokeNoArgMethodReturningType(target, clazz, SERVER_LEVEL_CLASS)?.let { level ->
                findServerChunkCache(level)?.let { return it }
            }
            readFieldByType(target, clazz, SERVER_LEVEL_CLASS)?.let { level ->
                findServerChunkCache(level)?.let { return it }
            }
        }

        // 4) Generic one-hop fallback for subclasses/wrappers that expose a ServerLevel.
        invokeNoArgMethodReturningType(target, clazz, SERVER_LEVEL_CLASS)?.let { level ->
            findServerChunkCache(level)?.let { return it }
        }
        readFieldByType(target, clazz, SERVER_LEVEL_CLASS)?.let { level ->
            findServerChunkCache(level)?.let { return it }
        }

        return null
    }

    private fun findServerLevel(target: Any?): Any? {
        if (target == null) return null

        val clazz = target.javaClass
        if (clazz.name == SERVER_LEVEL_CLASS) return target

        // Preferred vanilla path for MinecraftServer
        if (isTypeOrSuper(clazz, MINECRAFT_SERVER_CLASS)) {
            invokeNamedNoArg(target, "overworld")?.let { overworld ->
                if (overworld.javaClass.name == SERVER_LEVEL_CLASS) return overworld
                findServerLevel(overworld)?.let { return it }
            }

            invokeNamedNoArg(target, "getAllLevels")?.let { allLevels ->
                firstServerLevelFromContainer(allLevels)?.let { return it }
            }

            invokeNamedNoArg(target, "levels")?.let { levels ->
                firstServerLevelFromContainer(levels)?.let { return it }
            }
        }

        // Generic method/field-based discovery
        invokeNoArgMethodReturningType(target, clazz, SERVER_LEVEL_CLASS)?.let { return it }
        readFieldByType(target, clazz, SERVER_LEVEL_CLASS)?.let { return it }

        // Common containers (Collection/Map/Iterable/arrays) that may hold ServerLevel
        for (type in sequenceOf(clazz, *clazz.superclasses().toTypedArray())) {
            for (method in type.declaredMethods) {
                if (method.parameterCount != 0) continue
                val value = runCatching {
                    method.isAccessible = true
                    method.invoke(target)
                }.getOrNull() ?: continue
                firstServerLevelFromContainer(value)?.let { return it }
            }
        }
        for (type in sequenceOf(clazz, *clazz.superclasses().toTypedArray())) {
            for (field in type.declaredFields) {
                val value = runCatching {
                    field.isAccessible = true
                    field.get(target)
                }.getOrNull() ?: continue
                firstServerLevelFromContainer(value)?.let { return it }
            }
        }

        return null
    }

    private fun invokeNamedNoArg(target: Any, methodName: String): Any? {
        val method = sequenceOf(target.javaClass, *target.javaClass.superclasses().toTypedArray())
            .flatMap { it.declaredMethods.asSequence() }
            .firstOrNull { it.name == methodName && it.parameterCount == 0 }
            ?: return null

        return runCatching {
            method.isAccessible = true
            method.invoke(target)
        }.getOrNull()
    }

    private fun invokeNoArgMethodReturningType(target: Any, clazz: Class<*>, returnTypeName: String): Any? {
        for (type in sequenceOf(clazz, *clazz.superclasses().toTypedArray())) {
            for (method in type.declaredMethods) {
                if (method.parameterCount != 0) continue
                if (method.returnType.name != returnTypeName) continue
                val value = runCatching {
                    method.isAccessible = true
                    method.invoke(target)
                }.getOrNull()
                if (value != null) return value
            }
        }
        return null
    }

    private fun readFieldByType(target: Any, clazz: Class<*>, fieldTypeName: String): Any? {
        for (type in sequenceOf(clazz, *clazz.superclasses().toTypedArray())) {
            for (field in type.declaredFields) {
                if (field.type.name != fieldTypeName) continue
                val value = runCatching {
                    field.isAccessible = true
                    field.get(target)
                }.getOrNull()
                if (value != null) return value
            }
        }
        return null
    }

    private fun isTypeOrSuper(clazz: Class<*>, fqcn: String): Boolean {
        return sequenceOf(clazz, *clazz.superclasses().toTypedArray()).any { it.name == fqcn }
    }

    private fun loadClass(name: String): Class<*> {
        val contextLoader = Thread.currentThread().contextClassLoader
        return try {
            Class.forName(name, false, contextLoader)
        } catch (_: ClassNotFoundException) {
            Class.forName(name)
        }
    }

    private fun implementsOrMatches(clazz: Class<*>, fqcn: String): Boolean {
        if (isTypeOrSuper(clazz, fqcn)) return true
        return sequenceOf(clazz, *clazz.superclasses().toTypedArray())
            .flatMap { it.interfaces.asSequence() }
            .any { it.name == fqcn }
    }

    private fun firstServerLevelFromContainer(container: Any?): Any? {
        if (container == null) return null
        if (container.javaClass.name == SERVER_LEVEL_CLASS) return container

        val values: Iterable<*> = when (container) {
            is Map<*, *> -> container.values
            is Iterable<*> -> container
            is Array<*> -> container.asIterable()
            else -> return null
        }

        for (element in values) {
            if (element == null) continue
            if (element.javaClass.name == SERVER_LEVEL_CLASS) return element
            findServerLevel(element)?.let { return it }
        }
        return null
    }

    private fun Class<*>.superclasses(): List<Class<*>> {
        val result = ArrayList<Class<*>>()
        var current = this.superclass
        while (current != null) {
            result += current
            current = current.superclass
        }
        return result
    }
}
